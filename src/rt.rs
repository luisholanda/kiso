//! # Kiso Runtime
//!
//! Kiso uses tokio as it executor, but add extra configurations on top of the executor
//! to ensure optimal performance for I/O bound servers.
//!
//! TODO(tracing): document tracing related features when implemented.
use std::{
    future::Future,
    process::Termination,
    sync::{atomic::AtomicU16, Arc},
};

use clap::Parser as _;
use once_cell::sync::Lazy;

crate::settings!(pub(crate) RuntimeSettings {
    /// The number of scheduler ticks before checking for external events.
    ///
    /// Default to 31, differently from tokio's default of 61, as we focus
    /// on I/O bound servers.
    ///
    /// See [`tokio::runtime::Builder::event_interval`] for more info.
    runtime_event_interval_ticks: u32 = 31,
    /// If we should start the runtime in a single CPU.
    ///
    /// If not set, the runtime will start in all the available CPUs.
    ///
    /// This is useful during development to reduce the load in the system.
    runtime_single_cpu: bool,
    /// If we should pin some runtime threads to each CPU core.
    ///
    /// This reduces the number of CPU cache misses, by ensuring a task is always
    /// executed in the same core.
    runtime_pin_threads: bool = true,
    /// The number of threads to pin per CPU core.
    ///
    /// Default to 2.
    runtime_pinned_threads_per_core: usize = 2,
    /// The size of the thread pool used by the executor.
    ///
    /// The thread pool is used to run both futures and background blocking operations.
    ///
    /// Default to 512.
    runtime_thread_pool_size: usize = 512,
});

/// Blocks the execution on a future.
pub fn block_on<R: Termination>(fut: impl Future<Output = R>) -> R {
    RUNTIME.block_on(fut)
}

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    let rt_settings = RuntimeSettings::parse();
    let mut builder = if rt_settings.runtime_single_cpu {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    };

    if !rt_settings.runtime_single_cpu {
        builder.on_thread_start(runtime_thread_start(&rt_settings));
    }

    builder
        .enable_all()
        .worker_threads(rt_settings.runtime_thread_pool_size)
        .event_interval(rt_settings.runtime_event_interval_ticks)
        .build()
        .expect("failed to start tokio runtime")
});

fn runtime_thread_start(settings: &RuntimeSettings) -> impl Fn() + Send + Sync + 'static {
    let ncpus = num_cpus::get();
    let mut indexes = Vec::with_capacity(settings.runtime_pinned_threads_per_core * ncpus);

    // We only pin the initial threads as they will be mostly used by the runtime
    // cores. We pin another batch to keep some blocking threads fixed to CPUs.
    indexes.extend((0..ncpus).chain(0..ncpus));

    let count = Arc::new(AtomicU16::new(0));

    let pin_threads = settings.runtime_pin_threads;

    move || {
        if pin_threads {
            let idx = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;

            if idx < indexes.len() {
                let cpu = indexes[idx];

                let mut cpuset = nix::sched::CpuSet::new();
                cpuset.set(cpu).expect("could not set valid CPU");

                let _ = nix::sched::sched_setaffinity(nix::unistd::getpid(), &cpuset);
            }
        }
    }
}
