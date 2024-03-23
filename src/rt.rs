//! # Kiso Runtime
//!
//! Kiso uses tokio as it executor, but add extra configurations on top of the executor
//! to ensure optimal performance for I/O bound servers.
use std::{
    future::Future,
    sync::{atomic::AtomicU16, Arc},
    time::Duration,
};

use clap::{CommandFactory, FromArgMatches};
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;

mod stall;

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
    runtime_single_cpu: bool = false,
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
    /// The budget tasks have to run a poll call.
    ///
    /// Any task that takes more than this duration to finish polling, will be
    /// considered a stall.
    ///
    /// Defaults to 10ms.
    #[arg(value_parser = crate::settings::DurationParser)]
    runtime_stall_detection_budget: Duration = Duration::from_millis(10),
    /// The max number of frames to capture from the stack position.
    ///
    /// Defaults to 512.
    runtime_stall_detection_max_stall_backtrace_frames: usize = 1 << 10,
});

/// Blocks the thread on the execution of a future.
///
/// Due to the runtime behavior when this function is called, it should only
/// be used for entry-points, like the `main` function.
///
/// See [`tokio::runtime::Runtime::block_on`] for more info.
pub fn block_on<F, R>(fut: F) -> R
where
    F: Future<Output = R> + Send + 'static,
    R: Send,
{
    // We don't run fut directly on the current thread as to properly initialize
    // the runtime and stall detection.
    RUNTIME.block_on(spawn(fut)).expect("failed to join future")
}

/// Spawns a future inside Kiso's runtime as a new task.
pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    // SAFETY: RUNTIME already takes care of initializing the stall detector.
    RUNTIME.spawn(unsafe { self::stall::detect_stall_on_poll(fut) })
}

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    let mut rt_settings = RuntimeSettings::default();
    rt_settings
        .update_from_arg_matches(&RuntimeSettings::command().get_matches())
        .expect("bad runtime settings");

    let mut builder = if rt_settings.runtime_single_cpu {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    };

    if !rt_settings.runtime_single_cpu {
        builder
            .on_thread_start(runtime_thread_start(&rt_settings))
            .on_thread_stop(runtime_thread_stop(&rt_settings));
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
    let stall_budget = settings.runtime_stall_detection_budget;
    let stall_max_frames = settings.runtime_stall_detection_max_stall_backtrace_frames;

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

        // SAFETY: the thread was just initialized, register was never called.
        unsafe {
            self::stall::LocalStallDetector::register(stall_budget, stall_max_frames);
        }
    }
}

fn runtime_thread_stop(_: &RuntimeSettings) -> impl Fn() + Send + Sync + 'static {
    move || {
        // SAFETY: register was called during thread start.
        unsafe {
            self::stall::LocalStallDetector::unregister();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_block_on() {
        let res = super::block_on(async { 1u8 });

        assert_eq!(res, 1);
    }

    #[tokio::test]
    async fn test_spawn() {
        let res = super::spawn(async { 1u8 }).await.unwrap();

        assert_eq!(res, 1);
    }
}
