#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::{
    cell::Cell,
    future::Future,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use backtrace::Frame;
use flume::{Receiver, Sender};
use nix::sys::timerfd;

/// Run a synchronous function, detecting if it stalls the executor.
///
/// Kiso's runtime already instrument most of its entry points with this function,
/// thus most application code will not need to interact with it. Manual instrumentation
/// is only necessary when integrating with others runtimes.
///
/// # Safety
///
/// This function assumes that the [`LocalStallDetector`] was previously registered
/// in the executing thread, which is always the case if running inside the Kiso's
/// runtime.
///
/// Is safe to reenter this function: The detector knowns that it is armed and turn
/// this call into an almost no-op.
#[inline(always)]
pub unsafe fn detect_stall<F, O>(func: F) -> O
where
    F: FnOnce() -> O,
{
    let guard = StallGuard {
        detector: LocalStallDetector::instance(),
        start: Instant::now(),
    };

    if guard.detector.armed.get() {
        std::mem::forget(guard);
        return func();
    }

    guard.detector.arm();

    func()
}

/// Run a future, detecting if it [`Future::poll`] implementation stalls the executor.
///
/// Kiso's runtime already instrument most of its entry points with this function,
/// thus most application code will not need to interact with it. Manual instrumentation
/// is only necessary when integrating with others runtimes.
///
/// # Safety
///
/// This function assumes that the [`LocalStallDetector`] was previously registered
/// in the executing thread, which is always the case if running inside the Kiso's
/// runtime.
///
/// Is safe to reenter this function: The detector knowns that it is armed and turn
/// this call into an almost no-op.
#[inline(always)]
pub async unsafe fn detect_stall_on_poll<F>(fut: F) -> F::Output
where
    F: Future,
{
    let mut fut = std::pin::pin!(fut);
    std::future::poll_fn(|ctx| detect_stall(|| fut.as_mut().poll(ctx))).await
}

pub(super) struct LocalStallDetector {
    terminated: Arc<AtomicBool>,
    timer: Arc<timerfd::TimerFd>,
    signal: signal_hook::SigId,
    stall_timeout: Duration,
    rx: Receiver<Frame>,
    armed: Cell<bool>,
    #[cfg(test)]
    detections_count: Arc<AtomicU64>,
}

impl Drop for LocalStallDetector {
    fn drop(&mut self) {
        self.terminated.store(true, Ordering::Release);

        signal_hook::low_level::unregister(self.signal);

        self.timer
            .set(
                timerfd::Expiration::OneShot(Duration::from_millis(1).into()),
                timerfd::TimerSetTimeFlags::empty(),
            )
            .expect("failed to set the timer for termination");
    }
}

impl LocalStallDetector {
    thread_local! {
        static INSTANCE: Cell<MaybeUninit<LocalStallDetector>> = const { Cell::new(MaybeUninit::uninit()) };
    }

    unsafe fn instance() -> &'static Self {
        Self::INSTANCE.with(|d| unsafe { (*d.as_ptr()).assume_init_ref() })
    }

    /// Register the stall detector for the current execution thread.
    ///
    /// # Safety
    ///
    /// Can only be called once per thread, calling it more than once will result in
    /// the previously detector being leaked and undefined behavior with stall signal hooks.
    pub(super) unsafe fn register(stall_timeout: Duration, max_backtrace_frames: usize) {
        let timer = Arc::new(
            timerfd::TimerFd::new(
                timerfd::ClockId::CLOCK_MONOTONIC,
                timerfd::TimerFlags::empty(),
            )
            .expect("failed to create stall timer"),
        );
        let terminated = Arc::new(AtomicBool::new(false));
        let (tx, rx) = flume::bounded(max_backtrace_frames);

        let sig_id = Self::install_handler(tx);

        Self::install_trigger(terminated.clone(), timer.clone());

        Self::INSTANCE.set(MaybeUninit::new(Self {
            terminated,
            timer,
            signal: sig_id,
            stall_timeout,
            rx,
            armed: Cell::new(false),
            #[cfg(test)]
            detections_count: Arc::default(),
        }))
    }

    pub(super) unsafe fn unregister() {
        Self::INSTANCE.with(|d| {
            d.replace(MaybeUninit::uninit()).assume_init_drop();
        })
    }

    fn install_handler(tx: Sender<Frame>) -> signal_hook::SigId {
        let exec_thread = std::thread::current().id();
        unsafe {
            signal_hook::low_level::register(nix::libc::SIGUSR1, move || {
                if tx.is_full()
                    || tx.is_disconnected()
                    || std::thread::current().id() != exec_thread
                {
                    return;
                }

                backtrace::trace_unsynchronized(|frame| tx.try_send(frame.clone()).is_ok());
            })
            .expect("failed to register signal hook")
        }
    }

    fn install_trigger(terminated: Arc<AtomicBool>, timer: Arc<timerfd::TimerFd>) {
        struct SendWrapper(nix::libc::pthread_t);
        unsafe impl Send for SendWrapper {}
        let tid = SendWrapper(unsafe { nix::libc::pthread_self() });

        std::thread::spawn(move || {
            while timer.wait().is_ok() {
                if terminated.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }

                unsafe { nix::libc::pthread_kill(tid.0, nix::libc::SIGUSR1) };
            }
        });
    }

    fn arm(&self) {
        self.timer
            .set(
                timerfd::Expiration::OneShot(self.stall_timeout.into()),
                timerfd::TimerSetTimeFlags::empty(),
            )
            .expect("failed to set stall timer");
    }

    fn disarm(&self) {
        self.timer.unset().expect("failed to unset stall timer");
    }
}

struct StallGuard {
    detector: &'static LocalStallDetector,
    start: Instant,
}

impl Drop for StallGuard {
    fn drop(&mut self) {
        self.detector.disarm();

        let mut frames = vec![];
        while let Ok(frame) = self.detector.rx.try_recv() {
            if frames.is_empty() {
                frames.reserve(128);
            }

            frames.push(backtrace::BacktraceFrame::from(frame));
        }

        if frames.is_empty() {
            return;
        }

        let strace = backtrace::Backtrace::from(frames);

        #[cfg(test)]
        self.detector
            .detections_count
            .fetch_add(1, Ordering::Release);

        let elapsed = self.start.elapsed();
        let overage = elapsed.saturating_sub(self.detector.stall_timeout);

        crate::error!(
            "Runtime stall detected, max budget of {}ms, took {}ms, overaged by {}us",
            self.detector.stall_timeout.as_millis(),
            elapsed.as_millis(),
            overage.as_micros()
        )
        .backtrace(strace);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_detect_stall() {
        let detections = unsafe {
            LocalStallDetector::register(Duration::from_millis(100), 128);

            detect_stall(|| {
                std::thread::sleep(Duration::from_millis(20));
            });

            LocalStallDetector::instance()
                .detections_count
                .load(Ordering::Acquire)
        };

        assert_eq!(detections, 0, "erroneous detected stall");
    }

    #[test]
    fn should_detect_stall() {
        let detections = unsafe {
            LocalStallDetector::register(Duration::from_millis(100), 128);

            detect_stall(|| {
                std::thread::sleep(Duration::from_millis(120));
            });

            LocalStallDetector::instance()
                .detections_count
                .load(Ordering::Acquire)
        };

        assert_eq!(
            detections, 1,
            "did not detect stall or detected more than was expected"
        );
    }
}
