use std::{
    any::TypeId,
    cell::RefCell,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    time::{Duration, Instant},
};

macro_rules! impl_current_static_getter {
    () => {
        /// Get the current value from the context stack.
        ///
        /// # Panics
        ///
        /// Panics if no value is set in the context.
        #[inline(always)]
        pub fn current() -> Self {
            $crate::context::current::<Self>()
        }
    };
}

/// Deadline for this specific request.
///
/// Kiso uses this context value for setting timeouts and deadlines for RPCs and other
/// requests during request processing. If a more thight deadline is needed for parts
/// of the flow, use the context functions to run them with a different deadline. Note
/// that this wont check if the deadline is really shorter than the current one.
#[derive(Debug, Clone, Copy)]
pub struct Deadline(pub(crate) Instant);

impl Deadline {
    /// Instant when the deadline will be reached.
    pub const fn instant(self) -> Instant {
        self.0
    }

    /// Timeout to the deadline instant.
    pub fn timeout(self) -> Duration {
        self.0.duration_since(Instant::now())
    }

    /// Create a deadline that expires after the given duration.
    pub fn after(dur: Duration) -> Self {
        Self(Instant::now() + dur)
    }

    impl_current_static_getter!();
}

/// Get the current value from the context stack.
///
/// Use this only if you need the value for longer than a simple transformation, [`with`]
/// should be preferred whenever possible.
///
/// All Kiso's context types have a static `current` getter, thus using this function with
/// them is not necessary.
///
/// # Panics
///
/// Panics if no value for the given type is set, or if the cloning of the value panics.
pub fn current<T: Clone + 'static>() -> T {
    with(|val: &T| val.clone())
}

/// Get the current value from the context stack.
///
/// Use this only if you need the value for longer than a simple transformation, [`try_with`]
/// should be preferred whenever possible.
///
/// All Kiso's context types have a static `current` getter, thus using this function with
/// them is not necessary.
///
/// # Panics
///
/// Panics if no value for the given type is set, or if the cloning of the value panics.
pub fn try_current<T: Clone + 'static>() -> Option<T> {
    try_with(|val: &T| val.clone())
}

/// Executes a function with a reference to the current value in the context stack.
///
/// # Panics
///
/// Panics if no value for the given type is set.
pub fn with<T: 'static, O>(f: impl FnOnce(&T) -> O) -> O {
    let ptr = current_ptr(TypeId::of::<T>());

    assert!(
        !ptr.is_null(),
        "no context value for type {}",
        std::any::type_name::<T>()
    );

    f(unsafe { &*(ptr as *const T) })
}

/// Executes a function with a reference to the current value in the context stack.
pub fn try_with<T: 'static, O>(f: impl FnOnce(&T) -> O) -> Option<O> {
    let ptr = current_ptr(TypeId::of::<T>());

    unsafe {
        if ptr.is_null() {
            None
        } else {
            Some(f(&*(ptr as *const T)))
        }
    }
}

/// Push a value into the context stack inside a function execution.
#[allow(clippy::needless_pass_by_value)]
pub fn scope_sync<T: 'static, O>(value: T, func: impl FnOnce() -> O) -> O {
    let _guard = StackGuard::new(TypeId::of::<T>(), &value as *const T as _);
    func()
}

/// Push a value into the context stack inside a future execution.
///
/// Note that this function only work with created futures. Sometimes, the futures's
/// creation itself also needs the context value, like in tower's services. In these
/// cases, you need to use both [`scope_sync`] and [`scope`].
pub async fn scope<T: 'static, O>(value: T, fut: impl Future<Output = O>) -> O {
    struct AsyncStackGuard<F, U: 'static> {
        value: U,
        fut: F,
    }

    impl<F, U> Future for AsyncStackGuard<F, U>
    where
        F: Future,
    {
        type Output = F::Output;

        fn poll(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            unsafe {
                let this = self.get_unchecked_mut();

                let _guard = StackGuard::new(TypeId::of::<U>(), &this.value as *const U as _);

                Pin::new_unchecked(&mut this.fut).poll(cx)
            }
        }
    }

    AsyncStackGuard { value, fut }.await
}

// # Implementation notes
//
// Each context stack can be thought as a stack-based linked list, where each node is a
// combination of the scope value, kept pinned inside the scope functions, the
// associated StackGuard.
//
// The STACK vector's elements always points to the current stack value, for each type.
// We never remove elements from the vector, its size is limited to the number of types
// inserted into a context scope.
//
// StackGuard's responsability is to ensure the previous stack value is (almost) always
// reset at the end of its scope. Note that, due to Rust not guaranteeing that destructors
// will be run, the value may not be reset, but this only occurs in extreme circuntances.

std::thread_local! {
    static STACK: RefCell<Vec<(TypeId, *const ())>> = const { RefCell::new(Vec::new()) };
}

fn current_ptr(type_id: TypeId) -> *const () {
    STACK
        .with_borrow(|s| {
            s.iter()
                .find_map(|(t, ptr)| (*t == type_id).then_some(ptr))
                .copied()
        })
        .unwrap_or(std::ptr::null())
}

struct StackGuard {
    prev: *const (),
    idx: usize,
    _phantom: PhantomData<*mut ()>,
    _pinned: PhantomPinned,
}

impl StackGuard {
    fn new(type_id: TypeId, value: *const ()) -> Self {
        let (idx, curr_item) = STACK.with_borrow_mut(|s| {
            let idx = s
                .iter()
                .position(|(t, _)| *t == type_id)
                .unwrap_or_else(|| {
                    s.push((type_id, std::ptr::null()));
                    s.len() - 1
                });

            let curr_ptr = unsafe { &mut s.get_unchecked_mut(idx).1 };
            let new_ptr = std::mem::replace(curr_ptr, value);

            (idx, new_ptr)
        });

        StackGuard {
            prev: curr_item,
            idx,
            _phantom: PhantomData,
            _pinned: PhantomPinned,
        }
    }
}

impl Drop for StackGuard {
    fn drop(&mut self) {
        STACK.with_borrow_mut(|s| unsafe { s.get_unchecked_mut(self.idx) }.1 = self.prev);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_stack() {
        scope_sync("bla", || {
            assert_eq!(current::<&'static str>(), "bla");

            scope_sync("foo", || {
                assert_eq!(current::<&'static str>(), "foo");
            });

            scope_sync(vec![1, 2, 3u8], || {
                assert_eq!(current::<Vec<u8>>(), vec![1, 2, 3]);
            });

            assert_eq!(current::<&'static str>(), "bla");
        });
    }

    #[test]
    #[should_panic(expected = "no context value for type &str")]
    fn test_no_value() {
        let _ = current::<&'static str>();
    }
}
