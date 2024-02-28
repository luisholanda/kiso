use std::{
    cell::Cell,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    thread::LocalKey,
    time::{Duration, Instant},
};

/// Deadline for this specific request.
///
/// Kiso uses this context value for setting timeouts and deadlines for RPCs and other
/// requests during request processing. If a more thight deadline is needed for parts
/// of the flow, use the context functions to run them with a different deadline. Note
/// that this wont check if the deadline is really shorter than the current one.
#[derive(Debug, Clone, Copy)]
pub struct Deadline(pub(crate) Instant);

crate::impl_context_for_type!(Deadline);

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
pub fn current<T: Context + Clone>() -> T {
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
pub fn try_current<T: Context + Clone>() -> Option<T> {
    try_with(|val: &T| val.clone())
}

/// Executes a function with a reference to the current value in the context stack.
///
/// # Panics
///
/// Panics if no value for the given type is set.
pub fn with<T: Context, O>(f: impl FnOnce(&T) -> O) -> O {
    let ptr = current_ptr::<T>();

    assert!(
        !ptr.is_null(),
        "no context value for type {}",
        std::any::type_name::<T>()
    );

    f(unsafe { &*ptr })
}

/// Executes a function with a reference to the current value in the context stack.
pub fn try_with<T: Context, O>(f: impl FnOnce(&T) -> O) -> Option<O> {
    let ptr = current_ptr::<T>();

    unsafe {
        if ptr.is_null() {
            None
        } else {
            Some(f(&*ptr))
        }
    }
}

/// Push a value into the context stack inside a function execution.
#[allow(clippy::needless_pass_by_value)]
pub fn scope_sync<T: Context, O>(value: T, func: impl FnOnce() -> O) -> O {
    let _guard = StackGuard::new(&value as *const T);
    func()
}

/// Push a value into the context stack inside a future execution.
///
/// Note that this function only work with created futures. Sometimes, the futures's
/// creation itself also needs the context value, like in tower's services. In these
/// cases, you need to use both [`scope_sync`] and [`scope`].
pub async fn scope<T: Context, O>(value: T, fut: impl Future<Output = O>) -> O {
    AsyncStackGuard::new(value, fut).await
}

// # Implementation notes
//
// Each context stack can be thought as a stack-based linked list, where each node is a
// combination of the scope value, kept pinned inside the scope functions/futures.
//
// The Context trait allows us to create a thread-local for each context type. The type
// stored in the local is a Cell to a *const (). Using a () ptr instead of a ptr to the
// type itself allow us to have non-'static context types. Having a thread-local per type
// allow us to bypass thread-local checks after the first access, specially important for
// the async guard.
//
// StackGuard's responsability is to ensure the previous stack value is (almost) always
// reset at the end of its scope. Note that, due to Rust not guaranteeing that destructors
// will be run, the value may not be reset, but this only occurs in extreme circuntances.
//
// SAFETY: The code assumes that the destructor for StackGuard being called. As the type
// is module private and only created in controlled functions, namely `scope` and
// `AsyncStackGuard::poll`, the only case where the destructor wouldn't be called is when
// panics abort the program (either `panic = "abort"` or double panicking), but in these
// cases, the thread would be destroyed, together with the thread-local, thus not causing
// memory errors.

pub trait Context: Sized {
    fn cell() -> &'static LocalKey<Cell<*const ()>>;
}

#[macro_export]
macro_rules! impl_context_for_type {
    ($ty: ty) => {
        $crate::impl_context_for_type!(noextension $ty);

        impl $ty {
            /// Get the current value from the context stack.
            ///
            /// # Panics
            ///
            /// Panics if no value is set in the context.
            #[inline(always)]
            pub fn current() -> Self {
                $crate::context::current::<Self>()
            }
        }
    };

    (noextension $ty: ty) => {
        impl $crate::context::Context for $ty {
            fn cell() -> &'static ::std::thread::LocalKey<::std::cell::Cell<*const ()>> {
                thread_local! {
                    static STACK: ::std::cell::Cell<*const ()> = const { Cell::new(std::ptr::null()) };
                }

                &STACK
            }
        }
    };
}

#[inline(always)]
fn current_ptr<T: Context>() -> *const T {
    T::cell().with(|s| s.get()) as *const T
}

struct StackGuard {
    prev: *const (),
    // Storing this pointer prevents us from having to be generic over the context
    // and also from having to pass by the thread-local checks on drop.
    cell: *mut *const (),
    _pinned: PhantomPinned,
}

impl StackGuard {
    fn new<T: Context>(value: *const T) -> Self {
        let (curr_item, cell) = T::cell().with(|c| {
            let ptr = c.as_ptr();
            (c.replace(value as *const ()), ptr)
        });

        StackGuard {
            prev: curr_item,
            cell,
            _pinned: PhantomPinned,
        }
    }
}

impl Drop for StackGuard {
    fn drop(&mut self) {
        // SAFETY: self.cell is valid as Context requires a thread-local static to the cell.
        unsafe { std::ptr::write(self.cell, self.prev) };
    }
}

struct AsyncStackGuard<F, U> {
    fut: F,
    value: U,
    cell: *mut *const U,
    prev_item: *const U,
}

impl<F, U: Context> AsyncStackGuard<F, U> {
    fn new(value: U, fut: F) -> Self {
        let (curr_item, cell) = U::cell().with(|s| (s.get(), s.as_ptr()));

        Self {
            value,
            fut,
            cell: cell as *mut *const U,
            prev_item: curr_item as *const U,
        }
    }
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

            std::ptr::write(this.cell, std::ptr::from_ref(&this.value));

            let _guard = StackGuard {
                prev: this.prev_item as *const (),
                cell: this.cell as *mut *const (),
                _pinned: PhantomPinned,
            };

            Pin::new_unchecked(&mut this.fut).poll(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl_context_for_type!(noextension &'static str);
    impl_context_for_type!(noextension Vec<u8>);

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
    fn test_panic_with_no_value() {
        let _ = current::<&'static str>();
    }

    #[test]
    #[should_panic(expected = "no context value for type &str")]
    fn test_panic_no_value_after_scope() {
        scope_sync("foo", || {});
        let _ = current::<&'static str>();
    }
}
