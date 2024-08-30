//! # Body for Request Retries.
//!
//! Source: <https://github.com/linkerd/linkerd2-proxy/blob/611b8eddffb1f8b3f796edc344967feede3f7e27/linkerd/http-retry/src/lib.rs>
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use http_body::{Body, SizeHint};
use hyper::{body::Frame, http::HeaderMap};
use parking_lot::Mutex;
use tonic::Status;

/// A [`http_body::Body`] that can be replayed in requests' retries.
///
/// # Diferences from linkerd2-proxy version
///
/// Although we use linkerd's version as base, some simplifications were made as
/// we only handle a specific type of body in our requests (`BoxBody`). They
/// are:
///
/// * `BoxBody` doesn't forward the method to the inner body. Meaning that max
///   buffer size check in the constructor and the empty body optimization can't
///   be made. XXX: This can change in later versions.
/// * We always return `Bytes` in the data, returning each buffered chunk
///   instead of sending a data that stores all of them. This is due a type
///   constraint limitation of tonic.
/// * We use `Status` as our error type, again due to a type constraint
///   limitation.
pub(crate) struct ReplayBody<B> {
    /// Buffered state owned by this body if it is actively being polled. If
    /// this body has been polled and no other body owned the state, this will
    /// be `Some`.
    state: Option<BodyState<B>>,
    /// Copy of the state shared across all clones. When the active clone is
    /// dropped, it moves its state back into the shared state to be taken by
    /// the next clone to be polled.
    shared: Arc<SharedState<B>>,

    /// Should this clone replay the buffered body from the shared state before
    /// polling the initial body?
    replay_body: bool,

    /// Next chunk in the buffered state to return when replaying the body.
    next_buf_idx: usize,

    /// Should this clone replay trailers from the shared state?
    replay_trailers: bool,
}

// SAFETY: The implementation ensures that only a single clone of the body can
// be polled at a given time.
//
// XXX: This can be removed if we remove the local `Option<BodyState>` field,
// which would mean having to follow a pointer and lock a mutex at every call
// to `poll_next`, which may or may not cause a noticeable regression.
#[allow(unsafe_code)]
unsafe impl<B> Sync for ReplayBody<B> {}

struct SharedState<B> {
    body: Mutex<Option<BodyState<B>>>,
}

struct BodyState<B> {
    /// Buffered body bytes.
    buf: BufList,
    /// Trailers returned after the body was consumed.
    trailers: Option<HeaderMap>,
    /// Remaining of the wrapped body.
    rest: B,
    /// If `rest` was completely consumed or not.
    is_completed: bool,

    /// Maximum number of bytes to still buffer.
    ///
    /// If this reaches 0, we drop the buffer and cannot replay the body.
    max_remaining_bytes: usize,
}

impl<B> BodyState<B> {
    #[inline]
    const fn is_capped(&self) -> bool {
        self.max_remaining_bytes == 0
    }
}

impl<B> Clone for ReplayBody<B> {
    fn clone(&self) -> Self {
        Self {
            state: None,
            shared: self.shared.clone(),
            // The clone should try to replay from the shared state before
            // reading any additional data from the initial body.
            replay_body: true,
            replay_trailers: true,
            next_buf_idx: 0,
        }
    }
}

impl<B> Drop for ReplayBody<B> {
    fn drop(&mut self) {
        // If this clone owned the shared state, put it back.
        if let Some(state) = self.state.take() {
            *self.shared.body.lock() = Some(state);
        }
    }
}

impl<B> ReplayBody<B> {
    /// Wraps an initial `Body` in a `ReplayBody`.
    ///
    /// In order to prevent unbounded buffering, this takes a maximum number of
    /// bytes to buffer as a second parameter. If more than than that number
    /// of bytes would be buffered, the buffered data is discarded and any
    /// subsequent clones of this body will fail. However, the *currently
    /// active* clone of the body is allowed to continue without erroring. It
    /// will simply stop buffering any additional data for retries.
    ///
    /// If the body has a size hint with a lower bound greater than `max_bytes`,
    /// the original body is returned in the error variant.
    pub fn new(body: B, max_bytes: usize) -> Self {
        Self {
            // Differently from linkerd2-proxy's design, we keep the body in
            // the shared state, as we always clone the body before sending a request.
            shared: Arc::new(SharedState {
                body: Mutex::new(Some(BodyState {
                    buf: Default::default(),
                    trailers: None,
                    rest: body,
                    is_completed: false,
                    max_remaining_bytes: max_bytes.saturating_add(1),
                })),
            }),
            state: None,
            // The initial `ReplayBody` has nothing to replay
            replay_body: false,
            replay_trailers: false,
            next_buf_idx: 0,
        }
    }

    /// Mutably borrows the body state if this clone currently owns it,
    /// or else tries to acquire it from the shared state.
    ///
    /// # Panics
    ///
    /// This panics if another clone has currently acquired the state, based on
    /// the assumption that a retry body will not be polled until the previous
    /// request has been dropped.
    fn acquire_state<'a>(
        state: &'a mut Option<BodyState<B>>,
        shared: &Mutex<Option<BodyState<B>>>,
    ) -> &'a mut BodyState<B> {
        state.get_or_insert_with(|| shared.lock().take().expect("missing body state"))
    }

    /// Returns `true` if the body previously exceeded the configured maximum
    /// length limit.
    ///
    /// If this is true, the body is now empty, and the request should *not* be
    /// retried with this body.
    pub fn is_capped(&self) -> bool {
        self.state.as_ref().map_or_else(
            || {
                self.shared
                    .body
                    .lock()
                    .as_ref()
                    .expect("if our `state` was `None`, the shared state must be `Some`")
                    .is_capped()
            },
            BodyState::is_capped,
        )
    }
}

impl<B: Body<Data = Bytes>> Body for ReplayBody<B>
where
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = unsafe { self.get_unchecked_mut() };
        let state = Self::acquire_state(&mut this.state, &this.shared.body);

        // If we haven't replayed the buffer yet, and its not empty, return the
        // buffered data first.
        if this.replay_body {
            if this.next_buf_idx == state.buf.bufs.len() {
                this.replay_body = false;
            } else {
                let chunk = state.buf.bufs[this.next_buf_idx].clone();
                this.next_buf_idx += 1;

                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }

            if state.is_capped() {
                tracing::warn!(
                    name: "kiso.client.request.body.retry.max_length_reached", 
                    "Cannot replay buffered body, maximum buffer length reached");

                return Poll::Ready(Some(Err(Box::new(Status::aborted(
                    "cannot replay buffered body",
                )))));
            }
        }

        if this.replay_trailers {
            this.replay_trailers = false;
            if let Some(ref trailers) = state.trailers {
                return Poll::Ready(Some(Ok(Frame::trailers(trailers.clone()))));
            }
        }

        // If the inner body has previously ended, don't poll it again.
        //
        // NOTE(Eliza): we would expect the inner body to just happily return
        // `None` multiple times here, but `tonic::transport::Body::channel` (which we
        // use in the tests) will panic if it is polled after returning `None`,
        // so we have to special-case this. :/
        if state.is_completed {
            return Poll::Ready(None);
        }

        // Poll the inner body for more data. If the body has ended, remember
        // that so that future clones will not try polling it again (as
        // described above).
        let mut frame = match futures_util::ready!(unsafe {
            Pin::new_unchecked(&mut state.rest).poll_frame(cx)
        }) {
            Some(Ok(frame)) => frame,
            Some(Err(e)) => return Poll::Ready(Some(Err(Box::new(e)))),
            None => {
                state.is_completed = true;
                return Poll::Ready(None);
            }
        };

        if let Some(data) = frame.data_ref() {
            let length = data.remaining();
            state.max_remaining_bytes = state.max_remaining_bytes.saturating_sub(length);
            let chunk = if state.is_capped() {
                if !state.buf.bufs.is_empty() {
                    tracing::warn!(
                        name: "kiso.client.request.body.retry.max_length_reached", 
                        "Buffered maximum capacity, discarding buffer");
                    state.buf = Default::default();
                }

                data.slice(..length)
            } else {
                state.buf.push_chunk(data.clone())
            };

            frame = frame.map_data(|_| chunk);
        } else if let Some(trailers) = frame.trailers_ref() {
            if state.trailers.is_none() {
                state.trailers = Some(trailers.clone());
            }
        }

        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        let is_inner_eos = self.state.as_ref().map_or(false, |state| {
            state.is_completed || state.rest.is_end_stream()
        });

        // if this body has data or trailers remaining to play back, it
        // is not EOS
        !self.replay_body && !self.replay_trailers
            // if we have replayed everything, the initial body may
            // still have data remaining, so ask it
            && is_inner_eos
    }

    fn size_hint(&self) -> SizeHint {
        // If this clone isn't holding the body, return the original size hint.
        let Some(state) = self.state.as_ref() else {
            return SizeHint::default();
        };

        // Otherwise, add the inner body's size hint to the amount of buffered
        // data. An upper limit is only set if the inner body has an upper
        // limit.
        let buffered = state.buf.bufs.iter().map(Buf::remaining).sum::<usize>() as u64;
        let rest_hint = state.rest.size_hint();
        let mut hint = SizeHint::default();
        hint.set_lower(buffered + rest_hint.lower());
        if let Some(rest_upper) = rest_hint.upper() {
            hint.set_upper(buffered + rest_upper);
        }

        hint
    }
}

/// Body data composed of multiple `Bytes` chunks.
#[derive(Clone, Debug, Default)]
struct BufList {
    bufs: Vec<Bytes>,
}

impl BufList {
    fn push_chunk(&mut self, data: Bytes) -> Bytes {
        // Buffer a clone of the bytes read on this poll.
        self.bufs.push(data.clone());

        // Return the bytes
        data
    }
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt;
    use tonic::body::BoxBody;

    use super::*;

    #[tokio::test]
    async fn replays_one_chunk() {
        let Test {
            mut tx,
            initial,
            replay,
        } = Test::new();
        tx.send_data("hello world").await;
        drop(tx);

        let initial = body_to_string(initial).await;
        assert_eq!(initial, "hello world");

        let replay = body_to_string(replay).await;
        assert_eq!(replay, "hello world");
    }

    #[tokio::test]
    async fn replays_several_chunks() {
        let Test {
            mut tx,
            initial,
            replay,
        } = Test::new();

        tokio::spawn(async move {
            tx.send_data("hello").await;
            tx.send_data(" world").await;
            tx.send_data(", have lots").await;
            tx.send_data(" of fun!").await;
        });

        let initial = body_to_string(initial).await;
        assert_eq!(initial, "hello world, have lots of fun!");

        let replay = body_to_string(replay).await;
        assert_eq!(replay, "hello world, have lots of fun!");
    }

    #[tokio::test]
    async fn replays_trailers() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::new();

        tx.send_data("hello world").await;
        drop(tx);

        while let Some(frame) = initial.frame().await {
            assert!(frame.is_ok(), "frames should not error: {frame:?}");
        }

        // drop the initial body to send the data to the replay
        drop(initial);

        while replay.frame().await.is_some() {
            // do nothing
        }
    }

    #[tokio::test]
    async fn trailers_only() {
        let Test {
            tx,
            mut initial,
            mut replay,
        } = Test::new();

        drop(tx);

        assert!(initial.frame().await.is_none());

        // drop the initial body to send the data to the replay
        drop(initial);

        assert!(replay.frame().await.is_none(), "no data in body");
    }

    #[tokio::test]
    async fn switches_with_body_remaining() {
        // This simulates a case where the server returns an error _before_ the
        // entire body has been read.
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::new();

        tx.send_data("hello").await;
        assert_eq!(chunk(&mut initial).await.unwrap(), "hello");

        tx.send_data(" world").await;
        assert_eq!(chunk(&mut initial).await.unwrap(), " world");

        // drop the initial body to send the data to the replay
        drop(initial);

        tokio::spawn(async move {
            tx.send_data(", have lots of fun").await;
        });

        assert_eq!(
            body_to_string(&mut replay).await,
            "hello world, have lots of fun"
        );
    }

    #[tokio::test]
    async fn multiple_replays() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::new();

        tokio::spawn(async move {
            tx.send_data("hello").await;
            tx.send_data(" world").await;
        });

        assert_eq!(body_to_string(&mut initial).await, "hello world");
        assert!(initial.frame().await.is_none());

        // drop the initial body to send the data to the replay
        drop(initial);

        let mut replay2 = replay.clone();
        assert_eq!(body_to_string(&mut replay).await, "hello world");

        assert!(replay.frame().await.is_none());

        drop(replay);

        assert_eq!(body_to_string(&mut replay2).await, "hello world");

        assert!(replay2.frame().await.is_none());
    }

    #[tokio::test]
    async fn multiple_incomplete_replays() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::new();

        tx.send_data("hello").await;
        assert_eq!(chunk(&mut initial).await.unwrap(), "hello");

        // drop the initial body to send the data to the replay
        drop(initial);

        let mut replay2 = replay.clone();

        tx.send_data(" world").await;
        assert_eq!(chunk(&mut replay).await.unwrap(), "hello");
        assert_eq!(chunk(&mut replay).await.unwrap(), " world");

        // drop the replay body to send the data to the second replay
        drop(replay);

        tokio::spawn(async move {
            tx.send_data(", have lots").await;
            tx.send_data(" of fun!").await;
        });

        assert_eq!(
            body_to_string(&mut replay2).await,
            "hello world, have lots of fun!"
        );

        assert!(replay2.frame().await.is_none());
    }

    #[tokio::test]
    async fn drop_clone_early() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::new();

        tokio::spawn(async move {
            tx.send_data("hello").await;
            tx.send_data(" world").await;
        });

        assert_eq!(body_to_string(&mut initial).await, "hello world");
        assert!(initial.frame().await.is_none());

        // drop the initial body to send the data to the replay
        drop(initial);

        // clone the body again and then drop it
        let replay2 = replay.clone();
        drop(replay2);

        assert_eq!(body_to_string(&mut replay).await, "hello world");
        assert!(replay.frame().await.is_none());
    }

    #[tokio::test]
    async fn eos_only_when_fully_replayed() {
        // Test that each clone of a body is not EOS until the data has been
        // fully replayed.
        let Test {
            mut initial,
            mut replay,
            ..
        } = Test::new();

        body_to_string(&mut initial).await;
        assert!(!replay.is_end_stream());

        assert!(initial.frame().await.is_none());
        assert!(initial.is_end_stream());
        assert!(!replay.is_end_stream());

        // drop the initial body to send the data to the replay
        drop(initial);

        assert!(!replay.is_end_stream());

        body_to_string(&mut replay).await;

        assert!(replay.frame().await.is_none());
        assert!(replay.is_end_stream());

        // Even if we clone a body _after_ it has been driven to EOS, the clone
        // must not be EOS.
        let mut replay2 = replay.clone();
        assert!(!replay2.is_end_stream());

        // drop the initial body to send the data to the replay
        drop(replay);

        body_to_string(&mut replay2).await;

        assert!(replay2.frame().await.is_none());
        assert!(replay2.is_end_stream());
    }

    #[tokio::test]
    async fn caps_buffer() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::with_cap(8);

        // Send enough data to reach the cap
        tx.send_data(Bytes::from("aaaaaaaa")).await;
        assert_eq!(chunk(&mut initial).await, Some("aaaaaaaa".to_string()));

        // Further chunks are still forwarded on the initial body
        tx.send_data(Bytes::from("bbbbbbbb")).await;
        assert_eq!(chunk(&mut initial).await, Some("bbbbbbbb".to_string()));

        drop(initial);

        // The request's replay should error, since we discarded the buffer when
        // we hit the cap.
        let _res = replay
            .frame()
            .await
            .expect("replay must yield Some(Err(..)) when capped")
            .expect_err("replay must error when capped");
    }

    #[tokio::test]
    async fn caps_across_replays() {
        let Test {
            mut tx,
            mut initial,
            mut replay,
        } = Test::with_cap(8);

        // Send enough data to reach the cap
        tx.send_data(Bytes::from("aaaaaaaa")).await;
        assert_eq!(chunk(&mut initial).await, Some("aaaaaaaa".to_string()));
        drop(initial);

        let mut replay2 = replay.clone();

        // The replay will reach the cap, but it should still return data from
        // the original body.
        tx.send_data(Bytes::from("bbbbbbbb")).await;
        assert_eq!(chunk(&mut replay).await, Some("aaaaaaaa".to_string()));
        assert_eq!(chunk(&mut replay).await, Some("bbbbbbbb".to_string()));
        drop(replay);

        // The second replay will fail, though, because the buffer was discarded.
        let _res = replay2
            .frame()
            .await
            .expect("replay must yield Some(Err(..)) when capped")
            .expect_err("replay must error when capped");
    }

    struct Test {
        tx: Tx,
        initial: ReplayBody<BoxBody>,
        replay: ReplayBody<BoxBody>,
    }

    struct Tx(flume::Sender<Result<bytes::Bytes, std::convert::Infallible>>);

    impl Test {
        fn new() -> Self {
            Self::with_cap(64 * 1024)
        }

        fn with_cap(cap: usize) -> Self {
            let (tx, rx) = flume::unbounded();
            let initial = ReplayBody::new(
                axum::body::Body::from_stream(rx.into_stream())
                    .map_err(|err| Status::internal(err.to_string()))
                    .boxed_unsync(),
                cap,
            );
            let replay = initial.clone();
            Self {
                tx: Tx(tx),
                initial,
                replay,
            }
        }
    }

    impl Tx {
        async fn send_data(&mut self, data: impl Into<Bytes> + std::fmt::Debug) {
            let data = data.into();
            self.0
                .send_async(Ok(data))
                .await
                .expect("rx is not dropped");
        }
    }

    async fn chunk<T>(body: &mut T) -> Option<String>
    where
        T: http_body::Body + Unpin,
    {
        body.frame()
            .await
            .map(|res| res.map_err(|_| ()).unwrap())
            .and_then(|f| f.into_data().ok())
            .map(string)
    }

    async fn body_to_string<T>(mut body: T) -> String
    where
        T: http_body::Body + Unpin,
        T::Error: std::fmt::Debug,
    {
        let mut s = String::new();
        while let Some(chunk) = chunk(&mut body).await {
            s.push_str(&chunk[..]);
        }
        s
    }

    fn string(mut data: impl Buf) -> String {
        let mut slice = vec![0; data.remaining()];
        data.copy_to_slice(&mut slice[..]);
        String::from_utf8(slice).unwrap()
    }
}
