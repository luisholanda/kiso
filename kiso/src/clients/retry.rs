use std::{error::Error, time::Duration};

use backoff::backoff::Backoff;
use bytes::Bytes;
use futures_util::future::{BoxFuture, Either};
use http_body::combinators::UnsyncBoxBody;
use hyper::{body::HttpBody, http};
use tower::ServiceExt;

use self::body::ReplayBody;

mod body;

/// Settings for RPC retries.
#[derive(Clone, Copy)]
pub struct RetrySettings {
    /// Initial interval to wait before retrying.
    pub initial_retry_interval: Duration,
    /// Multiplier for the retry interval time.
    pub backoff_multiplier: f64,
    /// Max number of attepts to try.
    pub max_attempts: u8,
    /// Maximum size of body that can be retried.
    ///
    /// This prevents retries using too much memory and causing a self-DoS.
    pub max_retry_body_size: usize,
    /// Maximum duration of a back off interval.
    pub max_interval: Duration,
    /// Maximum time that can be used for the entire process.
    pub max_elapsed_time: Duration,
    /// gRPC codes that should be considered transient and be retried.
    pub retryable_grpc_codes: &'static [tonic::Code],
    /// HTTP status codes that should be considered transient and be retried.
    pub retryable_http_statuses: &'static [http::StatusCode],
    #[doc(hidden)]
    pub __non_exhaustive_but_allow_record_syntax: (),
}

impl Default for RetrySettings {
    fn default() -> Self {
        use backoff::default::*;
        Self {
            initial_retry_interval: Duration::from_millis(INITIAL_INTERVAL_MILLIS),
            backoff_multiplier: MULTIPLIER,
            max_attempts: u8::MAX,
            max_retry_body_size: usize::MAX,
            max_interval: Duration::from_millis(MAX_INTERVAL_MILLIS),
            max_elapsed_time: Duration::from_millis(MAX_ELAPSED_TIME_MILLIS),
            retryable_grpc_codes: &[],
            retryable_http_statuses: &[],
            __non_exhaustive_but_allow_record_syntax: (),
        }
    }
}

#[derive(Clone)]
pub(super) struct RetryService<S> {
    pub(super) inner: S,
}

pub(super) type UnsyncBoxBodyBoxError =
    UnsyncBoxBody<Bytes, Box<dyn Error + Send + Sync + 'static>>;

impl<S, B, ResBody> tower::Service<http::Request<B>> for RetryService<S>
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Error + Send + Sync + 'static,
    S: tower::Service<http::Request<UnsyncBoxBodyBoxError>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    ResBody: Send + 'static,
    S::Error: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<S::Future, BoxFuture<'static, Result<Self::Response, Self::Error>>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.extensions().get::<RetrySettings>().copied() {
            None => Either::Left(self.inner.call(req.map(|b| {
                UnsyncBoxBody::new(b.map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>))
            }))),
            Some(retry_settings) => {
                let (parts, body) = req.into_parts();
                let mut service = self.inner.clone();
                let body = ReplayBody::new(body, retry_settings.max_retry_body_size);

                Either::Right(Box::pin(async move {
                    let mut attempts = 0;
                    let mut backoff = backoff::ExponentialBackoffBuilder::new()
                        .with_initial_interval(retry_settings.initial_retry_interval)
                        .with_multiplier(retry_settings.backoff_multiplier)
                        .with_max_interval(retry_settings.max_interval)
                        .with_max_elapsed_time(Some(retry_settings.max_elapsed_time))
                        .build();

                    loop {
                        service.ready().await?;

                        let req = recreate_request(body.clone(), &parts);
                        let f: BoxFuture<'static, _> = Box::pin(service.call(req));
                        let res: http::Response<ResBody> = f.await?;

                        if attempts < retry_settings.max_attempts {
                            let mut should_retry = body.is_capped();

                            if retry_settings
                                .retryable_http_statuses
                                .contains(&res.status())
                            {
                                should_retry &= true;
                            } else if let Some(status) =
                                tonic::Status::from_header_map(res.headers())
                            {
                                if retry_settings.retryable_grpc_codes.contains(&status.code()) {
                                    should_retry &= true;
                                }
                            }

                            if should_retry {
                                if let Some(backoff_period) = backoff.next_backoff() {
                                    attempts += 1;
                                    tokio::time::sleep(backoff_period).await;
                                    continue;
                                }
                            }
                        }

                        return Ok(res);
                    }
                }))
            }
        }
    }
}

fn recreate_request<B>(
    body: ReplayBody<B>,
    parts: &http::request::Parts,
) -> http::Request<UnsyncBoxBodyBoxError>
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Error + Send + Sync + 'static,
{
    let mut req = http::Request::new(UnsyncBoxBodyBoxError::new(body));
    req.method_mut().clone_from(&parts.method);
    req.uri_mut().clone_from(&parts.uri);
    req.headers_mut().clone_from(&parts.headers);
    *req.version_mut() = parts.version;

    req
}
