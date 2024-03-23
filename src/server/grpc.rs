use std::{
    convert::Infallible,
    error::Error,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use axum::{body::BoxBody, extract::DefaultBodyLimit, http};
use futures_util::{future::BoxFuture, Future, FutureExt as _, TryFutureExt as _};
use http_body::{combinators::UnsyncBoxBody, Body as _};
use hyper::{body::Bytes, Body, Request, Response};
use tonic::{body::BoxBody as TonicBoxBody, Code};
use tower::{util::BoxCloneService, Service, ServiceBuilder};
use tower_http::decompression::{DecompressionBody, RequestDecompressionLayer};

use crate::context::Deadline;

#[derive(Clone)]
pub(super) struct GrpcService {
    inner: BoxCloneService<Request<Body>, Response<BoxBody>, Box<dyn Error + Send + Sync>>,
    default_deadline: Duration,
    overloaded: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ServiceConfiguration {
    pub(super) default_deadline: Duration,
    pub(super) body_limit: usize,
    pub(super) request_concurrency_limit: u32,
}

impl GrpcService {
    pub(super) fn new<S>(inner: S, config: ServiceConfiguration) -> Self
    where
        S: Service<Request<DecompressionBody<Body>>, Response = Response<TonicBoxBody>>
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Error + Send + Sync + 'static,
    {
        let service = ServiceBuilder::default()
            .boxed_clone()
            .map_result(|res| match res {
                Ok(res) => Ok(res),
                Err(err) => {
                    crate::warn!("Error inside gRPC service stack: {err}");
                    Ok(error_response(Code::Internal))
                }
            })
            .map_future(catch_panic)
            .map_response(
                |res: Response<UnsyncBoxBody<Bytes, Box<dyn Error + Send + Sync>>>| {
                    res.map(|b| b.map_err(axum::Error::new).boxed_unsync())
                },
            )
            .load_shed()
            .concurrency_limit(config.request_concurrency_limit as usize)
            .layer(DefaultBodyLimit::max(config.body_limit))
            .layer(RequestDecompressionLayer::new())
            .service(inner);

        Self {
            inner: service,
            default_deadline: config.default_deadline,
            overloaded: false,
        }
    }

    fn get_and_set_request_deadline(&self, req_start: Instant, req: &Request<Body>) -> Deadline {
        let request_timeout = get_grpc_timeout(req).unwrap_or(self.default_deadline);
        Deadline(req_start + request_timeout)
    }
}

impl Service<Request<Body>> for GrpcService {
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    #[inline(always)]
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.overloaded = std::task::ready!(self.inner.poll_ready(cx)).is_err();

        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if std::mem::take(&mut self.overloaded) {
            return Box::pin(std::future::ready(Ok(error_response(Code::Cancelled))));
        }

        let start = Instant::now();
        let deadline = self.get_and_set_request_deadline(start, &req);
        let inner = crate::context::scope_sync(deadline, || self.inner.call(req));

        let base_fut = async move {
            let res = inner.await.unwrap_or_else(|err| {
                crate::warn!("error inside gRPC service stack: {err}");
                error_response(Code::Internal)
            });

            Ok(res)
        };
        let full_fut = async move {
            if let Ok(res) = tokio::time::timeout_at(deadline.instant().into(), base_fut).await {
                res
            } else {
                crate::warn!("gRPC request reached deadline");
                Ok(error_response(Code::DeadlineExceeded))
            }
        };

        Box::pin(crate::context::scope(deadline, full_fut))
    }
}

#[inline(always)]
async fn catch_panic<F, E>(fut: F) -> Result<Response<BoxBody>, E>
where
    F: Future<Output = Result<Response<BoxBody>, E>>,
{
    AssertUnwindSafe(fut)
        .catch_unwind()
        .unwrap_or_else(|_| Ok(error_response(Code::Internal)))
        .await
}

fn get_grpc_timeout(req: &Request<Body>) -> Option<Duration> {
    if let Some(timeout) = req.headers().get("grpc-timeout") {
        if let Ok(timeout) = timeout.to_str() {
            let builder = match timeout.as_bytes()[timeout.len() - 1] {
                b'n' => Duration::from_nanos,
                b'u' => Duration::from_micros,
                b'm' => Duration::from_millis,
                _ => return None,
            };

            let Ok(value) = timeout[..timeout.len() - 1].parse::<u64>() else {
                return None;
            };

            return Some(builder(value));
        }
    }

    None
}

fn error_response(code: Code) -> Response<BoxBody> {
    Response::builder()
        .status(http::StatusCode::OK)
        .header("grpc-status", code as i32)
        .header(axum::http::header::CONTENT_TYPE, "application/grpc")
        .body(
            hyper::Body::empty()
                .map_err(axum::Error::new)
                .boxed_unsync(),
        )
        .unwrap()
}
