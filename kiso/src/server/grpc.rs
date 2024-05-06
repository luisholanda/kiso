use std::{
    convert::Infallible,
    error::Error,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use axum::{body::BoxBody, extract::DefaultBodyLimit, http};
use futures_util::{future::BoxFuture, Future, FutureExt as _, TryFutureExt as _};
use http_body::Body as _;
use hyper::{Body, Request, Response};
use tonic::{body::BoxBody as TonicBoxBody, Code};
use tower::{util::BoxCloneService, Service, ServiceBuilder};

use crate::context::Deadline;

crate::settings! {
    pub(crate) GrpcServiceSettings {
        /// The default deadline for gRPC services.
        ///
        /// Defaults to 30 seconds.
        ///
        /// This can be overwritten per-service using `grpc-service-deadline`.
        #[arg(value_parser = crate::settings::DurationParser)]
        grpc_default_deadline: Duration = Duration::from_secs(30),
        /// The default body limit for gRPC services.
        ///
        /// Defaults to 30Kib.
        ///
        /// This can be overwritten per-service using `grpc-service-body-limit`.
        #[arg(value_parser = crate::settings::SizeParser)]
        grpc_default_body_limit: usize = 30 * 1024,
        /// The default maximum number of inflight requests that each service supports.
        ///
        /// Defaults to no limit.
        ///
        /// This can be overwritten per-service using `grpc-service-concurrency`.
        ///
        /// # Note
        ///
        /// Currently, when this limit is reached, any further requests are cancelled.
        grpc_default_concurrency: u32 = u32::MAX,
        /// The deadline for individual gRPC services.
        ///
        /// This should be `<service name>=<deadline>`.
        #[arg(value_parser = crate::settings::KeyValueParser::<crate::settings::DurationParser>::default())]
        grpc_service_deadline: Vec<(String, Duration)>,
        /// The request body limit for individual gRPC services.
        ///
        /// This should be `<service name>=<limit>`.
        #[arg(long, value_parser = crate::settings::KeyValueParser::<crate::settings::SizeParser>::default())]
        grpc_service_body_limit: Vec<(String, usize)>,
        /// The concurrency limit for in-flight requests for individual gRPC services.
        ///
        /// This should be `<service name>=<concurrency>`.
        #[arg(long, value_parser = crate::settings::KeyValueParser::<clap::builder::RangedI64ValueParser<u32>>::default())]
        grpc_service_concurrency: Vec<(String, u32)>,
    }
}

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
        S: Service<Request<Body>, Response = Response<TonicBoxBody>> + Clone + Send + 'static,
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
            .map_response(|res: Response<TonicBoxBody>| {
                res.map(|b| b.map_err(axum::Error::new).boxed_unsync())
            })
            .load_shed()
            .concurrency_limit(config.request_concurrency_limit as usize)
            .layer(DefaultBodyLimit::max(config.body_limit))
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
