use std::{
    convert::Infallible,
    future::Ready,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use axum::{body::Body, extract::DefaultBodyLimit, http};
use futures_util::{
    future::{BoxFuture, Either},
    Future, FutureExt as _, TryFutureExt as _,
};
use hyper::{Request, Response};
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
    inner: BoxCloneService<Request<Body>, Response<TonicBoxBody>, Infallible>,
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
        S: Service<Request<Body>, Response = Response<TonicBoxBody>, Error = Infallible>
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        let service = ServiceBuilder::default()
            .boxed_clone()
            .concurrency_limit(config.request_concurrency_limit as usize)
            .map_future(catch_panic)
            .layer(DefaultBodyLimit::max(config.body_limit))
            .service(inner);

        Self {
            inner: service,
            default_deadline: config.default_deadline,
            overloaded: false,
        }
    }

    fn get_request_deadline(&self, req_start: Instant, req: &Request<Body>) -> Deadline {
        let request_timeout = get_grpc_timeout(req).unwrap_or(self.default_deadline);
        Deadline(req_start + request_timeout)
    }
}

impl Service<Request<Body>> for GrpcService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Either<
        BoxFuture<'static, Result<Self::Response, Self::Error>>,
        Ready<Result<Self::Response, Self::Error>>,
    >;

    #[inline(always)]
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.overloaded = self.inner.poll_ready(cx)?.is_pending();
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if self.overloaded {
            return Either::Right(std::future::ready(Ok(error_response(
                Code::ResourceExhausted,
            ))));
        }

        let start = Instant::now();
        let deadline = self.get_request_deadline(start, &req);
        let inner = crate::context::scope_sync(deadline, || self.inner.call(req));

        Either::Left(Box::pin(async move {
            let fut = crate::context::scope(deadline, inner);
            if let Ok(Ok(res)) = tokio::time::timeout_at(deadline.instant().into(), fut).await {
                Ok(res.map(Body::new))
            } else {
                tracing::warn!(name: "kiso.server.grpc.deadline_reached", "gRPC request reached deadline");
                Ok(error_response(Code::DeadlineExceeded))
            }
        }))
    }
}

#[inline(always)]
async fn catch_panic<F, B, E>(fut: F) -> Result<Response<B>, E>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Default,
{
    AssertUnwindSafe(fut)
        .catch_unwind()
        .unwrap_or_else(|_| Ok(error_response(Code::Internal)))
        .await
}

fn get_grpc_timeout(req: &Request<Body>) -> Option<Duration> {
    if let Some(timeout) = req.headers().get("grpc-timeout") {
        if let Ok(timeout) = timeout.to_str() {
            let Ok(value) = timeout[..timeout.len() - 1].parse::<u64>() else {
                return None;
            };

            let dur = match timeout.as_bytes()[timeout.len() - 1] {
                b'n' => Duration::from_nanos(value),
                b'u' => Duration::from_micros(value),
                b'm' => Duration::from_millis(value),
                _ => return None,
            };

            return Some(dur);
        }
    }

    None
}

fn error_response<B: Default>(code: Code) -> Response<B> {
    Response::builder()
        .status(http::StatusCode::OK)
        .header("grpc-status", code as i32)
        .header(axum::http::header::CONTENT_TYPE, "application/grpc")
        .body(B::default())
        .unwrap()
}
