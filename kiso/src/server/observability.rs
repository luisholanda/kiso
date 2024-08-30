use std::{
    future::Future,
    task::{Context, Poll},
};

use axum::extract::{ConnectInfo, MatchedPath};
use futures_util::future::BoxFuture;
use hyper::{
    header::{CONTENT_TYPE, USER_AGENT},
    http::{request::Parts, HeaderName},
    Method, Request, Response,
};
use opentelemetry_semantic_conventions::trace;
use tonic::Code;
use tower::Service;
use tracing::Instrument;

use super::listener::Addrs;
use crate::tracing::RemoteContext;

#[derive(Clone)]
pub(super) struct TracingService<S> {
    inner: S,
}

impl<S> TracingService<S> {
    pub(super) fn new(inner: S) -> Self {
        Self { inner }
    }

    fn http_builder_from_req(&mut self, req: &Parts) -> (tracing::Span, bool) {
        static TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");
        static TRACESTATE: HeaderName = HeaderName::from_static("tracestate");
        const KNOWN_METHODS: &[Method] = &[
            Method::CONNECT,
            Method::DELETE,
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
            Method::PATCH,
            Method::POST,
            Method::PUT,
            Method::TRACE,
        ];

        let remote_ctx = if let Some(h) = req.headers.get(&TRACEPARENT) {
            if let Ok(traceparent) = h.to_str() {
                let tracestate = req.headers.get(&TRACESTATE).and_then(|t| t.to_str().ok());
                RemoteContext::from_w3c_traceparent(traceparent, tracestate.unwrap_or_default())
            } else {
                None
            }
        } else {
            None
        };

        let is_grpc = req
            .headers
            .get(&CONTENT_TYPE)
            .map_or(false, |ce| ce.as_bytes().starts_with(b"application/grpc"));

        let method = if KNOWN_METHODS.contains(&req.method) {
            req.method.as_str()
        } else {
            "_OTHER"
        };

        let route = req
            .extensions
            .get::<MatchedPath>()
            .map_or(req.uri.path(), |m| m.as_str());
        let ConnectInfo(addrs) = req
            .extensions
            .get::<ConnectInfo<Addrs>>()
            .expect("missing ConnectInfo extension");

        let make_span = || {
            tracing::info_span!(
                "kiso.server.request",
                otel.name = tracing::field::Empty,
                otel.span_kind = "server",
                otel.status_code = tracing::field::Empty,
                error = tracing::field::Empty,
                { trace::HTTP_ROUTE } = route,
                { trace::HTTP_REQUEST_METHOD } = method,
                { trace::HTTP_REQUEST_METHOD_ORIGINAL } = tracing::field::Empty,
                { trace::HTTP_RESPONSE_STATUS_CODE } = tracing::field::Empty,
                { trace::NETWORK_TRANSPORT } = "tcp",
                { trace::NETWORK_PROTOCOL_NAME } = "http",
                "network.peer.address" = %addrs.peer.ip(),
                "network.peer.port" = addrs.peer.port(),
                "network.local.address" = %addrs.local.ip(),
                "network.local.port" = addrs.local.port(),
                { trace::URL_PATH } = req.uri.path(),
                { trace::URL_SCHEME } = req.uri.scheme_str().unwrap_or("https"),
                { trace::URL_QUERY } = tracing::field::Empty,
                { trace::USER_AGENT_ORIGINAL } = tracing::field::Empty,
                // gRPC specific fields
                { trace::RPC_SYSTEM } = tracing::field::Empty,
                { trace::RPC_SERVICE } = tracing::field::Empty,
                { trace::RPC_METHOD } = tracing::field::Empty,
                { trace::RPC_GRPC_STATUS_CODE } = tracing::field::Empty,
            )
        };

        let span = remote_ctx.map_or_else(make_span, |c| crate::context::scope_sync(c, make_span));

        if is_grpc {
            let mut splits = req.uri.path().rsplit('/');
            // FIXME: if these return None, the request is wrong.
            let method = splits.next().unwrap_or_default();
            let service = splits.next().unwrap_or_default();

            span.record("otel.name", format_args!("{service}/{method}"));
            span.record(trace::RPC_SERVICE, service);
            span.record(trace::RPC_METHOD, method);
        } else if method == "_OTHER" {
            span.record("otel.name", format_args!("HTTP {route}"));
            span.record(trace::HTTP_REQUEST_METHOD_ORIGINAL, req.method.as_str());
        } else {
            span.record("otel.name", format_args!("{method} {route}"));
        };

        if let Some(q) = req.uri.query() {
            span.record(trace::URL_QUERY, q.to_string());
        }

        if let Some(ua) = req.headers.get(USER_AGENT) {
            if let Ok(ua) = ua.to_str() {
                span.record(trace::USER_AGENT_ORIGINAL, ua.to_string());
            }
        }

        (span, is_grpc)
    }
}

impl<S, B1, B2> Service<Request<B1>> for TracingService<S>
where
    S: Service<Request<B1>, Response = Response<B2>>,
    S::Future: Future<Output = Result<Response<B2>, S::Error>> + Send + 'static,
    S::Error: std::fmt::Display + 'static,
    B2: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B1>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let (span, is_grpc) = self.http_builder_from_req(&parts);

        let fut = span.in_scope(|| self.inner.call(Request::from_parts(parts, body)));

        let span1 = span.clone();
        Box::pin(
            async move {
                let res = fut.await.inspect_err(|err| {
                    span.record("error", tracing::field::display(err));
                })?;

                if is_grpc {
                    if let Some(status) = tonic::Status::from_header_map(res.headers()) {
                        span.record(trace::RPC_GRPC_STATUS_CODE, status.code() as i64);

                        if matches!(
                            status.code(),
                            Code::Unknown
                                | Code::DeadlineExceeded
                                | Code::Unimplemented
                                | Code::Internal
                                | Code::Unavailable
                                | Code::DataLoss
                        ) {
                            span.record("otel.status_code", "error");
                        }
                    }
                } else {
                    span.record(trace::HTTP_RESPONSE_STATUS_CODE, res.status().as_u16());

                    if res.status().is_server_error() {
                        span.record("otel.status_code", "error");
                    }
                }

                Ok(res)
            }
            .instrument(span1),
        )
    }
}
