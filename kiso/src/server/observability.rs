use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    task::{Context, Poll},
};

use axum::extract::{ConnectInfo, MatchedPath};
use futures_util::future::BoxFuture;
use hyper::{
    header::{CONTENT_ENCODING, USER_AGENT},
    http::HeaderName,
    Method, Request, Response,
};
use opentelemetry::{
    trace::{SpanKind, Status},
    Key,
};
use opentelemetry_semantic_conventions::trace;
use tonic::Code;
use tower::Service;

use super::listener::Addrs;
use crate::observability::tracing::{Span, SpanBuilder};

#[derive(Clone)]
pub(super) struct TracingService<S> {
    inner: S,
    request_headers_key_cache: HashMap<HeaderName, Key>,
}

impl<S> TracingService<S> {
    pub(super) fn new(inner: S) -> Self {
        Self {
            inner,
            request_headers_key_cache: Default::default(),
        }
    }

    fn http_builder_from_req<B>(&mut self, req: &Request<B>) -> (SpanBuilder, bool) {
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

        let is_grpc = req
            .headers()
            .get(&CONTENT_ENCODING)
            .map_or(false, |ce| ce.as_bytes().starts_with(b"application/grpc"));

        let method = if KNOWN_METHODS.contains(req.method()) {
            req.method().as_str()
        } else {
            "_OTHER"
        };
        let route = req
            .extensions()
            .get::<MatchedPath>()
            .map_or(req.uri().path(), |m| m.as_str());
        let ConnectInfo(addrs) = req
            .extensions()
            .get::<ConnectInfo<Addrs>>()
            .expect("missing ConnectInfo extension");

        let mut builder = if is_grpc {
            let mut splits = req.uri().path().rsplit('/');
            // FIXME: if these return None, the request is wrong.
            let method = splits.next().unwrap_or_default();
            let service = splits.next().unwrap_or_default();

            Span::builder(format!("{service}/{method}"), SpanKind::Server)
                .attr(trace::RPC_SYSTEM, "grpc")
                .attr(trace::RPC_SERVICE, service.to_string())
                .attr(trace::RPC_METHOD, method.to_string())
        } else if method == "_OTHER" {
            Span::builder(format!("HTTP {route}"), SpanKind::Server).attr(
                trace::HTTP_REQUEST_METHOD_ORIGINAL,
                req.method().to_string(),
            )
        } else {
            Span::builder(format!("{method} {route}"), SpanKind::Server)
        };

        builder = builder
            .attr(trace::HTTP_ROUTE, route.to_string())
            .attr(trace::HTTP_REQUEST_METHOD, method.to_string())
            .attr(trace::NETWORK_TRANSPORT, "tcp")
            .attr(trace::NETWORK_PROTOCOL_NAME, "http")
            .attr("network.peer.address", addrs.peer.ip().to_string())
            .attr("network.peer.port", addrs.peer.port() as i64)
            .attr("network.local.address", addrs.local.ip().to_string())
            .attr("network.local.port", addrs.local.port() as i64)
            .attr(trace::URL_PATH, req.uri().path().to_string())
            .attr(
                trace::URL_SCHEME,
                req.uri().scheme_str().unwrap_or("https").to_string(),
            );

        if let Some(q) = req.uri().query() {
            builder = builder.attr(trace::URL_QUERY, q.to_string());
        }

        if let Some(ua) = req.headers().get(USER_AGENT) {
            if let Ok(ua) = ua.to_str() {
                builder = builder.attr(trace::USER_AGENT_ORIGINAL, ua.to_string());
            }
        }

        for (h, v) in req.headers() {
            if v.is_sensitive() {
                continue;
            }

            if let Ok(v) = v.to_str() {
                let key = self
                    .request_headers_key_cache
                    .entry(h.clone())
                    .or_insert_with(|| Arc::<str>::from(format!("http.request.header.{h}")).into())
                    .clone();
                builder = builder.attr(key, v.to_string());
            }
        }

        if let Some(h) = req.headers().get(&TRACEPARENT) {
            if let Ok(traceparent) = h.to_str() {
                let tracestate = req.headers().get(&TRACESTATE).and_then(|t| t.to_str().ok());
                builder = match builder.with_w3c_parent(traceparent, tracestate) {
                    Ok(b) | Err(b) => b,
                };
            }
        }

        (builder, is_grpc)
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
        let (builder, is_grpc) = self.http_builder_from_req(&req);
        let span = builder.start();

        let fut = crate::context::scope_sync(span.clone(), || self.inner.call(req));

        Box::pin(crate::context::scope(span, async move {
            let span = crate::context::current::<Span>();

            let res = match fut.await {
                Ok(res) => res,
                Err(err) => {
                    span.set_status(Status::error(err.to_string()));
                    return Err(err);
                }
            };

            if is_grpc {
                if let Some(status) = tonic::Status::from_header_map(res.headers()) {
                    span.set_attribute(trace::RPC_GRPC_STATUS_CODE, status.code() as i64);

                    if matches!(
                        status.code(),
                        Code::Unknown
                            | Code::DeadlineExceeded
                            | Code::Unimplemented
                            | Code::Internal
                            | Code::Unavailable
                            | Code::DataLoss
                    ) {
                        span.set_status(Status::error(""));
                    }
                }
            } else {
                span.set_attribute(
                    trace::HTTP_RESPONSE_STATUS_CODE,
                    res.status().as_u16() as i64,
                );

                if res.status().is_server_error() {
                    span.set_status(Status::error(""));
                }
            }

            for (h, v) in res.headers() {
                if v.is_sensitive() {
                    continue;
                }

                if let Ok(v) = v.to_str() {
                    span.set_attribute(format!("http.response.header.{h}"), v.to_string());
                }
            }

            Ok(res)
        }))
    }
}
