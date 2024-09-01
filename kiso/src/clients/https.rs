use std::{
    error::Error,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, HttpBody},
    http::{header, request::Parts, HeaderValue},
};
use bytes::{Buf, BufMut, BytesMut};
use futures_util::future::BoxFuture;
use hickory_resolver::error::ResolveError;
use http_body::SizeHint;
use http_body_util::BodyExt;
use hyper::{body::Bytes, Request, Response, StatusCode, Uri};
use hyper_util::{
    client::legacy::{
        connect::{Connected, Connection},
        Client as HyperClient, ResponseFuture,
    },
    rt::{TokioIo, TokioTimer},
};
use once_cell::sync::Lazy;
use opentelemetry_semantic_conventions::{attribute, trace};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use tower::{buffer::Buffer, util::BoxService, Service, ServiceExt};
use tower_http::{
    decompression::{DecompressionBody, DecompressionLayer},
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
};
use tracing::Instrument;

use crate::{context::Deadline, tracing::TraceContext};

pub type ClientBody = DecompressionBody<TracedBody<Body>>;

/// An HTTPS client.
///
/// This is a wrapper around [`hyper::Client`] that provides basic features like TLS
/// and response body decompression, while also integrating with the Kiso contexts,
/// like deadlines.
#[derive(Clone)]
pub struct Client {
    inner: Buffer<
        BoxService<Request<Body>, Response<ClientBody>, hyper_util::client::legacy::Error>,
        Request<Body>,
    >,
}

impl Client {
    /// Sends an HTTP request to the network.
    ///
    /// # Errors
    ///
    /// Fails if the internal client fails. No status code processing is made.
    pub async fn request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<ClientBody>, hyper::Error>
    where
        B: HttpBody<Data = Bytes> + Send + 'static,
        B::Error: Error + Send + Sync + 'static,
    {
        let req = request.map(|b| Body::new(b.map_err(axum::Error::new)));

        let res = match self.inner.clone().oneshot(req).await {
            Ok(res) => res,
            Err(err) => match err.downcast() {
                Ok(err) => return Err(*err),
                Err(err) => unreachable!("client closed unexpectedly: {err:?}"),
            },
        };

        Ok(res)
    }

    /// Helper for calling JSON APIs.
    ///
    /// Automatically serializes the request's body and deserialize the response's.
    ///
    /// # Errors
    ///
    /// This method may fail for a multitude of reasons, see [`JsonApiError`] for more info.
    pub async fn json_request<T, U>(&self, req: Request<T>) -> Result<U, JsonApiError>
    where
        T: serde::Serialize,
        U: for<'de> serde::Deserialize<'de>,
    {
        let buffer = BytesMut::with_capacity(1024);
        let mut writer = buffer.writer();

        serde_json::to_writer(&mut writer, req.body()).map_err(JsonApiError::Serialization)?;

        let req = req.map(|_| {
            #[allow(unreachable_code)]
            let full = http_body_util::Full::new(writer.into_inner().freeze())
                .map_err(|_| unreachable!() as axum::Error);
            Body::new(full)
        });

        let res = self.do_json_api_request(req).await?;

        let body = res
            .into_body()
            .collect()
            .await
            .map_err(JsonApiError::ResponseBody)?;

        serde_json::from_reader(body.aggregate().reader()).map_err(JsonApiError::Deserialization)
    }

    #[inline(never)]
    async fn do_json_api_request(
        &self,
        mut req: Request<Body>,
    ) -> Result<Response<ClientBody>, JsonApiError> {
        if !req.headers().contains_key(header::CONTENT_TYPE) {
            req.headers_mut().append(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }

        let res = self.request(req).await.map_err(JsonApiError::Response)?;

        if res.status().is_client_error() {
            Err(JsonApiError::Client(res))
        } else if res.status().is_server_error() {
            Err(JsonApiError::Server(res))
        } else {
            Ok(res)
        }
    }
}

/// Errors that may happen with a call to [`Client::json_api_request`].
pub enum JsonApiError {
    /// The response failed with a status code of class 4XX.
    Client(Response<ClientBody>),
    /// The response failed with a status code of class 5XX.
    Server(Response<ClientBody>),
    /// The request failed to be sent.
    Response(hyper::Error),
    /// The request body failed to be serialized.
    Serialization(serde_json::Error),
    /// The response body failed to be deserialized.
    Deserialization(serde_json::Error),
    /// Some error ocurred while reading the response data.
    ResponseBody(Box<dyn Error + Send + Sync>),
}

impl Default for Client {
    fn default() -> Self {
        Self::new(crate::settings::get())
    }
}

impl Client {
    /// Crete a new HTTPS client, using the provided settings.
    ///
    /// This is most useful when there is the need to change in specific ways the
    /// configuration provided via command line, e.g. a specific host that doesn't
    /// support HTTP/2, but the all others do.
    ///
    /// If this isn't needed, prever to use the default instance.
    pub fn new(mut settings: HttpsClientSettings) -> Self {
        let default_timeout = settings.https_client_default_timeout;

        let client = HyperClient::builder(hyper_util::rt::TokioExecutor::new())
            .pool_max_idle_per_host(settings.https_client_max_idle_connections_per_host)
            .http2_adaptive_window(settings.https_client_http2_use_adaptive_window)
            .http2_keep_alive_interval(settings.https_client_http2_keep_alive_interval)
            .http2_keep_alive_while_idle(settings.https_client_http2_keep_alive_while_idle)
            .timer(TokioTimer::new())
            .pool_timer(TokioTimer::new())
            .build(HttpsConnector::default());

        let inner = tower::ServiceBuilder::new()
            .boxed()
            .layer(settings.take_request_sensitive_headers_layer())
            .layer(settings.take_response_sensitive_headers_layer())
            .layer(DecompressionLayer::new())
            .map_response(|res: Response<Body>| res.map(TracedBody::new))
            .layer_fn(|inner| super::retry::RetryService { inner })
            .layer_fn(|inner| TracingService { inner })
            .map_future(move |fut: ResponseFuture| {
                let deadline = crate::context::try_current::<Deadline>();
                async move {
                    let instant = if let Some(deadline) = deadline {
                        deadline.instant()
                    } else {
                        Instant::now() + default_timeout
                    };

                    if let Ok(res) = tokio::time::timeout_at(instant.into(), fut).await {
                        res.map(|res| res.map(Body::new))
                    } else {
                        let mut resp = Response::new(Body::empty());
                        *resp.status_mut() = StatusCode::REQUEST_TIMEOUT;
                        Ok(resp)
                    }
                }
            })
            .service(client);

        Self {
            inner: Buffer::new(inner, settings.https_client_requests_buffer_size),
        }
    }
}

crate::settings!(
    /// Settings controlling the behavior of [`Client`].
    pub HttpsClientSettings {
        /// Max number of idle connections in the pool, per host.
        ///
        /// Defaults to no limit.
        https_client_max_idle_connections_per_host: usize = usize::MAX,
        /// Enable use of HTTP/2 adaptive flow control via BDP.
        https_client_http2_use_adaptive_window: bool = true,
        /// Whether HTTP/2 Ping frames should be sent while idle.
        ///
        /// This allows the connection to be kept alive without active streams.
        https_client_http2_keep_alive_while_idle: bool = true,
        /// Interval for sending HTTP/2 Ping frames.
        #[arg(value_parser = crate::settings::DurationParser)]
        https_client_http2_keep_alive_interval: Duration = Duration::from_secs(30),
        /// Request timeout used when the context has no deadline set.
        ///
        /// Defaults to 10s.
        #[arg(value_parser = crate::settings::DurationParser)]
        https_client_default_timeout: Duration = Duration::from_secs(10),
        /// Extra client request headers to mark as sensitive.
        ///
        /// "Authorization", "Proxy-Authorization" and "Cookie" are already marked as sensitive.
        https_client_extra_request_sensitive_headers: Vec<String>,
        /// Extra client response headers to mark as sensitive.
        ///
        /// "Set-Cookie" is already marked as sensitive.
        https_client_extra_response_sensitive_headers: Vec<String>,
        /// The size of the requests buffer.
        ///
        /// This should be set to at least the maximum number of concurrent requests
        /// the client will see.
        https_client_requests_buffer_size: usize = 1024,
    }
);

impl HttpsClientSettings {
    pub(crate) fn take_request_sensitive_headers_layer(
        &mut self,
    ) -> SetSensitiveRequestHeadersLayer {
        let mut req_sensitive_hdrs = vec![
            header::AUTHORIZATION,
            header::COOKIE,
            header::PROXY_AUTHORIZATION,
        ];

        for hdr_name in self.https_client_extra_request_sensitive_headers.drain(..) {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                req_sensitive_hdrs.push(hdr);
            }
        }

        SetSensitiveRequestHeadersLayer::new(req_sensitive_hdrs)
    }

    pub(crate) fn take_response_sensitive_headers_layer(
        &mut self,
    ) -> SetSensitiveResponseHeadersLayer {
        let mut res_sensitive_hdrs = vec![header::SET_COOKIE];

        for hdr_name in self.https_client_extra_response_sensitive_headers.drain(..) {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                res_sensitive_hdrs.push(hdr);
            }
        }

        SetSensitiveResponseHeadersLayer::new(res_sensitive_hdrs)
    }
}

#[derive(Clone)]
struct HttpsConnector {
    connector: TlsConnector,
}

impl Default for HttpsConnector {
    fn default() -> Self {
        static TLS_CONFIG: Lazy<Arc<ClientConfig>> = Lazy::new(|| {
            let mut root_store = RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs()
                .expect("failed to load native certificates")
            {
                let _res = root_store.add(cert);
            }

            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let mut config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            // FIXME: Make this runtime configurable.
            config.resumption = rustls::client::Resumption::in_memory_sessions(512);

            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

            Arc::new(config)
        });

        Self {
            connector: TlsConnector::from(TLS_CONFIG.clone()),
        }
    }
}

impl HttpsConnector {
    async fn connect(&self, uri: &Uri) -> Result<TcpStream, ResolveError> {
        let ips = super::resolver::resolve_uri(uri).await?;

        let port = uri.port_u16().unwrap_or(443);

        for ip in ips {
            let sock_addr = SocketAddr::from((ip, port));
            let Ok(stream) = TcpStream::connect(sock_addr).await else {
                continue;
            };

            stream.set_nodelay(true)?;
            return Ok(stream);
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "could not connect to any resolved IP",
        )
        .into())
    }

    async fn secure_connect(&self, uri: Uri) -> Result<TlsStream<TcpStream>, ResolveError> {
        let hostname = ServerName::try_from(uri.host().unwrap_or_default())
            .map_err(|_| std::io::Error::other("invalid dnsname"))?;

        let stream = self.connect(&uri).await?;

        Ok(self.connector.connect(hostname.to_owned(), stream).await?)
    }
}

impl Service<Uri> for HttpsConnector {
    type Response = TlsConnection;
    type Error = ResolveError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let connector = self.clone();

        Box::pin(async move {
            let tls_stream = connector.secure_connect(req).await?;
            Ok(TlsConnection {
                inner: TokioIo::new(tls_stream),
            })
        })
    }
}

struct TlsConnection {
    inner: TokioIo<TlsStream<TcpStream>>,
}

impl Connection for TlsConnection {
    fn connected(&self) -> Connected {
        let connected = Connected::new();

        if self.inner.inner().get_ref().1.alpn_protocol() == Some(b"h2") {
            connected.negotiated_h2()
        } else {
            connected
        }
    }
}

impl hyper::rt::Read for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for TlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[derive(Clone)]
pub(super) struct TracingService<S> {
    pub(super) inner: S,
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
        #[inline(never)]
        fn build_span(parts: &mut Parts) -> tracing::Span {
            let span = tracing::info_span!("http.client.request",
                otel.name = %parts.method,
                otel.span_kind = "client",
                // assume request will be successful, we can fix it later.
                otel.status_code = "ok",
                error = tracing::field::Empty,
                {trace::HTTP_REQUEST_METHOD} = %parts.method,
                {trace::URL_FULL} = %parts.uri,
                {trace::SERVER_ADDRESS} = tracing::field::Empty,
                {trace::SERVER_PORT} = tracing::field::Empty,
                {trace::HTTP_RESPONSE_STATUS_CODE} = tracing::field::Empty,

                // gRPC specific.
                {trace::RPC_SYSTEM} = tracing::field::Empty,
                {trace::RPC_SERVICE} = tracing::field::Empty,
                {trace::RPC_METHOD} = tracing::field::Empty,
                {trace::RPC_GRPC_STATUS_CODE} = tracing::field::Empty,
            );

            if span.is_disabled() {
                return span;
            }

            let fields = RequestSpanFields::instance(&span);

            if let Some(h) = parts.uri.host() {
                span.record(&fields.server_address, h.to_string());
            }

            'port: {
                let p = if let Some(p) = parts.uri.port_u16() {
                    p as i64
                } else if parts.uri.scheme_str() == Some("https") {
                    443
                } else {
                    break 'port;
                };

                span.record(&fields.server_port, p);
            }

            TraceContext::with_span(&span, |trace_ctx| {
                if let Ok(tracestate) =
                    HeaderValue::from_str(&trace_ctx.span_context().trace_state().header())
                {
                    parts.headers.insert("tracestate", tracestate);
                }

                let traceparent = trace_ctx.to_w3c_traceparent();

                if let Ok(traceparent) = HeaderValue::from_str(&traceparent) {
                    parts.headers.insert("traceparent", traceparent);
                }
            });

            span
        }

        #[inline(never)]
        fn after_response(span: tracing::Span, status: StatusCode) {
            let fields = RequestSpanFields::instance(&span);
            if status.is_client_error() || status.is_server_error() {
                // we can't get any meaningful description without reading the body.
                span.record(&fields.otel_status_code, "error");
            }

            span.record(&fields.http_response_status_code, status.as_u16());
        }

        let (mut parts, body) = req.into_parts();

        let span = build_span(&mut parts);

        let fut = span.in_scope(|| self.inner.call(Request::from_parts(parts, body)));

        if span.is_disabled() {
            return Box::pin(fut.instrument(span));
        }

        let span1 = span.clone();
        Box::pin(
            async move {
                let res = match fut.await {
                    Ok(res) => res,
                    Err(err) => {
                        let fields = RequestSpanFields::instance(&span);
                        span.record(&fields.error, tracing::field::display(&err));
                        return Err(err);
                    }
                };

                after_response(span, res.status());

                Ok(res)
            }
            .instrument(span1),
        )
    }
}

crate::declare_span_fields_struct!(pub(super) RequestSpanFields {
    otel_name: "otel.name",
    otel_status_code: "otel.status_code",
    error: "error",
    server_address: trace::SERVER_ADDRESS,
    server_port: trace::SERVER_PORT,
    http_response_status_code: trace::HTTP_RESPONSE_STATUS_CODE,
    rpc_system: trace::RPC_SYSTEM,
    rpc_service: trace::RPC_SERVICE,
    rpc_method: trace::RPC_METHOD,
    rpc_grpc_status_code: trace::RPC_GRPC_STATUS_CODE,
});

/// A [`HttpBody`] that adds a span for the duration of a body reading.
pub struct TracedBody<B> {
    body: B,
    span: tracing::Span,
    data_frames: i64,
    body_size: i64,
}

impl<B> TracedBody<B> {
    pub fn new(body: B) -> Self {
        Self {
            body,
            span: tracing::debug_span!(
                "kiso.client.http.response.body",
                otel.span_kind = "client",
                otel.status_code = tracing::field::Empty,
                error = tracing::field::Empty,
                { attribute::HTTP_RESPONSE_BODY_SIZE } = tracing::field::Empty,
                http.response.body.data_frames = tracing::field::Empty,
            ),
            data_frames: 0,
            body_size: 0,
        }
    }
}

impl<B: HttpBody> HttpBody for TracedBody<B>
where
    B::Error: std::fmt::Display,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = unsafe { self.get_unchecked_mut() };
        let inner_frame =
            unsafe { std::task::ready!(Pin::new_unchecked(&mut this.body).poll_frame(cx)) };
        match inner_frame {
            None => Poll::Ready(None),
            Some(Err(err)) => {
                this.span.record("error", tracing::field::display(&err));

                Poll::Ready(Some(Err(err)))
            }
            Some(Ok(data_frame)) if data_frame.is_data() => {
                this.data_frames += 1;
                if let Some(data) = data_frame.data_ref() {
                    this.body_size += data.remaining() as i64;
                }

                Poll::Ready(Some(Ok(data_frame)))
            }
            Some(Ok(trailer_frame)) => {
                this.span
                    .record(attribute::HTTP_RESPONSE_BODY_SIZE, this.body_size);
                this.span
                    .record("http.response.body.data_frames", this.data_frames);

                Poll::Ready(Some(Ok(trailer_frame)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request() {
        let client = Client::default();

        let res = client
            .request(
                Request::get("https://one.one.one.one")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("failed request");

        assert_eq!(res.status(), StatusCode::OK);
    }
}
