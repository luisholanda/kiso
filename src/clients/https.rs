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
    body::{BoxBody, HttpBody},
    http::{header, HeaderMap, HeaderValue},
};
use bytes::{Buf, BufMut, BytesMut};
use futures_util::future::BoxFuture;
use hickory_resolver::{error::ResolveError, lookup_ip::LookupIp, TokioAsyncResolver};
use http_body::SizeHint;
use hyper::{
    body::Bytes,
    client::{
        connect::{Connected, Connection},
        ResponseFuture,
    },
    Body, Request, Response, StatusCode, Uri,
};
use once_cell::sync::Lazy;
use opentelemetry::trace::{SpanKind, Status, TraceFlags};
use opentelemetry_semantic_conventions::trace;
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{client::TlsStream, TlsConnector};
use tower::{util::BoxCloneService, Service, ServiceExt};
use tower_http::{
    decompression::{DecompressionBody, DecompressionLayer},
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
};

use crate::{context::Deadline, observability::tracing::Span};

pub type ClientBody = DecompressionBody<TracedBody<Body>>;

/// An HTTPS client.
///
/// This is a wrapper around [`hyper::Client`] that provides basic features like TLS
/// and response body decompression, while also integrating with the Kiso contexts,
/// like deadlines.
#[derive(Clone)]
pub struct Client {
    inner: BoxCloneService<Request<BoxBody>, Response<ClientBody>, hyper::Error>,
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
        let req = request.map(|b| b.map_err(axum::Error::new).boxed_unsync());

        let res = self.inner.clone().oneshot(req).await?;

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
            http_body::Full::new(writer.into_inner().freeze())
                .map_err(|_| unreachable!() as axum::Error)
                .boxed_unsync()
        });

        let res = self.do_json_api_request(req).await?;

        let body = hyper::body::aggregate(res.into_body())
            .await
            .map_err(JsonApiError::ResponseBody)?;

        serde_json::from_reader(body.reader()).map_err(JsonApiError::Deserialization)
    }

    #[inline(never)]
    async fn do_json_api_request(
        &self,
        mut req: Request<BoxBody>,
    ) -> Result<Response<ClientBody>, JsonApiError> {
        if !req.headers().contains_key(header::CONTENT_TYPE) {
            req.headers_mut().append(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }

        let res = self
            .inner
            .clone()
            .oneshot(req)
            .await
            .map_err(JsonApiError::Response)?;

        if res.status().is_client_error() {
            return Err(JsonApiError::Client(res));
        }

        if res.status().is_server_error() {
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

        let client = hyper::Client::builder()
            .pool_max_idle_per_host(settings.https_client_max_idle_connections_per_host)
            .http2_adaptive_window(settings.https_client_http2_use_adaptive_window)
            .http2_keep_alive_interval(settings.https_client_http2_keep_alive_interval)
            .http2_keep_alive_while_idle(settings.https_client_http2_keep_alive_while_idle)
            .build(HttpsConnector::default());

        let inner = tower::ServiceBuilder::new()
            .boxed_clone()
            .layer(settings.take_request_sensitive_headers_layer())
            .layer(settings.take_response_sensitive_headers_layer())
            .layer(DecompressionLayer::new())
            .map_response(|res: Response<Body>| res.map(TracedBody::new))
            .layer_fn(|inner| TracingService { inner })
            .map_future(move |fut: ResponseFuture| async move {
                let instant = if let Some(deadline) = crate::context::try_current::<Deadline>() {
                    deadline.instant()
                } else {
                    Instant::now() + default_timeout
                };

                if let Ok(res) = tokio::time::timeout_at(instant.into(), fut).await {
                    res
                } else {
                    let mut resp = Response::new(Body::empty());
                    *resp.status_mut() = StatusCode::REQUEST_TIMEOUT;
                    Ok(resp)
                }
            })
            .service(client);

        Self { inner }
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
    resolver: Arc<TokioAsyncResolver>,
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

            let mut config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

            Arc::new(config)
        });

        Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()
                .expect("failed to create DNS resolver")
                .into(),
            connector: TlsConnector::from(TLS_CONFIG.clone()),
        }
    }
}

impl HttpsConnector {
    async fn resolve_uri(&self, uri: &Uri) -> Result<LookupIp, ResolveError> {
        let Some(host) = uri.host() else {
            panic!("tried to resolve URI without host: {uri}");
        };

        self.resolver.lookup_ip(host).await
    }

    async fn connect(&self, uri: &Uri) -> Result<TcpStream, ResolveError> {
        let ips = self.resolve_uri(uri).await?;

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
            Ok(TlsConnection { inner: tls_stream })
        })
    }
}

struct TlsConnection {
    inner: TlsStream<TcpStream>,
}

impl Connection for TlsConnection {
    fn connected(&self) -> Connected {
        let connected = Connected::new();

        if self.inner.get_ref().1.alpn_protocol() == Some(b"h2") {
            connected.negotiated_h2()
        } else {
            connected
        }
    }
}

impl AsyncRead for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
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

    fn call(&mut self, mut req: Request<B1>) -> Self::Future {
        let mut builder = Span::builder(req.method().to_string(), SpanKind::Client);

        builder = builder
            .attr(trace::HTTP_REQUEST_METHOD, req.method().to_string())
            .attr(trace::URL_FULL, req.uri().to_string());

        if let Some(h) = req.uri().host() {
            builder = builder.attr(trace::SERVER_ADDRESS, h.to_string());
        }

        'port: {
            let p = if let Some(p) = req.uri().port_u16() {
                p as i64
            } else if req.uri().scheme_str() == Some("https") {
                443
            } else {
                break 'port;
            };

            builder = builder.attr(trace::SERVER_PORT, p);
        }

        for (h, v) in req.headers() {
            if v.is_sensitive() {
                continue;
            }

            if let Ok(v) = v.to_str() {
                builder = builder.attr(format!("http.request.header.{h}"), v.to_string());
            }
        }

        let span = builder.start();

        if let Ok(tracestate) = HeaderValue::from_str(&span.span_context().trace_state().header()) {
            req.headers_mut().insert("tracestate", tracestate);
        }

        let traceparent = format!(
            "00-{}-{}-{:02x}",
            span.span_context().trace_id(),
            span.span_context().span_id(),
            span.span_context().trace_flags() & TraceFlags::SAMPLED
        );

        if let Ok(traceparent) = HeaderValue::from_str(&traceparent) {
            req.headers_mut().insert("traceparent", traceparent);
        }

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

            if res.status().is_client_error() || res.status().is_server_error() {
                // we can't get any meaningful description without reading the body.
                span.set_status(Status::error(""));
            }

            span.set_attribute(
                trace::HTTP_RESPONSE_STATUS_CODE,
                res.status().as_u16() as i64,
            );

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

/// A [`HttpBody`] that adds a span for the duration of a body reading.
pub struct TracedBody<B> {
    body: B,
    span: Span,
    data_frames: i64,
    body_size: i64,
}

impl<B> TracedBody<B> {
    pub fn new(body: B) -> Self {
        Self {
            body,
            span: Span::builder("recv HTTP response body", SpanKind::Client).start(),
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

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { std::task::ready!(Pin::new_unchecked(&mut this.body).poll_data(cx)) } {
            None => Poll::Ready(None),
            Some(Ok(data_frame)) => {
                this.data_frames += 1;
                this.body_size += data_frame.remaining() as i64;

                Poll::Ready(Some(Ok(data_frame)))
            }
            Some(Err(err)) => {
                this.span.set_status(Status::error(err.to_string()));

                Poll::Ready(Some(Err(err)))
            }
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        match unsafe { std::task::ready!(Pin::new_unchecked(&mut this.body).poll_trailers(cx)) } {
            Ok(trailers) => {
                if let Some(t) = &trailers {
                    for (h, v) in t {
                        if v.is_sensitive() {
                            continue;
                        }

                        if let Ok(v) = v.to_str() {
                            this.span
                                .set_attribute(format!("http.response.trailer.{h}"), v.to_string());
                        }
                    }
                }

                this.span
                    .set_attribute(trace::HTTP_RESPONSE_BODY_SIZE, this.body_size);
                this.span
                    .set_attribute("http.response.body.data_frames", this.data_frames);

                Poll::Ready(Ok(trailers))
            }
            Err(err) => {
                this.span.set_status(Status::error(err.to_string()));

                Poll::Ready(Err(err))
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
