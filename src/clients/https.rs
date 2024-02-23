use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{BoxBody, HttpBody},
    http::header,
};
use futures_util::future::BoxFuture;
use hickory_resolver::{error::ResolveError, lookup_ip::LookupIp, TokioAsyncResolver};
use hyper::{
    body::Bytes,
    client::{
        connect::{Connected, Connection},
        ResponseFuture,
    },
    Body, Request, Response, StatusCode, Uri,
};
use once_cell::sync::Lazy;
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

use crate::context::Deadline;

/// An HTTPS client.
///
/// This is a wrapper around [`hyper::Client`] that provides basic features like TLS
/// and response body decompression, while also integrating with the Kiso contexts,
/// like deadlines.
#[derive(Clone)]
pub struct Client {
    inner: BoxCloneService<Request<BoxBody>, Response<DecompressionBody<Body>>, hyper::Error>,
}

impl Client {
    /// Sends a HTTP request to the network.
    ///
    /// # Errors
    ///
    /// Fails if the internal client fails. No status code processing is made.
    pub async fn request<B>(&self, request: Request<B>) -> Result<Response<BoxBody>, hyper::Error>
    where
        B: HttpBody<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let req = request.map(|b| b.map_err(axum::Error::new).boxed_unsync());

        let mut service = self.inner.clone();
        service.ready().await?;

        let res = service
            .call(req)
            .await?
            .map(|b| b.map_err(axum::Error::new).boxed_unsync());

        Ok(res)
    }
}

impl Default for Client {
    fn default() -> Self {
        let settings: HttpsClientSettings = crate::settings::get();
        let default_timeout = settings.https_client_default_timeout;

        let client = hyper::Client::builder()
            .pool_max_idle_per_host(settings.https_client_max_idle_connections_per_host)
            .http2_adaptive_window(settings.https_client_http2_use_adaptive_window)
            .http2_keep_alive_interval(settings.https_client_http2_keep_alive_interval)
            .http2_keep_alive_while_idle(settings.https_client_http2_keep_alive_while_idle)
            .build(HttpsConnector::default());

        let mut req_sensitive_hdrs = vec![
            header::AUTHORIZATION,
            header::COOKIE,
            header::PROXY_AUTHORIZATION,
        ];

        for hdr_name in settings.https_client_extra_request_sensitive_headers {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                req_sensitive_hdrs.push(hdr);
            }
        }

        let mut res_sensitive_hdrs = vec![header::SET_COOKIE];

        for hdr_name in settings.https_client_extra_response_sensitive_headers {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                res_sensitive_hdrs.push(hdr);
            }
        }

        let inner = tower::ServiceBuilder::new()
            .boxed_clone()
            .layer(SetSensitiveRequestHeadersLayer::new(req_sensitive_hdrs))
            .layer(SetSensitiveResponseHeadersLayer::new(res_sensitive_hdrs))
            .layer(DecompressionLayer::new())
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
            .service(client.clone());

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
        /// Extra request headers to mark as sensitive.
        ///
        /// "Authorization", "Proxy-Authorization" and "Cookie" are already marked as sensitive.
        https_client_extra_request_sensitive_headers: Vec<String>,
        /// Extra response headers to mark as sensitive.
        ///
        /// "Set-Cookie" is already marked as sensitive.
        https_client_extra_response_sensitive_headers: Vec<String>,
    }
);

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

impl tower::Service<Uri> for HttpsConnector {
    type Response = TlsConnection;
    type Error = ResolveError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let connector = self.clone();

        Box::pin(async move {
            let tls_stream = connector.secure_connect(req).await?;
            Ok(TlsConnection { inner: tls_stream })
        })
    }
}

pub struct TlsConnection {
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
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::pin!(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
