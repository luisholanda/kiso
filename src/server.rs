use std::{
    convert::Infallible,
    net::{IpAddr, SocketAddr},
    os::fd::{AsRawFd, FromRawFd},
    sync::Arc,
    time::Duration,
};

use axum::response::IntoResponse;
use clap::builder::TypedValueParser;
use hyper::{
    header,
    server::conn::{AddrIncoming, AddrStream},
    Body, Request,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore},
};
use tonic::server::NamedService;
use tower::{limit::GlobalConcurrencyLimitLayer, Service};
use tower_http::{
    compression::CompressionLayer,
    decompression::DecompressionLayer,
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
};

/// A gRPC/HTTP server, preconfigured for production workloads.
///
/// Automatically configures the following features:
///
/// - Connection limit and load sheding.
/// - Accept compressed bodies.
/// - Returns compressed bodies.
/// - Health checking service and HTTP route (TODO).
/// - Optimized HTTP/2 and socket configuration.
/// - Graceful shutdown support.
/// - Multiple acceptors for better connection stablishment latency.
/// - Mark certain request and response headers as sensitive.
/// - Observability traces and metrics (TODO).
pub struct Server {
    router: axum::Router,
    settings: ServerSettings,
    grpc_enabled: bool,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            router: axum::Router::new(),
            settings: crate::settings::get(),
            grpc_enabled: false,
        }
    }
}

impl Server {
    /// Add HTTP routes to the server.
    ///
    /// The routes are nested in the given path, i.e. if the router contains
    /// a route `/bar` and `path` is `/foo`, the route will be available in `/foo/bar`.
    pub fn add_routes(&mut self, path: &str, router: axum::Router) -> &mut Self {
        crate::debug!("Mounting HTTP routes under {path}");

        // TODO(http): add HTTP default middleware stack.
        self.router = std::mem::take(&mut self.router).nest(path, router);
        self
    }

    /// Add a gRPC service to the server.
    ///
    /// The service will be available as expected by the gRPC spec.
    ///
    /// Note that there is no need to mount the `gprc.health.v1.Health` service,
    /// as the server will add it automatically.
    pub fn add_grpc_service<S>(&mut self, service: S) -> &mut Self
    where
        S: Clone
            + NamedService
            + Service<Request<Body>, Error = Infallible>
            + Send
            + Sync
            + 'static,
        S::Response: IntoResponse,
        S::Future: Send,
    {
        crate::debug!("Mounting gRPC service {}", S::NAME);

        self.grpc_enabled = true;
        // TODO(grpc): add gRPC default middleware stack.
        self.router = std::mem::take(&mut self.router).route_service(S::NAME, service);
        self
    }

    /// Start the server.
    ///
    /// This will start multiple acceptor tasks (configured by `--server-acceptor-tasks-count`),
    /// listening in the socket configured by `--server-ip` and `--server-port`.
    ///
    /// Returns a [`Shutdown`] that can be used to gracefully shutdown the server.
    pub async fn start(&mut self) -> Shutdown {
        crate::info!("Starting server...");

        if self.grpc_enabled {
            crate::debug!("gRPC enabled, adding gprc.health.v1.Health service for health checking");
            let (_, srv) = tonic_health::server::health_reporter();
            self.add_grpc_service(srv);
        }

        crate::debug!("Server settings: {:?}", self.settings);

        crate::info!(
            "Starting server tasks, {} will be created",
            self.settings.server_acceptor_tasks_count
        );

        let notify = Arc::new(Notify::new());
        let service = self.make_service();

        let mut futs = Vec::with_capacity(self.settings.server_acceptor_tasks_count as _);
        for _ in 0..self.settings.server_acceptor_tasks_count {
            futs.push(async {
                let listener = self.listen().await;
                let addr_incoming =
                    AddrIncoming::from_listener(listener).expect("failed to configure listener");

                let notify = notify.clone();
                let server = hyper::Server::builder(addr_incoming)
                    .http2_only(self.grpc_enabled || self.settings.server_http2_only)
                    .http2_adaptive_window(self.settings.server_http2_adaptive_flow_control)
                    .http2_keep_alive_interval(Some(self.settings.server_http2_keep_alive_interval))
                    .serve(service.clone())
                    .with_graceful_shutdown(async move { notify.notified().await });

                // TODO(observability): add log for error.
                tokio::spawn(server);
            });
        }

        futures_util::future::join_all(futs).await;

        crate::info!(
            "Server started! Listening on {}:{}",
            self.settings.server_ip,
            self.settings.server_port
        );

        Shutdown(notify)
    }

    async fn listen(&self) -> TcpListener {
        crate::debug!(
            "Starting listener in {}:{}",
            self.settings.server_ip,
            self.settings.server_port
        );

        let sock_addr = SocketAddr::new(self.settings.server_ip, self.settings.server_port);

        let listener = match TcpListener::bind(sock_addr).await {
            Ok(l) => l,
            Err(err) => panic!("failed to bind socket {sock_addr}: {err}"),
        };

        if let Err(err) = (|| unsafe {
            let socket = socket2::Socket::from_raw_fd(listener.as_raw_fd());
            socket.set_nodelay(true)?;
            socket.set_reuse_port(true)?;

            Ok(()) as std::io::Result<()>
        })() {
            panic!("failed to configure socket {sock_addr}: {err}");
        };

        listener
    }

    fn make_service(
        &self,
    ) -> tower::load_shed::LoadShed<
        tower::limit::ConcurrencyLimit<
            tower::util::ServiceFn<
                impl FnMut(&AddrStream) -> axum::routing::future::IntoMakeServiceFuture<axum::Router>
                    + Clone,
            >,
        >,
    > {
        let mut req_sensitive_hdrs = vec![
            header::AUTHORIZATION,
            header::COOKIE,
            header::PROXY_AUTHORIZATION,
        ];

        for hdr_name in &self.settings.server_extra_request_sensitive_headers {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                req_sensitive_hdrs.push(hdr);
            }
        }

        let mut res_sensitive_hdrs = vec![header::SET_COOKIE];

        for hdr_name in &self.settings.server_extra_response_sensitive_headers {
            if let Ok(hdr) = header::HeaderName::try_from(hdr_name) {
                res_sensitive_hdrs.push(hdr);
            }
        }

        // Global middleware stack. Runs for both gRPC and HTTP requests.
        let stack = tower::ServiceBuilder::new()
            .layer(SetSensitiveRequestHeadersLayer::new(req_sensitive_hdrs))
            .layer(SetSensitiveResponseHeadersLayer::new(res_sensitive_hdrs))
            .layer(CompressionLayer::new())
            .layer(DecompressionLayer::new())
            .into_inner();

        let mut axum_service = self.router.clone().layer(stack).into_make_service();

        let conn_limit = Arc::new(Semaphore::new(self.settings.server_connection_limit));

        let settings = self.settings.clone();
        tower::ServiceBuilder::new()
            .load_shed()
            .layer(GlobalConcurrencyLimitLayer::with_semaphore(conn_limit))
            .service_fn(move |stream: &AddrStream| {
                crate::debug!(
                    "New connection accepted: {}:{} -> {}:{}",
                    stream.remote_addr().ip(),
                    stream.remote_addr().port(),
                    stream.local_addr().ip(),
                    stream.local_addr().port()
                )
                .attr("peer_ip", stream.remote_addr().ip().to_string())
                .attr("peer_port", stream.remote_addr().port());

                let _res = (|| unsafe {
                    let socket = socket2::Socket::from_raw_fd(stream.as_raw_fd());
                    socket.set_nodelay(true)?;
                    socket.set_thin_linear_timeouts(true)?;
                    socket.set_linger(Some(settings.server_linger_timeout))?;

                    Ok(()) as std::io::Result<()>
                })();

                axum_service.call(stream)
            })
    }
}

pub struct Shutdown(Arc<Notify>);

impl Shutdown {
    pub fn shutdown(self) {
        self.0.notify_waiters();
        crate::info!("Shutdown notified for server tasks");
    }
}

crate::settings! {
    #[derive(Clone, Debug)]
    pub(crate) ServerSettings {
        /// Port that the server should listen in.
        ///
        /// Defaults to 8080.
        server_port: u16 = 8080,
        /// IP that the server should listen in.
        ///
        /// Defaults to 127.0.0.1.
        #[arg(value_parser = clap::builder::StringValueParser::new().try_map(|s| s.parse::<IpAddr>()))]
        server_ip: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST),
        /// The number of connection acceptor tasks to spawn.
        ///
        /// When the server has to accept a high number of connections, increasing this
        /// number can help reduce the latency to stablish them.
        ///
        /// Defaults to 2.
        server_acceptor_tasks_count: u8 = 2,
        /// The limit of connections the server can handle at once.
        ///
        /// Defaults to no limit.
        server_connection_limit: usize = usize::MAX,
        /// Timeout for queued messed to be sent when the server is shutdown.
        ///
        /// Defaults to 5s.
        #[arg(value_parser = crate::settings::DurationParser)]
        server_linger_timeout: Duration = Duration::from_secs(5),
        /// If the server should require HTTP/2 connections.
        ///
        /// Note that gRPC requires HTTP/2, thus, adding any gRPC service to the server
        /// will override this value.
        ///
        /// Defaults to false.
        server_http2_only: bool,
        /// If the server should use HTTP/2 adaptive flow control via BDP.
        ///
        /// Defaults to true.
        server_http2_adaptive_flow_control: bool = true,
        /// The interval of which HTTP/2 Ping frames should be set to keep the connection alive.
        ///
        /// Defaults to 20s.
        #[arg(value_parser = crate::settings::DurationParser)]
        server_http2_keep_alive_interval: Duration = Duration::from_secs(60),
        /// Extra server request headers to mark as sensitive.
        ///
        /// "Authorization", "Proxy-Authorization" and "Cookie" are already marked as sensitive.
        server_extra_request_sensitive_headers: Vec<String>,
        /// Extra server response headers to mark as sensitive.
        ///
        /// "Set-Cookie" is already marked as sensitive.
        server_extra_response_sensitive_headers: Vec<String>,
    }
}
