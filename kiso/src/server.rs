use std::{
    convert::Infallible,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::response::IntoResponse;
use clap::builder::TypedValueParser;
use http_body::combinators::UnsyncBoxBody;
use hyper::{
    body::Bytes,
    header,
    server::conn::{AddrIncoming, AddrStream},
    Body, Request, Response, StatusCode,
};
use tokio::{
    net::TcpListener,
    signal::unix::SignalKind,
    sync::{Notify, Semaphore},
    task::JoinHandle,
};
use tonic::server::NamedService;
use tower::{limit::GlobalConcurrencyLimitLayer, Service};
use tower_http::{
    compression::CompressionLayer,
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
};

use self::{grpc::ServiceConfiguration, observability::Addrs};
use crate::server::grpc::GrpcService;
pub(crate) use crate::server::grpc::GrpcServiceSettings;

mod grpc;
mod observability;

/// A gRPC/HTTP server, preconfigured for production workloads.
///
/// Automatically configures the following features:
///
/// - Connection limit and load sheding.
/// - Accept compressed bodies for gRPC.
/// - Returns compressed bodies (TODO for gRPC).
/// - Health checking service and HTTP route.
/// - Optimized HTTP/2 and socket configuration.
/// - Graceful shutdown support.
/// - Multiple acceptors for better connection establishment latency.
/// - Mark certain request and response headers as sensitive.
/// - Observability traces and metrics (TODO for metris).
pub struct Server {
    router: axum::Router,
    settings: ServerSettings,
    grpc_settings: GrpcServiceSettings,
    grpc_enabled: bool,
    grpc_services: Vec<&'static str>,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            router: axum::Router::new(),
            settings: crate::settings::get(),
            grpc_settings: crate::settings::get(),
            grpc_enabled: false,
            grpc_services: vec!["grpc.health.v1.Health"],
        }
    }
}

impl Server {
    /// Add HTTP routes to the server.
    ///
    /// The routes are nested in the given path, i.e. if the router contains
    /// a route `/bar` and `path` is `/foo`, the route will be available in `/foo/bar`.
    pub fn add_routes(mut self, path: &str, router: axum::Router) -> Self {
        crate::debug!("Mounting HTTP routes under {path}");

        let layer = tower::ServiceBuilder::new().layer(CompressionLayer::new());

        self.router = std::mem::take(&mut self.router).nest(path, router.route_layer(layer));
        self
    }

    /// Add a gRPC service to the server.
    ///
    /// The service will be available as expected by the gRPC spec.
    ///
    /// Note that there is no need to mount the `gprc.health.v1.Health` service,
    /// as the server will add it automatically.
    ///
    /// Also, remember to properly configure compression and message sizes, as
    /// there isn't a global way of doing this.
    pub fn add_grpc_service<S>(mut self, service: S) -> Self
    where
        S: Clone
            + NamedService
            + Service<
                Request<Body>,
                Response = Response<UnsyncBoxBody<Bytes, tonic::Status>>,
                Error = Infallible,
            > + Send
            + Sync
            + 'static,
        S::Response: IntoResponse,
        S::Future: Send,
    {
        crate::debug!("Mounting gRPC service {}", S::NAME);

        let srv_config = self.get_service_config(S::NAME);

        self.grpc_enabled = true;
        self.grpc_services.push(S::NAME);
        self.router = std::mem::take(&mut self.router).route_service(
            &format!("/{}/:method", S::NAME),
            GrpcService::new(service, srv_config),
        );
        self
    }

    /// Start the server.
    ///
    /// This will start multiple acceptor tasks (configured by `--server-acceptor-tasks-count`),
    /// listening in the socket configured by `--server-ip` and `--server-port`.
    ///
    /// Returns a [`Shutdown`] that can be used to gracefully shutdown the server.
    pub async fn start(mut self) -> Shutdown {
        crate::info!("Starting server...");

        if self.grpc_enabled {
            crate::debug!("gRPC enabled, adding gprc.health.v1.Health service for health checking");
            let (mut reporter, srv) = tonic_health::server::health_reporter();
            self = self.add_grpc_service(srv);

            for srv in &self.grpc_services {
                reporter
                    .set_service_status(srv, tonic_health::ServingStatus::Serving)
                    .await;
            }
        }

        self.router = std::mem::take(&mut self.router).route(
            &self.settings.server_http_health_checking_route,
            axum::routing::get(|| std::future::ready(StatusCode::OK)),
        );

        crate::debug!("Server settings: {:?}", self.settings);

        crate::info!(
            "Starting server tasks, {} will be created",
            self.settings.server_acceptor_tasks_count
        );

        let notify = Arc::new(Notify::new());
        let service = self.make_service();

        let mut server_tasks = Vec::with_capacity(self.settings.server_acceptor_tasks_count as _);
        for _ in 0..self.settings.server_acceptor_tasks_count {
            let task = start_server_listener(
                service.clone(),
                notify.clone(),
                &self.settings,
                self.grpc_enabled,
            )
            .await;

            server_tasks.push(task);
        }

        crate::info!(
            "Server started! Listening on {}:{}",
            self.settings.server_ip,
            self.settings.server_port
        );

        Shutdown {
            notify,
            server_tasks,
        }
    }

    fn make_service(&self) -> ServerService {
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
            .layer_fn(self::observability::TracingService::new)
            .into_inner();

        let axum_service = self
            .router
            .clone()
            .route_layer(stack)
            .into_make_service_with_connect_info::<Addrs>();

        let conn_limit = Arc::new(Semaphore::new(self.settings.server_connection_limit));

        tower::ServiceBuilder::new()
            .load_shed()
            .layer(GlobalConcurrencyLimitLayer::with_semaphore(conn_limit))
            .service(MakeConnectionService {
                inner: axum_service,
            })
    }

    fn get_service_config(&self, service: &str) -> ServiceConfiguration {
        let mut config = ServiceConfiguration {
            default_deadline: self.grpc_settings.grpc_default_deadline,
            body_limit: self.grpc_settings.grpc_default_body_limit,
            request_concurrency_limit: self.grpc_settings.grpc_default_concurrency,
        };

        for (srv, dur) in &self.grpc_settings.grpc_service_deadline {
            if service == srv {
                config.default_deadline = *dur;
            }
        }

        for (srv, limit) in &self.grpc_settings.grpc_service_body_limit {
            if service == srv {
                config.body_limit = *limit;
            }
        }

        for (srv, limit) in &self.grpc_settings.grpc_service_concurrency {
            if service == srv {
                config.request_concurrency_limit = *limit;
            }
        }

        config
    }
}

async fn start_server_listener(
    service: ServerService,
    notify: Arc<Notify>,
    settings: &ServerSettings,
    grpc_enabled: bool,
) -> JoinHandle<()> {
    let try_listen = |sock_addr: SocketAddr| {
        let addr: socket2::SockAddr = sock_addr.into();

        let socket = socket2::Socket::new(
            addr.domain(),
            socket2::Type::STREAM.nonblocking(),
            Some(socket2::Protocol::TCP),
        )?;

        socket.set_nodelay(true)?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&addr)?;
        socket.listen(settings.server_connection_limit as _)?;

        TcpListener::from_std(socket.into())
    };

    crate::debug!(
        "Starting listener in {}:{}",
        settings.server_ip,
        settings.server_port
    );

    let sock_addr = SocketAddr::new(settings.server_ip, settings.server_port);

    let listener = try_listen(sock_addr)
        .unwrap_or_else(|err| panic!("failed to bind to {sock_addr}: {err:?}"));

    let addr_incoming =
        AddrIncoming::from_listener(listener).expect("failed to configure listener");

    let server = hyper::Server::builder(addr_incoming)
        .http2_only(grpc_enabled || settings.server_http2_only)
        .http2_adaptive_window(settings.server_http2_adaptive_flow_control)
        .http2_keep_alive_interval(Some(settings.server_http2_keep_alive_interval))
        .executor(KisoExecutor)
        .serve(service)
        .with_graceful_shutdown(async move { notify.notified().await });

    tokio::spawn(async move {
        if let Err(err) = server.await {
            crate::error!("Server killed unexpectedly: {err}");
        }
    })
}

type ServerService =
    tower::load_shed::LoadShed<tower::limit::ConcurrencyLimit<MakeConnectionService>>;

type AxumMakeRouterService =
    axum::extract::connect_info::IntoMakeServiceWithConnectInfo<axum::Router, Addrs>;

#[derive(Clone)]
struct MakeConnectionService {
    inner: AxumMakeRouterService,
}

impl<'a> Service<&'a AddrStream> for MakeConnectionService {
    type Response = <AxumMakeRouterService as Service<&'a AddrStream>>::Response;
    type Error = <AxumMakeRouterService as Service<&'a AddrStream>>::Error;
    type Future = <AxumMakeRouterService as Service<&'a AddrStream>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        <AxumMakeRouterService as Service<&'a AddrStream>>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, stream: &'_ AddrStream) -> Self::Future {
        crate::debug!(
            "New connection accepted: {}:{} -> {}:{}",
            stream.remote_addr().ip(),
            stream.remote_addr().port(),
            stream.local_addr().ip(),
            stream.local_addr().port()
        )
        .attr("peer_ip", stream.remote_addr().ip().to_string())
        .attr("peer_port", stream.remote_addr().port());

        self.inner.call(stream)
    }
}

pub struct Shutdown {
    notify: Arc<Notify>,
    server_tasks: Vec<JoinHandle<()>>,
}

impl Shutdown {
    pub fn shutdown(self) {
        self.start_shutdown();
    }

    pub async fn shutdown_and_wait(self) {
        self.start_shutdown();
        futures_util::future::join_all(self.server_tasks).await;
    }

    pub async fn wait_for_shutdown_signals(self) {
        let (mut sigterm, mut sigint) = tokio::task::spawn_blocking(|| {
            let sigterm = tokio::signal::unix::signal(SignalKind::terminate())
                .expect("failed to register SIGTERM handler");

            let sigint = tokio::signal::unix::signal(SignalKind::interrupt())
                .expect("failed to register SIGINT handler");

            (sigterm, sigint)
        })
        .await
        .unwrap();

        tokio::select! {
            _ = sigterm.recv() => {
                crate::info!("received SIGTERM, shutting down server");
            },
            _ = sigint.recv() => {
                crate::info!("received SIGINT, shuttind down server");
            }
        }

        self.shutdown_and_wait().await;
    }

    fn start_shutdown(&self) {
        self.notify.notify_waiters();
        crate::info!("Shutdown notified for server tasks");
    }
}

#[derive(Clone)]
struct KisoExecutor;

impl<Fut> hyper::rt::Executor<Fut> for KisoExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    fn execute(&self, fut: Fut) {
        crate::rt::spawn(fut);
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
        // TODO(io-uring): tokio doesn't support SO_REUSEPORT
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
        server_connection_limit: usize = Semaphore::MAX_PERMITS,
        /// Route to use for HTTP health checking.
        server_http_health_checking_route: String = "/health".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::CmdDescriptor;

    #[tokio::test]
    async fn test_server() {
        crate::settings::builder(CmdDescriptor {
            name: "test_server",
            about: "A test for Server",
        })
        .install_from_args();

        let server = Server {
            grpc_enabled: true,
            ..Default::default()
        };

        let shutdown = server.start().await;

        let mut client = tonic_health::pb::health_client::HealthClient::new(
            crate::clients::GrpcChannel::with_default_settings(
                &"http://localhost:8080".parse().unwrap(),
            ),
        );

        let resp = client
            .check(tonic_health::pb::HealthCheckRequest {
                service: "grpc.health.v1.Health".to_string(),
            })
            .await
            .expect("failed to send RPC request");

        assert_eq!(resp.into_inner().status, 1);

        shutdown.shutdown_and_wait().await;

        client
            .check(tonic_health::pb::HealthCheckRequest {
                service: "grpc.health.v1.Health".to_string(),
            })
            .await
            .expect_err("received response from a server that should be down");
    }
}
