use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use axum::extract::connect_info::Connected;
use futures_util::{
    future::{select, Either},
    Future, FutureExt,
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::task::TaskTracker;
use tower::{Service, ServiceExt};

use crate::server::KisoExecutor;

pub(super) struct Listener {
    pub(super) listener: TcpListener,
    pub(super) http2_adaptive_flow_control: bool,
    pub(super) http2_keep_alive_interval: Duration,
    pub(super) service: super::ServerService,
    pub(super) notify: Arc<Notify>,
    pub(super) default_local_addr: SocketAddr,
}

impl Listener {
    pub(super) async fn run(mut self) {
        let notify = self.notify.clone();
        let mut notified = std::pin::pin!(notify.notified().fuse());

        let tracker = TaskTracker::new();

        loop {
            let (stream, peer_addr) = {
                let accept = std::pin::pin!(self.tcp_accept());
                match select(accept, &mut notified).await {
                    Either::Left((Some(conn), _)) => conn,
                    Either::Left((None, _)) => continue,
                    Either::Right(_) => break,
                }
            };

            let local_addr = stream.local_addr().unwrap_or(self.default_local_addr);
            let conn = Connection {
                stream,
                peer_addr,
                local_addr,
            };

            let conn_service = self
                .service
                .call(&conn)
                .await
                .unwrap_or_else(|err| match err {});

            crate::spawn(tracker.track_future(self.run_conn(conn, conn_service)));
        }

        tracing::debug!(name: "kiso.server.listener.shutdown", "shutdown signal received, stopping listener");

        tracker.close();
        tracker.wait().await;
    }

    async fn tcp_accept(&mut self) -> Option<(TcpStream, SocketAddr)> {
        fn is_connection_error(err: &io::Error) -> bool {
            matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionRefused
            )
        }

        // Acquire connection permit before accepting a new connection.
        let _ = self.service.ready().await;
        match self.listener.accept().await {
            Ok(conn) => Some(conn),
            Err(err) if is_connection_error(&err) => None,
            Err(err) => {
                // If the error was not caused by the connection itself, it could be that
                // the process reached the limit of open file descriptors. Then trying to
                // open a new connection would fail with `EMFILE`.
                //
                // In most cases, the server will close FDs after some time, either due to
                // connections being closed or other reasons. Therefore, we can just wait
                // a little before trying to create a new connection.
                tracing::error!(name: "kiso.server.listener.accept_error", error = ?err);
                tokio::time::sleep(Duration::from_secs(1)).await;
                None
            }
        }
    }

    fn run_conn(&self, conn: Connection, service: ConnService) -> impl Future<Output = ()> {
        let hyper_io = TokioIo::new(conn.stream);
        let hyper_service = TowerToHyperService::new(service);

        let mut serve = hyper_util::server::conn::auto::Builder::new(KisoExecutor).http2_only();

        serve
            .http2()
            .timer(TokioTimer::new())
            .adaptive_window(self.http2_adaptive_flow_control)
            .keep_alive_interval(self.http2_keep_alive_interval);

        let notify = self.notify.clone();
        async move {
            let conn = std::pin::pin!(serve.serve_connection(hyper_io, hyper_service));
            let notified = std::pin::pin!(notify.notified().fuse());

            if let Either::Right((_, mut conn)) = select(conn, notified).await {
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            }
        }
    }
}

struct Connection {
    stream: TcpStream,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
}

type ConnService = <super::ServerService as tower::Service<&'static Connection>>::Response;

#[derive(Clone)]
pub(super) struct Addrs {
    pub(super) peer: SocketAddr,
    pub(super) local: SocketAddr,
}

impl Connected<&'_ Connection> for Addrs {
    fn connect_info(target: &'_ Connection) -> Self {
        Self {
            peer: target.peer_addr,
            local: target.local_addr,
        }
    }
}
