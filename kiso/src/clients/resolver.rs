use std::{
    collections::HashSet,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{FutureExt, Stream, StreamExt};
use hickory_resolver::{
    error::{ResolveError, ResolveErrorKind},
    proto::error::ProtoErrorKind,
    TokioAsyncResolver,
};
use hyper::Uri;
use once_cell::sync::Lazy;
use tokio::task::JoinHandle;
use tower::discover::Change;

#[inline(always)]
pub async fn resolve_uri(uri: &Uri) -> Result<Vec<IpAddr>, ResolveError> {
    RESOLVER.resolve_uri(uri).await
}

static RESOLVER: Lazy<NameResolver> = Lazy::new(NameResolver::default);

#[derive(Clone)]
struct NameResolver {
    resolver: TokioAsyncResolver,
}

impl Default for NameResolver {
    fn default() -> Self {
        Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()
                .expect("failed to initialize DNS resolver"),
        }
    }
}

impl NameResolver {
    async fn resolve_uri(&self, uri: &Uri) -> Result<Vec<IpAddr>, ResolveError> {
        let Some(host) = uri.host() else {
            panic!("tried to resolve URI without host: {uri}");
        };

        loop {
            return match self.resolver.srv_lookup(host).await {
                Ok(srv_lookup) => Ok(srv_lookup.ip_iter().collect()),
                Err(err) if matches!(err.kind(), ResolveErrorKind::NoRecordsFound { .. }) => {
                    let ips = self.resolver.lookup_ip(host).await?;
                    Ok(ips.into_iter().collect())
                }
                Err(err) if transient_error(&err) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(err) => Err(err),
            };
        }
    }
}

/// A stream for continuous service discovery of an URI.
pub struct ServiceDiscoveryStream {
    host: String,
    task: Option<JoinHandle<Result<(), ResolveError>>>,
    tx: flume::Sender<Change<IpAddr, ()>>,
    rx: flume::r#async::RecvStream<'static, Change<IpAddr, ()>>,
}

impl ServiceDiscoveryStream {
    /// Starts the discovery task, returning the stream of results.
    pub fn start(uri: &Uri) -> Self {
        let Some(host) = uri.host() else {
            panic!("cannot resolve URI without host: {uri}");
        };

        let (tx, rx) = flume::bounded(64);

        Self {
            host: host.to_string(),
            rx: rx.into_stream(),
            tx: tx.clone(),
            task: Some(crate::rt::spawn(BackgroundDiscoverer::start(
                host.to_string(),
                tx,
            ))),
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }
}

impl Drop for ServiceDiscoveryStream {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Stream for ServiceDiscoveryStream {
    type Item = Result<Change<IpAddr, ()>, ResolveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(task) = &mut this.task {
            if let Poll::Ready(res) = task.poll_unpin(cx) {
                match res {
                    // Resolver finished, continue polling from the channel.
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Err(err) if err.is_panic() => {
                        // TODO: Propagate the panic.
                        tracing::warn!(name: "kiso.client.dns.resolver.failure", "Background DNS resolver panicked: {err:?}");
                        *task = crate::spawn(BackgroundDiscoverer::start(
                            this.host.clone(),
                            this.tx.clone(),
                        ));
                    }
                    Err(_) => unreachable!("discovery task cancelled"),
                }

                this.task = None;
            }
        }

        match this.rx.poll_next_unpin(cx).map(|o| o.map(Ok)) {
            Poll::Pending if this.task.is_none() => Poll::Ready(None),
            res => res,
        }
    }
}

struct BackgroundDiscoverer {
    host: String,
    previous_set: HashSet<IpAddr>,
    new_set: HashSet<IpAddr>,
    tx: flume::Sender<Change<IpAddr, ()>>,
    resolver: &'static TokioAsyncResolver,
}

impl BackgroundDiscoverer {
    async fn start(
        host: String,
        tx: flume::Sender<Change<IpAddr, ()>>,
    ) -> Result<(), ResolveError> {
        BackgroundDiscoverer {
            host,
            previous_set: HashSet::default(),
            new_set: HashSet::default(),
            tx,
            resolver: &RESOLVER.resolver,
        }
        .run()
        .await
    }
}

impl BackgroundDiscoverer {
    async fn run(&mut self) -> Result<(), ResolveError> {
        if self.host == "localhost" {
            let _ = self.tx.send(Change::Insert(
                IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                (),
            ));
            return Ok(());
        }

        if let Ok(ip) = self.host.parse::<IpAddr>() {
            let _ = self.tx.send(Change::Insert(ip, ()));
            return Ok(());
        }

        while !self.tx.is_disconnected() {
            let res = match self.resolver.srv_lookup(&self.host).await {
                Ok(srv_lookup) => {
                    self.process_ips(srv_lookup.ip_iter()).await;
                    Ok(srv_lookup.as_lookup().valid_until())
                }
                Err(err) if matches!(err.kind(), ResolveErrorKind::NoRecordsFound { .. }) => {
                    match self.resolver.lookup_ip(&self.host).await {
                        Err(err) => Err(err),
                        Ok(lookup) => {
                            self.process_ips(lookup.iter()).await;
                            Ok(lookup.valid_until())
                        }
                    }
                }
                Err(err) => Err(err),
            };

            match res {
                Ok(sleep_until) => tokio::time::sleep_until(sleep_until.into()).await,
                Err(err) if transient_error(&err) => {
                    // FIXME: add exponential backoff.
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    async fn process_ips(&mut self, ips: impl Iterator<Item = IpAddr>) {
        self.new_set.clear();
        self.new_set.extend(ips);

        for &ip in self.new_set.difference(&self.previous_set) {
            if !self.insert(ip).await {
                return;
            }
        }

        for &ip in self.previous_set.difference(&self.new_set) {
            if !self.remove(ip).await {
                return;
            }
        }

        std::mem::swap(&mut self.previous_set, &mut self.new_set);
    }

    async fn insert(&self, ip: IpAddr) -> bool {
        self.tx.send_async(Change::Insert(ip, ())).await.is_ok()
    }

    async fn remove(&self, ip: IpAddr) -> bool {
        self.tx.send_async(Change::Remove(ip)).await.is_ok()
    }
}

fn transient_error(error: &ResolveError) -> bool {
    match error.kind() {
        ResolveErrorKind::NoConnections | ResolveErrorKind::Io(_) | ResolveErrorKind::Timeout => {
            true
        }
        ResolveErrorKind::Proto(err) => matches!(
            err.kind(),
            ProtoErrorKind::Busy
                | ProtoErrorKind::Canceled(_)
                | ProtoErrorKind::NoError
                | ProtoErrorKind::Io(_)
                | ProtoErrorKind::Timer
                | ProtoErrorKind::Timeout
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use futures_util::TryStreamExt;

    use super::*;

    static ONE_ONE_ONE_ONE: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1));
    static ONE_ZERO_ZERO_ONE: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(1, 0, 0, 1));

    #[tokio::test]
    async fn test_resolve_uri() {
        let cloudflare_dns = Uri::from_static("https://one.one.one.one");

        let ips = resolve_uri(&cloudflare_dns).await.unwrap();

        assert!(ips.contains(&ONE_ONE_ONE_ONE));
        assert!(ips.contains(&ONE_ZERO_ZERO_ONE));
    }

    #[tokio::test]
    async fn test_service_discovery_stream() {
        let cloudflare_dns = Uri::from_static("https://one.one.one.one");

        let ips: Vec<_> = ServiceDiscoveryStream::start(&cloudflare_dns)
            .take(2)
            .try_collect()
            .await
            .unwrap();

        for ip in ips {
            if let Change::Insert(ip, _) = ip {
                if ip != ONE_ONE_ONE_ONE && ip != ONE_ZERO_ZERO_ONE {
                    panic!("unknown cloudflare DNS ip: {ip}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_service_discovery_stream_loopback() {
        let localhost = Uri::from_static("http://localhost");

        let ips: Vec<_> = ServiceDiscoveryStream::start(&localhost)
            .try_collect()
            .await
            .unwrap();

        for ip in ips {
            if let Change::Insert(ip, _) = ip {
                assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
            }
        }
    }
}
