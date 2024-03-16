use std::{collections::HashSet, net::IpAddr, sync::Arc, time::Duration};

use hickory_resolver::TokioAsyncResolver;
use tokio::{sync::mpsc::Sender, task::AbortHandle};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};
use tower::{discover::Change, Service};

use crate::clients::HttpsClientSettings;

/// A production-ready gRPC channel.
///
/// # Load Balancing and Name Resolution
///
/// Upon DNS name resolution, the channel automatically opens a connection to each
/// IP in the DNS query result. It then balances requests using the "Power of two
/// choices" algorithm. It also automatically refreshes the servers set upon DNS
/// result invalidation.
///
/// # Deadlines
///
/// TODO
///
/// # Retries
///
/// TODO
///
/// # Tracing
///
/// TODO
#[derive(Clone)]
pub struct GrpcChannel {
    inner: Channel,
    // Will kill the background resolver task when the last channel is dropped.
    _resolver_task_aborter: Arc<Aborter>,
}

impl GrpcChannel {
    /// Create a new channel, with the given settings.
    ///
    /// If no settings modifications are needed, use [`GrpcChannel::with_default_settings`].
    pub async fn new(
        uri: Uri,
        opts: GrpcChannelSettings,
        https_client_settings: HttpsClientSettings,
    ) -> Self {
        let (inner, tx) =
            Channel::balance_channel::<IpAddr>(opts.grpc_channel_load_balancing_initial_capacity);

        let mut resolver = Box::new(BackgroundResolver {
            resolver: TokioAsyncResolver::tokio_from_system_conf()
                .expect("failed to initialize resolver"),
            scheme: uri.scheme_str().unwrap_or("https").to_string(),
            host: uri.host().expect("no host in service URI").to_string(),
            port: uri.port_u16().unwrap_or(443),
            path_and_query: uri.path_and_query().map_or("", |p| p.as_str()).to_string(),
            previous_set: HashSet::default(),
            new_set: HashSet::default(),
            tx,
            grpc_settings: opts,
            https_settings: https_client_settings,
        });

        let handle = crate::rt::spawn(async move {
            loop {
                resolver.run_once().await;
            }
        });

        Self {
            inner,
            _resolver_task_aborter: Arc::new(Aborter(handle.abort_handle())),
        }
    }

    /// Create a new channel, with the default command line given settings.
    pub async fn with_default_settings(uri: Uri) -> Self {
        Self::new(uri, crate::settings::get(), crate::settings::get()).await
    }
}

struct Aborter(AbortHandle);

impl Drop for Aborter {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<Req> Service<Req> for GrpcChannel
where
    Channel: Service<Req>,
{
    type Response = <Channel as Service<Req>>::Response;
    type Error = <Channel as Service<Req>>::Error;
    type Future = <Channel as Service<Req>>::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.inner.call(req)
    }
}

crate::settings!(pub GrpcChannelSettings {
    /// Initial capacity for the gRPC channel load balancer buffer.
    ///
    /// Defaults to 8.
    grpc_channel_load_balancing_initial_capacity: usize = 8,
    /// Size for the gRPC channel request buffer.
    ///
    /// Defaults to 1024.
    grpc_channel_buffer_size: usize = 1024,
});

struct BackgroundResolver {
    resolver: TokioAsyncResolver,
    scheme: String,
    host: String,
    port: u16,
    path_and_query: String,
    previous_set: HashSet<IpAddr>,
    new_set: HashSet<IpAddr>,
    tx: Sender<Change<IpAddr, Endpoint>>,
    grpc_settings: GrpcChannelSettings,
    https_settings: HttpsClientSettings,
}

impl BackgroundResolver {
    async fn run_once(&mut self) {
        match self.resolver.lookup_ip(&self.host).await {
            Err(err) => {
                crate::error!("failed to resolve {}: {err:?}", self.host);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(lookup) => {
                self.new_set.clear();

                for ip in lookup.iter() {
                    if !self.previous_set.contains(&ip) {
                        let endpoint = self.endpoint_for_ip(ip);
                        if self.tx.send(Change::Insert(ip, endpoint)).await.is_err() {
                            return;
                        };
                    }

                    self.new_set.insert(ip);
                }

                for &ip in self.previous_set.difference(&self.new_set) {
                    if self.tx.send(Change::Remove(ip)).await.is_err() {
                        return;
                    }
                }

                std::mem::swap(&mut self.previous_set, &mut self.new_set);

                tokio::time::sleep_until(lookup.valid_until().into()).await;
            }
        }
    }

    fn endpoint_for_ip(&self, ip: IpAddr) -> Endpoint {
        let uri = Uri::builder()
            .authority(format!("{ip}:{}", self.port))
            .path_and_query(&self.path_and_query)
            .scheme(&*self.scheme)
            .build()
            .expect("failed to build endpoint URI");

        Channel::builder(uri)
            .http2_adaptive_window(self.https_settings.https_client_http2_use_adaptive_window)
            .http2_keep_alive_interval(self.https_settings.https_client_http2_keep_alive_interval)
            .buffer_size(self.grpc_settings.grpc_channel_buffer_size)
            .tcp_nodelay(true)
            .tls_config(ClientTlsConfig::new().domain_name(self.host.clone()))
            .expect("failed to build endpoint")
    }
}
