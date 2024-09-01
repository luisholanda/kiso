use std::{
    error::Error,
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::http::HeaderValue;
use bytes::Buf;
use futures_util::{future::BoxFuture, FutureExt, TryStreamExt};
use http_body::Frame;
use http_body_util::BodyExt;
use hyper::{body::Body as HttpBody, Request, Response};
use opentelemetry_semantic_conventions::attribute;
use tokio::{sync::mpsc::Sender, task::AbortHandle};
use tonic::{
    body::BoxBody,
    transport::{Channel, ClientTlsConfig, Endpoint, Uri},
};
use tower::{buffer::Buffer, discover::Change, util::BoxService, Service, ServiceBuilder};

use super::{
    https::{TracedBody, TracingService as HttpTracingService},
    resolver::ServiceDiscoveryStream,
};
use crate::{clients::HttpsClientSettings, context::Deadline};

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
/// The channel automatically sets the `grpc-timeout` header, based on the context
/// value for [`Deadline`], which the kiso's server implementation sets automatically.
///
/// # Retries
///
/// TODO
///
/// # Tracing
///
/// This channel automatically creates two spans for each RPC request made through it:
/// one for the RPC itself, and another for the response's body recv window. In both,
/// the channel send logs for each message received/sent, allowing for easier understanding
/// of message timings.
#[derive(Clone)]
pub struct GrpcChannel {
    #[allow(clippy::type_complexity)]
    inner: Buffer<
        BoxService<
            Request<BoxBody>,
            Response<TracedBody<LogMsgsBody<BoxBody>>>,
            tonic::transport::Error,
        >,
        Request<BoxBody>,
    >,
    // Will kill the background resolver task when the last channel is dropped.
    _resolver_task_aborter: Arc<Aborter>,
}

impl GrpcChannel {
    /// Create a new channel, with the given settings.
    ///
    /// If no settings modifications are needed, use [`GrpcChannel::with_default_settings`].
    pub fn new(
        uri: &Uri,
        opts: GrpcChannelSettings,
        mut https_settings: HttpsClientSettings,
    ) -> Self {
        // TODO: stop using tonic's default Channel.
        //
        // It doesn't allow us to implement all the features we want, specially load-based load balacing
        // and service rediscovery on disconnects or reuse our HTTPS connector. It also handles timeouts
        // too low in the stack, which can cause problems on high load.
        let (inner, tx) =
            Channel::balance_channel::<IpAddr>(opts.grpc_channel_load_balancing_initial_capacity);
        let default_deadline = opts.grpc_channel_default_deadline;

        let service = ServiceBuilder::new()
            .boxed()
            .map_response(|res: Response<LogMsgsBody<BoxBody>>| res.map(TracedBody::new))
            .layer(https_settings.take_request_sensitive_headers_layer())
            .layer(https_settings.take_response_sensitive_headers_layer())
            .layer_fn(|inner| super::retry::RetryService { inner })
            .map_request(|req: Request<super::retry::UnsyncBoxBodyBoxError>| {
                req.map(|b| {
                    b.map_err(|err| match err.downcast::<tonic::Status>() {
                        Ok(status) => *status,
                        Err(err) => tonic::Status::internal(err.to_string()),
                    })
                    .boxed_unsync()
                })
            })
            .layer_fn(|inner| HttpTracingService { inner })
            .layer_fn(|inner| TracingService { inner })
            .map_request(move |mut req: Request<BoxBody>| {
                let timeout = crate::context::try_current::<Deadline>()
                    .map_or(default_deadline, |d| d.timeout());

                let header = HeaderValue::from_str(&duration_to_grpc_timeout(timeout))
                    .expect("invalid grpc-timeout header value");
                req.headers_mut().insert("grpc-timeout", header);

                req
            })
            .service(inner);

        let service = Buffer::new(service, opts.grpc_channel_requests_buffer_size);

        let resolver = Box::new(BackgroundResolver {
            scheme: uri.scheme_str().unwrap_or("https").to_string(),
            port: uri.port_u16().unwrap_or(443),
            path_and_query: uri.path_and_query().map_or("", |p| p.as_str()).to_string(),
            discovery_stream: ServiceDiscoveryStream::start(uri),
            tx,
            grpc_settings: opts,
            https_settings,
        });

        let handle = crate::rt::spawn(resolver.run());

        Self {
            inner: service,
            _resolver_task_aborter: Arc::new(Aborter(handle.abort_handle())),
        }
    }

    /// Create a new channel, with the default command line given settings.
    pub fn with_default_settings(uri: &Uri) -> Self {
        Self::new(uri, crate::settings::get(), crate::settings::get())
    }
}

struct Aborter(AbortHandle);

impl Drop for Aborter {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Service<Request<BoxBody>> for GrpcChannel {
    type Response = Response<TracedBody<LogMsgsBody<BoxBody>>>;
    type Error = Box<dyn Error + Send + Sync + 'static>;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        self.inner.call(req).boxed()
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
    /// Default deadline for RPC calls when no deadline is available in the context.
    #[arg(value_parser = crate::settings::DurationParser)]
    grpc_channel_default_deadline: Duration = Duration::from_secs(10),
    /// The size of the requests buffer.
    ///
    /// This should be set to at least the maximum number of concurrent requests
    /// the channel will see.
    grpc_channel_requests_buffer_size: usize = 1024,
});

struct BackgroundResolver {
    scheme: String,
    port: u16,
    path_and_query: String,
    discovery_stream: ServiceDiscoveryStream,
    tx: Sender<Change<IpAddr, Endpoint>>,
    grpc_settings: GrpcChannelSettings,
    https_settings: HttpsClientSettings,
}

impl BackgroundResolver {
    async fn run(mut self) {
        while !self.tx.is_closed() {
            match self.discovery_stream.try_next().await {
                Ok(Some(Change::Insert(ip, _))) => {
                    let endpoint = self.endpoint_for_ip(ip);
                    drop(self.tx.send(Change::Insert(ip, endpoint)).await);
                }
                Ok(Some(Change::Remove(ip))) => {
                    drop(self.tx.send(Change::Remove(ip)).await);
                }
                Ok(None) => return,
                Err(err) => {
                    tracing::error!(
                        name: "kiso.client.grpc.resolver.failure", 
                        error = &err as &(dyn std::error::Error + 'static),
                        "resolve failure in service discovery");
                    return;
                }
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

        let endpoint = Channel::builder(uri)
            .http2_adaptive_window(self.https_settings.https_client_http2_use_adaptive_window)
            .http2_keep_alive_interval(self.https_settings.https_client_http2_keep_alive_interval)
            .buffer_size(self.grpc_settings.grpc_channel_buffer_size)
            .tcp_nodelay(true);

        if self.scheme == "https" {
            endpoint
                .tls_config(ClientTlsConfig::new().domain_name(self.discovery_stream.host()))
                .expect("failed to build endpoint")
        } else {
            endpoint
        }
    }
}

fn duration_to_grpc_timeout(dur: Duration) -> String {
    const MAX_ALLOWED_TIMEOUT: u128 = 10_000_000;
    const SUFFIXES: &[&str] = &["n", "u", "m", "S"];

    let mut val = dur.as_nanos();
    let mut suf_idx = 0;

    while val > MAX_ALLOWED_TIMEOUT {
        val /= 1000;
        suf_idx += 1;
    }

    let suf = SUFFIXES.get(suf_idx).expect("deadline too large");

    format!("{val}{suf}")
}

#[derive(Clone)]
struct TracingService<S> {
    inner: S,
}

impl<S, B2> Service<Request<BoxBody>> for TracingService<S>
where
    S: Service<Request<BoxBody>, Response = Response<B2>>,
    S::Future: Future<Output = Result<Response<B2>, S::Error>> + Send + 'static,
    S::Error: std::fmt::Display + 'static,
    B2: 'static,
{
    type Response = Response<LogMsgsBody<B2>>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.path())
            .expect("gRPC call without a path??");

        let mut splits = path.rsplit('/');
        let method = splits.next().unwrap();
        let service = splits.next().unwrap();
        let rpc_name = format!("{service}/{method}");

        let span = tracing::Span::current();
        if !span.is_disabled() {
            let fields = super::https::RequestSpanFields::instance(&span);

            span.record(&fields.otel_name, &rpc_name)
                .record(&fields.rpc_system, "grpc")
                .record(&fields.rpc_service, service)
                .record(&fields.rpc_method, method);
        }

        let req = req.map(|b| LogMsgsBody::new("SENT", rpc_name.clone(), b).boxed_unsync());
        let fut = self.inner.call(req);

        Box::pin(async move {
            let res = fut.await?;

            if !span.is_disabled() {
                let fields = super::https::RequestSpanFields::instance(&span);
                if let Some(status) = tonic::Status::from_header_map(res.headers()) {
                    span.record(&fields.rpc_grpc_status_code, status.code() as i64);
                    if status.code() != tonic::Code::Ok {
                        span.record(&fields.error, status.message());
                    }
                }
            }

            Ok(res.map(|b| LogMsgsBody::new("RECV", rpc_name, b)))
        })
    }
}

pub struct LogMsgsBody<B> {
    body: B,
    counter: i64,
    typ: &'static str,
    rpc_name: String,
    event_name: &'static str,
}

impl<B> LogMsgsBody<B> {
    fn new(typ: &'static str, rpc_name: String, body: B) -> Self {
        Self {
            body,
            counter: 1,
            typ,
            rpc_name,
            event_name: if typ == "SENT" {
                "kiso.client.grpc.message.sent"
            } else {
                "kiso.client.grpc.message.recv"
            },
        }
    }
}

impl<B: HttpBody> HttpBody for LogMsgsBody<B>
where
    B::Error: std::fmt::Display,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = unsafe { self.get_unchecked_mut() };
        let inner_frame =
            unsafe { std::task::ready!(Pin::new_unchecked(&mut this.body).poll_frame(cx)?) };

        if let Some(data_frame) = inner_frame.as_ref().and_then(|f| f.data_ref()) {
            tracing::debug!(
                {
                    otel.name = this.event_name,
                    { attribute::RPC_MESSAGE_COMPRESSED_SIZE } = data_frame.remaining() as i64,
                    { attribute::RPC_MESSAGE_ID } = this.counter,
                    { attribute::RPC_MESSAGE_TYPE } = this.typ,
                },
                "{}: {} message {} with size {}",
                this.rpc_name,
                this.typ,
                this.counter,
                data_frame.remaining()
            );
        }

        Poll::Ready(inner_frame.map(Ok))
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
