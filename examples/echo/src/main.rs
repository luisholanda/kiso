use futures_util::{
    stream::{MapOk, Repeat, Take},
    StreamExt, TryStreamExt,
};
use kiso::{observability::Exporters, settings::CmdDescriptor};
use tonic::{Request, Response, Status};

tonic::include_proto!("grpc.examples.echo");

pub struct EchoServer;

#[tonic::async_trait]
impl echo_server::Echo for EchoServer {
    /// UnaryEcho is unary echo.
    async fn unary_echo(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<EchoResponse>, tonic::Status> {
        Ok(Response::new(EchoResponse {
            message: request.into_inner().message,
        }))
    }

    /// Server streaming response type for the ServerStreamingEcho method.
    type ServerStreamingEchoStream = Take<Repeat<Result<EchoResponse, Status>>>;

    /// ServerStreamingEcho is server side streaming.
    async fn server_streaming_echo(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, tonic::Status> {
        Ok(Response::new(
            futures_util::stream::repeat(Ok(EchoResponse {
                message: request.into_inner().message,
            }))
            .take(10),
        ))
    }

    /// ClientStreamingEcho is client side streaming.
    async fn client_streaming_echo(
        &self,
        request: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<EchoResponse>, tonic::Status> {
        let Some(first) = request.into_inner().try_next().await? else {
            return Err(Status::not_found("no message received to echo back"));
        };

        Ok(Response::new(EchoResponse {
            message: first.message,
        }))
    }

    /// Server streaming response type for the BidirectionalStreamingEcho method.
    type BidirectionalStreamingEchoStream =
        MapOk<tonic::Streaming<EchoRequest>, fn(EchoRequest) -> EchoResponse>;

    /// BidirectionalStreamingEcho is bidi streaming.
    async fn bidirectional_streaming_echo(
        &self,
        request: Request<tonic::Streaming<EchoRequest>>,
    ) -> Result<Response<Self::BidirectionalStreamingEchoStream>, tonic::Status> {
        let stream = request.into_inner().map_ok(
            (|req| EchoResponse {
                message: req.message,
            }) as fn(EchoRequest) -> EchoResponse,
        );

        Ok(Response::new(stream))
    }
}

fn main() {
    kiso::settings::builder(CmdDescriptor {
        name: "echo-server",
        about: "A server that echoes back what clients sends.",
    })
    .install_from_args();

    kiso::rt::block_on(async {
        kiso::observability::initialize(Exporters {
            log_exporter: opentelemetry_stdout::LogExporter::default(),
            log_backtrace_printer: Box::new(|bc| format!("{bc:?}").into()),
            span_exporter: opentelemetry_sdk::testing::trace::NoopSpanExporter::new(),
        });

        let echo = echo_server::EchoServer::new(EchoServer);

        kiso::server::Server::default()
            .add_grpc_service(echo)
            .start()
            .await
            .wait_for_shutdown_signals()
            .await;
    })
}
