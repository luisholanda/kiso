mod grpc;
mod https;
mod resolver;
mod retry;

#[doc(inline)]
pub use self::{
    grpc::{GrpcChannel, GrpcChannelSettings},
    https::{Client as HttpsClient, HttpsClientSettings},
    retry::RetrySettings,
};
