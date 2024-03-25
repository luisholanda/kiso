mod grpc;
mod https;
mod resolver;

#[doc(inline)]
pub use self::{
    grpc::{GrpcChannel, GrpcChannelSettings},
    https::{Client as HttpsClient, HttpsClientSettings},
};
