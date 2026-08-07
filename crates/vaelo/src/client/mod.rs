// SPDX-License-Identifier: Apache-2.0

//! Capability channels and protocol-neutral client transport machinery.

pub(crate) mod brpc;
pub(crate) mod call;
pub(crate) mod channel;
pub(crate) mod grpc;
pub(crate) mod http;
pub(crate) mod recycler;
mod request;
mod response;
mod streaming;

pub use call::{CallHandle, UnaryCall};
pub(crate) use channel::{Channel, ChannelOptions};
pub use channel::{
    ConnectionMode, ConnectionTag, ObservationPolicy, RpcChannel, RpcChannelBuilder,
};
pub(crate) use request::ServerRequestInfo;
pub use request::{IntoRequest, Request, RequestContext};
pub use response::{CallStats, Response};
#[doc(hidden)]
pub use streaming::RawStreaming;
pub use streaming::{
    BidiStreaming, BidiStreamingCall, ClientStreaming, ClientStreamingCall, SendStream,
    ServerStreamingCall, Streaming, StreamingCallHandle,
};
