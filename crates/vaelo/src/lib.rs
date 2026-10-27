// SPDX-License-Identifier: Apache-2.0

//! `vaelo` is an asynchronous, multi-protocol RPC and HTTP framework.
//!
//! A generated client method creates an unstarted [`UnaryCall`]. Awaiting it
//! starts the request and waits for the response. Latency-sensitive callers
//! may call [`UnaryCall::start`] explicitly to encode and submit immediately,
//! receiving a [`CallHandle`] that represents only active completion.
//!
//! Internal diagnostics use the [`log`] facade. Applications install and
//! configure their preferred logger; vaelo does not install a global logger.
//! Targets follow module paths (`vaelo::transport`, `vaelo::protocol`,
//! `vaelo::server`, and `vaelo::runtime`). System failures are logged at error
//! level, peer/environment failures at warn level, and lifecycle transitions at
//! debug level. Normal EOF, cancellation races, and late responses are expected
//! control flow, not errors. Formatting is deferred until the corresponding log
//! level is enabled; request bodies and metadata are not logged.
#![feature(arbitrary_self_types)]

extern crate self as vaelo;

pub mod brpc;
mod buffer;
mod client;
mod descriptor;
mod error;
mod foundation;
pub mod http;
mod metadata;
mod protocol;
mod request_options;
pub mod runtime;
mod server;
mod transport;

pub use buffer::{IoBuf, IoBufBuilder, IoBufChunks, IoBufMut};
#[doc(hidden)]
pub use client::RawStreaming;
pub use client::{
    BidiStreaming, BidiStreamingCall, CallHandle, CallStats, ClientStreaming, ClientStreamingCall,
    ConnectionMode, ConnectionTag, Endpoint, EndpointId, EndpointSnapshot, IntoRequest,
    NamingError, NamingService, NamingWatch, ObservationPolicy, Request, RequestContext, Response,
    RpcChannel, RpcChannelBuilder, RpcTarget, SendStream, ServerStreamingCall, ServiceName,
    Streaming, StreamingCallHandle, UnaryCall, register_naming_service,
};
pub(crate) use descriptor::{MethodDescriptor, MethodKind, Protocol};
pub use descriptor::{RequestProtocol, RpcProtocol};
pub use error::{Error, ExecutionCertainty, Result, Status, StatusCode, StatusDetails};
pub(crate) use foundation::resource_pool::{
    Resource, ResourceId, ResourceLocal, ResourcePool, ResourcePoolConfig, ResourcePoolError,
    StaticResource, UninitResource,
};
pub use metadata::{InvalidMetadataKey, MetadataEntry, MetadataKey, MetadataMap, MetadataValue};
pub use protocol::grpc::GrpcOptions;
pub(crate) use protocol::{Decode, Encode};
pub use server::{
    BoundServer, BuiltinExposure, BuiltinServices, BuiltinServicesBuilder, HealthCheck,
    HttpRpcMapping, HttpServerOptions, ServerBuilder, ServerHandle, TcpOptions,
};
pub(crate) use server::{ServerController, ServerResponse, ServiceRegistry};
pub(crate) use transport::{TcpTransport, Transport};
pub(crate) use vaelo_derive::StaticResource;

#[doc(hidden)]
pub mod __private {
    pub use serde::{self, Deserialize, Serialize};
    pub use tokio_stream::Stream;

    pub use crate::descriptor::{Idempotency, MethodDescriptor, MethodKind, Protocol};
    pub use crate::foundation::resource_pool::{
        Resource, ResourceLocal, ResourcePool, ResourcePoolConfig, StaticResource,
    };
    pub use crate::protocol::{Decode, Encode, ProtocolSet};
    pub use crate::runtime::profile;
    pub use crate::server::{
        AcceptedTcpSocket, Acceptor, BidiStreamingProstMethod, BuiltinServices,
        ClientStreamingProstMethod, HttpRegistry, HttpRequest, HttpResponse, HttpServerOptions,
        IntoServiceRegistry, ListenerHandle, RegisterService, RegisteredMethod, RpcAcceptor,
        RpcAcceptorOptions, Server, ServerCall, ServerStreamingProstMethod, ServiceRegistry,
        ServiceRegistryBuilder, UnaryProstMethod,
    };
}
