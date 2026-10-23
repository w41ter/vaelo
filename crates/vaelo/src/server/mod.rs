// SPDX-License-Identifier: Apache-2.0

//! Server listener, acceptor, and protocol-driven socket machinery.

mod acceptor;
mod builder;
pub mod builtin;
pub mod http;
mod response;
mod service;

pub use acceptor::{
    AcceptedTcpSocket, Acceptor, ListenerHandle, RpcAcceptor, RpcAcceptorOptions, Server,
};
pub use builder::{BoundServer, ServerBuilder, ServerHandle, TcpOptions};
pub use builtin::{BuiltinExposure, BuiltinServices, BuiltinServicesBuilder, HealthCheck};
pub use http::{
    HttpRegistry, HttpRegistryBuilder, HttpRequest, HttpResponse, HttpRpcMapping, HttpServerOptions,
};
pub use response::{ServerResponse, ServerResponseBody};
pub use service::{
    BidiStreamingProstMethod, ClientStreamingProstMethod, IntoServiceRegistry, RegisterService,
    RegisteredMethod, ServerCall, ServerController, ServerStreamingProstMethod, ServiceRegistry,
    ServiceRegistryBuilder, UnaryProstMethod,
};
pub(crate) use service::{MethodId, ServerRequestContext};

#[derive(Clone)]
pub(crate) struct ServerSessionContext {
    pub services: ServiceRegistry,
    pub http_registry: HttpRegistry,
    pub builtin_registry: HttpRegistry,
    pub http_options: HttpServerOptions,
}
