// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::ServerResponse;
use super::http::{HttpRpcRequest, RpcBodyEncoding};
use crate::protocol::InboundRequest;
use crate::runtime::rpc_variables::{self, RpcCallGuard, RpcMethodVariables};
use crate::{MethodDescriptor, Status, StatusCode};

mod controller;
mod methods;

pub use controller::ServerController;
pub(crate) use controller::ServerRequestContext;
pub use methods::{
    BidiStreamingProstMethod, ClientStreamingProstMethod, ServerStreamingProstMethod,
    UnaryProstMethod,
};

type BoxServerFuture = Pin<Box<dyn Future<Output = ServerResponse> + Send + 'static>>;

trait RawService: Send + Sync + 'static {
    fn start(&self, request: InboundRequest) -> ServerCall;
}

impl<F, Fut> RawService for F
where
    F: Fn(InboundRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServerResponse> + Send + 'static,
{
    fn start(&self, request: InboundRequest) -> ServerCall {
        ServerCall::new(self(request))
    }
}

/// A method call whose concrete protobuf request and response types are hidden.
pub struct ServerCall {
    future: BoxServerFuture,
}

impl ServerCall {
    pub fn new(future: impl Future<Output = ServerResponse> + Send + 'static) -> Self {
        Self { future: Box::pin(future) }
    }

    pub async fn finish(self) -> ServerResponse {
        self.future.await
    }

    pub(crate) fn poll(&mut self, context: &mut Context<'_>) -> Poll<ServerResponse> {
        self.future.as_mut().poll(context)
    }

    fn recorded(self, guard: RpcCallGuard) -> Self {
        Self::new(async move {
            let response = self.finish().await;
            guard.finish(response.status.as_ref().map(Status::code));
            response
        })
    }

    pub(super) fn failed(status: Status) -> Self {
        Self::new(async move { ServerResponse::failed(status) })
    }
}

/// Type-erased registry entry. Implementations retain their concrete prost
/// request/response types and perform the conversion before invoking service
/// code.
pub trait RegisteredMethod: Send + Sync + 'static {
    fn start(&self, controller: ServerController, payload: crate::IoBuf) -> ServerCall;

    fn start_stream(
        &self,
        _controller: ServerController,
        _request: crate::client::RawStreaming,
    ) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "streaming request is not enabled for method",
        ))
    }

    fn start_http(&self, controller: ServerController, request: HttpRpcRequest) -> ServerCall {
        match request.body {
            RpcBodyEncoding::Protobuf(payload) => self.start(controller, payload),
            RpcBodyEncoding::Json(_) | RpcBodyEncoding::Query(_) => ServerCall::failed(
                Status::new(StatusCode::Unknown, "JSON/query mapping is not enabled for method"),
            ),
        }
    }
}

struct MethodEntry {
    descriptor: &'static MethodDescriptor,
    method: Arc<dyn RegisteredMethod>,
    variables: Arc<RpcMethodVariables>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct MethodId(u32);

impl MethodId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone)]
pub struct ServiceRegistry {
    inner: Arc<ServiceRegistryInner>,
}

struct ServiceRegistryInner {
    routes: HashMap<(&'static str, &'static str), MethodId>,
    methods: Box<[MethodEntry]>,
    raw: Option<Arc<dyn RawService>>,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(ServiceRegistryInner {
                routes: HashMap::new(),
                methods: Box::new([]),
                raw: None,
            }),
        }
    }
}

impl ServiceRegistry {
    pub fn builder() -> ServiceRegistryBuilder {
        ServiceRegistryBuilder::default()
    }

    pub fn start(&self, remote_addr: SocketAddr, request: InboundRequest) -> ServerCall {
        if let Some(raw) = &self.inner.raw {
            return raw.start(request);
        }
        match self.resolve(&request.service, &request.method) {
            Some(method) => self.start_resolved(
                method,
                ServerRequestContext::brpc(
                    remote_addr,
                    request.deadline,
                    request.log_id,
                    request.request_id,
                    request.attachment,
                ),
                request.payload,
            ),
            None => {
                ServerCall::failed(Status::new(StatusCode::Unimplemented, "unknown RPC method"))
            }
        }
    }

    pub(crate) fn resolve(&self, service: &str, method: &str) -> Option<MethodId> {
        self.inner.routes.get(&(service, method)).copied()
    }

    pub(crate) fn start_resolved(
        &self,
        method: MethodId,
        context: ServerRequestContext,
        payload: crate::IoBuf,
    ) -> ServerCall {
        let Some(entry) = self.inner.methods.get(method.index()) else {
            return ServerCall::failed(Status::new(
                StatusCode::Unimplemented,
                "unknown RPC method",
            ));
        };
        let guard = entry.variables.start_call();
        entry.method.start(ServerController::new(context), payload).recorded(guard)
    }

    pub(crate) fn start_stream_resolved(
        &self,
        method: MethodId,
        context: ServerRequestContext,
        request: crate::client::RawStreaming,
    ) -> ServerCall {
        let Some(entry) = self.inner.methods.get(method.index()) else {
            return ServerCall::failed(Status::new(
                StatusCode::Unimplemented,
                "unknown RPC method",
            ));
        };
        let guard = entry.variables.start_call();
        entry.method.start_stream(ServerController::new(context), request).recorded(guard)
    }

    pub(crate) fn descriptor(&self, method: MethodId) -> Option<&'static MethodDescriptor> {
        self.inner.methods.get(method.index()).map(|entry| entry.descriptor)
    }

    pub fn method_descriptors(&self) -> Vec<&'static MethodDescriptor> {
        let mut descriptors: Vec<_> =
            self.inner.methods.iter().map(|entry| entry.descriptor).collect();
        descriptors.sort_by_key(|descriptor| (descriptor.service, descriptor.method));
        descriptors
    }

    pub fn from_raw<F, Fut>(handler: F) -> Self
    where
        F: Fn(InboundRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResponse> + Send + 'static,
    {
        Self {
            inner: Arc::new(ServiceRegistryInner {
                routes: HashMap::new(),
                methods: Box::new([]),
                raw: Some(Arc::new(handler)),
            }),
        }
    }

    pub(crate) fn start_http(
        &self,
        remote_addr: SocketAddr,
        service: &str,
        method: &str,
        request: HttpRpcRequest,
    ) -> Option<ServerCall> {
        let entry = self.inner.methods.get(self.resolve(service, method)?.index())?;
        let controller = ServerController::new(ServerRequestContext::http(remote_addr));
        let guard = entry.variables.start_call();
        Some(entry.method.start_http(controller, request).recorded(guard))
    }
}

#[derive(Default)]
pub struct ServiceRegistryBuilder {
    routes: HashMap<(&'static str, &'static str), MethodId>,
    methods: Vec<MethodEntry>,
}

impl ServiceRegistryBuilder {
    pub fn add_service(mut self, service: impl RegisterService) -> Self {
        service.register(&mut self);
        self
    }

    pub fn register(
        &mut self,
        descriptor: &'static MethodDescriptor,
        method: impl RegisteredMethod,
    ) {
        let entry = MethodEntry {
            descriptor,
            method: Arc::new(method),
            variables: rpc_variables::server_method(descriptor),
        };
        let key = (descriptor.service, descriptor.method);
        if let Some(id) = self.routes.get(&key).copied() {
            self.methods[id.index()] = entry;
        } else {
            let id = MethodId(self.methods.len() as u32);
            self.routes.insert(key, id);
            self.methods.push(entry);
        }
    }

    pub fn build(self) -> ServiceRegistry {
        ServiceRegistry {
            inner: Arc::new(ServiceRegistryInner {
                routes: self.routes,
                methods: self.methods.into(),
                raw: None,
            }),
        }
    }
}

pub trait IntoServiceRegistry {
    fn into_service_registry(self) -> ServiceRegistry;
}

impl IntoServiceRegistry for ServiceRegistry {
    fn into_service_registry(self) -> ServiceRegistry {
        self
    }
}

impl<F, Fut> IntoServiceRegistry for F
where
    F: Fn(InboundRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ServerResponse> + Send + 'static,
{
    fn into_service_registry(self) -> ServiceRegistry {
        ServiceRegistry::from_raw(self)
    }
}

pub trait RegisterService: Send + Sync + 'static {
    fn register(self, registry: &mut ServiceRegistryBuilder);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MethodKind;
    use crate::descriptor::Idempotency;

    static FIRST: MethodDescriptor =
        MethodDescriptor::new("example.Echo", "First", MethodKind::Unary, Idempotency::Unknown);
    static SECOND: MethodDescriptor =
        MethodDescriptor::new("example.Echo", "Second", MethodKind::Unary, Idempotency::Unknown);

    struct DummyMethod;

    impl RegisteredMethod for DummyMethod {
        fn start(&self, _controller: ServerController, _payload: crate::IoBuf) -> ServerCall {
            ServerCall::failed(Status::new(StatusCode::Internal, "unused test method"))
        }
    }

    #[test]
    fn resolved_method_ids_are_compact_and_stable_on_replacement() {
        let mut builder = ServiceRegistry::builder();
        builder.register(&FIRST, DummyMethod);
        builder.register(&SECOND, DummyMethod);
        let first = builder.routes[&("example.Echo", "First")];
        builder.register(&FIRST, DummyMethod);
        let registry = builder.build();

        assert_eq!(registry.resolve("example.Echo", "First"), Some(first));
        assert_eq!(registry.method_descriptors().len(), 2);
    }
}
