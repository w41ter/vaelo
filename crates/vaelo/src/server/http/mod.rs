// SPDX-License-Identifier: Apache-2.0

//! HTTP application routing and the HTTP/1 server frontend.
//!
//! HTTP routing is deliberately independent from RPC service routing. Explicit
//! HTTP routes win; the optional RPC mapping is only a fallback.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{HeaderMap, Method, StatusCode as HttpStatusCode};

use super::ServiceRegistry;
use crate::IoBuf;
use crate::http::Incoming;

pub type HttpHandlerFuture = Pin<Box<dyn Future<Output = HttpResponse> + Send + 'static>>;

pub struct HttpRequest {
    pub method: Method,
    pub version: http::Version,
    pub path: String,
    pub query: Option<String>,
    pub headers: HeaderMap,
    pub body: Incoming,
    pub path_parameters: HashMap<String, String>,
    pub remote_addr: SocketAddr,
}

pub struct HttpResponse {
    pub status: HttpStatusCode,
    pub headers: HeaderMap,
    pub body: IoBuf,
    pub(crate) streaming: Option<Pin<Box<dyn ErasedResponseBody + Send>>>,
}

impl HttpResponse {
    pub fn new(status: HttpStatusCode, body: IoBuf) -> Self {
        Self { status, headers: HeaderMap::new(), body, streaming: None }
    }

    pub fn empty(status: HttpStatusCode) -> Self {
        Self::new(status, IoBuf::new())
    }

    pub(crate) fn streaming<B>(status: HttpStatusCode, headers: HeaderMap, body: B) -> Self
    where
        B: crate::http::Body + Send + 'static,
    {
        Self {
            status,
            headers,
            body: IoBuf::new(),
            streaming: Some(Box::pin(ResponseBodyAdapter(body))),
        }
    }
}

pub(crate) trait ErasedResponseBody {
    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<crate::http::Frame, crate::http::Error>>>;
    fn size_hint(&self) -> crate::http::SizeHint;
}

struct ResponseBodyAdapter<B>(B);

impl<B> ErasedResponseBody for ResponseBodyAdapter<B>
where
    B: crate::http::Body,
{
    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<crate::http::Frame, crate::http::Error>>> {
        // SAFETY: the adapter and its body are pinned together and never moved.
        let body = unsafe { self.map_unchecked_mut(|adapter| &mut adapter.0) };
        match body.poll_frame(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
        }
    }

    fn size_hint(&self) -> crate::http::SizeHint {
        self.0.size_hint()
    }
}

pub trait HttpHandler: Send + Sync + 'static {
    fn call(&self, request: HttpRequest) -> HttpHandlerFuture;
}

impl<F, Fut> HttpHandler for F
where
    F: Fn(HttpRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HttpResponse> + Send + 'static,
{
    fn call(&self, request: HttpRequest) -> HttpHandlerFuture {
        Box::pin(self(request))
    }
}

struct Route {
    method: Method,
    segments: Vec<RouteSegment>,
    handler: Arc<dyn HttpHandler>,
}

enum RouteSegment {
    Literal(String),
    Parameter(String),
}

#[derive(Clone, Default)]
pub struct HttpRegistry {
    routes: Arc<[Route]>,
}

impl HttpRegistry {
    pub fn builder() -> HttpRegistryBuilder {
        HttpRegistryBuilder::default()
    }

    fn resolve(
        &self,
        method: &Method,
        path: &str,
    ) -> Option<(Arc<dyn HttpHandler>, HashMap<String, String>)> {
        let input: Vec<_> =
            path.trim_matches('/').split('/').filter(|part| !part.is_empty()).collect();
        for route in self.routes.iter() {
            if route.method != *method || route.segments.len() != input.len() {
                continue;
            }
            let mut parameters = HashMap::new();
            let matched =
                route.segments.iter().zip(&input).all(|(expected, actual)| match expected {
                    RouteSegment::Literal(value) => value == actual,
                    RouteSegment::Parameter(name) => {
                        parameters.insert(name.clone(), percent_decode(actual));
                        true
                    }
                });
            if matched {
                return Some((route.handler.clone(), parameters));
            }
        }
        None
    }
}

#[derive(Default)]
pub struct HttpRegistryBuilder {
    routes: Vec<Route>,
}

impl HttpRegistryBuilder {
    pub fn route(
        mut self,
        method: Method,
        path: impl Into<String>,
        handler: impl HttpHandler,
    ) -> Self {
        let path = path.into();
        let segments = path
            .trim_matches('/')
            .split('/')
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.strip_prefix(':').map_or_else(
                    || RouteSegment::Literal(part.to_owned()),
                    |name| RouteSegment::Parameter(name.to_owned()),
                )
            })
            .collect();
        self.routes.push(Route { method, segments, handler: Arc::new(handler) });
        self
    }

    pub fn build(self) -> HttpRegistry {
        HttpRegistry { routes: self.routes.into() }
    }
}

#[derive(Clone, Debug)]
pub struct HttpRpcMapping {
    allow_protobuf: bool,
    allow_json: bool,
    allow_query: bool,
}

impl HttpRpcMapping {
    pub fn allow_protobuf(mut self, enabled: bool) -> Self {
        self.allow_protobuf = enabled;
        self
    }

    pub fn allow_json(mut self, enabled: bool) -> Self {
        self.allow_json = enabled;
        self
    }

    pub fn allow_query(mut self, enabled: bool) -> Self {
        self.allow_query = enabled;
        self
    }
}

impl Default for HttpRpcMapping {
    fn default() -> Self {
        Self { allow_protobuf: true, allow_json: true, allow_query: true }
    }
}

#[derive(Clone, Debug)]
pub struct HttpServerOptions {
    pub(crate) max_header_size: usize,
    pub(crate) max_body_size: usize,
    pub(crate) max_pipeline_depth: usize,
    pub(crate) rpc_mapping: Option<HttpRpcMapping>,
}

impl Default for HttpServerOptions {
    fn default() -> Self {
        Self {
            max_header_size: 32 * 1024,
            max_body_size: 64 * 1024 * 1024,
            max_pipeline_depth: 128,
            rpc_mapping: None,
        }
    }
}

impl HttpServerOptions {
    pub fn max_header_size(mut self, limit: usize) -> Self {
        self.max_header_size = limit;
        self
    }

    pub fn max_body_size(mut self, limit: usize) -> Self {
        self.max_body_size = limit;
        self
    }

    pub fn max_pipeline_depth(mut self, limit: usize) -> Self {
        self.max_pipeline_depth = limit;
        self
    }

    pub fn enable_rpc_mapping(mut self, mapping: HttpRpcMapping) -> Self {
        self.rpc_mapping = Some(mapping);
        self
    }
}

pub(crate) async fn dispatch(
    registry: HttpRegistry,
    builtin_registry: HttpRegistry,
    services: ServiceRegistry,
    options: HttpServerOptions,
    mut request: HttpRequest,
) -> HttpResponse {
    if let Some((handler, parameters)) = registry.resolve(&request.method, &request.path) {
        request.path_parameters = parameters;
        return handler.call(request).await;
    }
    if let Some((handler, parameters)) = builtin_registry.resolve(&request.method, &request.path) {
        request.path_parameters = parameters;
        return handler.call(request).await;
    }
    let Some(mapping) = options.rpc_mapping else {
        return HttpResponse::empty(HttpStatusCode::NOT_FOUND);
    };
    rpc::dispatch(services, mapping, request).await
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2]))
        {
            output.push(high << 4 | low);
            index += 3;
        } else {
            output.push(if bytes[index] == b'+' { b' ' } else { bytes[index] });
            index += 1;
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) mod rpc;

#[doc(hidden)]
pub use rpc::{HttpRpcRequest, RpcBodyEncoding, RpcResponseEncoding};
