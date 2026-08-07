// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BufMut;
use http::{Method, StatusCode as HttpStatusCode};
use prost::Message;
use vaelo::__private::{
    BuiltinServices, HttpRegistry, HttpRequest, HttpResponse, Idempotency, MethodDescriptor,
    MethodKind, ProtocolSet, RegisterService, RpcAcceptor, Server, ServiceRegistryBuilder,
    UnaryProstMethod,
};
use vaelo::{ConnectionMode, IoBuf, IoBufBuilder, Request, Response, RpcChannel, Status};

pub static ECHO_METHOD: MethodDescriptor =
    MethodDescriptor::new("demo.EchoService", "Echo", MethodKind::Unary, Idempotency::Idempotent);

static DETACHED_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, PartialEq, Message)]
pub struct EchoRequest {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct EchoResponse {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Default)]
pub struct EchoService;

impl EchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let request = request.into_inner();
        if request.message.starts_with("detached-probe-") {
            DETACHED_CALLS.fetch_add(1, Ordering::Release);
        }
        Ok(Response::new(EchoResponse { message: request.message }))
    }
}

#[doc(hidden)]
pub fn detached_call_count() -> usize {
    DETACHED_CALLS.load(Ordering::Acquire)
}

impl RegisterService for EchoService {
    fn register(self, registry: &mut ServiceRegistryBuilder) {
        registry.register(
            &ECHO_METHOD,
            UnaryProstMethod::new(
                std::sync::Arc::new(self),
                |service: std::sync::Arc<EchoService>, request| async move {
                    service.echo(request).await
                },
            ),
        );
    }
}

pub struct DemoServer {
    server: Server,
    address: SocketAddr,
}

impl DemoServer {
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn shutdown(&self) {
        self.server.shutdown();
    }
}

pub async fn start_server() -> vaelo::Result<DemoServer> {
    let services = ServiceRegistryBuilder::default().add_service(EchoService).build();
    let http = HttpRegistry::builder().route(Method::POST, "/echo", http_echo).build();
    let acceptor = RpcAcceptor::new(services)
        .allow_protocols(ProtocolSet::all())
        .with_builtin(BuiltinServices::default())
        .with_http(http, vaelo::__private::HttpServerOptions::default());
    let mut server = Server::new();
    let listener = server
        .listen_tcp("127.0.0.1:0", acceptor)
        .await
        .map_err(|error| vaelo::Error::Channel(error.to_string().into()))?;
    let address = listener.local_addr();
    Ok(DemoServer { server, address })
}

async fn http_echo(request: HttpRequest) -> HttpResponse {
    use vaelo::http::BodyExt;

    let body = request.body.collect().await.unwrap_or_else(|_| IoBuf::new());
    HttpResponse::new(HttpStatusCode::OK, body)
}

pub async fn brpc_echo(address: SocketAddr, message: impl Into<String>) -> Result<String, Status> {
    let channel = RpcChannel::brpc(address).build().map_err(Status::from)?;
    let request = EchoRequest { message: message.into() };
    let response = channel
        .call::<_, EchoResponse>(&ECHO_METHOD, request.into())
        .await
        .map_err(Status::from)?;
    Ok(response.into_inner().message)
}

pub async fn http_echo_client(
    address: SocketAddr,
    message: impl AsRef<[u8]>,
) -> Result<Vec<u8>, vaelo::http::Error> {
    use vaelo::http::BodyExt;

    let client =
        vaelo::http::Client::http1(address).connection_mode(ConnectionMode::Short).build()?;
    let body = iobuf_from_bytes(message.as_ref());
    let request = http::Request::post("/echo")
        .header(http::header::HOST, "localhost")
        .body(vaelo::http::Full::new(body))
        .map_err(|error| vaelo::http::Error::InvalidRequest(error.to_string()))?;
    let response = client.request(request).await?;
    Ok(collect_iobuf(&response.into_body().collect().await?))
}

pub async fn http_get(
    address: SocketAddr,
    path: &str,
) -> Result<http::Response<vaelo::http::Incoming>, vaelo::http::Error> {
    let client =
        vaelo::http::Client::http1(address).connection_mode(ConnectionMode::Short).build()?;
    let request = http::Request::get(path)
        .header(http::header::HOST, "localhost")
        .body(vaelo::http::Empty::new())
        .map_err(|error| vaelo::http::Error::InvalidRequest(error.to_string()))?;
    client.request(request).await
}

pub fn iobuf_from_bytes(bytes: &[u8]) -> IoBuf {
    let mut builder = IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}

pub fn collect_iobuf(buffer: &IoBuf) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(buffer.len());
    for chunk in buffer.chunks() {
        bytes.extend_from_slice(chunk);
    }
    bytes
}
