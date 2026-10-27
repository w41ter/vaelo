// SPDX-License-Identifier: Apache-2.0

//! High-level server construction API.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;

use super::{
    AcceptedTcpSocket, Acceptor, BuiltinServices, HttpRegistryBuilder, HttpRequest, HttpResponse,
    HttpServerOptions, RegisterService, RpcAcceptor, RpcAcceptorOptions, ServiceRegistryBuilder,
};
use crate::protocol::ProtocolSet;
use crate::{ObservationPolicy, Protocol, RpcProtocol, http};

/// TCP listener configuration applied while binding.
///
/// This is cold-path configuration and deliberately says nothing about the
/// runtime's worker, task, queue or connection-cache topology.
#[derive(Clone, Debug)]
pub struct TcpOptions {
    reuse_port: bool,
    backlog: u32,
    nodelay: bool,
}

impl TcpOptions {
    pub fn reuse_port(mut self, enabled: bool) -> Self {
        self.reuse_port = enabled;
        self
    }

    pub fn backlog(mut self, backlog: u32) -> Self {
        self.backlog = backlog;
        self
    }

    pub fn nodelay(mut self, enabled: bool) -> Self {
        self.nodelay = enabled;
        self
    }
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self { reuse_port: false, backlog: 1024, nodelay: true }
    }
}

/// Application-facing server builder.
///
/// `RpcAcceptor`, registries and listener handles remain available as low-level
/// integration points, but ordinary services should be expressible entirely
/// through this builder.
pub struct ServerBuilder {
    rpc_protocols: Vec<RpcProtocol>,
    http_versions: Vec<http::Version>,
    services: ServiceRegistryBuilder,
    http_routes: HttpRegistryBuilder,
    http_options: HttpServerOptions,
    tcp_options: TcpOptions,
    builtins: Option<BuiltinServices>,
    observation: ObservationPolicy,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            rpc_protocols: vec![RpcProtocol::Brpc, RpcProtocol::Grpc],
            http_versions: vec![http::Version::Http1],
            services: ServiceRegistryBuilder::default(),
            http_routes: HttpRegistryBuilder::default(),
            http_options: HttpServerOptions::default(),
            tcp_options: TcpOptions::default(),
            builtins: None,
            observation: ObservationPolicy::Disabled,
        }
    }
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the accepted RPC protocol set for the listener.
    pub fn rpc_protocols(mut self, protocols: impl IntoIterator<Item = RpcProtocol>) -> Self {
        self.rpc_protocols = protocols.into_iter().collect();
        self
    }

    /// Replaces the accepted ordinary HTTP version set for the listener.
    /// gRPC remains configured through [`rpc_protocols`](Self::rpc_protocols).
    pub fn http_versions(mut self, versions: impl IntoIterator<Item = http::Version>) -> Self {
        self.http_versions = versions.into_iter().collect();
        self
    }

    pub fn add_service(mut self, service: impl RegisterService) -> Self {
        self.services = self.services.add_service(service);
        self
    }

    pub fn http_options(mut self, options: HttpServerOptions) -> Self {
        self.http_options = options;
        self
    }

    pub fn tcp_options(mut self, options: TcpOptions) -> Self {
        self.tcp_options = options;
        self
    }

    /// Enables explicitly configured operational endpoints.
    ///
    /// Built-ins are opt-in at the high-level builder so adding the framework
    /// never silently reserves application routes.
    pub fn builtins(mut self, services: BuiltinServices) -> Self {
        self.builtins = Some(services);
        self
    }

    pub fn observation(mut self, policy: ObservationPolicy) -> Self {
        self.observation = policy;
        self
    }

    /// Adds an ordinary HTTP route.
    ///
    /// The handler future and response body remain concrete at registration;
    /// applications are never required to box either one. Any necessary type
    /// erasure happens behind the server registry boundary.
    pub fn route<H, Fut, B>(
        mut self,
        method: http::Method,
        path: impl Into<String>,
        handler: H,
    ) -> Self
    where
        H: Fn(http::Request<http::Incoming>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<http::Response<B>, http::Error>> + Send + 'static,
        B: http::Body + Send + 'static,
    {
        self.http_routes = self.http_routes.route(method, path, move |request| {
            let future = handler(to_public_request(request));
            async move { to_internal_response(future.await).await }
        });
        self
    }

    /// Adds a stateful HTTP service without forcing its call future to be
    /// boxed or detached from a borrow of the service.
    pub fn route_service(
        mut self,
        method: http::Method,
        path: impl Into<String>,
        service: impl http::Service,
    ) -> Self {
        let service = Arc::new(service);
        self.http_routes = self.http_routes.route(method, path, move |request| {
            let service = service.clone();
            async move {
                let response = service.call(to_public_request(request)).await;
                to_internal_response(response).await
            }
        });
        self
    }

    /// Binds sockets and finalizes immutable dispatch tables without starting
    /// request admission.
    ///
    /// Separating bind from serve lets tests inspect an ephemeral address and
    /// leaves implementations free to choose their worker/task topology when
    /// the bound server is driven.
    pub async fn bind(self, address: impl ToSocketAddrs) -> std::io::Result<BoundServer> {
        let mut allowed = ProtocolSet::empty();
        for protocol in self.rpc_protocols {
            allowed.insert(Protocol::from(protocol));
        }
        for version in self.http_versions {
            allowed.insert(match version {
                http::Version::Http1 => Protocol::Http1,
                http::Version::Http2 => Protocol::Http2,
            });
        }
        if allowed == ProtocolSet::empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "server must enable at least one RPC protocol or HTTP version",
            ));
        }

        let services = self.services.build();
        let mut acceptor = RpcAcceptor::with_options(
            services,
            RpcAcceptorOptions { allowed_protocols: allowed, ..RpcAcceptorOptions::default() },
        )
        .with_http(self.http_routes.build(), self.http_options);
        if let Some(builtins) = self.builtins {
            acceptor = acceptor.with_builtin(builtins);
        }
        let listener = super::acceptor::bind_tcp_listener(
            address,
            self.tcp_options.reuse_port,
            self.tcp_options.backlog,
        )
        .await?;
        let local_addr = listener.local_addr()?;
        let _ = self.observation;
        Ok(BoundServer {
            listener,
            local_addrs: Box::new([local_addr]),
            acceptor: Arc::new(acceptor),
            nodelay: self.tcp_options.nodelay,
            shutdown: CancellationToken::new(),
        })
    }

    /// Convenience equivalent to bind followed by BoundServer::serve.
    pub async fn serve(self, address: impl ToSocketAddrs) -> std::io::Result<()> {
        self.bind(address).await?.serve().await
    }

    /// Serves until `shutdown` resolves, then stops admitting new connections.
    pub async fn serve_with_shutdown<S>(
        self,
        address: impl ToSocketAddrs,
        shutdown: S,
    ) -> std::io::Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        self.bind(address).await?.serve_with_shutdown(shutdown).await
    }
}

/// A configured, bound server which has not yet started request admission.
///
/// The type owns listener resources but does not prescribe whether serving is
/// implemented with tasks, direct polling, per-core loops or another runtime
/// strategy.
pub struct BoundServer {
    listener: TcpListener,
    local_addrs: Box<[SocketAddr]>,
    acceptor: Arc<RpcAcceptor>,
    nodelay: bool,
    shutdown: CancellationToken,
}

impl BoundServer {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addrs[0]
    }

    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.local_addrs
    }

    pub fn handle(&self) -> ServerHandle {
        ServerHandle { shutdown: self.shutdown.clone() }
    }

    pub async fn serve(self) -> std::io::Result<()> {
        let shutdown = self.shutdown.clone();
        self.serve_until(shutdown.cancelled()).await
    }

    /// Stops admission when shutdown resolves. Existing protocol tasks retain
    /// ownership of their connections and finish independently.
    pub async fn serve_with_shutdown<S>(self, shutdown: S) -> std::io::Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        self.serve_until(shutdown).await
    }

    async fn serve_until<S>(self, shutdown_signal: S) -> std::io::Result<()>
    where
        S: Future<Output = ()>,
    {
        tokio::pin!(shutdown_signal);
        loop {
            let accepted = tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => break Ok(()),
                _ = &mut shutdown_signal => {
                    self.shutdown.cancel();
                    break Ok(());
                }
                accepted = self.listener.accept() => accepted,
            };
            let (stream, remote_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => break Err(error),
            };
            let local_addr = match stream.local_addr() {
                Ok(address) => address,
                Err(error) => break Err(error),
            };
            if self.nodelay
                && let Err(error) = stream.set_nodelay(true)
            {
                break Err(error);
            }
            if let Err(error) =
                self.acceptor.accept(AcceptedTcpSocket { stream, local_addr, remote_addr })
            {
                break Err(std::io::Error::other(error.to_string()));
            }
        }
    }
}

async fn to_internal_response<B>(response: Result<http::Response<B>, http::Error>) -> HttpResponse
where
    B: http::Body + Send + 'static,
{
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            log::warn!("HTTP handler failed: {error}");
            return HttpResponse::empty(http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let (parts, body) = response.into_parts();
    HttpResponse::streaming(parts.status, parts.headers, body)
}

fn to_public_request(request: HttpRequest) -> http::Request<http::Incoming> {
    let HttpRequest { method, version, path, query, headers, body, path_parameters, remote_addr } =
        request;
    let uri = query.map_or(path.clone(), |query| format!("{path}?{query}"));
    let mut request = http::Request::builder()
        .method(method)
        .version(version)
        .uri(uri)
        .body(body)
        .expect("validated HTTP server request must form a public request");
    *request.headers_mut() = headers;
    request
        .extensions_mut()
        .insert(http::ServerRequestInfo::new(remote_addr, path_parameters.into_iter().collect()));
    request
}

/// Cloneable control-plane handle for a bound or running server.
///
/// Signalling is synchronous and cheap; callers observe completion by awaiting
/// the owning serve future, which remains the sole owner of runtime resources.
#[derive(Clone)]
pub struct ServerHandle {
    shutdown: CancellationToken,
}

impl ServerHandle {
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::BufMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_stream::StreamExt;

    use super::*;
    use crate::descriptor::Idempotency;
    use crate::server::{
        ClientStreamingProstMethod, RegisterService, ServerStreamingProstMethod,
        ServiceRegistryBuilder,
    };
    use crate::{
        IoBufBuilder, MethodDescriptor, MethodKind, Request, Response, Status, StatusCode,
    };

    static LARGE_GRPC_STREAM: MethodDescriptor = MethodDescriptor::new(
        "test.Streaming",
        "Large",
        MethodKind::ServerStreaming,
        Idempotency::NoSideEffects,
    );

    static LARGE_GRPC_UPLOAD: MethodDescriptor = MethodDescriptor::new(
        "test.Streaming",
        "Upload",
        MethodKind::ClientStreaming,
        Idempotency::NoSideEffects,
    );

    static IMMEDIATE_GRPC_STREAM_ERROR: MethodDescriptor = MethodDescriptor::new(
        "test.Streaming",
        "ImmediateError",
        MethodKind::ServerStreaming,
        Idempotency::NoSideEffects,
    );

    static OK_GRPC_TRAILERS_ONLY: MethodDescriptor = MethodDescriptor::new(
        "test.Streaming",
        "EmptyTrailersOnly",
        MethodKind::ServerStreaming,
        Idempotency::NoSideEffects,
    );

    static DEFERRED_GRPC_STREAM_ERROR: MethodDescriptor = MethodDescriptor::new(
        "test.Streaming",
        "DeferredError",
        MethodKind::ServerStreaming,
        Idempotency::NoSideEffects,
    );

    #[derive(Clone, PartialEq, prost::Message)]
    struct EmptyMessage {}

    #[derive(Clone, PartialEq, prost::Message)]
    struct BlobMessage {
        #[prost(bytes = "vec", tag = "1")]
        data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct CountMessage {
        #[prost(uint64, tag = "1")]
        bytes: u64,
    }

    struct LargeStreamingService;

    impl RegisterService for LargeStreamingService {
        fn register(self, registry: &mut ServiceRegistryBuilder) {
            registry.register(
                &LARGE_GRPC_STREAM,
                ServerStreamingProstMethod::new(
                    Arc::new(()),
                    |_service: Arc<()>, _request: Request<EmptyMessage>| async move {
                        let messages = (0..128).map(|index| {
                            Ok(BlobMessage { data: vec![(index % 251) as u8; 32 * 1024] })
                        });
                        Ok(Response::new(tokio_stream::iter(messages)))
                    },
                ),
            );
            registry.register(
                &LARGE_GRPC_UPLOAD,
                ClientStreamingProstMethod::new(
                    Arc::new(()),
                    |_service: Arc<()>, request: Request<crate::Streaming<BlobMessage, Status>>| async move {
                        let mut stream = request.into_inner();
                        let mut bytes = 0;
                        while let Some(message) = stream.next().await {
                            bytes += message?.data.len() as u64;
                        }
                        Ok(Response::new(CountMessage { bytes }))
                    },
                ),
            );
        }
    }

    struct StreamingStatusService;

    impl RegisterService for StreamingStatusService {
        fn register(self, registry: &mut ServiceRegistryBuilder) {
            registry.register(
                &IMMEDIATE_GRPC_STREAM_ERROR,
                ServerStreamingProstMethod::new(
                    Arc::new(()),
                    |_service: Arc<()>, _request: Request<EmptyMessage>| async move {
                        let mut status =
                            Status::unknown("not root").with_details(b"root-2".to_vec());
                        status.metadata_mut().insert("leader", "node-2".to_owned()).unwrap();
                        Err::<Response<tokio_stream::Empty<Result<BlobMessage, Status>>>, _>(status)
                    },
                ),
            );
            registry.register(
                &OK_GRPC_TRAILERS_ONLY,
                ServerStreamingProstMethod::new(
                    Arc::new(()),
                    |_service: Arc<()>, _request: Request<EmptyMessage>| async move {
                        Err::<Response<tokio_stream::Empty<Result<BlobMessage, Status>>>, _>(
                            Status::new(StatusCode::Ok, ""),
                        )
                    },
                ),
            );
            registry.register(
                &DEFERRED_GRPC_STREAM_ERROR,
                ServerStreamingProstMethod::new(
                    Arc::new(()),
                    |_service: Arc<()>, _request: Request<EmptyMessage>| async move {
                        Ok(Response::new(tokio_stream::iter([Err::<BlobMessage, _>(
                            Status::unknown("stream failed"),
                        )])))
                    },
                ),
            );
        }
    }

    #[tokio::test]
    async fn high_level_server_routes_http_and_stops_from_handle() {
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http1])
            .route(http::Method::GET, "/hello/:name", |request| async move {
                let name = http::ServerRequestExt::path_param(&request, "name").unwrap();
                let mut body = IoBufBuilder::new();
                body.put_slice(b"hello ");
                body.put_slice(name.as_bytes());
                Ok(http::Response::new(http::Full::new(body.finish())))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /hello/world HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.windows(b"hello world".len()).any(|part| part == b"hello world"));

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn streaming_http_response_uses_chunked_owner_and_preserves_frames() {
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http1])
            .route(http::Method::GET, "/chunks", |_request| async move {
                let frames = tokio_stream::iter([
                    Ok::<_, http::Error>(http::Frame::Data(http_data(b"hello "))),
                    Ok(http::Frame::Data(http_data(b"world"))),
                    Ok(http::Frame::Trailers(http::HeaderMap::from_iter([(
                        ::http::HeaderName::from_static("x-check"),
                        ::http::HeaderValue::from_static("yes"),
                    )]))),
                ]);
                Ok(http::Response::new(http::Stream::new(frames)))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /chunks HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        for expected in [
            &b"transfer-encoding: chunked\r\n"[..],
            &b"6\r\nhello \r\n"[..],
            &b"5\r\nworld\r\n"[..],
            &b"x-check: yes\r\n\r\n"[..],
        ] {
            assert!(response.windows(expected.len()).any(|part| part == expected));
        }

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_client_and_server_exchange_without_http1_fallback() {
        use crate::http::BodyExt;

        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::GET, "/h2/:name", |request| async move {
                assert_eq!(request.version(), ::http::Version::HTTP_2);
                let name = http::ServerRequestExt::path_param(&request, "name").unwrap();
                Ok(http::Response::new(http::Full::new(http_data(
                    format!("hello {name}").as_bytes(),
                ))))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address)
            .connection_mode(crate::ConnectionMode::Short)
            .build()
            .unwrap();
        let request = ::http::Request::builder()
            .method(::http::Method::GET)
            .uri("/h2/world")
            .body(http::Empty::new())
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), client.request(request))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.version(), ::http::Version::HTTP_2);
        assert_eq!(response.status(), ::http::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap();
        assert!(body.chunks().flatten().copied().eq(b"hello world".iter().copied()));

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_start_detach_submits_without_polling_the_handle() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::POST, "/detached", move |_request| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::Release);
                    Ok(http::Response::new(http::Empty::new()))
                }
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let handle = server.handle();
        let address = server.local_addr();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address).build().unwrap();
        let request = ::http::Request::post("/detached").body(http::Empty::new()).unwrap();

        client.request(request).start().detach();
        tokio::time::timeout(Duration::from_secs(2), async {
            while calls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_session_reconnects_after_initial_connect_failure() {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let client = http::Client::http2(address).build().unwrap();
        let request = || ::http::Request::get("/retry").body(http::Empty::new()).unwrap();
        assert!(client.request(request()).await.is_err());

        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::GET, "/retry", |_request| async move {
                Ok(http::Response::new(http::Full::new(http_data(b"ok"))))
            })
            .bind(address)
            .await
            .unwrap();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());

        let response = tokio::time::timeout(Duration::from_secs(2), client.request(request()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), ::http::StatusCode::OK);

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_deadline_runs_without_polling_the_handle() {
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::GET, "/slow", |_request| async move {
                std::future::pending::<Result<http::Response<http::Empty>, http::Error>>().await
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let handle = server.handle();
        let address = server.local_addr();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address).build().unwrap();
        let request = ::http::Request::get("/slow").body(http::Empty::new()).unwrap();

        let call = client.request(request).timeout(Duration::from_millis(20)).start();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let Err(error) = call.await else { panic!("deadline must fail the HTTP/2 call") };
        assert!(
            matches!(error, http::Error::Protocol(message) if message.contains("DeadlineExceeded"))
        );

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_server_enforces_header_and_body_limits() {
        use crate::http::BodyExt;

        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .http_options(HttpServerOptions::default().max_header_size(128).max_body_size(4))
            .route(http::Method::POST, "/limited", |request| async move {
                let _ = request.into_body().collect().await;
                Ok(http::Response::new(http::Empty::new()))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let handle = server.handle();
        let address = server.local_addr();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address)
            .connection_mode(crate::ConnectionMode::Short)
            .build()
            .unwrap();

        let oversized_body =
            ::http::Request::post("/limited").body(http::Full::new(http_data(b"12345"))).unwrap();
        assert!(client.request(oversized_body).await.is_err());

        let oversized_header = ::http::Request::post("/limited")
            .header("x-large", "x".repeat(256))
            .body(http::Empty::new())
            .unwrap();
        assert!(client.request(oversized_header).await.is_err());

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_streams_request_and_response_frames() {
        use crate::http::BodyExt;

        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::POST, "/stream", |request| async move {
                let request_body = request.into_body().collect().await.unwrap();
                assert!(request_body.chunks().flatten().copied().eq(b"request".iter().copied()));
                let frames = tokio_stream::iter([
                    Ok::<_, http::Error>(http::Frame::Data(http_data(b"hello "))),
                    Ok(http::Frame::Data(http_data(b"h2"))),
                    Ok(http::Frame::Trailers(http::HeaderMap::from_iter([(
                        ::http::HeaderName::from_static("x-finished"),
                        ::http::HeaderValue::from_static("yes"),
                    )]))),
                ]);
                Ok(http::Response::new(http::Stream::new(frames)))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address).build().unwrap();
        let request_frames = tokio_stream::iter([
            Ok::<_, http::Error>(http::Frame::Data(http_data(b"req"))),
            Ok(http::Frame::Data(http_data(b"uest"))),
        ]);
        let request = ::http::Request::builder()
            .method(::http::Method::POST)
            .uri("/stream")
            .body(http::Stream::new(request_frames))
            .unwrap();

        let mut response = tokio::time::timeout(Duration::from_secs(2), client.request(request))
            .await
            .unwrap()
            .unwrap();
        let mut data = Vec::new();
        let mut trailers = None;
        while let Some(frame) = response.body_mut().frame().await.transpose().unwrap() {
            match frame {
                http::Frame::Data(buffer) => data.extend(buffer.chunks().flatten().copied()),
                http::Frame::Trailers(value) => trailers = Some(value),
            }
        }
        assert_eq!(data, b"hello h2");
        assert_eq!(
            trailers.unwrap().get("x-finished").unwrap(),
            ::http::HeaderValue::from_static("yes")
        );

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_streaming_response_crosses_flow_control_windows() {
        use crate::http::BodyExt;

        const CHUNKS: usize = 128;
        const CHUNK_SIZE: usize = 32 * 1024;
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::GET, "/large-stream", |_request| async move {
                let frames = (0..CHUNKS).map(|index| {
                    let data = vec![(index % 251) as u8; CHUNK_SIZE];
                    Ok::<_, http::Error>(http::Frame::Data(http_data(&data)))
                });
                Ok(http::Response::new(http::Stream::new(tokio_stream::iter(frames))))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address).build().unwrap();
        let request = ::http::Request::get("/large-stream").body(http::Empty::new()).unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), client.request(request))
            .await
            .unwrap()
            .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body.len(), CHUNKS * CHUNK_SIZE);

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_server_streaming_crosses_flow_control_windows() {
        let server = ServerBuilder::new()
            .rpc_protocols([RpcProtocol::Grpc])
            .http_versions([])
            .add_service(LargeStreamingService)
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let channel = crate::RpcChannel::grpc(address).build().unwrap();

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            channel.server_streaming::<_, BlobMessage>(
                &LARGE_GRPC_STREAM,
                Request::new(EmptyMessage {}),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let mut stream = response.into_inner();
        let mut bytes = 0;
        while let Some(message) =
            tokio::time::timeout(Duration::from_secs(5), stream.next()).await.unwrap()
        {
            bytes += message.unwrap().data.len();
        }
        assert_eq!(bytes, 128 * 32 * 1024);

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_streaming_distinguishes_trailers_only_from_deferred_errors() {
        let server = ServerBuilder::new()
            .rpc_protocols([RpcProtocol::Grpc])
            .http_versions([])
            .add_service(StreamingStatusService)
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let channel = crate::RpcChannel::grpc(address).build().unwrap();

        let immediate = channel
            .server_streaming::<_, BlobMessage>(
                &IMMEDIATE_GRPC_STREAM_ERROR,
                Request::new(EmptyMessage {}),
            )
            .await;
        let Err(error) = immediate else {
            panic!("a handler failure must fail while establishing the response stream")
        };
        let status = error.remote_status().expect("remote status");
        assert_eq!(status.code(), StatusCode::Unknown);
        assert_eq!(status.message(), "not root");
        assert_eq!(status.details(), b"root-2");
        assert_eq!(status.metadata().get("leader"), Some(b"node-2".as_slice()));

        let response = channel
            .server_streaming::<_, BlobMessage>(
                &OK_GRPC_TRAILERS_ONLY,
                Request::new(EmptyMessage {}),
            )
            .await
            .unwrap();
        let mut empty = response.into_inner();
        assert!(empty.next().await.is_none());
        assert!(empty.trailers().is_some());

        let response = channel
            .server_streaming::<_, BlobMessage>(
                &DEFERRED_GRPC_STREAM_ERROR,
                Request::new(EmptyMessage {}),
            )
            .await
            .unwrap();
        let mut deferred = response.into_inner();
        let Some(Err(error)) = deferred.next().await else {
            panic!("an error produced by an established stream must remain a stream item")
        };
        assert_eq!(error.remote_status().expect("remote status").message(), "stream failed");
        assert!(deferred.next().await.is_none());

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_client_streaming_crosses_flow_control_windows() {
        const MESSAGES: usize = 128;
        const MESSAGE_SIZE: usize = 32 * 1024;
        let server = ServerBuilder::new()
            .rpc_protocols([RpcProtocol::Grpc])
            .http_versions([])
            .add_service(LargeStreamingService)
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let channel = crate::RpcChannel::grpc(address).build().unwrap();
        let mut call =
            channel.client_streaming::<BlobMessage, CountMessage>(&LARGE_GRPC_UPLOAD).start();

        for index in 0..MESSAGES {
            tokio::time::timeout(
                Duration::from_secs(5),
                call.send(BlobMessage { data: vec![(index % 251) as u8; MESSAGE_SIZE] }),
            )
            .await
            .unwrap_or_else(|_| panic!("client-streaming send {index} timed out"))
            .unwrap();
        }
        let response =
            tokio::time::timeout(Duration::from_secs(5), call.finish()).await.unwrap().unwrap();
        assert_eq!(response.into_inner().bytes, (MESSAGES * MESSAGE_SIZE) as u64);

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn http2_early_response_does_not_wait_for_large_streaming_request() {
        use crate::http::BodyExt;

        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http2])
            .route(http::Method::POST, "/early", |_request| async move {
                Ok(http::Response::new(http::Full::new(http_data(b"accepted"))))
            })
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let client = http::Client::http2(address).build().unwrap();
        let frames = (0..16)
            .map(|_| Ok::<_, http::Error>(http::Frame::Data(http_data(&vec![b'x'; 16 * 1024]))));
        let request = ::http::Request::builder()
            .method(::http::Method::POST)
            .uri("/early")
            .body(http::Stream::new(tokio_stream::iter(frames)))
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), client.request(request))
            .await
            .unwrap()
            .unwrap();
        let body = response.into_body().collect().await.unwrap();
        assert!(body.chunks().flatten().copied().eq(b"accepted".iter().copied()));

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(2), serving).await.unwrap().unwrap().unwrap();
    }

    fn http_data(bytes: &[u8]) -> crate::IoBuf {
        let mut body = IoBufBuilder::new();
        body.put_slice(bytes);
        body.finish()
    }

    #[tokio::test]
    async fn empty_protocol_configuration_is_rejected() {
        let result =
            ServerBuilder::new().rpc_protocols([]).http_versions([]).bind("127.0.0.1:0").await;
        let Err(error) = result else { panic!("empty protocol set must be rejected") };
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_active_connections() {
        let server = ServerBuilder::new()
            .rpc_protocols([])
            .http_versions([http::Version::Http1])
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = server.local_addr();
        let handle = server.handle();
        let serving = tokio::spawn(server.serve());
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"G").await.unwrap();

        handle.shutdown();
        tokio::time::timeout(std::time::Duration::from_secs(2), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
