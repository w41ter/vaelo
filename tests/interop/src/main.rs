// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{MetadataMap as TonicMetadataMap, MetadataValue as TonicMetadataValue};
use tonic::transport::{Endpoint, Server as TonicServer};
use tonic::{
    Code as TonicCode, Request as TonicRequest, Response as TonicResponse, Status as TonicStatus,
};
use vaelo::__private::{
    BidiStreamingProstMethod, ClientStreamingProstMethod, Idempotency, MethodDescriptor,
    MethodKind, Protocol, ProtocolSet, RegisterService, RpcAcceptor, Server,
    ServerStreamingProstMethod, ServiceRegistryBuilder, UnaryProstMethod,
};
use vaelo::{ConnectionTag, Request, Response, RpcChannel, Status, StatusCode};

const ERROR_PAYLOAD: &[u8] = b"interop-status-details";
const ERROR_DETAILS: &[u8] = b"\0\xfftest-structured-error";
const ERROR_MESSAGE: &str = "leader changed: 50% / 节点";
const METADATA_PAYLOAD: &[u8] = b"interop-binary-metadata";
const REQUEST_METADATA: &[u8] = b"\0\xffrequest-context";
const RESPONSE_METADATA: &[u8] = b"\xff\0response-context";
const LATE_RESPONSE_PAYLOAD: &[u8] = b"interop-late-response";
const EARLY_STREAM_RESPONSE_PAYLOAD: &[u8] = b"interop-early-stream-response";
const LONG_STREAM_PAYLOAD: &[u8] = b"interop-long-stream";
const STREAM_ERROR_PAYLOAD: &[u8] = b"interop-stream-error";

pub mod perf {
    tonic::include_proto!("perf");
}

use perf::echo_service_client::EchoServiceClient;
use perf::echo_service_server::{EchoService, EchoServiceServer};
use perf::{EchoRequest, EchoResponse};

static ECHO_METHOD: MethodDescriptor =
    MethodDescriptor::new("perf.EchoService", "Echo", MethodKind::Unary, Idempotency::Idempotent);

static EXPAND_METHOD: MethodDescriptor = MethodDescriptor::new(
    "perf.EchoService",
    "Expand",
    MethodKind::ServerStreaming,
    Idempotency::Idempotent,
);

static COLLECT_METHOD: MethodDescriptor = MethodDescriptor::new(
    "perf.EchoService",
    "Collect",
    MethodKind::ClientStreaming,
    Idempotency::Idempotent,
);

static CHAT_METHOD: MethodDescriptor = MethodDescriptor::new(
    "perf.EchoService",
    "Chat",
    MethodKind::BidirectionalStreaming,
    Idempotency::Idempotent,
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), run_checks()).await??;
    Ok(())
}

async fn run_checks() -> Result<(), Box<dyn std::error::Error>> {
    check_failed_grpc_connect_completes().await?;
    let tonic_server = start_tonic_server().await?;
    let vaelo_server = start_vaelo_server().await?;

    let payload = b"grpc interop payload".to_vec();
    check_vaelo_client_to_tonic_server(tonic_server.address, payload.clone()).await?;
    check_tonic_client_to_vaelo_server(vaelo_server.address, payload.clone()).await?;
    check_vaelo_client_reuses_h2_connection_to_tonic_server(
        tonic_server.address,
        &tonic_server.accepted_connections,
    )
    .await?;
    check_vaelo_channel_cache_eviction(tonic_server.address, &tonic_server.accepted_connections)
        .await?;
    check_tonic_client_uses_one_h2_connection_to_vaelo_server(
        vaelo_server.address,
        &vaelo_server.seen_remotes,
    )
    .await?;
    check_vaelo_client_multiplexes_h2_streams_to_tonic_server(
        tonic_server.address,
        &tonic_server.accepted_connections,
    )
    .await?;
    check_vaelo_server_streaming_client_to_tonic_server(tonic_server.address, payload.clone())
        .await?;
    check_vaelo_client_streaming_client_to_tonic_server(tonic_server.address, payload.clone())
        .await?;
    check_vaelo_bidi_streaming_client_to_tonic_server(tonic_server.address, payload.clone())
        .await?;
    check_tonic_client_multiplexes_h2_streams_to_vaelo_server(
        vaelo_server.address,
        &vaelo_server.seen_remotes,
    )
    .await?;
    check_tonic_server_streaming_client_to_vaelo_server(vaelo_server.address, payload.clone())
        .await?;
    check_tonic_client_streaming_client_to_vaelo_server(vaelo_server.address, payload.clone())
        .await?;
    check_tonic_bidi_streaming_client_to_vaelo_server(vaelo_server.address, payload).await?;
    check_vaelo_client_reads_tonic_status_details(tonic_server.address).await?;
    check_error_then_success_reuses_connection(tonic_server.address).await?;
    check_many_sequential_status_transitions(tonic_server.address).await?;
    check_sustained_concurrent_unary_calls(tonic_server.address).await?;
    check_unary_while_server_stream_remains_open(tonic_server.address).await?;
    check_vaelo_stream_errors_interleaved_with_unary(vaelo_server.address).await?;
    check_large_unary_requests_to_vaelo_server(vaelo_server.address).await?;
    check_tonic_client_reads_vaelo_status_details(vaelo_server.address).await?;
    check_vaelo_client_binary_metadata_with_tonic_server(tonic_server.address).await?;
    check_tonic_client_binary_metadata_with_vaelo_server(vaelo_server.address).await?;
    check_late_headers_after_timeout_do_not_break_connection(tonic_server.address).await?;
    check_client_streaming_observes_early_response(tonic_server.address).await?;

    vaelo_server.server.shutdown();
    println!(
        "grpc interop checks passed: unary/streaming both directions and connection reuse/multiplexing"
    );
    Ok(())
}

async fn check_failed_grpc_connect_completes() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    drop(listener);
    let channel = grpc_channel(address);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        channel.call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"connect failure".to_vec() }.into(),
        ),
    )
    .await
    .expect("failed gRPC connect must complete the call");
    assert!(result.is_err());

    let listener = TcpListener::bind(address).await?;
    let server = start_tonic_server_with_listener(listener)?;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        channel.call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"reconnected".to_vec() }.into(),
        ),
    )
    .await
    .expect("gRPC reconnect must complete")?;
    assert_eq!(response.payload, b"reconnected");
    drop(server);
    println!("vaelo failed gRPC connect completes and reconnects: ok");
    Ok(())
}

async fn check_vaelo_server_streaming_client_to_tonic_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let response = channel
        .server_streaming::<_, EchoResponse>(
            &EXPAND_METHOD,
            EchoRequest { payload: payload.clone() }.into(),
        )
        .timeout(Duration::from_secs(3))
        .await?;
    let mut stream = response.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
    }
    assert_eq!(actual, expanded_payloads(&payload));
    println!("vaelo server-streaming client -> tonic server: ok");
    Ok(())
}

async fn check_vaelo_client_streaming_client_to_tonic_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let requests = numbered_requests(&payload);
    let mut call = channel
        .client_streaming::<EchoRequest, EchoResponse>(&COLLECT_METHOD)
        .timeout(Duration::from_secs(3))
        .start();
    for request in requests.clone() {
        call.send(request).await?;
    }
    let response = call.finish().await?;
    assert_eq!(
        response.into_inner().payload,
        collect_payloads(requests.into_iter().map(|request| request.payload))
    );
    println!("vaelo client-streaming client -> tonic server: ok");
    Ok(())
}

async fn check_vaelo_bidi_streaming_client_to_tonic_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let requests = numbered_requests(&payload);
    let (mut sender, response) = channel
        .bidirectional_streaming::<EchoRequest, EchoResponse>(&CHAT_METHOD)
        .timeout(Duration::from_secs(3))
        .start()
        .into_parts();
    for request in requests.clone() {
        sender.send(request).await?;
    }
    sender.finish().await?;
    let mut stream = response.await?.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
    }
    let expected =
        requests.into_iter().map(|request| chat_payload(request.payload)).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    println!("vaelo bidi-streaming client -> tonic server: ok");
    Ok(())
}

struct TonicServerHandle {
    address: SocketAddr,
    accepted_connections: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TonicServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_tonic_server() -> Result<TonicServerHandle, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    start_tonic_server_with_listener(listener)
}

fn start_tonic_server_with_listener(
    listener: TcpListener,
) -> Result<TonicServerHandle, Box<dyn std::error::Error>> {
    let address = listener.local_addr()?;
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let incoming = CountingIncoming {
        inner: tokio_stream::wrappers::TcpListenerStream::new(listener),
        accepted_connections: accepted_connections.clone(),
    };
    let task = tokio::spawn(async move {
        let result = TonicServer::builder()
            .add_service(EchoServiceServer::new(TonicEchoService))
            .serve_with_incoming(incoming)
            .await;
        if let Err(error) = result {
            eprintln!("tonic server failed: {error}");
        }
    });
    Ok(TonicServerHandle { address, accepted_connections, task })
}

struct CountingIncoming {
    inner: tokio_stream::wrappers::TcpListenerStream,
    accepted_connections: Arc<AtomicUsize>,
}

impl tokio_stream::Stream for CountingIncoming {
    type Item = io::Result<tokio::net::TcpStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(_))) = &result {
            this.accepted_connections.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[derive(Default)]
struct TonicEchoService;

#[tonic::async_trait]
impl EchoService for TonicEchoService {
    type ExpandStream = ReceiverStream<Result<EchoResponse, TonicStatus>>;
    type ChatStream = ReceiverStream<Result<EchoResponse, TonicStatus>>;

    async fn echo(
        &self,
        request: TonicRequest<EchoRequest>,
    ) -> Result<TonicResponse<EchoResponse>, TonicStatus> {
        let request_metadata = request.metadata().clone();
        let payload = request.into_inner().payload;
        if payload == LATE_RESPONSE_PAYLOAD {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        if payload == ERROR_PAYLOAD {
            let mut metadata = TonicMetadataMap::new();
            metadata.insert_bin(
                "test-error-details-bin",
                TonicMetadataValue::from_bytes(ERROR_DETAILS),
            );
            return Err(TonicStatus::with_details_and_metadata(
                TonicCode::FailedPrecondition,
                ERROR_MESSAGE,
                ERROR_DETAILS.into(),
                metadata,
            ));
        }
        let mut response = TonicResponse::new(EchoResponse { payload });
        if response.get_ref().payload == METADATA_PAYLOAD {
            assert_eq!(
                request_metadata
                    .get_bin("request-context-bin")
                    .expect("binary request metadata")
                    .to_bytes()
                    .expect("valid binary request metadata"),
                REQUEST_METADATA,
            );
            response.metadata_mut().insert_bin(
                "response-context-bin",
                TonicMetadataValue::from_bytes(RESPONSE_METADATA),
            );
        }
        Ok(response)
    }

    async fn expand(
        &self,
        request: TonicRequest<EchoRequest>,
    ) -> Result<TonicResponse<Self::ExpandStream>, TonicStatus> {
        let payload = request.into_inner().payload;
        let (sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            if payload == LONG_STREAM_PAYLOAD {
                let _ = sender.send(Ok(EchoResponse { payload })).await;
                tokio::time::sleep(Duration::from_secs(10)).await;
                return;
            }
            for index in 0..3 {
                let mut chunk = payload.clone();
                chunk.push(index);
                if sender.send(Ok(EchoResponse { payload: chunk })).await.is_err() {
                    return;
                }
            }
        });
        Ok(TonicResponse::new(ReceiverStream::new(receiver)))
    }

    async fn collect(
        &self,
        request: TonicRequest<tonic::Streaming<EchoRequest>>,
    ) -> Result<TonicResponse<EchoResponse>, TonicStatus> {
        let mut stream = request.into_inner();
        let mut payloads = Vec::new();
        while let Some(message) = stream.message().await? {
            if message.payload == EARLY_STREAM_RESPONSE_PAYLOAD {
                return Ok(TonicResponse::new(EchoResponse { payload: message.payload }));
            }
            payloads.push(message.payload);
        }
        Ok(TonicResponse::new(EchoResponse { payload: collect_payloads(payloads) }))
    }

    async fn chat(
        &self,
        request: TonicRequest<tonic::Streaming<EchoRequest>>,
    ) -> Result<TonicResponse<Self::ChatStream>, TonicStatus> {
        let mut stream = request.into_inner();
        let (sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok(Some(request)) = stream.message().await {
                if sender
                    .send(Ok(EchoResponse { payload: chat_payload(request.payload) }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(TonicResponse::new(ReceiverStream::new(receiver)))
    }
}

struct VaeloServerHandle {
    server: Server,
    address: SocketAddr,
    seen_remotes: Arc<Mutex<HashSet<SocketAddr>>>,
}

async fn start_vaelo_server() -> Result<VaeloServerHandle, Box<dyn std::error::Error>> {
    let seen_remotes = Arc::new(Mutex::new(HashSet::new()));
    let services = ServiceRegistryBuilder::default()
        .add_service(VaeloEchoService { seen_remotes: seen_remotes.clone() })
        .build();
    let acceptor = RpcAcceptor::new(services).allow_protocols(ProtocolSet::only(Protocol::Http2));
    let mut server = Server::new();
    let listener = server.listen_tcp("127.0.0.1:0", acceptor).await?;
    let address = listener.local_addr();
    Ok(VaeloServerHandle { server, address, seen_remotes })
}

#[derive(Clone)]
struct VaeloEchoService {
    seen_remotes: Arc<Mutex<HashSet<SocketAddr>>>,
}

impl VaeloEchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        self.seen_remotes
            .lock()
            .expect("remote address set lock poisoned")
            .insert(request.remote_addr().expect("server request has a remote address"));
        let request_metadata = request.metadata().clone();
        let payload = request.into_inner().payload;
        if payload == ERROR_PAYLOAD {
            let mut status = Status::new(StatusCode::FailedPrecondition, ERROR_MESSAGE)
                .with_details(ERROR_DETAILS.to_vec());
            status
                .metadata_mut()
                .insert("test-error-details-bin", ERROR_DETAILS.to_vec())
                .expect("valid binary metadata key");
            return Err(status);
        }
        let mut response = Response::new(EchoResponse { payload });
        if response.get_ref().payload == METADATA_PAYLOAD {
            assert_eq!(request_metadata.get("request-context-bin"), Some(REQUEST_METADATA));
            response
                .metadata_mut()
                .insert("response-context-bin", RESPONSE_METADATA.to_vec())
                .expect("valid binary metadata key");
        }
        Ok(response)
    }

    async fn expand(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<
        Response<
            impl tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send + 'static + use<>,
        >,
        Status,
    > {
        if request.get_ref().payload == STREAM_ERROR_PAYLOAD {
            return Err(Status::unknown("stream rejected before body"));
        }
        self.seen_remotes
            .lock()
            .expect("remote address set lock poisoned")
            .insert(request.remote_addr().expect("server request has a remote address"));
        let request = request.into_inner();
        let mut responses = Vec::new();
        for index in 0..3 {
            let mut payload = request.payload.clone();
            payload.push(index);
            responses.push(Ok(EchoResponse { payload }));
        }
        Ok(Response::new(tokio_stream::iter(responses)))
    }

    async fn collect(
        &self,
        request: Request<vaelo::Streaming<EchoRequest, Status>>,
    ) -> Result<Response<EchoResponse>, Status> {
        self.seen_remotes
            .lock()
            .expect("remote address set lock poisoned")
            .insert(request.remote_addr().expect("server request has a remote address"));
        let mut requests = request.into_inner();
        let mut payloads = Vec::new();
        while let Some(request) = requests.next().await {
            payloads.push(request?.payload);
        }
        Ok(Response::new(EchoResponse { payload: collect_payloads(payloads) }))
    }

    async fn chat(
        &self,
        request: Request<vaelo::Streaming<EchoRequest, Status>>,
    ) -> Result<
        Response<
            impl tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send + 'static + use<>,
        >,
        Status,
    > {
        self.seen_remotes
            .lock()
            .expect("remote address set lock poisoned")
            .insert(request.remote_addr().expect("server request has a remote address"));
        let mut requests = request.into_inner();
        let mut responses = Vec::new();
        while let Some(request) = requests.next().await {
            responses.push(Ok(EchoResponse { payload: chat_payload(request?.payload) }));
        }
        Ok(Response::new(tokio_stream::iter(responses)))
    }
}

impl RegisterService for VaeloEchoService {
    fn register(self, registry: &mut ServiceRegistryBuilder) {
        let service = Arc::new(self);
        registry.register(
            &ECHO_METHOD,
            UnaryProstMethod::new(
                service.clone(),
                |service: Arc<VaeloEchoService>, request| async move {
                    service.echo(request).await
                },
            ),
        );
        registry.register(
            &EXPAND_METHOD,
            ServerStreamingProstMethod::new(
                service.clone(),
                |service: Arc<VaeloEchoService>, request| async move {
                    service.expand(request).await
                },
            ),
        );
        registry.register(
            &COLLECT_METHOD,
            ClientStreamingProstMethod::new(
                service.clone(),
                |service: Arc<VaeloEchoService>, request| async move {
                    service.collect(request).await
                },
            ),
        );
        registry.register(
            &CHAT_METHOD,
            BidiStreamingProstMethod::new(
                service,
                |service: Arc<VaeloEchoService>, request| async move {
                    service.chat(request).await
                },
            ),
        );
    }
}

async fn check_vaelo_client_to_tonic_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let request = EchoRequest { payload: payload.clone() };
    let response = channel
        .call::<_, EchoResponse>(&ECHO_METHOD, request.into())
        .timeout(Duration::from_secs(3))
        .await?
        .into_inner();

    assert_eq!(response.payload, payload);
    println!("vaelo client -> tonic server: ok");
    Ok(())
}

async fn check_vaelo_client_reads_tonic_status_details(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = grpc_channel(address)
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: ERROR_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(3))
        .await;
    let error = match result {
        Ok(_) => panic!("tonic service must return a structured error"),
        Err(error) => error,
    };
    let status = error.remote_status().expect("remote gRPC status");
    assert_eq!(status.code(), StatusCode::FailedPrecondition);
    assert_eq!(status.message(), ERROR_MESSAGE);
    assert_eq!(status.details(), ERROR_DETAILS);
    assert_eq!(status.metadata().get("test-error-details-bin"), Some(ERROR_DETAILS));
    println!("vaelo client reads tonic status details and metadata: ok");
    Ok(())
}

async fn check_error_then_success_reuses_connection(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let error = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: ERROR_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(1))
        .await;
    assert!(error.is_err());
    let response = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"after remote status".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(1))
        .await?;
    assert_eq!(response.payload, b"after remote status");
    println!("remote status followed by immediate connection reuse: ok");
    Ok(())
}

async fn check_many_sequential_status_transitions(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    for index in 0..6_000_u32 {
        let payload =
            if index % 17 == 0 { ERROR_PAYLOAD.to_vec() } else { index.to_be_bytes().to_vec() };
        let result = channel
            .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { payload: payload.clone() }.into())
            .timeout(Duration::from_secs(1))
            .await;
        if index % 17 == 0 {
            assert!(result.is_err());
        } else {
            assert_eq!(result?.payload, payload);
        }
    }
    println!("6000 sequential success/error RPC transitions on one H2 connection: ok");
    Ok(())
}

async fn check_sustained_concurrent_unary_calls(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..64_u32 {
        let channel = channel.clone();
        tasks.spawn(async move {
            for sequence in 0..200_u32 {
                let payload = [worker.to_be_bytes(), sequence.to_be_bytes()].concat();
                let response = channel
                    .call::<_, EchoResponse>(
                        &ECHO_METHOD,
                        EchoRequest { payload: payload.clone() }.into(),
                    )
                    .timeout(Duration::from_secs(2))
                    .await
                    .map_err(Status::from)?;
                if response.payload != payload {
                    return Err(Status::internal("concurrent echo payload mismatch"));
                }
            }
            Ok::<_, Status>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    println!("12800 sustained concurrent unary RPCs on one H2 connection: ok");
    Ok(())
}

async fn check_unary_while_server_stream_remains_open(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let response = channel
        .server_streaming::<_, EchoResponse>(
            &EXPAND_METHOD,
            EchoRequest { payload: LONG_STREAM_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    let mut stream = response.into_inner();
    assert_eq!(stream.message().await?.unwrap().payload, LONG_STREAM_PAYLOAD);
    let unary = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"beside long stream".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    assert_eq!(unary.payload, b"beside long stream");
    drop(stream);
    println!("unary RPC beside an open server stream on the same H2 connection: ok");
    Ok(())
}

async fn check_vaelo_stream_errors_interleaved_with_unary(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    for index in 0..100_u32 {
        let response = channel
            .server_streaming::<_, EchoResponse>(
                &EXPAND_METHOD,
                EchoRequest { payload: STREAM_ERROR_PAYLOAD.to_vec() }.into(),
            )
            .timeout(Duration::from_secs(2))
            .await;
        match response {
            Err(_) => {}
            Ok(response) => {
                let mut stream = response.into_inner();
                let terminal = stream.message().await;
                assert!(terminal.is_err(), "expected stream status, got {terminal:?}");
            }
        }
        let payload = index.to_be_bytes().to_vec();
        let unary = channel
            .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { payload: payload.clone() }.into())
            .timeout(Duration::from_secs(2))
            .await?;
        assert_eq!(unary.payload, payload);
    }
    println!("vaelo stream errors interleaved with unary reuse: ok");
    Ok(())
}

async fn check_large_unary_requests_to_vaelo_server(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    for index in 0..64_u32 {
        let mut payload = vec![0_u8; 128 * 1024];
        payload[..4].copy_from_slice(&index.to_be_bytes());
        let response = channel
            .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { payload: payload.clone() }.into())
            .timeout(Duration::from_secs(2))
            .await?;
        assert_eq!(response.payload, payload);
    }
    println!("64 large unary RPCs through vaelo server inbound backpressure: ok");
    Ok(())
}

#[allow(dead_code)] // Manual long-running diagnostic; intentionally excluded from the fast gate.
async fn check_connection_reuse_after_idle(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let first = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"before idle".to_vec() }.into(),
        )
        .await?;
    assert_eq!(first.payload, b"before idle");
    tokio::time::sleep(Duration::from_secs(65)).await;
    let second = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"after idle".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    assert_eq!(second.payload, b"after idle");
    println!("connection reuse after 65 seconds idle: ok");
    Ok(())
}

async fn check_tonic_client_reads_vaelo_status_details(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true)
        .connect()
        .await?;
    let error = EchoServiceClient::new(channel)
        .echo(EchoRequest { payload: ERROR_PAYLOAD.to_vec() })
        .await
        .expect_err("vaelo service must return a structured error");
    assert_eq!(error.code(), TonicCode::FailedPrecondition);
    assert_eq!(error.message(), ERROR_MESSAGE);
    assert_eq!(error.details(), ERROR_DETAILS);
    assert_eq!(
        error
            .metadata()
            .get_bin("test-error-details-bin")
            .expect("binary status metadata")
            .to_bytes()?,
        ERROR_DETAILS,
    );
    println!("tonic client reads vaelo status details and metadata: ok");
    Ok(())
}

async fn check_vaelo_client_binary_metadata_with_tonic_server(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = Request::new(EchoRequest { payload: METADATA_PAYLOAD.to_vec() });
    request.metadata_mut().insert("request-context-bin", REQUEST_METADATA.to_vec())?;
    let response = grpc_channel(address)
        .call::<_, EchoResponse>(&ECHO_METHOD, request)
        .timeout(Duration::from_secs(3))
        .await?;
    assert_eq!(response.metadata().get("response-context-bin"), Some(RESPONSE_METADATA));
    println!("vaelo client exchanges binary metadata with tonic server: ok");
    Ok(())
}

async fn check_tonic_client_binary_metadata_with_vaelo_server(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true)
        .connect()
        .await?;
    let mut request = TonicRequest::new(EchoRequest { payload: METADATA_PAYLOAD.to_vec() });
    request
        .metadata_mut()
        .insert_bin("request-context-bin", TonicMetadataValue::from_bytes(REQUEST_METADATA));
    let response = EchoServiceClient::new(channel).echo(request).await?;
    assert_eq!(
        response
            .metadata()
            .get_bin("response-context-bin")
            .expect("binary response metadata")
            .to_bytes()?,
        RESPONSE_METADATA,
    );
    println!("tonic client exchanges binary metadata with vaelo server: ok");
    Ok(())
}

async fn check_late_headers_after_timeout_do_not_break_connection(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(address);
    let result = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: LATE_RESPONSE_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_millis(2))
        .await;
    assert!(result.is_err(), "delayed response must miss the deadline");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"after late headers".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(1))
        .await?;
    assert_eq!(response.payload, b"after late headers");
    println!("late headers on a closed stream preserve the H2 connection: ok");
    Ok(())
}

async fn check_client_streaming_observes_early_response(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut sender, response) = grpc_channel(address)
        .client_streaming::<EchoRequest, EchoResponse>(&COLLECT_METHOD)
        .timeout(Duration::from_secs(1))
        .start()
        .into_parts();
    sender.send(EchoRequest { payload: EARLY_STREAM_RESPONSE_PAYLOAD.to_vec() }).await?;
    let response = tokio::time::timeout(Duration::from_secs(1), response).await??;
    assert_eq!(response.payload, EARLY_STREAM_RESPONSE_PAYLOAD);
    println!("client-streaming observes a server response before half-close: ok");
    Ok(())
}

async fn check_tonic_client_to_vaelo_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = EchoServiceClient::new(channel);
    let response = client.echo(EchoRequest { payload: payload.clone() }).await?;

    assert_eq!(response.into_inner().payload, payload);
    println!("tonic client -> vaelo server: ok");
    Ok(())
}

async fn check_vaelo_client_reuses_h2_connection_to_tonic_server(
    address: SocketAddr,
    accepted_connections: &AtomicUsize,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = accepted_connections.load(Ordering::Relaxed);
    let channel = grpc_channel(address);
    for index in 0..8 {
        let payload = format!("vaelo long connection stream {index}").into_bytes();
        let request = EchoRequest { payload: payload.clone() };
        let response = channel
            .call::<_, EchoResponse>(&ECHO_METHOD, request.into())
            .timeout(Duration::from_secs(3))
            .await?
            .into_inner();

        assert_eq!(response.payload, payload);
    }
    let accepted = accepted_connections.load(Ordering::Relaxed) - before;
    assert_eq!(accepted, 1, "vaelo channel should reuse one TCP/H2 connection");
    println!("vaelo client -> tonic server over one h2 connection: ok");
    Ok(())
}

async fn check_vaelo_channel_cache_eviction(
    address: SocketAddr,
    accepted_connections: &AtomicUsize,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = accepted_connections.load(Ordering::Relaxed);
    let channel = grpc_channel(address);
    for expected_connections in 1..=2 {
        let response = channel
            .call::<_, EchoResponse>(
                &ECHO_METHOD,
                EchoRequest { payload: b"cache eviction".to_vec() }.into(),
            )
            .timeout(Duration::from_secs(3))
            .await?;
        assert_eq!(response.payload, b"cache eviction");
        assert_eq!(accepted_connections.load(Ordering::Relaxed) - before, expected_connections,);
        channel.close_cached_connections();
    }
    println!("vaelo channel cache eviction opens a fresh connection: ok");
    Ok(())
}

async fn check_tonic_client_uses_one_h2_connection_to_vaelo_server(
    address: SocketAddr,
    seen_remotes: &Mutex<HashSet<SocketAddr>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = seen_remotes.lock().expect("remote address set lock poisoned").len();
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = EchoServiceClient::new(channel);

    for index in 0..8 {
        let payload = format!("tonic long connection stream {index}").into_bytes();
        let response = client.echo(EchoRequest { payload: payload.clone() }).await?;
        assert_eq!(response.into_inner().payload, payload);
    }
    let remotes = seen_remotes.lock().expect("remote address set lock poisoned").len() - before;
    assert_eq!(remotes, 1, "tonic channel should reuse one TCP/H2 connection");
    println!("tonic client -> vaelo server over one h2 connection: ok");
    Ok(())
}

async fn check_vaelo_client_multiplexes_h2_streams_to_tonic_server(
    address: SocketAddr,
    accepted_connections: &AtomicUsize,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = accepted_connections.load(Ordering::Relaxed);
    let channel = grpc_channel(address);
    let mut handles = Vec::new();

    for index in 0..8 {
        let channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let payload = format!("vaelo concurrent h2 stream {index}").into_bytes();
            let request = EchoRequest { payload: payload.clone() };
            let response = channel
                .call::<_, EchoResponse>(&ECHO_METHOD, request.into())
                .timeout(Duration::from_secs(3))
                .await?
                .into_inner();
            Ok::<_, Status>((response.payload, payload))
        }));
    }

    for handle in handles {
        let (actual, expected) = handle.await??;
        assert_eq!(actual, expected);
    }
    let accepted = accepted_connections.load(Ordering::Relaxed) - before;
    assert_eq!(accepted, 1, "vaelo channel should multiplex streams on one TCP/H2 connection");
    println!("vaelo client -> tonic server concurrent h2 streams: ok");
    Ok(())
}

async fn check_tonic_client_multiplexes_h2_streams_to_vaelo_server(
    address: SocketAddr,
    seen_remotes: &Mutex<HashSet<SocketAddr>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = seen_remotes.lock().expect("remote address set lock poisoned").len();
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut handles = Vec::new();

    for index in 0..8 {
        let channel = channel.clone();
        handles.push(tokio::spawn(async move {
            let payload = format!("tonic concurrent h2 stream {index}").into_bytes();
            let mut client = EchoServiceClient::new(channel);
            let response = client.echo(EchoRequest { payload: payload.clone() }).await?;
            Ok::<_, TonicStatus>((response.into_inner().payload, payload))
        }));
    }

    for handle in handles {
        let (actual, expected) = handle.await??;
        assert_eq!(actual, expected);
    }
    let remotes = seen_remotes.lock().expect("remote address set lock poisoned").len() - before;
    assert_eq!(remotes, 1, "tonic channel should multiplex streams on one TCP/H2 connection");
    println!("tonic client -> vaelo server concurrent h2 streams: ok");
    Ok(())
}

async fn check_tonic_server_streaming_client_to_vaelo_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = EchoServiceClient::new(channel);
    let response = client.expand(EchoRequest { payload: payload.clone() }).await?;
    let mut stream = response.into_inner();
    let mut messages = Vec::new();
    while let Some(message) = stream.message().await? {
        messages.push(message.payload);
    }

    assert_eq!(messages, expanded_payloads(&payload));
    println!("tonic server-streaming client -> vaelo server: ok");
    Ok(())
}

async fn check_tonic_client_streaming_client_to_vaelo_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = EchoServiceClient::new(channel);
    let requests = numbered_requests(&payload);
    let response = client.collect(tokio_stream::iter(requests.clone())).await?;

    assert_eq!(
        response.into_inner().payload,
        collect_payloads(requests.into_iter().map(|request| request.payload))
    );
    println!("tonic client-streaming client -> vaelo server: ok");
    Ok(())
}

async fn check_tonic_bidi_streaming_client_to_vaelo_server(
    address: SocketAddr,
    payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = EchoServiceClient::new(channel);
    let requests = numbered_requests(&payload);
    let response = client.chat(tokio_stream::iter(requests.clone())).await?;
    let mut stream = response.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
    }

    let expected: Vec<_> =
        requests.into_iter().map(|request| chat_payload(request.payload)).collect();
    assert_eq!(actual, expected);
    println!("tonic bidi-streaming client -> vaelo server: ok");
    Ok(())
}

fn expanded_payloads(payload: &[u8]) -> Vec<Vec<u8>> {
    (0..3)
        .map(|index| {
            let mut chunk = payload.to_vec();
            chunk.push(index);
            chunk
        })
        .collect()
}

fn numbered_requests(payload: &[u8]) -> Vec<EchoRequest> {
    expanded_payloads(payload).into_iter().map(|payload| EchoRequest { payload }).collect()
}

fn collect_payloads(payloads: impl IntoIterator<Item = Vec<u8>>) -> Vec<u8> {
    let mut output = Vec::new();
    for (index, payload) in payloads.into_iter().enumerate() {
        if index > 0 {
            output.push(b'|');
        }
        output.extend(payload);
    }
    output
}

fn chat_payload(payload: Vec<u8>) -> Vec<u8> {
    let mut output = b"chat:".to_vec();
    output.extend(payload);
    output
}

fn grpc_channel(address: SocketAddr) -> RpcChannel {
    static NEXT_TAG: AtomicUsize = AtomicUsize::new(1);
    RpcChannel::grpc(address)
        .connection_tag(ConnectionTag::new(NEXT_TAG.fetch_add(1, Ordering::Relaxed) as u64))
        .build()
        .expect("valid gRPC test channel")
}
