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
use tonic::codec::CompressionEncoding;
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
const SLOW_STREAM_PAYLOAD: &[u8] = b"interop-slow-stream";
const STREAM_ERROR_PAYLOAD: &[u8] = b"interop-stream-error";
const PARTIAL_STREAM_ERROR_PAYLOAD: &[u8] = b"interop-partial-stream-error";
const DEADLINE_PAYLOAD: &[u8] = b"interop-deadline";
const ASCII_METADATA_PAYLOAD: &[u8] = b"interop-ascii-metadata";
const SUCCESS_TRAILERS_PAYLOAD: &[u8] = b"interop-success-trailers";
const OVERSIZED_RESPONSE_PAYLOAD: &[u8] = b"interop-oversized-response";
const ASCII_METADATA_KEY: &str = "test-ascii-metadata";
const TRAILING_METADATA_KEY: &str = "test-trailer";

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
    check_stream_cancellation_preserves_connections(
        tonic_server.address,
        &tonic_server.cancellations_seen,
        vaelo_server.address,
        &vaelo_server.cancellations_seen,
    )
    .await?;
    check_slow_consumers_bidirectional(tonic_server.address, vaelo_server.address).await?;
    check_empty_messages_and_streams(tonic_server.address, vaelo_server.address).await?;
    check_large_messages_bidirectional(tonic_server.address, vaelo_server.address).await?;
    check_message_size_limits(tonic_server.address, vaelo_server.address).await?;
    check_deadlines_bidirectional(
        tonic_server.address,
        &tonic_server.deadlines_seen,
        vaelo_server.address,
        &vaelo_server.deadlines_seen,
    )
    .await?;
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
    check_ascii_duplicate_metadata_bidirectional(tonic_server.address, vaelo_server.address)
        .await?;
    check_tonic_client_reads_vaelo_success_trailers(vaelo_server.address).await?;
    check_partial_stream_errors_bidirectional(tonic_server.address, vaelo_server.address).await?;
    check_tonic_compression_is_rejected_without_poisoning_connection(vaelo_server.address).await?;
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
    deadlines_seen: Arc<AtomicUsize>,
    cancellations_seen: Arc<AtomicUsize>,
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
    let deadlines_seen = Arc::new(AtomicUsize::new(0));
    let cancellations_seen = Arc::new(AtomicUsize::new(0));
    let incoming = CountingIncoming {
        inner: tokio_stream::wrappers::TcpListenerStream::new(listener),
        accepted_connections: accepted_connections.clone(),
    };
    let service_deadlines_seen = deadlines_seen.clone();
    let service_cancellations_seen = cancellations_seen.clone();
    let task = tokio::spawn(async move {
        let result = TonicServer::builder()
            .add_service(EchoServiceServer::new(TonicEchoService {
                deadlines_seen: service_deadlines_seen,
                cancellations_seen: service_cancellations_seen,
            }))
            .serve_with_incoming(incoming)
            .await;
        if let Err(error) = result {
            eprintln!("tonic server failed: {error}");
        }
    });
    Ok(TonicServerHandle {
        address,
        accepted_connections,
        deadlines_seen,
        cancellations_seen,
        task,
    })
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

struct TonicEchoService {
    deadlines_seen: Arc<AtomicUsize>,
    cancellations_seen: Arc<AtomicUsize>,
}

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
        if payload == DEADLINE_PAYLOAD {
            if request_metadata.get("grpc-timeout").is_some() {
                self.deadlines_seen.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
        let payload = if payload == OVERSIZED_RESPONSE_PAYLOAD {
            vec![b'x'; 4 * 1024 * 1024 + 1]
        } else {
            payload
        };
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
        if response.get_ref().payload == ASCII_METADATA_PAYLOAD {
            let actual = request_metadata
                .get_all(ASCII_METADATA_KEY)
                .iter()
                .map(|value| value.to_str().expect("ASCII request metadata"))
                .collect::<Vec<_>>();
            assert_eq!(actual, ["first", "second"]);
            response.metadata_mut().append(ASCII_METADATA_KEY, "first-response".parse().unwrap());
            response.metadata_mut().append(ASCII_METADATA_KEY, "second-response".parse().unwrap());
        }
        Ok(response)
    }

    async fn expand(
        &self,
        request: TonicRequest<EchoRequest>,
    ) -> Result<TonicResponse<Self::ExpandStream>, TonicStatus> {
        let payload = request.into_inner().payload;
        let cancellations_seen = self.cancellations_seen.clone();
        let (sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            if payload == LONG_STREAM_PAYLOAD {
                if sender.send(Ok(EchoResponse { payload })).await.is_ok() {
                    sender.closed().await;
                    cancellations_seen.fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
            if payload == PARTIAL_STREAM_ERROR_PAYLOAD {
                let _ = sender.send(Ok(EchoResponse { payload: b"before-error".to_vec() })).await;
                let mut metadata = TonicMetadataMap::new();
                metadata.insert(TRAILING_METADATA_KEY, "tonic-error-trailer".parse().unwrap());
                let _ = sender
                    .send(Err(TonicStatus::with_metadata(
                        TonicCode::DataLoss,
                        "tonic stream failed after one message",
                        metadata,
                    )))
                    .await;
                return;
            }
            if payload == SLOW_STREAM_PAYLOAD {
                for payload in slow_stream_payloads() {
                    if sender.send(Ok(EchoResponse { payload })).await.is_err() {
                        return;
                    }
                }
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
    deadlines_seen: Arc<AtomicUsize>,
    cancellations_seen: Arc<AtomicUsize>,
}

async fn start_vaelo_server() -> Result<VaeloServerHandle, Box<dyn std::error::Error>> {
    let seen_remotes = Arc::new(Mutex::new(HashSet::new()));
    let deadlines_seen = Arc::new(AtomicUsize::new(0));
    let cancellations_seen = Arc::new(AtomicUsize::new(0));
    let services = ServiceRegistryBuilder::default()
        .add_service(VaeloEchoService {
            seen_remotes: seen_remotes.clone(),
            deadlines_seen: deadlines_seen.clone(),
            cancellations_seen: cancellations_seen.clone(),
        })
        .build();
    let acceptor = RpcAcceptor::new(services).allow_protocols(ProtocolSet::only(Protocol::Http2));
    let mut server = Server::new();
    let listener = server.listen_tcp("127.0.0.1:0", acceptor).await?;
    let address = listener.local_addr();
    Ok(VaeloServerHandle { server, address, seen_remotes, deadlines_seen, cancellations_seen })
}

#[derive(Clone)]
struct VaeloEchoService {
    seen_remotes: Arc<Mutex<HashSet<SocketAddr>>>,
    deadlines_seen: Arc<AtomicUsize>,
    cancellations_seen: Arc<AtomicUsize>,
}

struct CancellationObservedStream {
    first: Option<EchoResponse>,
    cancellations_seen: Arc<AtomicUsize>,
}

impl tokio_stream::Stream for CancellationObservedStream {
    type Item = Result<EchoResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.first.take() {
            Some(first) => Poll::Ready(Some(Ok(first))),
            None => Poll::Pending,
        }
    }
}

impl Drop for CancellationObservedStream {
    fn drop(&mut self) {
        self.cancellations_seen.fetch_add(1, Ordering::Relaxed);
    }
}

impl VaeloEchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        self.seen_remotes
            .lock()
            .expect("remote address set lock poisoned")
            .insert(request.remote_addr().expect("server request has a remote address"));
        let (context, request) = request.into_parts();
        let request_metadata = context.metadata().clone();
        let payload = request.payload;
        if payload == DEADLINE_PAYLOAD {
            assert!(context.deadline().is_some(), "tonic deadline must reach vaelo service");
            self.deadlines_seen.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
        if response.get_ref().payload == ASCII_METADATA_PAYLOAD {
            assert_eq!(
                request_metadata.get_all(ASCII_METADATA_KEY).collect::<Vec<_>>(),
                [b"first".as_slice(), b"second".as_slice()]
            );
            response
                .metadata_mut()
                .append(ASCII_METADATA_KEY, b"first-response".to_vec())
                .expect("valid ASCII metadata key");
            response
                .metadata_mut()
                .append(ASCII_METADATA_KEY, b"second-response".to_vec())
                .expect("valid ASCII metadata key");
        }
        Ok(response)
    }

    async fn expand(
        &self,
        request: Request<EchoRequest>,
    ) -> Result<
        Response<Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send>>>,
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
        if request.payload == LONG_STREAM_PAYLOAD {
            return Ok(Response::new(Box::pin(CancellationObservedStream {
                first: Some(EchoResponse { payload: request.payload }),
                cancellations_seen: self.cancellations_seen.clone(),
            })));
        }
        if request.payload == PARTIAL_STREAM_ERROR_PAYLOAD {
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![
                Ok(EchoResponse { payload: b"before-error".to_vec() }),
                Err(Status::new(StatusCode::DataLoss, "vaelo stream failed after one message")),
            ]))));
        }
        if request.payload == SLOW_STREAM_PAYLOAD {
            let responses = slow_stream_payloads()
                .into_iter()
                .map(|payload| Ok(EchoResponse { payload }))
                .collect::<Vec<_>>();
            return Ok(Response::new(Box::pin(tokio_stream::iter(responses))));
        }
        let mut responses = Vec::new();
        for index in 0..3 {
            let mut payload = request.payload.clone();
            payload.push(index);
            responses.push(Ok(EchoResponse { payload }));
        }
        let mut response = Response::new(Box::pin(tokio_stream::iter(responses))
            as Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoResponse, Status>> + Send>>);
        if request.payload == SUCCESS_TRAILERS_PAYLOAD {
            response
                .trailers_mut()
                .append(TRAILING_METADATA_KEY, b"vaelo-success-trailer".to_vec())
                .expect("valid trailing metadata key");
        }
        Ok(response)
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

async fn check_stream_cancellation_preserves_connections(
    tonic_address: SocketAddr,
    tonic_cancellations_seen: &AtomicUsize,
    vaelo_address: SocketAddr,
    vaelo_cancellations_seen: &AtomicUsize,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = tonic_cancellations_seen.load(Ordering::Relaxed);
    let channel = grpc_channel(tonic_address);
    let response = channel
        .server_streaming::<_, EchoResponse>(
            &EXPAND_METHOD,
            EchoRequest { payload: LONG_STREAM_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    let mut stream = response.into_inner();
    assert_eq!(stream.message().await?.unwrap().payload, LONG_STREAM_PAYLOAD);
    drop(stream);
    wait_for_counter(tonic_cancellations_seen, before + 1).await?;
    let response = channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"after tonic stream cancellation".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    assert_eq!(response.payload, b"after tonic stream cancellation");

    let before = vaelo_cancellations_seen.load(Ordering::Relaxed);
    let channel = tonic_channel(vaelo_address).await?;
    let response = EchoServiceClient::new(channel.clone())
        .expand(EchoRequest { payload: LONG_STREAM_PAYLOAD.to_vec() })
        .await?;
    let mut stream = response.into_inner();
    assert_eq!(stream.message().await?.unwrap().payload, LONG_STREAM_PAYLOAD);
    drop(stream);
    wait_for_counter(vaelo_cancellations_seen, before + 1).await?;
    let response = EchoServiceClient::new(channel)
        .echo(EchoRequest { payload: b"after vaelo stream cancellation".to_vec() })
        .await?;
    assert_eq!(response.into_inner().payload, b"after vaelo stream cancellation");
    println!(
        "stream cancellation releases server work and preserves connections both directions: ok"
    );
    Ok(())
}

async fn check_slow_consumers_bidirectional(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = slow_stream_payloads();
    let response = grpc_channel(tonic_address)
        .server_streaming::<_, EchoResponse>(
            &EXPAND_METHOD,
            EchoRequest { payload: SLOW_STREAM_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(5))
        .await?;
    let mut stream = response.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(actual, expected);

    let response = EchoServiceClient::new(tonic_channel(vaelo_address).await?)
        .expand(EchoRequest { payload: SLOW_STREAM_PAYLOAD.to_vec() })
        .await?;
    let mut stream = response.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(actual, expected);
    println!("slow consumers preserve ordered streaming data both directions: ok");
    Ok(())
}

async fn wait_for_counter(
    counter: &AtomicUsize,
    expected: usize,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::Relaxed) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
}

async fn check_empty_messages_and_streams(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = grpc_channel(tonic_address)
        .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { payload: Vec::new() }.into())
        .timeout(Duration::from_secs(2))
        .await?;
    assert!(response.payload.is_empty());

    let channel = tonic_channel(vaelo_address).await?;
    let response =
        EchoServiceClient::new(channel.clone()).echo(EchoRequest { payload: Vec::new() }).await?;
    assert!(response.into_inner().payload.is_empty());

    let call = grpc_channel(tonic_address)
        .client_streaming::<EchoRequest, EchoResponse>(&COLLECT_METHOD)
        .timeout(Duration::from_secs(2))
        .start();
    let response = call.finish().await?;
    assert!(response.payload.is_empty());

    let response = EchoServiceClient::new(channel.clone())
        .collect(tokio_stream::empty::<EchoRequest>())
        .await?;
    assert!(response.into_inner().payload.is_empty());

    let (mut sender, response) = grpc_channel(tonic_address)
        .bidirectional_streaming::<EchoRequest, EchoResponse>(&CHAT_METHOD)
        .timeout(Duration::from_secs(2))
        .start()
        .into_parts();
    sender.finish().await?;
    let mut stream = response.await?.into_inner();
    assert!(stream.message().await?.is_none());

    let response =
        EchoServiceClient::new(channel).chat(tokio_stream::empty::<EchoRequest>()).await?;
    let mut stream = response.into_inner();
    assert!(stream.message().await?.is_none());
    println!("empty unary messages and empty client/bidi streams interoperate: ok");
    Ok(())
}

async fn check_large_messages_bidirectional(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = vec![b'l'; 256 * 1024];
    let response = grpc_channel(tonic_address)
        .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { payload: payload.clone() }.into())
        .timeout(Duration::from_secs(3))
        .await?;
    assert_eq!(response.payload, payload);

    let channel = tonic_channel(vaelo_address).await?;
    let response = EchoServiceClient::new(channel.clone())
        .echo(EchoRequest { payload: payload.clone() })
        .await?;
    assert_eq!(response.into_inner().payload, payload);

    let response = grpc_channel(tonic_address)
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

    let response =
        EchoServiceClient::new(channel).expand(EchoRequest { payload: payload.clone() }).await?;
    let mut stream = response.into_inner();
    let mut actual = Vec::new();
    while let Some(message) = stream.message().await? {
        actual.push(message.payload);
    }
    assert_eq!(actual, expanded_payloads(&payload));
    println!("messages larger than the initial H2 window interoperate both directions: ok");
    Ok(())
}

async fn check_message_size_limits(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = tonic_channel(vaelo_address).await?;
    let oversized = vec![b'x'; 4 * 1024 * 1024 + 1];
    let error = EchoServiceClient::new(channel.clone())
        .max_encoding_message_size(8 * 1024 * 1024)
        .echo(EchoRequest { payload: oversized })
        .await
        .expect_err("vaelo must reject a request over its decoding limit");
    assert_eq!(error.code(), TonicCode::ResourceExhausted);
    let response = EchoServiceClient::new(channel)
        .echo(EchoRequest { payload: b"after oversized request".to_vec() })
        .await?;
    assert_eq!(response.into_inner().payload, b"after oversized request");

    let vaelo_channel = grpc_channel(tonic_address);
    let error = match vaelo_channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: OVERSIZED_RESPONSE_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(3))
        .await
    {
        Ok(_) => panic!("vaelo must reject a response over its decoding limit"),
        Err(error) => error,
    };
    assert_eq!(error.remote_status().map(Status::code), Some(StatusCode::ResourceExhausted));
    let response = vaelo_channel
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: b"after oversized response".to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    assert_eq!(response.payload, b"after oversized response");
    println!("message limits return ResourceExhausted without poisoning H2 connections: ok");
    Ok(())
}

async fn check_deadlines_bidirectional(
    tonic_address: SocketAddr,
    tonic_deadlines_seen: &AtomicUsize,
    vaelo_address: SocketAddr,
    vaelo_deadlines_seen: &AtomicUsize,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = tonic_deadlines_seen.load(Ordering::Relaxed);
    let error = match grpc_channel(tonic_address)
        .call::<_, EchoResponse>(
            &ECHO_METHOD,
            EchoRequest { payload: DEADLINE_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_millis(40))
        .await
    {
        Ok(_) => panic!("vaelo deadline must expire"),
        Err(error) => error,
    };
    assert!(
        error.is_timeout()
            || error.remote_status().is_some_and(|status| matches!(
                status.code(),
                StatusCode::Canceled | StatusCode::DeadlineExceeded
            )),
        "unexpected vaelo deadline error: {error:?}"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while tonic_deadlines_seen.load(Ordering::Relaxed) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("tonic server must observe vaelo grpc-timeout metadata");

    let before = vaelo_deadlines_seen.load(Ordering::Relaxed);
    let channel = tonic_channel(vaelo_address).await?;
    let mut request = TonicRequest::new(EchoRequest { payload: DEADLINE_PAYLOAD.to_vec() });
    request.set_timeout(Duration::from_millis(40));
    let error = EchoServiceClient::new(channel.clone())
        .echo(request)
        .await
        .expect_err("vaelo server must enforce tonic deadline");
    assert!(matches!(error.code(), TonicCode::Cancelled | TonicCode::DeadlineExceeded));
    assert_eq!(vaelo_deadlines_seen.load(Ordering::Relaxed), before + 1);
    let response = EchoServiceClient::new(channel)
        .echo(EchoRequest { payload: b"after deadline".to_vec() })
        .await?;
    assert_eq!(response.into_inner().payload, b"after deadline");
    println!("grpc-timeout and DeadlineExceeded interoperate both directions: ok");
    Ok(())
}

async fn check_ascii_duplicate_metadata_bidirectional(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = Request::new(EchoRequest { payload: ASCII_METADATA_PAYLOAD.to_vec() });
    request.metadata_mut().append(ASCII_METADATA_KEY, b"first".to_vec())?;
    request.metadata_mut().append(ASCII_METADATA_KEY, b"second".to_vec())?;
    let response = grpc_channel(tonic_address)
        .call::<_, EchoResponse>(&ECHO_METHOD, request)
        .timeout(Duration::from_secs(2))
        .await?;
    assert_eq!(
        response.metadata().get_all(ASCII_METADATA_KEY).collect::<Vec<_>>(),
        [b"first-response".as_slice(), b"second-response".as_slice()]
    );

    let mut request = TonicRequest::new(EchoRequest { payload: ASCII_METADATA_PAYLOAD.to_vec() });
    request.metadata_mut().append(ASCII_METADATA_KEY, "first".parse()?);
    request.metadata_mut().append(ASCII_METADATA_KEY, "second".parse()?);
    let response =
        EchoServiceClient::new(tonic_channel(vaelo_address).await?).echo(request).await?;
    let actual = response
        .metadata()
        .get_all(ASCII_METADATA_KEY)
        .iter()
        .map(|value| value.to_str())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(actual, ["first-response", "second-response"]);
    println!("duplicate ASCII metadata preserves values and order both directions: ok");
    Ok(())
}

async fn check_tonic_client_reads_vaelo_success_trailers(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = EchoServiceClient::new(tonic_channel(address).await?)
        .expand(EchoRequest { payload: SUCCESS_TRAILERS_PAYLOAD.to_vec() })
        .await?;
    let mut stream = response.into_inner();
    while stream.message().await?.is_some() {}
    let trailers = stream.trailers().await?.expect("successful stream trailers");
    assert_eq!(
        trailers.get(TRAILING_METADATA_KEY).expect("vaelo success trailer").to_str()?,
        "vaelo-success-trailer"
    );
    println!("tonic client reads vaelo successful trailing metadata: ok");
    Ok(())
}

async fn check_partial_stream_errors_bidirectional(
    tonic_address: SocketAddr,
    vaelo_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = grpc_channel(tonic_address);
    let response = channel
        .server_streaming::<_, EchoResponse>(
            &EXPAND_METHOD,
            EchoRequest { payload: PARTIAL_STREAM_ERROR_PAYLOAD.to_vec() }.into(),
        )
        .timeout(Duration::from_secs(2))
        .await?;
    let mut stream = response.into_inner();
    assert_eq!(stream.message().await?.unwrap().payload, b"before-error");
    let error = stream.message().await.expect_err("tonic stream must end with DataLoss");
    let status = error.remote_status().expect("remote tonic stream status");
    assert_eq!(status.code(), StatusCode::DataLoss);
    assert_eq!(
        status.metadata().get(TRAILING_METADATA_KEY),
        Some(b"tonic-error-trailer".as_slice())
    );

    let tonic_channel = tonic_channel(vaelo_address).await?;
    let response = EchoServiceClient::new(tonic_channel.clone())
        .expand(EchoRequest { payload: PARTIAL_STREAM_ERROR_PAYLOAD.to_vec() })
        .await?;
    let mut stream = response.into_inner();
    assert_eq!(stream.message().await?.unwrap().payload, b"before-error");
    let error = stream.message().await.expect_err("vaelo stream must end with DataLoss");
    assert_eq!(error.code(), TonicCode::DataLoss);
    let response = EchoServiceClient::new(tonic_channel)
        .echo(EchoRequest { payload: b"after partial stream errors".to_vec() })
        .await?;
    assert_eq!(response.into_inner().payload, b"after partial stream errors");
    println!("partial stream success followed by non-OK trailers interoperates: ok");
    Ok(())
}

async fn check_tonic_compression_is_rejected_without_poisoning_connection(
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = tonic_channel(address).await?;
    let error = EchoServiceClient::new(channel.clone())
        .send_compressed(CompressionEncoding::Gzip)
        .echo(EchoRequest { payload: b"compressed request".to_vec() })
        .await
        .expect_err("vaelo does not currently support compressed gRPC messages");
    assert_eq!(error.code(), TonicCode::Unimplemented);
    let response = EchoServiceClient::new(channel)
        .echo(EchoRequest { payload: b"after rejected compression".to_vec() })
        .await?;
    assert_eq!(response.into_inner().payload, b"after rejected compression");
    println!("unsupported tonic compression is rejected without poisoning the connection: ok");
    Ok(())
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

fn slow_stream_payloads() -> Vec<Vec<u8>> {
    (0..64_u32)
        .map(|index| {
            let mut payload = vec![0; 32 * 1024];
            payload[..4].copy_from_slice(&index.to_be_bytes());
            payload
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

async fn tonic_channel(
    address: SocketAddr,
) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3))
        .tcp_nodelay(true)
        .connect()
        .await
}
