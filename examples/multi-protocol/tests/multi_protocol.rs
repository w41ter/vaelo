// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use demo::{
    ECHO_METHOD, EchoRequest, EchoResponse, brpc_echo, collect_iobuf, detached_call_count,
    http_echo_client, http_get, start_server,
};
use vaelo::http::{BodyExt, Frame};
use vaelo::{IoBufBuilder, RpcChannel};

const CLIENTS: usize = 16;
const CALLS_PER_CLIENT: usize = 16;
const MULTI_PROTOCOL_CALLS: usize = 8;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_server_handles_concurrent_brpc_and_http_echo_clients() {
    let server = start_server().await.unwrap();
    let address = server.address();

    let brpc = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for client in 0..CLIENTS {
            tasks.push(tokio::spawn(async move {
                for call in 0..CALLS_PER_CLIENT {
                    let message = format!("brpc-{client}-{call}");
                    let echoed = brpc_echo(address, message.clone()).await.unwrap();
                    assert_eq!(echoed, message);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });

    let http = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for client in 0..CLIENTS {
            tasks.push(tokio::spawn(async move {
                for call in 0..CALLS_PER_CLIENT {
                    let message = format!("http-{client}-{call}");
                    let echoed = http_echo_client(address, message.as_bytes()).await.unwrap();
                    assert_eq!(echoed, message.as_bytes());
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });

    tokio::time::timeout(TEST_TIMEOUT, async {
        brpc.await.unwrap();
        http.await.unwrap();
    })
    .await
    .unwrap();
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_server_handles_http1_http2_grpc_and_brpc_concurrently() {
    let server = start_server().await.unwrap();
    let address = server.address();
    let start = Arc::new(tokio::sync::Barrier::new(4));

    let brpc_channel = RpcChannel::brpc(address).build().unwrap();
    let brpc_start = start.clone();
    let brpc = tokio::spawn(async move {
        brpc_start.wait().await;
        for call in 0..MULTI_PROTOCOL_CALLS {
            let message = format!("brpc-{call}");
            let response = brpc_channel
                .call::<_, EchoResponse>(
                    &ECHO_METHOD,
                    EchoRequest { message: message.clone() }.into(),
                )
                .await
                .unwrap();
            assert_eq!(response.message, message);
        }
    });

    let grpc_channel = RpcChannel::grpc(address).build().unwrap();
    let grpc_start = start.clone();
    let grpc = tokio::spawn(async move {
        grpc_start.wait().await;
        for call in 0..MULTI_PROTOCOL_CALLS {
            let message = format!("grpc-{call}");
            let response = grpc_channel
                .call::<_, EchoResponse>(
                    &ECHO_METHOD,
                    EchoRequest { message: message.clone() }.into(),
                )
                .await
                .unwrap();
            assert_eq!(response.message, message);
        }
    });

    let http1_client = vaelo::http::Client::http1(address).build().unwrap();
    let http1_start = start.clone();
    let http1 = tokio::spawn(async move {
        http1_start.wait().await;
        for call in 0..MULTI_PROTOCOL_CALLS {
            assert_http_echo(&http1_client, http::Version::HTTP_11, format!("http1-{call}")).await;
        }
    });

    let http2_client = vaelo::http::Client::http2(address).build().unwrap();
    let http2 = tokio::spawn(async move {
        start.wait().await;
        for call in 0..MULTI_PROTOCOL_CALLS {
            assert_http_echo(&http2_client, http::Version::HTTP_2, format!("http2-{call}")).await;
        }
    });

    tokio::time::timeout(TEST_TIMEOUT, async {
        brpc.await.unwrap();
        grpc.await.unwrap();
        http1.await.unwrap();
        http2.await.unwrap();
    })
    .await
    .unwrap();
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builtin_services_expose_health_and_status() {
    let server = start_server().await.unwrap();
    let address = server.address();

    let health = http_get(address, "/health").await.unwrap();
    assert_eq!(health.status(), http::StatusCode::OK);
    assert_eq!(collect_iobuf(&health.into_body().collect().await.unwrap()), b"OK\n");

    let status = http_get(address, "/status").await.unwrap();
    assert_eq!(status.status(), http::StatusCode::OK);
    let body = status.into_body().collect().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&collect_iobuf(&body)).unwrap();
    assert_eq!(body["service_count"], 1);
    assert_eq!(body["method_count"], 1);
    assert_eq!(body["methods"][0]["service"], "demo.EchoService");
    assert_eq!(body["methods"][0]["method"], "Echo");

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_is_submitted_even_when_response_handle_is_dropped() {
    let server = start_server().await.unwrap();
    let address = server.address();
    let before = detached_call_count();

    let brpc = RpcChannel::brpc(address).build().unwrap();
    brpc.call::<_, EchoResponse>(
        &ECHO_METHOD,
        EchoRequest { message: "detached-probe-brpc".into() }.into(),
    )
    .start()
    .detach();

    let grpc = RpcChannel::grpc(address).build().unwrap();
    grpc.call::<_, EchoResponse>(
        &ECHO_METHOD,
        EchoRequest { message: "detached-probe-grpc".into() }.into(),
    )
    .start()
    .detach();

    tokio::time::timeout(TEST_TIMEOUT, async {
        while detached_call_count() < before + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_channel_decodes_response() {
    let server = start_server().await.unwrap();
    let grpc = RpcChannel::grpc(server.address()).build().unwrap();

    let response = grpc
        .call::<_, EchoResponse>(&ECHO_METHOD, EchoRequest { message: "hello-grpc".into() }.into())
        .await
        .unwrap();
    assert_eq!(response.message, "hello-grpc");
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_streaming_request_body_is_submitted_without_a_per_call_task() {
    let server = start_server().await.unwrap();
    let client = vaelo::http::Client::http1(server.address()).build().unwrap();
    let body = TestBody(VecDeque::from([http_data(b"stream-"), http_data(b"body")]));
    let request =
        http::Request::post("/echo").header(http::header::HOST, "localhost").body(body).unwrap();

    let response = client.request(request).await.unwrap();
    let response = response.into_body().collect().await.unwrap();
    assert_eq!(collect_iobuf(&response), b"stream-body");
    server.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunked_response_is_delivered_incrementally_with_trailers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (head_sent, head_received) = tokio::sync::oneshot::channel();
    let (continue_body, body_allowed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        head_sent.send(()).unwrap();
        body_allowed.await.unwrap();
        socket.write_all(b"6\r\nhello \r\n5\r\nworld\r\n0\r\nx-check: yes\r\n\r\n").await.unwrap();
    });

    let client = vaelo::http::Client::http1(address)
        .connection_mode(vaelo::ConnectionMode::Short)
        .build()
        .unwrap();
    let call = client.request(
        http::Request::get("/")
            .header(http::header::HOST, "localhost")
            .body(vaelo::http::Empty::new())
            .unwrap(),
    );
    let response = tokio::spawn(async move { call.await });
    head_received.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !response.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("response headers should complete before its body");
    assert!(response.is_finished(), "response headers should complete before its body");
    continue_body.send(()).unwrap();

    let mut body = response.await.unwrap().unwrap().into_body();
    let mut bytes = Vec::new();
    let mut trailer_seen = false;
    while let Some(frame) = body.frame().await {
        match frame.unwrap() {
            Frame::Data(data) => bytes.extend(data.chunks().flatten().copied()),
            Frame::Trailers(trailers) => {
                trailer_seen = trailers.get("x-check").is_some_and(|value| value == "yes");
            }
        }
    }
    assert_eq!(bytes, b"hello world");
    assert!(trailer_seen);
    server.await.unwrap();
}

fn http_data(bytes: &[u8]) -> vaelo::IoBuf {
    use bytes::BufMut;

    let mut builder = IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}

async fn assert_http_echo(
    client: &vaelo::http::Client,
    expected_version: http::Version,
    message: String,
) {
    let request = http::Request::post("/echo")
        .header(http::header::HOST, "localhost")
        .body(vaelo::http::Full::new(http_data(message.as_bytes())))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.version(), expected_version);
    let body = response.into_body().collect().await.unwrap();
    assert_eq!(collect_iobuf(&body), message.as_bytes());
}

struct TestBody(VecDeque<vaelo::IoBuf>);

impl vaelo::http::Body for TestBody {
    type Error = vaelo::http::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>> {
        Poll::Ready(self.get_mut().0.pop_front().map(|data| Ok(Frame::Data(data))))
    }
}
