// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BufMut;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use super::*;
use crate::transport::inflight::{CallContext, CallToken};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

struct CountingOnConnected {
    count: Arc<AtomicUsize>,
}

impl OnConnected for CountingOnConnected {
    fn on_connected(&self, _connection: Resource<TcpConnection>, _socket: Resource<TcpSocket>) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn client_connection_reconnects_after_initial_connect_failure() {
    let address = unused_loopback_addr().await;
    let connected = Arc::new(AtomicUsize::new(0));
    let connection = TcpConnection::create(
        address,
        None,
        Some(Box::new(CountingOnConnected { count: connected.clone() })),
    )
    .unwrap();
    let (failed_context, failed_token) = call_context();

    TcpConnection::submit_write(
        connection.clone(),
        iobuf_from_bytes(b"first write fails"),
        WriteOptions::client(failed_context),
    )
    .unwrap();

    let failed = timeout_completion(failed_token).await;
    assert_eq!(failed.status.as_ref().map(Status::code), Some(StatusCode::Unavailable));
    assert_eq!(connected.load(Ordering::Acquire), 0);

    let listener = TcpListener::bind(address).await.unwrap();
    let expected = b"second write succeeds".to_vec();
    let server = tokio::spawn({
        let expected = expected.clone();
        async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = vec![0; expected.len()];
            stream.read_exact(&mut received).await.unwrap();
            received
        }
    });

    TcpConnection::submit_write(connection, iobuf_from_bytes(&expected), WriteOptions::server())
        .unwrap();

    let received = tokio::time::timeout(TEST_TIMEOUT, server).await.unwrap().unwrap();
    assert_eq!(received, expected);
    assert_eq!(connected.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn close_while_connecting_rejects_late_successful_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connected = Arc::new(AtomicUsize::new(0));
    let connection = TcpConnection::create(
        address,
        None,
        Some(Box::new(CountingOnConnected { count: connected.clone() })),
    )
    .unwrap();
    assert!(
        connection
            .state
            .compare_exchange(
                CONNECTION_DISCONNECTED,
                CONNECTION_CONNECTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    );
    let (context, token) = call_context();
    let request = WriteRequest::resource_pool()
        .acquire_with_init(|| {
            WriteRequest::new(
                iobuf_from_bytes(b"must not be written"),
                WriteOptions::client(context),
            )
        })
        .unwrap();
    request.set_next_none();
    connection.write_head.store(request.id().as_u64(), Ordering::Release);

    let connect = tokio::spawn(async move { TcpTransport::connect_endpoint(address).await });
    let (_peer, _) = listener.accept().await.unwrap();
    let transport = connect.await.unwrap().unwrap();
    connection.close();
    TcpConnection::finish_connect(connection.clone(), request, Ok(transport)).await;

    assert_eq!(connection.state.load(Ordering::Acquire), CONNECTION_CLOSED);
    assert!(connection.current_socket().is_none());
    assert_eq!(connection.write_head.load(Ordering::Acquire), NULL_ID);
    assert_eq!(connected.load(Ordering::Acquire), 0);
    let completion = timeout_completion(token).await;
    assert_eq!(completion.status.as_ref().map(Status::code), Some(StatusCode::Unavailable));
}

#[tokio::test]
async fn socket_invalidation_wakes_reader_fails_pending_and_closes_accepted_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
    let (stream, remote_addr) = listener.accept().await.unwrap();
    let connection = TcpConnection::accept(stream, remote_addr).unwrap();
    let socket = connection.current_socket().unwrap();
    let _client_stream = client.await.unwrap().unwrap();
    let (pending_context, pending_token) = call_context();
    assert!(socket.register_pending(pending_context).is_ok());

    let reader = tokio::spawn({
        let socket = socket.clone();
        async move {
            let mut input = IoBuf::new();
            socket.read_more(&mut input).await
        }
    });
    tokio::task::yield_now().await;

    let status = Status::new(StatusCode::Unavailable, "read side reset");
    let actual = connection.invalidate(socket, status.clone());
    assert_eq!(actual, status);

    let read_result = tokio::time::timeout(TEST_TIMEOUT, reader).await.unwrap().unwrap();
    assert!(matches!(read_result, Ok(0) | Err(_)));
    let pending = timeout_completion(pending_token).await;
    assert_eq!(pending.status, Some(status));

    let (late_context, late_token) = call_context();
    TcpConnection::submit_write(
        connection,
        iobuf_from_bytes(b"must not reconnect"),
        WriteOptions::client(late_context),
    )
    .unwrap();
    let late = timeout_completion(late_token).await;
    assert_eq!(late.status.as_ref().map(Status::code), Some(StatusCode::Unavailable));
}

#[tokio::test]
async fn socket_invalidation_wakes_writer_waiting_for_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
    let (stream, remote_addr) = listener.accept().await.unwrap();
    let connection = TcpConnection::accept(stream, remote_addr).unwrap();
    let socket = connection.current_socket().unwrap();
    let _client_stream = client.await.unwrap().unwrap();

    let data = vec![0; 64 * 1024];
    let mut blocked = false;
    for _ in 0..4096 {
        match socket.transport().try_write(&data) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                blocked = true;
                break;
            }
            Err(error) => panic!("failed to fill socket send buffer: {error}"),
        }
    }
    assert!(blocked, "socket send buffer did not become full");

    let writer = tokio::spawn({
        let socket = socket.clone();
        async move { socket.transport().writable().await }
    });
    tokio::task::yield_now().await;

    connection.invalidate(socket, Status::new(StatusCode::Unavailable, "read side failed"));
    tokio::time::timeout(TEST_TIMEOUT, writer).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn outbound_invalidation_fails_old_pending_and_next_write_reconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connected = Arc::new(AtomicUsize::new(0));
    let connection = TcpConnection::create(
        address,
        None,
        Some(Box::new(CountingOnConnected { count: connected.clone() })),
    )
    .unwrap();
    let first_frame = b"first generation".to_vec();
    let (first_context, first_token) = call_context();

    TcpConnection::submit_write(
        connection.clone(),
        iobuf_from_bytes(&first_frame),
        WriteOptions::client(first_context),
    )
    .unwrap();
    let (mut first_peer, _) = listener.accept().await.unwrap();
    let mut received = vec![0; first_frame.len()];
    first_peer.read_exact(&mut received).await.unwrap();
    assert_eq!(received, first_frame);

    let socket = connection.current_socket().unwrap();
    let status = Status::new(StatusCode::Unavailable, "peer reset generation one");
    connection.invalidate(socket, status.clone());
    let failed = timeout_completion(first_token).await;
    assert_eq!(failed.status, Some(status));

    let second_frame = b"second generation".to_vec();
    TcpConnection::submit_write(
        connection.clone(),
        iobuf_from_bytes(&second_frame),
        WriteOptions::server(),
    )
    .unwrap();
    let (mut second_peer, _) = listener.accept().await.unwrap();
    let mut received = vec![0; second_frame.len()];
    second_peer.read_exact(&mut received).await.unwrap();
    assert_eq!(received, second_frame);
    assert_eq!(connected.load(Ordering::Acquire), 2);
    connection.close();
}

#[tokio::test]
async fn writer_refreshes_published_stack_in_fifo_order_before_releasing_ownership() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
    let (stream, remote_addr) = listener.accept().await.unwrap();
    let connection = TcpConnection::accept(stream, remote_addr).unwrap();
    let socket = connection.current_socket().unwrap();
    drop(client.await.unwrap().unwrap());
    let first = WriteRequest::resource_pool()
        .acquire_with_init(|| WriteRequest::new(IoBuf::new(), WriteOptions::server()))
        .unwrap();
    first.set_next_none();
    let mut cursor = WriteCursor::new(first);
    connection.write_head.store(cursor.tail_id, Ordering::Release);

    let second = WriteRequest::resource_pool()
        .acquire_with_init(|| WriteRequest::new(IoBuf::new(), WriteOptions::server()))
        .unwrap();
    let second_id = second.id().as_u64();
    let previous = connection.write_head.swap(second_id, Ordering::AcqRel);
    second.set_next_raw(previous);
    std::mem::forget(second);

    let third = WriteRequest::resource_pool()
        .acquire_with_init(|| WriteRequest::new(IoBuf::new(), WriteOptions::server()))
        .unwrap();
    let third_id = third.id().as_u64();
    let previous = connection.write_head.swap(third_id, Ordering::AcqRel);
    third.set_next_raw(previous);
    std::mem::forget(third);

    TcpConnection::refresh_published(&connection, &mut cursor);
    assert_eq!(cursor.tail_id, third_id);

    cursor = match TcpConnection::finish_request(connection.clone(), socket.clone(), cursor) {
        WriteProgress::Next(next) => next,
        WriteProgress::Done => panic!("second request was not collected"),
    };
    assert_eq!(cursor.request.id().as_u64(), second_id);
    cursor = match TcpConnection::finish_request(connection.clone(), socket.clone(), cursor) {
        WriteProgress::Next(next) => next,
        WriteProgress::Done => panic!("third request was not collected"),
    };
    assert_eq!(cursor.request.id().as_u64(), third_id);
    assert!(matches!(
        TcpConnection::finish_request(connection.clone(), socket, cursor),
        WriteProgress::Done
    ));
    assert_eq!(connection.write_head.load(Ordering::Acquire), NULL_ID);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_submit_write_preserves_frame_boundaries_and_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
    let (server_stream, remote_addr) = listener.accept().await.unwrap();
    let mut client_stream = client.await.unwrap().unwrap();
    let connection = TcpConnection::accept(server_stream, remote_addr).unwrap();

    let frame_count = 8;
    let frame_body_len = 256 * 1024;
    let expected = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(frame_count + 1));
    let submit_lock = Arc::new(Mutex::new(()));
    let mut tasks = Vec::new();

    for index in 0..frame_count {
        let connection = connection.clone();
        let expected = expected.clone();
        let barrier = barrier.clone();
        let submit_lock = submit_lock.clone();
        tasks.push(tokio::spawn(async move {
            let frame = framed_payload(index, frame_body_len);
            barrier.wait().await;
            let _guard = submit_lock.lock().unwrap();
            expected.lock().unwrap().push(frame.clone());
            TcpConnection::submit_write(
                connection,
                iobuf_from_bytes(&frame),
                WriteOptions::server(),
            )
            .unwrap();
        }));
    }

    barrier.wait().await;
    for task in tasks {
        task.await.unwrap();
    }

    let expected = expected.lock().unwrap().concat();
    let mut received = vec![0; expected.len()];
    tokio::time::timeout(TEST_TIMEOUT, client_stream.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received, expected);
    connection.close();
}

#[tokio::test]
async fn close_after_write_flushes_current_frame_then_closes_and_fails_successors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });
    let (server_stream, remote_addr) = listener.accept().await.unwrap();
    let mut client_stream = client.await.unwrap().unwrap();
    let connection = TcpConnection::accept(server_stream, remote_addr).unwrap();
    let first = b"flush this before closing".to_vec();
    let second = b"must not be written".to_vec();
    let (successor_context, successor_token) = call_context();

    TcpConnection::submit_write(
        connection.clone(),
        iobuf_from_bytes(&first),
        WriteOptions::server().close_after_write(),
    )
    .unwrap();
    TcpConnection::submit_write(
        connection.clone(),
        iobuf_from_bytes(&second),
        WriteOptions::client(successor_context),
    )
    .unwrap();

    let mut received = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, client_stream.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    let failed = timeout_completion(successor_token).await;

    assert_eq!(received, first);
    assert_eq!(failed.status.as_ref().map(Status::code), Some(StatusCode::Unavailable));
    assert!(connection.current_socket().is_none());
    connection.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_tasks_echo_many_messages_over_one_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let tasks = 2;
    let writes_per_task = 32;
    let total_messages = tasks * writes_per_task;
    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(address).await });

    let (server_stream, server_remote_addr) = listener.accept().await.unwrap();
    let client_stream = client.await.unwrap().unwrap();
    let client_remote_addr = client_stream.peer_addr().unwrap();
    let server_connection = TcpConnection::accept(server_stream, server_remote_addr).unwrap();
    let client_connection = TcpConnection::accept(client_stream, client_remote_addr).unwrap();

    let server_reader = tokio::spawn({
        let server_connection = server_connection.clone();
        async move {
            let mut input = IoBuf::new();
            let mut echoed = 0;
            let mut echo_tasks = Vec::with_capacity(total_messages);
            while echoed < total_messages {
                while let Some(frame) = try_take_echo_frame(&mut input) {
                    let server_connection = server_connection.clone();
                    echo_tasks.push(tokio::spawn(async move {
                        TcpConnection::submit_write(
                            server_connection,
                            iobuf_from_bytes(&frame),
                            WriteOptions::server(),
                        )
                        .unwrap();
                    }));
                    echoed += 1;
                    if echoed == total_messages {
                        break;
                    }
                }

                if echoed == total_messages {
                    break;
                }

                let read = server_connection.read_more(&mut input).await.unwrap();
                assert_ne!(read, 0, "client closed before all messages arrived");
            }

            for task in echo_tasks {
                task.await.unwrap();
            }
        }
    });

    let pending = Arc::new(Mutex::new(HashMap::<u64, Resource<CallContext>>::new()));
    let client_reader = tokio::spawn({
        let client_connection = client_connection.clone();
        let pending = pending.clone();
        async move {
            let mut input = IoBuf::new();
            let mut completed = 0;
            while completed < total_messages {
                while let Some(frame) = try_take_echo_frame(&mut input) {
                    let message = echo_message_from_frame(&frame);
                    let correlation_id = echo_correlation_id(message);
                    let context = pending.lock().unwrap().remove(&correlation_id).unwrap();
                    assert!(context.complete(iobuf_from_bytes(message), IoBuf::new(), None));
                    completed += 1;
                    if completed == total_messages {
                        return;
                    }
                }

                let read = client_connection.read_more(&mut input).await.unwrap();
                assert_ne!(read, 0, "server closed before all echoes arrived");
            }
        }
    });

    let barrier = Arc::new(tokio::sync::Barrier::new(tasks + 1));
    let mut writers = Vec::new();
    for task_id in 0..tasks {
        let connection = client_connection.clone();
        let pending = pending.clone();
        let barrier = barrier.clone();
        writers.push(tokio::spawn(async move {
            let mut calls = Vec::with_capacity(writes_per_task);
            barrier.wait().await;
            for sequence in 0..writes_per_task {
                let (context, token) = call_context();
                let message = echo_message(token.correlation_id(), task_id, sequence);
                let frame = echo_frame(&message);
                pending.lock().unwrap().insert(token.correlation_id(), context.clone());
                TcpConnection::submit_write(
                    connection.clone(),
                    iobuf_from_bytes(&frame),
                    WriteOptions::client(context),
                )
                .unwrap();
                calls.push((token, message));
            }

            for (token, expected) in calls {
                let completion = timeout_completion(token).await;
                assert_eq!(completion.status, None);
                assert_eq!(collect_iobuf(&completion.payload), expected);
            }
        }));
    }

    barrier.wait().await;
    for writer in writers {
        writer.await.unwrap();
    }
    client_reader.await.unwrap();
    server_reader.await.unwrap();
    client_connection.close();
    server_connection.close();
}

async fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn call_context() -> (Resource<CallContext>, CallToken) {
    let context =
        CallContext::resource_pool().acquire_with_init(|| CallContext::new(None, false)).unwrap();
    let token = CallToken::new(context.clone());
    (context, token)
}

async fn timeout_completion(token: CallToken) -> crate::transport::inflight::CallOutput {
    tokio::time::timeout(TEST_TIMEOUT, std::future::poll_fn(|cx| token.poll(cx))).await.unwrap()
}

fn iobuf_from_bytes(bytes: &[u8]) -> IoBuf {
    let mut builder = crate::IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}

fn framed_payload(index: usize, body_len: usize) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + body_len);
    frame.extend_from_slice(&(body_len as u32).to_be_bytes());
    frame.extend_from_slice(&(index as u32).to_be_bytes());
    frame.extend(std::iter::repeat_n(index as u8, body_len));
    frame
}

fn echo_message(correlation_id: u64, task_id: usize, sequence: usize) -> Vec<u8> {
    let payload_len = 1024;
    let mut message = Vec::with_capacity(16 + payload_len);
    message.extend_from_slice(&correlation_id.to_be_bytes());
    message.extend_from_slice(&(task_id as u32).to_be_bytes());
    message.extend_from_slice(&(sequence as u32).to_be_bytes());
    message.extend(std::iter::repeat_n((task_id * 31 + sequence) as u8, payload_len));
    message
}

fn echo_frame(message: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + message.len());
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend_from_slice(message);
    frame
}

fn try_take_echo_frame(input: &mut IoBuf) -> Option<Vec<u8>> {
    if input.len() < 4 {
        return None;
    }
    let bytes = collect_iobuf(input);
    let body_len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    let total_len = 4 + body_len;
    if bytes.len() < total_len {
        return None;
    }
    let frame = bytes[..total_len].to_vec();
    input.consume(total_len);
    Some(frame)
}

fn echo_message_from_frame(frame: &[u8]) -> &[u8] {
    &frame[4..]
}

fn echo_correlation_id(message: &[u8]) -> u64 {
    u64::from_be_bytes(message[..8].try_into().unwrap())
}

fn collect_iobuf(input: &IoBuf) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    for chunk in input.chunks() {
        output.extend_from_slice(chunk);
    }
    output
}
