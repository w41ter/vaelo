// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Barrier;

use super::*;
use crate::StaticResource;
use crate::transport::TcpTransport;

const CLIENT_SETTINGS_PAYLOAD: usize = 24;
const INITIAL_CONNECTION_WINDOW_UPDATE: usize = 9 + 4;
const CLIENT_HANDSHAKE_SIZE: usize =
    CLIENT_PREFACE.len() + 9 + CLIENT_SETTINGS_PAYLOAD + INITIAL_CONNECTION_WINDOW_UPDATE;

#[test]
fn hot_stream_state_stays_compact() {
    assert!(std::mem::size_of::<H2StreamShared>() <= 768);
}

#[tokio::test]
async fn releasing_next_wire_stream_does_not_publish_duplicates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (_peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(ConnectionRole::Client, socket);

    while connection
        .owner
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        tokio::task::yield_now().await;
    }
    while connection.ready_streams.pop().is_some() {}
    let stream = H2Connection::new_stream_shared(
        StreamId::new(1).unwrap(),
        connection.resource_id(),
        INITIAL_WINDOW_SIZE as u32,
        None,
    )
    .unwrap();
    stream.write_ready.store(true, Ordering::Release);
    // SAFETY: the test owns the connection baton.
    let core = unsafe { &mut *connection.core.get() };
    core.next_wire_stream_id = 1;
    core.next_local_stream_id = 3;
    core.streams.insert(stream.id, stream.clone());

    connection.release_next_wire_stream(core);
    connection.release_next_wire_stream(core);
    assert_eq!(connection.ready_streams.pop().unwrap().id, stream.id);
    stream.ready_queued.store(false, Ordering::Release);
    assert!(connection.ready_streams.pop().is_none());

    assert!(connection.enqueue_ready(stream.clone()));
    connection.write_waiting.store(true, Ordering::Release);
    assert!(!connection.has_runnable_work());
    connection.read_ready.store(true, Ordering::Release);
    assert!(connection.has_runnable_work());

    core.streams.clear();
    connection.ready_streams.pop();
    connection.write_waiting.store(false, Ordering::Release);
    connection.read_ready.store(false, Ordering::Release);
    connection.owner.store(false, Ordering::Release);
    connection.close();
}

fn payload(bytes: &[u8]) -> IoBuf {
    let mut builder = IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}

fn flatten(buffer: &IoBuf) -> Vec<u8> {
    buffer.chunks().flatten().copied().collect()
}

fn decode_client_handshake(input: &mut IoBuf) {
    let settings =
        frame::decode(input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(settings.header.kind, FrameKind::Settings);
    let update =
        frame::decode(input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(update.header.kind, FrameKind::WindowUpdate);
    assert!(update.header.stream_id.is_connection());
    assert_eq!(
        flatten(&update.payload),
        ((LOCAL_CONNECTION_RECV_WINDOW - INITIAL_WINDOW_SIZE as usize) as u32).to_be_bytes()
    );
}

async fn read_wire_frame(peer: &mut TcpStream) -> Frame {
    let mut header = [0_u8; 9];
    peer.read_exact(&mut header).await.unwrap();
    let length =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let mut wire = header.to_vec();
    wire.resize(9 + length, 0);
    peer.read_exact(&mut wire[9..]).await.unwrap();
    frame::decode(&mut payload(&wire), super::super::frame::DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

fn data_frames(stream_id: u32, bytes: usize) -> Vec<u8> {
    let max_frame = super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize;
    let mut remaining = bytes;
    let mut wire = Vec::new();
    while remaining != 0 {
        let length = remaining.min(max_frame);
        remaining -= length;
        let encoded = frame::encode(Frame {
            header: FrameHeader {
                length: length as u32,
                kind: FrameKind::Data,
                flags: FrameFlags::new(if remaining == 0 { FrameFlags::END_STREAM } else { 0 }),
                stream_id: StreamId::new(stream_id).unwrap(),
            },
            payload: payload(&vec![1_u8; length]),
        })
        .unwrap();
        wire.extend_from_slice(&flatten(&encoded));
    }
    wire
}

fn request_headers() -> HeaderBlock {
    let mut headers = HeaderBlock::new();
    headers.pseudo.method = Some(Bytes::from_static(b"POST"));
    headers.pseudo.scheme = Some(Bytes::from_static(b"https"));
    headers.pseudo.path = Some(Bytes::from_static(b"/"));
    headers
}

fn response_headers_frame(stream_id: u32, end_stream: bool) -> Frame {
    let mut headers = HeaderBlock::new();
    headers.pseudo.status = Some(200);
    let payload = HpackEncoder::new(HpackConfig::default()).encode(&headers).unwrap();
    Frame {
        header: FrameHeader {
            length: payload.len() as u32,
            kind: FrameKind::Headers,
            flags: FrameFlags::new(
                FrameFlags::END_HEADERS | if end_stream { FrameFlags::END_STREAM } else { 0 },
            ),
            stream_id: StreamId::new(stream_id).unwrap(),
        },
        payload,
    }
}

fn window_update_frame(stream_id: u32, increment: u32) -> Frame {
    Frame {
        header: FrameHeader {
            length: 4,
            kind: FrameKind::WindowUpdate,
            flags: FrameFlags::default(),
            stream_id: StreamId::new(stream_id).unwrap(),
        },
        payload: payload(&increment.to_be_bytes()),
    }
}

fn reset_frame(stream_id: u32) -> Frame {
    Frame {
        header: FrameHeader {
            length: 4,
            kind: FrameKind::RstStream,
            flags: FrameFlags::default(),
            stream_id: StreamId::new(stream_id).unwrap(),
        },
        payload: payload(&(ErrorCode::Cancel as u32).to_be_bytes()),
    }
}

fn ping_frame(payload: [u8; 8]) -> Frame {
    Frame {
        header: FrameHeader {
            length: 8,
            kind: FrameKind::Ping,
            flags: FrameFlags::default(),
            stream_id: StreamId::CONNECTION,
        },
        payload: self::payload(&payload),
    }
}

fn goaway_frame(last_stream_id: u32, error_code: u32) -> Frame {
    let mut builder = IoBufBuilder::new();
    builder.put_u32(last_stream_id);
    builder.put_u32(error_code);
    Frame {
        header: FrameHeader {
            length: 8,
            kind: FrameKind::GoAway,
            flags: FrameFlags::default(),
            stream_id: StreamId::CONNECTION,
        },
        payload: builder.finish(),
    }
}

fn window_update_core(role: ConnectionRole) -> H2Core {
    H2Core {
        role,
        next_local_stream_id: 9,
        next_wire_stream_id: 9,
        last_peer_stream_id: 0,
        last_local_stream_id: 0,
        streams: HashMap::new(),
        pending_opens: VecDeque::new(),
        control_frames: VecDeque::new(),
        write_batch: IoBuf::new(),
        write_segments: VecDeque::new(),
        input: IoBuf::new(),
        peer_settings: Settings::default(),
        peer_settings_received: true,
        hpack_encoder: HpackEncoder::new(HpackConfig::default()),
        hpack_decoder: HpackDecoder::new(HpackConfig::default()),
        continuation: None,
        blocked_inbound: None,
        peer_goaway_last_stream_id: None,
        connection_send_window: INITIAL_WINDOW_SIZE,
        connection_recv_window: INITIAL_WINDOW_SIZE,
        max_body_size: None,
        max_peer_streams: PENDING_COMMANDS,
    }
}

#[test]
fn late_window_updates_for_retired_streams_do_not_fail_the_connection() {
    for role in [ConnectionRole::Client, ConnectionRole::Server] {
        let mut core = window_update_core(role);
        match role {
            ConnectionRole::Client => core.last_local_stream_id = 3,
            ConnectionRole::Server => core.last_peer_stream_id = 3,
        }
        // Stream 1 has been reaped, or was implicitly closed when 3 opened.
        assert!(
            H2Connection::handle_window_update(&mut core, window_update_frame(1, 32768)).is_ok()
        );
        assert!(
            H2Connection::handle_window_update(&mut core, window_update_frame(3, 32768)).is_ok()
        );
        // Allocation alone must not mark never-opened IDs as closed.
        assert_eq!(
            H2Connection::handle_window_update(&mut core, window_update_frame(5, 1)),
            Err(RecvStreamError::ProtocolError)
        );
        assert_eq!(
            H2Connection::handle_window_update(&mut core, window_update_frame(2, 1)),
            Err(RecvStreamError::ProtocolError)
        );
        assert!(H2Connection::handle_window_update(&mut core, window_update_frame(0, 1)).is_ok());
        assert_eq!(core.connection_send_window, INITIAL_WINDOW_SIZE + 1);
    }
}

#[test]
fn late_resets_for_retired_streams_do_not_fail_the_connection() {
    for role in [ConnectionRole::Client, ConnectionRole::Server] {
        let mut core = window_update_core(role);
        match role {
            ConnectionRole::Client => core.last_local_stream_id = 3,
            ConnectionRole::Server => core.last_peer_stream_id = 3,
        }
        assert!(H2Connection::handle_reset(&mut core, reset_frame(1)).is_ok());
        assert_eq!(
            H2Connection::handle_reset(&mut core, reset_frame(5),),
            Err(RecvStreamError::ProtocolError)
        );
    }
}

#[test]
fn window_updates_distinguish_allocated_idle_streams_from_half_closed_streams() {
    let mut core = window_update_core(ConnectionRole::Client);
    let id = StreamId::new(1).unwrap();
    let stream = H2Connection::new_stream_shared(
        id,
        h2_connection_id(NULL_ID),
        INITIAL_WINDOW_SIZE as u32,
        None,
    )
    .unwrap();
    core.streams.insert(id, stream.clone());
    assert_eq!(
        H2Connection::handle_window_update(&mut core, window_update_frame(1, 1)),
        Err(RecvStreamError::ProtocolError)
    );
    stream.state.store(STREAM_WIRE_OPEN | STREAM_LOCAL_CLOSED, Ordering::Release);
    // No more DATA can be sent, so even a large late grant is ignored.
    assert!(
        H2Connection::handle_window_update(
            &mut core,
            window_update_frame(1, MAX_WINDOW_SIZE as u32)
        )
        .is_ok()
    );
    // Malformed frames remain errors.
    assert_eq!(
        H2Connection::handle_window_update(&mut core, window_update_frame(0, 0)),
        Err(RecvStreamError::ProtocolError)
    );
}

#[tokio::test]
async fn ping_is_acknowledged_without_waking_a_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);

    let mut client_handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut client_handshake).await.unwrap();
    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let opaque = *b"12345678";
    let ping = frame::encode(ping_frame(opaque)).unwrap();
    let mut inbound = flatten(&settings);
    inbound.extend_from_slice(&flatten(&ping));
    peer.write_all(&inbound).await.unwrap();

    let mut outbound = vec![0; 9 + 9 + opaque.len()];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut outbound))
        .await
        .unwrap()
        .unwrap();
    let mut frames = payload(&outbound);
    let settings_ack =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(settings_ack.header.kind, FrameKind::Settings);
    assert!(settings_ack.header.flags.contains(FrameFlags::ACK));
    let ping_ack =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(ping_ack.header.kind, FrameKind::Ping);
    assert!(ping_ack.header.flags.contains(FrameFlags::ACK));
    assert_eq!(flatten(&ping_ack.payload), opaque);
    connection.close();
}

#[tokio::test]
async fn goaway_only_rejects_streams_the_peer_did_not_process() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let first = connection.open_stream().await.unwrap();
    let second = connection.open_stream().await.unwrap();
    let (_first_send, mut first_recv) = first.split();
    let (_second_send, mut second_recv) = second.split();

    let mut client_handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut client_handshake).await.unwrap();
    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let goaway = frame::encode(goaway_frame(1, 0)).unwrap();
    let response = frame::encode(response_headers_frame(1, true)).unwrap();
    let mut inbound = flatten(&settings);
    inbound.extend_from_slice(&flatten(&goaway));
    inbound.extend_from_slice(&flatten(&response));
    peer.write_all(&inbound).await.unwrap();

    assert!(matches!(second_recv.recv().await, Err(RecvStreamError::UnprocessedByPeer(0))));
    assert!(matches!(
        first_recv.recv().await.unwrap(),
        Some(ReceivedEvent::Headers { end_stream: true, .. })
    ));
    assert_eq!(connection.state.load(Ordering::Acquire), CONNECTION_DRAINING);
    assert!(matches!(connection.open_stream().await, Err(OpenStreamError::ConnectionDraining)));
    connection.close();
}

#[tokio::test]
async fn max_concurrent_streams_parks_open_until_a_stream_finishes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);

    let mut client_handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut client_handshake).await.unwrap();
    let settings = Settings { max_concurrent_streams: Some(1), ..Settings::default() };
    peer.write_all(&flatten(&frame::encode(settings.frame(false)).unwrap())).await.unwrap();
    let mut ack = [0_u8; 9];
    peer.read_exact(&mut ack).await.unwrap();

    let first = connection.open_stream().await.unwrap();
    let mut second = Box::pin(connection.open_stream());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut second).await.is_err());

    let (mut first_send, mut first_recv) = first.split();
    first_send
        .send_headers(request_headers(), true)
        .unwrap_or_else(|_| panic!("send headers failed"));
    let mut request = [0_u8; 12];
    peer.read_exact(&mut request).await.unwrap();
    peer.write_all(&flatten(&frame::encode(response_headers_frame(1, true)).unwrap()))
        .await
        .unwrap();
    assert!(matches!(
        first_recv.recv().await.unwrap(),
        Some(ReceivedEvent::Headers { end_stream: true, .. })
    ));

    let second = tokio::time::timeout(Duration::from_secs(2), &mut second).await.unwrap().unwrap();
    assert_eq!(second.id().as_u32(), 3);
    connection.close();
}

#[tokio::test]
async fn canceled_pending_open_does_not_consume_a_stream_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);

    let mut client_handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut client_handshake).await.unwrap();
    let blocked = Settings { max_concurrent_streams: Some(0), ..Settings::default() };
    peer.write_all(&flatten(&frame::encode(blocked.frame(false)).unwrap())).await.unwrap();
    let mut ack = [0_u8; 9];
    peer.read_exact(&mut ack).await.unwrap();

    let mut canceled = Box::pin(connection.open_stream());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut canceled).await.is_err());
    drop(canceled);

    let enabled = Settings { max_concurrent_streams: Some(1), ..Settings::default() };
    peer.write_all(&flatten(&frame::encode(enabled.frame(false)).unwrap())).await.unwrap();
    peer.read_exact(&mut ack).await.unwrap();
    let stream = connection.open_stream().await.unwrap();
    assert_eq!(stream.id().as_u32(), 1);
    connection.close();
}

#[tokio::test]
async fn dropping_both_stream_halves_sends_cancel_after_headers_reach_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, recv) = stream.split();
    send.send_headers(request_headers(), false).unwrap_or_else(|_| panic!("send headers failed"));

    let mut initial = vec![0; CLIENT_HANDSHAKE_SIZE + 9 + 3];
    peer.read_exact(&mut initial).await.unwrap();
    drop(send);
    drop(recv);

    let mut reset = [0_u8; 13];
    peer.read_exact(&mut reset).await.unwrap();
    let reset = frame::decode(&mut payload(&reset), super::super::frame::DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap();
    assert_eq!(reset.header.kind, FrameKind::RstStream);
    assert_eq!(reset.header.stream_id.as_u32(), 1);
    assert_eq!(flatten(&reset.payload), (ErrorCode::Cancel as u32).to_be_bytes());
    connection.close();
}

#[tokio::test]
async fn abandoning_before_headers_reach_the_batch_emits_nothing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, recv) = stream.split();

    let mut handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut handshake).await.unwrap();
    connection.owner.store(true, Ordering::Release);
    send.send_headers(request_headers(), false).unwrap_or_else(|_| panic!("send headers failed"));
    drop(send);
    drop(recv);
    connection.owner.store(false, Ordering::Release);
    H2Connection::try_drive(connection.clone(), DriveQuota::INLINE);

    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.read_exact(&mut byte)).await.is_err()
    );
    connection.close();
}

#[tokio::test]
async fn data_outliving_a_reaped_stream_still_restores_connection_credit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, mut recv) = stream.split();
    send.send_headers(request_headers(), true).unwrap_or_else(|_| panic!("send headers failed"));

    let mut request = vec![0; CLIENT_HANDSHAKE_SIZE + 9 + 3];
    peer.read_exact(&mut request).await.unwrap();
    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(response_headers_frame(1, false)).unwrap();
    let mut inbound = flatten(&settings);
    inbound.extend_from_slice(&flatten(&headers));
    inbound.extend_from_slice(&data_frames(1, CONNECTION_WINDOW_UPDATE_THRESHOLD));
    peer.write_all(&inbound).await.unwrap();

    assert!(matches!(recv.recv().await.unwrap(), Some(ReceivedEvent::Headers { .. })));
    let mut received = Vec::new();
    for _ in 0..CONNECTION_WINDOW_UPDATE_THRESHOLD
        .div_ceil(super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize)
    {
        match recv.recv().await.unwrap().unwrap() {
            ReceivedEvent::Data(data) => received.push(data),
            ReceivedEvent::Headers { .. } => panic!("expected DATA"),
        }
    }
    drop(send);
    drop(recv);
    drop(received);

    let ack = read_wire_frame(&mut peer).await;
    assert_eq!(ack.header.kind, FrameKind::Settings);
    let mut restored = 0_u32;
    while restored == 0 {
        let update = read_wire_frame(&mut peer).await;
        assert_eq!(update.header.kind, FrameKind::WindowUpdate);
        if update.header.stream_id.is_connection() {
            restored = u32::from_be_bytes(flatten(&update.payload).try_into().unwrap());
        }
    }
    assert_eq!(restored, CONNECTION_WINDOW_UPDATE_THRESHOLD as u32);
    connection.close();
}

#[tokio::test]
async fn idle_submitter_opens_stream_and_writes_data_inline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);

    let stream = connection.open_stream().await.unwrap();
    assert_eq!(stream.id().as_u32(), 1);
    let (mut send, recv) = stream.split();
    assert_eq!(recv.id().as_u32(), 1);
    send.send_headers(request_headers(), false).unwrap_or_else(|_| panic!("send headers failed"));
    send.send_data(payload(b"hello"), true).unwrap_or_else(|_| panic!("send failed"));
    assert!(matches!(send.send_data(payload(b"late"), false), Err(SendStreamError::Closed(_))));

    // Client SETTINGS has three six-byte entries; DATA carries five bytes.
    let mut wire = vec![0; CLIENT_HANDSHAKE_SIZE + 9 + 3 + 9 + 5];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut wire))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&wire[..CLIENT_PREFACE.len()], CLIENT_PREFACE);
    let mut input = payload(&wire[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut input);
    let headers =
        frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(headers.header.kind, FrameKind::Headers);
    assert!(headers.header.flags.contains(FrameFlags::END_HEADERS));
    assert_eq!(flatten(&headers.payload), [0x83, 0x87, 0x84]);
    let data =
        frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(data.header.kind, FrameKind::Data);
    assert_eq!(data.header.stream_id.as_u32(), 1);
    assert!(data.header.flags.contains(FrameFlags::END_STREAM));
    assert_eq!(data.payload.chunks().flatten().copied().collect::<Vec<_>>(), b"hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_stream_writers_are_combined_without_corrupting_frames() {
    const STREAMS: usize = 32;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let barrier = Arc::new(Barrier::new(STREAMS + 1));
    let mut writers = Vec::with_capacity(STREAMS);

    for value in 0..STREAMS {
        let stream = connection.open_stream().await.unwrap();
        let stream_id = stream.id().as_u32();
        let (mut send, _recv) = stream.split();
        let barrier = barrier.clone();
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            send.send_headers(request_headers(), false)
                .unwrap_or_else(|_| panic!("send headers failed"));
            send.send_data(payload(&[value as u8]), true).unwrap_or_else(|_| panic!("send failed"));
            (stream_id, value as u8, _recv)
        }));
    }
    barrier.wait().await;

    let mut expected = HashMap::new();
    let mut receivers = Vec::with_capacity(STREAMS);
    for writer in writers {
        let (stream_id, value, recv) = writer.await.unwrap();
        expected.insert(stream_id, value);
        receivers.push(recv);
    }

    let mut wire = vec![0; CLIENT_HANDSHAKE_SIZE + STREAMS * (9 + 3 + 9 + 1)];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut wire))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&wire[..CLIENT_PREFACE.len()], CLIENT_PREFACE);

    let mut input = payload(&wire[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut input);
    let mut headers_seen = HashSet::new();
    for _ in 0..STREAMS * 2 {
        let frame = frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        match frame.header.kind {
            FrameKind::Headers => {
                assert!(frame.header.flags.contains(FrameFlags::END_HEADERS));
                headers_seen.insert(frame.header.stream_id.as_u32());
            }
            FrameKind::Data => {
                assert!(headers_seen.contains(&frame.header.stream_id.as_u32()));
                assert!(frame.header.flags.contains(FrameFlags::END_STREAM));
                let actual = flatten(&frame.payload);
                assert_eq!(actual, [expected.remove(&frame.header.stream_id.as_u32()).unwrap()]);
            }
            kind => panic!("unexpected frame: {kind:?}"),
        }
    }
    assert!(input.is_empty());
    assert!(expected.is_empty());
    drop(receivers);
}

#[tokio::test]
async fn large_data_is_split_at_the_peer_frame_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, _recv) = stream.split();
    send.send_headers(request_headers(), false).unwrap_or_else(|_| panic!("send headers failed"));
    let body = vec![7_u8; super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize + 3];
    send.send_data(payload(&body), true).unwrap_or_else(|_| panic!("send failed"));

    let mut wire = vec![
        0;
        CLIENT_HANDSHAKE_SIZE
            + 9
            + 3
            + 9
            + super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize
            + 9
            + 3
    ];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut wire))
        .await
        .unwrap()
        .unwrap();
    let mut input = payload(&wire[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut input);
    let _headers =
        frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    let first =
        frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    let second =
        frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(first.payload.len(), super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize);
    assert!(!first.header.flags.contains(FrameFlags::END_STREAM));
    assert_eq!(second.payload.len(), 3);
    assert!(second.header.flags.contains(FrameFlags::END_STREAM));
    connection.close();
}

#[tokio::test]
async fn data_waits_for_both_connection_and_stream_window_updates() {
    const REMAINDER: usize = 10;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, _recv) = stream.split();
    send.send_headers(request_headers(), false).unwrap_or_else(|_| panic!("send headers failed"));
    let body = vec![9_u8; INITIAL_WINDOW_SIZE as usize + REMAINDER];
    send.send_data(payload(&body), true).unwrap_or_else(|_| panic!("send failed"));

    // Four DATA frames consume the entire initial 65,535-byte connection
    // and stream windows. The ten-byte tail must remain owner-local.
    let mut initial = vec![0; CLIENT_HANDSHAKE_SIZE + 9 + 3 + INITIAL_WINDOW_SIZE as usize + 4 * 9];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut initial))
        .await
        .unwrap()
        .unwrap();
    let mut frames = payload(&initial[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut frames);
    let _headers =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    let mut sent = 0;
    while !frames.is_empty() {
        let data = frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        sent += data.payload.len();
        assert!(!data.header.flags.contains(FrameFlags::END_STREAM));
    }
    assert_eq!(sent, INITIAL_WINDOW_SIZE as usize);

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let connection_window = frame::encode(window_update_frame(0, REMAINDER as u32)).unwrap();
    let stream_window = frame::encode(window_update_frame(1, REMAINDER as u32)).unwrap();
    let mut updates = flatten(&settings);
    updates.extend_from_slice(&flatten(&connection_window));
    updates.extend_from_slice(&flatten(&stream_window));
    peer.write_all(&updates).await.unwrap();

    // SETTINGS ACK is emitted before the newly unblocked DATA tail.
    let mut resumed = vec![0; 9 + 9 + REMAINDER];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut resumed))
        .await
        .unwrap()
        .unwrap();
    let mut frames = payload(&resumed);
    let ack =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(ack.header.kind, FrameKind::Settings);
    assert!(ack.header.flags.contains(FrameFlags::ACK));
    let tail =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(tail.payload.len(), REMAINDER);
    assert!(tail.header.flags.contains(FrameFlags::END_STREAM));
    connection.close();
}

#[tokio::test]
async fn unary_direct_send_keeps_trailers_after_flow_control_resume() {
    const REMAINDER: usize = 10;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, _recv) = stream.split();
    let body = vec![9_u8; INITIAL_WINDOW_SIZE as usize + REMAINDER];

    // Prevent inline ownership so the producer-side representation can be
    // inspected before the connection consumes it.
    connection.owner.store(true, Ordering::Release);
    send.send_complete(request_headers(), vec![payload(&body)], Some(HeaderBlock::new()))
        .unwrap_or_else(|_| panic!("direct send failed"));
    assert!(send.shared.outbound.get().is_none());
    assert!(send.shared.complete_send.lock().unwrap().is_some());
    connection.owner.store(false, Ordering::Release);
    H2Connection::try_drive(connection.clone(), DriveQuota::INLINE);

    let mut initial = vec![0; CLIENT_HANDSHAKE_SIZE + 9 + 3 + INITIAL_WINDOW_SIZE as usize + 4 * 9];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut initial))
        .await
        .unwrap()
        .unwrap();
    let mut frames = payload(&initial[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut frames);
    let _headers =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    while !frames.is_empty() {
        let data = frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(data.header.kind, FrameKind::Data);
        assert!(!data.header.flags.contains(FrameFlags::END_STREAM));
    }

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let connection_window = frame::encode(window_update_frame(0, REMAINDER as u32)).unwrap();
    let stream_window = frame::encode(window_update_frame(1, REMAINDER as u32)).unwrap();
    let mut updates = flatten(&settings);
    updates.extend_from_slice(&flatten(&connection_window));
    updates.extend_from_slice(&flatten(&stream_window));
    peer.write_all(&updates).await.unwrap();

    // SETTINGS ACK, the DATA remainder, then trailers carrying END_STREAM.
    let mut resumed = vec![0; 9 + 9 + REMAINDER + 9];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut resumed))
        .await
        .unwrap()
        .unwrap();
    let mut frames = payload(&resumed);
    let ack =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(ack.header.kind, FrameKind::Settings);
    assert!(ack.header.flags.contains(FrameFlags::ACK));
    let tail =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(tail.header.kind, FrameKind::Data);
    assert_eq!(tail.payload.len(), REMAINDER);
    assert!(!tail.header.flags.contains(FrameFlags::END_STREAM));
    let trailers =
        frame::decode(&mut frames, super::super::frame::DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
    assert_eq!(trailers.header.kind, FrameKind::Headers);
    assert!(trailers.header.flags.contains(FrameFlags::END_HEADERS));
    assert!(trailers.header.flags.contains(FrameFlags::END_STREAM));
    connection.close();
}

#[tokio::test]
async fn large_streams_take_turns_at_the_data_quantum() {
    const BODY: usize = 20 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let first = connection.open_stream().await.unwrap();
    let second = connection.open_stream().await.unwrap();
    let (mut first_send, _first_recv) = first.split();
    let (mut second_send, _second_recv) = second.split();

    // Hold the baton so both producers publish before either can drain.
    connection.owner.store(true, Ordering::Release);
    first_send
        .send_headers(request_headers(), false)
        .unwrap_or_else(|_| panic!("first headers failed"));
    first_send
        .send_data(payload(&vec![1_u8; BODY]), true)
        .unwrap_or_else(|_| panic!("first data failed"));
    second_send
        .send_headers(request_headers(), false)
        .unwrap_or_else(|_| panic!("second headers failed"));
    second_send
        .send_data(payload(&vec![2_u8; BODY]), true)
        .unwrap_or_else(|_| panic!("second data failed"));
    connection.owner.store(false, Ordering::Release);
    H2Connection::try_drive(connection.clone(), DriveQuota::INLINE);

    let mut wire = vec![0; CLIENT_HANDSHAKE_SIZE + 2 * (9 + 3) + 4 * 9 + 2 * BODY];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut wire))
        .await
        .unwrap()
        .unwrap();
    let mut input = payload(&wire[CLIENT_PREFACE.len()..]);
    decode_client_handshake(&mut input);
    let mut sequence = Vec::new();
    while !input.is_empty() {
        let frame = frame::decode(&mut input, super::super::frame::DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        if frame.header.kind == FrameKind::Data {
            sequence.push((frame.header.stream_id.as_u32(), frame.payload.len()));
        }
    }
    assert_eq!(
        sequence,
        [
            (1, STREAM_DATA_QUANTUM),
            (3, STREAM_DATA_QUANTUM),
            (1, BODY - STREAM_DATA_QUANTUM),
            (3, BODY - STREAM_DATA_QUANTUM),
        ]
    );
    connection.close();
}

#[tokio::test]
async fn dropping_received_data_batches_connection_and_stream_window_updates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    let mut client_handshake = vec![0; CLIENT_HANDSHAKE_SIZE];
    peer.read_exact(&mut client_handshake).await.unwrap();
    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(response_headers_frame(1, false)).unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&headers));
    wire.extend_from_slice(&data_frames(1, CONNECTION_WINDOW_UPDATE_THRESHOLD));
    peer.write_all(&wire).await.unwrap();

    assert!(matches!(recv.recv().await.unwrap().unwrap(), ReceivedEvent::Headers { .. }));
    for _ in 0..CONNECTION_WINDOW_UPDATE_THRESHOLD
        .div_ceil(super::super::frame::DEFAULT_MAX_FRAME_SIZE as usize)
    {
        drop(recv.recv().await.unwrap().unwrap());
    }

    let ack = read_wire_frame(&mut peer).await;
    assert_eq!(ack.header.kind, FrameKind::Settings);
    let mut connection_credit = 0_u32;
    let mut stream_credit = 0_u32;
    while connection_credit < CONNECTION_WINDOW_UPDATE_THRESHOLD as u32
        || stream_credit < STREAM_WINDOW_UPDATE_THRESHOLD as u32
    {
        let update = read_wire_frame(&mut peer).await;
        assert_eq!(update.header.kind, FrameKind::WindowUpdate);
        let increment = u32::from_be_bytes(flatten(&update.payload).try_into().unwrap());
        if update.header.stream_id.is_connection() {
            connection_credit += increment;
        } else {
            assert_eq!(update.header.stream_id.as_u32(), 1);
            stream_credit += increment;
        }
    }
    assert_eq!(connection_credit, CONNECTION_WINDOW_UPDATE_THRESHOLD as u32);
    // Once END_STREAM is processed the stream no longer needs its final
    // half-window restored; only the still-live connection credit matters.
    assert!(stream_credit >= STREAM_WINDOW_UPDATE_THRESHOLD as u32);
    assert!(stream_credit < CONNECTION_WINDOW_UPDATE_THRESHOLD as u32);
    connection.close();
}

#[tokio::test]
async fn read_continuation_delivers_data_and_remote_end_to_one_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(response_headers_frame(1, false)).unwrap();
    let response = frame::encode(Frame {
        header: FrameHeader {
            length: 8,
            kind: FrameKind::Data,
            flags: FrameFlags::new(FrameFlags::END_STREAM),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: payload(b"response"),
    })
    .unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&headers));
    wire.extend_from_slice(&flatten(&response));
    peer.write_all(&wire).await.unwrap();

    let received_headers =
        tokio::time::timeout(Duration::from_secs(2), recv.recv()).await.unwrap().unwrap().unwrap();
    assert!(matches!(
        received_headers,
        ReceivedEvent::Headers { headers, end_stream: false, trailers: false }
            if headers.pseudo.status == Some(200)
    ));
    let received = recv.recv().await.unwrap().unwrap();
    let ReceivedEvent::Data(received) = received else { panic!("expected DATA") };
    assert!(received.end_stream);
    assert_eq!(flatten(&received.payload), b"response");
    assert!(recv.recv().await.unwrap().is_none());
    connection.close();
}

#[tokio::test]
async fn full_inbound_queue_pauses_and_resumes_without_closing_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(response_headers_frame(1, false)).unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&headers));
    for index in 0..(INBOUND_ITEMS + 8) {
        let data = frame::encode(Frame {
            header: FrameHeader {
                length: 1,
                kind: FrameKind::Data,
                flags: FrameFlags::new(if index + 1 == INBOUND_ITEMS + 8 {
                    FrameFlags::END_STREAM
                } else {
                    0
                }),
                stream_id: StreamId::new(1).unwrap(),
            },
            payload: payload(&[index as u8]),
        })
        .unwrap();
        wire.extend_from_slice(&flatten(&data));
    }
    peer.write_all(&wire).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut values = Vec::new();
        while let Some(event) = recv.recv().await.unwrap() {
            if let ReceivedEvent::Data(data) = event {
                values.push(flatten(&data.payload)[0]);
            }
        }
        values
    })
    .await
    .unwrap();
    assert_eq!(received.len(), INBOUND_ITEMS + 8);
    assert_eq!(received, (0..(INBOUND_ITEMS + 8)).map(|value| value as u8).collect::<Vec<_>>());
    assert_eq!(connection.state.load(Ordering::Acquire), CONNECTION_OPEN);
    connection.close();
}

#[tokio::test]
async fn canceling_stream_blocking_inbound_resumes_connection_reads() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (mut send, _recv) = stream.split();
    assert!(send.send_headers(request_headers(), false).is_ok());

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(response_headers_frame(1, false)).unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&headers));
    for index in 0..INBOUND_ITEMS {
        let data = frame::encode(Frame {
            header: FrameHeader {
                length: 1,
                kind: FrameKind::Data,
                flags: FrameFlags::default(),
                stream_id: StreamId::new(1).unwrap(),
            },
            payload: payload(&[index as u8]),
        })
        .unwrap();
        wire.extend_from_slice(&flatten(&data));
    }
    peer.write_all(&wire).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while !connection.inbound_blocked.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send.cancel_handle().cancel();

    let second = connection.open_stream().await.unwrap();
    let (mut second_send, mut second_recv) = second.split();
    assert_eq!(second_recv.id().as_u32(), 3);
    assert!(second_send.send_headers(request_headers(), false).is_ok());
    let response = frame::encode(response_headers_frame(3, true)).unwrap();
    peer.write_all(&flatten(&response)).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(2), second_recv.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(event, ReceivedEvent::Headers { end_stream: true, .. }));
    assert!(!connection.inbound_blocked.load(Ordering::Acquire));
    assert_eq!(connection.state.load(Ordering::Acquire), CONNECTION_OPEN);
    connection.close();
}

#[tokio::test]
async fn socket_eof_broadcasts_connection_error_to_waiting_receiver() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    drop(peer);
    let result = tokio::time::timeout(Duration::from_secs(2), recv.recv()).await.unwrap();
    assert!(matches!(result, Err(RecvStreamError::ConnectionClosed)));
}

#[tokio::test]
async fn continuation_fragments_are_decoded_as_one_header_event() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    let mut headers = HeaderBlock::new();
    headers.pseudo.status = Some(200);
    headers.push(super::super::hpack::HeaderField {
        name: Bytes::from_static(b"content-type"),
        value: Bytes::from_static(b"application/grpc"),
        indexing: super::super::hpack::HeaderIndexing::WithoutIndexing,
    });
    let mut block = HpackEncoder::new(HpackConfig::default()).encode(&headers).unwrap();
    let first_payload = block.split_to(1);
    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let first = frame::encode(Frame {
        header: FrameHeader {
            length: first_payload.len() as u32,
            kind: FrameKind::Headers,
            flags: FrameFlags::new(FrameFlags::END_STREAM),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: first_payload,
    })
    .unwrap();
    let continuation = frame::encode(Frame {
        header: FrameHeader {
            length: block.len() as u32,
            kind: FrameKind::Continuation,
            flags: FrameFlags::new(FrameFlags::END_HEADERS),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: block,
    })
    .unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&first));
    wire.extend_from_slice(&flatten(&continuation));
    peer.write_all(&wire).await.unwrap();

    let event =
        tokio::time::timeout(Duration::from_secs(2), recv.recv()).await.unwrap().unwrap().unwrap();
    assert!(matches!(
        event,
        ReceivedEvent::Headers { headers, end_stream: true, trailers: false }
            if headers.pseudo.status == Some(200)
                && headers.fields().any(|field| field.name == "content-type")
    ));
    assert!(recv.recv().await.unwrap().is_none());
    connection.close();
}

#[tokio::test]
async fn interleaved_frame_during_continuation_fails_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let stream = connection.open_stream().await.unwrap();
    let (_send, mut recv) = stream.split();

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let headers = frame::encode(Frame {
        header: FrameHeader {
            length: 0,
            kind: FrameKind::Headers,
            flags: FrameFlags::default(),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: IoBuf::new(),
    })
    .unwrap();
    let data = frame::encode(Frame {
        header: FrameHeader {
            length: 0,
            kind: FrameKind::Data,
            flags: FrameFlags::new(FrameFlags::END_STREAM),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: IoBuf::new(),
    })
    .unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&headers));
    wire.extend_from_slice(&flatten(&data));
    peer.write_all(&wire).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), recv.recv()).await.unwrap();
    assert!(matches!(result, Err(RecvStreamError::ProtocolError)));
}

#[tokio::test]
async fn reset_only_fails_its_target_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let socket = TcpSocket::resource_pool()
        .acquire_with_init(|| TcpSocket::new(TcpTransport::new(client)))
        .unwrap();
    let connection = H2Connection::new(super::ConnectionRole::Client, socket);
    let first = connection.open_stream().await.unwrap();
    let second = connection.open_stream().await.unwrap();
    let (_first_send, mut first_recv) = first.split();
    let (_second_send, mut second_recv) = second.split();

    let settings = frame::encode(Settings::default().frame(false)).unwrap();
    let reset = frame::encode(Frame {
        header: FrameHeader {
            length: 4,
            kind: FrameKind::RstStream,
            flags: FrameFlags::default(),
            stream_id: StreamId::new(1).unwrap(),
        },
        payload: payload(&8_u32.to_be_bytes()),
    })
    .unwrap();
    let second_headers = frame::encode(response_headers_frame(3, false)).unwrap();
    let second_data = frame::encode(Frame {
        header: FrameHeader {
            length: 2,
            kind: FrameKind::Data,
            flags: FrameFlags::new(FrameFlags::END_STREAM),
            stream_id: StreamId::new(3).unwrap(),
        },
        payload: payload(b"ok"),
    })
    .unwrap();
    let mut wire = flatten(&settings);
    wire.extend_from_slice(&flatten(&reset));
    wire.extend_from_slice(&flatten(&second_headers));
    wire.extend_from_slice(&flatten(&second_data));
    peer.write_all(&wire).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), first_recv.recv()).await.unwrap();
    assert!(matches!(result, Err(RecvStreamError::Reset(8))));
    assert!(matches!(second_recv.recv().await.unwrap().unwrap(), ReceivedEvent::Headers { .. }));
    let second_result = second_recv.recv().await.unwrap().unwrap();
    let ReceivedEvent::Data(second_result) = second_result else { panic!("expected DATA") };
    assert_eq!(flatten(&second_result.payload), b"ok");
    assert_eq!(connection.state.load(Ordering::Acquire), CONNECTION_OPEN);
    connection.close();
}
