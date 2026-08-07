// SPDX-License-Identifier: Apache-2.0

//! Caller-first HTTP/2 connection and first-class stream primitives.
//!
//! Protocol state has one owner at a time, but that owner is not a fixed task.
//! A request submitter or an I/O continuation can acquire the connection and
//! make progress inline. Contending producers publish immutable work and let
//! the current owner combine it into the same write batch.

use std::cell::UnsafeCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{self, IoSlice, IoSliceMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use bytes::{Buf, BufMut};
use crossbeam_queue::{ArrayQueue, SegQueue};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::frame::{self, FrameFlags, FrameHeader, FrameKind, StreamId};
use super::hpack::{HeaderBlock, HpackConfig, HpackDecoder, HpackEncoder};
use super::settings::Settings;
use super::{CLIENT_PREFACE, ErrorCode, Frame};
use crate::runtime::profile::{self, ProfileEvent};
use crate::transport::tcp::TcpSocket;
use crate::{IoBuf, IoBufBuilder, Resource, ResourceId, StaticResource, Transport, UninitResource};

const NULL_ID: u64 = u64::MAX;

const CONNECTION_OPEN: u8 = 1;
const CONNECTION_DRAINING: u8 = 2;
const CONNECTION_CLOSED: u8 = 3;
const STREAM_SEND_CLOSED: u8 = 1 << 0;
const STREAM_LOCAL_CLOSED: u8 = 1 << 1;
const STREAM_REMOTE_CLOSED: u8 = 1 << 2;
const STREAM_FAILED: u8 = 1 << 3;
const STREAM_WIRE_OPEN: u8 = 1 << 4;
const SEND_ENDPOINT_DROPPED: u8 = 1 << 0;
const RECV_ENDPOINT_DROPPED: u8 = 1 << 1;

const OUTBOUND_ITEMS: usize = 32;
const INBOUND_ITEMS: usize = 32;
const INBOUND_HEADER_ITEMS: usize = 4;
const PENDING_COMMANDS: usize = 1024;
const INLINE_COMMANDS: usize = 8;
// Let producer-owned turns reach the normal async byte budget, while the
// stream and command caps still bound inline monopolization.
const INLINE_STREAMS: usize = 16;
const INLINE_BYTES: usize = 256 * 1024;
const ASYNC_COMMANDS: usize = 256;
const ASYNC_STREAMS: usize = 128;
const ASYNC_BYTES: usize = 256 * 1024;
// Large enough for the normal inline batch; larger values did not improve
// throughput and increase the stack-resident IoSlice array.
const WRITEV_SLICES: usize = 64;
const READV_SLICES: usize = 16;
const READ_BYTES: usize = 256 * 1024;
const READ_FRAMES: usize = 128;
const INITIAL_WINDOW_SIZE: i64 = 65_535;
const MAX_WINDOW_SIZE: i64 = 0x7fff_ffff;
const STREAM_DATA_QUANTUM: usize = 16 * 1024;
const LOCAL_STREAM_RECV_WINDOW: usize = 1024 * 1024;
const LOCAL_CONNECTION_RECV_WINDOW: usize = 16 * 1024 * 1024;
const STREAM_WINDOW_UPDATE_THRESHOLD: usize = LOCAL_STREAM_RECV_WINDOW / 2;
const CONNECTION_WINDOW_UPDATE_THRESHOLD: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionRole {
    Client,
    Server,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct H2ServerLimits {
    pub max_header_size: usize,
    pub max_body_size: usize,
    pub max_concurrent_streams: usize,
}

#[derive(Clone, Copy)]
struct DriveQuota {
    commands: usize,
    streams: usize,
    bytes: usize,
}

impl DriveQuota {
    const INLINE: Self =
        Self { commands: INLINE_COMMANDS, streams: INLINE_STREAMS, bytes: INLINE_BYTES };
    const ASYNC: Self =
        Self { commands: ASYNC_COMMANDS, streams: ASYNC_STREAMS, bytes: ASYNC_BYTES };
}

/// A physical HTTP/2 connection bound to exactly one TCP socket generation.
///
/// `core` is protected by `owner`, not by a mutex. The successful owner may be
/// the caller submitting a stream operation or an asynchronous I/O
/// continuation. No owner is allowed to await while it holds the baton.
#[derive(StaticResource)]
pub(crate) struct H2Connection {
    id: AtomicU64,
    state: AtomicU8,
    owner: AtomicBool,
    notified: AtomicBool,
    yield_requested: AtomicBool,
    write_waiting: AtomicBool,
    read_ready: AtomicBool,
    inbound_blocked: AtomicBool,
    driver_notify: Notify,
    commands: ArrayQueue<ConnectionCommand>,
    accepted_streams: ArrayQueue<AcceptedStream>,
    accept_waker: Mutex<Option<Waker>>,
    abandoned_streams: SegQueue<StreamId>,
    ready_streams: SegQueue<Resource<H2StreamShared>>,
    ready_credits: SegQueue<Resource<H2StreamShared>>,
    recv_consumed: AtomicUsize,
    credit_notified: AtomicBool,
    socket: Resource<TcpSocket>,
    core: UnsafeCell<H2Core>,
}

// Every mutation of H2Core is guarded by the owner CAS. Queues and stream
// publication state provide their own synchronization.
unsafe impl Send for H2Connection {}
unsafe impl Sync for H2Connection {}

struct H2Core {
    role: ConnectionRole,
    next_local_stream_id: u32,
    next_wire_stream_id: u32,
    last_peer_stream_id: u32,
    // Highest locally initiated stream whose opening headers entered the wire
    // batch. Allocating/canceling a stream alone does not leave the idle state.
    last_local_stream_id: u32,
    streams: HashMap<StreamId, Resource<H2StreamShared>>,
    pending_opens: VecDeque<PendingOpen>,
    control_frames: VecDeque<Frame>,
    write_batch: IoBuf,
    write_segments: VecDeque<WriteSegment>,
    input: IoBuf,
    peer_settings: Settings,
    peer_settings_received: bool,
    hpack_encoder: HpackEncoder,
    hpack_decoder: HpackDecoder,
    continuation: Option<HeaderContinuation>,
    blocked_inbound: Option<(Resource<H2StreamShared>, ReceivedEvent)>,
    peer_goaway_last_stream_id: Option<StreamId>,
    connection_send_window: i64,
    connection_recv_window: i64,
    max_body_size: Option<usize>,
    max_peer_streams: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteClass {
    Application,
    Control,
}

struct WriteSegment {
    remaining: usize,
    class: WriteClass,
}

struct HeaderContinuation {
    stream_id: StreamId,
    block: IoBuf,
    end_stream: bool,
}

struct StreamOwnerState {
    send_window: i64,
    recv_window: i64,
    initial_send: Option<H2DirectSend>,
    pending_data: Option<PendingData>,
    pending_trailers: Option<HeaderBlock>,
    flow_blocked: bool,
    driver: Option<H2StreamDriver>,
    received_body_bytes: usize,
}

struct PendingData {
    payload: IoBuf,
    end_stream: bool,
}

struct DataRemainder {
    pending: PendingData,
    flow_blocked: bool,
}

enum ConnectionCommand {
    OpenStream(PendingOpen),
}

enum PendingOpen {
    #[cfg_attr(not(test), allow(dead_code))]
    Future(Arc<OpenCompletion>),
    Driven(H2StreamDriver),
    Handoff(H2StreamHandoff),
}

/// One-shot stream-open callback used by streaming RPCs. Unlike
/// `H2StreamDriver`, ownership of the complete stream is transferred to the
/// application and subsequent inbound events use the stream's bounded queue.
pub(crate) struct H2StreamHandoff {
    token: u64,
    active: fn(u64) -> bool,
    opened: fn(u64, H2Stream),
    rejected: fn(u64, OpenStreamError),
    release: fn(u64),
}

/// Type-erased application state driven directly by the HTTP/2 connection
/// owner. The token owns one application-state reference; `release` releases
/// it exactly once when the stream or queued open is destroyed.
///
/// Callbacks are serialized by the connection owner. They must not block or
/// await, but may publish more work to this connection.
pub(crate) struct H2StreamDriver {
    token: u64,
    active: fn(u64) -> bool,
    opened: fn(u64, H2StreamCancel) -> Option<H2DirectSend>,
    event: fn(u64, ReceivedEvent) -> H2StreamDisposition,
    rejected: fn(u64, OpenStreamError),
    failed: fn(u64, RecvStreamError),
    release: fn(u64),
}

/// The complete request side of an owner-driven unary stream. The H2 owner
/// consumes it directly, so unary calls do not allocate a streaming mailbox.
pub(crate) struct H2DirectSend {
    headers: HeaderBlock,
    payload: Option<IoBuf>,
    trailers: Option<HeaderBlock>,
}

impl H2DirectSend {
    pub(crate) fn complete(headers: HeaderBlock, payload: IoBuf) -> Self {
        Self { headers, payload: Some(payload), trailers: None }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum H2StreamDisposition {
    Continue,
    Complete,
    Cancel,
}

// Keeping headers inline avoids a heap allocation on every stream open.
#[allow(clippy::large_enum_variant)]
enum OutboundItem {
    Headers { headers: HeaderBlock, end_stream: bool },
    Data { payload: IoBuf, end_stream: bool },
}

pub(crate) struct ReceivedData {
    pub payload: IoBuf,
    pub end_stream: bool,
    _credit: FlowCredit,
}

impl ReceivedData {
    pub(crate) fn into_parts(self) -> (IoBuf, bool) {
        (self.payload, self.end_stream)
    }

    pub(crate) fn into_parts_with_credit(self) -> (IoBuf, bool, FlowCredit) {
        (self.payload, self.end_stream, self._credit)
    }
}

pub(crate) struct FlowCredit {
    connection: ResourceId<H2Connection>,
    stream: ResourceId<H2StreamShared>,
    bytes: usize,
}

// Keeping headers inline avoids a heap allocation on every received header
// block.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ReceivedEvent {
    Headers { headers: HeaderBlock, end_stream: bool, trailers: bool },
    Data(ReceivedData),
}

struct ReceivedHeader {
    headers: HeaderBlock,
    end_stream: bool,
}

#[derive(Default)]
struct InboundSlots {
    header: Option<ReceivedHeader>,
    data: Option<ReceivedData>,
    trailers: Option<ReceivedHeader>,
}

#[derive(StaticResource)]
struct H2StreamShared {
    id: StreamId,
    connection: ResourceId<H2Connection>,
    state: AtomicU8,
    dropped_endpoints: AtomicU8,
    headers_sent: AtomicBool,
    headers_received: AtomicBool,
    accepted: AtomicBool,
    write_ready: AtomicBool,
    // `write_ready` says the stream has producer or owner-local work. Keep
    // queue membership separate so owner-side wakeups cannot publish the
    // same stream repeatedly before its first queue entry is consumed.
    ready_queued: AtomicBool,
    send_waiting: AtomicBool,
    send_waker: Mutex<Option<Waker>>,
    outbound: OnceLock<Box<ArrayQueue<OutboundItem>>>,
    inbound_slots: Mutex<InboundSlots>,
    inbound_headers: OnceLock<Box<ArrayQueue<ReceivedHeader>>>,
    inbound_data_queue: OnceLock<Box<ArrayQueue<ReceivedData>>>,
    complete_send: Mutex<Option<H2DirectSend>>,
    recv_failure: Mutex<Option<RecvStreamError>>,
    cancellation: Mutex<Option<CancellationToken>>,
    recv_waker: Mutex<Option<Waker>>,
    recv_consumed: AtomicUsize,
    credit_ready: AtomicBool,
    owner: UnsafeCell<StreamOwnerState>,
}

// `owner` is accessed only while the parent connection owner CAS is held.
// Producers interact exclusively through atomics and concurrent queues.
unsafe impl Send for H2StreamShared {}
unsafe impl Sync for H2StreamShared {}

/// First-class HTTP/2 stream. Splitting it produces exactly one send half and
/// one receive half; neither half is cloneable.
pub(crate) struct H2Stream {
    shared: Resource<H2StreamShared>,
    connection: Resource<H2Connection>,
    split: bool,
}

/// A peer-initiated server stream together with the initial request headers.
///
/// Initial headers are decoded by the connection owner before publication so
/// the server can classify HTTP and gRPC synchronously before spawning a
/// protocol-specific handler task.
pub(crate) struct AcceptedStream {
    pub(crate) stream: H2Stream,
    pub(crate) headers: HeaderBlock,
    pub(crate) end_stream: bool,
}

pub(crate) struct H2SendStream {
    shared: Resource<H2StreamShared>,
    connection: Resource<H2Connection>,
}

#[derive(Clone, Copy)]
pub(crate) struct H2StreamCancel {
    connection: ResourceId<H2Connection>,
    stream_id: StreamId,
}

pub(crate) struct H2RecvStream {
    shared: Resource<H2StreamShared>,
    connection: Resource<H2Connection>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum OpenStreamError {
    ConnectionClosed,
    ConnectionDraining,
    Busy,
}

// Error values retain the unsent payload so future retrying callers do not
// clone it.
#[allow(dead_code)]
pub(crate) enum SendStreamError {
    Closed(IoBuf),
    HeadersRequired(IoBuf),
    Full(IoBuf),
}

// Errors return ownership so callers can retry without cloning a header block.
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
pub(crate) enum SendHeadersError {
    Closed(HeaderBlock),
    AlreadySent(HeaderBlock),
    Full(HeaderBlock),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SendBatchError {
    Closed,
    HeadersAlreadySent,
    Full,
    InvalidTrailers,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RecvStreamError {
    ConnectionClosed,
    ResourceExhausted,
    ProtocolError,
    Reset(u32),
    UnprocessedByPeer(u32),
}

pub(crate) struct RecvEventFuture<'a> {
    stream: &'a mut H2RecvStream,
}

#[cfg_attr(not(test), allow(dead_code))]
struct OpenCompletion {
    result: Mutex<Option<Result<H2Stream, OpenStreamError>>>,
    waker: Mutex<Option<Waker>>,
    canceled: AtomicBool,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct OpenStreamFuture {
    completion: Arc<OpenCompletion>,
}

pub(crate) struct AcceptStreamFuture<'a> {
    connection: &'a H2Connection,
}

impl H2StreamDriver {
    pub(crate) fn new(
        token: u64,
        active: fn(u64) -> bool,
        opened: fn(u64, H2StreamCancel) -> Option<H2DirectSend>,
        event: fn(u64, ReceivedEvent) -> H2StreamDisposition,
        rejected: fn(u64, OpenStreamError),
        failed: fn(u64, RecvStreamError),
        release: fn(u64),
    ) -> Self {
        Self { token, active, opened, event, rejected, failed, release }
    }

    fn is_active(&self) -> bool {
        (self.active)(self.token)
    }

    fn opened(&self, cancel: H2StreamCancel) -> Option<H2DirectSend> {
        (self.opened)(self.token, cancel)
    }

    fn event(&self, event: ReceivedEvent) -> H2StreamDisposition {
        (self.event)(self.token, event)
    }

    fn fail(&self, error: RecvStreamError) {
        (self.failed)(self.token, error);
    }

    pub(crate) fn reject(self, error: OpenStreamError) {
        (self.rejected)(self.token, error);
    }
}

impl H2StreamHandoff {
    pub(crate) fn new(
        token: u64,
        active: fn(u64) -> bool,
        opened: fn(u64, H2Stream),
        rejected: fn(u64, OpenStreamError),
        release: fn(u64),
    ) -> Self {
        Self { token, active, opened, rejected, release }
    }

    fn is_active(&self) -> bool {
        (self.active)(self.token)
    }

    fn opened(self, stream: H2Stream) {
        (self.opened)(self.token, stream);
    }

    pub(crate) fn reject(self, error: OpenStreamError) {
        (self.rejected)(self.token, error);
    }
}

impl Drop for H2StreamHandoff {
    fn drop(&mut self) {
        (self.release)(self.token);
    }
}

impl Drop for H2StreamDriver {
    fn drop(&mut self) {
        (self.release)(self.token);
    }
}

impl PendingOpen {
    fn is_active(&self) -> bool {
        match self {
            Self::Future(completion) => !completion.is_canceled(),
            Self::Driven(driver) => driver.is_active(),
            Self::Handoff(handoff) => handoff.is_active(),
        }
    }

    fn fail(self, error: OpenStreamError) {
        match self {
            Self::Future(completion) => completion.complete(Err(error)),
            Self::Driven(driver) => driver.reject(error),
            Self::Handoff(handoff) => handoff.reject(error),
        }
    }
}

enum DriveOutcome {
    Idle,
    More,
    WriteBlocked,
    Closed,
}

impl H2Connection {
    #[cfg(test)]
    pub(crate) fn new(role: ConnectionRole, socket: Resource<TcpSocket>) -> Resource<Self> {
        Self::try_new(role, socket).expect("test HTTP/2 connection allocation must succeed")
    }

    pub(crate) fn try_new(
        role: ConnectionRole,
        socket: Resource<TcpSocket>,
    ) -> Result<Resource<Self>, RecvStreamError> {
        Self::with_input(role, socket, IoBuf::new(), None)
    }

    fn resource(&self) -> Option<Resource<Self>> {
        Self::resource_pool().get(h2_connection_id(self.id.load(Ordering::Acquire)))
    }

    fn resource_id(&self) -> ResourceId<Self> {
        h2_connection_id(self.id.load(Ordering::Acquire))
    }

    fn mark_work(&self) {
        self.notified.store(true, Ordering::Release);
    }

    // Waking Tokio is reserved for work that the current thread cannot drive:
    // socket readiness, a quota yield, or an owner parked behind writable I/O.
    fn wake_driver(&self) {
        self.driver_notify.notify_one();
    }

    fn notify_driver(&self) {
        self.mark_work();
        self.wake_driver();
    }

    pub(crate) fn new_server(
        socket: Resource<TcpSocket>,
        input: IoBuf,
        limits: H2ServerLimits,
    ) -> Result<Resource<Self>, RecvStreamError> {
        Self::with_input(ConnectionRole::Server, socket, input, Some(limits))
    }

    fn with_input(
        role: ConnectionRole,
        socket: Resource<TcpSocket>,
        mut input: IoBuf,
        server_limits: Option<H2ServerLimits>,
    ) -> Result<Resource<Self>, RecvStreamError> {
        let next_local_stream_id = match role {
            ConnectionRole::Client => 1,
            ConnectionRole::Server => 2,
        };
        if role == ConnectionRole::Server {
            if input.len() < CLIENT_PREFACE.len()
                || !input
                    .chunks()
                    .flatten()
                    .take(CLIENT_PREFACE.len())
                    .copied()
                    .eq(CLIENT_PREFACE.iter().copied())
            {
                return Err(RecvStreamError::ProtocolError);
            }
            input.consume(CLIENT_PREFACE.len());
        }
        let mut write_batch = IoBuf::new();
        let mut write_segments = VecDeque::new();
        if role == ConnectionRole::Client {
            let mut builder = IoBufBuilder::new();
            builder.put_slice(CLIENT_PREFACE);
            write_batch = builder.finish();
            write_segments.push_back(WriteSegment {
                remaining: CLIENT_PREFACE.len(),
                class: WriteClass::Control,
            });
        }
        let mut local_settings = Settings {
            initial_window_size: LOCAL_STREAM_RECV_WINDOW as u32,
            ..Settings::default()
        };
        let mut hpack_config = HpackConfig::default();
        if let Some(limits) = server_limits {
            local_settings.max_concurrent_streams =
                Some(limits.max_concurrent_streams.min(u32::MAX as usize) as u32);
            local_settings.max_header_list_size =
                Some(limits.max_header_size.min(u32::MAX as usize) as u32);
            hpack_config.max_header_block_size = limits.max_header_size;
            hpack_config.max_header_list_size = limits.max_header_size;
        }
        let mut control_frames = VecDeque::new();
        // RFC 9113 permits SETTINGS_ENABLE_PUSH only from a client. We send
        // zero explicitly because PUSH_PROMISE is intentionally unsupported.
        control_frames.push_back(local_settings.frame(role == ConnectionRole::Client));
        control_frames.push_back(Self::window_update_frame(
            StreamId::CONNECTION,
            (LOCAL_CONNECTION_RECV_WINDOW - INITIAL_WINDOW_SIZE as usize) as u32,
        ));
        let input_ready = !input.is_empty();
        let connection = Self::resource_pool()
            .acquire_with_init(|| Self {
                id: AtomicU64::new(NULL_ID),
                state: AtomicU8::new(CONNECTION_OPEN),
                owner: AtomicBool::new(false),
                notified: AtomicBool::new(true),
                yield_requested: AtomicBool::new(false),
                write_waiting: AtomicBool::new(false),
                read_ready: AtomicBool::new(input_ready),
                inbound_blocked: AtomicBool::new(false),
                driver_notify: Notify::new(),
                commands: ArrayQueue::new(PENDING_COMMANDS),
                accepted_streams: ArrayQueue::new(PENDING_COMMANDS),
                accept_waker: Mutex::new(None),
                abandoned_streams: SegQueue::new(),
                ready_streams: SegQueue::new(),
                ready_credits: SegQueue::new(),
                recv_consumed: AtomicUsize::new(0),
                credit_notified: AtomicBool::new(false),
                socket,
                core: UnsafeCell::new(H2Core {
                    role,
                    next_local_stream_id,
                    next_wire_stream_id: next_local_stream_id,
                    last_peer_stream_id: 0,
                    last_local_stream_id: 0,
                    streams: HashMap::new(),
                    pending_opens: VecDeque::new(),
                    control_frames,
                    write_batch,
                    write_segments,
                    input,
                    peer_settings: Settings::default(),
                    peer_settings_received: false,
                    hpack_encoder: HpackEncoder::new(HpackConfig::default()),
                    hpack_decoder: HpackDecoder::new(hpack_config),
                    continuation: None,
                    blocked_inbound: None,
                    peer_goaway_last_stream_id: None,
                    connection_send_window: INITIAL_WINDOW_SIZE,
                    connection_recv_window: LOCAL_CONNECTION_RECV_WINDOW as i64,
                    max_body_size: server_limits.map(|limits| limits.max_body_size),
                    max_peer_streams: server_limits
                        .map_or(PENDING_COMMANDS, |limits| limits.max_concurrent_streams.max(1)),
                }),
            })
            .map_err(|_| RecvStreamError::ResourceExhausted)?;
        connection.id.store(connection.id().as_u64(), Ordering::Release);
        log::debug!(
            "HTTP/2 connection opened: socket={}, role={role:?}",
            connection.socket.id().as_u64()
        );
        Self::try_drive(connection.clone(), DriveQuota::INLINE);
        Self::start_driver(connection.clone());
        Ok(connection)
    }

    pub(crate) fn accept_stream(&self) -> AcceptStreamFuture<'_> {
        AcceptStreamFuture { connection: self }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open_stream(&self) -> OpenStreamFuture {
        let completion = Arc::new(OpenCompletion {
            result: Mutex::new(None),
            waker: Mutex::new(None),
            canceled: AtomicBool::new(false),
        });
        match self.state.load(Ordering::Acquire) {
            CONNECTION_OPEN => {}
            CONNECTION_DRAINING => {
                completion.complete(Err(OpenStreamError::ConnectionDraining));
                return OpenStreamFuture { completion };
            }
            _ => {
                completion.complete(Err(OpenStreamError::ConnectionClosed));
                return OpenStreamFuture { completion };
            }
        }
        if self
            .commands
            .push(ConnectionCommand::OpenStream(PendingOpen::Future(completion.clone())))
            .is_err()
        {
            completion.complete(Err(OpenStreamError::Busy));
            return OpenStreamFuture { completion };
        }
        self.mark_work();
        if let Some(connection) = self.resource() {
            Self::try_drive(connection, DriveQuota::INLINE);
        }
        OpenStreamFuture { completion }
    }

    /// Publishes an application-driven stream without creating or polling a
    /// per-stream Future. If this call acquires the connection owner, opening
    /// the stream and writing its initial batch happen inline.
    /// Publishes a driven stream without immediately acquiring the connection
    /// owner. A caller that owns an application batch can enqueue several
    /// streams and drive the connection once.
    pub(crate) fn publish_stream(
        &self,
        driver: H2StreamDriver,
    ) -> Result<(), (OpenStreamError, H2StreamDriver)> {
        match self.state.load(Ordering::Acquire) {
            CONNECTION_OPEN => {}
            CONNECTION_DRAINING => {
                return Err((OpenStreamError::ConnectionDraining, driver));
            }
            _ => return Err((OpenStreamError::ConnectionClosed, driver)),
        }
        if let Err(ConnectionCommand::OpenStream(PendingOpen::Driven(driver))) =
            self.commands.push(ConnectionCommand::OpenStream(PendingOpen::Driven(driver)))
        {
            return Err((OpenStreamError::Busy, driver));
        }
        self.mark_work();
        Ok(())
    }

    pub(crate) fn publish_handoff(
        &self,
        handoff: H2StreamHandoff,
    ) -> Result<(), (OpenStreamError, H2StreamHandoff)> {
        match self.state.load(Ordering::Acquire) {
            CONNECTION_OPEN => {}
            CONNECTION_DRAINING => {
                return Err((OpenStreamError::ConnectionDraining, handoff));
            }
            _ => return Err((OpenStreamError::ConnectionClosed, handoff)),
        }
        if let Err(ConnectionCommand::OpenStream(PendingOpen::Handoff(handoff))) =
            self.commands.push(ConnectionCommand::OpenStream(PendingOpen::Handoff(handoff)))
        {
            return Err((OpenStreamError::Busy, handoff));
        }
        self.mark_work();
        Ok(())
    }

    pub(crate) fn drive_published(&self) {
        self.mark_work();
        if let Some(connection) = self.resource() {
            Self::try_drive(connection, DriveQuota::INLINE);
        }
    }

    pub(crate) fn is_accepting_streams(&self) -> bool {
        self.state.load(Ordering::Acquire) == CONNECTION_OPEN
    }

    pub(crate) fn close(&self) {
        if self.state.swap(CONNECTION_CLOSED, Ordering::AcqRel) == CONNECTION_CLOSED {
            return;
        }
        self.notify_driver();
        self.shutdown_socket();
        if let Some(connection) = self.resource() {
            Self::try_drive(connection, DriveQuota::INLINE);
        }
    }

    fn publish_ready(&self, stream: Resource<H2StreamShared>) {
        if !self.enqueue_ready(stream) {
            return;
        }
        if !self.write_waiting.load(Ordering::Acquire) {
            let acquired = self
                .resource()
                .is_some_and(|connection| Self::try_drive(connection, DriveQuota::INLINE));
            if profile::enabled() {
                profile::record_event(
                    if acquired {
                        ProfileEvent::H2PublishInlineOwner
                    } else {
                        ProfileEvent::H2PublishOwnerBusy
                    },
                    1,
                );
            }
        } else {
            self.wake_driver();
        }
    }

    fn enqueue_ready(&self, stream: Resource<H2StreamShared>) -> bool {
        if stream
            .ready_queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            if profile::enabled() {
                profile::record_event(ProfileEvent::H2ReadyDuplicateSuppressed, 1);
            }
            return false;
        }
        self.ready_streams.push(stream);
        self.mark_work();
        true
    }

    fn try_drive(connection: Resource<Self>, quota: DriveQuota) -> bool {
        if connection
            .owner
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        Self::drive_owned(connection, quota);
        true
    }

    fn drive_owned(connection: Resource<Self>, quota: DriveQuota) {
        let outcome = {
            // SAFETY: this invocation owns the connection baton until it
            // releases `owner` below and never awaits while borrowing core.
            let core = unsafe { &mut *connection.core.get() };
            connection.drive_core(core, quota)
        };

        connection.owner.store(false, Ordering::Release);
        match outcome {
            DriveOutcome::Idle => {
                if connection.has_immediate_work() {
                    Self::try_drive(connection.clone(), DriveQuota::INLINE);
                }
            }
            DriveOutcome::More => {
                connection.yield_requested.store(true, Ordering::Release);
                // `More` may be caused solely by bytes left in write_batch,
                // which is owner-private and therefore invisible to
                // has_immediate_work(). Publish an explicit drive signal.
                connection.notify_driver();
            }
            DriveOutcome::WriteBlocked => {
                connection.write_waiting.store(true, Ordering::Release);
                connection.driver_notify.notify_one();
            }
            DriveOutcome::Closed => {
                connection.fail_queued_commands();
                while connection.accepted_streams.pop().is_some() {}
                if let Some(waker) = connection.accept_waker.lock().unwrap().take() {
                    waker.wake();
                }
            }
        }
    }

    fn drive_core(&self, core: &mut H2Core, quota: DriveQuota) -> DriveOutcome {
        if self.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            self.close_core(core, RecvStreamError::ConnectionClosed);
            return DriveOutcome::Closed;
        }
        self.notified.store(false, Ordering::Release);

        if let Some((stream, event)) = core.blocked_inbound.take() {
            match stream.push_inbound(event) {
                Ok(()) => {
                    self.inbound_blocked.store(false, Ordering::Release);
                    stream.wake_receiver();
                }
                Err(event) => core.blocked_inbound = Some((stream, event)),
            }
        }

        if core.blocked_inbound.is_none()
            && self.read_ready.swap(false, Ordering::AcqRel)
            && let Err(error) = self.read_available(core)
        {
            if error == RecvStreamError::ProtocolError {
                log::warn!(
                    "HTTP/2 inbound protocol failure: socket={}, error={error:?}",
                    self.socket.id().as_u64()
                );
            }
            self.state.store(CONNECTION_CLOSED, Ordering::Release);
            self.shutdown_socket();
            self.close_core(core, error);
            return DriveOutcome::Closed;
        }
        Self::drain_receive_credits(core, self);
        Self::drain_abandoned_streams(core, self);
        self.reap_terminal_streams(core);
        core.pending_opens.retain(PendingOpen::is_active);

        let mut commands = 0;
        while commands < quota.commands {
            let Some(command) = self.commands.pop() else { break };
            match command {
                ConnectionCommand::OpenStream(open) => {
                    if !open.is_active() {
                        continue;
                    }
                    if self.state.load(Ordering::Acquire) != CONNECTION_OPEN {
                        open.fail(OpenStreamError::ConnectionDraining);
                        continue;
                    }
                    if core.pending_opens.len() == PENDING_COMMANDS {
                        open.fail(OpenStreamError::Busy);
                    } else {
                        core.pending_opens.push_back(open);
                    }
                }
            }
            commands += 1;
        }
        self.service_pending_opens(core);

        // Opening a stream is local bookkeeping and must stay cheap even
        // while the kernel send buffer is full. Do not, however, keep
        // rebuilding batches or retrying the same non-writable socket.
        if self.write_waiting.load(Ordering::Acquire) {
            return DriveOutcome::WriteBlocked;
        }

        if let Err(error) = Self::fill_control_frames(core) {
            log::error!(
                "HTTP/2 control frame encode failed: socket={}, error={error:?}",
                self.socket.id().as_u64()
            );
            self.state.store(CONNECTION_CLOSED, Ordering::Release);
            self.shutdown_socket();
            self.close_core(core, RecvStreamError::ProtocolError);
            return DriveOutcome::Closed;
        }
        let mut streams = 0;
        while streams < quota.streams && core.write_batch.len() < quota.bytes {
            let Some(stream) = self.ready_streams.pop() else { break };
            stream.ready_queued.store(false, Ordering::Release);
            let stream_id = stream.id;
            let batch_len = core.write_batch.len();
            if let Err(error) = self.drain_stream(core, stream) {
                log::error!(
                    "HTTP/2 outbound stream encode failed: socket={}, stream={stream_id:?}, error={error:?}",
                    self.socket.id().as_u64()
                );
                self.state.store(CONNECTION_CLOSED, Ordering::Release);
                self.shutdown_socket();
                self.close_core(core, RecvStreamError::ProtocolError);
                return DriveOutcome::Closed;
            }
            if core.write_batch.len() == batch_len && profile::enabled() {
                profile::record_event(ProfileEvent::H2ReadyDrainNoOutput, 1);
            }
            streams += 1;
        }
        if streams != 0 && profile::enabled() {
            profile::record_event(ProfileEvent::H2DriveReadyStreams, streams as u64);
            profile::record_event(
                match core.role {
                    ConnectionRole::Client => ProfileEvent::H2ClientDriveReadyStreams,
                    ConnectionRole::Server => ProfileEvent::H2ServerDriveReadyStreams,
                },
                streams as u64,
            );
        }
        self.reap_terminal_streams(core);
        self.service_pending_opens(core);

        match self.flush(core, quota.bytes) {
            Ok(true) => {}
            Ok(false) => return DriveOutcome::WriteBlocked,
            Err(error) => {
                log::warn!(
                    "HTTP/2 write failed: socket={}, error={error}",
                    self.socket.id().as_u64()
                );
                self.state.store(CONNECTION_CLOSED, Ordering::Release);
                self.shutdown_socket();
                self.close_core(core, RecvStreamError::ConnectionClosed);
                return DriveOutcome::Closed;
            }
        }

        if self.has_immediate_work() || !core.write_batch.is_empty() {
            DriveOutcome::More
        } else {
            DriveOutcome::Idle
        }
    }

    fn read_available(&self, core: &mut H2Core) -> Result<(), RecvStreamError> {
        let mut total = 0;
        while total < READ_BYTES {
            let mut reservation = core.input.reserve::<READV_SLICES>();
            let result = {
                let mut slices: [IoSliceMut<'_>; READV_SLICES] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                let count = reservation.fill_io_slices(&mut slices);
                self.socket.transport().try_read_vectored(&mut slices[..count])
            };
            match result {
                Ok(0) => return Err(RecvStreamError::ConnectionClosed),
                Ok(read) => {
                    reservation.commit(read);
                    total += read;
                    if profile::enabled() {
                        profile::record_event(ProfileEvent::H2ReadOkBytes, read as u64);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    log::warn!(
                        "HTTP/2 read failed: socket={}, error={error}",
                        self.socket.id().as_u64()
                    );
                    return Err(RecvStreamError::ConnectionClosed);
                }
            }
        }

        let mut frames = 0;
        while frames < READ_FRAMES {
            let Some(frame) = frame::decode(&mut core.input, Settings::default().max_frame_size)
                .map_err(|error| {
                    log::warn!(
                        "HTTP/2 frame decode failed: socket={}, error={error:?}",
                        self.socket.id().as_u64()
                    );
                    RecvStreamError::ProtocolError
                })?
            else {
                break;
            };
            let header = frame.header;
            if let Err(error) = self.handle_inbound_frame(core, frame) {
                log::warn!(
                    "HTTP/2 inbound frame rejected: socket={}, role={:?}, kind={:?}, stream={:?}, length={}, flags={:?}, recv_window={}, error={error:?}",
                    self.socket.id().as_u64(),
                    core.role,
                    header.kind,
                    header.stream_id,
                    header.length,
                    header.flags,
                    core.connection_recv_window
                );
                return Err(error);
            }
            frames += 1;
            if core.blocked_inbound.is_some() {
                break;
            }
        }
        if frames == READ_FRAMES {
            self.read_ready.store(true, Ordering::Release);
            self.mark_work();
        }
        if frames != 0 && profile::enabled() {
            profile::record_event(ProfileEvent::H2ReadFrames, frames as u64);
        }
        Ok(())
    }

    fn handle_inbound_frame(&self, core: &mut H2Core, frame: Frame) -> Result<(), RecvStreamError> {
        if let Some(continuation) = core.continuation.as_ref() {
            if frame.header.kind != FrameKind::Continuation
                || frame.header.stream_id != continuation.stream_id
            {
                return Err(RecvStreamError::ProtocolError);
            }
        } else if frame.header.kind == FrameKind::Continuation {
            return Err(RecvStreamError::ProtocolError);
        }
        if !core.peer_settings_received
            && (frame.header.kind != FrameKind::Settings
                || frame.header.flags.contains(FrameFlags::ACK))
        {
            return Err(RecvStreamError::ProtocolError);
        }

        match frame.header.kind {
            FrameKind::Settings => {
                if !frame.header.stream_id.is_connection() {
                    return Err(RecvStreamError::ProtocolError);
                }
                if frame.header.flags.contains(FrameFlags::ACK) {
                    if !frame.payload.is_empty() {
                        return Err(RecvStreamError::ProtocolError);
                    }
                } else {
                    let previous_window = i64::from(core.peer_settings.initial_window_size);
                    let previous_header_table = core.peer_settings.header_table_size;
                    core.peer_settings
                        .apply_payload(&frame.payload)
                        .map_err(|_| RecvStreamError::ProtocolError)?;
                    if core.peer_settings.header_table_size != previous_header_table {
                        core.hpack_encoder.set_max_dynamic_table_size(
                            core.peer_settings.header_table_size as usize,
                        );
                    }
                    let window_delta =
                        i64::from(core.peer_settings.initial_window_size) - previous_window;
                    for stream in core.streams.values() {
                        // SAFETY: inbound frame handling runs under the
                        // connection owner and is the only accessor.
                        let owner = unsafe { &mut *stream.owner.get() };
                        owner.send_window = owner
                            .send_window
                            .checked_add(window_delta)
                            .ok_or(RecvStreamError::ProtocolError)?;
                        if owner.send_window > MAX_WINDOW_SIZE {
                            return Err(RecvStreamError::ProtocolError);
                        }
                        if window_delta > 0 {
                            Self::resume_flow_blocked(stream, core.connection_send_window);
                        }
                    }
                    core.peer_settings_received = true;
                    core.control_frames.push_back(super::settings::ack_frame());
                }
                Ok(())
            }
            FrameKind::Data => {
                if frame.header.stream_id.is_connection() {
                    return Err(RecvStreamError::ProtocolError);
                }
                let Some(stream) = core.streams.get(&frame.header.stream_id).cloned() else {
                    let local = frame.header.stream_id.is_client_initiated()
                        == (core.role == ConnectionRole::Client);
                    let last_opened =
                        if local { core.last_local_stream_id } else { core.last_peer_stream_id };
                    if frame.header.stream_id.as_u32() > last_opened {
                        // DATA on an idle stream is a connection error.
                        return Err(RecvStreamError::ProtocolError);
                    }
                    // A reset stream can still have DATA in flight. RFC 9113
                    // requires those bytes to count against connection flow
                    // control, but they no longer belong to an application
                    // stream and can be credited back immediately.
                    return Self::discard_closed_stream_data(core, frame.payload.len());
                };
                if stream.state.load(Ordering::Acquire) & (STREAM_REMOTE_CLOSED | STREAM_FAILED)
                    != 0
                {
                    return Self::discard_closed_stream_data(core, frame.payload.len());
                }
                if !stream.headers_received.load(Ordering::Acquire) {
                    return Err(RecvStreamError::ProtocolError);
                }
                let bytes = frame.payload.len();
                core.connection_recv_window -= bytes as i64;
                // SAFETY: inbound DATA handling runs under the connection owner.
                let owner = unsafe { &mut *stream.owner.get() };
                owner.recv_window -= bytes as i64;
                if core.connection_recv_window < 0 || owner.recv_window < 0 {
                    return Err(RecvStreamError::ProtocolError);
                }
                owner.received_body_bytes = owner
                    .received_body_bytes
                    .checked_add(bytes)
                    .ok_or(RecvStreamError::ResourceExhausted)?;
                if core.max_body_size.is_some_and(|limit| owner.received_body_bytes > limit) {
                    let credit =
                        FlowCredit { connection: stream.connection, stream: stream.id(), bytes };
                    drop(credit);
                    core.control_frames.push_back(Self::reset_frame(stream.id, ErrorCode::Cancel));
                    stream.fail_receiver(RecvStreamError::ResourceExhausted);
                    return Ok(());
                }
                let end_stream = frame.header.flags.contains(FrameFlags::END_STREAM);
                self.deliver_event(
                    core,
                    &stream,
                    ReceivedEvent::Data(ReceivedData {
                        payload: frame.payload,
                        end_stream,
                        _credit: FlowCredit {
                            connection: stream.connection,
                            stream: stream.id(),
                            bytes,
                        },
                    }),
                )?;
                if end_stream {
                    stream.state.fetch_or(STREAM_REMOTE_CLOSED, Ordering::AcqRel);
                }
                Ok(())
            }
            // These connection and header paths will be filled in alongside
            // flow control and HPACK. Silently accepting them would corrupt
            // protocol state, so the initial cooperative path fails closed.
            FrameKind::RstStream => Self::handle_reset(core, frame),
            FrameKind::Headers => self.handle_headers(core, frame),
            FrameKind::Continuation => self.handle_continuation(core, frame),
            FrameKind::Ping => Self::handle_ping(core, frame),
            FrameKind::GoAway => self.handle_goaway(core, frame),
            FrameKind::Priority | FrameKind::PushPromise => Err(RecvStreamError::ProtocolError),
            FrameKind::WindowUpdate => Self::handle_window_update(core, frame),
            FrameKind::Unknown(_) => Ok(()),
        }
    }

    fn discard_closed_stream_data(core: &mut H2Core, bytes: usize) -> Result<(), RecvStreamError> {
        core.connection_recv_window -= bytes as i64;
        if core.connection_recv_window < 0 {
            return Err(RecvStreamError::ProtocolError);
        }
        if bytes != 0 {
            core.connection_recv_window += bytes as i64;
            core.control_frames
                .push_back(Self::window_update_frame(StreamId::CONNECTION, bytes as u32));
        }
        Ok(())
    }

    fn handle_ping(core: &mut H2Core, mut frame: Frame) -> Result<(), RecvStreamError> {
        if !frame.header.stream_id.is_connection() || frame.payload.len() != 8 {
            return Err(RecvStreamError::ProtocolError);
        }
        if !frame.header.flags.contains(FrameFlags::ACK) {
            frame.header.flags = FrameFlags::new(FrameFlags::ACK);
            core.control_frames.push_back(frame);
        }
        Ok(())
    }

    fn handle_goaway(&self, core: &mut H2Core, frame: Frame) -> Result<(), RecvStreamError> {
        if !frame.header.stream_id.is_connection() || frame.payload.len() < 8 {
            return Err(RecvStreamError::ProtocolError);
        }
        let mut prefix = [0_u8; 8];
        for (target, source) in prefix.iter_mut().zip(frame.payload.chunks().flatten()) {
            *target = *source;
        }
        let last_stream_id =
            StreamId::new(u32::from_be_bytes(prefix[..4].try_into().unwrap()) & 0x7fff_ffff)
                .expect("masked stream id is valid");
        let error_code = u32::from_be_bytes(prefix[4..].try_into().unwrap());
        if core.peer_goaway_last_stream_id.is_some_and(|previous| last_stream_id > previous) {
            return Err(RecvStreamError::ProtocolError);
        }
        log::debug!(
            "HTTP/2 GOAWAY received: socket={}, last_stream={last_stream_id:?}, error_code={error_code}",
            self.socket.id().as_u64()
        );
        if error_code != 0 {
            log::warn!(
                "HTTP/2 peer reported GOAWAY error: socket={}, error_code={error_code}",
                self.socket.id().as_u64()
            );
        }
        core.peer_goaway_last_stream_id = Some(last_stream_id);
        let _ = self.state.compare_exchange(
            CONNECTION_OPEN,
            CONNECTION_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        for stream in core.streams.values() {
            if stream.id > last_stream_id
                && stream.state.load(Ordering::Acquire) & STREAM_FAILED == 0
            {
                stream.fail_receiver(RecvStreamError::UnprocessedByPeer(error_code));
            }
        }
        Ok(())
    }

    fn handle_headers(&self, core: &mut H2Core, mut frame: Frame) -> Result<(), RecvStreamError> {
        if frame.header.stream_id.is_connection() {
            return Err(RecvStreamError::ProtocolError);
        }
        if !core.streams.contains_key(&frame.header.stream_id) {
            let local = frame.header.stream_id.is_client_initiated()
                == (core.role == ConnectionRole::Client);
            let last_opened =
                if local { core.last_local_stream_id } else { core.last_peer_stream_id };
            if local && frame.header.stream_id.as_u32() <= last_opened {
                return Self::discard_closed_headers(core, frame);
            }
            self.open_peer_stream(core, frame.header.stream_id)?;
        }
        let Some(stream) = core.streams.get(&frame.header.stream_id) else {
            return Err(RecvStreamError::ProtocolError);
        };
        if stream.state.load(Ordering::Acquire) & (STREAM_REMOTE_CLOSED | STREAM_FAILED) != 0 {
            return Err(RecvStreamError::ProtocolError);
        }

        let padding = if frame.header.flags.contains(FrameFlags::PADDED) {
            if !frame.payload.has_remaining() {
                return Err(RecvStreamError::ProtocolError);
            }
            usize::from(frame.payload.get_u8())
        } else {
            0
        };
        if frame.header.flags.contains(FrameFlags::PRIORITY) {
            if frame.payload.remaining() < 5 {
                return Err(RecvStreamError::ProtocolError);
            }
            frame.payload.advance(5);
        }
        if padding > frame.payload.remaining() {
            return Err(RecvStreamError::ProtocolError);
        }
        let fragment_len = frame.payload.remaining() - padding;
        let block = frame.payload.split_to(fragment_len);
        let end_stream = frame.header.flags.contains(FrameFlags::END_STREAM);
        if frame.header.flags.contains(FrameFlags::END_HEADERS) {
            return self.deliver_headers(core, frame.header.stream_id, block, end_stream);
        }
        if block.len() > core.hpack_decoder.max_header_block_size() {
            return Err(RecvStreamError::ProtocolError);
        }
        core.continuation =
            Some(HeaderContinuation { stream_id: frame.header.stream_id, block, end_stream });
        Ok(())
    }

    fn discard_closed_headers(core: &mut H2Core, mut frame: Frame) -> Result<(), RecvStreamError> {
        if !frame.header.flags.contains(FrameFlags::END_HEADERS) {
            return Err(RecvStreamError::ProtocolError);
        }
        let padding = if frame.header.flags.contains(FrameFlags::PADDED) {
            if !frame.payload.has_remaining() {
                return Err(RecvStreamError::ProtocolError);
            }
            usize::from(frame.payload.get_u8())
        } else {
            0
        };
        if frame.header.flags.contains(FrameFlags::PRIORITY) {
            if frame.payload.remaining() < 5 {
                return Err(RecvStreamError::ProtocolError);
            }
            frame.payload.advance(5);
        }
        if padding > frame.payload.remaining() {
            return Err(RecvStreamError::ProtocolError);
        }
        let block = frame.payload.split_to(frame.payload.remaining() - padding);
        core.hpack_decoder.decode(block).map_err(|_| RecvStreamError::ProtocolError)?;
        Ok(())
    }

    fn handle_continuation(
        &self,
        core: &mut H2Core,
        mut frame: Frame,
    ) -> Result<(), RecvStreamError> {
        let Some(mut continuation) = core.continuation.take() else {
            return Err(RecvStreamError::ProtocolError);
        };
        continuation.block.append_movable(&mut frame.payload);
        if continuation.block.len() > core.hpack_decoder.max_header_block_size() {
            return Err(RecvStreamError::ProtocolError);
        }
        if frame.header.flags.contains(FrameFlags::END_HEADERS) {
            self.deliver_headers(
                core,
                continuation.stream_id,
                continuation.block,
                continuation.end_stream,
            )
        } else {
            core.continuation = Some(continuation);
            Ok(())
        }
    }

    fn deliver_headers(
        &self,
        core: &mut H2Core,
        stream_id: StreamId,
        block: IoBuf,
        end_stream: bool,
    ) -> Result<(), RecvStreamError> {
        let headers =
            core.hpack_decoder.decode(block).map_err(|_| RecvStreamError::ProtocolError)?;
        let Some(stream) = core.streams.get(&stream_id).cloned() else {
            return Err(RecvStreamError::ProtocolError);
        };
        let final_headers_received = stream.headers_received.load(Ordering::Acquire);
        let trailers = if final_headers_received {
            if !end_stream
                || headers.pseudo.method.is_some()
                || headers.pseudo.scheme.is_some()
                || headers.pseudo.authority.is_some()
                || headers.pseudo.path.is_some()
                || headers.pseudo.status.is_some()
                || headers.pseudo.protocol.is_some()
            {
                return Err(RecvStreamError::ProtocolError);
            }
            true
        } else {
            match core.role {
                ConnectionRole::Client => {
                    let Some(status) = headers.pseudo.status else {
                        return Err(RecvStreamError::ProtocolError);
                    };
                    if headers.pseudo.method.is_some()
                        || headers.pseudo.scheme.is_some()
                        || headers.pseudo.authority.is_some()
                        || headers.pseudo.path.is_some()
                        || headers.pseudo.protocol.is_some()
                    {
                        return Err(RecvStreamError::ProtocolError);
                    }
                    let informational = (100..200).contains(&status);
                    if informational && end_stream {
                        return Err(RecvStreamError::ProtocolError);
                    }
                    if !informational {
                        stream.headers_received.store(true, Ordering::Release);
                    }
                }
                ConnectionRole::Server => {
                    if headers.pseudo.status.is_some()
                        || headers.pseudo.method.is_none()
                        || headers.pseudo.scheme.is_none()
                        || headers.pseudo.path.is_none()
                    {
                        return Err(RecvStreamError::ProtocolError);
                    }
                    stream.headers_received.store(true, Ordering::Release);
                }
            }
            false
        };
        let accepting = core.role == ConnectionRole::Server
            && !final_headers_received
            && !stream.accepted.swap(true, Ordering::AcqRel);
        if accepting {
            let connection = self.resource().ok_or(RecvStreamError::ConnectionClosed)?;
            self.accepted_streams
                .push(AcceptedStream {
                    stream: H2Stream { shared: stream.clone(), connection, split: false },
                    headers,
                    end_stream,
                })
                .map_err(|_| RecvStreamError::ProtocolError)?;
            if let Some(waker) = self.accept_waker.lock().unwrap().take() {
                waker.wake();
            }
        } else {
            self.deliver_event(
                core,
                &stream,
                ReceivedEvent::Headers { headers, end_stream, trailers },
            )?;
        }
        if end_stream {
            stream.state.fetch_or(STREAM_REMOTE_CLOSED, Ordering::AcqRel);
        }
        Ok(())
    }

    fn deliver_event(
        &self,
        core: &mut H2Core,
        stream: &Resource<H2StreamShared>,
        event: ReceivedEvent,
    ) -> Result<(), RecvStreamError> {
        // SAFETY: inbound delivery runs under the connection owner.
        let owner = unsafe { &mut *stream.owner.get() };
        if let Some(driver) = owner.driver.as_ref() {
            match driver.event(event) {
                H2StreamDisposition::Continue => {}
                H2StreamDisposition::Complete => {
                    owner.driver.take();
                }
                H2StreamDisposition::Cancel => {
                    owner.driver.take();
                    let previous = stream.state.fetch_or(STREAM_FAILED, Ordering::AcqRel);
                    if previous & STREAM_WIRE_OPEN != 0
                        && previous & (STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED)
                            != STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED
                    {
                        core.control_frames
                            .push_back(Self::reset_frame(stream.id, ErrorCode::Cancel));
                    }
                }
            }
            return Ok(());
        }
        match stream.push_inbound(event) {
            Ok(()) => stream.wake_receiver(),
            Err(event) => {
                debug_assert!(core.blocked_inbound.is_none());
                core.blocked_inbound = Some((stream.clone(), event));
                self.inbound_blocked.store(true, Ordering::Release);
                self.read_ready.store(true, Ordering::Release);

                // Closing the backpressure gate must account for a receiver
                // that freed a slot just before `inbound_blocked` became
                // visible. Such a receiver observed the old false value and
                // therefore had no reason to notify the connection. Retry the
                // parked event after publishing the gate; if space was freed
                // in that window, consume it now. Otherwise a later receiver
                // pop observes true and schedules another drive.
                let (blocked_stream, blocked_event) =
                    core.blocked_inbound.take().expect("blocked inbound event was just parked");
                match blocked_stream.push_inbound(blocked_event) {
                    Ok(()) => {
                        self.inbound_blocked.store(false, Ordering::Release);
                        blocked_stream.wake_receiver();
                    }
                    Err(blocked_event) => {
                        core.blocked_inbound = Some((blocked_stream, blocked_event));
                    }
                }
            }
        }
        Ok(())
    }

    fn open_peer_stream(
        &self,
        core: &mut H2Core,
        stream_id: StreamId,
    ) -> Result<(), RecvStreamError> {
        if core.role != ConnectionRole::Server
            || !stream_id.is_client_initiated()
            || stream_id.as_u32() <= core.last_peer_stream_id
            || core.streams.len() >= core.max_peer_streams
        {
            return Err(RecvStreamError::ProtocolError);
        }
        core.last_peer_stream_id = stream_id.as_u32();
        let stream = Self::new_stream_shared(
            stream_id,
            self.resource_id(),
            core.peer_settings.initial_window_size,
            None,
        )?;
        stream.state.fetch_or(STREAM_WIRE_OPEN, Ordering::Release);
        core.streams.insert(stream_id, stream);
        Ok(())
    }

    fn handle_reset(core: &mut H2Core, frame: Frame) -> Result<(), RecvStreamError> {
        if frame.header.stream_id.is_connection() || frame.payload.len() != 4 {
            return Err(RecvStreamError::ProtocolError);
        }
        let mut bytes = [0_u8; 4];
        let mut copied = 0;
        for chunk in frame.payload.chunks() {
            let count = (bytes.len() - copied).min(chunk.len());
            bytes[copied..copied + count].copy_from_slice(&chunk[..count]);
            copied += count;
            if copied == bytes.len() {
                break;
            }
        }
        let Some(stream) = core.streams.get(&frame.header.stream_id) else {
            let local = frame.header.stream_id.is_client_initiated()
                == (core.role == ConnectionRole::Client);
            let last_opened =
                if local { core.last_local_stream_id } else { core.last_peer_stream_id };
            return if frame.header.stream_id.as_u32() <= last_opened {
                Ok(())
            } else {
                Err(RecvStreamError::ProtocolError)
            };
        };
        let error_code = u32::from_be_bytes(bytes);
        // DATA/HEADERS carrying END_STREAM may already be queued for the
        // application when a peer sends RST_STREAM solely to stop the opposite
        // request direction.  The completed receive direction wins: clearing
        // its queue here would turn a valid early response into a cancellation.
        if stream.state.load(Ordering::Acquire) & STREAM_REMOTE_CLOSED != 0 {
            stream.cancel_observer();
            return Ok(());
        }
        log::debug!("HTTP/2 stream reset by peer: stream={:?}, error_code={error_code}", stream.id);
        if error_code != 0 && error_code != 8 {
            log::warn!(
                "HTTP/2 peer reset stream with error: stream={:?}, error_code={error_code}",
                stream.id
            );
        }
        stream.fail_receiver(RecvStreamError::Reset(error_code));
        Ok(())
    }

    fn handle_window_update(core: &mut H2Core, frame: Frame) -> Result<(), RecvStreamError> {
        if frame.payload.len() != 4 {
            return Err(RecvStreamError::ProtocolError);
        }
        let mut bytes = [0_u8; 4];
        let mut copied = 0;
        for chunk in frame.payload.chunks() {
            let count = (bytes.len() - copied).min(chunk.len());
            bytes[copied..copied + count].copy_from_slice(&chunk[..count]);
            copied += count;
            if copied == bytes.len() {
                break;
            }
        }
        let increment = i64::from(u32::from_be_bytes(bytes) & 0x7fff_ffff);
        if increment == 0 {
            return Err(RecvStreamError::ProtocolError);
        }
        if frame.header.stream_id.is_connection() {
            core.connection_send_window = core
                .connection_send_window
                .checked_add(increment)
                .filter(|window| *window <= MAX_WINDOW_SIZE)
                .ok_or(RecvStreamError::ProtocolError)?;
            for stream in core.streams.values() {
                Self::resume_flow_blocked(stream, core.connection_send_window);
            }
            return Ok(());
        }

        let Some(stream) = core.streams.get(&frame.header.stream_id) else {
            // RFC 9113 sections 5.1 and 6.9: late WINDOW_UPDATE on a closed
            // stream is permitted. Distinguish retired/skipped IDs from idle
            // IDs without retaining every closed stream in the map.
            let local = frame.header.stream_id.is_client_initiated()
                == (core.role == ConnectionRole::Client);
            let last_opened =
                if local { core.last_local_stream_id } else { core.last_peer_stream_id };
            return if frame.header.stream_id.as_u32() <= last_opened {
                Ok(())
            } else {
                Err(RecvStreamError::ProtocolError)
            };
        };
        if stream.state.load(Ordering::Acquire) & (STREAM_LOCAL_CLOSED | STREAM_FAILED) != 0 {
            return Ok(());
        }
        if stream.state.load(Ordering::Acquire) & STREAM_WIRE_OPEN == 0 {
            return Err(RecvStreamError::ProtocolError);
        }
        // SAFETY: inbound frame handling runs under the connection owner.
        let owner = unsafe { &mut *stream.owner.get() };
        owner.send_window = owner
            .send_window
            .checked_add(increment)
            .filter(|window| *window <= MAX_WINDOW_SIZE)
            .ok_or(RecvStreamError::ProtocolError)?;
        Self::resume_flow_blocked(stream, core.connection_send_window);
        Ok(())
    }

    fn resume_flow_blocked(stream: &Resource<H2StreamShared>, connection_window: i64) {
        // SAFETY: callers hold the parent connection owner.
        let owner = unsafe { &mut *stream.owner.get() };
        if !owner.flow_blocked || owner.send_window <= 0 || connection_window <= 0 {
            return;
        }
        owner.flow_blocked = false;
        if profile::enabled() {
            profile::record_event(ProfileEvent::H2FlowResumed, 1);
        }
        if let Some(connection) = H2Connection::resource_pool().get(stream.connection) {
            connection.enqueue_ready(stream.clone());
        }
    }

    fn service_pending_opens(&self, core: &mut H2Core) {
        if self.state.load(Ordering::Acquire) != CONNECTION_OPEN {
            while let Some(open) = core.pending_opens.pop_front() {
                open.fail(OpenStreamError::ConnectionDraining);
            }
            return;
        }
        let limit = core.peer_settings.max_concurrent_streams.unwrap_or(u32::MAX) as usize;
        while core.streams.len() < limit {
            let Some(open) = core.pending_opens.pop_front() else { break };
            if !open.is_active() {
                continue;
            }
            let Some(id) = StreamId::new(core.next_local_stream_id) else {
                let _ = self.state.compare_exchange(
                    CONNECTION_OPEN,
                    CONNECTION_DRAINING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                open.fail(OpenStreamError::ConnectionDraining);
                while let Some(open) = core.pending_opens.pop_front() {
                    open.fail(OpenStreamError::ConnectionDraining);
                }
                return;
            };
            core.next_local_stream_id = core.next_local_stream_id.saturating_add(2);
            let slot = match unsafe { H2StreamShared::resource_pool().acquire_uninit() } {
                Ok(slot) => slot,
                Err(_) => {
                    open.fail(OpenStreamError::Busy);
                    return;
                }
            };
            let (driver, completion, handoff) = match open {
                PendingOpen::Future(completion) => (None, Some(completion), None),
                PendingOpen::Driven(driver) => (Some(driver), None, None),
                PendingOpen::Handoff(handoff) => (None, None, Some(handoff)),
            };
            let stream = Self::init_stream_shared(
                slot,
                id,
                self.resource_id(),
                core.peer_settings.initial_window_size,
                driver,
            );
            core.streams.insert(id, stream.clone());
            let Some(connection) = self.resource() else {
                stream.fail_receiver(RecvStreamError::ConnectionClosed);
                return;
            };
            if let Some(completion) = completion {
                completion.complete(Ok(H2Stream { shared: stream, connection, split: false }));
            } else if let Some(handoff) = handoff {
                handoff.opened(H2Stream { shared: stream, connection, split: false });
            } else {
                // SAFETY: driver access is serialized by the connection owner.
                let owner = unsafe { &mut *stream.owner.get() };
                let cancel = H2StreamCancel { connection: stream.connection, stream_id: stream.id };
                owner.initial_send = owner.driver.as_ref().and_then(|driver| driver.opened(cancel));
                if owner.initial_send.is_none() {
                    stream.state.fetch_or(STREAM_FAILED, Ordering::AcqRel);
                    owner.driver.take();
                } else if !stream.write_ready.swap(true, Ordering::AcqRel) {
                    self.enqueue_ready(stream);
                }
            }
        }
    }

    fn new_stream_shared(
        id: StreamId,
        connection: ResourceId<H2Connection>,
        initial_send_window: u32,
        driver: Option<H2StreamDriver>,
    ) -> Result<Resource<H2StreamShared>, RecvStreamError> {
        H2StreamShared::resource_pool()
            .acquire_with_init(|| Self::stream_shared(id, connection, initial_send_window, driver))
            .map_err(|_| RecvStreamError::ResourceExhausted)
    }

    fn init_stream_shared(
        slot: UninitResource<H2StreamShared>,
        id: StreamId,
        connection: ResourceId<H2Connection>,
        initial_send_window: u32,
        driver: Option<H2StreamDriver>,
    ) -> Resource<H2StreamShared> {
        slot.write(Self::stream_shared(id, connection, initial_send_window, driver))
    }

    fn stream_shared(
        id: StreamId,
        connection: ResourceId<H2Connection>,
        initial_send_window: u32,
        driver: Option<H2StreamDriver>,
    ) -> H2StreamShared {
        H2StreamShared {
            id,
            connection,
            state: AtomicU8::new(0),
            dropped_endpoints: AtomicU8::new(0),
            headers_sent: AtomicBool::new(false),
            headers_received: AtomicBool::new(false),
            accepted: AtomicBool::new(false),
            write_ready: AtomicBool::new(false),
            ready_queued: AtomicBool::new(false),
            send_waiting: AtomicBool::new(false),
            send_waker: Mutex::new(None),
            outbound: OnceLock::new(),
            inbound_slots: Mutex::new(InboundSlots::default()),
            inbound_headers: OnceLock::new(),
            inbound_data_queue: OnceLock::new(),
            complete_send: Mutex::new(None),
            recv_failure: Mutex::new(None),
            cancellation: Mutex::new(None),
            recv_waker: Mutex::new(None),
            recv_consumed: AtomicUsize::new(0),
            credit_ready: AtomicBool::new(false),
            owner: UnsafeCell::new(StreamOwnerState {
                send_window: i64::from(initial_send_window),
                recv_window: LOCAL_STREAM_RECV_WINDOW as i64,
                initial_send: None,
                pending_data: None,
                pending_trailers: None,
                flow_blocked: false,
                driver,
                received_body_bytes: 0,
            }),
        }
    }

    fn drain_abandoned_streams(core: &mut H2Core, connection: &Self) {
        while let Some(stream_id) = connection.abandoned_streams.pop() {
            if core.blocked_inbound.as_ref().is_some_and(|(stream, _)| stream.id == stream_id) {
                core.blocked_inbound = None;
                connection.inbound_blocked.store(false, Ordering::Release);
            }
            let Some(stream) = core.streams.remove(&stream_id) else { continue };
            let state = stream.state.load(Ordering::Acquire);
            if state & (STREAM_FAILED | STREAM_WIRE_OPEN) == STREAM_WIRE_OPEN
                && state & (STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED)
                    != STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED
            {
                core.control_frames.push_back(Self::reset_frame(stream_id, ErrorCode::Cancel));
            }
            stream.fail_receiver(RecvStreamError::Reset(ErrorCode::Cancel as u32));
        }
        connection.release_next_wire_stream(core);
    }

    fn reap_terminal_streams(&self, core: &mut H2Core) {
        core.streams.retain(|_, stream| {
            let state = stream.state.load(Ordering::Acquire);
            state & STREAM_FAILED == 0
                && state & (STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED)
                    != STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED
        });
        self.release_next_wire_stream(core);
    }

    fn release_next_wire_stream(&self, core: &mut H2Core) {
        while core.next_wire_stream_id < core.next_local_stream_id {
            let Some(id) = StreamId::new(core.next_wire_stream_id) else { return };
            if let Some(stream) = core.streams.get(&id) {
                if stream.write_ready.load(Ordering::Acquire) {
                    let stream = stream.clone();
                    self.enqueue_ready(stream);
                }
                return;
            }
            core.next_wire_stream_id = core.next_wire_stream_id.saturating_add(2);
        }
    }

    fn reset_frame(stream_id: StreamId, error: ErrorCode) -> Frame {
        let mut builder = IoBufBuilder::new();
        builder.put_u32(error as u32);
        Frame {
            header: FrameHeader {
                length: 4,
                kind: FrameKind::RstStream,
                flags: FrameFlags::default(),
                stream_id,
            },
            payload: builder.finish(),
        }
    }

    fn fill_control_frames(core: &mut H2Core) -> Result<(), super::error::H2Error> {
        while let Some(frame) = core.control_frames.pop_front() {
            Self::append_frame(core, frame)?;
        }
        Ok(())
    }

    fn append_frame(core: &mut H2Core, frame: Frame) -> Result<(), super::error::H2Error> {
        let kind = frame.header.kind;
        let class = match kind {
            FrameKind::Data | FrameKind::Headers | FrameKind::Continuation => {
                WriteClass::Application
            }
            _ => WriteClass::Control,
        };
        let mut encoded = frame::encode(frame)?;
        let encoded_len = encoded.len();
        if profile::enabled() {
            let event = match kind {
                FrameKind::Data => ProfileEvent::H2DataEncodedBytes,
                FrameKind::Headers | FrameKind::Continuation => ProfileEvent::H2HeadersEncodedBytes,
                FrameKind::WindowUpdate => ProfileEvent::H2WindowUpdateEncodedBytes,
                _ => ProfileEvent::H2ControlEncodedBytes,
            };
            profile::record_event(event, encoded_len as u64);
        }
        core.write_segments.push_back(WriteSegment { remaining: encoded_len, class });
        core.write_batch.append_movable(&mut encoded);
        Ok(())
    }

    fn drain_receive_credits(core: &mut H2Core, connection: &Self) {
        if connection.credit_notified.swap(false, Ordering::AcqRel) {
            let consumed = connection.recv_consumed.swap(0, Ordering::AcqRel);
            if consumed > 0 {
                core.connection_recv_window += consumed as i64;
                core.control_frames
                    .push_back(Self::window_update_frame(StreamId::CONNECTION, consumed as u32));
            }
        }

        while let Some(stream) = connection.ready_credits.pop() {
            let consumed = stream.recv_consumed.swap(0, Ordering::AcqRel);
            stream.credit_ready.store(false, Ordering::Release);
            if consumed > 0 && stream.state.load(Ordering::Acquire) & STREAM_FAILED == 0 {
                // SAFETY: credit draining runs under the connection owner.
                let owner = unsafe { &mut *stream.owner.get() };
                owner.recv_window += consumed as i64;
                core.control_frames
                    .push_back(Self::window_update_frame(stream.id, consumed as u32));
            }
            if stream.recv_consumed.load(Ordering::Acquire) >= STREAM_WINDOW_UPDATE_THRESHOLD
                && !stream.credit_ready.swap(true, Ordering::AcqRel)
            {
                connection.ready_credits.push(stream);
                connection.mark_work();
            }
        }

        if connection.recv_consumed.load(Ordering::Acquire) >= CONNECTION_WINDOW_UPDATE_THRESHOLD
            && !connection.credit_notified.swap(true, Ordering::AcqRel)
        {
            connection.mark_work();
        }
    }

    fn window_update_frame(stream_id: StreamId, increment: u32) -> Frame {
        let mut builder = IoBufBuilder::new();
        builder.put_u32(increment);
        Frame {
            header: FrameHeader {
                length: 4,
                kind: FrameKind::WindowUpdate,
                flags: FrameFlags::default(),
                stream_id,
            },
            payload: builder.finish(),
        }
    }

    fn drain_stream(&self, core: &mut H2Core, stream: Resource<H2StreamShared>) -> Result<(), ()> {
        if stream.state.load(Ordering::Acquire) & STREAM_FAILED != 0 {
            if let Some(outbound) = stream.outbound.get() {
                while outbound.pop().is_some() {}
            }
            stream.complete_send.lock().unwrap().take();
            stream.wake_sender();
            // SAFETY: stream draining runs under the connection owner.
            let owner = unsafe { &mut *stream.owner.get() };
            owner.initial_send = None;
            owner.pending_data = None;
            owner.pending_trailers = None;
            stream.write_ready.store(false, Ordering::Release);
            return Ok(());
        }
        if stream.state.load(Ordering::Acquire) & STREAM_WIRE_OPEN == 0
            && stream.id.as_u32() != core.next_wire_stream_id
        {
            return Ok(());
        }
        // Complete unary sends use one inline slot instead of allocating and
        // traversing a streaming outbound queue.
        let direct = unsafe { &mut *stream.owner.get() }
            .initial_send
            .take()
            .or_else(|| stream.complete_send.lock().unwrap().take());
        if let Some(direct) = direct {
            let headers_end_stream = direct.payload.is_none() && direct.trailers.is_none();
            let encoded = core.hpack_encoder.encode(&direct.headers).map_err(|_| ())?;
            Self::append_header_frames(core, stream.id, encoded, headers_end_stream)?;
            stream.headers_sent.store(true, Ordering::Release);
            if stream.state.load(Ordering::Acquire) & STREAM_WIRE_OPEN == 0 {
                core.last_local_stream_id = stream.id.as_u32();
            }
            stream.state.fetch_or(STREAM_WIRE_OPEN, Ordering::AcqRel);
            if stream.id.as_u32() == core.next_wire_stream_id {
                core.next_wire_stream_id = core.next_wire_stream_id.saturating_add(2);
                self.release_next_wire_stream(core);
            }
            if headers_end_stream {
                stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
            } else {
                // SAFETY: stream draining runs under the connection owner.
                unsafe { &mut *stream.owner.get() }.pending_trailers = direct.trailers;
                if let Some(payload) = direct.payload {
                    let end_stream = unsafe { &*stream.owner.get() }.pending_trailers.is_none();
                    let pending = PendingData { payload, end_stream };
                    if let Some(remainder) = Self::append_data_frames(core, &stream, pending)? {
                        Self::park_data_remainder(&stream, remainder);
                        return Ok(());
                    }
                    if end_stream {
                        stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
                    }
                }
            }
        }
        // SAFETY: stream draining runs under the connection owner.
        let pending = unsafe { &mut *stream.owner.get() }.pending_data.take();
        if let Some(pending) = pending {
            let closes_local = pending.end_stream;
            if let Some(remainder) = Self::append_data_frames(core, &stream, pending)? {
                Self::park_data_remainder(&stream, remainder);
                return Ok(());
            }
            if closes_local {
                stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
            }
        }
        // A trailer stays owner-local while a preceding DATA item is flow
        // controlled, then follows it without another producer handoff.
        let trailers = unsafe { &mut *stream.owner.get() }.pending_trailers.take();
        if let Some(trailers) = trailers {
            let encoded = core.hpack_encoder.encode(&trailers).map_err(|_| ())?;
            Self::append_header_frames(core, stream.id, encoded, true)?;
            stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
        }
        let mut drained = 0;
        while drained < INLINE_STREAMS {
            let Some(item) = stream.outbound.get().and_then(|queue| queue.pop()) else { break };
            stream.wake_sender();
            match item {
                OutboundItem::Headers { headers, end_stream } => {
                    let encoded = core.hpack_encoder.encode(&headers).map_err(|_| ())?;
                    Self::append_header_frames(core, stream.id, encoded, end_stream)?;
                    if stream.state.load(Ordering::Acquire) & STREAM_WIRE_OPEN == 0 {
                        core.last_local_stream_id = stream.id.as_u32();
                    }
                    stream.state.fetch_or(STREAM_WIRE_OPEN, Ordering::AcqRel);
                    if stream.id.as_u32() == core.next_wire_stream_id {
                        core.next_wire_stream_id = core.next_wire_stream_id.saturating_add(2);
                        self.release_next_wire_stream(core);
                    }
                    if end_stream {
                        stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
                    }
                }
                OutboundItem::Data { payload, end_stream } => {
                    let pending = PendingData { payload, end_stream };
                    if let Some(remainder) = Self::append_data_frames(core, &stream, pending)? {
                        Self::park_data_remainder(&stream, remainder);
                        return Ok(());
                    }
                    if end_stream {
                        stream.state.fetch_or(STREAM_LOCAL_CLOSED, Ordering::AcqRel);
                    }
                }
            }
            drained += 1;
        }

        // SAFETY: stream draining runs under the connection owner.
        unsafe { &mut *stream.owner.get() }.flow_blocked = false;
        stream.write_ready.store(false, Ordering::Release);
        let Some(outbound) = stream.outbound.get() else { return Ok(()) };
        if outbound.is_empty() || stream.write_ready.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(connection) = H2Connection::resource_pool().get(stream.connection) {
            connection.enqueue_ready(stream);
        }
        Ok(())
    }

    fn park_data_remainder(stream: &Resource<H2StreamShared>, remainder: DataRemainder) {
        // SAFETY: this helper is called only by the connection owner.
        let owner = unsafe { &mut *stream.owner.get() };
        owner.pending_data = Some(remainder.pending);
        owner.flow_blocked = remainder.flow_blocked;
        if remainder.flow_blocked {
            return;
        }
        if let Some(connection) = H2Connection::resource_pool().get(stream.connection) {
            connection.enqueue_ready(stream.clone());
        }
    }

    fn append_header_frames(
        core: &mut H2Core,
        stream_id: StreamId,
        mut block: IoBuf,
        end_stream: bool,
    ) -> Result<(), ()> {
        let max_frame_size = core.peer_settings.max_frame_size as usize;
        let mut first = true;
        loop {
            let payload = block.split_to(max_frame_size);
            let last = block.is_empty();
            let mut flags = if last { FrameFlags::END_HEADERS } else { 0 };
            if first && end_stream {
                flags |= FrameFlags::END_STREAM;
            }
            let frame = Frame {
                header: FrameHeader {
                    length: payload.len() as u32,
                    kind: if first { FrameKind::Headers } else { FrameKind::Continuation },
                    flags: FrameFlags::new(flags),
                    stream_id,
                },
                payload,
            };
            Self::append_frame(core, frame).map_err(|_| ())?;
            if last {
                return Ok(());
            }
            first = false;
        }
    }

    fn append_data_frames(
        core: &mut H2Core,
        stream: &Resource<H2StreamShared>,
        mut pending: PendingData,
    ) -> Result<Option<DataRemainder>, ()> {
        // SAFETY: DATA framing runs under the connection owner.
        let owner = unsafe { &mut *stream.owner.get() };
        if pending.payload.is_empty() {
            let frame = Frame {
                header: FrameHeader {
                    length: 0,
                    kind: FrameKind::Data,
                    flags: FrameFlags::new(if pending.end_stream {
                        FrameFlags::END_STREAM
                    } else {
                        0
                    }),
                    stream_id: stream.id,
                },
                payload: IoBuf::new(),
            };
            Self::append_frame(core, frame).map_err(|_| ())?;
            return Ok(None);
        }

        let available = core.connection_send_window.min(owner.send_window).max(0) as usize;
        if available == 0 {
            if profile::enabled() {
                profile::record_event(ProfileEvent::H2FlowBlocked, 1);
            }
            return Ok(Some(DataRemainder { pending, flow_blocked: true }));
        }
        let send_len = available.min(pending.payload.len()).min(STREAM_DATA_QUANTUM);
        let mut data = pending.payload.split_to(send_len);
        core.connection_send_window -= send_len as i64;
        owner.send_window -= send_len as i64;
        let max_frame_size = core.peer_settings.max_frame_size as usize;
        loop {
            let payload = data.split_to(max_frame_size);
            let last = data.is_empty();
            let frame = Frame {
                header: FrameHeader {
                    length: payload.len() as u32,
                    kind: FrameKind::Data,
                    flags: FrameFlags::new(
                        if last && pending.payload.is_empty() && pending.end_stream {
                            FrameFlags::END_STREAM
                        } else {
                            0
                        },
                    ),
                    stream_id: stream.id,
                },
                payload,
            };
            Self::append_frame(core, frame).map_err(|_| ())?;
            if last {
                if pending.payload.is_empty() {
                    return Ok(None);
                }
                let flow_blocked = core.connection_send_window <= 0 || owner.send_window <= 0;
                if flow_blocked && profile::enabled() {
                    profile::record_event(ProfileEvent::H2FlowBlocked, 1);
                }
                return Ok(Some(DataRemainder { pending, flow_blocked }));
            }
        }
    }

    fn flush(&self, core: &mut H2Core, budget: usize) -> io::Result<bool> {
        let mut written = 0;
        while !core.write_batch.is_empty() && written < budget {
            let mut slices: [IoSlice<'_>; WRITEV_SLICES] =
                std::array::from_fn(|_| IoSlice::new(&[]));
            let count = core.write_batch.fill_io_slices(&mut slices);
            if profile::enabled() {
                profile::record_event(ProfileEvent::H2WriteIovCount, count as u64);
                profile::record_event(
                    ProfileEvent::H2WriteAttemptBytes,
                    slices[..count].iter().map(|slice| slice.len() as u64).sum(),
                );
            }
            match self.socket.transport().try_write_vectored(&slices[..count]) {
                Ok(0) => return Ok(false),
                Ok(count) => {
                    core.write_batch.consume(count);
                    let (application, control) = Self::consume_write_segments(core, count);
                    written += count;
                    if profile::enabled() {
                        profile::record_event(ProfileEvent::H2WriteOk, 1);
                        profile::record_event(ProfileEvent::H2WriteOkBytes, count as u64);
                        profile::record_event(
                            match core.role {
                                ConnectionRole::Client => ProfileEvent::H2ClientWriteOk,
                                ConnectionRole::Server => ProfileEvent::H2ServerWriteOk,
                            },
                            1,
                        );
                        if application != 0 {
                            profile::record_event(
                                ProfileEvent::H2AppWriteBytes,
                                application as u64,
                            );
                        }
                        if control != 0 {
                            profile::record_event(
                                ProfileEvent::H2ControlWriteBytes,
                                control as u64,
                            );
                        }
                        profile::record_event(
                            match (application != 0, control != 0) {
                                (true, false) => ProfileEvent::H2AppOnlyWrite,
                                (false, true) => ProfileEvent::H2ControlOnlyWrite,
                                (true, true) => ProfileEvent::H2MixedWrite,
                                (false, false) => {
                                    unreachable!("positive write consumed no segment")
                                }
                            },
                            1,
                        );
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if profile::enabled() {
                        profile::record_event(ProfileEvent::H2WriteWouldBlock, 1);
                    }
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    fn consume_write_segments(core: &mut H2Core, mut bytes: usize) -> (usize, usize) {
        let mut application = 0;
        let mut control = 0;
        while bytes != 0 {
            let segment = core
                .write_segments
                .front_mut()
                .expect("HTTP/2 write batch and segment ledger diverged");
            let consumed = bytes.min(segment.remaining);
            match segment.class {
                WriteClass::Application => application += consumed,
                WriteClass::Control => control += consumed,
            }
            segment.remaining -= consumed;
            bytes -= consumed;
            if segment.remaining == 0 {
                core.write_segments.pop_front();
            }
        }
        (application, control)
    }

    fn has_immediate_work(&self) -> bool {
        self.notified.load(Ordering::Acquire)
            || (self.read_ready.load(Ordering::Acquire)
                && !self.inbound_blocked.load(Ordering::Acquire))
            || !self.commands.is_empty()
            || !self.abandoned_streams.is_empty()
            || !self.ready_streams.is_empty()
            || !self.ready_credits.is_empty()
    }

    fn has_runnable_work(&self) -> bool {
        let write_waiting = self.write_waiting.load(Ordering::Acquire);
        let read_work = self.read_ready.load(Ordering::Acquire)
            && !self.inbound_blocked.load(Ordering::Acquire);
        self.has_immediate_work() && (!write_waiting || read_work)
    }

    fn start_driver(connection: Resource<Self>) {
        tokio::spawn(Self::run_driver(connection));
    }

    /// Scheduling shell for one physical HTTP/2 connection.
    ///
    /// The task owns no protocol state across an await. Producers may still
    /// acquire the owner and drive inline; this task only provides a stable
    /// place to wait for socket readiness, fairness yields, and producer
    /// notifications before re-entering the synchronous owner.
    async fn run_driver(connection: Resource<Self>) {
        loop {
            if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
                while !Self::try_drive(connection.clone(), DriveQuota::ASYNC) {
                    tokio::task::yield_now().await;
                }
                return;
            }

            if connection.yield_requested.swap(false, Ordering::AcqRel) {
                tokio::task::yield_now().await;
            }

            // Once a write reports WouldBlock, queued stream work is not
            // runnable until writable readiness arrives. Re-entering solely
            // because those streams remain queued spins the driver and also
            // starves the peer's read side. Reads remain runnable so duplex
            // traffic can continue to release socket pressure.
            if connection.has_runnable_work() {
                if !Self::try_drive(connection.clone(), DriveQuota::ASYNC) {
                    // An inline producer currently owns the state machine. It
                    // cannot await while holding the owner, so one cooperative
                    // yield is sufficient before checking the published work.
                    tokio::task::yield_now().await;
                }
                continue;
            }

            let wait_read = !connection.inbound_blocked.load(Ordering::Acquire);
            let wait_write = connection.write_waiting.load(Ordering::Acquire);
            tokio::select! {
                _ = connection.driver_notify.notified() => {}
                result = connection.socket.transport().readable(), if wait_read => {
                    match result {
                        Ok(()) => connection.read_ready.store(true, Ordering::Release),
                        Err(error) => {
                            log::warn!(
                                "HTTP/2 readable wait failed: socket={}, error={error}",
                                connection.socket.id().as_u64()
                            );
                            connection.state.store(CONNECTION_CLOSED, Ordering::Release);
                        }
                    }
                    connection.notified.store(true, Ordering::Release);
                }
                result = connection.socket.transport().writable(), if wait_write => {
                    connection.write_waiting.store(false, Ordering::Release);
                    if let Err(error) = result {
                        log::warn!(
                            "HTTP/2 writable wait failed: socket={}, error={error}",
                            connection.socket.id().as_u64()
                        );
                        connection.state.store(CONNECTION_CLOSED, Ordering::Release);
                    }
                    connection.notified.store(true, Ordering::Release);
                }
            }
        }
    }

    fn shutdown_socket(&self) {
        if let Err(error) = self.socket.transport().shutdown() {
            log::warn!(
                "HTTP/2 socket shutdown failed: socket={}, error={error}",
                self.socket.id().as_u64()
            );
        }
    }

    fn close_core(&self, core: &mut H2Core, error: RecvStreamError) {
        log::debug!(
            "HTTP/2 connection closed: socket={}, reason={error:?}, streams={}",
            self.socket.id().as_u64(),
            core.streams.len()
        );
        for stream in core.streams.values() {
            stream.fail_receiver(error);
        }
        core.streams.clear();
        core.control_frames.clear();
        core.write_batch = IoBuf::new();
        core.write_segments.clear();
        core.input = IoBuf::new();
        core.continuation = None;
        core.blocked_inbound = None;
        while let Some(open) = core.pending_opens.pop_front() {
            open.fail(OpenStreamError::ConnectionClosed);
        }
    }

    fn fail_queued_commands(&self) {
        while let Some(command) = self.commands.pop() {
            match command {
                ConnectionCommand::OpenStream(open) => {
                    open.fail(OpenStreamError::ConnectionClosed);
                }
            }
        }
    }
}

impl H2Stream {
    #[cfg(test)]
    pub(crate) fn id(&self) -> StreamId {
        self.shared.id
    }

    pub(crate) fn split(mut self) -> (H2SendStream, H2RecvStream) {
        self.split = true;
        let send =
            H2SendStream { shared: self.shared.clone(), connection: self.connection.clone() };
        let recv =
            H2RecvStream { shared: self.shared.clone(), connection: self.connection.clone() };
        (send, recv)
    }
}

impl Drop for H2Stream {
    fn drop(&mut self) {
        if !self.split {
            self.shared.drop_endpoints(SEND_ENDPOINT_DROPPED | RECV_ENDPOINT_DROPPED);
        }
    }
}

impl Drop for FlowCredit {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let Some(connection) = H2Connection::resource_pool().get(self.connection) else { return };
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return;
        }

        let connection_consumed =
            connection.recv_consumed.fetch_add(self.bytes, Ordering::AcqRel) + self.bytes;
        let mut published = false;
        if let Some(stream) = H2StreamShared::resource_pool().get(self.stream) {
            let stream_consumed =
                stream.recv_consumed.fetch_add(self.bytes, Ordering::AcqRel) + self.bytes;
            if stream_consumed >= STREAM_WINDOW_UPDATE_THRESHOLD
                && !stream.credit_ready.swap(true, Ordering::AcqRel)
            {
                connection.ready_credits.push(stream);
                published = true;
            }
        }
        if connection_consumed >= CONNECTION_WINDOW_UPDATE_THRESHOLD
            && !connection.credit_notified.swap(true, Ordering::AcqRel)
        {
            published = true;
        }
        if published {
            connection.mark_work();
            // Receive-credit publication is control-plane work. Let the
            // stable driver aggregate credits from multiple streams instead
            // of turning an application buffer drop into a tiny inline
            // WINDOW_UPDATE write.
            connection.wake_driver();
        }
    }
}

impl H2StreamShared {
    fn outbound(&self) -> &ArrayQueue<OutboundItem> {
        self.outbound.get_or_init(|| Box::new(ArrayQueue::new(OUTBOUND_ITEMS)))
    }

    fn push_inbound(&self, event: ReceivedEvent) -> Result<(), ReceivedEvent> {
        let mut slots = self.inbound_slots.lock().unwrap();
        match event {
            ReceivedEvent::Headers { headers, end_stream, trailers: true } => {
                if slots.trailers.is_some() {
                    return Err(ReceivedEvent::Headers { headers, end_stream, trailers: true });
                }
                slots.trailers = Some(ReceivedHeader { headers, end_stream });
                Ok(())
            }
            ReceivedEvent::Headers { headers, end_stream, trailers: false } => {
                if let Some(queue) = self.inbound_headers.get() {
                    return queue.push(ReceivedHeader { headers, end_stream }).map_err(|header| {
                        ReceivedEvent::Headers {
                            headers: header.headers,
                            end_stream: header.end_stream,
                            trailers: false,
                        }
                    });
                }
                if slots.header.is_none() {
                    slots.header = Some(ReceivedHeader { headers, end_stream });
                    return Ok(());
                }
                let queue = self
                    .inbound_headers
                    .get_or_init(|| Box::new(ArrayQueue::new(INBOUND_HEADER_ITEMS)));
                queue.push(ReceivedHeader { headers, end_stream }).map_err(|header| {
                    ReceivedEvent::Headers {
                        headers: header.headers,
                        end_stream: header.end_stream,
                        trailers: false,
                    }
                })
            }
            ReceivedEvent::Data(data) => {
                if let Some(queue) = self.inbound_data_queue.get() {
                    return queue.push(data).map_err(ReceivedEvent::Data);
                }
                if slots.data.is_none() {
                    slots.data = Some(data);
                    return Ok(());
                }
                // The initial header previously occupied one entry in the
                // combined queue. Preserve the same backpressure bound while
                // keeping the common single DATA item inline.
                let queue = self
                    .inbound_data_queue
                    .get_or_init(|| Box::new(ArrayQueue::new(INBOUND_ITEMS - 2)));
                queue.push(data).map_err(ReceivedEvent::Data)
            }
        }
    }

    fn pop_inbound(&self) -> Option<ReceivedEvent> {
        let mut slots = self.inbound_slots.lock().unwrap();
        if let Some(header) = slots.header.take() {
            return Some(ReceivedEvent::Headers {
                headers: header.headers,
                end_stream: header.end_stream,
                trailers: false,
            });
        }
        if let Some(queue) = self.inbound_headers.get()
            && let Some(header) = queue.pop()
        {
            return Some(ReceivedEvent::Headers {
                headers: header.headers,
                end_stream: header.end_stream,
                trailers: false,
            });
        }
        if let Some(data) = slots.data.take() {
            return Some(ReceivedEvent::Data(data));
        }
        if let Some(queue) = self.inbound_data_queue.get()
            && let Some(data) = queue.pop()
        {
            return Some(ReceivedEvent::Data(data));
        }
        slots.trailers.take().map(|header| ReceivedEvent::Headers {
            headers: header.headers,
            end_stream: header.end_stream,
            trailers: true,
        })
    }

    fn cancel_observer(&self) {
        if let Some(cancellation) = self.cancellation.lock().unwrap().take() {
            cancellation.cancel();
        }
    }

    fn wake_receiver(&self) {
        let waker = self.recv_waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn wake_sender(&self) {
        if self.send_waiting.swap(false, Ordering::AcqRel)
            && let Some(waker) = self.send_waker.lock().unwrap().take()
        {
            waker.wake();
        }
    }

    fn fail_receiver(&self, error: RecvStreamError) {
        if self.state.fetch_or(STREAM_FAILED, Ordering::AcqRel) & STREAM_FAILED != 0 {
            return;
        }
        // SAFETY: all failure publication originates from the connection owner.
        let owner = unsafe { &mut *self.owner.get() };
        owner.initial_send = None;
        owner.pending_data = None;
        owner.pending_trailers = None;
        self.complete_send.lock().unwrap().take();
        if let Some(driver) = owner.driver.take() {
            driver.fail(error);
        }
        self.cancel_observer();
        let mut failure = self.recv_failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(error);
        }
        drop(failure);
        if let Some(outbound) = self.outbound.get() {
            while outbound.pop().is_some() {}
        }
        self.wake_sender();
        let mut slots = self.inbound_slots.lock().unwrap();
        slots.header = None;
        slots.data = None;
        slots.trailers = None;
        if let Some(headers) = self.inbound_headers.get() {
            while headers.pop().is_some() {}
        }
        if let Some(inbound) = self.inbound_data_queue.get() {
            while inbound.pop().is_some() {}
        }
        drop(slots);
        self.wake_receiver();
    }

    fn drop_endpoints(&self, endpoints: u8) {
        let dropped = self.dropped_endpoints.fetch_or(endpoints, Ordering::AcqRel) | endpoints;
        if dropped != SEND_ENDPOINT_DROPPED | RECV_ENDPOINT_DROPPED {
            return;
        }
        let state = self.state.load(Ordering::Acquire);
        if state & STREAM_FAILED != 0
            || state & (STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED)
                == STREAM_LOCAL_CLOSED | STREAM_REMOTE_CLOSED
            || state & (STREAM_SEND_CLOSED | STREAM_REMOTE_CLOSED)
                == STREAM_SEND_CLOSED | STREAM_REMOTE_CLOSED
        {
            return;
        }
        let Some(connection) = H2Connection::resource_pool().get(self.connection) else { return };
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return;
        }
        connection.abandoned_streams.push(self.id);
        connection.mark_work();
        if !connection.write_waiting.load(Ordering::Acquire) {
            H2Connection::try_drive(connection, DriveQuota::INLINE);
        } else {
            connection.wake_driver();
        }
    }
}

impl Drop for H2SendStream {
    fn drop(&mut self) {
        self.shared.drop_endpoints(SEND_ENDPOINT_DROPPED);
    }
}

impl Drop for H2RecvStream {
    fn drop(&mut self) {
        self.shared.drop_endpoints(RECV_ENDPOINT_DROPPED);
    }
}

impl H2SendStream {
    pub(crate) fn cancel_handle(&self) -> H2StreamCancel {
        H2StreamCancel { connection: self.shared.connection, stream_id: self.shared.id }
    }

    pub(crate) fn can_send_complete(&self, data_items: usize, has_trailers: bool) -> bool {
        if data_items <= 1 {
            return self.shared.complete_send.lock().unwrap().is_none();
        }
        1usize.saturating_add(data_items).saturating_add(usize::from(has_trailers))
            <= self.shared.outbound().capacity().saturating_sub(self.shared.outbound().len())
    }

    /// Allocation-free fast path for the common request shape: one HEADERS
    /// block followed by one end-of-stream DATA item.
    pub(crate) fn send_complete_one(
        &mut self,
        headers: HeaderBlock,
        payload: IoBuf,
    ) -> Result<(), SendBatchError> {
        self.send_complete_direct(headers, Some(payload), None)
    }

    /// Allocation-free publication for the common unary shape. The caller
    /// retains the single-writer guarantee and checks capacity before using a
    /// fallback that needs to recover ownership of the batch.
    pub(crate) fn send_complete_direct(
        &mut self,
        headers: HeaderBlock,
        payload: Option<IoBuf>,
        trailers: Option<HeaderBlock>,
    ) -> Result<(), SendBatchError> {
        if self.shared.state.load(Ordering::Acquire) & (STREAM_SEND_CLOSED | STREAM_FAILED) != 0 {
            return Err(SendBatchError::Closed);
        }
        let connection = &self.connection;
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return Err(SendBatchError::Closed);
        }
        if trailers.as_ref().is_some_and(|trailers| {
            trailers.pseudo.method.is_some()
                || trailers.pseudo.scheme.is_some()
                || trailers.pseudo.authority.is_some()
                || trailers.pseudo.path.is_some()
                || trailers.pseudo.status.is_some()
                || trailers.pseudo.protocol.is_some()
        }) {
            return Err(SendBatchError::InvalidTrailers);
        }
        if self
            .shared
            .headers_sent
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(SendBatchError::HeadersAlreadySent);
        }
        let mut slot = self.shared.complete_send.lock().unwrap();
        if slot.is_some() {
            self.shared.headers_sent.store(false, Ordering::Release);
            return Err(SendBatchError::Full);
        }
        *slot = Some(H2DirectSend { headers, payload, trailers });
        drop(slot);
        self.shared.state.fetch_or(STREAM_SEND_CLOSED, Ordering::AcqRel);
        if !self.shared.write_ready.swap(true, Ordering::AcqRel) {
            connection.publish_ready(self.shared.clone());
        }
        Ok(())
    }

    /// Publishes a complete HTTP/2 message as one unit of connection work.
    ///
    /// A logical RPC commonly consists of initial HEADERS, one or more DATA
    /// frames, and optional trailers. Publishing those parts independently
    /// lets an idle caller acquire and release the connection owner once per
    /// part, forcing an otherwise avoidable flush each time. This fast path
    /// fills the per-stream queue first and publishes it exactly once.
    pub(crate) fn send_complete(
        &mut self,
        headers: HeaderBlock,
        mut data: Vec<IoBuf>,
        trailers: Option<HeaderBlock>,
    ) -> Result<(), SendBatchError> {
        if self.shared.state.load(Ordering::Acquire) & (STREAM_SEND_CLOSED | STREAM_FAILED) != 0 {
            return Err(SendBatchError::Closed);
        }
        let connection = &self.connection;
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return Err(SendBatchError::Closed);
        }
        if trailers.as_ref().is_some_and(|trailers| {
            trailers.pseudo.method.is_some()
                || trailers.pseudo.scheme.is_some()
                || trailers.pseudo.authority.is_some()
                || trailers.pseudo.path.is_some()
                || trailers.pseudo.status.is_some()
                || trailers.pseudo.protocol.is_some()
        }) {
            return Err(SendBatchError::InvalidTrailers);
        }
        if data.len() <= 1 {
            return self.send_complete_direct(headers, data.pop(), trailers);
        }

        if self
            .shared
            .headers_sent
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(SendBatchError::HeadersAlreadySent);
        }

        if !self.can_send_complete(data.len(), trailers.is_some()) {
            self.shared.headers_sent.store(false, Ordering::Release);
            return Err(SendBatchError::Full);
        }

        let headers_end_stream = data.is_empty() && trailers.is_none();
        // The send half has a single writer and has not published initial
        // headers yet, so the capacity check above reserves this whole batch.
        let mut enqueue_failed = self
            .shared
            .outbound()
            .push(OutboundItem::Headers { headers, end_stream: headers_end_stream })
            .is_err();
        let last_data = data.len().saturating_sub(1);
        for (index, payload) in data.into_iter().enumerate() {
            let end_stream = trailers.is_none() && index == last_data;
            enqueue_failed |=
                self.shared.outbound().push(OutboundItem::Data { payload, end_stream }).is_err();
        }
        if let Some(headers) = trailers {
            enqueue_failed |= self
                .shared
                .outbound()
                .push(OutboundItem::Headers { headers, end_stream: true })
                .is_err();
        }
        if enqueue_failed {
            while self.shared.outbound().pop().is_some() {}
            self.shared.headers_sent.store(false, Ordering::Release);
            return Err(SendBatchError::Full);
        }

        self.shared.state.fetch_or(STREAM_SEND_CLOSED, Ordering::AcqRel);
        if !self.shared.write_ready.swap(true, Ordering::AcqRel) {
            connection.publish_ready(self.shared.clone());
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn send_headers(
        &mut self,
        headers: HeaderBlock,
        end_stream: bool,
    ) -> Result<(), SendHeadersError> {
        if self.shared.state.load(Ordering::Acquire) & (STREAM_SEND_CLOSED | STREAM_FAILED) != 0 {
            return Err(SendHeadersError::Closed(headers));
        }
        let connection = &self.connection;
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return Err(SendHeadersError::Closed(headers));
        }
        let initial = self
            .shared
            .headers_sent
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if !initial {
            let has_pseudo = headers.pseudo.method.is_some()
                || headers.pseudo.scheme.is_some()
                || headers.pseudo.authority.is_some()
                || headers.pseudo.path.is_some()
                || headers.pseudo.status.is_some()
                || headers.pseudo.protocol.is_some();
            if !end_stream || has_pseudo {
                return Err(SendHeadersError::AlreadySent(headers));
            }
        }
        if let Err(OutboundItem::Headers { headers, .. }) =
            self.shared.outbound().push(OutboundItem::Headers { headers, end_stream })
        {
            if initial {
                self.shared.headers_sent.store(false, Ordering::Release);
            }
            return Err(SendHeadersError::Full(headers));
        }
        if end_stream {
            self.shared.state.fetch_or(STREAM_SEND_CLOSED, Ordering::AcqRel);
        }
        if !self.shared.write_ready.swap(true, Ordering::AcqRel) {
            connection.publish_ready(self.shared.clone());
        }
        Ok(())
    }

    pub(crate) fn send_data(
        &mut self,
        payload: IoBuf,
        end_stream: bool,
    ) -> Result<(), SendStreamError> {
        if self.shared.state.load(Ordering::Acquire) & (STREAM_SEND_CLOSED | STREAM_FAILED) != 0 {
            return Err(SendStreamError::Closed(payload));
        }
        let connection = &self.connection;
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return Err(SendStreamError::Closed(payload));
        }
        if !self.shared.headers_sent.load(Ordering::Acquire) {
            return Err(SendStreamError::HeadersRequired(payload));
        }
        self.shared.outbound().push(OutboundItem::Data { payload, end_stream }).map_err(
            |item| match item {
                OutboundItem::Data { payload, .. } => SendStreamError::Full(payload),
                OutboundItem::Headers { .. } => unreachable!("DATA enqueue returned HEADERS"),
            },
        )?;
        if end_stream {
            self.shared.state.fetch_or(STREAM_SEND_CLOSED, Ordering::AcqRel);
        }
        if !self.shared.write_ready.swap(true, Ordering::AcqRel) {
            connection.publish_ready(self.shared.clone());
        }
        Ok(())
    }

    pub(crate) fn poll_send_data(
        &mut self,
        context: &mut Context<'_>,
        payload: &mut Option<IoBuf>,
        end_stream: bool,
    ) -> Poll<Result<(), SendStreamError>> {
        loop {
            let data = payload.take().expect("pending H2 DATA must retain its payload");
            match self.send_data(data, end_stream) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(SendStreamError::Full(data)) => *payload = Some(data),
                Err(error) => return Poll::Ready(Err(error)),
            }

            *self.shared.send_waker.lock().unwrap() = Some(context.waker().clone());
            self.shared.send_waiting.store(true, Ordering::Release);

            // The consumer may have freed capacity immediately before waiter
            // publication. Recheck after publishing to avoid a lost wakeup.
            if !self.shared.outbound().is_full() {
                self.shared.send_waiting.store(false, Ordering::Release);
                self.shared.send_waker.lock().unwrap().take();
                continue;
            }
            return Poll::Pending;
        }
    }

    pub(crate) fn poll_send_headers(
        &mut self,
        context: &mut Context<'_>,
        headers: &mut Option<HeaderBlock>,
        end_stream: bool,
    ) -> Poll<Result<(), SendHeadersError>> {
        loop {
            let block = headers.take().expect("pending H2 HEADERS must retain its block");
            match self.send_headers(block, end_stream) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(SendHeadersError::Full(block)) => *headers = Some(block),
                Err(error) => return Poll::Ready(Err(error)),
            }

            *self.shared.send_waker.lock().unwrap() = Some(context.waker().clone());
            self.shared.send_waiting.store(true, Ordering::Release);
            if !self.shared.outbound().is_full() {
                self.shared.send_waiting.store(false, Ordering::Release);
                self.shared.send_waker.lock().unwrap().take();
                continue;
            }
            return Poll::Pending;
        }
    }
}

impl H2StreamCancel {
    pub(crate) fn cancel(&self) {
        let Some(connection) = H2Connection::resource_pool().get(self.connection) else {
            return;
        };
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return;
        }
        connection.abandoned_streams.push(self.stream_id);
        connection.mark_work();
        if !connection.write_waiting.load(Ordering::Acquire) {
            H2Connection::try_drive(connection, DriveQuota::INLINE);
        } else {
            connection.wake_driver();
        }
    }
}

impl H2RecvStream {
    #[cfg(test)]
    pub(crate) fn id(&self) -> StreamId {
        self.shared.id
    }

    pub(crate) fn recv(&mut self) -> RecvEventFuture<'_> {
        RecvEventFuture { stream: self }
    }

    pub(crate) fn set_cancellation(&mut self, cancellation: CancellationToken) {
        let failed = self.shared.state.load(Ordering::Acquire) & STREAM_FAILED != 0;
        if failed {
            cancellation.cancel();
            return;
        }
        *self.shared.cancellation.lock().unwrap() = Some(cancellation.clone());
        if self.shared.state.load(Ordering::Acquire) & STREAM_FAILED != 0 {
            self.shared.cancellation.lock().unwrap().take();
            cancellation.cancel();
        }
    }

    fn notify_inbound_space(&self) {
        let connection = &self.connection;
        if !connection.inbound_blocked.load(Ordering::Acquire) {
            return;
        }
        connection.mark_work();
        H2Connection::try_drive(connection.clone(), DriveQuota::INLINE);
    }
}

impl Future for RecvEventFuture<'_> {
    type Output = Result<Option<ReceivedEvent>, RecvStreamError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let shared = &self.stream.shared;
        if let Some(event) = shared.pop_inbound() {
            self.stream.notify_inbound_space();
            return Poll::Ready(Ok(Some(event)));
        }
        let state = shared.state.load(Ordering::Acquire);
        if state & STREAM_FAILED != 0 {
            return Poll::Ready(Err(shared
                .recv_failure
                .lock()
                .unwrap()
                .unwrap_or(RecvStreamError::ConnectionClosed)));
        }
        if state & STREAM_REMOTE_CLOSED != 0 {
            if let Some(event) = shared.pop_inbound() {
                return Poll::Ready(Ok(Some(event)));
            }
            return Poll::Ready(Ok(None));
        }

        *shared.recv_waker.lock().unwrap() = Some(context.waker().clone());
        if let Some(event) = shared.pop_inbound() {
            shared.recv_waker.lock().unwrap().take();
            self.stream.notify_inbound_space();
            return Poll::Ready(Ok(Some(event)));
        }
        let state = shared.state.load(Ordering::Acquire);
        if state & STREAM_FAILED != 0 {
            shared.recv_waker.lock().unwrap().take();
            Poll::Ready(Err(shared
                .recv_failure
                .lock()
                .unwrap()
                .unwrap_or(RecvStreamError::ConnectionClosed)))
        } else if state & STREAM_REMOTE_CLOSED != 0 {
            shared.recv_waker.lock().unwrap().take();
            match shared.pop_inbound() {
                Some(event) => {
                    self.stream.notify_inbound_space();
                    Poll::Ready(Ok(Some(event)))
                }
                None => Poll::Ready(Ok(None)),
            }
        } else {
            Poll::Pending
        }
    }
}

impl OpenCompletion {
    fn complete(&self, result: Result<H2Stream, OpenStreamError>) {
        if self.is_canceled() {
            return;
        }
        *self.result.lock().unwrap() = Some(result);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }
}

impl Drop for OpenStreamFuture {
    fn drop(&mut self) {
        self.completion.canceled.store(true, Ordering::Release);
    }
}

impl Future for OpenStreamFuture {
    type Output = Result<H2Stream, OpenStreamError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.completion.result.lock().unwrap().take() {
            return Poll::Ready(result);
        }
        *self.completion.waker.lock().unwrap() = Some(context.waker().clone());
        match self.completion.result.lock().unwrap().take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

impl Future for AcceptStreamFuture<'_> {
    type Output = Option<AcceptedStream>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let connection = self.connection;
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            return Poll::Ready(None);
        }
        if let Some(stream) = connection.accepted_streams.pop() {
            return Poll::Ready(Some(stream));
        }
        *connection.accept_waker.lock().unwrap() = Some(context.waker().clone());
        if connection.state.load(Ordering::Acquire) == CONNECTION_CLOSED {
            connection.accept_waker.lock().unwrap().take();
            Poll::Ready(None)
        } else if let Some(stream) = connection.accepted_streams.pop() {
            connection.accept_waker.lock().unwrap().take();
            Poll::Ready(Some(stream))
        } else {
            Poll::Pending
        }
    }
}

fn h2_connection_id(raw: u64) -> ResourceId<H2Connection> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

#[cfg(test)]
mod tests;
