// SPDX-License-Identifier: Apache-2.0

use std::cell::UnsafeCell;
use std::io;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::TcpStream;

use super::socket::RegisterPendingError;
use super::{TcpSocket, TcpTransport};
use crate::runtime::profile::{self, ProfileEvent, ProfileStage};
use crate::runtime::{TimeScheduler, rpc_variables};
use crate::transport::inflight::{CallContext, call_id};
use crate::{
    Error, IoBuf, Resource, ResourceId, ResourcePoolError, Result, StaticResource, Status,
    StatusCode, Transport,
};

const NULL_ID: u64 = u64::MAX;
const UNCONNECTED_ID: u64 = u64::MAX - 1;

const CONNECTION_DISCONNECTED: u8 = 0;
const CONNECTION_CONNECTING: u8 = 1;
const CONNECTION_CONNECTED: u8 = 2;
const CONNECTION_CLOSED: u8 = 4;

const INLINE_BYTES: usize = 64 * 1024;
const INLINE_FRAMES: usize = 32;
const ASYNC_BYTES: usize = 256 * 1024;
const ASYNC_FRAMES: usize = 128;
const WRITEV_SLICES: usize = 64;
const WRITE_CLOSE_AFTER: u8 = 1;
const WRITE_COMPLETE_AFTER: u8 = 2;

/// Optional callback invoked after a socket is fully published.
///
/// The callback is intentionally outside protocol parsing. A client session may
/// use it to start or resume its read loop.
pub(crate) trait OnConnected: Send + Sync + 'static {
    fn on_connected(&self, connection: Resource<TcpConnection>, socket: Resource<TcpSocket>);
}

/// Transport-level write budget. The first producer uses a small inline quota;
/// continuation tasks use a larger quota to amortize wakeups.
#[derive(Clone, Copy, Debug)]
struct WriteQuota {
    bytes: usize,
    frames: usize,
}

impl WriteQuota {
    const INLINE: Self = Self { bytes: INLINE_BYTES, frames: INLINE_FRAMES };
    const ASYNC: Self = Self { bytes: ASYNC_BYTES, frames: ASYNC_FRAMES };
}

/// Intrusive write node.
///
/// `frame` and `context_id` are initialized once. The writer owns the current
/// node. The single active writer consumes `frame` in place as bytes are
/// accepted by the socket.
#[derive(StaticResource)]
pub(crate) struct WriteRequest {
    next: AtomicU64,
    context_id: AtomicU64,
    flags: u8,
    profile_sampled: bool,
    frame: UnsafeCell<IoBuf>,
}

// A `WriteRequest` frame is mutated only by the single writer that owns the
// active cursor. Producers may publish `next`, but never touch frame bytes.
unsafe impl Sync for WriteRequest {}

impl WriteRequest {
    fn new(frame: IoBuf, options: WriteOptions) -> Self {
        Self {
            next: AtomicU64::new(UNCONNECTED_ID),
            context_id: AtomicU64::new(
                options.context.map_or(NULL_ID, |context| context.into_owned_id().as_u64()),
            ),
            flags: options.flags,
            profile_sampled: options.profile_sampled,
            frame: UnsafeCell::new(frame),
        }
    }

    fn take_next(&self) -> Option<Resource<Self>> {
        let raw = self.next.swap(NULL_ID, Ordering::AcqRel);
        if raw == NULL_ID || raw == UNCONNECTED_ID {
            return None;
        }
        Some(unsafe { Self::resource_pool().reclaim_owned_id(write_request_id(raw)) })
    }

    fn peek_next(&self) -> Option<Resource<Self>> {
        let raw = self.next.load(Ordering::Acquire);
        if raw == NULL_ID || raw == UNCONNECTED_ID {
            return None;
        }
        Self::resource_pool().get(write_request_id(raw))
    }

    fn wait_connected_next_raw(&self) -> u64 {
        let mut spins = 0;
        loop {
            let raw = self.next.load(Ordering::Acquire);
            if raw != UNCONNECTED_ID {
                return raw;
            }
            if spins < 64 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        }
    }

    fn take_connected_next_raw(&self) -> u64 {
        let raw = self.wait_connected_next_raw();
        let taken = self.next.swap(NULL_ID, Ordering::AcqRel);
        debug_assert_eq!(taken, raw);
        raw
    }

    fn set_next(&self, next: Resource<Self>) {
        self.next.store(next.into_owned_id().as_u64(), Ordering::Release);
    }

    fn set_next_raw(&self, next: u64) {
        self.next.store(next, Ordering::Release);
    }

    fn set_next_none(&self) {
        self.next.store(NULL_ID, Ordering::Release);
    }

    fn take_context(&self) -> Option<Resource<CallContext>> {
        let raw = self.context_id.swap(NULL_ID, Ordering::AcqRel);
        if raw == NULL_ID {
            return None;
        }
        Some(unsafe { CallContext::resource_pool().reclaim_owned_id(call_id(raw)) })
    }

    fn frame(&self) -> &IoBuf {
        unsafe { &*self.frame.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn frame_mut(&self) -> &mut IoBuf {
        unsafe { &mut *self.frame.get() }
    }

    fn close_after_write(&self) -> bool {
        self.flags & WRITE_CLOSE_AFTER != 0
    }

    fn complete_after_write(&self) -> bool {
        self.flags & WRITE_COMPLETE_AFTER != 0
    }

    fn profile_sampled(&self) -> bool {
        self.profile_sampled
    }
}

impl Drop for WriteRequest {
    fn drop(&mut self) {
        let raw = self.context_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            drop(unsafe { CallContext::resource_pool().reclaim_owned_id(call_id(raw)) });
        }
    }
}

#[derive(Default)]
pub(crate) struct WriteOptions {
    context: Option<Resource<CallContext>>,
    flags: u8,
    profile_sampled: bool,
}

impl WriteOptions {
    pub(crate) fn client(context: Resource<CallContext>) -> Self {
        let profile_sampled = context.profile_sampled();
        Self { context: Some(context), flags: 0, profile_sampled }
    }

    pub(crate) fn server() -> Self {
        Self::default()
    }

    fn tracked(context: Resource<CallContext>) -> Self {
        Self { context: Some(context), flags: WRITE_COMPLETE_AFTER, profile_sampled: false }
    }

    pub(crate) fn profile_sampled(mut self, profile_sampled: bool) -> Self {
        self.profile_sampled = profile_sampled;
        self
    }

    pub(crate) fn close_after_write(mut self) -> Self {
        self.flags |= WRITE_CLOSE_AFTER;
        self
    }

    fn deadline_context(&self) -> Option<Resource<CallContext>> {
        self.context.as_ref().filter(|context| context.deadline().is_some()).cloned()
    }
}

/// TCP connection state shared by client sessions and accepted server sockets.
///
/// Lifetime is owned by `ResourcePool<TcpConnection>`. Client sessions keep a
/// disconnected endpoint until the first write connects it; accepted server
/// sockets start with an already published `TcpSocket`.
#[derive(StaticResource)]
pub(crate) struct TcpConnection {
    state: AtomicU8,
    write_head: AtomicU64,
    socket_id: AtomicU64,
    role: TcpConnectionRole,
    remote_addr: SocketAddr,
    connect_timeout: Option<Duration>,
    on_connected: Option<Box<dyn OnConnected>>,
}

enum TcpConnectionRole {
    Outbound,
    Accepted,
}

impl TcpConnection {
    pub(crate) fn create(
        remote_addr: SocketAddr,
        connect_timeout: Option<Duration>,
        on_connected: Option<Box<dyn OnConnected>>,
    ) -> Result<Resource<Self>> {
        Self::resource_pool()
            .acquire_with_init(|| Self {
                state: AtomicU8::new(CONNECTION_DISCONNECTED),
                write_head: AtomicU64::new(NULL_ID),
                socket_id: AtomicU64::new(NULL_ID),
                role: TcpConnectionRole::Outbound,
                remote_addr,
                connect_timeout,
                on_connected,
            })
            .map_err(pool_error)
    }

    pub(crate) fn accept(stream: TcpStream, remote_addr: SocketAddr) -> Result<Resource<Self>> {
        let socket = TcpSocket::resource_pool()
            .acquire_with_init(|| TcpSocket::new(TcpTransport::new(stream)))
            .map_err(pool_error)?;
        let socket_id = socket.into_owned_id().as_u64();
        match Self::resource_pool().acquire_with_init(|| Self {
            state: AtomicU8::new(CONNECTION_CONNECTED),
            write_head: AtomicU64::new(NULL_ID),
            socket_id: AtomicU64::new(socket_id),
            role: TcpConnectionRole::Accepted,
            remote_addr,
            connect_timeout: None,
            on_connected: None,
        }) {
            Ok(connection) => Ok(connection),
            Err(error) => {
                drop(unsafe {
                    TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(socket_id))
                });
                Err(pool_error(error))
            }
        }
    }

    /// Submits a frame for writing. The first node entering an empty queue owns
    /// immediate write progress; later nodes only link themselves after the
    /// previous tail and return.
    pub(crate) fn submit_write(
        connection: Resource<Self>,
        frame: IoBuf,
        options: WriteOptions,
    ) -> Result<()> {
        rpc_variables::transport().write_submitted();
        let profile_sampled = options.profile_sampled;
        let deadline_context = options.deadline_context();
        let stage_start = profile::start_if(profile_sampled);
        let request = WriteRequest::resource_pool()
            .acquire_with_init(|| WriteRequest::new(frame, options))
            .map_err(pool_error)?;
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::TransportWriteRequestAlloc, start);
        }

        if let Some(context) = deadline_context {
            TimeScheduler::register(context);
        }
        let stage_start = profile::start_if(profile_sampled);
        Self::submit_request(connection, request);
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::TransportSubmitQueue, start);
        }
        Ok(())
    }

    /// Submits one transport frame and waits until all of its bytes have been
    /// accepted by the socket. Protocol owners use this as their bounded
    /// backpressure point between incremental body frames.
    pub(crate) async fn write_frame(
        connection: Resource<Self>,
        frame: IoBuf,
        close_after_write: bool,
    ) -> std::result::Result<(), Status> {
        let context = CallContext::resource_pool()
            .acquire_with_init(|| CallContext::new(None, false))
            .map_err(|_| {
                Status::new(StatusCode::ResourceExhausted, "write receipt resources exhausted")
            })?;
        let token = crate::transport::inflight::CallToken::new(context.clone());
        let mut options = WriteOptions::tracked(context);
        if close_after_write {
            options = options.close_after_write();
        }
        Self::submit_write(connection, frame, options).map_err(Status::from)?;
        let completion = std::future::poll_fn(|cx| token.poll(cx)).await;
        match completion.status {
            Some(status) => Err(status),
            None => Ok(()),
        }
    }

    fn submit_request(connection: Resource<Self>, request: Resource<WriteRequest>) {
        let profile_sampled = request.profile_sampled();
        let request_id = request.id().as_u64();
        let previous = connection.write_head.swap(request_id, Ordering::AcqRel);
        if previous == NULL_ID {
            request.set_next_none();
            rpc_variables::transport().write_became_writer();
            if profile_sampled {
                profile::record_event(ProfileEvent::TransportSubmitBecameWriter, 1);
            }
            Self::ensure_connected_then_write(connection, request);
            return;
        }
        if profile_sampled {
            profile::record_event(ProfileEvent::TransportSubmitQueued, 1);
        }
        rpc_variables::transport().write_queued();

        request.set_next_raw(previous);
        std::mem::forget(request);
    }

    pub(crate) fn current_socket(&self) -> Option<Resource<TcpSocket>> {
        let raw = self.socket_id.load(Ordering::Acquire);
        if raw == NULL_ID {
            return None;
        }
        TcpSocket::resource_pool().get(tcp_socket_id(raw))
    }

    pub(crate) fn invalidate(&self, socket: Resource<TcpSocket>, status: Status) -> Status {
        let raw = socket.id().as_u64();
        let pending = match socket.begin_invalidate(status.clone()) {
            Ok(pending) => pending,
            Err(failure) => return failure,
        };
        let current = self
            .socket_id
            .compare_exchange(raw, NULL_ID, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if current {
            drop(unsafe { TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(raw)) });
            let next = match self.role {
                TcpConnectionRole::Outbound => CONNECTION_DISCONNECTED,
                TcpConnectionRole::Accepted => CONNECTION_CLOSED,
            };
            let _ = self.state.compare_exchange(
                CONNECTION_CONNECTED,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        log::debug!(
            "TCP socket invalidated: remote={}, socket={raw}, current={current}, status={status}",
            self.remote_addr
        );
        TcpSocket::fail_pending(pending, status.clone());
        // Shutdown comes last: a reader or writer woken by the kernel must observe the
        // canonical failure and the connection's next state before it resumes.
        if let Err(error) = socket.transport().shutdown() {
            log::warn!("TCP socket shutdown failed: remote={}, error={error}", self.remote_addr);
        }
        status
    }

    pub(crate) fn close(&self) {
        let previous = self.state.swap(CONNECTION_CLOSED, Ordering::AcqRel);
        if previous == CONNECTION_CLOSED {
            return;
        }
        log::debug!(
            "TCP connection closed: remote={}, previous_state={previous}",
            self.remote_addr
        );
        let raw = self.socket_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            let socket = unsafe { TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(raw)) };
            let status = Status::new(StatusCode::Unavailable, "closed");
            if let Ok(pending) = socket.begin_invalidate(status.clone()) {
                TcpSocket::fail_pending(pending, status);
                if let Err(error) = socket.transport().shutdown() {
                    log::warn!(
                        "TCP socket shutdown failed: remote={}, error={error}",
                        self.remote_addr
                    );
                }
            }
        }
    }

    pub(crate) fn remote_addr(&self) -> &SocketAddr {
        &self.remote_addr
    }

    pub(crate) async fn read_more(&self, input: &mut IoBuf) -> io::Result<usize> {
        let socket = self
            .current_socket()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "tcp socket closed"))?;
        let result = socket.read_more(input).await;
        if let Err(error) = &result {
            log::warn!(
                "TCP read failed: remote={}, socket={}, error={error}",
                self.remote_addr,
                socket.id().as_u64()
            );
            self.invalidate(socket, Status::new(StatusCode::Unavailable, error.to_string()));
        }
        result
    }

    fn ensure_connected_then_write(connection: Resource<Self>, request: Resource<WriteRequest>) {
        match connection.state.load(Ordering::Acquire) {
            CONNECTION_CONNECTED => Self::start_write(connection, request),
            CONNECTION_DISCONNECTED => {
                if connection
                    .state
                    .compare_exchange(
                        CONNECTION_DISCONNECTED,
                        CONNECTION_CONNECTING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    log::debug!("TCP connection connecting: remote={}", connection.remote_addr);
                    tokio::spawn(Self::connect_task(connection, request));
                }
            }
            CONNECTION_CONNECTING => {
                // The connect task will continue from the current queue head.
            }
            CONNECTION_CLOSED => {
                Self::fail_cursor_and_successors(
                    connection,
                    request,
                    Status::new(StatusCode::Unavailable, "closed"),
                );
            }
            _ => unreachable!(),
        }
    }

    async fn connect_task(connection: Resource<Self>, request: Resource<WriteRequest>) {
        let remote = connection.remote_addr;
        let result = match connection.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, TcpTransport::connect_endpoint(remote))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))
                .and_then(|result| result),
            None => TcpTransport::connect_endpoint(remote).await,
        };
        Self::finish_connect(connection, request, result).await;
    }

    async fn finish_connect(
        connection: Resource<Self>,
        request: Resource<WriteRequest>,
        result: io::Result<TcpTransport>,
    ) {
        match result {
            Ok(transport) => {
                let Ok(socket) =
                    TcpSocket::resource_pool().acquire_with_init(|| TcpSocket::new(transport))
                else {
                    log::error!("failed to allocate TCP socket: remote={}", connection.remote_addr);
                    let _ = connection.state.compare_exchange(
                        CONNECTION_CONNECTING,
                        CONNECTION_DISCONNECTED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    Self::fail_cursor_and_successors(
                        connection,
                        request,
                        Status::new(StatusCode::ResourceExhausted, "socket resources exhausted"),
                    );
                    return;
                };
                let raw = socket.clone().into_owned_id().as_u64();
                if connection
                    .socket_id
                    .compare_exchange(NULL_ID, raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    drop(unsafe {
                        TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(raw))
                    });
                    Self::fail_cursor_and_successors(
                        connection,
                        request,
                        Status::new(StatusCode::Unavailable, "closed"),
                    );
                    return;
                }
                if connection
                    .state
                    .compare_exchange(
                        CONNECTION_CONNECTING,
                        CONNECTION_CONNECTED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    if connection
                        .socket_id
                        .compare_exchange(raw, NULL_ID, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        drop(unsafe {
                            TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(raw))
                        });
                    }
                    let status = Status::new(StatusCode::Unavailable, "closed");
                    if let Ok(pending) = socket.begin_invalidate(status.clone()) {
                        TcpSocket::fail_pending(pending, status.clone());
                        if let Err(error) = socket.transport().shutdown() {
                            log::warn!(
                                "TCP socket shutdown failed: remote={}, error={error}",
                                connection.remote_addr
                            );
                        }
                    }
                    Self::fail_cursor_and_successors(connection, request, status);
                    return;
                }

                log::debug!(
                    "TCP connection connected: remote={}, socket={raw}",
                    connection.remote_addr
                );
                if let Some(callback) = connection.on_connected.as_ref() {
                    callback.on_connected(connection.clone(), socket.clone());
                }

                Self::keep_write(connection, socket, WriteCursor::new(request)).await;
            }
            Err(error) => {
                log::warn!("TCP connect failed: remote={}, error={error}", connection.remote_addr);
                let _ = connection.state.compare_exchange(
                    CONNECTION_CONNECTING,
                    CONNECTION_DISCONNECTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                let status = Status::new(StatusCode::Unavailable, error.to_string());
                Self::fail_cursor_and_successors(connection, request, status);
            }
        }
    }

    fn start_write(connection: Resource<Self>, request: Resource<WriteRequest>) {
        let Some(socket) = connection.current_socket() else {
            Self::fail_cursor_and_successors(
                connection,
                request,
                Status::new(StatusCode::Unavailable, "connection failed"),
            );
            return;
        };
        match Self::try_drain_write(
            connection.clone(),
            socket.clone(),
            WriteCursor::new(request),
            WriteQuota::INLINE,
        ) {
            WriteDrain::Done | WriteDrain::Failed => {}
            WriteDrain::Yield { cursor } => {
                tokio::spawn(async move {
                    Self::keep_write(connection, socket, cursor).await;
                });
            }
            WriteDrain::WouldBlock { cursor } => {
                tokio::spawn(async move {
                    if Self::wait_writable(&connection, &socket).await.is_ok() {
                        Self::keep_write(connection, socket, cursor).await;
                    } else {
                        Self::fail_cursor_and_successors(
                            connection,
                            cursor.request,
                            socket.failure(),
                        );
                    }
                });
            }
        }
    }

    async fn keep_write(
        connection: Resource<Self>,
        socket: Resource<TcpSocket>,
        mut cursor: WriteCursor,
    ) {
        loop {
            match Self::try_drain_write(
                connection.clone(),
                socket.clone(),
                cursor,
                WriteQuota::ASYNC,
            ) {
                WriteDrain::Done | WriteDrain::Failed => return,
                WriteDrain::Yield { cursor: next } => {
                    cursor = next;
                    tokio::task::yield_now().await;
                }
                WriteDrain::WouldBlock { cursor: next } => {
                    cursor = next;
                    if Self::wait_writable(&connection, &socket).await.is_err() {
                        Self::fail_cursor_and_successors(
                            connection,
                            cursor.request,
                            socket.failure(),
                        );
                        return;
                    }
                }
            }
        }
    }

    async fn wait_writable(
        connection: &Resource<Self>,
        socket: &Resource<TcpSocket>,
    ) -> io::Result<()> {
        match socket.transport().writable().await {
            Ok(()) => Ok(()),
            Err(error) => {
                log::warn!(
                    "TCP writable wait failed: remote={}, error={error}",
                    connection.remote_addr
                );
                connection.invalidate(
                    socket.clone(),
                    Status::new(StatusCode::Unavailable, error.to_string()),
                );
                Err(error)
            }
        }
    }

    fn try_drain_write(
        connection: Resource<Self>,
        socket: Resource<TcpSocket>,
        mut cursor: WriteCursor,
        quota: WriteQuota,
    ) -> WriteDrain {
        let mut bytes = 0;
        let mut frames = 0;
        loop {
            let profile_sampled = cursor.request.profile_sampled();
            let stage_start = profile::start_if(profile_sampled);
            if let Some(start) = stage_start {
                profile::record_elapsed(ProfileStage::TransportCurrentSocket, start);
            }

            while cursor.request.frame().is_empty() {
                frames += 1;
                match Self::finish_request(connection.clone(), socket.clone(), cursor) {
                    WriteProgress::Next(next) => cursor = next,
                    WriteProgress::Done => return WriteDrain::Done,
                }
            }

            Self::refresh_published(&connection, &mut cursor);

            let result = {
                let stage_start = profile::start_if(profile_sampled);
                let mut batch: [Option<Resource<WriteRequest>>; WRITEV_SLICES] =
                    std::array::from_fn(|_| None);
                let mut batch_len = 0;
                let mut next = Some(cursor.request.clone());
                while let Some(item) = next {
                    let close_after_write = item.close_after_write();
                    batch[batch_len] = Some(item);
                    batch_len += 1;
                    if close_after_write || batch_len == batch.len() {
                        break;
                    }
                    next = batch[batch_len - 1].as_ref().unwrap().peek_next();
                }
                if let Some(start) = stage_start {
                    profile::record_elapsed(ProfileStage::TransportBuildBatch, start);
                }

                let stage_start = profile::start_if(profile_sampled);
                let mut slices: [IoSlice<'_>; WRITEV_SLICES] =
                    std::array::from_fn(|_| IoSlice::new(&[]));
                let mut count = 0;
                for request in batch.iter().take(batch_len).flatten() {
                    count += request.frame().fill_io_slices(&mut slices[count..]);
                    if count == slices.len() {
                        break;
                    }
                }
                debug_assert!(count > 0);
                let attempted = slices[..count].iter().map(|slice| slice.len() as u64).sum::<u64>();
                if profile_sampled {
                    profile::record_event(ProfileEvent::TransportWriteAttemptBytes, attempted);
                    profile::record_event(ProfileEvent::TransportWriteIovCount, count as u64);
                }
                rpc_variables::transport().write_attempt(attempted, count as u64);
                if let Some(start) = stage_start {
                    profile::record_elapsed(ProfileStage::TransportBuildIoSlices, start);
                }

                let stage_start = profile::start_if(profile_sampled);
                let result = socket.transport().try_write_vectored(&slices[..count]);
                if let Some(start) = stage_start {
                    profile::record_elapsed(ProfileStage::TransportSyscallWrite, start);
                }
                result
            };

            let written = match result {
                Ok(0) => {
                    if profile_sampled {
                        profile::record_event(ProfileEvent::TransportWriteZero, 1);
                    }
                    rpc_variables::transport().write_zero();
                    return WriteDrain::WouldBlock { cursor };
                }
                Ok(written) => {
                    if profile_sampled {
                        profile::record_event(ProfileEvent::TransportWriteOk, 1);
                        profile::record_event(ProfileEvent::TransportWriteOkBytes, written as u64);
                    }
                    rpc_variables::transport().write_ok(written as u64);
                    written
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if profile_sampled {
                        profile::record_event(ProfileEvent::TransportWriteWouldBlock, 1);
                    }
                    rpc_variables::transport().write_would_block();
                    return WriteDrain::WouldBlock { cursor };
                }
                Err(error) => {
                    if profile_sampled {
                        profile::record_event(ProfileEvent::TransportWriteError, 1);
                    }
                    log::warn!(
                        "TCP write failed: remote={}, socket={}, error={error}",
                        connection.remote_addr,
                        socket.id().as_u64()
                    );
                    rpc_variables::transport().write_error();
                    let status = connection.invalidate(
                        socket,
                        Status::new(StatusCode::Unavailable, error.to_string()),
                    );
                    Self::fail_cursor_and_successors(connection, cursor.request, status);
                    return WriteDrain::Failed;
                }
            };

            bytes += written;
            let mut remaining = written;
            let stage_start = profile::start_if(profile_sampled);
            while remaining > 0 {
                let frame_len = cursor.request.frame().len();
                let consumed = remaining.min(frame_len);
                cursor.request.frame_mut().consume(consumed);
                remaining -= consumed;
                if !cursor.request.frame().is_empty() {
                    break;
                }

                frames += 1;
                match Self::finish_request(connection.clone(), socket.clone(), cursor) {
                    WriteProgress::Next(next) => cursor = next,
                    WriteProgress::Done => return WriteDrain::Done,
                }
            }
            if let Some(start) = stage_start {
                profile::record_elapsed(ProfileStage::TransportConsumeWritten, start);
            }

            if bytes >= quota.bytes || frames >= quota.frames {
                return WriteDrain::Yield { cursor };
            }
        }
    }

    /// Incorporates requests published since `cursor.tail_id` into the FIFO
    /// chain without releasing writer ownership.
    fn refresh_published(connection: &Self, cursor: &mut WriteCursor) {
        let published_head = connection.write_head.load(Ordering::Acquire);
        if published_head == cursor.tail_id {
            return;
        }

        assert_ne!(published_head, NULL_ID, "active writer lost queue ownership");
        let first = reverse_published_stack_until(published_head, cursor.tail_id);
        if cursor.request.id().as_u64() == cursor.tail_id {
            cursor.request.set_next(first);
        } else {
            let tail = WriteRequest::resource_pool()
                .get(write_request_id(cursor.tail_id))
                .expect("active write queue tail disappeared");
            tail.set_next(first);
        }
        cursor.tail_id = published_head;
    }

    fn finish_request(
        connection: Resource<Self>,
        socket: Resource<TcpSocket>,
        cursor: WriteCursor,
    ) -> WriteProgress {
        let profile_sampled = cursor.request.profile_sampled();
        let stage_start = profile::start_if(profile_sampled);
        let progress = Self::finish_request_inner(connection, socket, cursor, profile_sampled);
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::TransportFinishRequest, start);
        }
        progress
    }

    fn finish_request_inner(
        connection: Resource<Self>,
        socket: Resource<TcpSocket>,
        cursor: WriteCursor,
        profile_sampled: bool,
    ) -> WriteProgress {
        let WriteCursor { request, tail_id } = cursor;
        let close_after_write = request.close_after_write();
        if let Some(context) = request.take_context() {
            if request.complete_after_write() {
                context.complete(IoBuf::new(), IoBuf::new(), None);
            } else {
                let stage_start = profile::start_if(profile_sampled);
                let add_result = socket.register_pending(context);
                if let Some(start) = stage_start {
                    profile::record_elapsed(ProfileStage::TransportPendingAdd, start);
                }
                match add_result {
                    Ok(()) => {}
                    Err(RegisterPendingError::Failed { context, status }) => {
                        context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
                        Self::fail_successors_after(connection, request, status);
                        return WriteProgress::Done;
                    }
                    Err(RegisterPendingError::Capacity(context)) => {
                        log::error!(
                            "TCP pending call capacity exceeded: remote={}, socket={}",
                            connection.remote_addr,
                            socket.id().as_u64()
                        );
                        let status = Status::new(
                            StatusCode::ResourceExhausted,
                            "pending call capacity exceeded",
                        );
                        context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
                        connection.close();
                        Self::fail_successors_after(connection, request, status);
                        return WriteProgress::Done;
                    }
                }
            }
        }

        if close_after_write {
            connection.close();
            Self::fail_successors_after(
                connection,
                request,
                Status::new(StatusCode::Unavailable, "closed"),
            );
            return WriteProgress::Done;
        }

        if let Some(next) = request.take_next() {
            return WriteProgress::Next(WriteCursor { request: next, tail_id });
        }

        debug_assert_eq!(request.id().as_u64(), tail_id);
        match connection.write_head.compare_exchange(
            tail_id,
            NULL_ID,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => WriteProgress::Done,
            Err(actual) => WriteProgress::Next(WriteCursor {
                request: reverse_published_stack_until(actual, tail_id),
                tail_id: actual,
            }),
        }
    }

    fn fail_cursor(request: &WriteRequest, status: Status) {
        if let Some(context) = request.take_context() {
            context.complete(IoBuf::new(), IoBuf::new(), Some(status));
        }
    }

    fn fail_cursor_and_successors(
        connection: Resource<Self>,
        request: Resource<WriteRequest>,
        status: Status,
    ) {
        Self::fail_cursor(&request, status.clone());
        Self::fail_successors_after(connection, request, status);
    }

    fn fail_successors_after(
        connection: Resource<Self>,
        mut request: Resource<WriteRequest>,
        status: Status,
    ) {
        loop {
            if let Some(next) = request.take_next() {
                request = next;
                if let Some(context) = request.take_context() {
                    context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
                }
                continue;
            }

            if connection
                .write_head
                .compare_exchange(
                    request.id().as_u64(),
                    NULL_ID,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return;
            }

            let new_stack = connection.write_head.swap(NULL_ID, Ordering::AcqRel);
            if new_stack == NULL_ID {
                return;
            }
            request = reverse_published_stack_until(new_stack, request.id().as_u64());
            loop {
                if let Some(context) = request.take_context() {
                    context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
                }
                let Some(next) = request.take_next() else {
                    return;
                };
                request = next;
            }
        }
    }
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        let raw = self.socket_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            drop(unsafe { TcpSocket::resource_pool().reclaim_owned_id(tcp_socket_id(raw)) });
        }
    }
}

struct WriteCursor {
    /// Oldest request that has not been fully consumed yet.
    request: Resource<WriteRequest>,
    /// Newest request already attached to the writer-owned FIFO chain.
    tail_id: u64,
}

impl WriteCursor {
    fn new(request: Resource<WriteRequest>) -> Self {
        let tail_id = request.id().as_u64();
        Self { request, tail_id }
    }
}

enum WriteProgress {
    Next(WriteCursor),
    Done,
}

enum WriteDrain {
    Done,
    Failed,
    Yield { cursor: WriteCursor },
    WouldBlock { cursor: WriteCursor },
}

fn reverse_published_stack_until(raw_head: u64, stop_id: u64) -> Resource<WriteRequest> {
    debug_assert_ne!(raw_head, NULL_ID);
    debug_assert_ne!(raw_head, UNCONNECTED_ID);
    // `stop_id` is the active writer cursor. The published LIFO stack points to
    // it as a boundary marker, but does not own an extra Resource reference.
    let mut current =
        unsafe { WriteRequest::resource_pool().reclaim_owned_id(write_request_id(raw_head)) };
    let mut reversed: Option<Resource<WriteRequest>> = None;

    loop {
        let next_raw = current.take_connected_next_raw();
        if next_raw == stop_id {
            if let Some(previous) = reversed.take() {
                current.set_next(previous);
            } else {
                current.set_next_none();
            }
            return current;
        }
        if let Some(previous) = reversed.take() {
            current.set_next(previous);
        } else {
            current.set_next_none();
        }
        reversed = Some(current);

        if next_raw == NULL_ID {
            return reversed.expect("published stack must contain at least one node");
        }

        current =
            unsafe { WriteRequest::resource_pool().reclaim_owned_id(write_request_id(next_raw)) };
    }
}

fn pool_error(error: ResourcePoolError) -> Error {
    log::error!("TCP resource allocation failed: {error:?}");
    Error::ResourceExhausted
}

fn write_request_id(raw: u64) -> ResourceId<WriteRequest> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn tcp_socket_id(raw: u64) -> ResourceId<TcpSocket> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

#[cfg(test)]
mod tests;
