// SPDX-License-Identifier: Apache-2.0

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use bytes::Bytes;
use tokio::net::TcpStream;

use super::GrpcConfig;
use super::framing::{MessageStreamDecoder, encode_message};
use super::metadata::{
    append_metadata, decode_grpc_message, decode_status_details, metadata_from_headers,
};
use crate::client::recycler::{ClientActivityGuard, ClientConnectionState};
use crate::foundation::cow_hash_map::CowHashMap;
use crate::protocol::http2::connection::{
    ConnectionRole, FlowCredit, H2Connection, H2DirectSend, H2RecvStream, H2SendStream, H2Stream,
    H2StreamCancel, H2StreamDisposition, H2StreamDriver, H2StreamHandoff, OpenStreamError,
    ReceivedEvent, RecvStreamError, SendStreamError,
};
use crate::protocol::http2::hpack::{HeaderBlock, HeaderField, HeaderIndexing};
use crate::request_options::RequestOptions;
use crate::runtime::profile::{self, ProfileStage};
use crate::transport::connection::ConnectionKey;
use crate::transport::inflight::CallContext;
use crate::transport::tcp::TcpSocket;
use crate::{
    IoBuf, MethodDescriptor, Resource, ResourceId, StaticResource, Status, StatusCode, TcpTransport,
};

const NULL_ID: u64 = u64::MAX;
const UNCONNECTED_ID: u64 = u64::MAX - 1;
const CONNECTION_DISCONNECTED: u8 = 0;
const CONNECTION_CONNECTING: u8 = 1;
const CONNECTION_CONNECTED: u8 = 2;
const CONNECTION_CLOSED: u8 = 3;
const H2_SUBMIT_BATCH: usize = 8;
const INLINE_CALLS: usize = 32;
const ASYNC_CALLS: usize = 256;

#[derive(StaticResource)]
struct GrpcConnection {
    remote: SocketAddr,
    connect_timeout: Option<std::time::Duration>,
    state: AtomicU8,
    submit_head: AtomicU64,
    h2_id: AtomicU64,
    activity: Option<Resource<ClientConnectionState>>,
}

/// Pooled application state for one unary exchange. After publication, the
/// state is accessed only by the owning H2 connection driver.
#[derive(vaelo_derive::StaticResource)]
struct GrpcUnaryExchange {
    next: AtomicU64,
    context: Resource<CallContext>,
    cancel: Mutex<Option<H2StreamCancel>>,
    state: UnsafeCell<GrpcExchangeState>,
}

// Unary state is intentionally inline on the pooled exchange hot path.
#[allow(clippy::large_enum_variant)]
enum GrpcExchangeState {
    Unary(GrpcUnaryState),
    Streaming(Resource<GrpcStreamingCall>),
}

struct GrpcUnaryState {
    headers: Option<HeaderBlock>,
    payload: Option<IoBuf>,
    decoder: MessageStreamDecoder,
    response_headers_seen: bool,
    response: Option<IoBuf>,
    grpc_status: Option<Status>,
    metadata: crate::MetadataMap,
    trailers: crate::MetadataMap,
}

struct GrpcUnaryOutput {
    payload: IoBuf,
    metadata: crate::MetadataMap,
    trailers: crate::MetadataMap,
}

// The H2 connection owner serializes all access to `state` after publication.
unsafe impl Sync for GrpcUnaryExchange {}

#[derive(vaelo_derive::StaticResource)]
pub(crate) struct GrpcStreamingCall {
    context: Resource<CallContext>,
    open: Mutex<GrpcStreamingOpen>,
    cancel: Mutex<Option<H2StreamCancel>>,
    initial_headers: Mutex<Option<HeaderBlock>>,
    initial_payload: Mutex<Option<IoBuf>>,
    request_streaming: bool,
}

struct GrpcStreamingOpen {
    send: Option<Result<H2SendStream, Status>>,
    recv: Option<Result<H2RecvStream, Status>>,
    send_waker: Option<Waker>,
    recv_waker: Option<Waker>,
}

pub(crate) struct GrpcClientSendStream {
    call: Resource<GrpcStreamingCall>,
    h2: Option<H2SendStream>,
    finished: bool,
}

pub(crate) struct GrpcClientRecvStream {
    call: Resource<GrpcStreamingCall>,
    h2: Option<H2RecvStream>,
    decoder: MessageStreamDecoder,
    messages: VecDeque<IoBuf>,
    credits: Vec<FlowCredit>,
    release_credits_when_empty: bool,
    response_headers_seen: bool,
    trailers: Option<crate::MetadataMap>,
    terminal: bool,
}

static GRPC_CONNECTIONS: LazyLock<CowHashMap<ConnectionKey, Resource<GrpcConnection>>> =
    LazyLock::new(CowHashMap::new);

pub(crate) fn close_cached(key: &ConnectionKey) {
    GRPC_CONNECTIONS.remove(key);
}

pub(crate) fn submit_unary(
    key: ConnectionKey,
    short_connection: bool,
    method: &'static MethodDescriptor,
    options: RequestOptions,
    payload: IoBuf,
    config: GrpcConfig,
    context: Resource<CallContext>,
) -> Result<(), Status> {
    let payload = encode_message(payload, config.max_encoding_message_size)?;
    let headers = grpc_request_headers(key.remote, method, &options)?;
    let (connection, activity) = connection_for(key, short_connection)?;
    if let Some(activity) = activity {
        context.set_connection_activity(activity);
    }
    let exchange = GrpcUnaryExchange::resource_pool()
        .acquire_with_init(|| GrpcUnaryExchange {
            next: AtomicU64::new(UNCONNECTED_ID),
            context,
            cancel: Mutex::new(None),
            state: UnsafeCell::new(GrpcExchangeState::Unary(GrpcUnaryState {
                headers: Some(headers),
                payload: Some(payload),
                decoder: MessageStreamDecoder::new(config.max_decoding_message_size),
                response_headers_seen: false,
                response: None,
                grpc_status: None,
                metadata: crate::MetadataMap::new(),
                trailers: crate::MetadataMap::new(),
            })),
        })
        .map_err(|_| Status::new(StatusCode::ResourceExhausted, "gRPC call resources exhausted"))?;
    GrpcConnection::submit(connection, exchange);
    Ok(())
}

pub(crate) fn submit_streaming(
    key: ConnectionKey,
    short_connection: bool,
    method: &'static MethodDescriptor,
    options: RequestOptions,
    config: GrpcConfig,
    context: Resource<CallContext>,
    initial_payload: Option<IoBuf>,
) -> Result<(Option<GrpcClientSendStream>, GrpcClientRecvStream), Status> {
    let headers = grpc_request_headers(key.remote, method, &options)?;
    let (connection, activity) = connection_for(key, short_connection)?;
    if let Some(activity) = activity {
        context.set_connection_activity(activity);
    }
    let call = GrpcStreamingCall::resource_pool()
        .acquire_with_init(|| GrpcStreamingCall {
            context: context.clone(),
            open: Mutex::new(GrpcStreamingOpen {
                send: None,
                recv: None,
                send_waker: None,
                recv_waker: None,
            }),
            cancel: Mutex::new(None),
            initial_headers: Mutex::new(Some(headers)),
            request_streaming: initial_payload.is_none(),
            initial_payload: Mutex::new(initial_payload),
        })
        .map_err(|_| {
            Status::new(StatusCode::ResourceExhausted, "gRPC streaming resources exhausted")
        })?;
    if !context.set_completion_notifier(call.id().as_u64(), GrpcStreamingCall::context_completed) {
        return Err(context
            .completed_status()
            .unwrap_or_else(|| Status::new(StatusCode::Canceled, "gRPC streaming call canceled")));
    }
    let exchange = GrpcUnaryExchange::resource_pool()
        .acquire_with_init(|| GrpcUnaryExchange {
            next: AtomicU64::new(UNCONNECTED_ID),
            context,
            cancel: Mutex::new(None),
            state: UnsafeCell::new(GrpcExchangeState::Streaming(call.clone())),
        })
        .map_err(|_| Status::new(StatusCode::ResourceExhausted, "gRPC call resources exhausted"))?;
    GrpcConnection::submit(connection, exchange);
    let sender = call.request_streaming.then(|| GrpcClientSendStream {
        call: call.clone(),
        h2: None,
        finished: false,
    });
    Ok((
        sender,
        GrpcClientRecvStream {
            call,
            h2: None,
            decoder: MessageStreamDecoder::new(config.max_decoding_message_size),
            messages: VecDeque::new(),
            credits: Vec::new(),
            release_credits_when_empty: false,
            response_headers_seen: false,
            trailers: None,
            terminal: false,
        },
    ))
}

fn connection_for(
    key: ConnectionKey,
    short_connection: bool,
) -> Result<(Resource<GrpcConnection>, Option<ClientActivityGuard>), Status> {
    let create = || {
        let activity = if short_connection {
            None
        } else {
            Some(ClientConnectionState::create().map_err(Status::from)?)
        };
        GrpcConnection::resource_pool()
            .acquire_with_init(|| GrpcConnection {
                remote: key.remote,
                connect_timeout: key.connect_timeout,
                state: AtomicU8::new(CONNECTION_DISCONNECTED),
                submit_head: AtomicU64::new(NULL_ID),
                h2_id: AtomicU64::new(NULL_ID),
                activity,
            })
            .map_err(|_| {
                Status::new(StatusCode::ResourceExhausted, "gRPC connection resources exhausted")
            })
    };
    if short_connection {
        return create().map(|connection| (connection, None));
    }
    loop {
        let connection = GRPC_CONNECTIONS.get_or_try_insert_with(key, create)?;
        if let Some(activity) = connection.try_acquire_activity() {
            return Ok((connection, activity));
        }
        GRPC_CONNECTIONS.remove_if(&key, |cached| cached.id() == connection.id());
    }
}

impl GrpcConnection {
    fn try_acquire_activity(&self) -> Option<Option<ClientActivityGuard>> {
        match &self.activity {
            Some(activity) => ClientConnectionState::try_acquire(activity).map(Some),
            None => Some(None),
        }
    }

    fn try_retire(&self, now_ms: u64) -> bool {
        self.activity.as_ref().is_some_and(|activity| activity.try_retire(now_ms))
    }

    fn close(&self) {
        if self.state.swap(CONNECTION_CLOSED, Ordering::AcqRel) == CONNECTION_CLOSED {
            return;
        }
        let raw = self.h2_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            let h2 =
                unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_connection_id(raw)) };
            h2.close();
        }
    }

    fn submit(connection: Resource<Self>, exchange: Resource<GrpcUnaryExchange>) {
        let exchange_id = exchange.id().as_u64();
        let previous = connection.submit_head.swap(exchange_id, Ordering::AcqRel);
        if previous == NULL_ID {
            exchange.set_next_none();
            Self::ensure_connected_then_submit(connection, SubmitCursor::new(exchange));
        } else {
            exchange.set_next_raw(previous);
            std::mem::forget(exchange);
        }
    }

    fn ensure_connected_then_submit(connection: Resource<Self>, cursor: SubmitCursor) {
        loop {
            match connection.state.load(Ordering::Acquire) {
                CONNECTION_CONNECTED => {
                    let Some(h2) = connection.current_h2() else {
                        let _ = connection.state.compare_exchange(
                            CONNECTION_CONNECTED,
                            CONNECTION_DISCONNECTED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                        continue;
                    };
                    if h2.is_accepting_streams() {
                        Self::drain_connected(connection, h2, cursor, INLINE_CALLS);
                        return;
                    }
                    connection.invalidate_h2(&h2);
                }
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
                        tokio::spawn(Self::connect_task(connection, cursor));
                        return;
                    }
                }
                CONNECTION_CONNECTING => {
                    debug_assert!(false, "gRPC submit owner cannot race another connect owner");
                    return;
                }
                CONNECTION_CLOSED => {
                    Self::fail_cursor_and_successors(
                        connection,
                        cursor,
                        Status::new(StatusCode::Unavailable, "gRPC connection closed"),
                    );
                    return;
                }
                _ => unreachable!(),
            }
        }
    }

    async fn connect_task(connection: Resource<Self>, cursor: SubmitCursor) {
        let result =
            connect(connection.remote, connection.connect_timeout).await.and_then(|stream| {
                let socket = TcpSocket::resource_pool()
                    .acquire_with_init(|| TcpSocket::new(TcpTransport::new(stream)))
                    .map_err(|error| {
                        Status::new(
                            StatusCode::ResourceExhausted,
                            format!("gRPC socket resource unavailable: {error:?}"),
                        )
                    })?;
                H2Connection::try_new(ConnectionRole::Client, socket).map_err(recv_status)
            });
        match result {
            Ok(h2) => {
                let raw = h2.clone().into_owned_id().as_u64();
                if connection
                    .h2_id
                    .compare_exchange(NULL_ID, raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    drop(unsafe {
                        H2Connection::resource_pool().reclaim_owned_id(h2_connection_id(raw))
                    });
                    Self::fail_cursor_and_successors(
                        connection,
                        cursor,
                        Status::new(StatusCode::Unavailable, "gRPC connection closed"),
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
                    connection.invalidate_h2(&h2);
                    Self::fail_cursor_and_successors(
                        connection,
                        cursor,
                        Status::new(StatusCode::Unavailable, "gRPC connection closed"),
                    );
                    return;
                }
                Self::drain_connected_continuation(connection, h2, cursor).await;
            }
            Err(status) => {
                let _ = connection.state.compare_exchange(
                    CONNECTION_CONNECTING,
                    CONNECTION_DISCONNECTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                Self::fail_cursor_and_successors(connection, cursor, status);
            }
        }
    }

    fn current_h2(&self) -> Option<Resource<H2Connection>> {
        let raw = self.h2_id.load(Ordering::Acquire);
        if raw == NULL_ID { None } else { H2Connection::resource_pool().get(h2_connection_id(raw)) }
    }

    fn invalidate_h2(&self, h2: &Resource<H2Connection>) {
        let raw = h2.id().as_u64();
        if self.h2_id.compare_exchange(raw, NULL_ID, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            drop(unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_connection_id(raw)) });
            let _ = self.state.compare_exchange(
                CONNECTION_CONNECTED,
                CONNECTION_DISCONNECTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn drain_connected(
        connection: Resource<Self>,
        h2: Resource<H2Connection>,
        cursor: SubmitCursor,
        quota: usize,
    ) {
        // The submitting thread consumes the common case locally. Allocate one
        // continuation only when the fairness quota leaves published work.
        if let Some(cursor) = Self::drain_connected_chunk(&connection, &h2, cursor, quota) {
            tokio::spawn(Self::drain_connected_continuation(connection, h2, cursor));
        }
    }

    async fn drain_connected_continuation(
        connection: Resource<Self>,
        h2: Resource<H2Connection>,
        mut cursor: SubmitCursor,
    ) {
        loop {
            let Some(next) = Self::drain_connected_chunk(&connection, &h2, cursor, ASYNC_CALLS)
            else {
                return;
            };
            cursor = next;
            // Keep the same task across quota slices; yielding is sufficient.
            tokio::task::yield_now().await;
        }
    }

    fn drain_connected_chunk(
        connection: &Resource<Self>,
        h2: &Resource<H2Connection>,
        mut cursor: SubmitCursor,
        quota: usize,
    ) -> Option<SubmitCursor> {
        let mut published = 0;
        let mut calls = 0;
        loop {
            let current_id = cursor.exchange.id().as_u64();
            let next = cursor.exchange.take_next();
            let unavailable = if cursor.exchange.context.is_active() {
                let publication = GrpcUnaryExchange::into_publication(cursor.exchange);
                let result = match publication.publish(h2) {
                    Err((OpenStreamError::Busy, publication)) => {
                        h2.drive_published();
                        published = 0;
                        publication.publish(h2)
                    }
                    result => result,
                };
                match result {
                    Ok(()) => {
                        published += 1;
                        if published == H2_SUBMIT_BATCH {
                            h2.drive_published();
                            published = 0;
                        }
                        false
                    }
                    Err((error, publication)) => {
                        publication.reject(error);
                        matches!(
                            error,
                            OpenStreamError::ConnectionClosed | OpenStreamError::ConnectionDraining
                        )
                    }
                }
            } else {
                false
            };
            calls += 1;

            let Some(next_cursor) =
                Self::advance_cursor(connection, current_id, cursor.tail_id, next)
            else {
                if published != 0 {
                    h2.drive_published();
                }
                if unavailable {
                    connection.invalidate_h2(h2);
                }
                return None;
            };
            cursor = next_cursor;
            if unavailable {
                if published != 0 {
                    h2.drive_published();
                }
                connection.invalidate_h2(h2);
                Self::ensure_connected_then_submit(connection.clone(), cursor);
                return None;
            }
            if calls == quota {
                if published != 0 {
                    h2.drive_published();
                }
                return Some(cursor);
            }
        }
    }

    fn advance_cursor(
        connection: &Self,
        current_id: u64,
        tail_id: u64,
        next: Option<Resource<GrpcUnaryExchange>>,
    ) -> Option<SubmitCursor> {
        if let Some(exchange) = next {
            return Some(SubmitCursor { exchange, tail_id });
        }
        debug_assert_eq!(current_id, tail_id);
        match connection.submit_head.compare_exchange(
            tail_id,
            NULL_ID,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => None,
            Err(actual) => Some(SubmitCursor {
                exchange: reverse_published_stack_until(actual, tail_id),
                tail_id: actual,
            }),
        }
    }

    fn fail_cursor_and_successors(
        connection: Resource<Self>,
        mut cursor: SubmitCursor,
        status: Status,
    ) {
        loop {
            let current_id = cursor.exchange.id().as_u64();
            let next = cursor.exchange.take_next();
            cursor.exchange.complete(Err(status.clone()));
            let Some(next_cursor) =
                Self::advance_cursor(&connection, current_id, cursor.tail_id, next)
            else {
                return;
            };
            cursor = next_cursor;
        }
    }
}

pub(crate) fn recycle_idle_bucket(bucket: usize, now_ms: u64) {
    let mut retired = Vec::new();
    GRPC_CONNECTIONS.visit_bucket(bucket, |key, connection| {
        if connection.try_retire(now_ms) {
            retired.push((*key, connection.clone()));
        }
    });
    for (key, connection) in retired {
        GRPC_CONNECTIONS.remove_if(&key, |cached| cached.id() == connection.id());
        connection.close();
    }
}

impl Drop for GrpcConnection {
    fn drop(&mut self) {
        self.state.store(CONNECTION_CLOSED, Ordering::Release);
        let raw = self.h2_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            drop(unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_connection_id(raw)) });
        }
    }
}

struct SubmitCursor {
    exchange: Resource<GrpcUnaryExchange>,
    tail_id: u64,
}

enum GrpcOpenPublication {
    Driven(H2StreamDriver),
    Handoff(H2StreamHandoff),
}

impl GrpcOpenPublication {
    fn publish(self, h2: &H2Connection) -> Result<(), (OpenStreamError, GrpcOpenPublication)> {
        match self {
            Self::Driven(driver) => {
                h2.publish_stream(driver).map_err(|(error, driver)| (error, Self::Driven(driver)))
            }
            Self::Handoff(handoff) => h2
                .publish_handoff(handoff)
                .map_err(|(error, handoff)| (error, Self::Handoff(handoff))),
        }
    }

    fn reject(self, error: OpenStreamError) {
        match self {
            Self::Driven(driver) => driver.reject(error),
            Self::Handoff(handoff) => handoff.reject(error),
        }
    }
}

impl SubmitCursor {
    fn new(exchange: Resource<GrpcUnaryExchange>) -> Self {
        let tail_id = exchange.id().as_u64();
        Self { exchange, tail_id }
    }
}

impl GrpcUnaryExchange {
    fn into_publication(exchange: Resource<Self>) -> GrpcOpenPublication {
        // SAFETY: before publication, the submit owner is the only accessor.
        let streaming =
            unsafe { matches!(&*exchange.state.get(), GrpcExchangeState::Streaming(_)) };
        if streaming {
            GrpcOpenPublication::Handoff(Self::into_handoff(exchange))
        } else {
            GrpcOpenPublication::Driven(Self::into_driver(exchange))
        }
    }

    fn take_next(&self) -> Option<Resource<Self>> {
        let raw = self.next.swap(NULL_ID, Ordering::AcqRel);
        if raw == NULL_ID || raw == UNCONNECTED_ID {
            return None;
        }
        Some(unsafe { Self::resource_pool().reclaim_owned_id(resource_id(raw)) })
    }

    fn take_connected_next_raw(&self) -> u64 {
        let mut spins = 0;
        let raw = loop {
            let raw = self.next.load(Ordering::Acquire);
            if raw != UNCONNECTED_ID {
                break raw;
            }
            if spins < 64 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::yield_now();
            }
        };
        let taken = self.next.swap(NULL_ID, Ordering::AcqRel);
        debug_assert_eq!(taken, raw);
        raw
    }

    fn set_next(&self, next: Resource<Self>) {
        self.next.store(next.into_owned_id().as_u64(), Ordering::Release);
    }

    fn set_next_raw(&self, raw: u64) {
        self.next.store(raw, Ordering::Release);
    }

    fn set_next_none(&self) {
        self.next.store(NULL_ID, Ordering::Release);
    }

    fn into_driver(exchange: Resource<Self>) -> H2StreamDriver {
        let token = exchange.into_owned_id().as_u64();
        H2StreamDriver::new(
            token,
            Self::driver_active,
            Self::driver_opened,
            Self::driver_event,
            Self::driver_rejected,
            Self::driver_failed,
            Self::driver_release,
        )
    }

    fn into_handoff(exchange: Resource<Self>) -> H2StreamHandoff {
        let token = exchange.into_owned_id().as_u64();
        H2StreamHandoff::new(
            token,
            Self::handoff_active,
            Self::handoff_opened,
            Self::handoff_rejected,
            Self::driver_release,
        )
    }

    fn handoff_active(token: u64) -> bool {
        Self::get(token).is_some_and(|exchange| exchange.context.is_active())
    }

    fn handoff_opened(token: u64, stream: H2Stream) {
        let Some(exchange) = Self::get(token) else { return };
        let call = {
            // SAFETY: the H2 owner serializes the one-shot open callback.
            let state = unsafe { &*exchange.state.get() };
            let GrpcExchangeState::Streaming(call) = state else { return };
            call.clone()
        };
        call.opened(stream);
    }

    fn handoff_rejected(token: u64, error: OpenStreamError) {
        if let Some(exchange) = Self::get(token) {
            let status = open_status(error);
            // SAFETY: rejection is serialized by the connection submit owner.
            match unsafe { &*exchange.state.get() } {
                GrpcExchangeState::Unary(_) => exchange.complete(Err(status)),
                GrpcExchangeState::Streaming(call) => call.fail(status),
            }
        }
    }

    fn driver_active(token: u64) -> bool {
        Self::get(token).is_some_and(|exchange| exchange.context.is_active())
    }

    fn driver_opened(token: u64, cancel: H2StreamCancel) -> Option<H2DirectSend> {
        let exchange = Self::get(token)?;
        if !exchange.context.is_active() {
            return None;
        }
        // SAFETY: this callback is invoked by the H2 connection owner.
        let GrpcExchangeState::Unary(state) = (unsafe { &mut *exchange.state.get() }) else {
            return None;
        };
        *exchange.cancel.lock().unwrap() = Some(cancel);
        if !exchange.context.set_completion_notifier(token, Self::driver_cancel) {
            exchange.cancel.lock().unwrap().take();
            return None;
        }
        let headers = state.headers.take().expect("gRPC stream is opened once");
        let payload = state.payload.take().expect("gRPC request payload is submitted once");
        Some(H2DirectSend::complete(headers, payload))
    }

    fn driver_event(token: u64, event: ReceivedEvent) -> H2StreamDisposition {
        let Some(exchange) = Self::get(token) else {
            return H2StreamDisposition::Cancel;
        };
        if !exchange.context.is_active() {
            return H2StreamDisposition::Cancel;
        }
        let end_stream = match &event {
            ReceivedEvent::Headers { end_stream, .. } => *end_stream,
            ReceivedEvent::Data(data) => data.end_stream,
        };
        // SAFETY: this callback is invoked by the H2 connection owner.
        let GrpcExchangeState::Unary(state) = (unsafe { &mut *exchange.state.get() }) else {
            return H2StreamDisposition::Cancel;
        };
        match state.receive(event) {
            Ok(Some(payload)) => {
                exchange.complete(Ok(payload));
                H2StreamDisposition::Complete
            }
            Ok(None) => H2StreamDisposition::Continue,
            Err(status) => {
                exchange.complete(Err(status));
                if end_stream { H2StreamDisposition::Complete } else { H2StreamDisposition::Cancel }
            }
        }
    }

    fn driver_failed(token: u64, error: RecvStreamError) {
        if let Some(exchange) = Self::get(token) {
            exchange.complete(Err(recv_status(error)));
        }
    }

    fn driver_rejected(token: u64, error: OpenStreamError) {
        let status = open_status(error);
        if let Some(exchange) = Self::get(token) {
            exchange.complete(Err(status));
        }
    }

    fn driver_cancel(token: u64) {
        let Some(exchange) = Self::get(token) else {
            return;
        };
        let cancel = exchange.cancel.lock().unwrap().take();
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
    }

    fn driver_release(token: u64) {
        let id = resource_id(token);
        // SAFETY: H2StreamDriver owns exactly the reference transferred by
        // `into_driver`, and invokes this release callback exactly once.
        drop(unsafe { Self::resource_pool().reclaim_owned_id(id) });
    }

    fn get(token: u64) -> Option<Resource<Self>> {
        Self::resource_pool().get(resource_id(token))
    }

    fn complete(&self, result: Result<GrpcUnaryOutput, Status>) {
        // SAFETY: completion is serialized by the submit or H2 owner.
        if let GrpcExchangeState::Streaming(call) = unsafe { &*self.state.get() } {
            let status = match result {
                Ok(_) => Status::new(StatusCode::Internal, "invalid streaming completion"),
                Err(status) => status,
            };
            call.fail(status);
            return;
        }
        self.context.clear_completion_notifier();
        self.cancel.lock().unwrap().take();
        let complete_start = profile::start_if(self.context.profile_sampled());
        match result {
            Ok(output) => {
                let _ = self.context.complete_with_metadata(
                    output.payload,
                    IoBuf::new(),
                    None,
                    output.metadata,
                    output.trailers,
                );
            }
            Err(status) => {
                let _ = self.context.complete(IoBuf::new(), IoBuf::new(), Some(status));
            }
        }
        if let Some(start) = complete_start {
            profile::record_elapsed(ProfileStage::ClientCompleteContext, start);
        }
    }
}

impl GrpcStreamingCall {
    fn opened(self: Resource<Self>, stream: H2Stream) {
        if !self.context.is_active() {
            drop(stream);
            return;
        }
        let (mut send, recv) = stream.split();
        let Some(headers) = self.initial_headers.lock().unwrap().take() else {
            drop(send);
            drop(recv);
            return;
        };
        if send.send_headers(headers, false).is_err() {
            self.fail(Status::new(StatusCode::Unavailable, "failed to send gRPC request headers"));
            return;
        }
        if let Some(payload) = self.initial_payload.lock().unwrap().take()
            && send.send_data(payload, true).is_err()
        {
            self.fail(Status::new(StatusCode::Unavailable, "failed to send gRPC request message"));
            return;
        }
        let cancel = send.cancel_handle();
        *self.cancel.lock().unwrap() = Some(cancel);
        let (send_waker, recv_waker) = {
            let mut open = self.open.lock().unwrap();
            if open.send.is_some() || open.recv.is_some() {
                drop(open);
                if let Some(cancel) = self.cancel.lock().unwrap().take() {
                    cancel.cancel();
                }
                return;
            }
            if self.request_streaming {
                open.send = Some(Ok(send));
            } else {
                drop(send);
            }
            open.recv = Some(Ok(recv));
            (open.send_waker.take(), open.recv_waker.take())
        };
        if let Some(waker) = send_waker {
            waker.wake();
        }
        if let Some(waker) = recv_waker {
            waker.wake();
        }
    }

    fn context_completed(token: u64) {
        let Some(call) = Self::resource_pool().get(resource_id_for::<Self>(token)) else {
            return;
        };
        if let Some(cancel) = call.cancel.lock().unwrap().take() {
            cancel.cancel();
        }
        let status = call
            .context
            .completed_status()
            .unwrap_or_else(|| Status::new(StatusCode::Canceled, "gRPC streaming call canceled"));
        call.publish_failure(status);
    }

    fn fail(&self, status: Status) {
        self.context.clear_completion_notifier();
        let _ = self.context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
        if let Some(cancel) = self.cancel.lock().unwrap().take() {
            cancel.cancel();
        }
        self.publish_failure(status);
    }

    fn publish_failure(&self, status: Status) {
        let (send_waker, recv_waker) = {
            let mut open = self.open.lock().unwrap();
            if open.send.is_none() {
                open.send = Some(Err(status.clone()));
            }
            if open.recv.is_none() {
                open.recv = Some(Err(status));
            }
            (open.send_waker.take(), open.recv_waker.take())
        };
        if let Some(waker) = send_waker {
            waker.wake();
        }
        if let Some(waker) = recv_waker {
            waker.wake();
        }
    }

    fn poll_send(&self, context: &mut Context<'_>) -> Poll<Result<H2SendStream, Status>> {
        let mut open = self.open.lock().unwrap();
        if let Some(result) = open.send.take() {
            return Poll::Ready(result);
        }
        open.send_waker = Some(context.waker().clone());
        Poll::Pending
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Result<H2RecvStream, Status>> {
        let mut open = self.open.lock().unwrap();
        if let Some(result) = open.recv.take() {
            return Poll::Ready(result);
        }
        open.recv_waker = Some(context.waker().clone());
        Poll::Pending
    }

    fn complete_ok(&self) {
        self.context.clear_completion_notifier();
        self.cancel.lock().unwrap().take();
        let _ = self.context.complete(IoBuf::new(), IoBuf::new(), None);
    }

    pub(crate) fn cancel(&self) {
        let _ = self.context.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::Canceled, "gRPC streaming call canceled")),
        );
    }
}

impl GrpcClientSendStream {
    fn poll_open(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        if self.h2.is_some() {
            return Poll::Ready(Ok(()));
        }
        match self.call.poll_send(context) {
            Poll::Ready(Ok(send)) => {
                self.h2 = Some(send);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(status)) => Poll::Ready(Err(status)),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn poll_send(
        &mut self,
        context: &mut Context<'_>,
        payload: &mut Option<IoBuf>,
    ) -> Poll<Result<(), Status>> {
        if self.finished {
            return Poll::Ready(Err(Status::new(
                StatusCode::FailedPrecondition,
                "gRPC request stream is already finished",
            )));
        }
        match self.poll_open(context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(status)) => return Poll::Ready(Err(status)),
            Poll::Pending => return Poll::Pending,
        }
        match self.h2.as_mut().unwrap().poll_send_data(context, payload, false) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(send_stream_status(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn poll_finish(
        &mut self,
        context: &mut Context<'_>,
        payload: &mut Option<IoBuf>,
    ) -> Poll<Result<(), Status>> {
        if self.finished {
            return Poll::Ready(Ok(()));
        }
        match self.poll_open(context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(status)) => return Poll::Ready(Err(status)),
            Poll::Pending => return Poll::Pending,
        }
        match self.h2.as_mut().unwrap().poll_send_data(context, payload, true) {
            Poll::Ready(Ok(())) => {
                self.finished = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(send_stream_status(error))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn cancel(&self) {
        self.call.cancel();
    }
}

impl Drop for GrpcClientSendStream {
    fn drop(&mut self) {
        if !self.finished && self.call.context.is_active() {
            self.call.cancel();
        }
    }
}

impl GrpcClientRecvStream {
    fn poll_open(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        if self.h2.is_some() {
            return Poll::Ready(Ok(()));
        }
        match self.call.poll_recv(context) {
            Poll::Ready(Ok(recv)) => {
                self.h2 = Some(recv);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(status)) => Poll::Ready(Err(status)),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn poll_response_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<crate::MetadataMap, Status>> {
        match self.poll_open(context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(status)) => return Poll::Ready(Err(status)),
            Poll::Pending => return Poll::Pending,
        }
        match std::pin::pin!(self.h2.as_mut().unwrap().recv()).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(recv_status(error))),
            Poll::Ready(Ok(None)) => Poll::Ready(Err(Status::new(
                StatusCode::Unknown,
                "gRPC response ended before initial metadata",
            ))),
            Poll::Ready(Ok(Some(ReceivedEvent::Data(_)))) => Poll::Ready(Err(Status::new(
                StatusCode::Internal,
                "gRPC DATA arrived before response headers",
            ))),
            Poll::Ready(Ok(Some(ReceivedEvent::Headers { headers, end_stream, trailers }))) => {
                if trailers {
                    return Poll::Ready(Err(Status::new(
                        StatusCode::Internal,
                        "gRPC response started with trailers",
                    )));
                }
                if headers.pseudo.status != Some(200) {
                    return Poll::Ready(Err(Status::new(
                        StatusCode::Unavailable,
                        format!("gRPC HTTP status {:?}", headers.pseudo.status),
                    )));
                }
                validate_grpc_content_type(&headers)?;
                if end_stream {
                    return Poll::Ready(Err(Status::new(
                        StatusCode::Unknown,
                        "gRPC response ended without grpc-status",
                    )));
                }
                self.response_headers_seen = true;
                Poll::Ready(metadata_from_headers(&headers))
            }
        }
    }

    pub(crate) fn poll_message(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<IoBuf>, Status>> {
        if let Some(message) = self.messages.pop_front() {
            self.release_credits_if_possible();
            return Poll::Ready(Ok(Some(message)));
        }
        if self.terminal {
            return Poll::Ready(Ok(None));
        }
        if !self.response_headers_seen {
            return Poll::Ready(Err(Status::new(
                StatusCode::Internal,
                "gRPC response metadata has not been consumed",
            )));
        }
        loop {
            match std::pin::pin!(self.h2.as_mut().unwrap().recv()).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(recv_status(error))),
                Poll::Ready(Ok(None)) => {
                    return Poll::Ready(Err(Status::new(
                        StatusCode::Unknown,
                        "gRPC response ended without grpc-status",
                    )));
                }
                Poll::Ready(Ok(Some(ReceivedEvent::Data(data)))) => {
                    let (payload, end_stream, credit) = data.into_parts_with_credit();
                    self.credits.push(credit);
                    let messages = &mut self.messages;
                    self.decoder.push_each(payload, end_stream, |message| {
                        messages.push_back(message);
                        Ok(())
                    })?;
                    // Once the bounded decoder owns the bytes, retaining H2
                    // credit until a whole gRPC message arrives would
                    // deadlock responses larger than the initial H2 window.
                    self.release_credits_when_empty = true;
                    self.release_credits_if_possible();
                    if end_stream {
                        return Poll::Ready(Err(Status::new(
                            StatusCode::Unknown,
                            "gRPC response DATA ended without grpc-status",
                        )));
                    }
                    if let Some(message) = self.messages.pop_front() {
                        self.release_credits_if_possible();
                        return Poll::Ready(Ok(Some(message)));
                    }
                }
                Poll::Ready(Ok(Some(ReceivedEvent::Headers { headers, end_stream, trailers }))) => {
                    if !trailers || !end_stream {
                        return Poll::Ready(Err(Status::new(
                            StatusCode::Internal,
                            "invalid gRPC response trailers",
                        )));
                    }
                    self.decoder.push_each(IoBuf::new(), true, |_| Ok(()))?;
                    let status = grpc_status_from_headers(&headers)?.ok_or_else(|| {
                        Status::new(StatusCode::Unknown, "gRPC response missing grpc-status")
                    })?;
                    self.trailers = Some(metadata_from_headers(&headers)?);
                    self.terminal = true;
                    self.credits.clear();
                    if status.code() == StatusCode::Ok {
                        self.call.complete_ok();
                        return Poll::Ready(Ok(None));
                    }
                    self.call.fail(status.clone());
                    return Poll::Ready(Err(status));
                }
            }
        }
    }

    fn release_credits_if_possible(&mut self) {
        if self.messages.is_empty() && self.release_credits_when_empty {
            self.credits.clear();
            self.release_credits_when_empty = false;
        }
    }

    pub(crate) fn trailers(&self) -> Option<&crate::MetadataMap> {
        self.trailers.as_ref()
    }

    pub(crate) fn cancel(&self) {
        self.call.cancel();
    }
}

impl Drop for GrpcClientRecvStream {
    fn drop(&mut self) {
        if !self.terminal && self.call.context.is_active() {
            self.call.cancel();
        }
    }
}

fn open_status(error: OpenStreamError) -> Status {
    match error {
        OpenStreamError::Busy => {
            Status::new(StatusCode::ResourceExhausted, "gRPC connection open queue is full")
        }
        OpenStreamError::ConnectionClosed | OpenStreamError::ConnectionDraining => {
            Status::new(StatusCode::Unavailable, "gRPC connection is unavailable")
        }
    }
}

impl GrpcUnaryState {
    fn receive(&mut self, event: ReceivedEvent) -> Result<Option<GrpcUnaryOutput>, Status> {
        let end_stream = match event {
            ReceivedEvent::Headers { headers, end_stream, trailers } => {
                if !trailers {
                    if headers.pseudo.status != Some(200) {
                        return Err(Status::new(
                            StatusCode::Unavailable,
                            format!("gRPC HTTP status {:?}", headers.pseudo.status),
                        ));
                    }
                    validate_grpc_content_type(&headers)?;
                    self.response_headers_seen = true;
                    self.metadata = metadata_from_headers(&headers)?;
                }
                if trailers || end_stream {
                    self.grpc_status = grpc_status_from_headers(&headers)?;
                    self.trailers = metadata_from_headers(&headers)?;
                }
                end_stream
            }
            ReceivedEvent::Data(data) => {
                if !self.response_headers_seen {
                    return Err(Status::new(
                        StatusCode::Internal,
                        "gRPC DATA arrived before response headers",
                    ));
                }
                let (payload, end_stream) = data.into_parts();
                self.push_payload(payload, end_stream)?;
                end_stream
            }
        };
        if !end_stream {
            return Ok(None);
        }
        let status = self
            .grpc_status
            .take()
            .ok_or_else(|| Status::new(StatusCode::Unknown, "gRPC response missing grpc-status"))?;
        if status.code() != StatusCode::Ok {
            return Err(status);
        }
        self.push_payload(IoBuf::new(), true)?;
        Ok(Some(GrpcUnaryOutput {
            payload: self.response.take().unwrap_or_default(),
            metadata: std::mem::take(&mut self.metadata),
            trailers: std::mem::take(&mut self.trailers),
        }))
    }

    fn push_payload(&mut self, payload: IoBuf, end_stream: bool) -> Result<(), Status> {
        let response = &mut self.response;
        self.decoder.push_each(payload, end_stream, |message| {
            if response.replace(message).is_some() {
                return Err(Status::new(
                    StatusCode::Internal,
                    "gRPC unary response contains more than one message",
                ));
            }
            Ok(())
        })
    }
}

async fn connect(
    remote: SocketAddr,
    connect_timeout: Option<std::time::Duration>,
) -> Result<TcpStream, Status> {
    let connecting = TcpStream::connect(remote);
    let stream = match connect_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connecting)
            .await
            .map_err(|_| Status::new(StatusCode::DeadlineExceeded, "connect timeout"))?,
        None => connecting.await,
    }
    .map_err(|error| Status::new(StatusCode::Unavailable, error.to_string()))?;
    stream
        .set_nodelay(true)
        .map_err(|error| Status::new(StatusCode::Unavailable, error.to_string()))?;
    Ok(stream)
}

fn resource_id(raw: u64) -> ResourceId<GrpcUnaryExchange> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn resource_id_for<T>(raw: u64) -> ResourceId<T> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn send_stream_status(error: SendStreamError) -> Status {
    match error {
        SendStreamError::Closed(_) => {
            Status::new(StatusCode::Unavailable, "gRPC request stream is closed")
        }
        SendStreamError::HeadersRequired(_) => {
            Status::new(StatusCode::Internal, "gRPC request headers were not sent")
        }
        SendStreamError::Full(_) => {
            Status::new(StatusCode::ResourceExhausted, "gRPC request stream queue is full")
        }
    }
}

fn h2_connection_id(raw: u64) -> ResourceId<H2Connection> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn reverse_published_stack_until(raw_head: u64, stop_id: u64) -> Resource<GrpcUnaryExchange> {
    debug_assert_ne!(raw_head, NULL_ID);
    debug_assert_ne!(raw_head, UNCONNECTED_ID);
    let mut current =
        unsafe { GrpcUnaryExchange::resource_pool().reclaim_owned_id(resource_id(raw_head)) };
    let mut reversed: Option<Resource<GrpcUnaryExchange>> = None;
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
            return reversed.expect("published stack must contain at least one exchange");
        }
        current =
            unsafe { GrpcUnaryExchange::resource_pool().reclaim_owned_id(resource_id(next_raw)) };
    }
}

fn grpc_request_headers(
    remote: SocketAddr,
    method: &MethodDescriptor,
    options: &RequestOptions,
) -> Result<HeaderBlock, Status> {
    let mut headers = HeaderBlock::new();
    headers.pseudo.method = Some(Bytes::from_static(b"POST"));
    headers.pseudo.scheme = Some(Bytes::from_static(b"http"));
    headers.pseudo.authority = Some(Bytes::from(remote.to_string()));
    headers.pseudo.path =
        Some(Bytes::from(format!("/{}/{}", method.service.trim_start_matches('/'), method.method)));
    headers.push(HeaderField {
        name: Bytes::from_static(b"content-type"),
        value: Bytes::from_static(b"application/grpc"),
        indexing: HeaderIndexing::WithoutIndexing,
    });
    headers.push(HeaderField {
        name: Bytes::from_static(b"te"),
        value: Bytes::from_static(b"trailers"),
        indexing: HeaderIndexing::WithoutIndexing,
    });
    if let Some(deadline) = options.deadline {
        headers.push(HeaderField {
            name: Bytes::from_static(b"grpc-timeout"),
            value: grpc_timeout_value(deadline),
            indexing: HeaderIndexing::NeverIndexed,
        });
    }
    append_metadata(&mut headers, &options.metadata)?;
    Ok(headers)
}

fn grpc_timeout_value(deadline: Instant) -> Bytes {
    const MAX_VALUE: u128 = 99_999_999;
    const UNITS: [(u128, u8); 6] = [
        (1, b'n'),
        (1_000, b'u'),
        (1_000_000, b'm'),
        (1_000_000_000, b'S'),
        (60_000_000_000, b'M'),
        (3_600_000_000_000, b'H'),
    ];
    let nanos = deadline
        .checked_duration_since(Instant::now())
        .map_or(1, |duration| duration.as_nanos().max(1));
    for (scale, suffix) in UNITS {
        let value = nanos.saturating_add(scale - 1) / scale;
        if value <= MAX_VALUE {
            return Bytes::from(format!("{value}{}", char::from(suffix)));
        }
    }
    Bytes::from_static(b"99999999H")
}

fn grpc_status_from_headers(headers: &HeaderBlock) -> Result<Option<Status>, Status> {
    let Some(value) = header_value(headers, b"grpc-status") else { return Ok(None) };
    let code = std::str::from_utf8(value)
        .map_err(|_| Status::new(StatusCode::Internal, "grpc-status is not ASCII"))?
        .parse::<i32>()
        .map_err(|_| Status::new(StatusCode::Internal, "grpc-status is not numeric"))?;
    let code = StatusCode::from_i32(code)
        .ok_or_else(|| Status::new(StatusCode::Internal, "unknown gRPC status code"))?;
    let message = header_value(headers, b"grpc-message")
        .map(decode_grpc_message)
        .transpose()?
        .unwrap_or_default();
    let details = header_value(headers, b"grpc-status-details-bin")
        .map(decode_status_details)
        .transpose()?
        .unwrap_or_default();
    let mut status = Status::new(code, message).with_details(details);
    *status.metadata_mut() = metadata_from_headers(headers)?;
    Ok(Some(status))
}

fn validate_grpc_content_type(headers: &HeaderBlock) -> Result<(), Status> {
    if header_value(headers, b"content-type")
        .is_some_and(|value| value.starts_with(b"application/grpc"))
    {
        Ok(())
    } else {
        Err(Status::new(StatusCode::Internal, "invalid gRPC response content-type"))
    }
}

fn header_value<'a>(headers: &'a HeaderBlock, name: &[u8]) -> Option<&'a [u8]> {
    headers.fields().find(|field| field.name.as_ref() == name).map(|field| field.value.as_ref())
}

fn recv_status(error: RecvStreamError) -> Status {
    match error {
        RecvStreamError::ConnectionClosed => {
            Status::new(StatusCode::Unavailable, "gRPC connection closed")
        }
        RecvStreamError::ResourceExhausted => {
            Status::new(StatusCode::ResourceExhausted, "HTTP/2 connection resources exhausted")
        }
        RecvStreamError::ProtocolError => {
            Status::new(StatusCode::Internal, "invalid HTTP/2 response")
        }
        RecvStreamError::Reset(code) => {
            Status::new(StatusCode::Unavailable, format!("gRPC stream reset: {code}"))
        }
        RecvStreamError::UnprocessedByPeer(code) => Status::new(
            StatusCode::Unavailable,
            format!("gRPC stream was not processed before GOAWAY: {code}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::cow_hash_map::DEFAULT_BUCKETS;

    fn test_exchange() -> Resource<GrpcUnaryExchange> {
        let context = CallContext::resource_pool()
            .acquire_with_init(|| CallContext::new(None, false))
            .unwrap();
        GrpcUnaryExchange::resource_pool()
            .acquire_with_init(|| GrpcUnaryExchange {
                next: AtomicU64::new(UNCONNECTED_ID),
                context,
                cancel: Mutex::new(None),
                state: UnsafeCell::new(GrpcExchangeState::Unary(GrpcUnaryState {
                    headers: Some(HeaderBlock::new()),
                    payload: Some(IoBuf::new()),
                    decoder: MessageStreamDecoder::new(1024),
                    response_headers_seen: false,
                    response: None,
                    grpc_status: None,
                    metadata: crate::MetadataMap::new(),
                    trailers: crate::MetadataMap::new(),
                })),
            })
            .unwrap()
    }

    #[test]
    fn grpc_timeout_uses_a_bounded_rounding_up_wire_value() {
        assert_eq!(grpc_timeout_value(Instant::now() - std::time::Duration::from_secs(1)), "1n");
        let value = grpc_timeout_value(Instant::now() + std::time::Duration::from_secs(200));
        assert_eq!(value.last(), Some(&b'm'));
        assert!(value.len() <= 9);
        assert!(value[..value.len() - 1].iter().all(u8::is_ascii_digit));
    }

    #[test]
    fn published_exchange_stack_is_restored_to_fifo_order() {
        let first = test_exchange();
        first.set_next_none();
        let second = test_exchange();
        let second_id = second.id().as_u64();
        second.set_next_raw(first.id().as_u64());
        std::mem::forget(second);
        let third = test_exchange();
        let third_id = third.id().as_u64();
        third.set_next_raw(second_id);
        std::mem::forget(third);

        let second = reverse_published_stack_until(third_id, first.id().as_u64());
        assert_eq!(second.id().as_u64(), second_id);
        let third = second.take_next().expect("third exchange must follow second");
        assert_eq!(third.id().as_u64(), third_id);
        assert!(third.take_next().is_none());
    }

    #[test]
    fn recycler_preserves_active_connection_then_replaces_it_when_idle() {
        let key = ConnectionKey {
            remote: "127.0.0.1:61235".parse().unwrap(),
            tag: 0,
            slot: 0,
            connect_timeout: None,
        };
        let (connection, activity) = connection_for(key, false).unwrap();
        let original_id = connection.id();

        for bucket in 0..DEFAULT_BUCKETS {
            recycle_idle_bucket(bucket, u64::MAX);
        }
        let (same_connection, second_activity) = connection_for(key, false).unwrap();
        assert_eq!(same_connection.id(), original_id);

        drop(second_activity);
        drop(activity);
        for bucket in 0..DEFAULT_BUCKETS {
            recycle_idle_bucket(bucket, u64::MAX);
        }
        let (replacement, replacement_activity) = connection_for(key, false).unwrap();
        assert_ne!(replacement.id(), original_id);

        drop(replacement_activity);
        close_cached(&key);
    }

    #[test]
    fn short_connections_do_not_allocate_recycler_state() {
        let key = ConnectionKey {
            remote: "127.0.0.1:61236".parse().unwrap(),
            tag: 0,
            slot: 0,
            connect_timeout: None,
        };
        let (connection, activity) = connection_for(key, true).unwrap();
        assert!(activity.is_none());
        assert!(connection.activity.is_none());
    }
}
