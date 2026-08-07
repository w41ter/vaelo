// SPDX-License-Identifier: Apache-2.0

//! Ordinary HTTP client adapter over the caller-first HTTP/2 connection.
//!
//! The buffered request path is a single future driven by its caller: opening
//! a stream, publishing HEADERS + DATA, and receiving response HEADERS never
//! requires a per-request task.  Only connection establishment is detached,
//! once per physical connection.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Response};
use tokio::net::TcpStream;

use super::connection::{
    ConnectionRole, H2Connection, H2RecvStream, H2SendStream, H2Stream, H2StreamCancel,
    H2StreamHandoff, OpenStreamError, ReceivedEvent, RecvStreamError,
};
use super::hpack::{HeaderBlock, HeaderField, HeaderIndexing};
use crate::foundation::cow_hash_map::CowHashMap;
use crate::http::{ErasedBody, Frame, Incoming, SizeHint};
use crate::runtime::TimeScheduler;
use crate::transport::connection::ConnectionKey;
use crate::transport::inflight::{CallContext, CallToken};
use crate::transport::tcp::TcpSocket;
use crate::{Error, IoBuf, Resource, ResourceId, StaticResource, Status, StatusCode, TcpTransport};

const NULL_ID: u64 = u64::MAX;
const UNCONNECTED_ID: u64 = u64::MAX - 1;
const DISCONNECTED: u8 = 0;
const CONNECTING: u8 = 1;
const CONNECTED: u8 = 2;
const CLOSED: u8 = 3;
const H2_SUBMIT_BATCH: usize = 8;
const INLINE_CALLS: usize = 32;
const ASYNC_CALLS: usize = 256;

static CONNECTIONS: LazyLock<CowHashMap<ConnectionKey, Resource<Http2ClientSession>>> =
    LazyLock::new(CowHashMap::new);

#[derive(StaticResource)]
pub(crate) struct Http2ClientSession {
    remote: SocketAddr,
    connect_timeout: Option<std::time::Duration>,
    state: AtomicU8,
    submit_head: AtomicU64,
    h2_id: AtomicU64,
}

pub(crate) enum RequestBody {
    Buffered(Option<IoBuf>),
    Streaming(Pin<Box<dyn ErasedBody + Send>>),
}

pub(crate) struct Http2Call {
    open: Option<Resource<Http2Open>>,
    context: Resource<CallContext>,
    recv: Option<H2RecvStream>,
    cancel: Option<H2StreamCancel>,
    finished: bool,
}

#[derive(StaticResource)]
struct Http2Open {
    next: AtomicU64,
    active: AtomicBool,
    attached: AtomicBool,
    context: Resource<CallContext>,
    cancel: Mutex<Option<H2StreamCancel>>,
    inner: Mutex<Http2OpenInner>,
}

struct Http2OpenInner {
    remote: SocketAddr,
    request: Option<(http::request::Parts, RequestBody)>,
    result: Option<crate::Result<Http2Opened>>,
    waker: Option<Waker>,
}

struct Http2Opened {
    recv: H2RecvStream,
    cancel: H2StreamCancel,
    context: Resource<CallContext>,
}

struct SubmitCursor {
    open: Resource<Http2Open>,
    tail_id: u64,
}

impl Http2ClientSession {
    pub(crate) fn get_or_create(key: ConnectionKey, short: bool) -> crate::Result<Resource<Self>> {
        let create = || {
            Self::resource_pool()
                .acquire_with_init(|| Self {
                    remote: key.remote,
                    connect_timeout: key.connect_timeout,
                    state: AtomicU8::new(DISCONNECTED),
                    submit_head: AtomicU64::new(NULL_ID),
                    h2_id: AtomicU64::new(NULL_ID),
                })
                .map_err(|_| Error::ResourceExhausted)
        };
        if short { create() } else { CONNECTIONS.get_or_try_insert_with(key, create) }
    }

    pub(crate) fn issue(
        self: Resource<Self>,
        request: RequestBody,
        parts: http::request::Parts,
        deadline: Option<Instant>,
    ) -> crate::Result<Http2Call> {
        let context = CallContext::resource_pool()
            .acquire_with_init(|| CallContext::new(deadline, false))
            .map_err(|_| Error::ResourceExhausted)?;
        let open = Http2Open::resource_pool()
            .acquire_with_init(|| Http2Open {
                next: AtomicU64::new(UNCONNECTED_ID),
                active: AtomicBool::new(true),
                attached: AtomicBool::new(true),
                context: context.clone(),
                cancel: Mutex::new(None),
                inner: Mutex::new(Http2OpenInner {
                    remote: self.remote,
                    request: Some((parts, request)),
                    result: None,
                    waker: None,
                }),
            })
            .map_err(|_| Error::ResourceExhausted)?;
        let call = Http2Call {
            open: Some(open.clone()),
            context: context.clone(),
            recv: None,
            cancel: None,
            finished: false,
        };
        if !context.set_completion_notifier(open.id().as_u64(), Http2Open::context_completed) {
            return Err(Error::Remote(
                context.completed_status().unwrap_or_else(|| {
                    Status::new(StatusCode::Canceled, "HTTP/2 request canceled")
                }),
            ));
        }
        Self::submit(self, open);
        TimeScheduler::register(context);
        Ok(call)
    }

    fn submit(session: Resource<Self>, open: Resource<Http2Open>) {
        let open_id = open.id().as_u64();
        let previous = session.submit_head.swap(open_id, Ordering::AcqRel);
        if previous == NULL_ID {
            open.set_next_none();
            Self::ensure_connected_then_submit(session, SubmitCursor::new(open));
        } else {
            open.set_next_raw(previous);
            std::mem::forget(open);
        }
    }

    fn ensure_connected_then_submit(session: Resource<Self>, cursor: SubmitCursor) {
        loop {
            match session.state.load(Ordering::Acquire) {
                CONNECTED => {
                    let Some(h2) = session.current_h2() else {
                        let _ = session.state.compare_exchange(
                            CONNECTED,
                            DISCONNECTED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                        continue;
                    };
                    if h2.is_accepting_streams() {
                        Self::drain_connected(session, h2, cursor, INLINE_CALLS);
                        return;
                    }
                    session.invalidate_h2(&h2);
                }
                DISCONNECTED => {
                    if session
                        .state
                        .compare_exchange(
                            DISCONNECTED,
                            CONNECTING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        tokio::spawn(Self::connect_task(session, cursor));
                        return;
                    }
                }
                CONNECTING => {
                    debug_assert!(false, "HTTP/2 submit owner cannot race another connect owner");
                    return;
                }
                CLOSED => {
                    Self::fail_cursor_and_successors(
                        session,
                        cursor,
                        Error::Channel("HTTP/2 session closed".into()),
                    );
                    return;
                }
                _ => unreachable!(),
            }
        }
    }

    async fn connect_task(session: Resource<Self>, cursor: SubmitCursor) {
        let result = async {
            let connecting = TcpStream::connect(session.remote);
            let stream = match session.connect_timeout {
                Some(timeout) => tokio::time::timeout(timeout, connecting)
                    .await
                    .map_err(|_| Error::Channel("HTTP/2 connect timeout".into()))?,
                None => connecting.await,
            }
            .map_err(|error| Error::Channel(format!("HTTP/2 connect failed: {error}").into()))?;
            stream.set_nodelay(true).map_err(|error| {
                Error::Channel(format!("HTTP/2 TCP_NODELAY failed: {error}").into())
            })?;
            let socket = TcpSocket::resource_pool()
                .acquire_with_init(|| TcpSocket::new(TcpTransport::new(stream)))
                .map_err(|_| Error::ResourceExhausted)?;
            H2Connection::try_new(ConnectionRole::Client, socket).map_err(|error| {
                Error::Channel(format!("HTTP/2 initialization failed: {error:?}").into())
            })
        }
        .await;
        match result {
            Ok(h2) => {
                let raw = h2.clone().into_owned_id().as_u64();
                if session
                    .h2_id
                    .compare_exchange(NULL_ID, raw, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    drop(unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_id(raw)) });
                    Self::fail_cursor_and_successors(
                        session,
                        cursor,
                        Error::Channel("HTTP/2 session closed".into()),
                    );
                    return;
                }
                if session
                    .state
                    .compare_exchange(CONNECTING, CONNECTED, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    session.invalidate_h2(&h2);
                    Self::fail_cursor_and_successors(
                        session,
                        cursor,
                        Error::Channel("HTTP/2 session closed".into()),
                    );
                    return;
                }
                Self::drain_connected_continuation(session, h2, cursor).await;
            }
            Err(error) => {
                let _ = session.state.compare_exchange(
                    CONNECTING,
                    DISCONNECTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                Self::fail_cursor_and_successors(session, cursor, error);
            }
        }
    }

    fn current_h2(&self) -> Option<Resource<H2Connection>> {
        let raw = self.h2_id.load(Ordering::Acquire);
        if raw == NULL_ID { None } else { H2Connection::resource_pool().get(h2_id(raw)) }
    }

    fn invalidate_h2(&self, h2: &Resource<H2Connection>) {
        let raw = h2.id().as_u64();
        if self.h2_id.compare_exchange(raw, NULL_ID, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            drop(unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_id(raw)) });
            let _ = self.state.compare_exchange(
                CONNECTED,
                DISCONNECTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn drain_connected(
        session: Resource<Self>,
        h2: Resource<H2Connection>,
        cursor: SubmitCursor,
        quota: usize,
    ) {
        // The submitting thread consumes the common case locally. Allocate one
        // continuation only when the fairness quota leaves published work.
        if let Some(cursor) = Self::drain_connected_chunk(&session, &h2, cursor, quota) {
            tokio::spawn(Self::drain_connected_continuation(session, h2, cursor));
        }
    }

    async fn drain_connected_continuation(
        session: Resource<Self>,
        h2: Resource<H2Connection>,
        mut cursor: SubmitCursor,
    ) {
        loop {
            let Some(next) = Self::drain_connected_chunk(&session, &h2, cursor, ASYNC_CALLS) else {
                return;
            };
            cursor = next;
            // Keep the same task across quota slices; yielding is sufficient.
            tokio::task::yield_now().await;
        }
    }

    fn drain_connected_chunk(
        session: &Resource<Self>,
        h2: &Resource<H2Connection>,
        mut cursor: SubmitCursor,
        quota: usize,
    ) -> Option<SubmitCursor> {
        let mut published = 0;
        let mut calls = 0;
        loop {
            let current_id = cursor.open.id().as_u64();
            let next = cursor.open.take_next();
            let unavailable = if cursor.open.active.load(Ordering::Acquire) {
                let handoff = Http2Open::handoff(cursor.open);
                let result = match h2.publish_handoff(handoff) {
                    Err((OpenStreamError::Busy, handoff)) => {
                        h2.drive_published();
                        published = 0;
                        h2.publish_handoff(handoff)
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
                    Err((error, handoff)) => {
                        handoff.reject(error);
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

            let Some(next_cursor) = Self::advance_cursor(session, current_id, cursor.tail_id, next)
            else {
                if published != 0 {
                    h2.drive_published();
                }
                if unavailable {
                    session.invalidate_h2(h2);
                }
                return None;
            };
            cursor = next_cursor;
            if unavailable {
                if published != 0 {
                    h2.drive_published();
                }
                session.invalidate_h2(h2);
                Self::ensure_connected_then_submit(session.clone(), cursor);
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
        session: &Self,
        current_id: u64,
        tail_id: u64,
        next: Option<Resource<Http2Open>>,
    ) -> Option<SubmitCursor> {
        if let Some(open) = next {
            return Some(SubmitCursor { open, tail_id });
        }
        debug_assert_eq!(current_id, tail_id);
        match session.submit_head.compare_exchange(
            tail_id,
            NULL_ID,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => None,
            Err(actual) => Some(SubmitCursor {
                open: reverse_published_stack_until(actual, tail_id),
                tail_id: actual,
            }),
        }
    }

    fn fail_cursor_and_successors(session: Resource<Self>, mut cursor: SubmitCursor, error: Error) {
        let message = error.to_string();
        loop {
            let current_id = cursor.open.id().as_u64();
            let next = cursor.open.take_next();
            cursor.open.fail(Error::Channel(message.clone().into()));
            let Some(next_cursor) =
                Self::advance_cursor(&session, current_id, cursor.tail_id, next)
            else {
                return;
            };
            cursor = next_cursor;
        }
    }
}

impl Drop for Http2ClientSession {
    fn drop(&mut self) {
        self.state.store(CLOSED, Ordering::Release);
        let raw = self.h2_id.swap(NULL_ID, Ordering::AcqRel);
        if raw != NULL_ID {
            // SAFETY: h2_id owns exactly one pool reference.
            drop(unsafe { H2Connection::resource_pool().reclaim_owned_id(h2_id(raw)) });
        }
    }
}

impl Http2Call {
    pub(crate) fn cancel(&self) {
        let _ = self.context.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::Canceled, "HTTP/2 request canceled")),
        );
    }

    fn fail(&mut self, error: Error) -> Poll<crate::Result<Response<Incoming>>> {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
        finish_context(&self.context, error_status(&error));
        self.finished = true;
        Poll::Ready(Err(error))
    }
}

impl Http2Open {
    fn take_next(&self) -> Option<Resource<Self>> {
        let raw = self.next.swap(NULL_ID, Ordering::AcqRel);
        if raw == NULL_ID || raw == UNCONNECTED_ID {
            return None;
        }
        Some(unsafe { Self::resource_pool().reclaim_owned_id(open_id(raw)) })
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

    fn handoff(open: Resource<Self>) -> H2StreamHandoff {
        let token = open.into_owned_id().as_u64();
        H2StreamHandoff::new(
            token,
            Self::handoff_active,
            Self::handoff_opened,
            Self::handoff_rejected,
            Self::handoff_release,
        )
    }

    fn handoff_active(token: u64) -> bool {
        Self::get(token).is_some_and(|open| open.active.load(Ordering::Acquire))
    }

    fn handoff_opened(token: u64, stream: H2Stream) {
        if let Some(open) = Self::get(token) {
            open.opened(stream);
        }
    }

    fn handoff_rejected(token: u64, error: OpenStreamError) {
        if let Some(open) = Self::get(token) {
            open.fail(Error::Channel(format!("failed to open HTTP/2 stream: {error:?}").into()));
        }
    }

    fn handoff_release(token: u64) {
        // SAFETY: the handoff owns exactly one reference transferred by
        // `handoff` and releases it exactly once.
        drop(unsafe { Self::resource_pool().reclaim_owned_id(open_id(token)) });
    }

    fn get(token: u64) -> Option<Resource<Self>> {
        Self::resource_pool().get(open_id(token))
    }

    fn opened(&self, stream: H2Stream) {
        if !self.active.load(Ordering::Acquire) {
            drop(stream);
            return;
        }
        let request = self.inner.lock().unwrap().request.take();
        let Some((parts, body)) = request else {
            drop(stream);
            return;
        };
        let (mut send, recv) = stream.split();
        let cancel = send.cancel_handle();
        *self.cancel.lock().unwrap() = Some(cancel);
        // Cancellation may win after the initial active check but before the
        // stream handle is published. Recheck after installing the handle so
        // the completion callback cannot miss the reset.
        if !self.active.load(Ordering::Acquire) {
            if let Some(cancel) = self.cancel.lock().unwrap().take() {
                cancel.cancel();
            }
            return;
        }
        let result =
            encode_request_headers(&parts, self.inner.lock().unwrap().remote).and_then(|headers| {
                match body {
                    RequestBody::Buffered(body) => {
                        let payload = body.unwrap_or_default();
                        if payload.is_empty() {
                            send.send_headers(headers, true).map_err(|_| {
                                Error::Channel("failed to publish empty HTTP/2 request".into())
                            })
                        } else {
                            send.send_complete_one(headers, payload).map_err(|error| {
                                Error::Channel(
                                    format!("failed to publish HTTP/2 request: {error:?}").into(),
                                )
                            })
                        }
                    }
                    RequestBody::Streaming(body) => {
                        let task_cancel = send.cancel_handle();
                        tokio::spawn(async move {
                            if drive_streaming_request(send, headers, body).await.is_err() {
                                task_cancel.cancel();
                            }
                        });
                        Ok(())
                    }
                }
            });
        match result {
            Ok(()) => {
                self.complete(Ok(Http2Opened { recv, cancel, context: self.context.clone() }))
            }
            Err(error) => {
                cancel.cancel();
                self.fail(error);
            }
        }
    }

    fn complete(&self, result: crate::Result<Http2Opened>) {
        let waker = {
            let mut inner = self.inner.lock().unwrap();
            if inner.result.is_some() {
                return;
            }
            inner.result = Some(result);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        if !self.attached.load(Ordering::Acquire) {
            self.detach_result();
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<crate::Result<Http2Opened>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(result) = inner.result.take() {
            return Poll::Ready(result);
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn context_completed(token: u64) {
        let Some(open) = Self::get(token) else { return };
        open.active.store(false, Ordering::Release);
        let status = open
            .context
            .completed_status()
            .unwrap_or_else(|| Status::new(StatusCode::Canceled, "HTTP/2 request canceled"));
        let waker = {
            let mut inner = open.inner.lock().unwrap();
            if let Some(cancel) = open.cancel.lock().unwrap().take() {
                cancel.cancel();
            }
            inner.result.take();
            inner.result = Some(Err(Error::Remote(status)));
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn fail(&self, error: Error) {
        finish_context(&self.context, error_status(&error));
        self.complete(Err(error));
    }

    fn detach(&self) {
        self.attached.store(false, Ordering::Release);
        self.detach_result();
    }

    fn detach_result(&self) {
        let opened = {
            let mut inner = self.inner.lock().unwrap();
            match inner.result.take() {
                Some(Ok(opened)) => Some(opened),
                Some(Err(_)) | None => None,
            }
        };
        if let Some(opened) = opened {
            spawn_detached_response(opened.recv, opened.cancel, opened.context);
        }
    }
}

impl SubmitCursor {
    fn new(open: Resource<Http2Open>) -> Self {
        let tail_id = open.id().as_u64();
        Self { open, tail_id }
    }
}

impl std::future::Future for Http2Call {
    type Output = crate::Result<Response<Incoming>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finished {
            panic!("HTTP/2 call polled after completion");
        }
        if self.recv.is_none() {
            let opened = match self.open.as_ref().unwrap().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return self.fail(error),
                Poll::Ready(Ok(opened)) => opened,
            };
            self.cancel = Some(opened.cancel);
            self.recv = Some(opened.recv);
        }
        let recv = self.recv.as_mut().unwrap();
        match Pin::new(&mut recv.recv()).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => self.fail(recv_error(error)),
            Poll::Ready(Ok(None)) => {
                self.fail(Error::Protocol("HTTP/2 response ended before headers".into()))
            }
            Poll::Ready(Ok(Some(ReceivedEvent::Data(_)))) => {
                self.fail(Error::Protocol("HTTP/2 response DATA preceded headers".into()))
            }
            Poll::Ready(Ok(Some(ReceivedEvent::Headers { headers, end_stream, trailers }))) => {
                if trailers {
                    return self
                        .fail(Error::Protocol("HTTP/2 response trailers preceded headers".into()));
                }
                let response = match decode_response_headers(headers) {
                    Ok((status, headers, hint)) => {
                        let recv = self.recv.take().unwrap();
                        let body = Incoming::http2(recv, end_stream, hint);
                        let mut response = Response::new(body);
                        *response.status_mut() = status;
                        *response.version_mut() = http::Version::HTTP_2;
                        *response.headers_mut() = headers;
                        response
                    }
                    Err(error) => return self.fail(error),
                };
                self.cancel.take();
                finish_context(&self.context, None);
                self.finished = true;
                Poll::Ready(Ok(response))
            }
        }
    }
}

impl Drop for Http2Call {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(open) = self.open.take() {
            open.detach();
        }
        if let Some(recv) = self.recv.take()
            && let Some(cancel) = self.cancel.take()
        {
            spawn_detached_response(recv, cancel, self.context.clone());
        }
    }
}

fn spawn_detached_response(
    mut recv: H2RecvStream,
    cancel: H2StreamCancel,
    context: Resource<CallContext>,
) {
    tokio::spawn(async move {
        let token = CallToken::new(context.clone());
        let mut headers_seen = false;
        loop {
            let event = if headers_seen {
                recv.recv().await
            } else {
                tokio::select! {
                    event = recv.recv() => event,
                    _ = std::future::poll_fn(|cx| token.poll(cx)) => {
                        cancel.cancel();
                        break;
                    }
                }
            };
            match event {
                Ok(Some(ReceivedEvent::Headers { trailers: false, .. })) => {
                    finish_context(&context, None);
                    headers_seen = true;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    finish_context(&context, None);
                    break;
                }
                Err(error) => {
                    finish_context(&context, Some(recv_error(error).to_string()));
                    break;
                }
            }
        }
    });
}

fn finish_context(context: &CallContext, error: Option<String>) {
    context.clear_completion_notifier();
    let status = error.map(|message| Status::new(StatusCode::Unavailable, message));
    let _ = context.complete(IoBuf::new(), IoBuf::new(), status);
}

fn error_status(error: &Error) -> Option<String> {
    Some(error.to_string())
}

fn encode_request_headers(
    parts: &http::request::Parts,
    remote: SocketAddr,
) -> crate::Result<HeaderBlock> {
    let mut block = HeaderBlock::new();
    block.pseudo.method = Some(Bytes::copy_from_slice(parts.method.as_str().as_bytes()));
    block.pseudo.scheme = Some(Bytes::from_static(b"http"));
    block.pseudo.path = Some(Bytes::copy_from_slice(
        parts.uri.path_and_query().map_or("/", |value| value.as_str()).as_bytes(),
    ));
    let authority = parts
        .uri
        .authority()
        .map(|value| value.as_str().to_owned())
        .or_else(|| {
            parts
                .headers
                .get(http::header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| remote.to_string());
    block.pseudo.authority = Some(Bytes::from(authority));
    append_headers(&mut block, &parts.headers)?;
    Ok(block)
}

fn encode_trailers(headers: HeaderMap) -> crate::Result<HeaderBlock> {
    let mut block = HeaderBlock::new();
    append_headers(&mut block, &headers)?;
    Ok(block)
}

fn append_headers(block: &mut HeaderBlock, headers: &HeaderMap) -> crate::Result<()> {
    for (name, value) in headers {
        if matches!(
            name.as_str(),
            "connection"
                | "proxy-connection"
                | "keep-alive"
                | "transfer-encoding"
                | "upgrade"
                | "host"
        ) {
            continue;
        }
        block.push(HeaderField {
            name: Bytes::copy_from_slice(name.as_str().as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            indexing: if matches!(name.as_str(), "authorization" | "cookie") {
                HeaderIndexing::NeverIndexed
            } else {
                HeaderIndexing::WithoutIndexing
            },
        });
    }
    Ok(())
}

fn decode_response_headers(
    block: HeaderBlock,
) -> crate::Result<(http::StatusCode, HeaderMap, SizeHint)> {
    let status = block
        .pseudo
        .status
        .and_then(|status| http::StatusCode::from_u16(status).ok())
        .ok_or_else(|| Error::Protocol("HTTP/2 response is missing :status".into()))?;
    let mut headers = HeaderMap::new();
    for field in block.fields() {
        let name = HeaderName::from_bytes(field.name.as_ref())
            .map_err(|_| Error::Protocol("invalid HTTP/2 response header name".into()))?;
        let value = HeaderValue::from_bytes(field.value.as_ref())
            .map_err(|_| Error::Protocol("invalid HTTP/2 response header value".into()))?;
        headers.append(name, value);
    }
    let hint = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .map_or_else(SizeHint::unknown, SizeHint::exact);
    Ok((status, headers, hint))
}

fn recv_error(error: RecvStreamError) -> Error {
    Error::Channel(format!("HTTP/2 response stream failed: {error:?}").into())
}

async fn drive_streaming_request(
    mut send: H2SendStream,
    headers: HeaderBlock,
    mut body: Pin<Box<dyn ErasedBody + Send>>,
) -> crate::Result<()> {
    let mut headers = Some(headers);
    std::future::poll_fn(|cx| send.poll_send_headers(cx, &mut headers, false))
        .await
        .map_err(|_| Error::Channel("failed to send HTTP/2 request headers".into()))?;
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        match frame.map_err(|error| Error::Encode(error.to_string().into()))? {
            Frame::Data(data) => {
                let mut data = Some(data);
                std::future::poll_fn(|cx| send.poll_send_data(cx, &mut data, false))
                    .await
                    .map_err(|_| Error::Channel("failed to send HTTP/2 request body".into()))?;
            }
            Frame::Trailers(trailers) => {
                let mut trailers = Some(encode_trailers(trailers)?);
                std::future::poll_fn(|cx| send.poll_send_headers(cx, &mut trailers, true))
                    .await
                    .map_err(|_| Error::Channel("failed to send HTTP/2 request trailers".into()))?;
                return Ok(());
            }
        }
    }
    let mut end = Some(IoBuf::new());
    std::future::poll_fn(|cx| send.poll_send_data(cx, &mut end, true))
        .await
        .map_err(|_| Error::Channel("failed to finish HTTP/2 request body".into()))
}

fn h2_id(raw: u64) -> ResourceId<H2Connection> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn open_id(raw: u64) -> ResourceId<Http2Open> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

fn reverse_published_stack_until(raw_head: u64, stop_id: u64) -> Resource<Http2Open> {
    debug_assert_ne!(raw_head, NULL_ID);
    debug_assert_ne!(raw_head, UNCONNECTED_ID);
    let mut current = unsafe { Http2Open::resource_pool().reclaim_owned_id(open_id(raw_head)) };
    let mut reversed: Option<Resource<Http2Open>> = None;
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
            return reversed.expect("published stack must contain at least one HTTP/2 call");
        }
        current = unsafe { Http2Open::resource_pool().reclaim_owned_id(open_id(next_raw)) };
    }
}
