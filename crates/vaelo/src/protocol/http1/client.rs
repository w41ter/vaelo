// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use http::Request;

use crate::protocol::http_call::{HttpCall, HttpCompletion};
use crate::protocol::http1::BodyDecodeEvent;
use crate::transport::inflight::{CallContext, CallToken};
use crate::transport::tcp::{OnConnected, TcpConnection, TcpSocket, WriteOptions};
use crate::{Error, IoBuf, Resource, StaticResource, Status, StatusCode};

#[derive(vaelo_derive::StaticResource)]
pub(crate) struct Http1ClientSession {
    connection: Resource<TcpConnection>,
    shared: Arc<Http1ClientShared>,
    request_owner: AtomicBool,
    queued: Mutex<VecDeque<OutboundRequest>>,
}

struct Http1ClientShared {
    protocol: &'static super::Http1Protocol,
    pending: Mutex<Http1Pending>,
    close_after_response: bool,
}

/// FIFO response association for one HTTP/1 connection.
///
/// HTTP/1 responses do not carry a stream id or correlation id. The only valid
/// association rule is request order. Dropping an `HttpCall` does not cancel
/// its request, so every submitted request remains queued until its wire
/// response arrives or the connection fails.
#[derive(Default)]
struct Http1Pending {
    queue: VecDeque<Http1PendingCall>,
}

struct Http1PendingCall {
    context: Resource<CallContext>,
    completion: Arc<HttpCompletion>,
}

enum OutboundRequest {
    Full {
        request: Request<IoBuf>,
        context: Resource<CallContext>,
        completion: Arc<HttpCompletion>,
    },
    Streaming {
        parts: http::request::Parts,
        body: Pin<Box<dyn crate::http::ErasedBody + Send>>,
        context: Resource<CallContext>,
        completion: Arc<HttpCompletion>,
    },
}

impl Http1ClientSession {
    pub(crate) fn create(
        remote_addr: SocketAddr,
        connect_timeout: Option<std::time::Duration>,
        close_after_response: bool,
    ) -> crate::Result<Resource<Self>> {
        let shared = Arc::new(Http1ClientShared {
            protocol: &super::HTTP1_PROTOCOL,
            pending: Mutex::new(Http1Pending::default()),
            close_after_response,
        });
        let connection = TcpConnection::create(
            remote_addr,
            connect_timeout,
            Some(Box::new(Http1OnConnected { shared: shared.clone() })),
        )?;
        Self::resource_pool()
            .acquire_with_init(|| Self {
                connection,
                shared,
                request_owner: AtomicBool::new(false),
                queued: Mutex::new(VecDeque::new()),
            })
            .map_err(|_| Error::ResourceExhausted)
    }

    /// Issues one HTTP/1 request.
    ///
    /// The submission path:
    /// 1. encode exactly one HTTP/1 request frame;
    /// 2. allocate a `CallContext` as the completion target;
    /// 3. push a pending FIFO entry before submitting the frame;
    /// 4. submit `(CallContext, frame)` to `TcpConnection`;
    /// 5. return an `HttpCall` backed by that context.
    ///
    /// The session owns response association. `TcpConnection` can guarantee
    /// write durability/failure cleanup, but it cannot map HTTP/1 responses to
    /// calls because the wire protocol has no correlation id.
    pub(crate) fn issue_request(
        self: Resource<Self>,
        request: Request<IoBuf>,
        deadline: Option<Instant>,
    ) -> HttpCall {
        let context = match CallContext::resource_pool()
            .acquire_with_init(|| CallContext::new(deadline, false))
        {
            Ok(context) => context,
            Err(_) => return HttpCall::ready(Err(Error::ResourceExhausted)),
        };
        let token = CallToken::new(context.clone());
        let completion = HttpCompletion::new();
        self.submit(OutboundRequest::Full { request, context, completion: completion.clone() });
        HttpCall::pending(token, completion)
    }

    pub(crate) fn issue_streaming_request(
        self: Resource<Self>,
        parts: http::request::Parts,
        body: Pin<Box<dyn crate::http::ErasedBody + Send>>,
        deadline: Option<Instant>,
    ) -> HttpCall {
        let context = match CallContext::resource_pool()
            .acquire_with_init(|| CallContext::new(deadline, false))
        {
            Ok(context) => context,
            Err(_) => return HttpCall::ready(Err(Error::ResourceExhausted)),
        };
        let token = CallToken::new(context.clone());
        let completion = HttpCompletion::new();
        self.submit(OutboundRequest::Streaming {
            parts,
            body,
            context,
            completion: completion.clone(),
        });
        HttpCall::pending(token, completion)
    }

    fn submit(self: Resource<Self>, request: OutboundRequest) {
        if self
            .request_owner
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.drive_request_owner(request);
        } else {
            self.queued.lock().unwrap().push_back(request);
            if !self.request_owner.load(Ordering::Acquire)
                && self
                    .request_owner
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                let next = self.queued.lock().unwrap().pop_front();
                if let Some(request) = next {
                    self.drive_request_owner(request);
                } else {
                    self.request_owner.store(false, Ordering::Release);
                }
            }
        }
    }

    fn drive_request_owner(self: Resource<Self>, mut request: OutboundRequest) {
        loop {
            match request {
                OutboundRequest::Full { request, context, completion } => {
                    self.submit_full(request, context, completion);
                }
                OutboundRequest::Streaming { .. } => {
                    tokio::spawn(async move {
                        self.drive_streaming_owner(request).await;
                    });
                    return;
                }
            }
            let Some(next) = self.take_next_request() else { return };
            request = next;
        }
    }

    fn take_next_request(&self) -> Option<OutboundRequest> {
        if let Some(request) = self.queued.lock().unwrap().pop_front() {
            return Some(request);
        }
        self.request_owner.store(false, Ordering::Release);
        let has_request = !self.queued.lock().unwrap().is_empty();
        if has_request
            && self
                .request_owner
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.queued.lock().unwrap().pop_front()
        } else {
            None
        }
    }

    fn submit_full(
        &self,
        request: Request<IoBuf>,
        context: Resource<CallContext>,
        completion: Arc<HttpCompletion>,
    ) {
        if !context.is_active()
            || context.deadline().is_some_and(|deadline| deadline <= Instant::now())
        {
            if context.is_active() {
                let _ = context.complete_deadline_exceeded();
            }
            return;
        }
        let frame = match self.shared.protocol.encode_client_request(request) {
            Ok(frame) => frame,
            Err(error) => {
                completion.complete(Err(error));
                let _ = context.complete(
                    IoBuf::new(),
                    IoBuf::new(),
                    Some(Status::new(StatusCode::Internal, "HTTP request encode failed")),
                );
                return;
            }
        };
        let mut pending = self.shared.pending.lock().unwrap();
        pending.queue.push_back(Http1PendingCall {
            context: context.clone(),
            completion: completion.clone(),
        });
        if let Err(error) = TcpConnection::submit_write(
            self.connection.clone(),
            frame,
            WriteOptions::client(context),
        ) {
            pending.queue.pop_back();
            completion.complete(Err(error));
        }
    }

    async fn drive_streaming_owner(self: Resource<Self>, mut request: OutboundRequest) {
        loop {
            let OutboundRequest::Streaming { parts, body, context, completion } = request else {
                self.drive_request_owner(request);
                return;
            };
            self.emit_streaming(parts, body, context, completion).await;
            let Some(next) = self.take_next_request() else { return };
            request = next;
        }
    }

    async fn emit_streaming(
        &self,
        parts: http::request::Parts,
        mut body: Pin<Box<dyn crate::http::ErasedBody + Send>>,
        context: Resource<CallContext>,
        completion: Arc<HttpCompletion>,
    ) {
        if !context.is_active() {
            return;
        }
        if context.deadline().is_some_and(|deadline| deadline <= Instant::now()) {
            let _ = context.complete_deadline_exceeded();
            return;
        }
        let deadline = context.deadline();
        let hint = body.as_ref().get_ref().size_hint();
        let exact = (hint.upper() == Some(hint.lower())).then_some(hint.lower());
        let head = match self.shared.protocol.encode_client_request_head(parts, exact) {
            Ok(head) => head,
            Err(error) => {
                completion.complete(Err(error));
                let _ = context.complete(
                    IoBuf::new(),
                    IoBuf::new(),
                    Some(Status::new(StatusCode::Internal, "HTTP request encode failed")),
                );
                return;
            }
        };
        {
            let mut pending = self.shared.pending.lock().unwrap();
            pending.queue.push_back(Http1PendingCall {
                context: context.clone(),
                completion: completion.clone(),
            });
        }

        if exact == Some(0) {
            if let Err(error) = TcpConnection::submit_write(
                self.connection.clone(),
                head,
                WriteOptions::client(context.clone()),
            ) {
                self.fail_streaming(context, completion, Status::from(error), false);
            }
            return;
        }
        if let Err(status) = TcpConnection::write_frame(self.connection.clone(), head, false).await
        {
            self.fail_streaming(context, completion, status, false);
            return;
        }

        let mut remaining = exact;
        let mut delayed = None;
        let mut trailers = None;
        loop {
            let next = if let Some(deadline) = deadline {
                tokio::select! {
                    frame = poll_fn(|cx| body.as_mut().poll_frame(cx)) => frame,
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        self.fail_streaming(
                            context,
                            completion,
                            Status::new(StatusCode::DeadlineExceeded, "deadline exceeded"),
                            true,
                        );
                        return;
                    }
                }
            } else {
                poll_fn(|cx| body.as_mut().poll_frame(cx)).await
            };
            let Some(frame) = next else { break };
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    self.fail_streaming(
                        context,
                        completion,
                        Status::new(StatusCode::Internal, error.to_string()),
                        true,
                    );
                    return;
                }
            };
            match frame {
                crate::http::Frame::Data(data) if data.is_empty() => {}
                crate::http::Frame::Data(data) => {
                    if let Some(left) = &mut remaining {
                        let length = data.len() as u64;
                        if length > *left {
                            self.fail_streaming(
                                context,
                                completion,
                                Status::new(
                                    StatusCode::Internal,
                                    "HTTP request body exceeds Content-Length",
                                ),
                                true,
                            );
                            return;
                        }
                        *left -= length;
                        if let Some(previous) = delayed.replace(data)
                            && let Err(status) =
                                TcpConnection::write_frame(self.connection.clone(), previous, false)
                                    .await
                        {
                            self.fail_streaming(context, completion, status, false);
                            return;
                        }
                    } else {
                        let frame = self.shared.protocol.encode_chunk(data);
                        if let Err(status) =
                            TcpConnection::write_frame(self.connection.clone(), frame, false).await
                        {
                            self.fail_streaming(context, completion, status, false);
                            return;
                        }
                    }
                }
                crate::http::Frame::Trailers(value) => {
                    if exact.is_some() {
                        self.fail_streaming(
                            context,
                            completion,
                            Status::new(
                                StatusCode::Internal,
                                "HTTP request trailers require chunked transfer encoding",
                            ),
                            true,
                        );
                        return;
                    }
                    trailers = Some(value);
                }
            }
        }

        let final_frame = if let Some(remaining) = remaining {
            if remaining != 0 {
                self.fail_streaming(
                    context,
                    completion,
                    Status::new(
                        StatusCode::Internal,
                        "HTTP request body ended before Content-Length",
                    ),
                    true,
                );
                return;
            }
            delayed.expect("non-empty fixed body must retain its final frame")
        } else {
            self.shared.protocol.encode_last_chunk(trailers)
        };
        if let Err(error) = TcpConnection::submit_write(
            self.connection.clone(),
            final_frame,
            WriteOptions::client(context.clone()),
        ) {
            self.fail_streaming(context, completion, Status::from(error), false);
        }
    }

    fn fail_streaming(
        &self,
        context: Resource<CallContext>,
        completion: Arc<HttpCompletion>,
        status: Status,
        close: bool,
    ) {
        {
            let mut pending = self.shared.pending.lock().unwrap();
            if pending.queue.back().is_some_and(|pending| pending.context.id() == context.id()) {
                pending.queue.pop_back();
            }
        }
        completion.complete(Err(Error::Channel(status.to_string().into())));
        let _ = context.complete(IoBuf::new(), IoBuf::new(), Some(status));
        if close {
            self.connection.close();
        }
    }

    pub(crate) fn issue_short_request(
        remote_addr: SocketAddr,
        request: Request<IoBuf>,
        deadline: Option<Instant>,
    ) -> HttpCall {
        match Self::create(remote_addr, None, true) {
            Ok(session) => session.issue_request(request, deadline),
            Err(error) => HttpCall::ready(Err(error)),
        }
    }
}

struct Http1OnConnected {
    shared: Arc<Http1ClientShared>,
}

impl OnConnected for Http1OnConnected {
    fn on_connected(&self, connection: Resource<TcpConnection>, socket: Resource<TcpSocket>) {
        let shared = self.shared.clone();
        tokio::spawn(async move { read_loop(connection, shared, socket).await });
    }
}

/// Reads HTTP/1 responses for one socket and completes FIFO pending calls.
///
/// For every complete decoded response, the reader must pop exactly one queue
/// entry. If that entry is canceled, the response is discarded and the queue
/// still advances. Protocol errors and transport EOF invalidate the socket and
/// let `TcpConnection` fail calls it owns for the current connection cycle.
async fn read_loop(
    connection: Resource<TcpConnection>,
    shared: Arc<Http1ClientShared>,
    socket: Resource<TcpSocket>,
) {
    const INCOMING_FRAMES: usize = 16;
    let mut input = IoBuf::new();
    let failure = loop {
        match shared
            .protocol
            .decode_client_response_head(&mut input, &super::HttpClientResponseOptions::default())
        {
            Ok(Some(mut decoded)) => {
                let close_delimited = decoded.body.is_close_delimited();
                let pending = {
                    let mut queue = shared.pending.lock().unwrap();
                    queue.queue.pop_front()
                };
                let Some(pending) = pending else {
                    log::warn!(
                        "unexpected HTTP/1 response without pending request: remote={}",
                        connection.remote_addr()
                    );
                    break Status::new(
                        StatusCode::Internal,
                        "HTTP/1 response has no pending request",
                    );
                };
                let sender = if pending.context.is_active() {
                    let hint = decoded.body.size_hint();
                    let (sender, incoming) = crate::http::Incoming::channel(INCOMING_FRAMES, hint);
                    pending.completion.complete(Ok(decoded.response.map(|()| incoming)));
                    let _ = pending.context.complete(IoBuf::new(), IoBuf::new(), None);
                    Some(sender)
                } else {
                    None
                };

                let body_failure = loop {
                    match decoded.body.decode(&mut input) {
                        Ok(Some(BodyDecodeEvent::Frame(frame))) => {
                            if let Some(sender) = &sender
                                && sender.send(Ok(frame)).await.is_err()
                            {
                                break Some(Status::new(
                                    StatusCode::Canceled,
                                    "HTTP/1 response body was abandoned",
                                ));
                            }
                        }
                        Ok(Some(BodyDecodeEvent::End)) => break None,
                        Ok(None) => match socket.read_more(&mut input).await {
                            Ok(0) => {
                                if close_delimited {
                                    break None;
                                }
                                break Some(Status::new(
                                    StatusCode::Unavailable,
                                    "connection closed during HTTP/1 response body",
                                ));
                            }
                            Ok(_) => {}
                            Err(error) => {
                                break Some(Status::new(
                                    StatusCode::Unavailable,
                                    error.to_string(),
                                ));
                            }
                        },
                        Err(error) => {
                            break Some(Status::new(
                                StatusCode::Unavailable,
                                format!("HTTP/1 response body error: {}", error.status()),
                            ));
                        }
                    }
                };
                if let Some(failure) = body_failure {
                    if let Some(sender) = sender {
                        let _ = sender
                            .send(Err(crate::http::Error::Transport(failure.to_string())))
                            .await;
                    }
                    break failure;
                }
                if close_delimited {
                    break Status::new(
                        StatusCode::Unavailable,
                        "HTTP/1 close-delimited response completed",
                    );
                }
                if shared.close_after_response {
                    break Status::new(
                        StatusCode::Unavailable,
                        "HTTP/1 short connection completed",
                    );
                }
            }
            Ok(None) => match socket.read_more(&mut input).await {
                Ok(0) => {
                    log::debug!(
                        "http1 peer closed connection: remote={}",
                        connection.remote_addr()
                    );
                    break Status::new(StatusCode::Unavailable, "connection closed by peer");
                }
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        "http1 response read failed: remote={}, socket={}, error={error}",
                        connection.remote_addr(),
                        socket.id().as_u64()
                    );
                    break Status::new(StatusCode::Unavailable, error.to_string());
                }
            },
            Err(error) => {
                log::warn!(
                    "HTTP/1 response decode failed: remote={}, status={}",
                    connection.remote_addr(),
                    error.status()
                );
                break Status::new(
                    StatusCode::Unavailable,
                    format!("HTTP/1 response error: {}", error.status()),
                );
            }
        }
    };
    let pending = {
        let mut queue = shared.pending.lock().unwrap();
        let pending = std::mem::take(&mut queue.queue);
        // Keep connection generation rollover atomic with HTTP/1 FIFO
        // publication. A submitter holds this same lock while it appends and
        // submits its frame, so it either belongs to this failed socket or to
        // the next one, never both.
        connection.invalidate(socket, failure.clone());
        pending
    };
    for call in pending {
        call.completion.complete(Err(Error::Channel(failure.to_string().into())));
        let _ = call.context.complete(IoBuf::new(), IoBuf::new(), Some(failure.clone()));
    }
}
