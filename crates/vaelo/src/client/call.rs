// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::{Channel, ConnectionTag, Request, Response};
use crate::protocol::grpc::client::GrpcClientRecvStream;
use crate::runtime::profile::{self, ProfileStage};
use crate::runtime::rpc_variables;
use crate::runtime::rpc_variables::RpcCallGuard;
use crate::transport::inflight::CallToken;
use crate::{Decode, Encode, Error, MethodDescriptor, MethodKind, Protocol, Status, StatusCode};

// Keeping the active transport inline avoids one allocation on every RPC.
#[allow(clippy::large_enum_variant)]
enum RpcCallState {
    Active(CallToken),
    StreamingUnary {
        recv: GrpcClientRecvStream,
        metadata: Option<crate::MetadataMap>,
        payload: Option<crate::IoBuf>,
    },
    Ready(Option<Status>),
    Finished,
}

/// Invocation policy shared by all four call builders.
///
/// Keeping this as data on the unstarted builder makes configuring a call
/// allocation-free. The relative timeout is deliberately converted to an
/// absolute deadline only when `start` is called.
pub(crate) struct CallOptions {
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    connection_tag: Option<ConnectionTag>,
}

impl CallOptions {
    pub(crate) const fn new() -> Self {
        Self { timeout: None, deadline: None, connection_tag: None }
    }

    pub(crate) fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
        self.deadline = None;
    }

    pub(crate) fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
        self.timeout = None;
    }

    pub(crate) fn set_connection_tag(&mut self, tag: ConnectionTag) {
        self.connection_tag = Some(tag);
    }

    pub(crate) fn apply<T>(self, request: &mut Request<T>) -> Option<ConnectionTag> {
        request.options.deadline =
            self.deadline.or_else(|| self.timeout.map(|timeout| Instant::now() + timeout));
        self.connection_tag
    }
}

/// An eagerly submitted unary RPC handle.
///
/// Creating this value serializes and submits the request. Awaiting it only
/// waits for and decodes the response. Dropping or detaching the handle does
/// not retract a request which has already been submitted.
#[must_use = "await the response, call detach(), or explicitly ignore the RPC handle"]
pub struct CallHandle<Response> {
    state: RpcCallState,
    variables: Option<RpcCallGuard>,
    marker: std::marker::PhantomData<fn() -> Response>,
}

/// An owned unary call that has not been submitted yet.
///
/// This value is a small call description. Constructing or dropping it does
/// not encode the message, acquire a connection, spawn a task, or submit bytes.
/// Calling [`start`](Self::start) performs all immediately available local
/// work and hands the request to the RPC runtime. Any local start failure is
/// retained by the returned [`CallHandle`] and observed through `await`.
#[must_use = "await the call, call start(), or explicitly drop it without sending"]
pub struct UnaryCall<RequestMessage, ResponseMessage> {
    channel: Channel,
    method: &'static MethodDescriptor,
    request: Request<RequestMessage>,
    options: CallOptions,
    marker: std::marker::PhantomData<fn() -> ResponseMessage>,
}

impl<RequestMessage, ResponseMessage> UnaryCall<RequestMessage, ResponseMessage> {
    pub(crate) fn new(
        channel: Channel,
        method: &'static MethodDescriptor,
        request: Request<RequestMessage>,
    ) -> Self {
        Self {
            channel,
            method,
            request,
            options: CallOptions::new(),
            marker: std::marker::PhantomData,
        }
    }

    /// Sets a relative timeout whose clock starts when the call is started.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options.set_timeout(timeout);
        self
    }

    /// Sets an absolute deadline. This replaces a previously configured
    /// relative timeout.
    pub fn deadline(mut self, deadline: Instant) -> Self {
        self.options.set_deadline(deadline);
        self
    }

    /// Overrides the channel's default connection tag for this call only.
    pub fn connection_tag(mut self, tag: ConnectionTag) -> Self {
        self.options.set_connection_tag(tag);
        self
    }
}

impl<RequestMessage, ResponseMessage> UnaryCall<RequestMessage, ResponseMessage>
where
    RequestMessage: Encode,
    ResponseMessage: Decode + Default,
{
    /// Immediately submits the call and returns its completion handle.
    ///
    /// This method never exposes socket readiness or queue admission to the
    /// application. If progress cannot be made synchronously, the RPC runtime
    /// owns the pending work and wakes the handle later. Submission, transport
    /// and remote failures are all reported by awaiting the returned handle.
    pub fn start(mut self) -> CallHandle<ResponseMessage> {
        let connection_tag = self.options.apply(&mut self.request);
        start_unary_call(&self.channel, self.method, self.request, connection_tag)
    }
}

impl<RequestMessage, ResponseMessage> std::future::IntoFuture
    for UnaryCall<RequestMessage, ResponseMessage>
where
    RequestMessage: Encode,
    ResponseMessage: Decode + Default,
{
    type Output = Result<Response<ResponseMessage>, Error>;
    type IntoFuture = CallHandle<ResponseMessage>;

    fn into_future(self) -> Self::IntoFuture {
        self.start()
    }
}

impl<Response> Unpin for CallHandle<Response> {}

impl<Response> CallHandle<Response> {
    pub(crate) fn from_streaming(
        method: &'static MethodDescriptor,
        recv: GrpcClientRecvStream,
    ) -> Self {
        Self {
            state: RpcCallState::StreamingUnary { recv, metadata: None, payload: None },
            variables: Some(rpc_variables::client_method(method).start_call()),
            marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn ready_error(method: &'static MethodDescriptor, status: Status) -> Self {
        Self {
            state: RpcCallState::Ready(Some(status)),
            variables: Some(rpc_variables::client_method(method).start_call()),
            marker: std::marker::PhantomData,
        }
    }
}

impl<Response> CallHandle<Response>
where
    Response: Decode + Default + Send + 'static,
{
    /// Discards the response value while allowing completion accounting to run.
    /// The submitted RPC continues independently under ownership of the RPC
    /// runtime. Detaching does not change the call's progress model and must
    /// not require the caller to keep polling a future.
    pub fn detach(mut self) {
        match std::mem::replace(&mut self.state, RpcCallState::Finished) {
            RpcCallState::Active(token) => {
                // The protocol owns its own CallContext reference and drives
                // the unary exchange to completion. Transfer the metrics guard
                // to that context instead of spawning a response-waiting task.
                if let Some(variables) = self.variables.take() {
                    token.detach(variables);
                }
                return;
            }
            RpcCallState::Ready(status) => {
                let status = status.expect("detached ready RPC must retain its status");
                if let Some(variables) = self.variables.take() {
                    variables.finish(Some(status.code()));
                }
                log::warn!("detached RPC failed before submission: {status}");
                return;
            }
            state @ RpcCallState::StreamingUnary { .. } => self.state = state,
            RpcCallState::Finished => return,
        }
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(error) = self.await {
                        if error.remote_status().is_some_and(|status| {
                            matches!(status.code(), StatusCode::Internal | StatusCode::DataLoss)
                        }) {
                            log::error!("detached RPC failed: {error}");
                        } else {
                            log::warn!("detached RPC failed: {error}");
                        }
                    }
                });
            }
            Err(error) => log::warn!("cannot detach RPC without a Tokio runtime: {error}"),
        }
    }

    /// Requests best-effort cancellation of an already submitted call.
    ///
    /// Cancellation races with normal completion and does not prove that the
    /// peer did not execute the request. It completes the local observer
    /// immediately; protocol-specific reset propagation is handled separately.
    pub fn cancel(&self) {
        match &self.state {
            RpcCallState::Active(token) => {
                token.cancel();
            }
            RpcCallState::StreamingUnary { recv, .. } => recv.cancel(),
            RpcCallState::Ready(_) | RpcCallState::Finished => {}
        }
    }
}

pub fn start_unary_call<Request, Response>(
    channel: &Channel,
    method: &'static MethodDescriptor,
    request: super::Request<Request>,
    connection_tag: Option<ConnectionTag>,
) -> CallHandle<Response>
where
    Request: Encode,
    Response: Decode + Default,
{
    let variables = rpc_variables::client_method(method).start_call();
    let result = match method.kind {
        MethodKind::Unary => match channel.wire_protocol() {
            Protocol::Brpc => super::brpc::start_owned(channel, method, request, connection_tag),
            Protocol::Grpc => super::grpc::start_owned(channel, method, request, connection_tag),
            Protocol::Http1 | Protocol::Http2 => Err(Status::new(
                StatusCode::InvalidArgument,
                "typed RPC requires a bRPC or gRPC channel",
            )),
        },
        _ => Err(Status::new(
            StatusCode::InvalidArgument,
            "unary call requires a unary method descriptor",
        )),
    };
    let state = match result {
        Ok(token) => RpcCallState::Active(token),
        Err(status) => RpcCallState::Ready(Some(status)),
    };
    CallHandle { state, variables: Some(variables), marker: std::marker::PhantomData }
}

impl<ResponseMessage: Decode + Default> Future for CallHandle<ResponseMessage> {
    type Output = Result<Response<ResponseMessage>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match &mut this.state {
            RpcCallState::Active(token) => {
                let completion = std::task::ready!(token.poll(cx));
                if completion.profile_sampled {
                    profile::record(ProfileStage::ClientRpcLatency, completion.latency);
                }
                let decode_start = profile::start_if(completion.profile_sampled);
                let result = if let Some(status) = completion.status {
                    Err(status)
                } else {
                    let mut response = ResponseMessage::default();
                    match response.decode_from(completion.payload) {
                        Ok(()) => {
                            let mut response = Response::from_parts(
                                response,
                                completion.attachment,
                                super::CallStats::observed_latency(completion.latency),
                            );
                            *response.metadata_mut() = completion.metadata;
                            *response.trailers_mut() = completion.trailers;
                            Ok(response)
                        }
                        Err(error) => Err(Status::from(error)),
                    }
                };
                if let Some(start) = decode_start {
                    profile::record_elapsed(ProfileStage::ClientResponseDecode, start);
                }
                result
            }
            RpcCallState::StreamingUnary { recv, metadata, payload } => {
                if metadata.is_none() {
                    *metadata = Some(std::task::ready!(recv.poll_response_headers(cx))?);
                }
                loop {
                    match std::task::ready!(recv.poll_message(cx))? {
                        Some(message) => {
                            if payload.replace(message).is_some() {
                                break Err(Status::new(
                                    StatusCode::Internal,
                                    "gRPC client-streaming response contains multiple messages",
                                ));
                            }
                        }
                        None => {
                            let Some(payload) = payload.take() else {
                                break Err(Status::new(
                                    StatusCode::Internal,
                                    "gRPC client-streaming response is missing a message",
                                ));
                            };
                            let mut message = ResponseMessage::default();
                            message.decode_from(payload)?;
                            let mut response = Response::new(message);
                            *response.metadata_mut() = metadata.take().unwrap_or_default();
                            if let Some(trailers) = recv.trailers() {
                                *response.trailers_mut() = trailers.clone();
                            }
                            break Ok(response);
                        }
                    }
                }
            }
            RpcCallState::Ready(status) => {
                Err(status.take().expect("RPC call polled after completion"))
            }
            RpcCallState::Finished => panic!("RPC call polled after completion"),
        };
        if let Some(variables) = this.variables.take() {
            variables.finish(result.as_ref().err().map(Status::code));
        }
        this.state = RpcCallState::Finished;
        Poll::Ready(result.map_err(Error::from))
    }
}
