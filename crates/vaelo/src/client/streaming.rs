// SPDX-License-Identifier: Apache-2.0

//! Public streaming RPC call shapes.

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio_stream::Stream;

use super::call::CallOptions;
use super::{CallHandle, ConnectionTag, Request, Response};
use crate::protocol::grpc::client::{GrpcClientRecvStream, GrpcClientSendStream};
use crate::protocol::grpc::framing::encode_message;
use crate::protocol::grpc::server::GrpcServerRecvStream;
use crate::{
    CallStats, Decode, Encode, Error, IoBuf, IoBufBuilder, MetadataMap, MethodDescriptor,
    MethodKind, RpcChannel, Status, StatusCode,
};

/// Incremental messages received from a streaming RPC.
///
/// Messages are decoded and yielded only as the consumer polls this stream.
/// Dropping it requests protocol-level cancellation and releases stream flow
/// control; implementations must not collect the full response into a `Vec`.
pub struct Streaming<T, E = Error> {
    state: StreamingState<T, E>,
    trailers: Option<MetadataMap>,
    stats: Option<CallStats>,
}

// The receive state stays inline so constructing a stream remains
// allocation-free.
#[allow(clippy::large_enum_variant)]
enum StreamingState<T, E> {
    Active {
        receiver: RawStreaming,
        map_status: fn(Status) -> E,
        map_decode: fn(Error) -> E,
        marker: PhantomData<fn() -> T>,
    },
    Finished,
}

#[doc(hidden)]
pub struct RawStreaming {
    inner: RawStreamingInner,
}

enum RawStreamingInner {
    Client(GrpcClientRecvStream),
    Server(GrpcServerRecvStream),
}

impl RawStreaming {
    pub(crate) fn server(receiver: GrpcServerRecvStream) -> Self {
        Self { inner: RawStreamingInner::Server(receiver) }
    }

    fn client(receiver: GrpcClientRecvStream) -> Self {
        Self { inner: RawStreamingInner::Client(receiver) }
    }

    fn poll_message(&mut self, context: &mut Context<'_>) -> Poll<Result<Option<IoBuf>, Status>> {
        match &mut self.inner {
            RawStreamingInner::Client(receiver) => receiver.poll_message(context),
            RawStreamingInner::Server(receiver) => receiver.poll_message(context),
        }
    }

    fn trailers(&self) -> Option<&MetadataMap> {
        match &self.inner {
            RawStreamingInner::Client(receiver) => receiver.trailers(),
            RawStreamingInner::Server(_) => None,
        }
    }
}

impl<T, E> Unpin for Streaming<T, E> {}

impl<T, E> Streaming<T, E> {
    /// Creates the receive side of an active transport stream.
    pub(crate) fn active_with(
        receiver: RawStreaming,
        map_status: fn(Status) -> E,
        map_decode: fn(Error) -> E,
    ) -> Self {
        Self {
            state: StreamingState::Active { receiver, map_status, map_decode, marker: PhantomData },
            trailers: None,
            stats: None,
        }
    }

    pub(crate) fn from_raw(
        receiver: RawStreaming,
        map_status: fn(Status) -> E,
        map_decode: fn(Error) -> E,
    ) -> Self {
        Self::active_with(receiver, map_status, map_decode)
    }

    /// Receives one message without requiring StreamExt.
    pub async fn message(&mut self) -> Result<Option<T>, E>
    where
        T: Decode + Default,
    {
        match std::future::poll_fn(|context| Pin::new(&mut *self).poll_next(context)).await {
            Some(Ok(message)) => Ok(Some(message)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    /// Finishes the receive side and returns final trailing metadata.
    ///
    /// Implementations must not buffer an unbounded number of skipped
    /// messages. Calling this before consuming the stream means abandoning the
    /// remainder under the call's existing deadline/cancellation policy.
    pub async fn finish(&mut self) -> Result<&MetadataMap, E>
    where
        T: Decode + Default,
    {
        while self.message().await?.is_some() {}
        Ok(self.trailers.as_ref().expect("completed stream has trailers"))
    }

    /// Returns trailers only after the stream reached a valid terminal state.
    pub fn trailers(&self) -> Option<&MetadataMap> {
        self.trailers.as_ref()
    }

    /// Returns final stream statistics when observation was enabled and the
    /// stream has completed.
    pub fn stats(&self) -> Option<CallStats> {
        self.stats
    }
}

impl<T, E> Stream for Streaming<T, E>
where
    T: Decode + Default,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.state {
            StreamingState::Active { receiver, map_status, map_decode, .. } => {
                match receiver.poll_message(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(Some(payload))) => {
                        let mut message = T::default();
                        Poll::Ready(Some(
                            message.decode_from(payload).map(|_| message).map_err(*map_decode),
                        ))
                    }
                    Poll::Ready(Ok(None)) => {
                        this.trailers =
                            Some(receiver.trailers().cloned().unwrap_or_else(MetadataMap::new));
                        this.state = StreamingState::Finished;
                        Poll::Ready(None)
                    }
                    Poll::Ready(Err(status)) => {
                        let map_status = *map_status;
                        this.state = StreamingState::Finished;
                        Poll::Ready(Some(Err(map_status(status))))
                    }
                }
            }
            StreamingState::Finished => Poll::Ready(None),
        }
    }
}

macro_rules! call_options {
    ($type:ident<$($generic:ident),+>) => {
        impl<$($generic),+> $type<$($generic),+> {
            pub fn timeout(mut self, timeout: Duration) -> Self {
                self.options.set_timeout(timeout);
                self
            }

            pub fn deadline(mut self, deadline: Instant) -> Self {
                self.options.set_deadline(deadline);
                self
            }

            pub fn connection_tag(mut self, tag: ConnectionTag) -> Self {
                self.options.set_connection_tag(tag);
                self
            }
        }
    };
}

/// Unstarted server-streaming call.
pub struct ServerStreamingCall<RequestMessage, ResponseMessage> {
    channel: RpcChannel,
    method: &'static MethodDescriptor,
    request: Request<RequestMessage>,
    options: CallOptions,
    response: PhantomData<fn() -> ResponseMessage>,
}

call_options!(ServerStreamingCall<RequestMessage, ResponseMessage>);

impl<RequestMessage, ResponseMessage> ServerStreamingCall<RequestMessage, ResponseMessage> {
    pub(crate) fn new(
        channel: RpcChannel,
        method: &'static MethodDescriptor,
        request: Request<RequestMessage>,
    ) -> Self {
        Self { channel, method, request, options: CallOptions::new(), response: PhantomData }
    }

    pub fn start(mut self) -> StreamingCallHandle<ResponseMessage>
    where
        RequestMessage: Encode,
    {
        let connection_tag = self.options.apply(&mut self.request);
        if self.method.kind != MethodKind::ServerStreaming {
            return StreamingCallHandle::failed(Status::new(
                StatusCode::InvalidArgument,
                "server-streaming call requires a server-streaming method descriptor",
            ));
        }
        let (message, request) = erase_request(self.request);
        let payload = match encode_request_message(&message) {
            Ok(payload) => payload,
            Err(status) => return StreamingCallHandle::failed(status),
        };
        match super::grpc::start_streaming_owned(
            &self.channel,
            self.method,
            request,
            connection_tag,
            Some(payload),
        ) {
            Ok((None, recv)) => StreamingCallHandle::active(recv),
            Ok((Some(_), _)) => StreamingCallHandle::failed(Status::new(
                StatusCode::Internal,
                "server-streaming call unexpectedly returned a request sender",
            )),
            Err(status) => StreamingCallHandle::failed(status),
        }
    }
}

impl<RequestMessage, ResponseMessage> IntoFuture
    for ServerStreamingCall<RequestMessage, ResponseMessage>
where
    RequestMessage: Encode,
    ResponseMessage: Decode + Default,
{
    type Output = Result<Response<Streaming<ResponseMessage>>, Error>;
    type IntoFuture = StreamingCallHandle<ResponseMessage>;

    fn into_future(self) -> Self::IntoFuture {
        self.start()
    }
}

/// Unstarted client-streaming call.
///
/// Starting it opens one uniquely-owned [`SendStream`]. The builder itself
/// never accepts an application `Stream`, which keeps the public API fixed and
/// avoids an implicit producer task or queue in the RPC core.
pub struct ClientStreamingCall<RequestMessage, ResponseMessage> {
    channel: RpcChannel,
    method: &'static MethodDescriptor,
    request: Request<()>,
    options: CallOptions,
    messages: PhantomData<fn(RequestMessage) -> ResponseMessage>,
}

call_options!(ClientStreamingCall<RequestMessage, ResponseMessage>);

impl<RequestMessage, ResponseMessage> ClientStreamingCall<RequestMessage, ResponseMessage> {
    pub(crate) fn new(channel: RpcChannel, method: &'static MethodDescriptor) -> Self {
        Self {
            channel,
            method,
            request: Request::new(()),
            options: CallOptions::new(),
            messages: PhantomData,
        }
    }

    /// Returns metadata that will be sent before the first request message.
    pub fn metadata(&self) -> &MetadataMap {
        self.request.metadata()
    }

    /// Mutates metadata that will be sent before the first request message.
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        self.request.metadata_mut()
    }

    pub fn start(mut self) -> ClientStreaming<RequestMessage, ResponseMessage> {
        let connection_tag = self.options.apply(&mut self.request);
        let result = if self.method.kind != MethodKind::ClientStreaming {
            Err(Status::new(
                StatusCode::InvalidArgument,
                "client-streaming call requires a client-streaming method descriptor",
            ))
        } else {
            super::grpc::start_streaming_owned(
                &self.channel,
                self.method,
                self.request,
                connection_tag,
                None,
            )
        };
        match result {
            Ok((Some(send), recv)) => ClientStreaming {
                sender: SendStream::active(send),
                response: CallHandle::from_streaming(self.method, recv),
            },
            Ok((None, _)) => unreachable!("client-streaming call must have a request sender"),
            Err(status) => ClientStreaming {
                sender: SendStream::failed(status.clone()),
                response: CallHandle::ready_error(self.method, status),
            },
        }
    }
}

/// Unstarted bidirectional-streaming call. Both request and response messages
/// are transferred incrementally under protocol flow control.
pub struct BidiStreamingCall<RequestMessage, ResponseMessage> {
    channel: RpcChannel,
    method: &'static MethodDescriptor,
    request: Request<()>,
    options: CallOptions,
    messages: PhantomData<fn(RequestMessage) -> ResponseMessage>,
}

call_options!(BidiStreamingCall<RequestMessage, ResponseMessage>);

impl<RequestMessage, ResponseMessage> BidiStreamingCall<RequestMessage, ResponseMessage> {
    pub(crate) fn new(channel: RpcChannel, method: &'static MethodDescriptor) -> Self {
        Self {
            channel,
            method,
            request: Request::new(()),
            options: CallOptions::new(),
            messages: PhantomData,
        }
    }

    /// Returns metadata that will be sent before the first request message.
    pub fn metadata(&self) -> &MetadataMap {
        self.request.metadata()
    }

    /// Mutates metadata that will be sent before the first request message.
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        self.request.metadata_mut()
    }

    pub fn start(mut self) -> BidiStreaming<RequestMessage, ResponseMessage> {
        let connection_tag = self.options.apply(&mut self.request);
        let result = if self.method.kind != MethodKind::BidirectionalStreaming {
            Err(Status::new(
                StatusCode::InvalidArgument,
                "bidirectional call requires a bidirectional-streaming method descriptor",
            ))
        } else {
            super::grpc::start_streaming_owned(
                &self.channel,
                self.method,
                self.request,
                connection_tag,
                None,
            )
        };
        match result {
            Ok((Some(send), recv)) => BidiStreaming {
                sender: SendStream::active(send),
                response: StreamingCallHandle::active(recv),
            },
            Ok((None, _)) => unreachable!("bidirectional call must have a request sender"),
            Err(status) => BidiStreaming {
                sender: SendStream::failed(status.clone()),
                response: StreamingCallHandle::failed(status),
            },
        }
    }
}

/// Writable request half of a client- or bidirectional-streaming RPC.
///
/// `send` is the backpressure boundary: it remains pending until the active
/// RPC can accept the message under its transport flow control. Completion
/// does not mean that the peer has received or processed the message. This
/// contract does not prescribe an internal channel, queue or writer task.
pub struct SendStream<T> {
    inner: Option<GrpcClientSendStream>,
    failure: Option<Status>,
    _message: PhantomData<fn(T)>,
}

impl<T> SendStream<T> {
    fn active(inner: GrpcClientSendStream) -> Self {
        Self { inner: Some(inner), failure: None, _message: PhantomData }
    }

    fn failed(status: Status) -> Self {
        Self { inner: None, failure: Some(status), _message: PhantomData }
    }

    /// Transfers one message to the active RPC under transport backpressure.
    pub async fn send(&mut self, message: T) -> Result<(), Error>
    where
        T: Encode,
    {
        if let Some(status) = &self.failure {
            return Err(status.clone().into());
        }
        let mut payload = Some(encode_request_message(&message)?);
        std::future::poll_fn(|context| {
            self.inner
                .as_mut()
                .expect("active sender has H2 state")
                .poll_send(context, &mut payload)
        })
        .await
        .map_err(Error::from)
    }

    /// Closes the request half after all previously accepted messages.
    ///
    /// This is a local half-close, not an acknowledgement from the peer.
    pub async fn finish(&mut self) -> Result<(), Error> {
        if let Some(status) = &self.failure {
            return Err(status.clone().into());
        }
        let mut payload = Some(IoBuf::new());
        std::future::poll_fn(|context| {
            self.inner
                .as_mut()
                .expect("active sender has H2 state")
                .poll_finish(context, &mut payload)
        })
        .await
        .map_err(Error::from)
    }

    pub fn cancel(&self) {
        if let Some(inner) = &self.inner {
            inner.cancel();
        }
    }
}

/// Active client-streaming RPC.
///
/// The send half is uniquely owned. `into_parts` moves the two handles apart
/// when request production and response completion are driven concurrently;
/// it does not allocate a channel or spawn a forwarding task.
pub struct ClientStreaming<RequestMessage, ResponseMessage> {
    sender: SendStream<RequestMessage>,
    response: CallHandle<ResponseMessage>,
}

impl<RequestMessage, ResponseMessage> ClientStreaming<RequestMessage, ResponseMessage> {
    pub async fn send(&mut self, message: RequestMessage) -> Result<(), Error>
    where
        RequestMessage: Encode,
    {
        self.sender.send(message).await
    }

    pub async fn finish(mut self) -> Result<Response<ResponseMessage>, Error>
    where
        ResponseMessage: Decode + Default,
    {
        self.sender.finish().await?;
        self.response.await
    }

    pub fn into_parts(self) -> (SendStream<RequestMessage>, CallHandle<ResponseMessage>) {
        (self.sender, self.response)
    }

    pub fn cancel(&self) {
        self.sender.cancel();
    }
}

/// Active bidirectional-streaming RPC.
///
/// Applications which can fill the peer's receive window before polling
/// responses must use `into_parts` and drive both halves concurrently.
pub struct BidiStreaming<RequestMessage, ResponseMessage> {
    sender: SendStream<RequestMessage>,
    response: StreamingCallHandle<ResponseMessage>,
}

impl<RequestMessage, ResponseMessage> BidiStreaming<RequestMessage, ResponseMessage> {
    pub async fn send(&mut self, message: RequestMessage) -> Result<(), Error>
    where
        RequestMessage: Encode,
    {
        self.sender.send(message).await
    }

    pub async fn finish_send(&mut self) -> Result<(), Error> {
        self.sender.finish().await
    }

    pub fn into_parts(self) -> (SendStream<RequestMessage>, StreamingCallHandle<ResponseMessage>) {
        (self.sender, self.response)
    }

    pub fn cancel(&self) {
        self.sender.cancel();
    }
}

/// Active server- or bidirectional-streaming call.
pub struct StreamingCallHandle<T> {
    state: StreamingHandleState,
    _message: PhantomData<fn() -> T>,
}

enum StreamingHandleState {
    Active(GrpcClientRecvStream),
    Failed(Option<Status>),
    Finished,
}

impl<T> StreamingCallHandle<T> {
    fn active(recv: GrpcClientRecvStream) -> Self {
        Self { state: StreamingHandleState::Active(recv), _message: PhantomData }
    }

    fn failed(status: Status) -> Self {
        Self { state: StreamingHandleState::Failed(Some(status)), _message: PhantomData }
    }

    pub fn cancel(&self) {
        if let StreamingHandleState::Active(recv) = &self.state {
            recv.cancel();
        }
    }
}

impl<T> Unpin for StreamingCallHandle<T> {}

impl<T> Future for StreamingCallHandle<T>
where
    T: Decode + Default,
{
    type Output = Result<Response<Streaming<T>>, Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            StreamingHandleState::Active(recv) => {
                let metadata = match recv.poll_response_headers(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(metadata)) => metadata,
                    Poll::Ready(Err(status)) => {
                        this.state = StreamingHandleState::Finished;
                        return Poll::Ready(Err(status.into()));
                    }
                };
                let StreamingHandleState::Active(recv) =
                    std::mem::replace(&mut this.state, StreamingHandleState::Finished)
                else {
                    unreachable!()
                };
                let mut response = Response::new(Streaming::active_with(
                    RawStreaming::client(recv),
                    Error::from,
                    |error| error,
                ));
                *response.metadata_mut() = metadata;
                Poll::Ready(Ok(response))
            }
            StreamingHandleState::Failed(status) => Poll::Ready(Err(status
                .take()
                .expect("streaming handle polled after completion")
                .into())),
            StreamingHandleState::Finished => panic!("streaming handle polled after completion"),
        }
    }
}

fn erase_request<T>(request: Request<T>) -> (T, Request<()>) {
    let (message, options, attachment) = request.into_wire_parts();
    let mut erased = Request::new(());
    erased.options = options;
    erased.attachment = attachment;
    (message, erased)
}

fn encode_request_message<T: Encode>(message: &T) -> Result<IoBuf, Status> {
    let mut builder = IoBufBuilder::new();
    message.encode_to(&mut builder)?;
    encode_message(
        builder.finish(),
        crate::protocol::grpc::GrpcConfig::default().max_encoding_message_size,
    )
}
