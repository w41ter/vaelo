// SPDX-License-Identifier: Apache-2.0

//! gRPC server adapter over the HTTP/2 connection engine.

use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::framing::{MessageDirection, MessageStreamDecoder, encode_message};
use super::metadata::{
    append_metadata, append_metadata_unchecked, encode_grpc_message, encode_status_details,
    metadata_from_headers, validate_metadata,
};
use super::{GrpcOptions, status_from_h2_reset};
use crate::protocol::http2::connection::{
    FlowCredit, H2RecvStream, H2SendStream, ReceivedEvent, RecvStreamError,
};
use crate::protocol::http2::hpack::{HeaderBlock, HeaderField, HeaderIndexing};
use crate::server::{MethodId, ServerRequestContext, ServerResponseBody};
use crate::{
    IoBuf, MetadataMap, MethodKind, RequestProtocol, ServerResponse, ServiceRegistry,
    StaticResource, Status, StatusCode,
};

pub(crate) struct GrpcServerRecvStream {
    h2: H2RecvStream,
    decoder: MessageStreamDecoder,
    messages: VecDeque<IoBuf>,
    credits: Vec<FlowCredit>,
    release_credits_when_empty: bool,
    terminal: bool,
}

impl GrpcServerRecvStream {
    fn new(h2: H2RecvStream, terminal: bool, max_message_size: usize) -> Self {
        Self {
            h2,
            decoder: MessageStreamDecoder::new(max_message_size, MessageDirection::Request),
            messages: VecDeque::new(),
            credits: Vec::new(),
            release_credits_when_empty: false,
            terminal,
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
        loop {
            match std::pin::pin!(self.h2.recv()).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(server_recv_status(error))),
                Poll::Ready(Ok(None)) => {
                    return Poll::Ready(Err(Status::new(
                        StatusCode::Internal,
                        "gRPC request ended unexpectedly",
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
                    // The decoder has taken ownership of this DATA payload and
                    // bounds its partial-message buffer by the configured
                    // maximum message size. H2 credit therefore no longer has
                    // to wait for the complete gRPC message. Waiting until the
                    // decoder becomes idle deadlocks every message larger than
                    // the initial H2 window: the peer needs WINDOW_UPDATE to
                    // send the remainder that would make the decoder idle.
                    self.release_credits_when_empty = true;
                    self.release_credits_if_possible();
                    if end_stream {
                        self.terminal = true;
                    }
                    if let Some(message) = self.messages.pop_front() {
                        self.release_credits_if_possible();
                        return Poll::Ready(Ok(Some(message)));
                    }
                    if self.terminal {
                        self.credits.clear();
                        return Poll::Ready(Ok(None));
                    }
                }
                Poll::Ready(Ok(Some(ReceivedEvent::Headers { headers, end_stream, trailers }))) => {
                    if !trailers || !end_stream {
                        return Poll::Ready(Err(Status::new(
                            StatusCode::Internal,
                            "invalid gRPC request trailers",
                        )));
                    }
                    validate_request_trailers(&headers)?;
                    self.decoder.push_each(IoBuf::new(), true, |_| Ok(()))?;
                    self.terminal = true;
                    self.credits.clear();
                    return Poll::Ready(Ok(None));
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
}

fn server_recv_status(error: RecvStreamError) -> Status {
    match error {
        RecvStreamError::Reset(code) => status_from_h2_reset(code),
        RecvStreamError::ResourceExhausted => {
            Status::new(StatusCode::ResourceExhausted, "HTTP/2 stream resources exhausted")
        }
        _ => Status::new(StatusCode::Unavailable, "HTTP/2 request stream failed"),
    }
}

pub(crate) async fn handle_grpc_request_headers(
    remote_addr: SocketAddr,
    services: ServiceRegistry,
    mut send: H2SendStream,
    mut recv: H2RecvStream,
    headers: HeaderBlock,
    end_stream: bool,
    config: GrpcOptions,
) -> Result<(), Status> {
    let (method, context) = match resolve_request(remote_addr, &services, &headers) {
        Ok(resolved) => resolved,
        Err(status) => {
            log::warn!("gRPC request rejected: remote={remote_addr}, status={status}");
            send_response(&mut send, ServerResponse::failed(status.clone()), config)
                .finish()
                .await?;
            return Ok(());
        }
    };
    let descriptor = services
        .descriptor(method)
        .ok_or_else(|| Status::new(StatusCode::Unimplemented, "unknown RPC method"))?;
    let deadline = context.deadline;
    let cancellation = context.cancellation.clone();
    recv.set_cancellation(cancellation.clone());
    let mut execution = GrpcCallExecution::resource_pool()
        .acquire_with_init(|| {
            GrpcCallExecution::new(
                services,
                method,
                descriptor.kind,
                context,
                recv,
                end_stream,
                config.max_decoding_message_size,
            )
        })
        .map_err(|_| Status::new(StatusCode::ResourceExhausted, "gRPC call resources exhausted"))?;
    let work = std::future::poll_fn(move |cx| {
        // SAFETY: this private resource never publishes its ID and the sole
        // handle is moved into this one server task.
        unsafe { execution.get_mut_unchecked() }
            .expect("gRPC call execution unexpectedly shared")
            .poll(cx)
    });
    tokio::pin!(work);
    let (response, observe_cancellation_while_sending) = if let Some(deadline) = deadline {
        tokio::select! {
            response = &mut work => match response {
                Ok(response) => (response, true),
                Err(status) => (ServerResponse::failed(status), true),
            },
            _ = cancellation.cancelled() => return Ok(()),
            _ = tokio::time::sleep_until(deadline.into()) => {
                cancellation.cancel();
                (
                    ServerResponse::failed(Status::new(
                        StatusCode::DeadlineExceeded,
                        "request deadline exceeded",
                    )),
                    false,
                )
            }
        }
    } else {
        tokio::select! {
            response = &mut work => match response {
                Ok(response) => (response, true),
                Err(status) => (ServerResponse::failed(status), true),
            },
            _ = cancellation.cancelled() => return Ok(()),
        }
    };
    if !observe_cancellation_while_sending {
        return send_response(&mut send, response, config).finish().await;
    }
    tokio::select! {
        result = send_response(&mut send, response, config).finish() => result,
        _ = cancellation.cancelled() => Ok(()),
    }
}

#[derive(vaelo_derive::StaticResource)]
struct GrpcCallExecution {
    services: ServiceRegistry,
    method: MethodId,
    state: GrpcCallExecutionState,
}

// The execution resource is created and polled through one unique Resource
// handle owned by the server task. It is never published to another producer;
// Sync is required only so ResourcePool can recycle slots across workers.
unsafe impl Sync for GrpcCallExecution {}

enum GrpcCallExecutionState {
    UnaryFirst { context: ServerRequestContext, recv: GrpcServerRecvStream },
    UnarySecond { context: ServerRequestContext, recv: GrpcServerRecvStream, message: IoBuf },
    Running(crate::server::ServerCall),
    Complete,
}

impl GrpcCallExecution {
    fn new(
        services: ServiceRegistry,
        method: MethodId,
        kind: MethodKind,
        context: ServerRequestContext,
        recv: H2RecvStream,
        end_stream: bool,
        max_decoding_message_size: usize,
    ) -> Self {
        let request_stream = GrpcServerRecvStream::new(recv, end_stream, max_decoding_message_size);
        let state = match kind {
            MethodKind::Unary | MethodKind::ServerStreaming => {
                GrpcCallExecutionState::UnaryFirst { context, recv: request_stream }
            }
            MethodKind::ClientStreaming | MethodKind::BidirectionalStreaming => {
                GrpcCallExecutionState::Running(services.start_stream_resolved(
                    method,
                    context,
                    crate::client::RawStreaming::server(request_stream),
                ))
            }
        };
        Self { services, method, state }
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<ServerResponse, Status>> {
        loop {
            match &mut self.state {
                GrpcCallExecutionState::UnaryFirst { recv, .. } => {
                    let first = match recv.poll_message(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(first)) => first,
                        Poll::Ready(Err(error)) => {
                            self.state = GrpcCallExecutionState::Complete;
                            return Poll::Ready(Err(error));
                        }
                    };
                    let Some(message) = first else {
                        self.state = GrpcCallExecutionState::Complete;
                        return Poll::Ready(Ok(ServerResponse::failed(Status::new(
                            StatusCode::InvalidArgument,
                            "gRPC method expects one request message, received none",
                        ))));
                    };
                    let previous =
                        std::mem::replace(&mut self.state, GrpcCallExecutionState::Complete);
                    let GrpcCallExecutionState::UnaryFirst { context, recv } = previous else {
                        unreachable!();
                    };
                    self.state = GrpcCallExecutionState::UnarySecond { context, recv, message };
                }
                GrpcCallExecutionState::UnarySecond { recv, .. } => {
                    let second = match recv.poll_message(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(second)) => second,
                        Poll::Ready(Err(error)) => {
                            self.state = GrpcCallExecutionState::Complete;
                            return Poll::Ready(Err(error));
                        }
                    };
                    if second.is_some() {
                        self.state = GrpcCallExecutionState::Complete;
                        return Poll::Ready(Ok(ServerResponse::failed(Status::new(
                            StatusCode::InvalidArgument,
                            "gRPC method expects exactly one request message",
                        ))));
                    }
                    let previous =
                        std::mem::replace(&mut self.state, GrpcCallExecutionState::Complete);
                    let GrpcCallExecutionState::UnarySecond { context, message, .. } = previous
                    else {
                        unreachable!();
                    };
                    self.state = GrpcCallExecutionState::Running(self.services.start_resolved(
                        self.method,
                        context,
                        message,
                    ));
                }
                GrpcCallExecutionState::Running(call) => match call.poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(response) => {
                        self.state = GrpcCallExecutionState::Complete;
                        return Poll::Ready(Ok(response));
                    }
                },
                GrpcCallExecutionState::Complete => {
                    panic!("completed gRPC call execution polled again")
                }
            }
        }
    }
}

fn resolve_request(
    remote_addr: SocketAddr,
    services: &ServiceRegistry,
    headers: &HeaderBlock,
) -> Result<(MethodId, ServerRequestContext), Status> {
    if headers.pseudo.method.as_deref() != Some(b"POST".as_slice()) {
        return Err(Status::new(StatusCode::Unimplemented, "gRPC requires POST"));
    }
    let path = headers
        .pseudo
        .path
        .as_deref()
        .and_then(|path| std::str::from_utf8(path).ok())
        .ok_or_else(|| Status::new(StatusCode::Unimplemented, "invalid gRPC path"))?;
    let (service, method) = parse_grpc_path(path)
        .ok_or_else(|| Status::new(StatusCode::Unimplemented, "invalid gRPC path"))?;
    let method = services
        .resolve(service, method)
        .ok_or_else(|| Status::new(StatusCode::Unimplemented, "unknown RPC method"))?;
    let content_type = header_value(headers, b"content-type")
        .ok_or_else(|| Status::new(StatusCode::InvalidArgument, "missing gRPC content-type"))?;
    if !content_type.starts_with(b"application/grpc") {
        return Err(Status::new(StatusCode::InvalidArgument, "invalid gRPC content-type"));
    }
    if header_value(headers, b"te") != Some(b"trailers".as_slice()) {
        return Err(Status::new(StatusCode::InvalidArgument, "gRPC requires TE: trailers"));
    }

    let metadata = metadata_from_headers(headers)
        .map_err(|error| Status::new(StatusCode::InvalidArgument, error.to_string()))?;
    let deadline = header_value(headers, b"grpc-timeout")
        .map(parse_grpc_timeout)
        .transpose()?
        .map(|timeout| Instant::now() + timeout);
    Ok((
        method,
        ServerRequestContext {
            remote_addr,
            protocol: RequestProtocol::Grpc,
            deadline,
            cancellation: CancellationToken::new(),
            metadata,
            log_id: None,
            request_id: None,
            attachment: IoBuf::new(),
        },
    ))
}

fn validate_request_trailers(headers: &HeaderBlock) -> Result<(), Status> {
    if header_value(headers, b"grpc-status").is_some_and(|value| value != b"0") {
        return Err(Status::new(StatusCode::Internal, "client sent non-OK gRPC trailers"));
    }
    Ok(())
}

enum SendResponse<'a> {
    Ready(Result<(), Status>),
    Pending(Pin<Box<dyn Future<Output = Result<(), Status>> + Send + 'a>>),
}

impl SendResponse<'_> {
    async fn finish(self) -> Result<(), Status> {
        match self {
            Self::Ready(result) => result,
            Self::Pending(future) => future.await,
        }
    }
}

fn send_response<'a>(
    send: &'a mut H2SendStream,
    response: ServerResponse,
    config: GrpcOptions,
) -> SendResponse<'a> {
    let ServerResponse { body, status, metadata, trailing_metadata, .. } = response;
    let mut headers = HeaderBlock::new();
    headers.pseudo.status = Some(200);
    headers.push(HeaderField {
        name: Bytes::from_static(b"content-type"),
        value: Bytes::from_static(b"application/grpc"),
        indexing: HeaderIndexing::WithoutIndexing,
    });
    let mut status = status;
    if let Err(error) = append_metadata(&mut headers, &metadata) {
        status = Some(error);
    }

    // When the service fails before producing a response body, preserve that
    // distinction on the wire. A trailers-only response lets streaming
    // clients observe the failure while awaiting the call handle instead of
    // first publishing a stream which can only fail when it is polled.
    if status.is_some() {
        append_response_trailers(&mut headers, status.as_ref(), &trailing_metadata);
        return SendResponse::Ready(send.send_headers(headers, true).map_err(|_| {
            Status::new(StatusCode::Unavailable, "failed to send gRPC trailers-only response")
        }));
    }

    let body = match body {
        ServerResponseBody::Incremental(mut stream) => {
            return SendResponse::Pending(Box::pin(async move {
                send_headers_wait(send, headers, false).await?;
                let mut terminal_status = None;
                while terminal_status.is_none() {
                    let Some(item) =
                        std::future::poll_fn(|context| stream.as_mut().poll_next(context)).await
                    else {
                        break;
                    };
                    match item {
                        Ok(payload) => {
                            let payload = encode_message(
                                payload,
                                config.max_encoding_message_size,
                                MessageDirection::Response,
                            );
                            match payload {
                                Ok(payload) => send_data_wait(send, payload, false).await?,
                                Err(status) => terminal_status = Some(status),
                            }
                        }
                        Err(status) => terminal_status = Some(status),
                    }
                }
                let trailers = response_trailers(terminal_status.as_ref(), &trailing_metadata);
                send_headers_wait(send, trailers, true).await
            }));
        }
        body => body,
    };

    let encoded = match body {
        ServerResponseBody::Unary(payload) => {
            match encode_message(
                payload,
                config.max_encoding_message_size,
                MessageDirection::Response,
            ) {
                Ok(payload) => {
                    let trailers = response_trailers(None, &trailing_metadata);
                    if send.can_send_complete(1, true) {
                        return SendResponse::Ready(
                            send.send_complete_direct(headers, Some(payload), Some(trailers))
                                .map_err(|_| {
                                    Status::new(
                                        StatusCode::Unavailable,
                                        "failed to send gRPC response",
                                    )
                                }),
                        );
                    }
                    Ok(vec![payload])
                }
                Err(error) => Err(error),
            }
        }
        ServerResponseBody::Stream(messages) => messages
            .into_iter()
            .map(|payload| {
                encode_message(
                    payload,
                    config.max_encoding_message_size,
                    MessageDirection::Response,
                )
            })
            .collect(),
        ServerResponseBody::Incremental(_) => unreachable!(),
    };
    let data = match encoded {
        Ok(data) => data,
        Err(error) => {
            append_response_trailers(&mut headers, Some(&error), &trailing_metadata);
            return SendResponse::Ready(send.send_headers(headers, true).map_err(|_| {
                Status::new(StatusCode::Unavailable, "failed to send gRPC trailers-only response")
            }));
        }
    };

    let trailers = response_trailers(status.as_ref(), &trailing_metadata);
    if send.can_send_complete(data.len(), true) {
        return SendResponse::Ready(
            send.send_complete(headers, data, Some(trailers))
                .map_err(|_| Status::new(StatusCode::Unavailable, "failed to send gRPC response")),
        );
    }

    SendResponse::Pending(Box::pin(async move {
        send_headers_wait(send, headers, false).await?;
        for payload in data {
            send_data_wait(send, payload, false).await?;
        }
        send_headers_wait(send, trailers, true).await
    }))
}

fn response_trailers(status: Option<&Status>, metadata: &MetadataMap) -> HeaderBlock {
    let mut trailers = HeaderBlock::new();
    append_response_trailers(&mut trailers, status, metadata);
    trailers
}

fn append_response_trailers(
    trailers: &mut HeaderBlock,
    status: Option<&Status>,
    metadata: &MetadataMap,
) {
    let metadata_error = status
        .map(Status::metadata)
        .map(validate_metadata)
        .transpose()
        .and_then(|_| validate_metadata(metadata))
        .err()
        .map(|error| Status::new(StatusCode::Internal, error.message().to_owned()));
    let status = status.or(metadata_error.as_ref());
    let code = status.map_or(StatusCode::Ok, Status::code);
    trailers.push(HeaderField {
        name: Bytes::from_static(b"grpc-status"),
        value: Bytes::from(code.as_i32().to_string()),
        indexing: HeaderIndexing::NeverIndexed,
    });
    if let Some(status) = status
        && !status.message().is_empty()
    {
        trailers.push(HeaderField {
            name: Bytes::from_static(b"grpc-message"),
            value: encode_grpc_message(status.message()),
            indexing: HeaderIndexing::NeverIndexed,
        });
    }
    if let Some(status) = status {
        if !status.details().is_empty() {
            trailers.push(HeaderField {
                name: Bytes::from_static(b"grpc-status-details-bin"),
                value: encode_status_details(status.details()),
                indexing: HeaderIndexing::NeverIndexed,
            });
        }
        if metadata_error.is_none() {
            append_metadata_unchecked(trailers, status.metadata());
        }
    }
    if metadata_error.is_none() {
        append_metadata_unchecked(trailers, metadata);
    }
}

async fn send_data_wait(
    send: &mut H2SendStream,
    payload: IoBuf,
    end_stream: bool,
) -> Result<(), Status> {
    let mut payload = Some(payload);
    std::future::poll_fn(|context| send.poll_send_data(context, &mut payload, end_stream))
        .await
        .map_err(|_| Status::new(StatusCode::Unavailable, "failed to send gRPC response DATA"))
}

async fn send_headers_wait(
    send: &mut H2SendStream,
    headers: HeaderBlock,
    end_stream: bool,
) -> Result<(), Status> {
    let mut headers = Some(headers);
    std::future::poll_fn(|context| send.poll_send_headers(context, &mut headers, end_stream))
        .await
        .map_err(|_| Status::new(StatusCode::Unavailable, "failed to send gRPC response headers"))
}

fn header_value<'a>(headers: &'a HeaderBlock, name: &[u8]) -> Option<&'a [u8]> {
    headers.fields().find(|field| field.name.as_ref() == name).map(|field| field.value.as_ref())
}

fn parse_grpc_timeout(value: &[u8]) -> Result<std::time::Duration, Status> {
    if value.len() < 2 || value.len() > 9 {
        return Err(Status::new(StatusCode::InvalidArgument, "invalid grpc-timeout"));
    }
    let amount = std::str::from_utf8(&value[..value.len() - 1])
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| Status::new(StatusCode::InvalidArgument, "invalid grpc-timeout"))?;
    let duration = match value[value.len() - 1] {
        b'H' => std::time::Duration::from_secs(amount.saturating_mul(3600)),
        b'M' => std::time::Duration::from_secs(amount.saturating_mul(60)),
        b'S' => std::time::Duration::from_secs(amount),
        b'm' => std::time::Duration::from_millis(amount),
        b'u' => std::time::Duration::from_micros(amount),
        b'n' => std::time::Duration::from_nanos(amount),
        _ => return Err(Status::new(StatusCode::InvalidArgument, "invalid grpc-timeout unit")),
    };
    Ok(duration)
}

fn parse_grpc_path(path: &str) -> Option<(&str, &str)> {
    let path = path.strip_prefix('/')?;
    let split = path.rfind('/')?;
    let (service, method) = path.split_at(split);
    let method = method.strip_prefix('/')?;
    (!service.is_empty() && !method.is_empty()).then_some((service, method))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_call_execution_stays_compact() {
        assert!(std::mem::size_of::<GrpcCallExecution>() <= 512);
    }
}
