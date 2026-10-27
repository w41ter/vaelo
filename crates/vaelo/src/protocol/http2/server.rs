// SPDX-License-Identifier: Apache-2.0

use std::future::poll_fn;
use std::net::SocketAddr;

use bytes::Bytes;

use super::Http2Protocol;
use super::connection::{AcceptedStream, H2Connection, H2RecvStream, H2SendStream, H2ServerLimits};
use super::hpack::{HeaderBlock, HeaderField, HeaderIndexing};
use crate::error::{Error, Result};
use crate::http::{Frame as HttpFrame, Incoming, SizeHint};
use crate::server::ServerSessionContext;
use crate::server::http::{HttpResponse, dispatch};
use crate::transport::tcp::TcpConnection;
use crate::{IoBuf, Resource, StaticResource};

pub(super) fn new_session(
    connection: Resource<TcpConnection>,
    protocol: &'static Http2Protocol,
    context: ServerSessionContext,
) -> Result<Resource<ServerSession>> {
    ServerSession::resource_pool()
        .acquire_with_init(|| ServerSession { connection, protocol, context })
        .map_err(|_| Error::ResourceExhausted)
}

/// HTTP/2 server session for one accepted TCP connection.
///
/// HTTP/2 is a connection-level state machine. A session must not spawn an
/// independent read continuation per completed request the way bRPC or HTTP/1
/// can. SETTINGS, HPACK, flow control, GOAWAY, and stream transitions all share
/// `Http2ConnectionState`.
#[derive(vaelo_derive::StaticResource)]
pub(crate) struct ServerSession {
    connection: Resource<TcpConnection>,
    protocol: &'static Http2Protocol,
    context: ServerSessionContext,
}

impl ServerSession {
    /// Starts the single HTTP/2 connection driver.
    ///
    /// The driver owns the read buffer. When it reconstructs a complete request
    /// stream, it may spawn a service task, but protocol state mutation must
    /// re-enter `Http2ConnectionState` before response frames are encoded.
    pub(crate) fn handle_read_event(self: Resource<Self>, input: IoBuf) {
        tokio::spawn(self.run_connection(input));
    }

    async fn run_connection(self: Resource<Self>, input: IoBuf) {
        let _ = self.protocol;
        let Some(socket) = self.connection.current_socket() else {
            self.connection.close();
            return;
        };
        let remote_addr = *self.connection.remote_addr();
        let limits = H2ServerLimits {
            max_header_size: self.context.http_options.max_header_size,
            max_body_size: self.context.http_options.max_body_size,
            max_concurrent_streams: self.context.http_options.max_pipeline_depth.max(1),
        };
        let connection = match H2Connection::new_server(socket, input, limits) {
            Ok(connection) => connection,
            Err(error) => {
                log::warn!(
                    "HTTP/2 server initialization failed: remote={remote_addr}, error={error:?}"
                );
                self.connection.close();
                return;
            }
        };
        while let Some(accepted) = connection.accept_stream().await {
            let context = self.context.clone();
            if is_grpc(&accepted.headers) {
                tokio::spawn(async move {
                    let AcceptedStream { stream, headers, end_stream } = accepted;
                    let (send, recv) = stream.split();
                    if let Err(error) = crate::protocol::grpc::server::handle_grpc_request_headers(
                        remote_addr,
                        context.services,
                        send,
                        recv,
                        headers,
                        end_stream,
                        context.grpc_options,
                    )
                    .await
                    {
                        log::warn!(
                            "HTTP/2 gRPC stream failed: remote={remote_addr}, error={error}"
                        );
                    }
                });
            } else {
                tokio::spawn(async move {
                    let AcceptedStream { stream, headers, end_stream } = accepted;
                    let (send, recv) = stream.split();
                    if let Err(error) =
                        handle_http_request(remote_addr, context, send, recv, headers, end_stream)
                            .await
                    {
                        log::warn!("HTTP/2 stream failed: remote={remote_addr}, error={error}");
                    }
                });
            }
        }
        self.connection.close();
    }
}

fn is_grpc(headers: &HeaderBlock) -> bool {
    headers.fields().any(|field| {
        field.name.as_ref() == b"content-type"
            && field.value.as_ref().starts_with(b"application/grpc")
    })
}

async fn handle_http_request(
    remote_addr: SocketAddr,
    context: ServerSessionContext,
    mut send: H2SendStream,
    recv: H2RecvStream,
    headers: HeaderBlock,
    end_stream: bool,
) -> Result<()> {
    let method = headers
        .pseudo
        .method
        .as_deref()
        .and_then(|value| http::Method::from_bytes(value).ok())
        .ok_or_else(|| Error::Protocol("HTTP/2 request has invalid :method".into()))?;
    let target = headers
        .pseudo
        .path
        .as_deref()
        .and_then(|value| std::str::from_utf8(value).ok())
        .ok_or_else(|| Error::Protocol("HTTP/2 request has invalid :path".into()))?;
    let uri: http::Uri = target
        .parse()
        .map_err(|_| Error::Protocol("HTTP/2 request has invalid request target".into()))?;
    let path = uri.path().to_owned();
    let query = uri.query().map(str::to_owned);
    let mut regular = http::HeaderMap::new();
    for field in headers.fields() {
        let name = http::HeaderName::from_bytes(field.name.as_ref())
            .map_err(|_| Error::Protocol("HTTP/2 request has invalid header name".into()))?;
        let value = http::HeaderValue::from_bytes(field.value.as_ref())
            .map_err(|_| Error::Protocol("HTTP/2 request has invalid header value".into()))?;
        regular.append(name, value);
    }
    let hint = regular
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .map_or_else(SizeHint::unknown, SizeHint::exact);
    let request = crate::server::HttpRequest {
        method,
        version: http::Version::HTTP_2,
        path,
        query,
        headers: regular,
        body: Incoming::http2(recv, end_stream, hint),
        path_parameters: std::collections::HashMap::new(),
        remote_addr,
    };
    let response = dispatch(
        context.http_registry,
        context.builtin_registry,
        context.services,
        context.http_options,
        request,
    )
    .await;
    send_http_response(&mut send, response).await
}

async fn send_http_response(send: &mut H2SendStream, mut response: HttpResponse) -> Result<()> {
    let headers = response_headers(response.status, &response.headers)?;
    let Some(mut body) = response.streaming.take() else {
        if response.body.is_empty() {
            send.send_headers(headers, true)
                .map_err(|_| Error::Channel("failed to send HTTP/2 response headers".into()))?;
        } else {
            send.send_complete_one(headers, response.body)
                .map_err(|_| Error::Channel("failed to publish HTTP/2 response".into()))?;
        }
        return Ok(());
    };
    send_headers_wait(send, headers, false).await?;
    while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx))
        .await
        .transpose()
        .map_err(|error| Error::Encode(error.to_string().into()))?
    {
        match frame {
            HttpFrame::Data(data) => send_data_wait(send, data, false).await?,
            HttpFrame::Trailers(trailers) => {
                send_headers_wait(send, regular_headers(&trailers)?, true).await?;
                return Ok(());
            }
        }
    }
    send_data_wait(send, IoBuf::new(), true).await
}

fn response_headers(status: http::StatusCode, headers: &http::HeaderMap) -> Result<HeaderBlock> {
    let mut block = regular_headers(headers)?;
    block.pseudo.status = Some(status.as_u16());
    Ok(block)
}

fn regular_headers(headers: &http::HeaderMap) -> Result<HeaderBlock> {
    let mut block = HeaderBlock::new();
    for (name, value) in headers {
        if matches!(
            name.as_str(),
            "connection" | "proxy-connection" | "keep-alive" | "transfer-encoding" | "upgrade"
        ) {
            continue;
        }
        block.push(HeaderField {
            name: Bytes::copy_from_slice(name.as_str().as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            indexing: if name == http::header::SET_COOKIE {
                HeaderIndexing::NeverIndexed
            } else {
                HeaderIndexing::WithoutIndexing
            },
        });
    }
    Ok(block)
}

async fn send_data_wait(send: &mut H2SendStream, data: IoBuf, end_stream: bool) -> Result<()> {
    let mut data = Some(data);
    poll_fn(|cx| send.poll_send_data(cx, &mut data, end_stream))
        .await
        .map_err(|_| Error::Channel("failed to send HTTP/2 response DATA".into()))
}

async fn send_headers_wait(
    send: &mut H2SendStream,
    headers: HeaderBlock,
    end_stream: bool,
) -> Result<()> {
    let mut headers = Some(headers);
    poll_fn(|cx| send.poll_send_headers(cx, &mut headers, end_stream))
        .await
        .map_err(|_| Error::Channel("failed to send HTTP/2 response HEADERS".into()))
}
