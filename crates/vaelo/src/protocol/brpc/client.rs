// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::client::recycler::{ClientActivityGuard, ClientConnectionState};
use crate::protocol::{InboundResponse, RequestEncodeContext};
use crate::request_options::RequestOptions;
use crate::runtime::profile::{self, ProfileStage};
use crate::transport::inflight::{CallContext, CallToken, call_id};
use crate::transport::tcp::{OnConnected, TcpConnection, TcpSocket, WriteOptions};
use crate::{Error, IoBuf, MethodDescriptor, Resource, Result, StaticResource, Status, StatusCode};

pub(crate) struct PreparedRequest {
    pub(crate) method: &'static MethodDescriptor,
    pub(crate) options: RequestOptions,
    pub(crate) payload: IoBuf,
    pub(crate) attachment: IoBuf,
}

/// bRPC client session bound to one logical TCP connection.
///
/// The session intentionally does not maintain a protocol-level pending table.
/// Client bRPC correlation ids are the owned `CallContext` resource ids, so a
/// response can complete its call by looking the context up directly from the
/// resource pool.
///
/// Ownership split:
/// - request bytes are owned by `TcpConnection` write requests until written;
/// - in-flight call state is owned by `CallContext`;
/// - socket liveness, reconnect, and failure completion are owned by
///   `TcpConnection`;
/// - this session owns only bRPC codec use and read-task dispatch.
#[derive(vaelo_derive::StaticResource)]
pub(crate) struct BrpcClientSession {
    connection: Resource<TcpConnection>,
    protocol: &'static super::BrpcProtocol,
    activity: Option<Resource<ClientConnectionState>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrpcClientConnectionMode {
    Persistent,
    CloseAfterResponse,
}

impl BrpcClientSession {
    /// Creates a bRPC session and installs its TCP `OnConnected` hook.
    ///
    /// The callback does not reference the session. A connected socket only
    /// needs the TCP connection handle, the socket handle, and the bRPC codec
    /// to run its reader, so the callback can stay independent from channel
    /// lifetime management.
    pub(crate) fn create(
        remote_addr: SocketAddr,
        connect_timeout: Option<Duration>,
    ) -> Result<Resource<Self>> {
        Self::create_with_mode(remote_addr, connect_timeout, BrpcClientConnectionMode::Persistent)
    }

    pub(crate) fn create_short(
        remote_addr: SocketAddr,
        connect_timeout: Option<Duration>,
    ) -> Result<Resource<Self>> {
        Self::create_with_mode(
            remote_addr,
            connect_timeout,
            BrpcClientConnectionMode::CloseAfterResponse,
        )
    }

    fn create_with_mode(
        remote_addr: SocketAddr,
        connect_timeout: Option<Duration>,
        mode: BrpcClientConnectionMode,
    ) -> Result<Resource<Self>> {
        let activity = match mode {
            BrpcClientConnectionMode::Persistent => Some(ClientConnectionState::create()?),
            BrpcClientConnectionMode::CloseAfterResponse => None,
        };
        let connection = TcpConnection::create(
            remote_addr,
            connect_timeout,
            Some(Box::new(BrpcOnConnected { protocol: &super::BRPC_PROTOCOL, mode })),
        )?;
        Self::resource_pool()
            .acquire_with_init(|| Self { connection, protocol: &super::BRPC_PROTOCOL, activity })
            .map_err(|_| Error::ResourceExhausted)
    }

    pub(crate) fn try_acquire_activity(&self) -> Option<Option<ClientActivityGuard>> {
        match &self.activity {
            Some(activity) => ClientConnectionState::try_acquire(activity).map(Some),
            None => Some(None),
        }
    }

    pub(crate) fn try_retire(&self, now_ms: u64) -> bool {
        self.activity.as_ref().is_some_and(|activity| activity.try_retire(now_ms))
    }

    pub(crate) fn close(&self) {
        self.connection.close();
    }

    /// Issues one protobuf RPC over bRPC.
    ///
    /// This method is deliberately thin:
    /// 1. allocate and initialize a `CallContext`;
    /// 2. use the context resource id as the bRPC correlation id;
    /// 3. encode one bRPC request frame;
    /// 4. submit `(CallContext, frame)` to `TcpConnection`;
    /// 5. return a `CallToken` for the caller to await.
    ///
    /// The request frame must not be stored in `CallContext`. The frame belongs
    /// to the TCP write request and should be released as soon as the write
    /// path no longer needs it. Retry, if added later, should reserialize
    /// the request instead of keeping every in-flight request buffer alive.
    pub(crate) fn issue_rpc(
        &self,
        request: PreparedRequest,
        activity: Option<ClientActivityGuard>,
    ) -> Result<CallToken> {
        let profile_sampled = request.options.profile_sampled;
        let stage_start = profile::start_if(profile_sampled);
        let context = CallContext::resource_pool()
            .acquire_with_init(|| {
                CallContext::new_with_activity(
                    request.options.deadline,
                    request.options.profile_sampled,
                    activity,
                )
            })
            .map_err(|_| Error::ResourceExhausted)?;
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::ClientCallContextAlloc, start);
        }
        let correlation_id = context.id().as_u64();
        let stage_start = profile::start_if(profile_sampled);
        let frame = self.protocol.encode_request(
            RequestEncodeContext {
                method: request.method,
                options: &request.options,
                correlation_id,
            },
            request.payload,
            request.attachment,
        )?;
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::ClientFrameEncode, start);
        }

        let token = CallToken::new(context.clone());
        let stage_start = profile::start_if(profile_sampled);
        TcpConnection::submit_write(self.connection.clone(), frame, WriteOptions::client(context))?;
        if let Some(start) = stage_start {
            profile::record_elapsed(ProfileStage::ClientSubmitWrite, start);
        }
        Ok(token)
    }
}

struct BrpcOnConnected {
    protocol: &'static super::BrpcProtocol,
    mode: BrpcClientConnectionMode,
}

impl OnConnected for BrpcOnConnected {
    fn on_connected(&self, connection: Resource<TcpConnection>, socket: Resource<TcpSocket>) {
        let protocol = self.protocol;
        let mode = self.mode;
        tokio::spawn(async move { read_loop(connection, protocol, socket, mode).await });
    }
}

/// Reads and completes bRPC responses for one connected TCP socket.
///
/// The read loop decodes only bRPC protocol metadata. It must not decode the
/// typed protobuf response, because the reader has no response type and must
/// stay cheap enough to serve every call sharing this connection.
///
/// A valid response whose correlation id no longer maps to an active
/// `CallContext` is ignored. This covers cancellation, timeout, duplicate
/// responses, and responses racing with connection failure. Malformed bRPC
/// frames invalidate the socket.
async fn read_loop(
    connection: Resource<TcpConnection>,
    protocol: &'static super::BrpcProtocol,
    socket: Resource<TcpSocket>,
    mode: BrpcClientConnectionMode,
) {
    let mut input = IoBuf::new();
    let failure = loop {
        let decode_start = profile::enabled().then(Instant::now);
        match protocol.decode_response(&mut input) {
            Ok(Some(response)) => {
                complete_response(response, decode_start);
                if mode == BrpcClientConnectionMode::CloseAfterResponse {
                    connection.close();
                    return;
                }
            }
            Ok(None) => match socket.read_more(&mut input).await {
                Ok(0) => {
                    log::debug!("brpc peer closed connection: remote={}", connection.remote_addr());
                    break Status::new(StatusCode::Unavailable, "connection closed by peer");
                }
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        "brpc response read failed: remote={}, socket={}, error={error}",
                        connection.remote_addr(),
                        socket.id().as_u64()
                    );
                    break Status::new(StatusCode::Unavailable, error.to_string());
                }
            },
            Err(error) => {
                log::warn!(
                    "bRPC response decode failed: remote={}, error={error}",
                    connection.remote_addr()
                );
                break Status::from(error);
            }
        }
    };
    connection.invalidate(socket, failure);
}

fn complete_response(response: InboundResponse, decode_start: Option<Instant>) {
    let Some(context) = CallContext::resource_pool().get(call_id(response.correlation_id)) else {
        log::debug!("discarding late bRPC response: correlation_id={}", response.correlation_id);
        return;
    };
    let profile_sampled = context.profile_sampled();
    if profile_sampled && let Some(start) = decode_start {
        profile::record_elapsed(ProfileStage::ClientFrameDecode, start);
    }
    let complete_start = profile::start_if(profile_sampled);
    let _ = context.complete(response.payload, response.attachment, response.status);
    if let Some(start) = complete_start {
        profile::record_elapsed(ProfileStage::ClientCompleteContext, start);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BufMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::IoBufBuilder;
    use crate::descriptor::{Idempotency, MethodDescriptor, MethodKind};
    use crate::protocol::ResponseEncodeContext;

    static METHOD: MethodDescriptor = MethodDescriptor::new(
        "example.EchoService",
        "Echo",
        MethodKind::Unary,
        Idempotency::Idempotent,
    );

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn short_connection_closes_after_first_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let protocol = super::super::BrpcProtocol::default();
            let mut input = IoBuf::new();

            let request = loop {
                let mut bytes = [0; 1024];
                let read = stream.read(&mut bytes).await.unwrap();
                assert_ne!(read, 0, "client closed before sending the request");
                let mut chunk = iobuf_from_bytes(&bytes[..read]);
                input.append_movable(&mut chunk);
                if let Some(request) = protocol.decode_request(&mut input).unwrap() {
                    break request;
                }
            };

            let response = protocol
                .encode_response(
                    ResponseEncodeContext { correlation_id: request.correlation_id, status: None },
                    request.payload,
                    request.attachment,
                )
                .unwrap();
            write_all_iobuf(&mut stream, &response).await;

            let mut byte = [0; 1];
            let read =
                tokio::time::timeout(TEST_TIMEOUT, stream.read(&mut byte)).await.unwrap().unwrap();
            assert_eq!(read, 0, "short client must close after receiving the response");
        });

        let session = BrpcClientSession::create_short(address, None).unwrap();
        let token = session
            .issue_rpc(
                PreparedRequest {
                    method: &METHOD,
                    options: RequestOptions::default(),
                    payload: iobuf_from_bytes(b"payload"),
                    attachment: IoBuf::new(),
                },
                None,
            )
            .unwrap();

        let completion =
            tokio::time::timeout(TEST_TIMEOUT, std::future::poll_fn(|cx| token.poll(cx)))
                .await
                .unwrap();
        assert!(completion.status.is_none());
        assert_eq!(collect_iobuf(&completion.payload), b"payload");

        server.await.unwrap();
        assert!(session.connection.current_socket().is_none());
    }

    fn iobuf_from_bytes(bytes: &[u8]) -> IoBuf {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(bytes);
        builder.finish()
    }

    fn collect_iobuf(buffer: &IoBuf) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(buffer.len());
        for chunk in buffer.chunks() {
            bytes.extend_from_slice(chunk);
        }
        bytes
    }

    async fn write_all_iobuf(stream: &mut tokio::net::TcpStream, buffer: &IoBuf) {
        for chunk in buffer.chunks() {
            stream.write_all(chunk).await.unwrap();
        }
    }
}
