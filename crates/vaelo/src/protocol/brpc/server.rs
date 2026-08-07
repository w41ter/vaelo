// SPDX-License-Identifier: Apache-2.0

use super::BrpcProtocol;
use crate::error::{Error, Result};
use crate::protocol::{InboundRequest, ResponseEncodeContext};
use crate::runtime::profile::{self, ProfileStage};
use crate::server::ServerSessionContext;
use crate::transport::tcp::{TcpConnection, WriteOptions};
use crate::{IoBuf, Resource, StaticResource};

pub(super) fn new_session(
    connection: Resource<TcpConnection>,
    protocol: &'static BrpcProtocol,
    context: ServerSessionContext,
) -> Result<Resource<ServerSession>> {
    ServerSession::resource_pool()
        .acquire_with_init(|| ServerSession {
            connection,
            protocol,
            services: context.services,
            _lifecycle: context.lifecycle,
        })
        .map_err(|_| Error::ResourceExhausted)
}

/// bRPC server session for one accepted TCP connection.
///
/// bRPC does not require response FIFO ordering. Each request carries a
/// correlation id and each response echoes that id, so handler tasks may finish
/// and write responses out of order.
///
/// The session owns protocol parsing and service dispatch only. It does not own
/// TCP lifecycle, write queueing, or transport buffers beyond the active read
/// continuation.
#[derive(vaelo_derive::StaticResource)]
pub(crate) struct ServerSession {
    connection: Resource<TcpConnection>,
    protocol: &'static BrpcProtocol,
    services: crate::ServiceRegistry,
    _lifecycle: Option<crate::server::ServerConnectionGuard>,
}

impl ServerSession {
    /// Starts or resumes the bRPC reader.
    ///
    /// `input` contains bytes already read during protocol probing or by a
    /// previous reader task. At any time exactly one task owns this buffer;
    /// ownership is moved to the next reader before the current task invokes
    /// user service code.
    pub(crate) fn handle_read_event(self: Resource<Self>, input: IoBuf) {
        tokio::spawn(Self::read_next_request(self, input));
    }

    /// Reads until one complete bRPC request is available.
    ///
    /// After decoding a request, this task immediately publishes a successor
    /// reader with the remaining input. The current task then handles the
    /// decoded request locally. This keeps the socket reader moving while a
    /// service method awaits user code.
    async fn read_next_request(self: Resource<Self>, mut input: IoBuf) {
        loop {
            let decode_start = profile::enabled().then(std::time::Instant::now);
            match self.protocol.decode_request(&mut input) {
                Ok(Some(request)) => {
                    let profile_sampled = profile::sample_next();
                    if profile_sampled && let Some(start) = decode_start {
                        profile::record_elapsed(ProfileStage::ServerFrameDecode, start);
                    }
                    Self::handle_read_event(self.clone(), input);
                    self.handle_request(request, profile_sampled).await;
                    return;
                }
                Ok(None) => match self.connection.read_more(&mut input).await {
                    Ok(0) => {
                        log::debug!(
                            "server peer closed connection: remote={}",
                            self.connection.remote_addr()
                        );
                        self.connection.close();
                        return;
                    }
                    Err(_) => {
                        // read_more logs the underlying I/O error.
                        self.connection.close();
                        return;
                    }
                    Ok(_) => {}
                },
                Err(error) => {
                    log::warn!(
                        "bRPC request decode failed: remote={}, error={error}",
                        self.connection.remote_addr()
                    );
                    self.connection.close();
                    return;
                }
            }
        }
    }

    /// Invokes the registered service method and writes one bRPC response.
    ///
    /// The payload and attachment stay opaque at the bRPC layer. Typed protobuf
    /// decoding belongs to the registered service implementation, while this
    /// method only wraps the returned payload into a bRPC response frame.
    async fn handle_request(self: Resource<Self>, request: InboundRequest, profile_sampled: bool) {
        let correlation_id = request.correlation_id;
        let remote_addr = *self.connection.remote_addr();
        let dispatch_start = profile::start_if(profile_sampled);
        let call = self.services.start(remote_addr, request);
        if let Some(start) = dispatch_start {
            profile::record_elapsed(ProfileStage::ServerServiceDispatch, start);
        }
        let finish_start = profile::start_if(profile_sampled);
        let response = call.finish().await;
        if let Some(start) = finish_start {
            profile::record_elapsed(ProfileStage::ServerServiceFinish, start);
        }
        let encode_ctx = ResponseEncodeContext { correlation_id, status: response.status.as_ref() };
        let encode_start = profile::start_if(profile_sampled);
        let frame = match self.protocol.encode_response(
            encode_ctx,
            response.body.into_unary(),
            response.attachment,
        ) {
            Ok(frame) => frame,
            Err(error) => {
                log::error!(
                    "bRPC response encode failed: remote={}, correlation_id={correlation_id}, error={error}",
                    self.connection.remote_addr()
                );
                self.connection.close();
                return;
            }
        };
        if let Some(start) = encode_start {
            profile::record_elapsed(ProfileStage::ServerFrameEncode, start);
        }

        let submit_start = profile::start_if(profile_sampled);
        if let Err(error) = TcpConnection::submit_write(
            self.connection.clone(),
            frame,
            WriteOptions::server().profile_sampled(profile_sampled),
        ) {
            log::error!(
                "bRPC response submission failed: remote={}, correlation_id={correlation_id}, error={error}",
                self.connection.remote_addr()
            );
            self.connection.close();
        }
        if let Some(start) = submit_start {
            profile::record_elapsed(ProfileStage::ServerSubmitWrite, start);
        }
    }
}
