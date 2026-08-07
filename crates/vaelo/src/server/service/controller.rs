// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::client::ServerRequestInfo;
use crate::{IoBuf, MetadataMap, Request, RequestProtocol};

/// Mutable context exposed to one generated server method.
pub struct ServerController {
    remote_addr: SocketAddr,
    protocol: RequestProtocol,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
    request_metadata: MetadataMap,
    response_metadata: MetadataMap,
    trailing_metadata: MetadataMap,
    log_id: Option<u64>,
    request_id: Option<String>,
    request_attachment: IoBuf,
    response_attachment: IoBuf,
}

impl ServerController {
    pub(super) fn new(context: ServerRequestContext) -> Self {
        Self {
            remote_addr: context.remote_addr,
            protocol: context.protocol,
            deadline: context.deadline,
            cancellation: context.cancellation,
            request_metadata: context.metadata,
            response_metadata: MetadataMap::new(),
            trailing_metadata: MetadataMap::new(),
            log_id: context.log_id,
            request_id: context.request_id,
            request_attachment: context.attachment,
            response_attachment: IoBuf::new(),
        }
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn protocol(&self) -> RequestProtocol {
        self.protocol
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub fn metadata(&self) -> &MetadataMap {
        &self.request_metadata
    }

    pub fn response_metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.response_metadata
    }

    pub fn trailing_metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.trailing_metadata
    }

    pub fn log_id(&self) -> Option<u64> {
        self.log_id
    }

    /// Returns the BAIDU_STD request identifier, when supplied by the client.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub fn request_attachment(&self) -> &IoBuf {
        &self.request_attachment
    }

    pub fn take_request_attachment(&mut self) -> IoBuf {
        std::mem::take(&mut self.request_attachment)
    }

    pub fn set_response_attachment(&mut self, attachment: IoBuf) {
        self.response_attachment = attachment;
    }

    pub fn response_attachment(&self) -> &IoBuf {
        &self.response_attachment
    }

    /// Moves the transport context into the stable server request envelope.
    /// Generated service methods receive only this envelope; `ServerController`
    /// remains an internal compatibility adapter while implementations migrate.
    pub(super) fn into_request<T>(self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.options.deadline = self.deadline;
        request.options.metadata = self.request_metadata;
        request.options.log_id = self.log_id;
        request.options.request_id = self.request_id;
        request.attachment = self.request_attachment;
        request.server = Some(ServerRequestInfo {
            remote_addr: self.remote_addr,
            protocol: self.protocol,
            deadline: self.deadline,
            cancellation: self.cancellation,
        });
        request
    }
}

pub(crate) struct ServerRequestContext {
    pub remote_addr: SocketAddr,
    pub protocol: RequestProtocol,
    pub deadline: Option<Instant>,
    pub cancellation: CancellationToken,
    pub metadata: MetadataMap,
    pub log_id: Option<u64>,
    pub request_id: Option<String>,
    pub attachment: IoBuf,
}

impl ServerRequestContext {
    pub(super) fn brpc(
        remote_addr: SocketAddr,
        deadline: Option<Instant>,
        log_id: Option<u64>,
        request_id: Option<String>,
        attachment: IoBuf,
    ) -> Self {
        Self {
            remote_addr,
            protocol: RequestProtocol::Brpc,
            deadline,
            cancellation: CancellationToken::new(),
            metadata: MetadataMap::new(),
            log_id,
            request_id,
            attachment,
        }
    }

    pub(super) fn http(remote_addr: SocketAddr) -> Self {
        Self {
            remote_addr,
            protocol: RequestProtocol::HttpMapping,
            deadline: None,
            cancellation: CancellationToken::new(),
            metadata: MetadataMap::new(),
            log_id: None,
            request_id: None,
            attachment: IoBuf::new(),
        }
    }
}
