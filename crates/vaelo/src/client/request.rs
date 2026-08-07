// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::request_options::RequestOptions;
use crate::{IoBuf, MetadataMap, RequestProtocol};

pub(crate) struct ServerRequestInfo {
    pub(crate) remote_addr: SocketAddr,
    pub(crate) protocol: RequestProtocol,
    pub(crate) deadline: Option<Instant>,
    pub(crate) cancellation: CancellationToken,
}

/// Protocol-neutral context which can outlive the request message.
///
/// The representation is intentionally opaque. It does not expose the
/// runtime's cancellation token, task model, or call-state ownership. Moving
/// a request into parts transfers this context without requiring callers to
/// clone metadata or protocol extensions.
pub struct RequestContext {
    metadata: MetadataMap,
    remote_addr: Option<SocketAddr>,
    protocol: Option<RequestProtocol>,
    deadline: Option<Instant>,
    cancellation: Option<CancellationToken>,
    pub(crate) attachment: IoBuf,
    pub(crate) request_id: Option<String>,
    pub(crate) log_id: Option<u64>,
}

impl RequestContext {
    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.metadata
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    pub fn protocol(&self) -> Option<RequestProtocol> {
        self.protocol
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.as_ref().is_some_and(CancellationToken::is_cancelled)
    }

    /// Waits for cancellation of a server-side invocation.
    ///
    /// A context created by a client request has no server cancellation
    /// source and therefore waits forever.
    pub async fn cancelled(&self) {
        match &self.cancellation {
            Some(cancellation) => cancellation.cancelled().await,
            None => std::future::pending().await,
        }
    }
}

/// An owned RPC message and its protocol-neutral metadata.
///
/// Timeouts, deadlines and connection selection belong to one invocation and
/// are configured on [`crate::UnaryCall`], not on the message. Protocol-only
/// fields are introduced by wrappers such as [`crate::brpc::Request`].
pub struct Request<T> {
    message: T,
    pub(crate) options: RequestOptions,
    pub(crate) attachment: IoBuf,
    pub(crate) server: Option<ServerRequestInfo>,
}

impl<T> Request<T> {
    pub fn new(message: T) -> Self {
        Self { message, options: RequestOptions::default(), attachment: IoBuf::new(), server: None }
    }

    pub fn get_ref(&self) -> &T {
        &self.message
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.message
    }

    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.options.metadata
    }

    pub fn metadata(&self) -> &MetadataMap {
        &self.options.metadata
    }

    /// Returns the peer address for a request delivered to a server method.
    /// Client-created requests return `None` until they reach a server.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(|context| context.remote_addr)
    }

    /// Returns the protocol through which a server received this request.
    pub fn protocol(&self) -> Option<RequestProtocol> {
        self.server.as_ref().map(|context| context.protocol)
    }

    /// Returns the absolute deadline visible to the server, when present.
    pub fn deadline(&self) -> Option<Instant> {
        self.server.as_ref().and_then(|context| context.deadline)
    }

    /// Reports whether the server-side call has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.server.as_ref().is_some_and(|context| context.cancellation.is_cancelled())
    }

    /// Waits for server-side cancellation.
    ///
    /// This method is intended for requests delivered to service handlers.
    /// Calling it on a client-created request waits forever.
    pub async fn cancelled(&self) {
        match &self.server {
            Some(context) => context.cancellation.cancelled().await,
            None => std::future::pending().await,
        }
    }

    pub fn into_inner(self) -> T {
        self.message
    }

    /// Separates the owned message from invocation context.
    ///
    /// This is the intended server-side pattern when the business future must
    /// move the message while independently observing cancellation or the
    /// deadline.
    pub fn into_parts(self) -> (RequestContext, T) {
        let Self { message, options, attachment, server } = self;
        let (remote_addr, protocol, server_deadline, cancellation) = match server {
            Some(server) => (
                Some(server.remote_addr),
                Some(server.protocol),
                server.deadline,
                Some(server.cancellation),
            ),
            None => (None, None, None, None),
        };
        (
            RequestContext {
                metadata: options.metadata,
                remote_addr,
                protocol,
                deadline: server_deadline.or(options.deadline),
                cancellation,
                attachment,
                request_id: options.request_id,
                log_id: options.log_id,
            },
            message,
        )
    }

    pub(crate) fn into_wire_parts(self) -> (T, RequestOptions, IoBuf) {
        (self.message, self.options, self.attachment)
    }
}

impl<T> From<T> for Request<T> {
    fn from(message: T) -> Self {
        Self::new(message)
    }
}

/// Conversion accepted by generated RPC client methods.
///
/// The canonical output is always [`Request<T>`], so generated calls retain a
/// concrete request type and do not require allocation or dynamic dispatch.
pub trait IntoRequest<T> {
    fn into_request(self) -> Request<T>;
}

impl<T> IntoRequest<T> for T {
    fn into_request(self) -> Request<T> {
        Request::new(self)
    }
}

impl<T> IntoRequest<T> for Request<T> {
    fn into_request(self) -> Request<T> {
        self
    }
}
