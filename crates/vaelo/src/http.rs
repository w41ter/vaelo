// SPDX-License-Identifier: Apache-2.0

//! HTTP/1.1 and HTTP/2 client API.
//!
//! HTTP deliberately uses the standard `http::Request`/`http::Response`
//! envelope instead of the typed RPC request model. Both HTTP versions share
//! this API; the client fixes the wire version and connection policy once,
//! while each call may override timeout, deadline and connection tag.

mod body;

use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub use ::http::{HeaderMap, Method, Request, Response, StatusCode, Uri};
pub use body::{Body, BodyExt, Collect, Empty, Frame, Full, Incoming, SizeHint, Stream};

use crate::client::http::HttpClient as RuntimeClient;
use crate::client::{Channel, ChannelOptions};
pub use crate::server::{HttpRpcMapping as RpcMapping, HttpServerOptions as ServerOptions};
use crate::{ConnectionMode, ConnectionTag, ObservationPolicy};

/// Server-side HTTP request information stored as one opaque extension.
///
/// Applications access it through ServerRequestExt; the representation of
/// route captures remains private so routers may use offsets or other
/// allocation-free forms later.
#[doc(hidden)]
#[derive(Clone)]
pub struct ServerRequestInfo {
    remote_addr: SocketAddr,
    path_parameters: Vec<(String, String)>,
}

impl ServerRequestInfo {
    pub(crate) fn new(remote_addr: SocketAddr, path_parameters: Vec<(String, String)>) -> Self {
        Self { remote_addr, path_parameters }
    }
}

/// Access to framework HTTP request context without replacing the standard
/// http::Request envelope.
pub trait ServerRequestExt {
    fn remote_addr(&self) -> Option<SocketAddr>;
    fn path_param(&self, name: &str) -> Option<&str>;
}

/// Stateful HTTP service whose concrete future and response body remain known
/// until the server registration boundary.
///
/// Unlike a static async closure, the returned future may borrow `&self`.
/// Implementations are not required to box the future or response body.
pub trait Service: Send + Sync + 'static {
    type ResponseBody: Body + Send + 'static;

    fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<Response<Self::ResponseBody>, Error>> + Send;
}

impl<B> ServerRequestExt for Request<B> {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.extensions().get::<ServerRequestInfo>().map(|info| info.remote_addr)
    }

    fn path_param(&self, name: &str) -> Option<&str> {
        self.extensions()
            .get::<ServerRequestInfo>()?
            .path_parameters
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }
}

/// HTTP wire version used by every connection owned by one client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Version {
    Http1,
    Http2,
}

/// HTTP request failure.
///
/// A non-success HTTP status is still a successful `Response`; this error is
/// reserved for invalid requests, body failures, protocol failures and I/O.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    InvalidRequest(String),
    Body(String),
    Protocol(String),
    Transport(String),
    ResourceExhausted,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(message) => write!(formatter, "invalid HTTP request: {message}"),
            Self::Body(message) => write!(formatter, "HTTP body error: {message}"),
            Self::Protocol(message) => write!(formatter, "HTTP protocol error: {message}"),
            Self::Transport(message) => write!(formatter, "HTTP transport error: {message}"),
            Self::ResourceExhausted => formatter.write_str("HTTP resources exhausted"),
        }
    }
}

impl std::error::Error for Error {}

/// A cloneable HTTP client with lazy physical connections.
#[derive(Clone)]
pub struct Client {
    runtime: RuntimeClient,
    remote: SocketAddr,
    version: Version,
    connection_mode: ConnectionMode,
    connection_tag: ConnectionTag,
}

impl Client {
    pub fn http1(remote: SocketAddr) -> ClientBuilder {
        ClientBuilder::new(remote, Version::Http1)
    }

    pub fn http2(remote: SocketAddr) -> ClientBuilder {
        ClientBuilder::new(remote, Version::Http2)
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote
    }

    pub fn connection_mode(&self) -> ConnectionMode {
        self.connection_mode
    }

    pub fn connection_tag(&self) -> ConnectionTag {
        self.connection_tag
    }

    /// Derives a logical client with another default connection tag.
    ///
    /// The tag remains part of the ordinary connection key; deriving a client
    /// does not allocate or create a physical connection.
    pub fn with_connection_tag(&self, tag: ConnectionTag) -> Self {
        let mut client = self.clone();
        client.connection_tag = tag;
        client
    }

    /// Creates an HTTP call without submitting it.
    pub fn request<B>(&self, request: Request<B>) -> HttpCall<B>
    where
        B: Body + Send + 'static,
    {
        HttpCall {
            client: self.clone(),
            request: Some(request),
            timeout: None,
            deadline: None,
            connection_tag: None,
        }
    }
}

pub struct ClientBuilder {
    remote: SocketAddr,
    version: Version,
    connection_mode: ConnectionMode,
    connection_tag: ConnectionTag,
    connect_timeout: Option<Duration>,
    observation: ObservationPolicy,
}

impl ClientBuilder {
    fn new(remote: SocketAddr, version: Version) -> Self {
        Self {
            remote,
            version,
            connection_mode: ConnectionMode::Single,
            connection_tag: ConnectionTag::DEFAULT,
            connect_timeout: None,
            observation: ObservationPolicy::Disabled,
        }
    }

    pub fn connection_mode(mut self, mode: ConnectionMode) -> Self {
        self.connection_mode = mode;
        self
    }

    pub fn connection_tag(mut self, tag: ConnectionTag) -> Self {
        self.connection_tag = tag;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn observation(mut self, policy: ObservationPolicy) -> Self {
        self.observation = policy;
        self
    }

    /// Builds a logical client without establishing a physical connection.
    pub fn build(self) -> Result<Client, Error> {
        if matches!(self.connection_mode, ConnectionMode::Pool { size: 0 }) {
            return Err(Error::InvalidRequest(
                "connection pool size must be greater than zero".into(),
            ));
        }
        Ok(Client {
            runtime: RuntimeClient::with_channel(
                Channel::with_options(
                    self.remote,
                    ChannelOptions {
                        protocol: match self.version {
                            Version::Http1 => crate::Protocol::Http1,
                            Version::Http2 => crate::Protocol::Http2,
                        },
                        connection_mode: self.connection_mode,
                        connection_tag: self.connection_tag,
                        connect_timeout: self.connect_timeout,
                        observation: self.observation,
                        grpc_options: crate::GrpcOptions::default(),
                    },
                )
                .map_err(map_runtime_error)?,
            )
            .map_err(map_runtime_error)?,
            remote: self.remote,
            version: self.version,
            connection_mode: self.connection_mode,
            connection_tag: self.connection_tag,
        })
    }
}

/// An HTTP request that has not been submitted.
///
/// Creating or dropping this value performs no I/O. Awaiting it is exactly
/// equivalent to calling [`start`](Self::start) and awaiting the returned
/// [`HttpCallHandle`].
#[must_use = "await the call, call start(), or explicitly drop it without sending"]
pub struct HttpCall<B> {
    client: Client,
    request: Option<Request<B>>,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    connection_tag: Option<ConnectionTag>,
}

impl<B> HttpCall<B> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self.deadline = None;
        self
    }

    pub fn deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self.timeout = None;
        self
    }

    pub fn connection_tag(mut self, tag: ConnectionTag) -> Self {
        self.connection_tag = Some(tag);
        self
    }
}

impl<B> HttpCall<B>
where
    B: Body + Send + 'static,
{
    /// Immediately hands this request to the HTTP runtime.
    ///
    /// Socket readiness, HTTP/2 stream capacity and bounded internal queuing
    /// are runtime concerns and are never exposed as `try_*` admission APIs.
    /// Any start, transport or response failure is observed by awaiting the
    /// returned handle. Body values are owned and static because the runtime
    /// may continue the request after the handle is dropped or detached; this
    /// does not require body frames to be contiguous or copied.
    pub fn start(self) -> HttpCallHandle {
        let deadline =
            self.deadline.or_else(|| self.timeout.map(|timeout| Instant::now() + timeout));
        let request = self.request.expect("HTTP call request is present until start");
        let (parts, body) = request.into_parts();
        match body.try_into_buffer() {
            Ok(body) => HttpCallHandle::active(self.client.runtime.send_with(
                Request::from_parts(parts, body),
                deadline,
                self.connection_tag,
            )),
            Err(body) => HttpCallHandle::active(self.client.runtime.send_streaming(
                parts,
                erase_body(body),
                deadline,
                self.connection_tag,
            )),
        }
    }
}

impl<B> IntoFuture for HttpCall<B>
where
    B: Body + Send + 'static,
{
    type Output = Result<Response<Incoming>, Error>;
    type IntoFuture = HttpCallHandle;

    fn into_future(self) -> Self::IntoFuture {
        self.start()
    }
}

/// Completion handle for an HTTP request already owned by the runtime.
#[must_use = "await the response, call detach(), or explicitly ignore the active request"]
pub struct HttpCallHandle {
    state: HttpCallState,
}

// The active call stays inline to avoid allocating for every HTTP request.
#[allow(clippy::large_enum_variant)]
enum HttpCallState {
    Active(crate::protocol::http_call::HttpCall),
    Finished,
}

pub(crate) trait ErasedBody {
    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Error>>>;
    fn size_hint(&self) -> SizeHint;
}

struct ErasedBodyImpl<B>(B);

impl<B> ErasedBody for ErasedBodyImpl<B>
where
    B: Body,
{
    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Error>>> {
        // SAFETY: `ErasedBodyImpl` is pinned together with B and B is never
        // moved again before it is dropped.
        let body = unsafe { self.map_unchecked_mut(|this| &mut this.0) };
        match body.poll_frame(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

fn erase_body<B>(body: B) -> Pin<Box<dyn ErasedBody + Send>>
where
    B: Body + Send + 'static,
{
    Box::pin(ErasedBodyImpl(body))
}

impl HttpCallHandle {
    fn active(call: crate::protocol::http_call::HttpCall) -> Self {
        Self { state: HttpCallState::Active(call) }
    }

    /// Requests best-effort cancellation. For HTTP/2 this should reset the
    /// stream; for HTTP/1 it may require closing a non-reusable connection.
    pub fn cancel(&self) {
        match &self.state {
            HttpCallState::Active(call) => call.cancel(),
            HttpCallState::Finished => {}
        }
    }

    pub fn detach(self) {
        drop(self);
    }
}

impl Future for HttpCallHandle {
    type Output = Result<Response<Incoming>, Error>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            HttpCallState::Active(call) => match Pin::new(call).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(response)) => {
                    self.state = HttpCallState::Finished;
                    Poll::Ready(Ok(response))
                }
                Poll::Ready(Err(error)) => {
                    self.state = HttpCallState::Finished;
                    Poll::Ready(Err(map_runtime_error(error)))
                }
            },
            HttpCallState::Finished => panic!("HttpCallHandle polled after completion"),
        }
    }
}

fn map_runtime_error(error: crate::Error) -> Error {
    match error {
        crate::Error::Encode(message) => Error::InvalidRequest(message.into_owned()),
        crate::Error::Decode(message) | crate::Error::Protocol(message) => {
            Error::Protocol(message.into_owned())
        }
        crate::Error::Channel(message) => Error::Transport(message.into_owned()),
        crate::Error::ResourceExhausted => Error::ResourceExhausted,
        crate::Error::Remote(status) => Error::Protocol(status.to_string()),
    }
}
