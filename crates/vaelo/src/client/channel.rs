// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::{BidiStreamingCall, ClientStreamingCall, Request, ServerStreamingCall, UnaryCall};
use crate::transport::connection::ConnectionKey;
use crate::{Decode, Encode, Error, MethodDescriptor, Protocol, Result, RpcProtocol};

/// Selects an independent local traffic class.
///
/// The tag is local routing state and is never transmitted to the peer. It is
/// not a process-global connection identity. Calls with different tags do not
/// share a physical connection.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct ConnectionTag(u64);

impl ConnectionTag {
    pub const DEFAULT: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Requested physical connection topology for an endpoint and traffic tag.
///
/// This does not define the implementation's connection-cache ownership,
/// worker topology or synchronization strategy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionMode {
    /// Use one connection for each endpoint, protocol and tag.
    #[default]
    Single,
    /// Spread calls over a fixed number of connections in the group.
    Pool { size: usize },
    /// Create one connection for each call.
    Short,
}

/// Optional per-call timing policy.
///
/// Aggregate counters need not depend on this policy. It controls detailed
/// clock reads exposed through CallStats and therefore remains optional on the
/// latency-sensitive path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObservationPolicy {
    #[default]
    Disabled,
    Latency,
    SampledLatency {
        one_in: NonZeroU64,
    },
}

/// Stable configuration captured when an [`RpcChannel`] is created.
#[derive(Clone, Debug)]
pub(crate) struct ChannelOptions {
    /// Application protocol bound to every physical connection used by this
    /// channel.
    pub protocol: Protocol,
    /// How calls select physical connections.
    pub connection_mode: ConnectionMode,
    /// Selects a local traffic class.
    pub connection_tag: ConnectionTag,
    pub connect_timeout: Option<Duration>,
    pub observation: ObservationPolicy,
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            protocol: Protocol::Brpc,
            connection_mode: ConnectionMode::Single,
            connection_tag: ConnectionTag::DEFAULT,
            connect_timeout: None,
            observation: ObservationPolicy::Disabled,
        }
    }
}

#[derive(Clone)]
pub struct RpcChannel {
    inner: Arc<ChannelInner>,
}

impl std::fmt::Debug for RpcChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RpcChannel")
            .field("remote", &self.inner.remote)
            .field("protocol", &self.protocol())
            .field("connection_mode", &self.inner.options.connection_mode)
            .field("connection_tag", &self.inner.options.connection_tag)
            .field("connect_timeout", &self.inner.options.connect_timeout)
            .finish()
    }
}

/// Compatibility name for the pre-stabilization API.
pub(crate) type Channel = RpcChannel;

/// Builder for an [`RpcChannel`].
pub struct RpcChannelBuilder {
    remote: SocketAddr,
    options: ChannelOptions,
}

impl RpcChannelBuilder {
    pub fn connection_mode(mut self, mode: ConnectionMode) -> Self {
        self.options.connection_mode = mode;
        self
    }

    pub fn connection_tag(mut self, tag: ConnectionTag) -> Self {
        self.options.connection_tag = tag;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.options.connect_timeout = Some(timeout);
        self
    }

    pub fn observation(mut self, policy: ObservationPolicy) -> Self {
        self.options.observation = policy;
        self
    }

    /// Builds a logical channel without performing network I/O.
    pub fn build(self) -> Result<RpcChannel> {
        RpcChannel::with_options(self.remote, self.options)
    }
}

struct ChannelInner {
    remote: SocketAddr,
    options: ChannelOptions,
    next_connection: AtomicU64,
}

impl RpcChannel {
    /// Starts a bRPC channel builder. No connection is opened by this method.
    pub fn brpc(remote: SocketAddr) -> RpcChannelBuilder {
        Self::rpc(remote, RpcProtocol::Brpc)
    }

    /// Starts a gRPC channel builder. No connection is opened by this method.
    pub fn grpc(remote: SocketAddr) -> RpcChannelBuilder {
        Self::rpc(remote, RpcProtocol::Grpc)
    }

    pub fn rpc(remote: SocketAddr, protocol: RpcProtocol) -> RpcChannelBuilder {
        RpcChannelBuilder {
            remote,
            options: ChannelOptions { protocol: protocol.into(), ..ChannelOptions::default() },
        }
    }

    /// Resolves a bRPC endpoint once and returns a lazy channel builder.
    pub async fn brpc_endpoint(
        endpoint: impl tokio::net::ToSocketAddrs,
    ) -> Result<RpcChannelBuilder> {
        Self::rpc_endpoint(endpoint, RpcProtocol::Brpc).await
    }

    /// Resolves a gRPC endpoint once and returns a lazy channel builder.
    pub async fn grpc_endpoint(
        endpoint: impl tokio::net::ToSocketAddrs,
    ) -> Result<RpcChannelBuilder> {
        Self::rpc_endpoint(endpoint, RpcProtocol::Grpc).await
    }

    /// Resolves an endpoint once and selects the first returned address.
    ///
    /// Name refresh remains an application/service-discovery responsibility;
    /// creating a new channel performs a new resolution. No TCP connection is
    /// opened until the first call is submitted.
    pub async fn rpc_endpoint(
        endpoint: impl tokio::net::ToSocketAddrs,
        protocol: RpcProtocol,
    ) -> Result<RpcChannelBuilder> {
        let mut addresses = tokio::net::lookup_host(endpoint)
            .await
            .map_err(|error| Error::Channel(format!("resolve RPC endpoint: {error}").into()))?;
        let remote = addresses
            .next()
            .ok_or_else(|| Error::Channel("RPC endpoint resolved to no addresses".into()))?;
        Ok(Self::rpc(remote, protocol))
    }

    /// Creates a single-server channel using the default bRPC protocol.
    #[cfg(test)]
    pub(crate) fn new(remote: SocketAddr) -> Self {
        Self::with_options(remote, ChannelOptions::default())
            .expect("default single-server channel options are valid")
    }

    pub(crate) fn with_options(remote: SocketAddr, options: ChannelOptions) -> Result<Self> {
        if matches!(options.connection_mode, ConnectionMode::Pool { size: 0 }) {
            return Err(Error::Channel("connection pool size must be greater than zero".into()));
        }
        Ok(Self {
            inner: Arc::new(ChannelInner { remote, options, next_connection: AtomicU64::new(0) }),
        })
    }

    /// Creates another logical channel which shares all configuration except
    /// the connection tag. Channels with different tags never share a physical
    /// connection.
    pub fn with_connection_tag(&self, connection_tag: ConnectionTag) -> Self {
        let mut options = self.inner.options.clone();
        options.connection_tag = connection_tag;
        Self {
            inner: Arc::new(ChannelInner {
                remote: self.inner.remote,
                options,
                next_connection: AtomicU64::new(0),
            }),
        }
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.inner.remote
    }

    pub(crate) fn options(&self) -> &ChannelOptions {
        &self.inner.options
    }

    pub fn protocol(&self) -> RpcProtocol {
        RpcProtocol::try_from(self.inner.options.protocol)
            .expect("RpcChannel cannot contain an HTTP protocol")
    }

    pub(crate) fn wire_protocol(&self) -> Protocol {
        self.inner.options.protocol
    }

    pub fn connection_tag(&self) -> ConnectionTag {
        self.inner.options.connection_tag
    }

    /// Removes this channel's default-tag connections from the process cache.
    ///
    /// Active calls retain their connection handles and may finish normally.
    /// A later call creates a fresh connection. Other logical channels using
    /// the same endpoint, protocol, tag and policy share the same cache entry
    /// and therefore observe the removal as well.
    pub fn close_cached_connections(&self) {
        let slots = match self.inner.options.connection_mode {
            ConnectionMode::Single => 1,
            ConnectionMode::Pool { size } => size,
            ConnectionMode::Short => 0,
        };
        for slot in 0..slots {
            let key = self.connection_key_for_slot(self.connection_tag(), slot as u32);
            match self.wire_protocol() {
                Protocol::Brpc => super::brpc::close_cached(&key),
                Protocol::Grpc => crate::protocol::grpc::client::close_cached(&key),
                Protocol::Http1 | Protocol::Http2 => unreachable!("RPC channel protocol"),
            }
        }
    }

    /// Creates an unstarted unary RPC.
    ///
    /// Awaiting the returned value is exactly equivalent to calling
    /// [`UnaryCall::start`] and awaiting the resulting handle. Merely creating
    /// and dropping it does not submit a request.
    pub fn call<RequestMessage, ResponseMessage>(
        &self,
        method: &'static MethodDescriptor,
        request: Request<RequestMessage>,
    ) -> UnaryCall<RequestMessage, ResponseMessage>
    where
        RequestMessage: Encode,
        ResponseMessage: Decode + Default,
    {
        UnaryCall::new(self.clone(), method, request)
    }

    pub fn server_streaming<RequestMessage, ResponseMessage>(
        &self,
        method: &'static MethodDescriptor,
        request: Request<RequestMessage>,
    ) -> ServerStreamingCall<RequestMessage, ResponseMessage> {
        ServerStreamingCall::new(self.clone(), method, request)
    }

    pub fn client_streaming<RequestMessage, ResponseMessage>(
        &self,
        method: &'static MethodDescriptor,
    ) -> ClientStreamingCall<RequestMessage, ResponseMessage> {
        ClientStreamingCall::new(self.clone(), method)
    }

    pub fn bidirectional_streaming<RequestMessage, ResponseMessage>(
        &self,
        method: &'static MethodDescriptor,
    ) -> BidiStreamingCall<RequestMessage, ResponseMessage> {
        BidiStreamingCall::new(self.clone(), method)
    }

    pub(crate) fn connection_slot(&self) -> u32 {
        match self.inner.options.connection_mode {
            ConnectionMode::Single | ConnectionMode::Short => 0,
            ConnectionMode::Pool { size } => {
                (self.inner.next_connection.fetch_add(1, Ordering::Relaxed) % size as u64) as u32
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn connection_key(&self) -> ConnectionKey {
        self.connection_key_with_tag(None)
    }

    /// Computes a key with an optional call-local tag override, without
    /// allocating another logical channel on the call-start path.
    pub(crate) fn connection_key_with_tag(
        &self,
        connection_tag: Option<ConnectionTag>,
    ) -> ConnectionKey {
        self.connection_key_for_slot(
            connection_tag.unwrap_or_else(|| self.connection_tag()),
            self.connection_slot(),
        )
    }

    fn connection_key_for_slot(&self, tag: ConnectionTag, slot: u32) -> ConnectionKey {
        ConnectionKey {
            remote: self.remote_addr(),
            tag: tag.get(),
            slot,
            connect_timeout: self.inner.options.connect_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_channels_share_configuration_but_not_connection_keys() {
        let remote = "127.0.0.1:8080".parse().unwrap();
        let base = Channel::new(remote);
        let tagged = base.with_connection_tag(ConnectionTag::new(7));

        assert_eq!(base.remote_addr(), tagged.remote_addr());
        assert_eq!(base.protocol(), tagged.protocol());
        assert_ne!(base.connection_key(), tagged.connection_key());
    }

    #[test]
    fn pooled_channel_cycles_through_fixed_global_slots() {
        let remote = "127.0.0.1:8080".parse().unwrap();
        let channel = Channel::with_options(
            remote,
            ChannelOptions {
                connection_mode: ConnectionMode::Pool { size: 2 },
                ..ChannelOptions::default()
            },
        )
        .unwrap();

        assert_eq!(channel.connection_key().slot, 0);
        assert_eq!(channel.connection_key().slot, 1);
        assert_eq!(channel.connection_key().slot, 0);
    }

    #[tokio::test]
    async fn endpoint_builder_resolves_host_and_preserves_connect_policy() {
        let channel = RpcChannel::grpc_endpoint(("localhost", 8080))
            .await
            .unwrap()
            .connect_timeout(Duration::from_millis(25))
            .build()
            .unwrap();

        assert_eq!(channel.remote_addr().port(), 8080);
        assert_eq!(channel.protocol(), RpcProtocol::Grpc);
        assert_eq!(channel.connection_key().connect_timeout, Some(Duration::from_millis(25)));
    }
}
