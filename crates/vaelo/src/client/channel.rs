// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use super::naming::{
    DynamicRoute, Endpoint, EndpointId, NamingService, ServiceName, resolve_list,
    resolve_naming_service,
};
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
            .field("remote", &self.remote_addr())
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
    target: ChannelTarget,
    options: ChannelOptions,
}

enum ChannelTarget {
    Static(SocketAddr),
    Uri(Box<str>),
}

/// A fixed endpoint or a `scheme://address` naming target.
pub struct RpcTarget(RpcTargetKind);

enum RpcTargetKind {
    Endpoint(SocketAddr),
    Naming(Box<str>),
}

impl From<SocketAddr> for RpcTarget {
    fn from(endpoint: SocketAddr) -> Self {
        Self(RpcTargetKind::Endpoint(endpoint))
    }
}

impl From<&str> for RpcTarget {
    fn from(target: &str) -> Self {
        Self(RpcTargetKind::Naming(target.into()))
    }
}

impl From<String> for RpcTarget {
    fn from(target: String) -> Self {
        Self(RpcTargetKind::Naming(target.into_boxed_str()))
    }
}

impl From<Box<str>> for RpcTarget {
    fn from(target: Box<str>) -> Self {
        Self(RpcTargetKind::Naming(target))
    }
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

    /// Builds a logical channel.
    ///
    /// Fixed-address and `list://` channels perform no background work. A
    /// refreshing naming channel starts its watch but opens no RPC connection.
    pub fn build(self) -> Result<RpcChannel> {
        match self.target {
            ChannelTarget::Static(remote) => RpcChannel::with_options(remote, self.options),
            ChannelTarget::Uri(target) => RpcChannel::with_uri_options(&target, self.options),
        }
    }
}

struct ChannelInner {
    target: Route,
    options: ChannelOptions,
    next_connection: AtomicU64,
}

#[derive(Clone)]
enum Route {
    Static(Endpoint),
    Dynamic(Arc<DynamicTarget>),
}

struct DynamicTarget {
    service: ServiceName,
    route: DynamicRoute,
    cancel: CancellationToken,
}

impl Drop for DynamicTarget {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl RpcChannel {
    /// Starts a bRPC channel builder for an endpoint or `scheme://address`.
    ///
    /// `dns://` and `list://` are built in; other schemes select a registered
    /// naming service. No connection is opened by this method.
    pub fn brpc(target: impl Into<RpcTarget>) -> RpcChannelBuilder {
        Self::rpc(target, RpcProtocol::Brpc)
    }

    /// Starts a gRPC channel builder for an endpoint or `scheme://address`.
    ///
    /// `dns://` and `list://` are built in; other schemes select a registered
    /// naming service. No connection is opened by this method.
    pub fn grpc(target: impl Into<RpcTarget>) -> RpcChannelBuilder {
        Self::rpc(target, RpcProtocol::Grpc)
    }

    pub fn rpc(target: impl Into<RpcTarget>, protocol: RpcProtocol) -> RpcChannelBuilder {
        let target = match target.into().0 {
            RpcTargetKind::Endpoint(endpoint) => ChannelTarget::Static(endpoint),
            RpcTargetKind::Naming(target) => ChannelTarget::Uri(target),
        };
        RpcChannelBuilder {
            target,
            options: ChannelOptions { protocol: protocol.into(), ..ChannelOptions::default() },
        }
    }

    /// Creates a single-server channel using the default bRPC protocol.
    #[cfg(test)]
    pub(crate) fn new(remote: SocketAddr) -> Self {
        Self::with_options(remote, ChannelOptions::default())
            .expect("default single-server channel options are valid")
    }

    pub(crate) fn with_options(remote: SocketAddr, options: ChannelOptions) -> Result<Self> {
        validate_options(&options)?;
        Ok(Self {
            inner: Arc::new(ChannelInner {
                target: Route::Static(Endpoint::new(EndpointId::new(0), remote)),
                options,
                next_connection: AtomicU64::new(0),
            }),
        })
    }

    fn with_uri_options(target: &str, options: ChannelOptions) -> Result<Self> {
        let (scheme, address) = target
            .split_once("://")
            .ok_or_else(|| Error::Channel("RPC target must use scheme://address syntax".into()))?;
        if address.is_empty() {
            return Err(Error::Channel("RPC target address must not be empty".into()));
        }
        validate_options(&options)?;
        let service =
            ServiceName::new(address).map_err(|error| Error::Channel(error.to_string().into()))?;
        if scheme.eq_ignore_ascii_case("list") {
            let snapshot =
                resolve_list(&service).map_err(|error| Error::Channel(error.to_string().into()))?;
            let target = Arc::new(DynamicTarget {
                service,
                route: DynamicRoute::new(),
                cancel: CancellationToken::new(),
            });
            target.route.publish(snapshot);
            return Ok(Self::from_dynamic_target(target, options));
        }
        let naming = resolve_naming_service(scheme)
            .map_err(|error| Error::Channel(error.to_string().into()))?;
        let handle = tokio::runtime::Handle::try_current().map_err(|error| {
            Error::Channel(format!("dynamic RPC channel requires a Tokio runtime: {error}").into())
        })?;
        let target = Arc::new(DynamicTarget {
            service: service.clone(),
            route: DynamicRoute::new(),
            cancel: CancellationToken::new(),
        });
        handle.spawn(run_naming_watch(
            Arc::downgrade(&target),
            target.cancel.clone(),
            naming,
            service,
        ));
        Ok(Self::from_dynamic_target(target, options))
    }

    fn from_dynamic_target(target: Arc<DynamicTarget>, options: ChannelOptions) -> Self {
        Self {
            inner: Arc::new(ChannelInner {
                target: Route::Dynamic(target),
                options,
                next_connection: AtomicU64::new(0),
            }),
        }
    }

    /// Creates another logical channel which shares all configuration except
    /// the connection tag. Channels with different tags never share a physical
    /// connection.
    pub fn with_connection_tag(&self, connection_tag: ConnectionTag) -> Self {
        let mut options = self.inner.options.clone();
        options.connection_tag = connection_tag;
        Self {
            inner: Arc::new(ChannelInner {
                target: self.inner.target.clone(),
                options,
                next_connection: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the fixed peer address, or `None` for a service-discovery
    /// channel.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        match &self.inner.target {
            Route::Static(endpoint) => Some(endpoint.address()),
            Route::Dynamic(_) => None,
        }
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
        self.visit_endpoints(|endpoint| {
            for slot in 0..slots {
                let key =
                    self.connection_key_for_slot(endpoint, self.connection_tag(), slot as u32);
                match self.wire_protocol() {
                    Protocol::Brpc => super::brpc::close_cached(&key),
                    Protocol::Grpc => crate::protocol::grpc::client::close_cached(&key),
                    Protocol::Http1 | Protocol::Http2 => unreachable!("RPC channel protocol"),
                }
            }
        });
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
        let endpoint = self.pick_endpoint().expect("test channel has an endpoint");
        self.connection_key_with_tag(endpoint, None)
    }

    /// Computes a key with an optional call-local tag override, without
    /// allocating another logical channel on the call-start path.
    pub(crate) fn connection_key_with_tag(
        &self,
        endpoint: Endpoint,
        connection_tag: Option<ConnectionTag>,
    ) -> ConnectionKey {
        self.connection_key_for_slot(
            endpoint,
            connection_tag.unwrap_or_else(|| self.connection_tag()),
            self.connection_slot(),
        )
    }

    fn connection_key_for_slot(
        &self,
        endpoint: Endpoint,
        tag: ConnectionTag,
        slot: u32,
    ) -> ConnectionKey {
        ConnectionKey {
            remote: endpoint.address(),
            endpoint_id: endpoint.id().get(),
            tag: tag.get(),
            slot,
            connect_timeout: self.inner.options.connect_timeout,
        }
    }

    pub(crate) fn pick_endpoint(&self) -> Result<Endpoint, crate::Status> {
        match &self.inner.target {
            Route::Static(endpoint) => Ok(*endpoint),
            Route::Dynamic(target) => target.route.pick(&target.service),
        }
    }

    pub(crate) fn static_endpoint(&self) -> Endpoint {
        match &self.inner.target {
            Route::Static(endpoint) => *endpoint,
            Route::Dynamic(_) => panic!("HTTP clients cannot use a dynamic RPC route"),
        }
    }

    fn visit_endpoints(&self, mut visitor: impl FnMut(Endpoint)) {
        match &self.inner.target {
            Route::Static(endpoint) => visitor(*endpoint),
            Route::Dynamic(target) => target.route.visit(visitor),
        }
    }
}

fn validate_options(options: &ChannelOptions) -> Result<()> {
    if matches!(options.connection_mode, ConnectionMode::Pool { size: 0 }) {
        return Err(Error::Channel("connection pool size must be greater than zero".into()));
    }
    Ok(())
}

async fn run_naming_watch(
    target: Weak<DynamicTarget>,
    cancel: CancellationToken,
    naming: Arc<dyn NamingService>,
    service: ServiceName,
) {
    let mut watch = naming.watch(service.clone());
    let mut expiry = Box::pin(tokio::time::sleep(Duration::ZERO));
    let mut expiry_active = false;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = &mut expiry, if expiry_active => {
                let Some(target) = target.upgrade() else { return };
                target.route.expire();
                expiry_active = false;
            }
            update = watch.next() => {
                match update {
                    Some(Ok(snapshot)) => {
                        let Some(target) = target.upgrade() else { return };
                        let ttl = snapshot.ttl();
                        target.route.publish(snapshot);
                        if let Some(ttl) = ttl {
                            expiry.as_mut().reset(tokio::time::Instant::now() + ttl);
                            expiry_active = true;
                        } else {
                            expiry_active = false;
                        }
                    }
                    Some(Err(error)) => {
                        log::warn!("naming update failed: service={}, error={error}", service);
                    }
                    None => {
                        log::warn!("naming watch ended: service={service}");
                        if expiry_active {
                            tokio::select! {
                                _ = cancel.cancelled() => {}
                                _ = &mut expiry => {
                                    if let Some(target) = target.upgrade() {
                                        target.route.expire();
                                    }
                                }
                            }
                        }
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{EndpointSnapshot, NamingError, NamingWatch, StatusCode};

    struct TestNaming {
        receiver: Mutex<Option<mpsc::Receiver<Result<EndpointSnapshot, NamingError>>>>,
    }

    impl NamingService for TestNaming {
        fn watch(&self, _service: ServiceName) -> NamingWatch {
            let receiver = self.receiver.lock().unwrap().take().unwrap();
            Box::pin(ReceiverStream::new(receiver))
        }
    }

    fn test_endpoint(id: u64, port: u16) -> Endpoint {
        Endpoint::new(EndpointId::new(id), ([127, 0, 0, 1], port).into())
    }

    async fn wait_for_endpoint(channel: &RpcChannel) -> Endpoint {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(endpoint) = channel.pick_endpoint() {
                    return endpoint;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

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

    #[test]
    fn dynamic_route_uses_round_robin_without_resetting_on_update() {
        let route = DynamicRoute::new();
        let service = ServiceName::new("echo").unwrap();
        route.publish(
            EndpointSnapshot::new(
                [test_endpoint(1, 8001), test_endpoint(2, 8002)],
                Duration::from_secs(1),
            )
            .unwrap(),
        );

        assert_eq!(route.pick(&service).unwrap().id(), EndpointId::new(1));
        assert_eq!(route.pick(&service).unwrap().id(), EndpointId::new(2));
        assert_eq!(route.pick(&service).unwrap().id(), EndpointId::new(1));

        route.publish(
            EndpointSnapshot::new(
                [test_endpoint(1, 8001), test_endpoint(2, 8002)],
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        assert_eq!(route.pick(&service).unwrap().id(), EndpointId::new(2));
    }

    #[tokio::test]
    async fn dynamic_channel_fails_fast_then_keeps_snapshot_until_ttl() {
        let (sender, receiver) = mpsc::channel(4);
        let naming = Arc::new(TestNaming { receiver: Mutex::new(Some(receiver)) });
        crate::register_naming_service("test-channel", naming).unwrap();
        let channel = RpcChannel::brpc("test-channel://echo").build().unwrap();

        let initial = channel.pick_endpoint().unwrap_err();
        assert_eq!(initial.code(), StatusCode::Unavailable);

        sender
            .send(Ok(
                EndpointSnapshot::new([test_endpoint(7, 8007)], Duration::from_millis(80)).unwrap()
            ))
            .await
            .unwrap();
        wait_for_endpoint(&channel).await;

        sender.send(Err(NamingError::new("temporary failure"))).await.unwrap();
        assert_eq!(channel.pick_endpoint().unwrap().id(), EndpointId::new(7));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(channel.pick_endpoint().unwrap_err().code(), StatusCode::Unavailable);
    }

    #[test]
    fn endpoint_identity_separates_cached_connection_keys() {
        let channel = Channel::new("127.0.0.1:8080".parse().unwrap());
        let first = channel.connection_key_with_tag(test_endpoint(1, 8080), None);
        let second = channel.connection_key_with_tag(test_endpoint(2, 8080), None);
        assert_ne!(first, second);
    }

    #[test]
    fn endpoint_builds_a_fixed_channel() {
        let endpoint: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let channel = RpcChannel::grpc(endpoint).build().unwrap();
        assert_eq!(channel.remote_addr(), Some(endpoint));
    }

    #[test]
    fn list_target_is_permanent_and_round_robins_endpoints() {
        let channel = RpcChannel::grpc("list://127.0.0.1:8001,127.0.0.1:8002").build().unwrap();
        assert_eq!(channel.pick_endpoint().unwrap().address().port(), 8001);
        assert_eq!(channel.pick_endpoint().unwrap().address().port(), 8002);
        assert_eq!(channel.pick_endpoint().unwrap().address().port(), 8001);
    }

    #[test]
    fn invalid_list_target_fails_during_build() {
        let error = RpcChannel::grpc("list://127.0.0.1:8001,").build().unwrap_err();
        assert!(error.to_string().contains("empty endpoint"));
    }

    #[tokio::test]
    async fn dns_target_resolves_all_addresses() {
        let channel = RpcChannel::grpc("dns://localhost:8080").build().unwrap();
        assert_eq!(wait_for_endpoint(&channel).await.address().port(), 8080);
    }
}
