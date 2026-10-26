// SPDX-License-Identifier: Apache-2.0

//! Dynamic RPC endpoint discovery.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use crossbeam_epoch::{self as epoch, Atomic, Owned};
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::{Stream, StreamExt};

use crate::{Status, StatusCode};

/// Logical name understood by a [`NamingService`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ServiceName(Box<str>);

impl ServiceName {
    pub fn new(name: impl Into<Box<str>>) -> Result<Self, NamingError> {
        let name = name.into();
        if name.is_empty() {
            return Err(NamingError::new("service name must not be empty"));
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identity for one service instance lifetime.
///
/// A naming implementation must assign a different id when an address is
/// reused by a new instance. This prevents a new instance from accidentally
/// inheriting an old instance's cached connection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct EndpointId(u64);

impl EndpointId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One dialable RPC service instance.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Endpoint {
    id: EndpointId,
    address: SocketAddr,
}

impl Endpoint {
    pub const fn new(id: EndpointId, address: SocketAddr) -> Self {
        Self { id, address }
    }

    pub const fn id(self) -> EndpointId {
        self.id
    }

    pub const fn address(self) -> SocketAddr {
        self.address
    }
}

/// A complete, ordered endpoint view and its validity period.
///
/// An empty snapshot is a successful discovery result with no available
/// endpoints. A discovery error is different: the channel keeps the last
/// successful snapshot until its TTL expires. Permanent snapshots remain valid
/// until another snapshot replaces them.
#[derive(Clone, Debug)]
pub struct EndpointSnapshot {
    endpoints: Box<[Endpoint]>,
    ttl: Option<Duration>,
}

impl EndpointSnapshot {
    pub fn new(
        endpoints: impl IntoIterator<Item = Endpoint>,
        ttl: Duration,
    ) -> Result<Self, NamingError> {
        let endpoints: Box<[Endpoint]> = endpoints.into_iter().collect();
        Self::from_parts(endpoints, Some(ttl))
    }

    /// Creates a snapshot that remains valid until another snapshot replaces
    /// it.
    pub fn permanent(endpoints: impl IntoIterator<Item = Endpoint>) -> Result<Self, NamingError> {
        let endpoints: Box<[Endpoint]> = endpoints.into_iter().collect();
        Self::from_parts(endpoints, None)
    }

    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.ttl
    }

    fn from_parts(endpoints: Box<[Endpoint]>, ttl: Option<Duration>) -> Result<Self, NamingError> {
        let mut ids = HashSet::with_capacity(endpoints.len());
        if endpoints.iter().any(|endpoint| !ids.insert(endpoint.id)) {
            return Err(NamingError::new("endpoint snapshot contains duplicate ids"));
        }
        Ok(Self { endpoints, ttl })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamingError {
    message: Cow<'static, str>,
}

impl NamingError {
    pub fn new(message: impl Into<Cow<'static, str>>) -> Self {
        Self { message: message.into() }
    }
}

impl fmt::Display for NamingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NamingError {}

pub type NamingWatch =
    Pin<Box<dyn Stream<Item = Result<EndpointSnapshot, NamingError>> + Send + 'static>>;

/// Produces complete endpoint snapshots for one logical service.
///
/// `watch` must return promptly. Network I/O is performed while the returned
/// stream is polled. A transient failure should be yielded as an error item;
/// the stream should remain alive so it can later publish a fresh snapshot.
/// Implementations must support independent, potentially concurrent watches.
pub trait NamingService: Send + Sync + 'static {
    fn watch(&self, service: ServiceName) -> NamingWatch;
}

const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const DNS_SNAPSHOT_TTL: Duration = Duration::from_secs(60);

struct DnsNamingService;

impl NamingService for DnsNamingService {
    fn watch(&self, service: ServiceName) -> NamingWatch {
        let mut interval = tokio::time::interval(DNS_REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let service = Arc::new(service);
        Box::pin(IntervalStream::new(interval).then(move |_| {
            let service = service.clone();
            async move { resolve_dns(&service).await }
        }))
    }
}

async fn resolve_dns(service: &ServiceName) -> Result<EndpointSnapshot, NamingError> {
    let mut addresses: Vec<_> = tokio::net::lookup_host(service.as_str())
        .await
        .map_err(|error| NamingError::new(format!("resolve DNS target {service}: {error}")))?
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(NamingError::new(format!("DNS target {service} resolved to no endpoints")));
    }
    EndpointSnapshot::new(
        addresses.into_iter().map(|address| Endpoint::new(endpoint_id(address), address)),
        DNS_SNAPSHOT_TTL,
    )
}

pub(crate) fn resolve_list(service: &ServiceName) -> Result<EndpointSnapshot, NamingError> {
    let mut addresses = Vec::new();
    for value in service.as_str().split(',') {
        let value = value.trim();
        if value.is_empty() {
            return Err(NamingError::new("list target contains an empty endpoint"));
        }
        addresses.push(value.parse::<SocketAddr>().map_err(|error| {
            NamingError::new(format!("parse list endpoint {value:?}: {error}"))
        })?);
    }
    addresses.sort_unstable();
    addresses.dedup();
    EndpointSnapshot::permanent(
        addresses.into_iter().map(|address| Endpoint::new(endpoint_id(address), address)),
    )
}

fn endpoint_id(address: SocketAddr) -> EndpointId {
    let mut hasher = DefaultHasher::new();
    address.hash(&mut hasher);
    EndpointId::new(hasher.finish())
}

type NamingServices = HashMap<Box<str>, Arc<dyn NamingService>>;

static NAMING_SERVICES: LazyLock<RwLock<NamingServices>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Registers the naming implementation selected by `scheme://address` targets.
///
/// Schemes are ASCII case-insensitive and can be registered only once. The
/// registry is consulted when a channel is built and is never touched by the
/// RPC request path. `dns` and `list` are reserved built-in schemes.
pub fn register_naming_service(
    scheme: &str,
    service: Arc<dyn NamingService>,
) -> Result<(), NamingError> {
    let scheme = normalize_scheme(scheme)?;
    if matches!(scheme.as_ref(), "dns" | "list") {
        return Err(NamingError::new(format!("naming scheme {scheme} is reserved")));
    }
    let mut services = NAMING_SERVICES
        .write()
        .map_err(|_| NamingError::new("naming service registry is poisoned"))?;
    if services.contains_key(scheme.as_ref()) {
        return Err(NamingError::new(format!("naming scheme {scheme} is already registered")));
    }
    services.insert(scheme, service);
    Ok(())
}

pub(crate) fn resolve_naming_service(scheme: &str) -> Result<Arc<dyn NamingService>, NamingError> {
    let scheme = normalize_scheme(scheme)?;
    if scheme.as_ref() == "dns" {
        return Ok(Arc::new(DnsNamingService));
    }
    NAMING_SERVICES
        .read()
        .map_err(|_| NamingError::new("naming service registry is poisoned"))?
        .get(scheme.as_ref())
        .cloned()
        .ok_or_else(|| NamingError::new(format!("naming scheme {scheme} is not registered")))
}

fn normalize_scheme(scheme: &str) -> Result<Box<str>, NamingError> {
    let valid = !scheme.is_empty()
        && scheme.as_bytes()[0].is_ascii_alphabetic()
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'));
    if !valid {
        return Err(NamingError::new(format!("invalid naming scheme {scheme:?}")));
    }
    Ok(scheme.to_ascii_lowercase().into_boxed_str())
}

struct RouteSnapshot {
    endpoints: Box<[Endpoint]>,
}

/// Read-mostly endpoint table. Updates publish a complete discovery snapshot;
/// picks pin one immutable table and perform a single relaxed increment.
pub(crate) struct DynamicRoute {
    snapshot: Atomic<RouteSnapshot>,
    next: AtomicU64,
}

impl DynamicRoute {
    pub(crate) fn new() -> Self {
        Self {
            snapshot: Atomic::new(RouteSnapshot { endpoints: Box::new([]) }),
            next: AtomicU64::new(0),
        }
    }

    pub(crate) fn publish(&self, snapshot: EndpointSnapshot) {
        self.replace(snapshot.endpoints);
    }

    pub(crate) fn expire(&self) {
        self.replace(Box::new([]));
    }

    pub(crate) fn pick(&self, service: &ServiceName) -> Result<Endpoint, Status> {
        let guard = epoch::pin();
        let snapshot = self.snapshot.load(Ordering::Acquire, &guard);
        let snapshot = unsafe { snapshot.deref() };
        if snapshot.endpoints.is_empty() {
            return Err(Status::new(
                StatusCode::Unavailable,
                format!("service {service} has no resolved endpoints"),
            ));
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) % snapshot.endpoints.len() as u64;
        Ok(snapshot.endpoints[index as usize])
    }

    pub(crate) fn visit(&self, mut visitor: impl FnMut(Endpoint)) {
        let guard = epoch::pin();
        let snapshot = self.snapshot.load(Ordering::Acquire, &guard);
        let snapshot = unsafe { snapshot.deref() };
        for endpoint in &snapshot.endpoints {
            visitor(*endpoint);
        }
    }

    fn replace(&self, endpoints: Box<[Endpoint]>) {
        let guard = epoch::pin();
        let old =
            self.snapshot.swap(Owned::new(RouteSnapshot { endpoints }), Ordering::AcqRel, &guard);
        unsafe { guard.defer_destroy(old) };
    }
}

impl Drop for DynamicRoute {
    fn drop(&mut self) {
        let snapshot = std::mem::replace(&mut self.snapshot, Atomic::null());
        unsafe {
            if let Some(snapshot) = snapshot.try_into_owned() {
                drop(snapshot);
            }
        }
    }
}
