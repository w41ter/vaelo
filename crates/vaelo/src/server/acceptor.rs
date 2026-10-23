// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpSocket, TcpStream, ToSocketAddrs};
use tokio_util::sync::CancellationToken;

use super::{IntoServiceRegistry, ServiceRegistry};
use crate::protocol::{ProbeSelection, ProtocolSet, probe_allowed};
use crate::transport::tcp::TcpConnection;
use crate::{IoBuf, Resource, Result};

/// Transport information handed from one listener to its Acceptor. The
/// Acceptor owns admission, protocol set, and server-connection construction;
/// the listener knows none of those policies.
pub struct AcceptedTcpSocket {
    pub stream: TcpStream,
    pub local_addr: SocketAddr,
    pub remote_addr: SocketAddr,
}

/// Per-listener policy. Different listening ports can install independent
/// protocol registries, admission controls, or service dispatchers.
pub trait Acceptor: Send + Sync + 'static {
    fn accept(&self, socket: AcceptedTcpSocket) -> Result<()>;
}

/// TCP defaults used by listeners created through [`Server`].
///
/// `SO_REUSEADDR` is always enabled before binding. `reuse_port` and `backlog`
/// affect only listeners created by [`Server::listen_tcp`]; `tcp_nodelay` is
/// applied to each accepted stream before it reaches the [`Acceptor`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TcpOptions {
    reuse_port: bool,
    backlog: u32,
    tcp_nodelay: bool,
}

impl TcpOptions {
    const DEFAULT_BACKLOG: u32 = 1024;

    async fn bind(self, address: impl ToSocketAddrs) -> io::Result<TcpListener> {
        let mut addresses = tokio::net::lookup_host(address).await?;
        let address = addresses.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "tcp listener address resolved empty")
        })?;
        self.bind_addr(address)
    }

    fn bind_addr(self, address: SocketAddr) -> io::Result<TcpListener> {
        let socket = if address.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
        socket.set_reuseaddr(true)?;
        if self.reuse_port {
            set_reuseport(&socket)?;
        }
        socket.bind(address)?;
        socket.listen(self.backlog)
    }
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self { reuse_port: false, backlog: Self::DEFAULT_BACKLOG, tcp_nodelay: true }
    }
}

/// Standard RPC acceptor: an accepted socket becomes one protocol-detecting
/// server connection and immediately starts its first read continuation.
pub struct RpcAcceptor {
    options: RpcAcceptorOptions,
    services: ServiceRegistry,
    http_registry: super::HttpRegistry,
    builtin_registry: super::HttpRegistry,
    http_options: super::HttpServerOptions,
}

#[derive(Clone, Debug)]
pub struct RpcAcceptorOptions {
    /// Protocol allow-list for this listener.
    ///
    /// By default every process-registered protocol participates in probing.
    /// Restrict this set only when a listener must reject specific protocols.
    pub allowed_protocols: ProtocolSet,
    /// Maximum prefix examined before selecting a protocol. Bytes already
    /// received after a matching prefix belong to the selected protocol.
    pub max_probe_bytes: usize,
}

impl Default for RpcAcceptorOptions {
    fn default() -> Self {
        Self { allowed_protocols: ProtocolSet::all(), max_probe_bytes: 32 * 1024 }
    }
}

impl RpcAcceptor {
    pub fn new(services: impl IntoServiceRegistry) -> Self {
        Self::with_options(services.into_service_registry(), RpcAcceptorOptions::default())
    }

    pub fn with_options(services: impl IntoServiceRegistry, options: RpcAcceptorOptions) -> Self {
        Self {
            options,
            services: services.into_service_registry(),
            http_registry: super::HttpRegistry::default(),
            builtin_registry: super::HttpRegistry::default(),
            http_options: super::HttpServerOptions::default(),
        }
    }

    pub fn allow_protocols(mut self, allowed_protocols: ProtocolSet) -> Self {
        self.options.allowed_protocols = allowed_protocols;
        self
    }

    pub fn with_http(
        mut self,
        registry: super::HttpRegistry,
        options: super::HttpServerOptions,
    ) -> Self {
        self.http_registry = registry;
        self.http_options = options;
        self
    }

    pub fn with_builtin(mut self, services: super::BuiltinServices) -> Self {
        self.builtin_registry = services.into_http_registry(self.services.clone());
        self
    }
}

impl Acceptor for RpcAcceptor {
    fn accept(&self, socket: AcceptedTcpSocket) -> Result<()> {
        let remote_addr = socket.remote_addr;
        let connection = TcpConnection::accept(socket.stream, remote_addr)?;
        spawn_protocol_detection(
            connection,
            remote_addr,
            self.options.clone(),
            self.services.clone(),
            self.http_registry.clone(),
            self.builtin_registry.clone(),
            self.http_options.clone(),
        );
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_protocol_detection(
    connection: Resource<TcpConnection>,
    remote_addr: SocketAddr,
    options: RpcAcceptorOptions,
    services: ServiceRegistry,
    http_registry: super::HttpRegistry,
    builtin_registry: super::HttpRegistry,
    http_options: super::HttpServerOptions,
) {
    tokio::spawn(async move {
        detect_protocol(
            connection,
            remote_addr,
            options,
            services,
            http_registry,
            builtin_registry,
            http_options,
        )
        .await;
    });
}

#[allow(clippy::too_many_arguments)]
async fn detect_protocol(
    connection: Resource<TcpConnection>,
    remote_addr: SocketAddr,
    options: RpcAcceptorOptions,
    services: ServiceRegistry,
    http_registry: super::HttpRegistry,
    builtin_registry: super::HttpRegistry,
    http_options: super::HttpServerOptions,
) {
    let mut input = IoBuf::new();
    loop {
        let selection = match probe_with_limit(&input, &options) {
            Ok(selection) => selection,
            Err(error) => {
                log::warn!(
                    "protocol probe failed: remote={}, bytes={}, limit={}, error={error}",
                    remote_addr,
                    input.len(),
                    options.max_probe_bytes
                );
                break;
            }
        };
        match selection {
            ProbeSelection::Matched(protocol) => {
                protocol.handle_read_connection(
                    connection.clone(),
                    super::ServerSessionContext {
                        services,
                        http_registry,
                        builtin_registry,
                        http_options,
                    },
                    input,
                );
                return;
            }
            ProbeSelection::NeedMoreData => match connection.read_more(&mut input).await {
                Ok(0) => {
                    log::debug!("peer closed during protocol detection: remote={}", remote_addr);
                    break;
                }
                Err(_) => break, // read_more logs the underlying I/O error.
                Ok(_) => {}
            },
        }
    }
    connection.close();
}

pub(crate) async fn bind_tcp_listener(
    address: impl ToSocketAddrs,
    reuse_port: bool,
    backlog: u32,
) -> io::Result<TcpListener> {
    TcpOptions { reuse_port, backlog, tcp_nodelay: false }.bind(address).await
}

fn probe_with_limit(input: &IoBuf, options: &RpcAcceptorOptions) -> Result<ProbeSelection> {
    let selection = if input.len() > options.max_probe_bytes {
        // Clone buffer references only, without copying payload bytes. Keep the
        // original input intact for the selected protocol's connection driver.
        let mut prefix = input.clone();
        probe_allowed(&prefix.split_to(options.max_probe_bytes), options.allowed_protocols)?
    } else {
        probe_allowed(input, options.allowed_protocols)?
    };
    if matches!(selection, ProbeSelection::NeedMoreData) && input.len() >= options.max_probe_bytes {
        return Err(crate::Error::Protocol("protocol probe limit exceeded".into()));
    }
    Ok(selection)
}

/// Cancellation and ownership for one listening endpoint.
#[derive(Clone)]
pub struct ListenerHandle {
    cancellation: CancellationToken,
    local_addr: SocketAddr,
}

impl ListenerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&self) {
        if !self.cancellation.is_cancelled() {
            log::debug!("TCP listener shutdown requested: local={}", self.local_addr);
        }
        self.cancellation.cancel();
    }

    pub fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// Owns any number of independently configured listening endpoints.
#[derive(Default)]
pub struct Server {
    tcp_options: TcpOptions,
    listeners: Vec<ListenerHandle>,
}

impl Server {
    pub const DEFAULT_TCP_BACKLOG: u32 = TcpOptions::DEFAULT_BACKLOG;

    pub fn new() -> Self {
        Self::default()
    }

    /// Starts the stable application-facing server API.
    pub fn builder() -> super::ServerBuilder {
        super::ServerBuilder::new()
    }

    pub fn tcp_reuse_port(&mut self, reuse_port: bool) -> &mut Self {
        self.tcp_options.reuse_port = reuse_port;
        self
    }

    pub fn tcp_backlog(&mut self, backlog: u32) -> &mut Self {
        self.tcp_options.backlog = backlog;
        self
    }

    pub fn tcp_nodelay(&mut self, tcp_nodelay: bool) -> &mut Self {
        self.tcp_options.tcp_nodelay = tcp_nodelay;
        self
    }

    pub fn add_listener(
        &mut self,
        listener: TcpListener,
        acceptor: impl Acceptor,
    ) -> ListenerHandle {
        self.add_listener_with_tcp_options(listener, self.tcp_options, acceptor)
    }

    pub fn add_shared_listener(
        &mut self,
        listener: TcpListener,
        acceptor: Arc<dyn Acceptor>,
    ) -> ListenerHandle {
        self.add_shared_listener_with_tcp_options(listener, self.tcp_options, acceptor)
    }

    pub async fn listen_tcp(
        &mut self,
        address: impl ToSocketAddrs,
        acceptor: impl Acceptor,
    ) -> io::Result<ListenerHandle> {
        let options = self.tcp_options;
        let listener = options.bind(address).await?;
        Ok(self.add_listener_with_tcp_options(listener, options, acceptor))
    }

    fn add_listener_with_tcp_options(
        &mut self,
        listener: TcpListener,
        options: TcpOptions,
        acceptor: impl Acceptor,
    ) -> ListenerHandle {
        self.add_shared_listener_with_tcp_options(listener, options, Arc::new(acceptor))
    }

    fn add_shared_listener_with_tcp_options(
        &mut self,
        listener: TcpListener,
        options: TcpOptions,
        acceptor: Arc<dyn Acceptor>,
    ) -> ListenerHandle {
        let local_addr =
            listener.local_addr().expect("registered TCP listener without local address");
        let cancellation = CancellationToken::new();
        log::debug!("TCP listener started: local={local_addr}");
        spawn_accept_continuation(Arc::new(listener), options, acceptor, cancellation.clone());
        let handle = ListenerHandle { cancellation, local_addr };
        self.listeners.push(handle.clone());
        handle
    }

    pub fn shutdown(&self) {
        for listener in &self.listeners {
            listener.shutdown();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn spawn_accept_continuation(
    listener: Arc<TcpListener>,
    options: TcpOptions,
    acceptor: Arc<dyn Acceptor>,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            _ = cancellation.cancelled() => return,
        };
        let (stream, remote_addr) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                log::error!("TCP accept failed: local={:?}, error={error}", listener.local_addr());
                // Accept errors are normally transient process/resource
                // pressure. Yield before reinstalling the continuation so one
                // broken listener cannot spin on a Tokio worker.
                tokio::task::yield_now().await;
                if !cancellation.is_cancelled() {
                    spawn_accept_continuation(listener, options, acceptor, cancellation);
                }
                return;
            }
        };
        // Publish the successor before connection setup. Admission and the
        // first read therefore never delay acceptance of the next socket.
        spawn_accept_continuation(listener, options, acceptor.clone(), cancellation);
        let local_addr = match stream.local_addr() {
            Ok(address) => address,
            Err(error) => {
                log::error!(
                    "failed to query accepted TCP socket address: remote={remote_addr}, error={error}"
                );
                return;
            }
        };
        if options.tcp_nodelay
            && let Err(error) = stream.set_nodelay(true)
        {
            log::error!("failed to set TCP_NODELAY: remote={remote_addr}, error={error}");
            return;
        }
        log::debug!("TCP socket accepted: local={local_addr}, remote={remote_addr}");
        if let Err(error) = acceptor.accept(AcceptedTcpSocket { stream, local_addr, remote_addr }) {
            match error {
                crate::Error::ResourceExhausted | crate::Error::Encode(_) => {
                    log::error!("TCP acceptor failed: remote={remote_addr}, error={error}");
                }
                _ => log::warn!(
                    "TCP socket rejected by acceptor: remote={remote_addr}, error={error}"
                ),
            }
        }
    });
}

#[cfg(all(
    unix,
    not(target_os = "solaris"),
    not(target_os = "illumos"),
    not(target_os = "cygwin"),
    not(target_os = "nuttx"),
))]
fn set_reuseport(socket: &TcpSocket) -> io::Result<()> {
    socket.set_reuseport(true)
}

#[cfg(not(all(
    unix,
    not(target_os = "solaris"),
    not(target_os = "illumos"),
    not(target_os = "cygwin"),
    not(target_os = "nuttx"),
)))]
fn set_reuseport(_socket: &TcpSocket) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "SO_REUSEPORT is not supported on this target"))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn coalesced_payload_does_not_count_against_protocol_probe_limit() {
        use bytes::BufMut;
        let mut builder = crate::IoBufBuilder::new();
        builder.put_slice(crate::protocol::http2::CLIENT_PREFACE);
        builder.put_slice(&vec![0_u8; 64 * 1024]);
        let input = builder.finish();
        let options = RpcAcceptorOptions {
            allowed_protocols: ProtocolSet::only(crate::Protocol::Http2),
            max_probe_bytes: crate::protocol::http2::CLIENT_PREFACE.len(),
        };
        assert!(matches!(probe_with_limit(&input, &options).unwrap(), ProbeSelection::Matched(_)));
        assert_eq!(input.len(), options.max_probe_bytes + 64 * 1024);
        let too_short =
            RpcAcceptorOptions { max_probe_bytes: options.max_probe_bytes - 1, ..options };
        assert!(probe_with_limit(&input, &too_short).is_err());
    }

    struct NodelayAcceptor {
        sender: Mutex<Option<oneshot::Sender<io::Result<bool>>>>,
    }

    impl Acceptor for NodelayAcceptor {
        fn accept(&self, socket: AcceptedTcpSocket) -> Result<()> {
            if let Some(sender) = self.sender.lock().unwrap().take() {
                let _ = sender.send(socket.stream.nodelay());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn listen_tcp_applies_nodelay_to_accepted_stream() {
        let (sender, receiver) = oneshot::channel();
        let mut server = Server::new();
        let listener = server
            .listen_tcp("127.0.0.1:0", NodelayAcceptor { sender: Mutex::new(Some(sender)) })
            .await
            .unwrap();

        let client = TcpStream::connect(listener.local_addr()).await.unwrap();
        assert!(receiver.await.unwrap().unwrap());
        drop(client);
        server.shutdown();
    }

    #[cfg(all(
        unix,
        not(target_os = "solaris"),
        not(target_os = "illumos"),
        not(target_os = "cygwin"),
        not(target_os = "nuttx"),
    ))]
    #[tokio::test]
    async fn server_tcp_reuse_port_allows_multiple_listeners_on_one_address() {
        let mut first = Server::new();
        first.tcp_reuse_port(true);
        let first_listener = first
            .listen_tcp("127.0.0.1:0", NodelayAcceptor { sender: Mutex::new(None) })
            .await
            .unwrap();

        let mut second = Server::new();
        second.tcp_reuse_port(true);
        let second_listener = second
            .listen_tcp(first_listener.local_addr(), NodelayAcceptor { sender: Mutex::new(None) })
            .await
            .unwrap();

        assert_eq!(second_listener.local_addr(), first_listener.local_addr());
    }
}
