// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(u16)]
pub enum Protocol {
    #[default]
    Brpc = 0,
    Http1 = 1,
    Http2 = 2,
    /// gRPC application semantics over a dedicated HTTP/2 connection.
    Grpc = 3,
}

/// RPC protocol selected by a logical [`crate::RpcChannel`].
///
/// This type deliberately excludes HTTP versions. gRPC is an RPC protocol
/// carried by HTTP/2, while `Http1` and `Http2` are selected through
/// [`crate::http::Client`]. One RPC channel is bound to exactly one protocol
/// for its entire lifetime; unsupported method kinds fail before bytes are
/// submitted to the peer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RpcProtocol {
    Brpc,
    Grpc,
}

impl From<RpcProtocol> for Protocol {
    fn from(protocol: RpcProtocol) -> Self {
        match protocol {
            RpcProtocol::Brpc => Self::Brpc,
            RpcProtocol::Grpc => Self::Grpc,
        }
    }
}

impl TryFrom<Protocol> for RpcProtocol {
    type Error = ();

    fn try_from(protocol: Protocol) -> Result<Self, Self::Error> {
        match protocol {
            Protocol::Brpc => Ok(Self::Brpc),
            Protocol::Grpc => Ok(Self::Grpc),
            Protocol::Http1 | Protocol::Http2 => Err(()),
        }
    }
}

/// Application-level RPC protocol. HTTP/2 connection probing cannot
/// distinguish gRPC from ordinary HTTP, so this is deliberately separate from
/// [`Protocol`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RequestProtocol {
    #[default]
    Brpc,
    Grpc,
    HttpMapping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MethodKind {
    Unary,
    ClientStreaming,
    ServerStreaming,
    BidirectionalStreaming,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Idempotency {
    Unknown,
    NoSideEffects,
    Idempotent,
}

/// Static method information emitted by code generation.
///
/// Keeping descriptors static avoids rebuilding method names and policy
/// metadata for every request.
#[derive(Clone, Copy, Debug)]
pub struct MethodDescriptor {
    pub service: &'static str,
    pub method: &'static str,
    pub kind: MethodKind,
    pub idempotency: Idempotency,
}

impl MethodDescriptor {
    pub const fn new(
        service: &'static str,
        method: &'static str,
        kind: MethodKind,
        idempotency: Idempotency,
    ) -> Self {
        Self { service, method, kind, idempotency }
    }
}
