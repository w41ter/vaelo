// SPDX-License-Identifier: Apache-2.0

//! Wire codecs and the server-side protocol registry.

pub(crate) mod brpc;
mod codec;
pub(crate) mod grpc;
pub(crate) mod http1;
pub(crate) mod http2;
pub(crate) mod http_call;

use std::time::Instant;

pub use codec::{Decode, Encode};

use crate::request_options::RequestOptions;
use crate::{Error, IoBuf, MethodDescriptor, Protocol, Result, Status};

pub(crate) struct RequestEncodeContext<'a> {
    pub method: &'static MethodDescriptor,
    pub options: &'a RequestOptions,
    pub correlation_id: u64,
}

pub(crate) struct InboundResponse {
    pub correlation_id: u64,
    pub payload: IoBuf,
    pub attachment: IoBuf,
    pub status: Option<Status>,
}

pub struct InboundRequest {
    pub protocol: Protocol,
    pub correlation_id: u64,
    pub service: String,
    pub method: String,
    pub log_id: Option<u64>,
    pub request_id: Option<String>,
    pub deadline: Option<Instant>,
    pub payload: IoBuf,
    pub attachment: IoBuf,
}

pub(crate) struct ResponseEncodeContext<'a> {
    pub correlation_id: u64,
    pub status: Option<&'a Status>,
}

pub(crate) trait ProtocolProbe: Send + Sync + 'static {
    fn probe(&self, input: &IoBuf) -> ProbeResult;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProbeResult {
    Match,
    NeedMoreData,
    NoMatch,
}

/// Process-wide protocol implementation. The accept path probes once, creates
/// a connection-local session, and removes this registry from the hot path.
pub(crate) trait ServerProtocol: ProtocolProbe + Send + Sync + 'static {
    fn handle_read_connection(
        &'static self,
        connection: crate::Resource<crate::transport::tcp::TcpConnection>,
        context: crate::server::ServerSessionContext,
        input: IoBuf,
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolSet {
    bits: u16,
}

impl ProtocolSet {
    pub const fn all() -> Self {
        Self {
            bits: (1 << Protocol::Brpc as u16)
                | (1 << Protocol::Http1 as u16)
                | (1 << Protocol::Http2 as u16),
        }
    }

    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const fn only(protocol: Protocol) -> Self {
        Self { bits: protocol_bit(protocol) }
    }

    pub const fn contains(self, protocol: Protocol) -> bool {
        self.bits & protocol_bit(protocol) != 0
    }

    pub fn insert(&mut self, protocol: Protocol) {
        self.bits |= protocol_bit(protocol);
    }
}

const fn protocol_bit(protocol: Protocol) -> u16 {
    // gRPC is detected as HTTP/2 on an accepted connection.
    let wire = match protocol {
        Protocol::Grpc => Protocol::Http2,
        protocol => protocol,
    };
    1 << wire as u16
}

impl Default for ProtocolSet {
    fn default() -> Self {
        Self::all()
    }
}

pub(crate) fn probe_allowed(input: &IoBuf, allowed: ProtocolSet) -> Result<ProbeSelection> {
    let mut need_more = false;
    for &(id, protocol) in implementations() {
        if !allowed.contains(id) {
            continue;
        }
        match protocol.probe(input) {
            ProbeResult::Match => return Ok(ProbeSelection::Matched(protocol)),
            ProbeResult::NeedMoreData => need_more = true,
            ProbeResult::NoMatch => {}
        }
    }
    if need_more {
        Ok(ProbeSelection::NeedMoreData)
    } else {
        Err(Error::Protocol("no registered protocol matched input".into()))
    }
}

pub(crate) enum ProbeSelection {
    Matched(&'static dyn ServerProtocol),
    NeedMoreData,
}

fn implementations() -> &'static [(Protocol, &'static dyn ServerProtocol)] {
    static IMPLEMENTATIONS: [(Protocol, &dyn ServerProtocol); 3] = [
        (Protocol::Brpc, &brpc::BRPC_PROTOCOL),
        (Protocol::Http1, &http1::HTTP1_PROTOCOL),
        (Protocol::Http2, &http2::HTTP2_PROTOCOL),
    ];
    &IMPLEMENTATIONS
}
