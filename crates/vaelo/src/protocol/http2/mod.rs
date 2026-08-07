// SPDX-License-Identifier: Apache-2.0

//! HTTP/2 transport core.
//!
//! HPACK, stream, and flow-control state always have one connection owner, but
//! that owner is acquired cooperatively rather than pinned to a permanent
//! driver task. An idle submitter progresses work inline; contending submitters
//! publish bounded work for the current owner to combine.

pub(crate) mod client;
pub(crate) mod connection;
pub(crate) mod error;
pub(crate) mod frame;
pub(crate) mod hpack;
mod server;
pub(crate) mod settings;
pub(crate) use error::{ConnectionError, ErrorCode, H2Error};
pub(crate) use frame::{Frame, FrameFlags, FrameHeader, FrameKind, StreamId};

use crate::IoBuf;
use crate::protocol::{ProbeResult, ProtocolProbe, ServerProtocol};

pub(crate) static HTTP2_PROTOCOL: Http2Protocol = Http2Protocol;

pub(crate) struct Http2Protocol;

pub(crate) const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

impl ProtocolProbe for Http2Protocol {
    fn probe(&self, input: &IoBuf) -> ProbeResult {
        let mut seen = 0;
        for chunk in input.chunks() {
            for byte in chunk {
                if *byte != CLIENT_PREFACE[seen] {
                    return ProbeResult::NoMatch;
                }
                seen += 1;
                if seen == CLIENT_PREFACE.len() {
                    return ProbeResult::Match;
                }
            }
        }
        ProbeResult::NeedMoreData
    }
}

impl ServerProtocol for Http2Protocol {
    fn handle_read_connection(
        &'static self,
        connection: crate::Resource<crate::transport::tcp::TcpConnection>,
        context: crate::server::ServerSessionContext,
        input: IoBuf,
    ) {
        let remote_addr = *connection.remote_addr();
        match server::new_session(connection, self, context) {
            Ok(session) => {
                log::debug!("http2 server session started: remote={remote_addr}");
                session.handle_read_event(input);
            }
            Err(error) => log::error!(
                "failed to create http2 server session: remote={remote_addr}, error={error}"
            ),
        }
    }
}
