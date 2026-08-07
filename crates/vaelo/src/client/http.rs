// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;
use std::sync::LazyLock;
use std::time::Instant;

use http::Request;

use super::{Channel, ConnectionMode};
use crate::foundation::cow_hash_map::CowHashMap;
pub use crate::protocol::http_call::HttpCall;
use crate::protocol::http1::client::Http1ClientSession;
use crate::protocol::http2::client::{Http2ClientSession, RequestBody};
use crate::transport::connection::ConnectionKey;
use crate::{Error, IoBuf, Resource};

/// Raw HTTP client backed by a logical [`Channel`].
///
/// Physical connections are shared process-wide according to the channel's
/// connection mode and tag.
#[derive(Clone)]
pub struct HttpClient {
    channel: Channel,
}

static HTTP1_CONNECTIONS: LazyLock<Http1ConnectionMap> = LazyLock::new(Http1ConnectionMap::new);

struct Http1ConnectionMap {
    entries: CowHashMap<ConnectionKey, Resource<Http1ClientSession>>,
}

impl HttpClient {
    pub fn with_channel(channel: Channel) -> crate::Result<Self> {
        match channel.wire_protocol() {
            crate::Protocol::Http1 | crate::Protocol::Http2 => Ok(Self { channel }),
            _ => Err(Error::Channel("HTTP client requires an HTTP/1 or HTTP/2 channel".into())),
        }
    }

    pub(crate) fn send_with(
        &self,
        request: Request<IoBuf>,
        deadline: Option<Instant>,
        connection_tag: Option<super::ConnectionTag>,
    ) -> HttpCall {
        match self.channel.options().connection_mode {
            ConnectionMode::Single | ConnectionMode::Pool { .. } => {
                match self.channel.wire_protocol() {
                    crate::Protocol::Http1 => {
                        match HTTP1_CONNECTIONS
                            .get_or_create(self.channel.connection_key_with_tag(connection_tag))
                        {
                            Ok(session) => session.issue_request(request, deadline),
                            Err(error) => HttpCall::ready(Err(error)),
                        }
                    }
                    crate::Protocol::Http2 => {
                        let key = self.channel.connection_key_with_tag(connection_tag);
                        match Http2ClientSession::get_or_create(key, false) {
                            Ok(session) => {
                                let (parts, body) = request.into_parts();
                                match session.issue(
                                    RequestBody::Buffered(Some(body)),
                                    parts,
                                    deadline,
                                ) {
                                    Ok(call) => HttpCall::http2(call),
                                    Err(error) => HttpCall::ready(Err(error)),
                                }
                            }
                            Err(error) => HttpCall::ready(Err(error)),
                        }
                    }
                    _ => unreachable!("HTTP client validates its channel protocol"),
                }
            }
            ConnectionMode::Short => match self.channel.wire_protocol() {
                crate::Protocol::Http1 => Http1ClientSession::issue_short_request(
                    self.channel.remote_addr(),
                    request,
                    deadline,
                ),
                crate::Protocol::Http2 => {
                    let key = self.channel.connection_key_with_tag(connection_tag);
                    match Http2ClientSession::get_or_create(key, true) {
                        Ok(session) => {
                            let (parts, body) = request.into_parts();
                            match session.issue(RequestBody::Buffered(Some(body)), parts, deadline)
                            {
                                Ok(call) => HttpCall::http2(call),
                                Err(error) => HttpCall::ready(Err(error)),
                            }
                        }
                        Err(error) => HttpCall::ready(Err(error)),
                    }
                }
                _ => unreachable!("HTTP client validates its channel protocol"),
            },
        }
    }

    pub(crate) fn send_streaming(
        &self,
        parts: http::request::Parts,
        body: Pin<Box<dyn crate::http::ErasedBody + Send>>,
        deadline: Option<Instant>,
        connection_tag: Option<super::ConnectionTag>,
    ) -> HttpCall {
        match self.channel.options().connection_mode {
            ConnectionMode::Single | ConnectionMode::Pool { .. } => {
                match self.channel.wire_protocol() {
                    crate::Protocol::Http1 => match HTTP1_CONNECTIONS
                        .get_or_create(self.channel.connection_key_with_tag(connection_tag))
                    {
                        Ok(session) => session.issue_streaming_request(parts, body, deadline),
                        Err(error) => HttpCall::ready(Err(error)),
                    },
                    crate::Protocol::Http2 => {
                        let key = self.channel.connection_key_with_tag(connection_tag);
                        match Http2ClientSession::get_or_create(key, false) {
                            Ok(session) => {
                                match session.issue(RequestBody::Streaming(body), parts, deadline) {
                                    Ok(call) => HttpCall::http2(call),
                                    Err(error) => HttpCall::ready(Err(error)),
                                }
                            }
                            Err(error) => HttpCall::ready(Err(error)),
                        }
                    }
                    _ => unreachable!("HTTP client validates its channel protocol"),
                }
            }
            ConnectionMode::Short => match self.channel.wire_protocol() {
                crate::Protocol::Http1 => {
                    match Http1ClientSession::create(
                        self.channel.remote_addr(),
                        self.channel.options().connect_timeout,
                        true,
                    ) {
                        Ok(session) => session.issue_streaming_request(parts, body, deadline),
                        Err(error) => HttpCall::ready(Err(error)),
                    }
                }
                crate::Protocol::Http2 => {
                    let key = self.channel.connection_key_with_tag(connection_tag);
                    match Http2ClientSession::get_or_create(key, true) {
                        Ok(session) => {
                            match session.issue(RequestBody::Streaming(body), parts, deadline) {
                                Ok(call) => HttpCall::http2(call),
                                Err(error) => HttpCall::ready(Err(error)),
                            }
                        }
                        Err(error) => HttpCall::ready(Err(error)),
                    }
                }
                _ => unreachable!("HTTP client validates its channel protocol"),
            },
        }
    }
}

impl Http1ConnectionMap {
    fn new() -> Self {
        Self { entries: CowHashMap::new() }
    }

    /// Process-wide HTTP/1 connection map.
    ///
    /// The container has no remote/local defaults. Those values are part of the
    /// lookup key because a static map contains sessions for every channel in
    /// the process.
    fn get_or_create(&self, key: ConnectionKey) -> crate::Result<Resource<Http1ClientSession>> {
        self.entries.get_or_try_insert_with(key, || {
            Http1ClientSession::create(key.remote, key.connect_timeout, false)
        })
    }
}
