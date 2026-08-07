// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use super::{Channel, ConnectionMode, ConnectionTag, Request as RequestEnvelope};
use crate::foundation::cow_hash_map::CowHashMap;
use crate::protocol::brpc::client::{BrpcClientSession, PreparedRequest};
use crate::runtime::profile::{self, ProfileStage};
use crate::transport::connection::ConnectionKey;
use crate::transport::inflight::CallToken;
use crate::{Encode, IoBufBuilder, MethodDescriptor, Resource, Status, StatusCode};

static BRPC_CONNECTIONS: LazyLock<BrpcConnectionMap> = LazyLock::new(BrpcConnectionMap::new);

struct BrpcConnectionMap {
    entries: CowHashMap<ConnectionKey, Resource<BrpcClientSession>>,
}

impl BrpcConnectionMap {
    fn new() -> Self {
        Self { entries: CowHashMap::new() }
    }

    /// Returns the process-wide bRPC session for `key`, creating it lazily.
    ///
    /// The map is intentionally static and typed. It does not store a default
    /// remote/local address because every entry may point at a different peer.
    /// Lifecycle maintenance tasks can later iterate this map to close idle
    /// sessions, run health checks, or enforce global connection policy.
    fn get_or_create(&self, key: ConnectionKey) -> crate::Result<Resource<BrpcClientSession>> {
        self.entries.get_or_try_insert_with(key, || {
            BrpcClientSession::create(key.remote, key.connect_timeout)
        })
    }

    fn remove(&self, key: &ConnectionKey) {
        self.entries.remove(key);
    }

    fn remove_if(&self, key: &ConnectionKey, expected: crate::ResourceId<BrpcClientSession>) {
        self.entries.remove_if(key, |session| session.id() == expected);
    }
}

pub(crate) fn close_cached(key: &ConnectionKey) {
    BRPC_CONNECTIONS.remove(key);
}

fn session_for(
    channel: &Channel,
    connection_tag: Option<ConnectionTag>,
) -> crate::Result<(Resource<BrpcClientSession>, Option<super::recycler::ClientActivityGuard>)> {
    let key = channel.connection_key_with_tag(connection_tag);
    loop {
        let session = BRPC_CONNECTIONS.get_or_create(key)?;
        if let Some(activity) = session.try_acquire_activity() {
            return Ok((session, activity));
        }
        BRPC_CONNECTIONS.remove_if(&key, session.id());
    }
}

fn issue_short_rpc(channel: &Channel, request: PreparedRequest) -> crate::Result<CallToken> {
    BrpcClientSession::create_short(channel.remote_addr(), channel.options().connect_timeout)
        .and_then(|session| session.issue_rpc(request, None))
}

pub(crate) fn start_owned<Request>(
    channel: &Channel,
    method: &'static MethodDescriptor,
    request: RequestEnvelope<Request>,
    connection_tag: Option<ConnectionTag>,
) -> Result<CallToken, Status>
where
    Request: Encode,
{
    if channel.wire_protocol() != crate::Protocol::Brpc {
        return Err(Status::new(StatusCode::InvalidArgument, "channel protocol is not bRPC"));
    }
    let profile_sampled = profile::sample_next();
    let (message, mut options, attachment) = request.into_wire_parts();
    let stage_start = profile::start_if(profile_sampled);
    let mut builder = IoBufBuilder::new();
    if let Err(error) = message.encode_to(&mut builder) {
        return Err(error.into());
    }
    let payload = builder.finish();
    if let Some(start) = stage_start {
        profile::record_elapsed(ProfileStage::ClientPayloadEncode, start);
    }
    options.profile_sampled = profile_sampled;
    let request = PreparedRequest { method, options, payload, attachment };
    match channel.options().connection_mode {
        ConnectionMode::Single | ConnectionMode::Pool { .. } => {
            session_for(channel, connection_tag)
                .and_then(|(session, activity)| session.issue_rpc(request, activity))
        }
        ConnectionMode::Short => issue_short_rpc(channel, request),
    }
    .map_err(Status::from)
}

pub(crate) fn recycle_idle_bucket(bucket: usize, now_ms: u64) {
    let mut retired = Vec::new();
    BRPC_CONNECTIONS.entries.visit_bucket(bucket, |key, session| {
        if session.try_retire(now_ms) {
            retired.push((*key, session.clone()));
        }
    });
    for (key, session) in retired {
        BRPC_CONNECTIONS.remove_if(&key, session.id());
        session.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::cow_hash_map::DEFAULT_BUCKETS;

    #[test]
    fn recycler_preserves_active_session_then_replaces_it_when_idle() {
        let channel = crate::RpcChannel::brpc("127.0.0.1:61234".parse().unwrap()).build().unwrap();
        let key = channel.connection_key();
        let (session, activity) = session_for(&channel, None).unwrap();
        let original_id = session.id();

        for bucket in 0..DEFAULT_BUCKETS {
            recycle_idle_bucket(bucket, u64::MAX);
        }
        let (same_session, second_activity) = session_for(&channel, None).unwrap();
        assert_eq!(same_session.id(), original_id);

        drop(second_activity);
        drop(activity);
        for bucket in 0..DEFAULT_BUCKETS {
            recycle_idle_bucket(bucket, u64::MAX);
        }
        let (replacement, replacement_activity) = session_for(&channel, None).unwrap();
        assert_ne!(replacement.id(), original_id);

        drop(replacement_activity);
        close_cached(&key);
    }
}
