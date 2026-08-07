// SPDX-License-Identifier: Apache-2.0

use super::{Channel, ConnectionMode, ConnectionTag, Request as RequestEnvelope};
use crate::protocol::grpc::client::{
    GrpcClientRecvStream, GrpcClientSendStream, submit_streaming, submit_unary,
};
use crate::runtime::TimeScheduler;
use crate::runtime::profile::{self, ProfileStage};
use crate::transport::inflight::{CallContext, CallToken};
use crate::{Encode, IoBuf, IoBufBuilder, MethodDescriptor, StaticResource, Status, StatusCode};

pub(crate) fn start_owned<RequestMessage>(
    channel: &Channel,
    method: &'static MethodDescriptor,
    request: RequestEnvelope<RequestMessage>,
    connection_tag: Option<ConnectionTag>,
) -> Result<CallToken, Status>
where
    RequestMessage: Encode,
{
    if channel.wire_protocol() != crate::Protocol::Grpc {
        return Err(Status::new(StatusCode::InvalidArgument, "channel protocol is not gRPC"));
    }
    let profile_sampled = profile::sample_next();
    let (message, mut options, attachment) = request.into_wire_parts();
    if options.request_id.is_some() || options.log_id.is_some() || !attachment.is_empty() {
        return Err(Status::new(
            StatusCode::InvalidArgument,
            "bRPC request options cannot be used on a gRPC channel",
        ));
    }
    let payload = encode_request(&message, profile_sampled)?;
    options.profile_sampled = profile_sampled;
    let config = crate::protocol::grpc::GrpcConfig::default();
    let key = channel.connection_key_with_tag(connection_tag);
    let short_connection = channel.options().connection_mode == ConnectionMode::Short;
    let alloc_start = profile::start_if(profile_sampled);
    let context = CallContext::resource_pool()
        .acquire_with_init(|| CallContext::new(options.deadline, profile_sampled))
        .map_err(|_| Status::new(StatusCode::ResourceExhausted, "call resources exhausted"))?;
    if let Some(start) = alloc_start {
        profile::record_elapsed(ProfileStage::ClientCallContextAlloc, start);
    }
    let token = CallToken::new(context.clone());
    submit_unary(key, short_connection, method, options, payload, config, context.clone())?;
    TimeScheduler::register(context.clone());
    Ok(token)
}

pub(crate) fn start_streaming_owned(
    channel: &Channel,
    method: &'static MethodDescriptor,
    request: RequestEnvelope<()>,
    connection_tag: Option<ConnectionTag>,
    initial_payload: Option<IoBuf>,
) -> Result<(Option<GrpcClientSendStream>, GrpcClientRecvStream), Status> {
    if channel.wire_protocol() != crate::Protocol::Grpc {
        return Err(Status::new(StatusCode::InvalidArgument, "streaming RPC requires gRPC"));
    }
    let profile_sampled = profile::sample_next();
    let (_, mut options, attachment) = request.into_wire_parts();
    if options.request_id.is_some() || options.log_id.is_some() || !attachment.is_empty() {
        return Err(Status::new(
            StatusCode::InvalidArgument,
            "bRPC request options cannot be used on a gRPC channel",
        ));
    }
    options.profile_sampled = profile_sampled;
    let context = CallContext::resource_pool()
        .acquire_with_init(|| CallContext::new(options.deadline, profile_sampled))
        .map_err(|_| Status::new(StatusCode::ResourceExhausted, "call resources exhausted"))?;
    let key = channel.connection_key_with_tag(connection_tag);
    let short_connection = channel.options().connection_mode == ConnectionMode::Short;
    let streams = submit_streaming(
        key,
        short_connection,
        method,
        options,
        crate::protocol::grpc::GrpcConfig::default(),
        context.clone(),
        initial_payload,
    )?;
    TimeScheduler::register(context);
    Ok(streams)
}

fn encode_request<RequestMessage: Encode>(
    request: &RequestMessage,
    profile_sampled: bool,
) -> Result<IoBuf, Status> {
    let stage_start = profile::start_if(profile_sampled);
    let mut builder = IoBufBuilder::new();
    request.encode_to(&mut builder)?;
    let payload = builder.finish();
    if let Some(start) = stage_start {
        profile::record_elapsed(ProfileStage::ClientPayloadEncode, start);
    }
    Ok(payload)
}
