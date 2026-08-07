// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use bytes::{Buf, BufMut};
use prost::Message;

use super::{
    InboundRequest, InboundResponse, ProbeResult, ProtocolProbe, RequestEncodeContext,
    ResponseEncodeContext, ServerProtocol,
};
use crate::runtime::profile;
use crate::{Error, IoBuf, IoBufBuilder, Protocol, Result, Status, StatusCode};

pub(crate) static BRPC_PROTOCOL: BrpcProtocol = BrpcProtocol {
    config: BrpcProtocolConfig {
        max_body_size: 64 * 1024 * 1024,
        max_meta_size: 1024 * 1024,
        deliver_timeout: true,
    },
};

pub(crate) mod client;
mod server;

const HEADER_LEN: usize = 12;
const MAGIC: &[u8; 4] = b"PRPC";
const COALESCE_SMALL_FRAME_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BrpcProtocolConfig {
    max_body_size: usize,
    max_meta_size: usize,
    deliver_timeout: bool,
}

impl Default for BrpcProtocolConfig {
    fn default() -> Self {
        Self { max_body_size: 64 * 1024 * 1024, max_meta_size: 1024 * 1024, deliver_timeout: true }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BrpcProtocol {
    config: BrpcProtocolConfig,
}

impl BrpcProtocol {
    fn parse_frame(&self, input: &mut IoBuf) -> Result<Option<(BrpcRpcMeta, IoBuf)>> {
        let Some(header) = peek_header(input) else {
            return Ok(None);
        };
        if &header[..4] != MAGIC {
            return Err(Error::Protocol("invalid bRPC magic".into()));
        }
        let body_size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
        let meta_size = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        if body_size > self.config.max_body_size {
            return Err(Error::Protocol("bRPC body exceeds configured limit".into()));
        }
        if meta_size > body_size || meta_size > self.config.max_meta_size {
            return Err(Error::Protocol("invalid bRPC meta size".into()));
        }
        let frame_size = HEADER_LEN
            .checked_add(body_size)
            .ok_or_else(|| Error::Protocol("bRPC frame length overflow".into()))?;
        if input.len() < frame_size {
            return Ok(None);
        }
        input.advance(HEADER_LEN);
        let meta_bytes = input.split_to(meta_size);
        let payload = input.split_to(body_size - meta_size);
        let meta = BrpcRpcMeta::decode(meta_bytes)
            .map_err(|error| Error::Protocol(error.to_string().into()))?;
        Ok(Some((meta, payload)))
    }
}

impl ProtocolProbe for BrpcProtocol {
    fn probe(&self, input: &IoBuf) -> ProbeResult {
        let mut seen = 0;
        for chunk in input.chunks() {
            for byte in chunk {
                if *byte != MAGIC[seen] {
                    return ProbeResult::NoMatch;
                }
                seen += 1;
                if seen == MAGIC.len() {
                    return ProbeResult::Match;
                }
            }
        }
        ProbeResult::NeedMoreData
    }
}

impl ServerProtocol for BrpcProtocol {
    fn handle_read_connection(
        &'static self,
        connection: crate::Resource<crate::transport::tcp::TcpConnection>,
        context: crate::server::ServerSessionContext,
        input: IoBuf,
    ) {
        let remote_addr = *connection.remote_addr();
        match server::new_session(connection, self, context) {
            Ok(session) => {
                log::debug!("brpc server session started: remote={remote_addr}");
                session.handle_read_event(input);
            }
            Err(error) => log::error!(
                "failed to create brpc server session: remote={remote_addr}, error={error}"
            ),
        }
    }
}

impl BrpcProtocol {
    fn encode_request(
        &self,
        context: RequestEncodeContext<'_>,
        payload: IoBuf,
        attachment: IoBuf,
    ) -> Result<IoBuf> {
        let attachment_size = checked_i32(attachment.len(), "request attachment")?;
        let timeout_ms =
            self.config.deliver_timeout.then_some(context.options.deadline).flatten().map(
                |deadline| {
                    deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis()
                        .clamp(1, i32::MAX as u128) as i32
                },
            );
        let meta = RequestMetaRef {
            service: context.method.service,
            method: context.method.method,
            log_id: context.options.log_id,
            request_id: context.options.request_id.as_deref(),
            timeout_ms,
            correlation_id: context.correlation_id,
            attachment_size,
        };
        let meta_len = request_meta_len(&meta);
        if meta_len > self.config.max_meta_size {
            return Err(Error::Protocol("bRPC meta exceeds configured limit".into()));
        }
        let body_size = checked_body_size(meta_len, payload.len(), attachment.len(), &self.config)?;
        let mut builder = IoBufBuilder::new();
        builder.put_slice(MAGIC);
        builder.put_u32(body_size as u32);
        builder.put_u32(meta_len as u32);
        encode_request_meta(&mut builder, &meta);
        Ok(finish_frame(builder.finish(), payload, attachment, body_size))
    }

    /// Returns `Ok(None)` without consuming input until a complete frame
    /// arrives.
    fn decode_response(&self, input: &mut IoBuf) -> Result<Option<InboundResponse>> {
        let Some((meta, mut body)) = self.parse_frame(input)? else {
            return Ok(None);
        };
        BrpcCompression::from_wire_optional(meta.compress_type)?;
        let correlation_id = required_correlation_id(&meta)?;
        let attachment_size = attachment_size(&meta, body.len())?;
        let payload_size = body.len() - attachment_size;
        let payload = body.split_to(payload_size);
        let status = meta.response.and_then(|response| {
            let error_code = BrpcErrorCode::from_wire(response.error_code);
            (!error_code.is_ok()).then(|| {
                Status::new(error_code.status_code(), response.error_text.unwrap_or_default())
            })
        });
        Ok(Some(InboundResponse { correlation_id, payload, attachment: body, status }))
    }
    fn decode_request(&self, input: &mut IoBuf) -> Result<Option<InboundRequest>> {
        let Some((meta, mut body)) = self.parse_frame(input)? else {
            return Ok(None);
        };
        BrpcCompression::from_wire_optional(meta.compress_type)?;
        let correlation_id = required_correlation_id(&meta)?;
        let attachment_size = attachment_size(&meta, body.len())?;
        let payload = body.split_to(body.len() - attachment_size);
        let request = meta.request.ok_or_else(|| Error::Protocol("missing request meta".into()))?;
        let deadline = request
            .timeout_ms
            .filter(|timeout_ms| *timeout_ms > 0)
            .map(|timeout_ms| Instant::now() + Duration::from_millis(timeout_ms as u64));
        Ok(Some(InboundRequest {
            protocol: Protocol::Brpc,
            correlation_id,
            service: request.service_name,
            method: request.method_name,
            log_id: request.log_id.map(|value| value as u64),
            request_id: request.request_id,
            deadline,
            payload,
            attachment: body,
        }))
    }

    fn encode_response(
        &self,
        context: ResponseEncodeContext<'_>,
        payload: IoBuf,
        attachment: IoBuf,
    ) -> Result<IoBuf> {
        let attachment_size = checked_i32(attachment.len(), "response attachment")?;
        let response =
            context.status.map_or(BrpcResponseMeta { error_code: 0, error_text: None }, |status| {
                BrpcResponseMeta {
                    error_code: BrpcErrorCode::from_status_code(status.code()).wire(),
                    error_text: (!status.message().is_empty()).then(|| status.message().to_owned()),
                }
            });
        let meta = BrpcRpcMeta {
            request: None,
            response: Some(response),
            compress_type: None,
            correlation_id: Some(context.correlation_id as i64),
            attachment_size: (attachment_size > 0).then_some(attachment_size),
        };
        let meta_len = meta.encoded_len();
        if meta_len > self.config.max_meta_size {
            return Err(Error::Protocol("bRPC meta exceeds configured limit".into()));
        }
        let body_size = checked_body_size(meta_len, payload.len(), attachment.len(), &self.config)?;
        let mut builder = IoBufBuilder::new();
        builder.put_slice(MAGIC);
        builder.put_u32(body_size as u32);
        builder.put_u32(meta_len as u32);
        meta.encode(&mut builder).map_err(|error| Error::Protocol(error.to_string().into()))?;
        Ok(finish_frame(builder.finish(), payload, attachment, body_size))
    }
}

fn finish_frame(
    mut header_and_meta: IoBuf,
    mut payload: IoBuf,
    mut attachment: IoBuf,
    body_size: usize,
) -> IoBuf {
    if profile::coalesce_small_frames_enabled()
        && HEADER_LEN + body_size <= COALESCE_SMALL_FRAME_BYTES
    {
        let mut builder = IoBufBuilder::new();
        append_iobuf_copy(&mut builder, &header_and_meta);
        append_iobuf_copy(&mut builder, &payload);
        append_iobuf_copy(&mut builder, &attachment);
        return builder.finish();
    }

    header_and_meta.append_movable(&mut payload);
    header_and_meta.append_movable(&mut attachment);
    header_and_meta
}

fn append_iobuf_copy(builder: &mut IoBufBuilder, buffer: &IoBuf) {
    for chunk in buffer.chunks() {
        builder.put_slice(chunk);
    }
}

#[derive(Clone, PartialEq, Message)]
struct BrpcRpcMeta {
    #[prost(message, optional, tag = "1")]
    request: Option<BrpcRequestMeta>,
    #[prost(message, optional, tag = "2")]
    response: Option<BrpcResponseMeta>,
    #[prost(int32, optional, tag = "3")]
    compress_type: Option<i32>,
    #[prost(int64, optional, tag = "4")]
    correlation_id: Option<i64>,
    #[prost(int32, optional, tag = "5")]
    attachment_size: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct BrpcRequestMeta {
    #[prost(string, required, tag = "1")]
    service_name: String,
    #[prost(string, required, tag = "2")]
    method_name: String,
    #[prost(int64, optional, tag = "3")]
    log_id: Option<i64>,
    #[prost(int64, optional, tag = "4")]
    trace_id: Option<i64>,
    #[prost(int64, optional, tag = "5")]
    span_id: Option<i64>,
    #[prost(int64, optional, tag = "6")]
    parent_span_id: Option<i64>,
    #[prost(string, optional, tag = "7")]
    request_id: Option<String>,
    #[prost(int32, optional, tag = "8")]
    timeout_ms: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct BrpcResponseMeta {
    #[prost(int32, required, tag = "1")]
    error_code: i32,
    #[prost(string, optional, tag = "2")]
    error_text: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrpcCompression {
    None,
}

impl BrpcCompression {
    fn from_wire_optional(code: Option<i32>) -> Result<Self> {
        match code.unwrap_or_default() {
            0 => Ok(Self::None),
            _ => Err(Error::Protocol("compressed bRPC message is not supported yet".into())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BrpcErrorCode {
    Ok,
    DeadlineExceeded,
    FailedSocket,
    ResourceExhausted,
    Internal,
    Unavailable(i32),
    Unknown(i32),
}

impl BrpcErrorCode {
    fn from_status_code(code: StatusCode) -> Self {
        match code {
            StatusCode::Ok => Self::Ok,
            // baidu_std carries brpc::Errno. Cancellation has no dedicated
            // server code, so transport-like termination is reported as
            // EFAILEDSOCKET.
            StatusCode::Canceled => Self::FailedSocket,
            StatusCode::DeadlineExceeded => Self::DeadlineExceeded,
            StatusCode::Unavailable => Self::FailedSocket,
            StatusCode::ResourceExhausted => Self::ResourceExhausted,
            StatusCode::Unknown
            | StatusCode::InvalidArgument
            | StatusCode::NotFound
            | StatusCode::AlreadyExists
            | StatusCode::PermissionDenied
            | StatusCode::FailedPrecondition
            | StatusCode::Aborted
            | StatusCode::OutOfRange
            | StatusCode::Unimplemented
            | StatusCode::Internal
            | StatusCode::DataLoss
            | StatusCode::Unauthenticated => Self::Internal,
        }
    }

    fn from_wire(code: i32) -> Self {
        match code {
            0 => Self::Ok,
            1008 => Self::DeadlineExceeded,
            1009 => Self::FailedSocket,
            1011 => Self::ResourceExhausted,
            1014 | 2003 | 2005 => Self::Unavailable(code),
            2004 => Self::ResourceExhausted,
            2001 => Self::Internal,
            _ => Self::Unknown(code),
        }
    }

    fn wire(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::DeadlineExceeded => 1008,
            Self::FailedSocket => 1009,
            Self::ResourceExhausted => 1011,
            Self::Internal => 2001,
            Self::Unavailable(code) | Self::Unknown(code) => code,
        }
    }

    fn is_ok(self) -> bool {
        self == Self::Ok
    }

    fn status_code(self) -> StatusCode {
        match self {
            Self::Ok => StatusCode::Ok,
            Self::DeadlineExceeded => StatusCode::DeadlineExceeded,
            Self::FailedSocket | Self::Unavailable(_) => StatusCode::Unavailable,
            Self::ResourceExhausted => StatusCode::ResourceExhausted,
            Self::Internal | Self::Unknown(_) => StatusCode::Internal,
        }
    }
}

fn peek_header(input: &IoBuf) -> Option<[u8; HEADER_LEN]> {
    if input.len() < HEADER_LEN {
        return None;
    }
    let mut header = [0; HEADER_LEN];
    let mut copied = 0;
    for chunk in input.chunks() {
        let count = (HEADER_LEN - copied).min(chunk.len());
        header[copied..copied + count].copy_from_slice(&chunk[..count]);
        copied += count;
        if copied == HEADER_LEN {
            break;
        }
    }
    Some(header)
}

fn attachment_size(meta: &BrpcRpcMeta, body_size: usize) -> Result<usize> {
    let size = meta.attachment_size.unwrap_or_default();
    if size < 0 || size as usize > body_size {
        return Err(Error::Protocol("invalid bRPC attachment size".into()));
    }
    Ok(size as usize)
}

fn required_correlation_id(meta: &BrpcRpcMeta) -> Result<u64> {
    meta.correlation_id
        .map(|value| value as u64)
        .ok_or_else(|| Error::Protocol("missing bRPC correlation id".into()))
}

fn checked_i32(size: usize, name: &'static str) -> Result<i32> {
    i32::try_from(size).map_err(|_| Error::Protocol(format!("{name} is too large").into()))
}

fn checked_body_size(
    meta: usize,
    payload: usize,
    attachment: usize,
    config: &BrpcProtocolConfig,
) -> Result<usize> {
    let size = meta
        .checked_add(payload)
        .and_then(|size| size.checked_add(attachment))
        .ok_or_else(|| Error::Protocol("bRPC frame length overflow".into()))?;
    if size > config.max_body_size || size > u32::MAX as usize {
        return Err(Error::Protocol("bRPC body exceeds configured limit".into()));
    }
    Ok(size)
}

// The request meta is encoded directly from static descriptor strings. Building
// prost-owned Strings here would add two allocations to every client call.
struct RequestMetaRef<'a> {
    service: &'a str,
    method: &'a str,
    log_id: Option<u64>,
    request_id: Option<&'a str>,
    timeout_ms: Option<i32>,
    correlation_id: u64,
    attachment_size: i32,
}

fn request_meta_len(meta: &RequestMetaRef<'_>) -> usize {
    let nested = string_field_len(1, meta.service)
        + string_field_len(2, meta.method)
        + meta.log_id.map_or(0, |value| key_len(3, 0) + varint_len(value))
        + meta.request_id.map_or(0, |value| string_field_len(7, value))
        + meta.timeout_ms.map_or(0, |value| key_len(8, 0) + varint_len(value as u64));
    key_len(1, 2)
        + varint_len(nested as u64)
        + nested
        + key_len(4, 0)
        + varint_len(meta.correlation_id)
        + if meta.attachment_size > 0 {
            key_len(5, 0) + varint_len(meta.attachment_size as u64)
        } else {
            0
        }
}

fn encode_request_meta(output: &mut impl BufMut, meta: &RequestMetaRef<'_>) {
    let nested = string_field_len(1, meta.service)
        + string_field_len(2, meta.method)
        + meta.log_id.map_or(0, |value| key_len(3, 0) + varint_len(value))
        + meta.request_id.map_or(0, |value| string_field_len(7, value))
        + meta.timeout_ms.map_or(0, |value| key_len(8, 0) + varint_len(value as u64));
    put_key(output, 1, 2);
    put_varint(output, nested as u64);
    put_string(output, 1, meta.service);
    put_string(output, 2, meta.method);
    if let Some(log_id) = meta.log_id {
        put_key(output, 3, 0);
        put_varint(output, log_id);
    }
    if let Some(request_id) = meta.request_id {
        put_string(output, 7, request_id);
    }
    if let Some(timeout_ms) = meta.timeout_ms {
        put_key(output, 8, 0);
        put_varint(output, timeout_ms as u64);
    }
    put_key(output, 4, 0);
    put_varint(output, meta.correlation_id);
    if meta.attachment_size > 0 {
        put_key(output, 5, 0);
        put_varint(output, meta.attachment_size as u64);
    }
}

fn put_string(output: &mut impl BufMut, field: u32, value: &str) {
    put_key(output, field, 2);
    put_varint(output, value.len() as u64);
    output.put_slice(value.as_bytes());
}

fn string_field_len(field: u32, value: &str) -> usize {
    key_len(field, 2) + varint_len(value.len() as u64) + value.len()
}

fn key_len(field: u32, wire: u8) -> usize {
    varint_len(((field << 3) | wire as u32) as u64)
}

fn put_key(output: &mut impl BufMut, field: u32, wire: u8) {
    put_varint(output, ((field << 3) | wire as u32) as u64);
}

fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn put_varint(output: &mut impl BufMut, mut value: u64) {
    while value >= 0x80 {
        output.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    output.put_u8(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::{Idempotency, MethodDescriptor, MethodKind};
    use crate::request_options::RequestOptions;

    static METHOD: MethodDescriptor = MethodDescriptor::new(
        "fixture.EchoService",
        "Echo",
        MethodKind::Unary,
        Idempotency::Idempotent,
    );

    // Kept separate from the production structs so these fixtures describe
    // the Apache bRPC BAIDU_STD schema rather than echoing our implementation.
    #[derive(Clone, PartialEq, Message)]
    struct StandardRpcMeta {
        #[prost(message, optional, tag = "1")]
        request: Option<StandardRequestMeta>,
        #[prost(int64, optional, tag = "4")]
        correlation_id: Option<i64>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct StandardRequestMeta {
        #[prost(string, required, tag = "1")]
        service_name: String,
        #[prost(string, required, tag = "2")]
        method_name: String,
        #[prost(int64, optional, tag = "3")]
        log_id: Option<i64>,
        #[prost(int64, optional, tag = "4")]
        trace_id: Option<i64>,
        #[prost(int64, optional, tag = "5")]
        span_id: Option<i64>,
        #[prost(int64, optional, tag = "6")]
        parent_span_id: Option<i64>,
        #[prost(string, optional, tag = "7")]
        request_id: Option<String>,
        #[prost(int32, optional, tag = "8")]
        timeout_ms: Option<i32>,
    }

    #[test]
    fn rust_request_uses_standard_request_id_and_timeout_tags() {
        let options = RequestOptions {
            deadline: Some(Instant::now() + Duration::from_secs(5)),
            log_id: Some(17),
            request_id: Some("request-42".to_owned()),
            ..RequestOptions::default()
        };

        let frame = BrpcProtocol::default()
            .encode_request(
                RequestEncodeContext { method: &METHOD, options: &options, correlation_id: 23 },
                IoBuf::new(),
                IoBuf::new(),
            )
            .unwrap();
        let wire: Vec<_> = frame.chunks().flatten().copied().collect();
        let meta_size = u32::from_be_bytes(wire[8..12].try_into().unwrap()) as usize;
        let meta = StandardRpcMeta::decode(&wire[HEADER_LEN..HEADER_LEN + meta_size]).unwrap();
        let request = meta.request.unwrap();

        assert_eq!(request.service_name, METHOD.service);
        assert_eq!(request.method_name, METHOD.method);
        assert_eq!(request.log_id, Some(17));
        assert_eq!(request.request_id.as_deref(), Some("request-42"));
        assert!(matches!(request.timeout_ms, Some(1..=5000)));
        assert_eq!(request.trace_id, None);
        assert_eq!(request.span_id, None);
        assert_eq!(request.parent_span_id, None);
        assert_eq!(meta.correlation_id, Some(23));
    }

    #[test]
    fn standard_request_delivers_request_id_and_deadline_to_rust() {
        let standard = StandardRpcMeta {
            request: Some(StandardRequestMeta {
                service_name: METHOD.service.to_owned(),
                method_name: METHOD.method.to_owned(),
                log_id: Some(17),
                trace_id: Some(101),
                span_id: Some(102),
                parent_span_id: Some(103),
                request_id: Some("request-42".to_owned()),
                timeout_ms: Some(250),
            }),
            correlation_id: Some(23),
        };
        let meta = standard.encode_to_vec();
        let mut builder = IoBufBuilder::new();
        builder.put_slice(MAGIC);
        builder.put_u32(meta.len() as u32);
        builder.put_u32(meta.len() as u32);
        builder.put_slice(&meta);
        let mut frame = builder.finish();

        let before_decode = Instant::now();
        let request = BrpcProtocol::default().decode_request(&mut frame).unwrap().unwrap();
        let delivered_timeout = request.deadline.unwrap().duration_since(before_decode);

        assert_eq!(request.correlation_id, 23);
        assert_eq!(request.log_id, Some(17));
        assert_eq!(request.request_id.as_deref(), Some("request-42"));
        assert!(delivered_timeout >= Duration::from_millis(200));
        assert!(delivered_timeout <= Duration::from_millis(300));
    }
}
