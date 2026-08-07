// SPDX-License-Identifier: Apache-2.0

pub(crate) mod client;
mod server;

use std::net::SocketAddr;

use bytes::{Buf, BufMut};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode};

use crate::IoBuf;
use crate::http::Frame;
use crate::protocol::{ProbeResult, ProtocolProbe, ServerProtocol};
use crate::server::http::{HttpRequest, HttpResponse, HttpServerOptions};

pub(crate) static HTTP1_PROTOCOL: Http1Protocol = Http1Protocol;

pub(crate) struct Http1Protocol;

impl ProtocolProbe for Http1Protocol {
    fn probe(&self, input: &IoBuf) -> ProbeResult {
        const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        let mut prefix = Vec::with_capacity(32);
        for chunk in input.chunks() {
            let count = (32 - prefix.len()).min(chunk.len());
            prefix.extend_from_slice(&chunk[..count]);
            if prefix.len() == 32 {
                break;
            }
        }
        if prefix.is_empty() {
            return ProbeResult::NeedMoreData;
        }
        if HTTP2_PREFACE.starts_with(&prefix) {
            return ProbeResult::NeedMoreData;
        }
        let Some(space) = prefix.iter().position(|byte| *byte == b' ') else {
            return if prefix.iter().all(|byte| is_method_token(*byte)) {
                ProbeResult::NeedMoreData
            } else {
                ProbeResult::NoMatch
            };
        };
        let method = &prefix[..space];
        if method.is_empty()
            || method == b"PRI"
            || !method.iter().all(|byte| is_method_token(*byte))
        {
            return ProbeResult::NoMatch;
        }
        ProbeResult::Match
    }
}

impl Http1Protocol {
    /// Decodes one HTTP/1 request from server-side input.
    ///
    /// This is a codec operation, not session policy. The returned
    /// `close_after_response` flag tells the session whether the decoded
    /// request is a keep-alive boundary. The session still owns pipeline
    /// admission, response ordering, and write-barrier handling.
    pub(crate) fn decode_server_request(
        &self,
        input: &mut IoBuf,
        remote_addr: SocketAddr,
        options: &HttpServerOptions,
    ) -> Result<Option<Http1ServerRequest>, Http1Error> {
        parse_request_head(input, remote_addr, options).map_err(Http1Error::from_status)
    }

    /// Encodes one HTTP/1 response frame.
    ///
    /// HTTP/1 response ordering is not enforced here. Callers must submit
    /// encoded frames to the TCP writer only after the session has restored the
    /// required FIFO order.
    pub(crate) fn encode_server_response(
        &self,
        response: HttpResponse,
        close_after_response: bool,
    ) -> IoBuf {
        encode_response(response, close_after_response)
    }

    pub(crate) fn encode_streaming_response_head(
        &self,
        response: &mut HttpResponse,
        content_length: Option<u64>,
        close: bool,
    ) -> IoBuf {
        encode_response_head(response, content_length, close)
    }

    pub(crate) fn encode_chunk(&self, mut payload: IoBuf) -> IoBuf {
        let mut builder = crate::IoBufBuilder::new();
        builder.put_slice(format!("{:x}\r\n", payload.len()).as_bytes());
        let mut frame = builder.finish();
        frame.append_movable(&mut payload);
        let mut suffix = crate::IoBufBuilder::new();
        suffix.put_slice(b"\r\n");
        frame.append_movable(&mut suffix.finish());
        frame
    }

    pub(crate) fn encode_last_chunk(&self, trailers: Option<HeaderMap>) -> IoBuf {
        let mut builder = crate::IoBufBuilder::new();
        builder.put_slice(b"0\r\n");
        if let Some(trailers) = trailers {
            for (name, value) in &trailers {
                builder.put_slice(name.as_str().as_bytes());
                builder.put_slice(b": ");
                builder.put_slice(value.as_bytes());
                builder.put_slice(b"\r\n");
            }
        }
        builder.put_slice(b"\r\n");
        builder.finish()
    }

    /// Encodes one client request.
    ///
    /// The eventual implementation should support the explicitly documented
    /// HTTP/1 feature set only: fixed `Content-Length`, no chunked request
    /// bodies, no CONNECT tunnel, and no upgrade. The client session owns FIFO
    /// pending publication before submitting the returned frame.
    pub(crate) fn encode_client_request(&self, request: Request<IoBuf>) -> crate::Result<IoBuf> {
        encode_request(request)
    }

    pub(crate) fn encode_client_request_head(
        &self,
        mut parts: http::request::Parts,
        content_length: Option<u64>,
    ) -> crate::Result<IoBuf> {
        if parts.headers.contains_key(http::header::TRANSFER_ENCODING)
            || parts.headers.contains_key(http::header::CONTENT_LENGTH)
        {
            return Err(crate::Error::Protocol(
                "streaming HTTP/1 framing headers are managed by the runtime".into(),
            ));
        }
        match content_length {
            Some(length) => {
                parts.headers.insert(
                    http::header::CONTENT_LENGTH,
                    HeaderValue::from_str(&length.to_string())
                        .map_err(|error| crate::Error::Protocol(error.to_string().into()))?,
                );
            }
            None => {
                parts
                    .headers
                    .insert(http::header::TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
            }
        }
        encode_request_head(parts)
    }

    pub(crate) fn decode_client_response_head(
        &self,
        input: &mut IoBuf,
        options: &HttpClientResponseOptions,
    ) -> Result<Option<Http1ClientResponseHead>, Http1Error> {
        parse_response_head(input, options).map_err(Http1Error::from_status)
    }
}

pub(crate) struct Http1ClientResponseHead {
    pub response: Response<()>,
    pub body: Http1BodyDecoder,
}

pub(crate) enum BodyDecodeEvent {
    Frame(Frame),
    End,
}

pub(crate) struct Http1BodyDecoder {
    state: BodyState,
    total: usize,
    limit: usize,
}

enum BodyState {
    Fixed(usize),
    CloseDelimited,
    ChunkSize,
    ChunkData(usize),
    ChunkDataCrlf,
    ChunkTrailers,
    EndAfterTrailers,
    Done,
}

impl Http1BodyDecoder {
    const FRAME_BYTES: usize = 16 * 1024;

    fn fixed(length: usize, limit: usize) -> Self {
        Self { state: BodyState::Fixed(length), total: 0, limit }
    }

    fn chunked(limit: usize) -> Self {
        Self { state: BodyState::ChunkSize, total: 0, limit }
    }

    fn close_delimited(limit: usize) -> Self {
        Self { state: BodyState::CloseDelimited, total: 0, limit }
    }

    pub(crate) fn is_close_delimited(&self) -> bool {
        matches!(self.state, BodyState::CloseDelimited)
    }

    pub(crate) fn size_hint(&self) -> crate::http::SizeHint {
        match self.state {
            BodyState::Fixed(length) => crate::http::SizeHint::exact(length as u64),
            _ => crate::http::SizeHint::unknown(),
        }
    }

    pub(crate) fn decode(
        &mut self,
        input: &mut IoBuf,
    ) -> Result<Option<BodyDecodeEvent>, Http1Error> {
        loop {
            match self.state {
                BodyState::Fixed(0) => {
                    self.state = BodyState::Done;
                    return Ok(Some(BodyDecodeEvent::End));
                }
                BodyState::Fixed(remaining) => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let count = remaining.min(input.len()).min(Self::FRAME_BYTES);
                    self.state = BodyState::Fixed(remaining - count);
                    self.total += count;
                    return Ok(Some(BodyDecodeEvent::Frame(Frame::Data(input.split_to(count)))));
                }
                BodyState::CloseDelimited => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let count = input.len().min(Self::FRAME_BYTES);
                    if self.total.saturating_add(count) > self.limit {
                        return Err(Http1Error::from_status(StatusCode::PAYLOAD_TOO_LARGE));
                    }
                    self.total += count;
                    return Ok(Some(BodyDecodeEvent::Frame(Frame::Data(input.split_to(count)))));
                }
                BodyState::ChunkSize => {
                    let Some(line_end) = find_bytes(input, b"\r\n", 128)? else {
                        return Ok(None);
                    };
                    let line = copy_prefix(input, line_end);
                    input.advance(line_end + 2);
                    let size_text = std::str::from_utf8(
                        line.split(|byte| *byte == b';').next().unwrap_or_default(),
                    )
                    .map_err(|_| Http1Error::from_status(StatusCode::BAD_GATEWAY))?;
                    let size = usize::from_str_radix(size_text.trim(), 16)
                        .map_err(|_| Http1Error::from_status(StatusCode::BAD_GATEWAY))?;
                    if size == 0 {
                        self.state = BodyState::ChunkTrailers;
                    } else {
                        if self.total.saturating_add(size) > self.limit {
                            return Err(Http1Error::from_status(StatusCode::PAYLOAD_TOO_LARGE));
                        }
                        self.state = BodyState::ChunkData(size);
                    }
                }
                BodyState::ChunkData(remaining) => {
                    if input.is_empty() {
                        return Ok(None);
                    }
                    let count = remaining.min(input.len()).min(Self::FRAME_BYTES);
                    self.total += count;
                    self.state = if count == remaining {
                        BodyState::ChunkDataCrlf
                    } else {
                        BodyState::ChunkData(remaining - count)
                    };
                    return Ok(Some(BodyDecodeEvent::Frame(Frame::Data(input.split_to(count)))));
                }
                BodyState::ChunkDataCrlf => {
                    if input.len() < 2 {
                        return Ok(None);
                    }
                    if copy_prefix(input, 2) != b"\r\n" {
                        return Err(Http1Error::from_status(StatusCode::BAD_GATEWAY));
                    }
                    input.advance(2);
                    self.state = BodyState::ChunkSize;
                }
                BodyState::ChunkTrailers => {
                    if input.len() >= 2 && copy_prefix(input, 2) == b"\r\n" {
                        input.advance(2);
                        self.state = BodyState::Done;
                        return Ok(Some(BodyDecodeEvent::End));
                    }
                    let Some(end) = find_bytes(input, b"\r\n\r\n", 32 * 1024)? else {
                        return Ok(None);
                    };
                    let bytes = copy_prefix(input, end);
                    input.advance(end + 4);
                    let trailers = parse_trailers(&bytes)?;
                    self.state = BodyState::EndAfterTrailers;
                    return Ok(Some(BodyDecodeEvent::Frame(Frame::Trailers(trailers))));
                }
                BodyState::EndAfterTrailers => {
                    self.state = BodyState::Done;
                    return Ok(Some(BodyDecodeEvent::End));
                }
                BodyState::Done => return Ok(Some(BodyDecodeEvent::End)),
            }
        }
    }
}

pub(crate) struct Http1ServerRequest {
    pub request: HttpRequest,
    pub body: Http1BodyDecoder,
    pub close_after_response: bool,
}

pub(crate) struct HttpClientResponseOptions {
    pub max_header_size: usize,
    pub max_body_size: usize,
}

impl Default for HttpClientResponseOptions {
    fn default() -> Self {
        Self { max_header_size: 32 * 1024, max_body_size: 64 * 1024 * 1024 }
    }
}

pub(crate) struct Http1Error {
    status: StatusCode,
}

impl Http1Error {
    fn from_status(status: StatusCode) -> Self {
        Self { status }
    }

    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }
}

impl ServerProtocol for Http1Protocol {
    fn handle_read_connection(
        &'static self,
        connection: crate::Resource<crate::transport::tcp::TcpConnection>,
        context: crate::server::ServerSessionContext,
        input: IoBuf,
    ) {
        let remote_addr = *connection.remote_addr();
        match server::new_session(connection, self, context) {
            Ok(session) => {
                log::debug!("http1 server session started: remote={remote_addr}");
                session.handle_read_event(input);
            }
            Err(error) => log::error!(
                "failed to create http1 server session: remote={remote_addr}, error={error}"
            ),
        }
    }
}

fn is_method_token(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
            | b'0'..=b'9'
            | b'A'..=b'Z'
            | b'a'..=b'z'
    )
}

fn parse_request_head(
    input: &mut IoBuf,
    remote_addr: SocketAddr,
    options: &HttpServerOptions,
) -> Result<Option<Http1ServerRequest>, StatusCode> {
    let bytes = copy_prefix(input, input.len().min(options.max_header_size.saturating_add(1)));
    if bytes.len() > options.max_header_size && !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
        return Err(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }
    let mut raw_headers = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Request::new(&mut raw_headers);
    let header_len = match parsed.parse(&bytes) {
        Ok(httparse::Status::Complete(size)) => size,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };
    let method = parsed
        .method
        .and_then(|value| Method::from_bytes(value.as_bytes()).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let target = parsed.path.ok_or(StatusCode::BAD_REQUEST)?;
    let version = parsed.version.ok_or(StatusCode::BAD_REQUEST)?;
    let mut headers = HeaderMap::with_capacity(parsed.headers.len());
    let mut content_length = None;
    let mut chunked = false;
    let mut close = version == 0;
    for header in parsed.headers.iter() {
        let name =
            HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| StatusCode::BAD_REQUEST)?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| StatusCode::BAD_REQUEST)?;
        if name == http::header::CONTENT_LENGTH {
            let length = value
                .to_str()
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or(StatusCode::BAD_REQUEST)?;
            if content_length.replace(length).is_some_and(|old| old != length) {
                return Err(StatusCode::BAD_REQUEST);
            }
        }
        if name == http::header::TRANSFER_ENCODING {
            chunked = value.to_str().is_ok_and(is_chunked_only);
            if !chunked {
                return Err(StatusCode::NOT_IMPLEMENTED);
            }
        }
        if name == http::header::CONNECTION {
            let connection = value.to_str().unwrap_or_default();
            close = connection.eq_ignore_ascii_case("close")
                || (version == 0 && !connection.eq_ignore_ascii_case("keep-alive"));
        }
        headers.append(name, value);
    }
    if chunked && content_length.is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let length = content_length.unwrap_or(0);
    if length > options.max_body_size {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(path, query)| (path, Some(query.to_owned())));
    input.advance(header_len);
    Ok(Some(Http1ServerRequest {
        request: HttpRequest {
            method,
            version: if version == 1 { http::Version::HTTP_11 } else { http::Version::HTTP_10 },
            path: path.to_owned(),
            query,
            headers,
            body: crate::http::Incoming::buffered(IoBuf::new()),
            path_parameters: Default::default(),
            remote_addr,
        },
        body: if chunked {
            Http1BodyDecoder::chunked(options.max_body_size)
        } else {
            Http1BodyDecoder::fixed(length, options.max_body_size)
        },
        close_after_response: close,
    }))
}

fn encode_request(request: Request<IoBuf>) -> crate::Result<IoBuf> {
    let (mut parts, mut body) = request.into_parts();
    if parts.headers.contains_key(http::header::TRANSFER_ENCODING) {
        return Err(crate::Error::Protocol(
            "HTTP/1 client does not support transfer-encoding".into(),
        ));
    }
    parts.headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string())
            .map_err(|error| crate::Error::Protocol(error.to_string().into()))?,
    );
    let mut frame = encode_request_head(parts)?;
    frame.append_movable(&mut body);
    Ok(frame)
}

fn encode_request_head(parts: http::request::Parts) -> crate::Result<IoBuf> {
    let target = parts.uri.path_and_query().map_or("/", |target| target.as_str());
    let version = match parts.version {
        http::Version::HTTP_10 => "HTTP/1.0",
        _ => "HTTP/1.1",
    };
    let mut builder = crate::IoBufBuilder::new();
    builder.put_slice(parts.method.as_str().as_bytes());
    builder.put_slice(b" ");
    builder.put_slice(target.as_bytes());
    builder.put_slice(b" ");
    builder.put_slice(version.as_bytes());
    builder.put_slice(b"\r\n");
    for (name, value) in &parts.headers {
        builder.put_slice(name.as_str().as_bytes());
        builder.put_slice(b": ");
        builder.put_slice(value.as_bytes());
        builder.put_slice(b"\r\n");
    }
    builder.put_slice(b"\r\n");
    Ok(builder.finish())
}

fn parse_response_head(
    input: &mut IoBuf,
    options: &HttpClientResponseOptions,
) -> Result<Option<Http1ClientResponseHead>, StatusCode> {
    let bytes = copy_prefix(input, input.len().min(options.max_header_size.saturating_add(1)));
    if bytes.len() > options.max_header_size && !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
        return Err(StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);
    }
    let mut raw_headers = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Response::new(&mut raw_headers);
    let header_len = match parsed.parse(&bytes) {
        Ok(httparse::Status::Complete(size)) => size,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(StatusCode::BAD_GATEWAY),
    };
    let status = parsed
        .code
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or(StatusCode::BAD_GATEWAY)?;
    let version = parsed.version.ok_or(StatusCode::BAD_GATEWAY)?;
    let mut headers = HeaderMap::with_capacity(parsed.headers.len());
    let mut content_length = None;
    let mut chunked = false;
    for header in parsed.headers.iter() {
        let name =
            HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| StatusCode::BAD_GATEWAY)?;
        let value = HeaderValue::from_bytes(header.value).map_err(|_| StatusCode::BAD_GATEWAY)?;
        if name == http::header::CONTENT_LENGTH {
            let length = value
                .to_str()
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or(StatusCode::BAD_GATEWAY)?;
            if content_length.replace(length).is_some_and(|old| old != length) {
                return Err(StatusCode::BAD_GATEWAY);
            }
        }
        if name == http::header::TRANSFER_ENCODING {
            chunked = value.to_str().is_ok_and(is_chunked_only);
            if !chunked {
                return Err(StatusCode::NOT_IMPLEMENTED);
            }
        }
        headers.append(name, value);
    }
    if chunked && content_length.is_some() {
        return Err(StatusCode::BAD_GATEWAY);
    }
    let no_body = status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    let length = if no_body { Some(0) } else { content_length };
    if length.is_some_and(|length| length > options.max_body_size) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    input.advance(header_len);
    let mut response = Response::new(());
    *response.status_mut() = status;
    *response.version_mut() =
        if version == 1 { http::Version::HTTP_11 } else { http::Version::HTTP_10 };
    *response.headers_mut() = headers;
    Ok(Some(Http1ClientResponseHead {
        response,
        body: if chunked {
            Http1BodyDecoder::chunked(options.max_body_size)
        } else if let Some(length) = length {
            Http1BodyDecoder::fixed(length, options.max_body_size)
        } else {
            Http1BodyDecoder::close_delimited(options.max_body_size)
        },
    }))
}

fn copy_prefix(input: &IoBuf, length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    for chunk in input.chunks() {
        let count = (length - bytes.len()).min(chunk.len());
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() == length {
            break;
        }
    }
    bytes
}

fn find_bytes(input: &IoBuf, needle: &[u8], limit: usize) -> Result<Option<usize>, Http1Error> {
    let length = input.len().min(limit.saturating_add(needle.len()));
    let bytes = copy_prefix(input, length);
    if let Some(position) = bytes.windows(needle.len()).position(|part| part == needle) {
        return Ok(Some(position));
    }
    if input.len() > limit {
        Err(Http1Error::from_status(StatusCode::BAD_GATEWAY))
    } else {
        Ok(None)
    }
}

fn parse_trailers(bytes: &[u8]) -> Result<HeaderMap, Http1Error> {
    let mut trailers = HeaderMap::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return Err(Http1Error::from_status(StatusCode::BAD_GATEWAY));
        };
        let name = HeaderName::from_bytes(&line[..colon])
            .map_err(|_| Http1Error::from_status(StatusCode::BAD_GATEWAY))?;
        let value = HeaderValue::from_bytes(trim_ascii(&line[colon + 1..]))
            .map_err(|_| Http1Error::from_status(StatusCode::BAD_GATEWAY))?;
        trailers.append(name, value);
    }
    Ok(trailers)
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn is_chunked_only(value: &str) -> bool {
    let mut codings = value.split(',').map(str::trim);
    codings.next().is_some_and(|coding| coding.eq_ignore_ascii_case("chunked"))
        && codings.next().is_none()
}

fn encode_response(mut response: HttpResponse, close: bool) -> IoBuf {
    response.headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&response.body.len().to_string()).unwrap(),
    );
    if close {
        response.headers.insert(http::header::CONNECTION, HeaderValue::from_static("close"));
    }
    let reason = response.status.canonical_reason().unwrap_or("Unknown");
    let mut builder = crate::IoBufBuilder::new();
    builder.put_slice(format!("HTTP/1.1 {} {}\r\n", response.status.as_u16(), reason).as_bytes());
    for (name, value) in &response.headers {
        builder.put_slice(name.as_str().as_bytes());
        builder.put_slice(b": ");
        builder.put_slice(value.as_bytes());
        builder.put_slice(b"\r\n");
    }
    builder.put_slice(b"\r\n");
    let mut frame = builder.finish();
    frame.append_movable(&mut response.body);
    frame
}

fn encode_response_head(
    response: &mut HttpResponse,
    content_length: Option<u64>,
    close: bool,
) -> IoBuf {
    response.headers.remove(http::header::CONTENT_LENGTH);
    response.headers.remove(http::header::TRANSFER_ENCODING);
    match content_length {
        Some(length) => {
            response.headers.insert(
                http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).unwrap(),
            );
        }
        None => {
            response
                .headers
                .insert(http::header::TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        }
    }
    if close {
        response.headers.insert(http::header::CONNECTION, HeaderValue::from_static("close"));
    }
    let reason = response.status.canonical_reason().unwrap_or("Unknown");
    let mut builder = crate::IoBufBuilder::new();
    builder.put_slice(format!("HTTP/1.1 {} {}\r\n", response.status.as_u16(), reason).as_bytes());
    for (name, value) in &response.headers {
        builder.put_slice(name.as_str().as_bytes());
        builder.put_slice(b": ");
        builder.put_slice(value.as_bytes());
        builder.put_slice(b"\r\n");
    }
    builder.put_slice(b"\r\n");
    builder.finish()
}
