// SPDX-License-Identifier: Apache-2.0

//! Zero-copy HTTP/2 frame envelope.

use bytes::BufMut;

use super::{ConnectionError, ErrorCode, H2Error};
use crate::{IoBuf, IoBufBuilder};

pub(crate) const FRAME_HEADER_LEN: usize = 9;
pub(crate) const DEFAULT_MAX_FRAME_SIZE: u32 = 16 * 1024;
pub(crate) const MAX_FRAME_SIZE: u32 = (1 << 24) - 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct StreamId(u32);

impl StreamId {
    pub const CONNECTION: Self = Self(0);

    pub const fn new(raw: u32) -> Option<Self> {
        if raw & 0x8000_0000 == 0 { Some(Self(raw)) } else { None }
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub const fn is_connection(self) -> bool {
        self.0 == 0
    }

    pub const fn is_client_initiated(self) -> bool {
        self.0 != 0 && self.0 & 1 == 1
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameKind {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    GoAway,
    WindowUpdate,
    Continuation,
    Unknown(u8),
}

impl FrameKind {
    pub const fn from_wire(value: u8) -> Self {
        match value {
            0x0 => Self::Data,
            0x1 => Self::Headers,
            0x2 => Self::Priority,
            0x3 => Self::RstStream,
            0x4 => Self::Settings,
            0x5 => Self::PushPromise,
            0x6 => Self::Ping,
            0x7 => Self::GoAway,
            0x8 => Self::WindowUpdate,
            0x9 => Self::Continuation,
            other => Self::Unknown(other),
        }
    }

    pub const fn wire(self) -> u8 {
        match self {
            Self::Data => 0x0,
            Self::Headers => 0x1,
            Self::Priority => 0x2,
            Self::RstStream => 0x3,
            Self::Settings => 0x4,
            Self::PushPromise => 0x5,
            Self::Ping => 0x6,
            Self::GoAway => 0x7,
            Self::WindowUpdate => 0x8,
            Self::Continuation => 0x9,
            Self::Unknown(value) => value,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FrameFlags(u8);

impl FrameFlags {
    pub const ACK: u8 = 0x1;
    pub const END_STREAM: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;

    pub const fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn contains(self, flag: u8) -> bool {
        self.0 & flag != 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
    pub length: u32,
    pub kind: FrameKind,
    pub flags: FrameFlags,
    pub stream_id: StreamId,
}

#[derive(Debug)]
pub(crate) struct Frame {
    pub header: FrameHeader,
    pub payload: IoBuf,
}

pub(crate) fn decode(input: &mut IoBuf, max_frame_size: u32) -> Result<Option<Frame>, H2Error> {
    if input.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }

    // A frame header may straddle pooled blocks. Copy only its fixed nine
    // bytes; the payload remains zero-copy in `IoBuf` references.
    let mut header = [0_u8; FRAME_HEADER_LEN];
    let mut copied = 0;
    for chunk in input.chunks() {
        let count = (FRAME_HEADER_LEN - copied).min(chunk.len());
        header[copied..copied + count].copy_from_slice(&chunk[..count]);
        copied += count;
        if copied == FRAME_HEADER_LEN {
            break;
        }
    }

    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
    if length > max_frame_size || length > MAX_FRAME_SIZE {
        return Err(H2Error::Connection(ConnectionError::new(
            ErrorCode::FrameSizeError,
            "inbound frame exceeds the configured maximum",
        )));
    }
    let frame_len = FRAME_HEADER_LEN + length as usize;
    if input.len() < frame_len {
        return Ok(None);
    }

    // The high bit is reserved and recipients must ignore it.
    let raw_stream_id =
        u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let stream_id = StreamId::new(raw_stream_id).expect("masked stream id is valid");
    input.consume(FRAME_HEADER_LEN);
    let payload = input.split_to(length as usize);
    Ok(Some(Frame {
        header: FrameHeader {
            length,
            kind: FrameKind::from_wire(header[3]),
            flags: FrameFlags::new(header[4]),
            stream_id,
        },
        payload,
    }))
}

pub(crate) fn encode(mut frame: Frame) -> Result<IoBuf, H2Error> {
    if frame.header.length != frame.payload.len() as u32 || frame.header.length > MAX_FRAME_SIZE {
        return Err(H2Error::Connection(ConnectionError::new(
            ErrorCode::FrameSizeError,
            "outbound frame length is invalid",
        )));
    }
    let length = frame.header.length;
    let mut builder = IoBufBuilder::new();
    builder.put_u8((length >> 16) as u8);
    builder.put_u8((length >> 8) as u8);
    builder.put_u8(length as u8);
    builder.put_u8(frame.header.kind.wire());
    builder.put_u8(frame.header.flags.bits());
    builder.put_u32(frame.header.stream_id.as_u32());
    let mut output = builder.finish();
    output.append_movable(&mut frame.payload);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    fn input(bytes: &[u8]) -> IoBuf {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(bytes);
        builder.finish()
    }

    #[test]
    fn incomplete_frame_is_not_consumed() {
        let mut bytes = input(&[0, 0, 3, 0, 0, 0, 0, 0, 1, 1, 2]);
        assert!(decode(&mut bytes, DEFAULT_MAX_FRAME_SIZE).unwrap().is_none());
        assert_eq!(bytes.len(), 11);
    }

    #[test]
    fn frame_payload_is_split_without_flattening() {
        let mut bytes = input(&[0, 0, 3, 0, 1, 0, 0, 0, 1, 1, 2, 3]);
        let frame = decode(&mut bytes, DEFAULT_MAX_FRAME_SIZE).unwrap().unwrap();
        assert_eq!(frame.header.kind, FrameKind::Data);
        assert_eq!(frame.header.stream_id.as_u32(), 1);
        assert!(frame.header.flags.contains(FrameFlags::END_STREAM));
        assert_eq!(frame.payload.chunks().flatten().copied().collect::<Vec<_>>(), [1, 2, 3]);
        assert!(bytes.is_empty());
    }

    #[test]
    fn oversized_frame_is_rejected_before_payload_arrives() {
        let mut bytes = input(&[0, 0x40, 1, 0, 0, 0, 0, 0, 1]);
        let error = decode(&mut bytes, DEFAULT_MAX_FRAME_SIZE).unwrap_err();
        assert!(
            matches!(error, H2Error::Connection(error) if error.code == ErrorCode::FrameSizeError)
        );
        assert_eq!(bytes.len(), FRAME_HEADER_LEN);
    }
}
