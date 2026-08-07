// SPDX-License-Identifier: Apache-2.0

//! Local and peer HTTP/2 settings.

use bytes::BufMut;

use super::frame::{DEFAULT_MAX_FRAME_SIZE, MAX_FRAME_SIZE};
use super::{
    ConnectionError, ErrorCode, Frame, FrameFlags, FrameHeader, FrameKind, H2Error, StreamId,
};
use crate::{IoBuf, IoBufBuilder};

const HEADER_TABLE_SIZE: u16 = 0x1;
const ENABLE_PUSH: u16 = 0x2;
const MAX_CONCURRENT_STREAMS: u16 = 0x3;
const INITIAL_WINDOW_SIZE: u16 = 0x4;
const MAX_FRAME_SIZE_ID: u16 = 0x5;
const MAX_HEADER_LIST_SIZE: u16 = 0x6;
const ENABLE_CONNECT_PROTOCOL: u16 = 0x8;
const MAX_WINDOW_SIZE: u32 = 0x7fff_ffff;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AppliedSettings {
    pub enable_push_present: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub header_table_size: u32,
    pub enable_push: bool,
    pub max_concurrent_streams: Option<u32>,
    pub initial_window_size: u32,
    pub max_frame_size: u32,
    pub max_header_list_size: Option<u32>,
    pub enable_connect_protocol: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            header_table_size: 4096,
            enable_push: false,
            max_concurrent_streams: None,
            initial_window_size: 65_535,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_header_list_size: None,
            enable_connect_protocol: false,
        }
    }
}

impl Settings {
    pub(crate) fn frame(&self, include_enable_push: bool) -> Frame {
        let mut builder = IoBufBuilder::new();
        put(&mut builder, HEADER_TABLE_SIZE, self.header_table_size);
        if include_enable_push {
            put(&mut builder, ENABLE_PUSH, u32::from(self.enable_push));
        }
        if let Some(value) = self.max_concurrent_streams {
            put(&mut builder, MAX_CONCURRENT_STREAMS, value);
        }
        put(&mut builder, INITIAL_WINDOW_SIZE, self.initial_window_size);
        put(&mut builder, MAX_FRAME_SIZE_ID, self.max_frame_size);
        if let Some(value) = self.max_header_list_size {
            put(&mut builder, MAX_HEADER_LIST_SIZE, value);
        }
        if self.enable_connect_protocol {
            put(&mut builder, ENABLE_CONNECT_PROTOCOL, 1);
        }
        let payload = builder.finish();
        Frame {
            header: FrameHeader {
                length: payload.len() as u32,
                kind: FrameKind::Settings,
                flags: FrameFlags::default(),
                stream_id: StreamId::CONNECTION,
            },
            payload,
        }
    }

    /// Applies entries in wire order. Duplicate identifiers intentionally use
    /// the last value, as required by HTTP/2.
    pub(crate) fn apply_payload(&mut self, payload: &IoBuf) -> Result<AppliedSettings, H2Error> {
        if !payload.len().is_multiple_of(6) {
            return Err(connection_error(
                ErrorCode::FrameSizeError,
                "SETTINGS payload is not a multiple of six bytes",
            ));
        }
        let mut applied = AppliedSettings::default();
        let mut entry = [0_u8; 6];
        let mut used = 0;
        for chunk in payload.chunks() {
            for byte in chunk {
                entry[used] = *byte;
                used += 1;
                if used == entry.len() {
                    let id = u16::from_be_bytes([entry[0], entry[1]]);
                    applied.enable_push_present |= id == ENABLE_PUSH;
                    self.apply_entry(
                        id,
                        u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]),
                    )?;
                    used = 0;
                }
            }
        }
        Ok(applied)
    }

    fn apply_entry(&mut self, id: u16, value: u32) -> Result<(), H2Error> {
        match id {
            HEADER_TABLE_SIZE => self.header_table_size = value,
            ENABLE_PUSH => self.enable_push = boolean_setting(value, "invalid ENABLE_PUSH")?,
            MAX_CONCURRENT_STREAMS => self.max_concurrent_streams = Some(value),
            INITIAL_WINDOW_SIZE if value <= MAX_WINDOW_SIZE => self.initial_window_size = value,
            INITIAL_WINDOW_SIZE => {
                return Err(connection_error(
                    ErrorCode::FlowControlError,
                    "INITIAL_WINDOW_SIZE exceeds 2^31-1",
                ));
            }
            MAX_FRAME_SIZE_ID if (DEFAULT_MAX_FRAME_SIZE..=MAX_FRAME_SIZE).contains(&value) => {
                self.max_frame_size = value;
            }
            MAX_FRAME_SIZE_ID => {
                return Err(connection_error(ErrorCode::ProtocolError, "invalid MAX_FRAME_SIZE"));
            }
            MAX_HEADER_LIST_SIZE => self.max_header_list_size = Some(value),
            ENABLE_CONNECT_PROTOCOL => {
                self.enable_connect_protocol =
                    boolean_setting(value, "invalid ENABLE_CONNECT_PROTOCOL")?;
            }
            _ => {}
        }
        Ok(())
    }
}

pub(crate) fn ack_frame() -> Frame {
    Frame {
        header: FrameHeader {
            length: 0,
            kind: FrameKind::Settings,
            flags: FrameFlags::new(FrameFlags::ACK),
            stream_id: StreamId::CONNECTION,
        },
        payload: IoBuf::new(),
    }
}

fn put(builder: &mut IoBufBuilder, id: u16, value: u32) {
    builder.put_u16(id);
    builder.put_u32(value);
}

fn boolean_setting(value: u32, detail: &'static str) -> Result<bool, H2Error> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(connection_error(ErrorCode::ProtocolError, detail)),
    }
}

fn connection_error(code: ErrorCode, detail: &'static str) -> H2Error {
    H2Error::Connection(ConnectionError::new(code, detail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip() {
        let expected = Settings {
            max_concurrent_streams: Some(123),
            initial_window_size: 1_000_000,
            max_header_list_size: Some(8192),
            enable_connect_protocol: true,
            ..Settings::default()
        };
        let frame = expected.frame(true);
        let mut actual = Settings::default();
        actual.apply_payload(&frame.payload).unwrap();
        assert_eq!(actual.max_concurrent_streams, Some(123));
        assert_eq!(actual.initial_window_size, 1_000_000);
        assert_eq!(actual.max_header_list_size, Some(8192));
        assert!(actual.enable_connect_protocol);
    }

    #[test]
    fn invalid_setting_value_is_rejected() {
        let mut builder = IoBufBuilder::new();
        put(&mut builder, MAX_FRAME_SIZE_ID, DEFAULT_MAX_FRAME_SIZE - 1);
        let error = Settings::default().apply_payload(&builder.finish()).unwrap_err();
        assert!(
            matches!(error, H2Error::Connection(error) if error.code == ErrorCode::ProtocolError)
        );
    }
}
