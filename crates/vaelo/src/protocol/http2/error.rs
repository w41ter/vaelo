// SPDX-License-Identifier: Apache-2.0

//! HTTP/2 connection and stream error taxonomy.

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
#[allow(dead_code)] // Keep the complete RFC 7540 wire-code mapping.
pub(crate) enum ErrorCode {
    NoError = 0x0,
    ProtocolError = 0x1,
    InternalError = 0x2,
    FlowControlError = 0x3,
    SettingsTimeout = 0x4,
    StreamClosed = 0x5,
    FrameSizeError = 0x6,
    RefusedStream = 0x7,
    Cancel = 0x8,
    CompressionError = 0x9,
    ConnectError = 0xa,
    EnhanceYourCalm = 0xb,
    InadequateSecurity = 0xc,
    Http11Required = 0xd,
}

impl ErrorCode {
    pub(crate) const fn from_u32(code: u32) -> Option<Self> {
        match code {
            0x0 => Some(Self::NoError),
            0x1 => Some(Self::ProtocolError),
            0x2 => Some(Self::InternalError),
            0x3 => Some(Self::FlowControlError),
            0x4 => Some(Self::SettingsTimeout),
            0x5 => Some(Self::StreamClosed),
            0x6 => Some(Self::FrameSizeError),
            0x7 => Some(Self::RefusedStream),
            0x8 => Some(Self::Cancel),
            0x9 => Some(Self::CompressionError),
            0xa => Some(Self::ConnectError),
            0xb => Some(Self::EnhanceYourCalm),
            0xc => Some(Self::InadequateSecurity),
            0xd => Some(Self::Http11Required),
            _ => None,
        }
    }

    pub(crate) const fn wire_name(self) -> &'static str {
        match self {
            Self::NoError => "NO_ERROR",
            Self::ProtocolError => "PROTOCOL_ERROR",
            Self::InternalError => "INTERNAL_ERROR",
            Self::FlowControlError => "FLOW_CONTROL_ERROR",
            Self::SettingsTimeout => "SETTINGS_TIMEOUT",
            Self::StreamClosed => "STREAM_CLOSED",
            Self::FrameSizeError => "FRAME_SIZE_ERROR",
            Self::RefusedStream => "REFUSED_STREAM",
            Self::Cancel => "CANCEL",
            Self::CompressionError => "COMPRESSION_ERROR",
            Self::ConnectError => "CONNECT_ERROR",
            Self::EnhanceYourCalm => "ENHANCE_YOUR_CALM",
            Self::InadequateSecurity => "INADEQUATE_SECURITY",
            Self::Http11Required => "HTTP_1_1_REQUIRED",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectionError {
    pub code: ErrorCode,
    pub detail: &'static str,
}

impl ConnectionError {
    pub const fn new(code: ErrorCode, detail: &'static str) -> Self {
        Self { code, detail }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum H2Error {
    Connection(ConnectionError),
}

impl fmt::Display for H2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(error) => {
                write!(formatter, "HTTP/2 connection {:?}: {}", error.code, error.detail)
            }
        }
    }
}

impl std::error::Error for H2Error {}

impl From<H2Error> for crate::Error {
    fn from(error: H2Error) -> Self {
        crate::Error::Protocol(error.to_string().into())
    }
}
