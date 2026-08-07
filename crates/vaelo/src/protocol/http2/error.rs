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
