// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::fmt;

/// Owned status-detail bytes.
///
/// Owned vectors are transferred without copying and static bytes can be
/// retained directly. The private representation may later reference shared
/// protocol receive buffers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusDetails(StatusDetailsRepr);

#[derive(Clone, Debug, Eq, PartialEq)]
enum StatusDetailsRepr {
    Static(&'static [u8]),
    Owned(Vec<u8>),
}

impl StatusDetails {
    pub const fn from_static(details: &'static [u8]) -> Self {
        Self(StatusDetailsRepr::Static(details))
    }

    pub fn from_vec(details: Vec<u8>) -> Self {
        Self(StatusDetailsRepr::Owned(details))
    }

    pub fn copy_from_slice(details: &[u8]) -> Self {
        Self::from_vec(details.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            StatusDetailsRepr::Static(details) => details,
            StatusDetailsRepr::Owned(details) => details,
        }
    }
}

impl Default for StatusDetails {
    fn default() -> Self {
        Self::from_static(b"")
    }
}

impl From<Vec<u8>> for StatusDetails {
    fn from(details: Vec<u8>) -> Self {
        Self::from_vec(details)
    }
}

impl From<String> for StatusDetails {
    fn from(details: String) -> Self {
        Self::from_vec(details.into_bytes())
    }
}

/// Client-side call failure.
///
/// This set is non-exhaustive so transports can expose more precise local
/// failure stages in future releases without collapsing them into strings or
/// breaking callers. Matchers should retain a conservative fallback arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Encode(Cow<'static, str>),
    Decode(Cow<'static, str>),
    Protocol(Cow<'static, str>),
    Channel(Cow<'static, str>),
    ResourceExhausted,
    /// A status returned by the remote RPC service.
    Remote(Status),
}

/// What a client can safely infer about remote execution.
///
/// The classification is intentionally conservative and does not expose
/// transport-internal byte counts or retry machinery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExecutionCertainty {
    NotExecuted,
    PossiblyExecuted,
    Executed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(message) => write!(f, "encode error: {message}"),
            Self::Decode(message) => write!(f, "decode error: {message}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Channel(message) => write!(f, "channel error: {message}"),
            Self::ResourceExhausted => f.write_str("call resources exhausted"),
            Self::Remote(status) => write!(f, "remote RPC status: {status}"),
        }
    }
}

impl Error {
    /// Returns the remote status when this error was produced by the service.
    pub fn remote_status(&self) -> Option<&Status> {
        match self {
            Self::Remote(status) => Some(status),
            _ => None,
        }
    }

    pub fn execution_certainty(&self) -> ExecutionCertainty {
        match self {
            Self::Encode(_) | Self::ResourceExhausted => ExecutionCertainty::NotExecuted,
            Self::Remote(_) => ExecutionCertainty::Executed,
            Self::Decode(_) | Self::Protocol(_) | Self::Channel(_) => {
                ExecutionCertainty::PossiblyExecuted
            }
        }
    }

    pub fn is_timeout(&self) -> bool {
        self.remote_status().is_some_and(|status| status.code() == StatusCode::DeadlineExceeded)
    }

    pub fn is_cancelled(&self) -> bool {
        self.remote_status().is_some_and(|status| status.code() == StatusCode::Canceled)
    }
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self::Remote(status)
    }
}

impl std::error::Error for Error {}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        match error {
            Error::ResourceExhausted => {
                Status::new(StatusCode::ResourceExhausted, "call resources exhausted")
            }
            Error::Remote(status) => status,
            other => Status::new(StatusCode::Internal, other.to_string()),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum StatusCode {
    #[default]
    Ok = 0,
    Canceled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl StatusCode {
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    pub const fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Ok),
            1 => Some(Self::Canceled),
            2 => Some(Self::Unknown),
            3 => Some(Self::InvalidArgument),
            4 => Some(Self::DeadlineExceeded),
            5 => Some(Self::NotFound),
            6 => Some(Self::AlreadyExists),
            7 => Some(Self::PermissionDenied),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::FailedPrecondition),
            10 => Some(Self::Aborted),
            11 => Some(Self::OutOfRange),
            12 => Some(Self::Unimplemented),
            13 => Some(Self::Internal),
            14 => Some(Self::Unavailable),
            15 => Some(Self::DataLoss),
            16 => Some(Self::Unauthenticated),
            _ => None,
        }
    }
}

/// Stable error surface exposed by generated stubs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Status {
    code: StatusCode,
    message: Cow<'static, str>,
    details: StatusDetails,
    metadata: crate::MetadataMap,
}

impl Status {
    pub fn new(code: StatusCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            message: message.into(),
            details: StatusDetails::default(),
            metadata: crate::MetadataMap::new(),
        }
    }

    pub fn code(&self) -> StatusCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn canceled(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::Canceled, message)
    }

    pub fn unknown(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::Unknown, message)
    }

    pub fn invalid_argument(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::InvalidArgument, message)
    }

    pub fn deadline_exceeded(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::DeadlineExceeded, message)
    }

    pub fn not_found(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::NotFound, message)
    }

    pub fn already_exists(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::AlreadyExists, message)
    }

    pub fn resource_exhausted(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::ResourceExhausted, message)
    }

    pub fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::Internal, message)
    }

    pub fn unavailable(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::Unavailable, message)
    }

    /// Attaches owned details without requiring a copy.
    ///
    /// Borrowed details must be copied explicitly with
    /// StatusDetails::copy_from_slice.
    pub fn with_details(mut self, details: impl Into<StatusDetails>) -> Self {
        self.details = details.into();
        self
    }

    pub fn details(&self) -> &[u8] {
        self.details.as_bytes()
    }

    pub fn metadata(&self) -> &crate::MetadataMap {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut crate::MetadataMap {
        &mut self.metadata
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Status {}

#[cfg(test)]
mod tests {
    use super::StatusCode;

    #[test]
    fn grpc_status_codes_round_trip_the_standard_wire_range() {
        for value in 0..=16 {
            let code = StatusCode::from_i32(value).expect("standard gRPC status code");
            assert_eq!(code.as_i32(), value);
        }
        assert_eq!(StatusCode::from_i32(17), None);
    }
}
