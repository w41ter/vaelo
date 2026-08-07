// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;

use tokio_stream::Stream;

use crate::{IoBuf, MetadataMap, Status};

pub type BoxResponseStream = Pin<Box<dyn Stream<Item = Result<IoBuf, Status>> + Send + 'static>>;

pub enum ServerResponseBody {
    Unary(IoBuf),
    Stream(Vec<IoBuf>),
    Incremental(BoxResponseStream),
}

impl ServerResponseBody {
    pub fn unary(payload: IoBuf) -> Self {
        Self::Unary(payload)
    }

    pub fn stream(messages: Vec<IoBuf>) -> Self {
        Self::Stream(messages)
    }

    pub fn into_unary(self) -> IoBuf {
        match self {
            Self::Unary(payload) => payload,
            Self::Stream(mut messages) => messages.pop().unwrap_or_else(IoBuf::new),
            Self::Incremental(_) => IoBuf::new(),
        }
    }
}

pub struct ServerResponse {
    pub body: ServerResponseBody,
    pub attachment: IoBuf,
    pub status: Option<Status>,
    pub metadata: MetadataMap,
    pub trailing_metadata: MetadataMap,
}

impl ServerResponse {
    pub fn success(payload: IoBuf) -> Self {
        Self::success_body(ServerResponseBody::unary(payload))
    }

    pub fn streaming(messages: Vec<IoBuf>) -> Self {
        Self::success_body(ServerResponseBody::stream(messages))
    }

    pub(crate) fn incremental(stream: BoxResponseStream) -> Self {
        Self::success_body(ServerResponseBody::Incremental(stream))
    }

    fn success_body(body: ServerResponseBody) -> Self {
        Self {
            body,
            attachment: IoBuf::new(),
            status: None,
            metadata: MetadataMap::new(),
            trailing_metadata: MetadataMap::new(),
        }
    }

    pub fn failed(status: Status) -> Self {
        Self {
            body: ServerResponseBody::unary(IoBuf::new()),
            attachment: IoBuf::new(),
            status: Some(status),
            metadata: MetadataMap::new(),
            trailing_metadata: MetadataMap::new(),
        }
    }
}
