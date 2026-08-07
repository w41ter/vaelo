// SPDX-License-Identifier: Apache-2.0

//! bRPC/BAIDU_STD-specific request extensions.
//!
//! The common [`crate::Request`] intentionally contains only protocol-neutral
//! state. Construct this wrapper when a call needs a BAIDU_STD request id,
//! log id, or attachment. Passing it to a non-bRPC channel is rejected before
//! the request is submitted; protocol-only options are never silently ignored.

use crate::{IntoRequest, IoBuf};

/// An RPC request carrying BAIDU_STD-only options.
pub struct Request<T> {
    inner: crate::Request<T>,
    request_id: Option<String>,
    log_id: Option<u64>,
    attachment: IoBuf,
}

impl<T> Request<T> {
    pub fn new(message: T) -> Self {
        Self::from(crate::Request::new(message))
    }

    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn log_id(mut self, log_id: u64) -> Self {
        self.log_id = Some(log_id);
        self
    }

    pub fn attachment(mut self, attachment: IoBuf) -> Self {
        self.attachment = attachment;
        self
    }
}

impl<T> IntoRequest<T> for Request<T> {
    fn into_request(self) -> crate::Request<T> {
        let mut request = self.inner;
        request.options.request_id = self.request_id;
        request.options.log_id = self.log_id;
        request.attachment = self.attachment;
        request
    }
}

impl<T> From<crate::Request<T>> for Request<T> {
    fn from(inner: crate::Request<T>) -> Self {
        Self { inner, request_id: None, log_id: None, attachment: IoBuf::new() }
    }
}

/// Access to a bRPC response attachment without adding protocol-only methods
/// to the common [`crate::Response`] surface.
pub trait ResponseExt {
    fn attachment(&self) -> &IoBuf;
    fn take_attachment(&mut self) -> IoBuf;
    fn set_attachment(&mut self, attachment: IoBuf);
}

/// Access to BAIDU_STD fields on a server-side bRPC request.
pub trait ServerRequestExt {
    fn attachment(&self) -> &IoBuf;
    fn take_attachment(&mut self) -> IoBuf;
    fn request_id(&self) -> Option<&str>;
    fn log_id(&self) -> Option<u64>;
}

impl<T> ServerRequestExt for crate::Request<T> {
    fn attachment(&self) -> &IoBuf {
        &self.attachment
    }

    fn take_attachment(&mut self) -> IoBuf {
        std::mem::take(&mut self.attachment)
    }

    fn request_id(&self) -> Option<&str> {
        self.options.request_id.as_deref()
    }

    fn log_id(&self) -> Option<u64> {
        self.options.log_id
    }
}

impl ServerRequestExt for crate::RequestContext {
    fn attachment(&self) -> &IoBuf {
        &self.attachment
    }

    fn take_attachment(&mut self) -> IoBuf {
        std::mem::take(&mut self.attachment)
    }

    fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    fn log_id(&self) -> Option<u64> {
        self.log_id
    }
}

impl<T> ResponseExt for crate::Response<T> {
    fn attachment(&self) -> &IoBuf {
        self.attachment()
    }

    fn take_attachment(&mut self) -> IoBuf {
        self.take_attachment()
    }

    fn set_attachment(&mut self, attachment: IoBuf) {
        self.set_attachment(attachment);
    }
}
