// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use http::Response;

use crate::Error;
use crate::http::Incoming;
use crate::transport::inflight::CallToken;

pub(crate) struct HttpCompletion {
    result: Mutex<Option<crate::Result<Response<Incoming>>>>,
}

impl HttpCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { result: Mutex::new(None) })
    }

    pub(crate) fn complete(&self, result: crate::Result<Response<Incoming>>) {
        let mut slot = self.result.lock().expect("HTTP completion mutex poisoned");
        if slot.is_none() {
            *slot = Some(result);
        }
    }

    fn take(&self) -> Option<crate::Result<Response<Incoming>>> {
        self.result.lock().expect("HTTP completion mutex poisoned").take()
    }
}

#[must_use = "await the response or explicitly ignore the HTTP call handle"]
pub struct HttpCall {
    state: HttpCallState,
}

enum HttpCallState {
    Ready(Option<crate::Result<Response<Incoming>>>),
    Pending { token: CallToken, completion: Arc<HttpCompletion> },
    Http2(crate::protocol::http2::client::Http2Call),
    Finished,
}

impl HttpCall {
    pub(crate) fn ready(result: crate::Result<Response<Incoming>>) -> Self {
        Self { state: HttpCallState::Ready(Some(result)) }
    }

    pub(crate) fn pending(token: CallToken, completion: Arc<HttpCompletion>) -> Self {
        Self { state: HttpCallState::Pending { token, completion } }
    }

    pub(crate) fn http2(call: crate::protocol::http2::client::Http2Call) -> Self {
        Self { state: HttpCallState::Http2(call) }
    }

    pub(crate) fn cancel(&self) {
        match &self.state {
            HttpCallState::Pending { token, .. } => {
                let _ = token.cancel();
            }
            HttpCallState::Http2(call) => call.cancel(),
            HttpCallState::Ready(_) | HttpCallState::Finished => {}
        }
    }
}

impl Future for HttpCall {
    type Output = crate::Result<Response<Incoming>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            HttpCallState::Ready(result) => {
                let result = result.take().expect("HttpCall polled after completion");
                self.state = HttpCallState::Finished;
                Poll::Ready(result)
            }
            HttpCallState::Pending { token, completion } => {
                if let Some(result) = completion.take() {
                    self.state = HttpCallState::Finished;
                    return Poll::Ready(result);
                }
                let transport = match token.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(completion) => completion,
                };
                if let Some(status) = transport.status {
                    self.state = HttpCallState::Finished;
                    return Poll::Ready(Err(Error::Channel(status.to_string().into())));
                }
                let result = completion.take().unwrap_or_else(|| {
                    Err(Error::Channel("HTTP response completed without payload".into()))
                });
                self.state = HttpCallState::Finished;
                Poll::Ready(result)
            }
            HttpCallState::Http2(call) => match Pin::new(call).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.state = HttpCallState::Finished;
                    Poll::Ready(result)
                }
            },
            HttpCallState::Finished => panic!("HttpCall polled after completion"),
        }
    }
}
