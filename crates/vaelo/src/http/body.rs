// SPDX-License-Identifier: Apache-2.0

//! Incremental HTTP bodies backed exclusively by [`crate::IoBuf`].

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::sync::mpsc;

use super::{Error, HeaderMap};
use crate::IoBuf;
use crate::protocol::http2::connection::{FlowCredit, H2RecvStream, ReceivedEvent};

/// One increment of an HTTP message body.
pub enum Frame {
    Data(IoBuf),
    Trailers(HeaderMap),
}

/// Conservative body length information.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SizeHint {
    lower: u64,
    upper: Option<u64>,
}

impl SizeHint {
    pub const fn unknown() -> Self {
        Self { lower: 0, upper: None }
    }

    pub const fn exact(length: u64) -> Self {
        Self { lower: length, upper: Some(length) }
    }

    pub const fn lower(self) -> u64 {
        self.lower
    }

    pub const fn upper(self) -> Option<u64> {
        self.upper
    }
}

/// Incremental HTTP body with natural transport backpressure.
///
/// Implementations yield segmented [`IoBuf`] values directly. The interface
/// never requires conversion to a contiguous byte container. A producer is
/// polled only when the selected HTTP/1 connection or HTTP/2 stream can accept
/// progress.
pub trait Body {
    type Error: Into<Error>;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>>;

    fn size_hint(&self) -> SizeHint {
        SizeHint::unknown()
    }

    /// Returns an already buffered body without dynamic dispatch.
    ///
    /// The default preserves `self` for incremental polling. Built-in bodies
    /// override this so the common empty/full request path can be submitted
    /// directly from `HttpCall::start` without boxing or spawning a task.
    #[doc(hidden)]
    fn try_into_buffer(self) -> Result<IoBuf, Self>
    where
        Self: Sized,
    {
        Err(self)
    }
}

pub trait BodyExt: Body {
    fn frame(&mut self) -> impl Future<Output = Option<Result<Frame, Self::Error>>> + '_
    where
        Self: Unpin,
    {
        poll_fn(|context| Pin::new(&mut *self).poll_frame(context))
    }

    fn collect(self) -> Collect<Self>
    where
        Self: Sized,
    {
        Collect { body: self, limit: None, buffer: IoBuf::new() }
    }
}

impl<T: Body + ?Sized> BodyExt for T {}

#[derive(Default)]
pub struct Empty;

impl Empty {
    pub const fn new() -> Self {
        Self
    }
}

impl Body for Empty {
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>> {
        Poll::Ready(None)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(0)
    }

    fn try_into_buffer(self) -> Result<IoBuf, Self> {
        Ok(IoBuf::new())
    }
}

pub struct Full {
    buffer: Option<IoBuf>,
    length: u64,
}

impl Full {
    pub fn new(buffer: IoBuf) -> Self {
        let length = buffer.len() as u64;
        Self { buffer: Some(buffer), length }
    }
}

impl From<IoBuf> for Full {
    fn from(buffer: IoBuf) -> Self {
        Self::new(buffer)
    }
}

impl Body for Full {
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>> {
        Poll::Ready(self.buffer.take().map(|buffer| Ok(Frame::Data(buffer))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::exact(self.length)
    }

    fn try_into_buffer(mut self) -> Result<IoBuf, Self> {
        Ok(self.buffer.take().unwrap_or_default())
    }
}

pin_project! {
    /// Adapts an asynchronous frame stream without requiring it to be
    /// Unpin or boxed. The adapter remains generic over S so streaming bodies
    /// do not force dynamic dispatch into the common HTTP path.
    pub struct Stream<S> {
        #[pin]
        inner: S,
    }
}

impl<S> Stream<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E> Body for Stream<S>
where
    S: tokio_stream::Stream<Item = Result<Frame, E>>,
    E: Into<Error>,
{
    type Error = E;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>> {
        self.project().inner.poll_next(context)
    }
}

/// A response/request body received from HTTP/1 or HTTP/2.
///
/// Dropping it before EOS abandons the body. HTTP/2 must release/reset the
/// stream; HTTP/1 may make the underlying connection non-reusable.
pub struct Incoming {
    inner: IncomingInner,
}

enum IncomingInner {
    Buffered(Option<Result<Frame, Error>>),
    Streaming { receiver: mpsc::Receiver<Result<Frame, Error>>, hint: SizeHint },
    Http2 { stream: H2RecvStream, terminal: bool, hint: SizeHint, credit: Option<FlowCredit> },
}

impl Incoming {
    pub(crate) fn buffered(buffer: IoBuf) -> Self {
        let frame = (!buffer.is_empty()).then_some(Ok(Frame::Data(buffer)));
        Self { inner: IncomingInner::Buffered(frame) }
    }

    pub(crate) fn channel(capacity: usize, hint: SizeHint) -> (IncomingSender, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (IncomingSender(sender), Self { inner: IncomingInner::Streaming { receiver, hint } })
    }

    pub(crate) fn http2(stream: H2RecvStream, terminal: bool, hint: SizeHint) -> Self {
        Self { inner: IncomingInner::Http2 { stream, terminal, hint, credit: None } }
    }
}

pub(crate) struct IncomingSender(mpsc::Sender<Result<Frame, Error>>);

impl IncomingSender {
    pub(crate) async fn send(
        &self,
        frame: Result<Frame, Error>,
    ) -> Result<(), Result<Frame, Error>> {
        self.0.send(frame).await.map_err(|error| error.0)
    }
}

impl Body for Incoming {
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame, Self::Error>>> {
        // SAFETY: neither enum variant contains a self-referential value.
        match unsafe { &mut self.get_unchecked_mut().inner } {
            IncomingInner::Buffered(frame) => Poll::Ready(frame.take()),
            IncomingInner::Streaming { receiver, .. } => receiver.poll_recv(context),
            IncomingInner::Http2 { stream, terminal, credit, .. } => {
                // Asking for the next frame proves that the previous DATA frame
                // was consumed, so its flow-control credit may now be returned.
                credit.take();
                if *terminal {
                    return Poll::Ready(None);
                }
                match std::pin::pin!(stream.recv()).poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        *terminal = true;
                        Poll::Ready(Some(Err(Error::Transport(format!(
                            "HTTP/2 response stream failed: {error:?}"
                        )))))
                    }
                    Poll::Ready(Ok(None)) => {
                        *terminal = true;
                        Poll::Ready(None)
                    }
                    Poll::Ready(Ok(Some(ReceivedEvent::Data(data)))) => {
                        let (payload, end_stream, flow_credit) = data.into_parts_with_credit();
                        *credit = Some(flow_credit);
                        *terminal = end_stream;
                        Poll::Ready(Some(Ok(Frame::Data(payload))))
                    }
                    Poll::Ready(Ok(Some(ReceivedEvent::Headers {
                        headers,
                        end_stream,
                        trailers,
                    }))) => {
                        if !trailers {
                            return Poll::Ready(Some(Err(Error::Protocol(
                                "unexpected HTTP/2 informational headers".into(),
                            ))));
                        }
                        let mut output = HeaderMap::new();
                        for field in headers.fields() {
                            let Ok(name) = http::HeaderName::from_bytes(field.name.as_ref()) else {
                                return Poll::Ready(Some(Err(Error::Protocol(
                                    "invalid HTTP/2 trailer name".into(),
                                ))));
                            };
                            let Ok(value) = http::HeaderValue::from_bytes(field.value.as_ref())
                            else {
                                return Poll::Ready(Some(Err(Error::Protocol(
                                    "invalid HTTP/2 trailer value".into(),
                                ))));
                            };
                            output.append(name, value);
                        }
                        *terminal = end_stream;
                        Poll::Ready(Some(Ok(Frame::Trailers(output))))
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            IncomingInner::Buffered(None) => SizeHint::exact(0),
            IncomingInner::Buffered(Some(Ok(Frame::Data(data)))) => {
                SizeHint::exact(data.len() as u64)
            }
            IncomingInner::Streaming { hint, .. } => *hint,
            IncomingInner::Http2 { hint, .. } => *hint,
            _ => SizeHint::unknown(),
        }
    }
}

pin_project! {
    /// Collects body data into a segmented IoBuf.
    ///
    /// The future pins the body in place and therefore supports body
    /// implementations which are not Unpin without boxing.
    pub struct Collect<B> {
        #[pin]
        body: B,
        limit: Option<usize>,
        buffer: IoBuf,
    }
}

impl<B> Collect<B> {
    /// Rejects the body with [`Error::Body`] once collected data would exceed
    /// `limit`; already collected segments never need to be coalesced.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl<B> Future for Collect<B>
where
    B: Body,
{
    type Output = Result<IoBuf, Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.body.as_mut().poll_frame(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(std::mem::take(this.buffer))),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Some(Ok(Frame::Trailers(_)))) => {}
                Poll::Ready(Some(Ok(Frame::Data(mut data)))) => {
                    if this
                        .limit
                        .is_some_and(|limit| this.buffer.len().saturating_add(data.len()) > limit)
                    {
                        return Poll::Ready(Err(Error::Body(
                            "collected HTTP body exceeds configured limit".into(),
                        )));
                    }
                    this.buffer.append_movable(&mut data);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;
    use crate::IoBufBuilder;

    fn data(bytes: &[u8]) -> IoBuf {
        let mut builder = IoBufBuilder::new();
        builder.put_slice(bytes);
        builder.finish()
    }

    #[tokio::test]
    async fn collect_preserves_segmented_frames_and_enforces_limit() {
        let body = Stream::new(tokio_stream::iter([
            Ok::<_, Error>(Frame::Data(data(b"hello"))),
            Ok(Frame::Trailers(HeaderMap::new())),
            Ok(Frame::Data(data(b" world"))),
        ]));
        let collected = body.collect().limit(11).await.unwrap();
        assert_eq!(collected.chunks().flatten().copied().collect::<Vec<_>>(), b"hello world");

        let body = Full::new(data(b"too large"));
        assert!(matches!(body.collect().limit(3).await, Err(Error::Body(_))));
    }
}
