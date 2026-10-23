// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use super::{BodyDecodeEvent, Http1Protocol};
use crate::error::{Error, Result};
use crate::server::ServerSessionContext;
use crate::server::http::{HttpRegistry, HttpResponse, HttpServerOptions, dispatch};
use crate::transport::tcp::{TcpConnection, WriteOptions};
use crate::{IoBuf, Resource, StaticResource};

pub(super) fn new_session(
    connection: Resource<TcpConnection>,
    protocol: &'static Http1Protocol,
    context: ServerSessionContext,
) -> Result<Resource<ServerSession>> {
    ServerSession::resource_pool()
        .acquire_with_init(|| ServerSession {
            connection,
            protocol,
            registry: context.http_registry,
            builtin_registry: context.builtin_registry,
            services: context.services,
            options: context.http_options.clone(),
            capacity: Arc::new(Semaphore::new(context.http_options.max_pipeline_depth.max(1))),
            next_request: AtomicU64::new(0),
            response_owner: AtomicBool::new(false),
            completed: Mutex::new(OrderedResponses::default()),
        })
        .map_err(|_| Error::ResourceExhausted)
}

#[derive(vaelo_derive::StaticResource)]
pub(crate) struct ServerSession {
    connection: Resource<TcpConnection>,
    protocol: &'static Http1Protocol,
    registry: HttpRegistry,
    builtin_registry: HttpRegistry,
    services: crate::ServiceRegistry,
    options: HttpServerOptions,
    capacity: Arc<Semaphore>,
    next_request: AtomicU64,
    response_owner: AtomicBool,
    completed: Mutex<OrderedResponses>,
}

#[derive(Default)]
struct OrderedResponses {
    next: u64,
    responses: BTreeMap<u64, (HttpResponse, bool)>,
}

impl ServerSession {
    pub(crate) fn handle_read_event(self: Resource<Self>, input: IoBuf) {
        tokio::spawn(Self::read_next_request(self, input));
    }

    /// Reads one HTTP/1 request and then hands the remaining input to a
    /// successor reader before awaiting service code.
    ///
    /// HTTP/1 differs from bRPC here: handler tasks may run concurrently, but
    /// responses must be written in request order. `sequence` and
    /// `OrderedResponses` therefore form the session-level FIFO restore point.
    /// A request that requires `Connection: close` is a write barrier: no
    /// successor reader is started after it.
    async fn read_next_request(self: Resource<Self>, mut input: IoBuf) {
        loop {
            match self.protocol.decode_server_request(
                &mut input,
                *self.connection.remote_addr(),
                &self.options,
            ) {
                Ok(Some(decoded)) => {
                    let mut request = decoded.request;
                    let mut body = decoded.body;
                    let close = decoded.close_after_response;
                    let sequence = self.next_request.fetch_add(1, Ordering::Relaxed);
                    let permit = match self.capacity.clone().acquire_owned().await {
                        Ok(permit) => permit,
                        Err(error) => {
                            log::error!(
                                "HTTP/1 pipeline semaphore closed: remote={}, error={error}",
                                self.connection.remote_addr()
                            );
                            return;
                        }
                    };
                    let hint = body.size_hint();
                    let (sender, incoming) = crate::http::Incoming::channel(16, hint);
                    request.body = incoming;
                    let handler = self.clone();
                    tokio::spawn(async move {
                        let response = dispatch(
                            handler.registry.clone(),
                            handler.builtin_registry.clone(),
                            handler.services.clone(),
                            handler.options.clone(),
                            request,
                        )
                        .await;
                        drop(permit);
                        handler.publish_response(sequence, response, close).await;
                    });

                    loop {
                        match body.decode(&mut input) {
                            Ok(Some(BodyDecodeEvent::Frame(frame))) => {
                                // A handler may intentionally ignore its body. Continue decoding
                                // and discarding in that case so the pipeline remains aligned.
                                let _ = sender.send(Ok(frame)).await;
                            }
                            Ok(Some(BodyDecodeEvent::End)) => break,
                            Ok(None) => match self.connection.read_more(&mut input).await {
                                Ok(0) | Err(_) => {
                                    let _ = sender
                                        .send(Err(crate::http::Error::Transport(
                                            "connection closed during HTTP/1 request body".into(),
                                        )))
                                        .await;
                                    self.connection.close();
                                    return;
                                }
                                Ok(_) => {}
                            },
                            Err(error) => {
                                let _ = sender
                                    .send(Err(crate::http::Error::Protocol(format!(
                                        "invalid HTTP/1 request body: {}",
                                        error.status()
                                    ))))
                                    .await;
                                self.connection.close();
                                return;
                            }
                        }
                    }
                    drop(sender);
                    if close {
                        return;
                    }
                }
                Ok(None) => match self.connection.read_more(&mut input).await {
                    Ok(0) => {
                        log::debug!(
                            "server peer closed connection: remote={}",
                            self.connection.remote_addr()
                        );
                        self.connection.close();
                        return;
                    }
                    Err(_) => {
                        // read_more logs the underlying I/O error.
                        self.connection.close();
                        return;
                    }
                    Ok(_) => {}
                },
                Err(error) => {
                    log::warn!(
                        "HTTP/1 request decode failed: remote={}, status={}",
                        self.connection.remote_addr(),
                        error.status()
                    );
                    if let Err(error) = TcpConnection::submit_write(
                        self.connection.clone(),
                        self.protocol
                            .encode_server_response(HttpResponse::empty(error.status()), true),
                        WriteOptions::server().close_after_write(),
                    ) {
                        log::error!(
                            "HTTP/1 error response submission failed: remote={}, error={error}",
                            self.connection.remote_addr()
                        );
                        self.connection.close();
                    }
                    return;
                }
            }
        }
    }

    async fn publish_response(
        self: Resource<Self>,
        sequence: u64,
        response: HttpResponse,
        close: bool,
    ) {
        {
            let mut completed = self.completed.lock().unwrap();
            completed.responses.insert(sequence, (response, close));
        }
        if self.response_owner.swap(true, Ordering::AcqRel) {
            return;
        }

        loop {
            let ready = {
                let mut completed = self.completed.lock().unwrap();
                let next = completed.next;
                let ready = completed.responses.remove(&next);
                if ready.is_some() {
                    completed.next += 1;
                }
                ready
            };
            if let Some((response, close)) = ready {
                if let Err(status) = self.write_response(response, close).await {
                    log::error!(
                        "HTTP/1 response write failed: remote={}, status={status}",
                        self.connection.remote_addr()
                    );
                    self.connection.close();
                    self.response_owner.store(false, Ordering::Release);
                    return;
                }
                continue;
            }

            self.response_owner.store(false, Ordering::Release);
            let has_next = {
                let completed = self.completed.lock().unwrap();
                completed.responses.contains_key(&completed.next)
            };
            if !has_next
                || self
                    .response_owner
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
        }
    }

    async fn write_response(
        &self,
        mut response: HttpResponse,
        close: bool,
    ) -> Result<(), crate::Status> {
        let Some(mut body) = response.streaming.take() else {
            let frame = self.protocol.encode_server_response(response, close);
            TcpConnection::submit_write(
                self.connection.clone(),
                frame,
                if close {
                    WriteOptions::server().close_after_write()
                } else {
                    WriteOptions::server()
                },
            )
            .map_err(crate::Status::from)?;
            return Ok(());
        };

        let hint = body.as_ref().get_ref().size_hint();
        let exact = (hint.upper() == Some(hint.lower())).then_some(hint.lower());
        let head = self.protocol.encode_streaming_response_head(&mut response, exact, close);
        TcpConnection::write_frame(self.connection.clone(), head, false).await?;
        let mut remaining = exact;
        let mut trailers = None;
        while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            match frame.map_err(|error| {
                crate::Status::new(crate::StatusCode::Internal, error.to_string())
            })? {
                crate::http::Frame::Data(data) if data.is_empty() => {}
                crate::http::Frame::Data(data) => {
                    if let Some(left) = &mut remaining {
                        let length = data.len() as u64;
                        if length > *left {
                            return Err(crate::Status::new(
                                crate::StatusCode::Internal,
                                "HTTP response body exceeds Content-Length",
                            ));
                        }
                        *left -= length;
                        TcpConnection::write_frame(self.connection.clone(), data, false).await?;
                    } else {
                        let frame = self.protocol.encode_chunk(data);
                        TcpConnection::write_frame(self.connection.clone(), frame, false).await?;
                    }
                }
                crate::http::Frame::Trailers(value) => {
                    if exact.is_some() {
                        return Err(crate::Status::new(
                            crate::StatusCode::Internal,
                            "HTTP trailers require chunked transfer encoding",
                        ));
                    }
                    trailers = Some(value);
                }
            }
        }
        if remaining.is_some_and(|remaining| remaining != 0) {
            return Err(crate::Status::new(
                crate::StatusCode::Internal,
                "HTTP response body ended before Content-Length",
            ));
        }
        if exact.is_none() {
            TcpConnection::write_frame(
                self.connection.clone(),
                self.protocol.encode_last_chunk(trailers),
                close,
            )
            .await?;
        } else if close {
            self.connection.close();
        }
        Ok(())
    }
}
