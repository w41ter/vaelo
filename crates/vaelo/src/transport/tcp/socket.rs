// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::io::IoSliceMut;
use std::sync::Mutex;

use super::TcpTransport;
use crate::foundation::stale_id_list::{StaleIdList, StaleIdListAdapter};
use crate::transport::inflight::CallContext;
use crate::{IoBuf, Resource, StaticResource, Status, StatusCode, Transport};

const READV_SLICES: usize = 8;

/// One established physical socket.
///
/// The socket is a pooled object so read/write/connect tasks can share a stable
/// handle without keeping the whole connection behind `Arc`.
#[derive(StaticResource)]
pub(crate) struct TcpSocket {
    lifecycle: Mutex<TcpSocketLifecycle>,
    transport: TcpTransport,
}

struct TcpSocketLifecycle {
    failure: Option<Status>,
    pending: StaleIdList<PendingCallAdapter>,
}

pub(super) enum RegisterPendingError {
    Failed { context: Resource<CallContext>, status: Status },
    Capacity(Resource<CallContext>),
}

impl TcpSocket {
    pub(crate) fn new(transport: TcpTransport) -> Self {
        Self {
            lifecycle: Mutex::new(TcpSocketLifecycle {
                failure: None,
                pending: StaleIdList::new(PendingCallAdapter),
            }),
            transport,
        }
    }

    pub(crate) fn transport(&self) -> &TcpTransport {
        &self.transport
    }

    pub(super) fn failure(&self) -> Status {
        self.lifecycle
            .lock()
            .unwrap()
            .failure
            .clone()
            .unwrap_or_else(|| Status::new(StatusCode::Unavailable, "connection failed"))
    }

    pub(super) fn begin_invalidate(
        &self,
        status: Status,
    ) -> std::result::Result<StaleIdList<PendingCallAdapter>, Status> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if let Some(failure) = &lifecycle.failure {
            return Err(failure.clone());
        }
        lifecycle.failure = Some(status);
        let mut detached = StaleIdList::new(PendingCallAdapter);
        lifecycle.pending.swap(&mut detached);
        Ok(detached)
    }

    pub(crate) async fn read_more(&self, input: &mut IoBuf) -> io::Result<usize> {
        loop {
            self.transport.readable().await?;
            let mut reservation = input.reserve::<READV_SLICES>();
            let result = {
                let mut slices: [IoSliceMut<'_>; READV_SLICES] =
                    std::array::from_fn(|_| IoSliceMut::new(&mut []));
                let count = reservation.fill_io_slices(&mut slices);
                self.transport.try_read_vectored(&mut slices[..count])
            };
            match result {
                Ok(read) => {
                    reservation.commit(read);
                    return Ok(read);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn register_pending(
        &self,
        context: Resource<CallContext>,
    ) -> std::result::Result<(), RegisterPendingError> {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if let Some(status) = &lifecycle.failure {
            return Err(RegisterPendingError::Failed { context, status: status.clone() });
        }
        lifecycle.pending.add(context.id()).map_err(|_| RegisterPendingError::Capacity(context))
    }

    pub(super) fn fail_pending(detached: StaleIdList<PendingCallAdapter>, status: Status) {
        detached.for_each_live(|context| {
            context.complete(IoBuf::new(), IoBuf::new(), Some(status.clone()));
        });
    }
}

pub(super) struct PendingCallAdapter;

impl StaleIdListAdapter for PendingCallAdapter {
    type Value = CallContext;

    fn is_live(&self, value: &Self::Value) -> bool {
        value.is_active()
    }
}
