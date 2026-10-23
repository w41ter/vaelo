// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::client::recycler::ClientActivityGuard;
use crate::foundation::intrusive_forward_list::IntrusiveForwardListLink;
use crate::runtime::rpc_variables::RpcCallGuard;
use crate::{IoBuf, MetadataMap, Resource, ResourceId, StaticResource, Status, StatusCode};

const ACTIVE: u8 = 1;
const COMPLETING: u8 = 2;
const COMPLETED: u8 = 3;

/// Protocol-neutral terminal data consumed by a typed call handle.
///
/// The protocol reader stores encoded payload bytes here. Decoding remains in
/// `CallHandle<T>`, so this pooled object never depends on an application type.
pub(crate) struct CallOutput {
    pub payload: IoBuf,
    pub attachment: IoBuf,
    pub metadata: MetadataMap,
    pub trailers: MetadataMap,
    pub status: Option<Status>,
    pub latency: Duration,
    pub profile_sampled: bool,
}

/// One pooled RPC lifecycle and completion target.
///
/// `CallContext` deliberately does not own request bytes. A serialized request
/// frame belongs to the transport write request and can be released once the
/// transport no longer needs it. During the in-flight period this object keeps
/// only call options, timing, completion data, and the waiter waker.
#[derive(StaticResource)]
pub(crate) struct CallContext {
    state: AtomicU8,
    deadline: Option<Instant>,
    profile_sampled: bool,
    started: Instant,
    timer_link: IntrusiveForwardListLink<CallContext>,
    inner: Mutex<CallContextInner>,
}

struct CallContextInner {
    completion: Option<CallOutput>,
    waiter: Option<Waker>,
    completion_notifier: Option<CompletionNotifier>,
    connection_activity: Option<ClientActivityGuard>,
    detached_variables: Option<RpcCallGuard>,
}

#[derive(Clone, Copy)]
struct CompletionNotifier {
    token: u64,
    notify: fn(u64),
}

impl CallContext {
    pub(crate) fn new(deadline: Option<Instant>, profile_sampled: bool) -> Self {
        Self::new_with_activity(deadline, profile_sampled, None)
    }

    pub(crate) fn new_with_activity(
        deadline: Option<Instant>,
        profile_sampled: bool,
        connection_activity: Option<ClientActivityGuard>,
    ) -> Self {
        Self {
            state: AtomicU8::new(ACTIVE),
            deadline,
            profile_sampled,
            started: Instant::now(),
            timer_link: IntrusiveForwardListLink::new(),
            inner: Mutex::new(CallContextInner {
                completion: None,
                waiter: None,
                completion_notifier: None,
                connection_activity,
                detached_variables: None,
            }),
        }
    }

    /// Attaches activity acquired after this context was allocated but before
    /// the call is published to a transport.
    pub(crate) fn set_connection_activity(&self, activity: ClientActivityGuard) {
        let mut inner = self.inner.lock().expect("call context mutex poisoned");
        debug_assert_eq!(self.state.load(Ordering::Acquire), ACTIVE);
        debug_assert!(inner.connection_activity.is_none());
        inner.connection_activity = Some(activity);
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn profile_sampled(&self) -> bool {
        self.profile_sampled
    }

    pub(crate) fn timer_link(&self) -> &IntrusiveForwardListLink<Self> {
        &self.timer_link
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == ACTIVE
    }

    pub(crate) fn completed_status(&self) -> Option<Status> {
        if self.state.load(Ordering::Acquire) != COMPLETED {
            return None;
        }
        self.inner
            .lock()
            .expect("call context mutex poisoned")
            .completion
            .as_ref()
            .and_then(|completion| completion.status.clone())
    }

    /// Installs one protocol cancellation hook. A false return means the call
    /// already completed and the protocol object must cancel itself instead.
    pub(crate) fn set_completion_notifier(&self, token: u64, notify: fn(u64)) -> bool {
        let mut inner = self.inner.lock().expect("call context mutex poisoned");
        if self.state.load(Ordering::Acquire) != ACTIVE {
            return false;
        }
        debug_assert!(inner.completion_notifier.is_none());
        inner.completion_notifier = Some(CompletionNotifier { token, notify });
        true
    }

    /// Removes the protocol hook before protocol-originated completion, so a
    /// normal response does not feed back into a spurious stream reset.
    pub(crate) fn clear_completion_notifier(&self) {
        self.inner.lock().expect("call context mutex poisoned").completion_notifier.take();
    }

    pub(crate) fn complete(
        &self,
        payload: IoBuf,
        attachment: IoBuf,
        status: Option<Status>,
    ) -> bool {
        self.complete_with_metadata(
            payload,
            attachment,
            status,
            MetadataMap::new(),
            MetadataMap::new(),
        )
    }

    pub(crate) fn complete_with_metadata(
        &self,
        payload: IoBuf,
        attachment: IoBuf,
        status: Option<Status>,
        metadata: MetadataMap,
        trailers: MetadataMap,
    ) -> bool {
        if self
            .state
            .compare_exchange(ACTIVE, COMPLETING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            log::debug!("call completion ignored: call already completed");
            return false;
        }
        let status_code = status.as_ref().map(Status::code);
        let (waker, notifier, connection_activity, detached_variables) = {
            let mut inner = self.inner.lock().expect("call context mutex poisoned");
            inner.completion = Some(CallOutput {
                payload,
                attachment,
                metadata,
                trailers,
                status,
                latency: self.started.elapsed(),
                profile_sampled: self.profile_sampled,
            });
            self.state.store(COMPLETED, Ordering::Release);
            (
                inner.waiter.take(),
                inner.completion_notifier.take(),
                inner.connection_activity.take(),
                inner.detached_variables.take(),
            )
        };
        drop(connection_activity);
        if let Some(variables) = detached_variables {
            variables.finish(status_code);
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        if let Some(notifier) = notifier {
            (notifier.notify)(notifier.token);
        }
        true
    }

    fn detach_variables(&self, variables: RpcCallGuard) {
        let status_code = {
            let mut inner = self.inner.lock().expect("call context mutex poisoned");
            if self.state.load(Ordering::Acquire) != COMPLETED {
                debug_assert!(inner.detached_variables.is_none());
                inner.detached_variables = Some(variables);
                return;
            }
            inner
                .completion
                .as_ref()
                .and_then(|completion| completion.status.as_ref().map(Status::code))
        };
        variables.finish(status_code);
    }

    pub(crate) fn complete_deadline_exceeded(&self) -> bool {
        self.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::DeadlineExceeded, "deadline exceeded")),
        )
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<CallOutput> {
        if self.state.load(Ordering::Acquire) == COMPLETED {
            let completion = self
                .inner
                .lock()
                .expect("call context mutex poisoned")
                .completion
                .take()
                .expect("completed call context must contain completion");
            return Poll::Ready(completion);
        }
        let mut inner = self.inner.lock().expect("call context mutex poisoned");
        inner.waiter = Some(cx.waker().clone());
        if self.state.load(Ordering::Acquire) == COMPLETED {
            let completion =
                inner.completion.take().expect("completed call context must contain completion");
            Poll::Ready(completion)
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct CallToken {
    context: Resource<CallContext>,
}

impl CallToken {
    pub(crate) fn new(context: Resource<CallContext>) -> Self {
        Self { context }
    }

    #[cfg(test)]
    pub(crate) fn correlation_id(&self) -> u64 {
        self.context.id().as_u64()
    }

    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<CallOutput> {
        self.context.poll(cx)
    }

    pub(crate) fn cancel(&self) -> bool {
        self.context.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::Canceled, "call canceled")),
        )
    }

    pub(crate) fn detach(self, variables: RpcCallGuard) {
        self.context.detach_variables(variables);
    }
}

pub(crate) fn call_id(raw: u64) -> ResourceId<CallContext> {
    ResourceId::from_parts(raw as u32, (raw >> 32) as u32)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use super::*;

    static EXTERNAL_NOTIFIED: AtomicUsize = AtomicUsize::new(0);
    static DISARMED_NOTIFIED: AtomicUsize = AtomicUsize::new(0);

    fn record_external_notification(token: u64) {
        EXTERNAL_NOTIFIED.fetch_add(token as usize, Ordering::Relaxed);
    }

    fn record_disarmed_notification(token: u64) {
        DISARMED_NOTIFIED.fetch_add(token as usize, Ordering::Relaxed);
    }

    #[test]
    fn cancellation_wins_completion_exactly_once() {
        let context = CallContext::new(None, false);

        assert!(context.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::Canceled, "call canceled")),
        ));
        assert!(!context.complete(IoBuf::new(), IoBuf::new(), None));

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(output) = context.poll(&mut cx) else {
            panic!("canceled call must be ready");
        };
        assert_eq!(output.status.as_ref().map(Status::code), Some(StatusCode::Canceled));
    }

    #[test]
    fn successful_completion_wins_late_cancellation() {
        let context = CallContext::new(None, false);

        assert!(context.complete(IoBuf::new(), IoBuf::new(), None));
        assert!(!context.complete(
            IoBuf::new(),
            IoBuf::new(),
            Some(Status::new(StatusCode::Canceled, "call canceled")),
        ));

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let Poll::Ready(output) = context.poll(&mut cx) else {
            panic!("completed call must be ready");
        };
        assert!(output.status.is_none());
    }

    #[test]
    fn external_completion_notifies_the_protocol_once() {
        EXTERNAL_NOTIFIED.store(0, Ordering::Relaxed);
        let context = CallContext::new(None, false);
        assert!(context.set_completion_notifier(7, record_external_notification));

        assert!(context.complete_deadline_exceeded());
        assert!(!context.complete_deadline_exceeded());
        assert_eq!(EXTERNAL_NOTIFIED.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn protocol_completion_can_disarm_cancellation_notification() {
        DISARMED_NOTIFIED.store(0, Ordering::Relaxed);
        let context = CallContext::new(None, false);
        assert!(context.set_completion_notifier(7, record_disarmed_notification));

        context.clear_completion_notifier();
        assert!(context.complete(IoBuf::new(), IoBuf::new(), None));
        assert_eq!(DISARMED_NOTIFIED.load(Ordering::Relaxed), 0);
    }
}
