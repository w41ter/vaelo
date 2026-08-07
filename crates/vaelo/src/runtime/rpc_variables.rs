// SPDX-License-Identifier: Apache-2.0

//! RPC and transport variables built from runtime variable primitives.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crate::runtime::variables::{self, Balance, Counter, LatencyRecorder, VariableSnapshot};
use crate::{MethodDescriptor, StatusCode};

static METHOD_VARIABLES: LazyLock<Mutex<HashMap<MethodVariableKey, Arc<RpcMethodVariables>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static TRANSPORT_VARIABLES: LazyLock<TransportVariables> = LazyLock::new(TransportVariables::new);

#[derive(Debug)]
pub(crate) struct RpcMethodVariables {
    requests: Counter,
    errors: Counter,
    concurrency: Balance,
    latency: LatencyRecorder,
}

impl RpcMethodVariables {
    fn new(side: RpcSide, descriptor: &'static MethodDescriptor) -> Self {
        let variables = Self {
            requests: Counter::new(),
            errors: Counter::new(),
            concurrency: Balance::new(),
            latency: LatencyRecorder::new(),
        };
        variables.expose(side, descriptor);
        variables
    }

    fn expose(&self, side: RpcSide, descriptor: &'static MethodDescriptor) {
        let prefix = variable_name(&["rpc", side.as_str(), descriptor.service, descriptor.method]);
        expose_counter(format!("{prefix}_requests"), self.requests);
        expose_counter(format!("{prefix}_errors"), self.errors);
        expose_balance(format!("{prefix}_concurrency"), self.concurrency);
        expose_latency(format!("{prefix}_latency"), self.latency);
        expose_counter_window(
            format!("{prefix}_requests_window"),
            self.requests,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_window(
            format!("{prefix}_errors_window"),
            self.errors,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(
            format!("{prefix}_qps"),
            self.requests,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(format!("{prefix}_eps"), self.errors, variables::DEFAULT_WINDOW);
        expose_latency_window(
            format!("{prefix}_latency_window"),
            self.latency,
            variables::DEFAULT_WINDOW,
        );
    }

    #[inline]
    pub(crate) fn start_call(self: &Arc<Self>) -> RpcCallGuard {
        self.requests.inc();
        self.concurrency.inc();
        RpcCallGuard { variables: self.clone(), start: Instant::now(), finished: false }
    }

    fn finish_call(&self, latency: Duration, status: Option<StatusCode>) {
        self.concurrency.dec();
        if !matches!(status, None | Some(StatusCode::Ok)) {
            self.errors.inc();
        }
        self.latency.record(latency);
    }

    #[cfg(test)]
    fn snapshot(&self) -> RpcMethodVariablesSnapshot {
        RpcMethodVariablesSnapshot {
            requests: self.requests.get(),
            errors: self.errors.get(),
            concurrency: self.concurrency.get(),
            latency_count: self.latency.snapshot().count,
        }
    }
}

pub(crate) struct RpcCallGuard {
    variables: Arc<RpcMethodVariables>,
    start: Instant,
    finished: bool,
}

impl RpcCallGuard {
    #[inline]
    pub(crate) fn finish(mut self, status: Option<StatusCode>) {
        self.variables.finish_call(self.start.elapsed(), status);
        self.finished = true;
    }
}

impl Drop for RpcCallGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.variables.finish_call(self.start.elapsed(), Some(StatusCode::Canceled));
        }
    }
}

#[derive(Debug)]
pub(crate) struct TransportVariables {
    write_submitted: Counter,
    write_queued: Counter,
    write_became_writer: Counter,
    write_calls: Counter,
    write_bytes: Counter,
    write_attempt_bytes: Counter,
    write_iov_total: Counter,
    write_would_block: Counter,
    write_zero: Counter,
    write_errors: Counter,
}

impl TransportVariables {
    fn new() -> Self {
        let variables = Self {
            write_submitted: Counter::new(),
            write_queued: Counter::new(),
            write_became_writer: Counter::new(),
            write_calls: Counter::new(),
            write_bytes: Counter::new(),
            write_attempt_bytes: Counter::new(),
            write_iov_total: Counter::new(),
            write_would_block: Counter::new(),
            write_zero: Counter::new(),
            write_errors: Counter::new(),
        };
        variables.expose();
        variables
    }

    fn expose(&self) {
        expose_counter("transport_write_submitted", self.write_submitted);
        expose_counter("transport_write_queued", self.write_queued);
        expose_counter("transport_write_became_writer", self.write_became_writer);
        expose_counter("transport_write_calls", self.write_calls);
        expose_counter("transport_write_bytes", self.write_bytes);
        expose_counter("transport_write_attempt_bytes", self.write_attempt_bytes);
        expose_counter("transport_write_iov_total", self.write_iov_total);
        expose_counter("transport_write_would_block", self.write_would_block);
        expose_counter("transport_write_zero", self.write_zero);
        expose_counter("transport_write_errors", self.write_errors);
        expose_counter_per_second(
            "transport_write_submitted_per_second",
            self.write_submitted,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_window(
            "transport_write_submitted_window",
            self.write_submitted,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(
            "transport_write_calls_per_second",
            self.write_calls,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_window(
            "transport_write_calls_window",
            self.write_calls,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(
            "transport_write_bytes_per_second",
            self.write_bytes,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_window(
            "transport_write_bytes_window",
            self.write_bytes,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(
            "transport_write_would_block_per_second",
            self.write_would_block,
            variables::DEFAULT_WINDOW,
        );
        expose_counter_per_second(
            "transport_write_errors_per_second",
            self.write_errors,
            variables::DEFAULT_WINDOW,
        );
    }

    #[inline]
    pub(crate) fn write_submitted(&self) {
        self.write_submitted.inc();
    }

    #[inline]
    pub(crate) fn write_queued(&self) {
        self.write_queued.inc();
    }

    #[inline]
    pub(crate) fn write_became_writer(&self) {
        self.write_became_writer.inc();
    }

    #[inline]
    pub(crate) fn write_attempt(&self, bytes: u64, iov_count: u64) {
        self.write_calls.inc();
        self.write_attempt_bytes.add(bytes);
        self.write_iov_total.add(iov_count);
    }

    #[inline]
    pub(crate) fn write_ok(&self, bytes: u64) {
        self.write_bytes.add(bytes);
    }

    #[inline]
    pub(crate) fn write_would_block(&self) {
        self.write_would_block.inc();
    }

    #[inline]
    pub(crate) fn write_zero(&self) {
        self.write_zero.inc();
    }

    #[inline]
    pub(crate) fn write_error(&self) {
        self.write_errors.inc();
    }
}

pub(crate) fn server_method(descriptor: &'static MethodDescriptor) -> Arc<RpcMethodVariables> {
    method_variables(RpcSide::Server, descriptor)
}

pub(crate) fn client_method(descriptor: &'static MethodDescriptor) -> Arc<RpcMethodVariables> {
    method_variables(RpcSide::Client, descriptor)
}

pub(crate) fn transport() -> &'static TransportVariables {
    &TRANSPORT_VARIABLES
}

pub fn snapshot() -> Vec<VariableSnapshot> {
    variables::dump()
}

fn method_variables(
    side: RpcSide,
    descriptor: &'static MethodDescriptor,
) -> Arc<RpcMethodVariables> {
    let key = MethodVariableKey { side, service: descriptor.service, method: descriptor.method };
    let mut variables = METHOD_VARIABLES.lock().expect("rpc variables registry mutex poisoned");
    variables
        .entry(key)
        .or_insert_with(|| Arc::new(RpcMethodVariables::new(side, descriptor)))
        .clone()
}

fn expose_counter(name: impl Into<String>, counter: Counter) {
    if let Err(error) = variables::expose_counter(name, counter) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_balance(name: impl Into<String>, balance: Balance) {
    if let Err(error) = variables::expose_balance(name, balance) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_latency(name: impl Into<String>, latency: LatencyRecorder) {
    if let Err(error) = variables::expose_latency(name, latency) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_counter_per_second(name: impl Into<String>, counter: Counter, window: Duration) {
    if let Err(error) = variables::expose_counter_per_second(name, counter, window) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_counter_window(name: impl Into<String>, counter: Counter, window: Duration) {
    if let Err(error) = variables::expose_counter_window(name, counter, window) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_latency_window(name: impl Into<String>, latency: LatencyRecorder, window: Duration) {
    if let Err(error) = variables::expose_latency_window(name, latency, window) {
        log::error!("failed to expose internal metric: {error}");
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RpcSide {
    Client,
    Server,
}

impl RpcSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct MethodVariableKey {
    side: RpcSide,
    service: &'static str,
    method: &'static str,
}

fn variable_name(parts: &[&str]) -> String {
    let mut name = String::new();
    for part in parts {
        if !name.is_empty() {
            name.push('_');
        }
        push_sanitized(&mut name, part);
    }
    name
}

fn push_sanitized(output: &mut String, input: &str) {
    let mut previous_underscore = output.ends_with('_');
    for byte in input.bytes() {
        let normalized = match byte {
            b'a'..=b'z' | b'0'..=b'9' => byte as char,
            b'A'..=b'Z' => byte.to_ascii_lowercase() as char,
            _ => '_',
        };
        if normalized == '_' {
            if !previous_underscore {
                output.push('_');
                previous_underscore = true;
            }
        } else {
            output.push(normalized);
            previous_underscore = false;
        }
    }
    if output.ends_with('_') {
        output.pop();
    }
}

#[cfg(test)]
#[derive(Debug)]
struct RpcMethodVariablesSnapshot {
    requests: u64,
    errors: u64,
    concurrency: i64,
    latency_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::{Idempotency, MethodKind};

    static TEST_METHOD: MethodDescriptor = MethodDescriptor::new(
        "variables.TestService",
        "Echo",
        MethodKind::Unary,
        Idempotency::Idempotent,
    );

    #[test]
    fn rpc_call_guard_records_success_and_drop_as_cancel() {
        let variables = Arc::new(RpcMethodVariables::new(RpcSide::Server, &TEST_METHOD));
        variables.start_call().finish(None);
        drop(variables.start_call());

        let snapshot = variables.snapshot();
        assert_eq!(snapshot.requests, 2);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.concurrency, 0);
        assert_eq!(snapshot.latency_count, 2);
    }

    #[test]
    fn variable_names_are_stable_and_sanitized() {
        assert_eq!(
            variable_name(&["rpc", "server", "foo.BarService", "Call/One"]),
            "rpc_server_foo_barservice_call_one"
        );
    }
}
