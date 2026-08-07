// SPDX-License-Identifier: Apache-2.0

//! Low-overhead sampled stage variables for perf experiments.

use std::array;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::runtime::variables::{self, Counter, Gauge, LatencyRecorder, MaxRecorder};

const STAGE_COUNT: usize = 22;
const EVENT_COUNT: usize = 36;

const STAGES: [ProfileStage; STAGE_COUNT] = [
    ProfileStage::ClientPayloadEncode,
    ProfileStage::ClientCallContextAlloc,
    ProfileStage::ClientFrameEncode,
    ProfileStage::ClientSubmitWrite,
    ProfileStage::ClientFrameDecode,
    ProfileStage::ClientCompleteContext,
    ProfileStage::ClientResponseDecode,
    ProfileStage::ClientRpcLatency,
    ProfileStage::ServerFrameDecode,
    ProfileStage::ServerServiceDispatch,
    ProfileStage::ServerServiceFinish,
    ProfileStage::ServerFrameEncode,
    ProfileStage::ServerSubmitWrite,
    ProfileStage::TransportWriteRequestAlloc,
    ProfileStage::TransportSubmitQueue,
    ProfileStage::TransportCurrentSocket,
    ProfileStage::TransportBuildBatch,
    ProfileStage::TransportBuildIoSlices,
    ProfileStage::TransportSyscallWrite,
    ProfileStage::TransportConsumeWritten,
    ProfileStage::TransportFinishRequest,
    ProfileStage::TransportPendingAdd,
];

const EVENTS: [ProfileEvent; EVENT_COUNT] = [
    ProfileEvent::TransportSubmitBecameWriter,
    ProfileEvent::TransportSubmitQueued,
    ProfileEvent::TransportWriteAttemptBytes,
    ProfileEvent::TransportWriteIovCount,
    ProfileEvent::TransportWriteOkBytes,
    ProfileEvent::TransportWriteOk,
    ProfileEvent::TransportWriteWouldBlock,
    ProfileEvent::TransportWriteZero,
    ProfileEvent::TransportWriteError,
    ProfileEvent::H2PublishInlineOwner,
    ProfileEvent::H2PublishOwnerBusy,
    ProfileEvent::H2DriveReadyStreams,
    ProfileEvent::H2WriteAttemptBytes,
    ProfileEvent::H2WriteIovCount,
    ProfileEvent::H2WriteOkBytes,
    ProfileEvent::H2WriteOk,
    ProfileEvent::H2WriteWouldBlock,
    ProfileEvent::H2ReadOkBytes,
    ProfileEvent::H2ReadFrames,
    ProfileEvent::H2ClientDriveReadyStreams,
    ProfileEvent::H2ServerDriveReadyStreams,
    ProfileEvent::H2ClientWriteOk,
    ProfileEvent::H2ServerWriteOk,
    ProfileEvent::H2ReadyDuplicateSuppressed,
    ProfileEvent::H2ReadyDrainNoOutput,
    ProfileEvent::H2DataEncodedBytes,
    ProfileEvent::H2HeadersEncodedBytes,
    ProfileEvent::H2ControlEncodedBytes,
    ProfileEvent::H2WindowUpdateEncodedBytes,
    ProfileEvent::H2AppWriteBytes,
    ProfileEvent::H2ControlWriteBytes,
    ProfileEvent::H2AppOnlyWrite,
    ProfileEvent::H2ControlOnlyWrite,
    ProfileEvent::H2MixedWrite,
    ProfileEvent::H2FlowBlocked,
    ProfileEvent::H2FlowResumed,
];

#[derive(Clone, Copy, Debug)]
pub enum ProfileStage {
    ClientPayloadEncode = 0,
    ClientCallContextAlloc = 1,
    ClientFrameEncode = 2,
    ClientSubmitWrite = 3,
    ClientFrameDecode = 4,
    ClientCompleteContext = 5,
    ClientResponseDecode = 6,
    ClientRpcLatency = 7,
    ServerFrameDecode = 8,
    ServerServiceDispatch = 9,
    ServerServiceFinish = 10,
    ServerFrameEncode = 11,
    ServerSubmitWrite = 12,
    TransportWriteRequestAlloc = 13,
    TransportSubmitQueue = 14,
    TransportCurrentSocket = 15,
    TransportBuildBatch = 16,
    TransportBuildIoSlices = 17,
    TransportSyscallWrite = 18,
    TransportConsumeWritten = 19,
    TransportFinishRequest = 20,
    TransportPendingAdd = 21,
}

impl ProfileStage {
    fn name(self) -> &'static str {
        match self {
            Self::ClientPayloadEncode => "client.payload_encode",
            Self::ClientCallContextAlloc => "client.call_context_alloc",
            Self::ClientFrameEncode => "client.frame_encode",
            Self::ClientSubmitWrite => "client.submit_write",
            Self::ClientFrameDecode => "client.frame_decode",
            Self::ClientCompleteContext => "client.complete_context",
            Self::ClientResponseDecode => "client.response_decode",
            Self::ClientRpcLatency => "client.rpc_latency",
            Self::ServerFrameDecode => "server.frame_decode",
            Self::ServerServiceDispatch => "server.service_dispatch",
            Self::ServerServiceFinish => "server.service_finish",
            Self::ServerFrameEncode => "server.frame_encode",
            Self::ServerSubmitWrite => "server.submit_write",
            Self::TransportWriteRequestAlloc => "transport.write_request_alloc",
            Self::TransportSubmitQueue => "transport.submit_queue",
            Self::TransportCurrentSocket => "transport.current_socket",
            Self::TransportBuildBatch => "transport.build_batch",
            Self::TransportBuildIoSlices => "transport.build_io_slices",
            Self::TransportSyscallWrite => "transport.syscall_write",
            Self::TransportConsumeWritten => "transport.consume_written",
            Self::TransportFinishRequest => "transport.finish_request",
            Self::TransportPendingAdd => "transport.pending_add",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProfileEvent {
    TransportSubmitBecameWriter = 0,
    TransportSubmitQueued = 1,
    TransportWriteAttemptBytes = 2,
    TransportWriteIovCount = 3,
    TransportWriteOkBytes = 4,
    TransportWriteOk = 5,
    TransportWriteWouldBlock = 6,
    TransportWriteZero = 7,
    TransportWriteError = 8,
    H2PublishInlineOwner = 9,
    H2PublishOwnerBusy = 10,
    H2DriveReadyStreams = 11,
    H2WriteAttemptBytes = 12,
    H2WriteIovCount = 13,
    H2WriteOkBytes = 14,
    H2WriteOk = 15,
    H2WriteWouldBlock = 16,
    H2ReadOkBytes = 17,
    H2ReadFrames = 18,
    H2ClientDriveReadyStreams = 19,
    H2ServerDriveReadyStreams = 20,
    H2ClientWriteOk = 21,
    H2ServerWriteOk = 22,
    H2ReadyDuplicateSuppressed = 23,
    H2ReadyDrainNoOutput = 24,
    H2DataEncodedBytes = 25,
    H2HeadersEncodedBytes = 26,
    H2ControlEncodedBytes = 27,
    H2WindowUpdateEncodedBytes = 28,
    H2AppWriteBytes = 29,
    H2ControlWriteBytes = 30,
    H2AppOnlyWrite = 31,
    H2ControlOnlyWrite = 32,
    H2MixedWrite = 33,
    H2FlowBlocked = 34,
    H2FlowResumed = 35,
}

impl ProfileEvent {
    fn name(self) -> &'static str {
        match self {
            Self::TransportSubmitBecameWriter => "transport.submit_became_writer",
            Self::TransportSubmitQueued => "transport.submit_queued",
            Self::TransportWriteAttemptBytes => "transport.write_attempt_bytes",
            Self::TransportWriteIovCount => "transport.write_iov_count",
            Self::TransportWriteOkBytes => "transport.write_ok_bytes",
            Self::TransportWriteOk => "transport.write_ok",
            Self::TransportWriteWouldBlock => "transport.write_would_block",
            Self::TransportWriteZero => "transport.write_zero",
            Self::TransportWriteError => "transport.write_error",
            Self::H2PublishInlineOwner => "h2.publish_inline_owner",
            Self::H2PublishOwnerBusy => "h2.publish_owner_busy",
            Self::H2DriveReadyStreams => "h2.drive_ready_streams",
            Self::H2WriteAttemptBytes => "h2.write_attempt_bytes",
            Self::H2WriteIovCount => "h2.write_iov_count",
            Self::H2WriteOkBytes => "h2.write_ok_bytes",
            Self::H2WriteOk => "h2.write_ok",
            Self::H2WriteWouldBlock => "h2.write_would_block",
            Self::H2ReadOkBytes => "h2.read_ok_bytes",
            Self::H2ReadFrames => "h2.read_frames",
            Self::H2ClientDriveReadyStreams => "h2.client_drive_ready_streams",
            Self::H2ServerDriveReadyStreams => "h2.server_drive_ready_streams",
            Self::H2ClientWriteOk => "h2.client_write_ok",
            Self::H2ServerWriteOk => "h2.server_write_ok",
            Self::H2ReadyDuplicateSuppressed => "h2.ready_duplicate_suppressed",
            Self::H2ReadyDrainNoOutput => "h2.ready_drain_no_output",
            Self::H2DataEncodedBytes => "h2.data_encoded_bytes",
            Self::H2HeadersEncodedBytes => "h2.headers_encoded_bytes",
            Self::H2ControlEncodedBytes => "h2.control_encoded_bytes",
            Self::H2WindowUpdateEncodedBytes => "h2.window_update_encoded_bytes",
            Self::H2AppWriteBytes => "h2.app_write_bytes",
            Self::H2ControlWriteBytes => "h2.control_write_bytes",
            Self::H2AppOnlyWrite => "h2.app_only_write",
            Self::H2ControlOnlyWrite => "h2.control_only_write",
            Self::H2MixedWrite => "h2.mixed_write",
            Self::H2FlowBlocked => "h2.flow_blocked",
            Self::H2FlowResumed => "h2.flow_resumed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct EventVariables {
    samples: Counter,
    total: Counter,
    max: MaxRecorder,
}

impl EventVariables {
    fn new() -> Self {
        Self { samples: Counter::new(), total: Counter::new(), max: MaxRecorder::new() }
    }

    fn record(&self, value: u64) {
        self.samples.inc();
        self.total.add(value);
        self.max.record(value);
    }
}

#[derive(Debug)]
struct ProfileVariables {
    enabled: Gauge,
    sample_every: Gauge,
    raw_writev: Gauge,
    coalesce_small_frames: Gauge,
    stages: [LatencyRecorder; STAGE_COUNT],
    events: [EventVariables; EVENT_COUNT],
}

impl ProfileVariables {
    fn new() -> Self {
        let variables = Self {
            enabled: Gauge::new(0),
            sample_every: Gauge::new(0),
            raw_writev: Gauge::new(0),
            coalesce_small_frames: Gauge::new(0),
            stages: array::from_fn(|_| LatencyRecorder::new()),
            events: array::from_fn(|_| EventVariables::new()),
        };
        variables.expose();
        variables
    }

    fn expose(&self) {
        expose_gauge("profile_enabled", self.enabled);
        expose_gauge("profile_sample_every", self.sample_every);
        expose_gauge("profile_raw_writev", self.raw_writev);
        expose_gauge("profile_coalesce_small_frames", self.coalesce_small_frames);

        for stage in STAGES {
            let recorder = self.stages[stage as usize];
            let name = variable_name("profile_stage", stage.name(), "latency");
            expose_latency(name, recorder);
            expose_latency_window(
                variable_name("profile_stage", stage.name(), "latency_window"),
                recorder,
            );
        }

        for event in EVENTS {
            let variables = self.events[event as usize];
            expose_counter(
                variable_name("profile_event", event.name(), "samples"),
                variables.samples,
            );
            expose_counter(variable_name("profile_event", event.name(), "total"), variables.total);
            expose_max(variable_name("profile_event", event.name(), "max"), variables.max);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProfileStageSnapshot {
    pub name: &'static str,
    pub samples: u64,
    pub total: Duration,
    pub average: Duration,
    pub max: Duration,
}

#[derive(Clone, Debug)]
pub struct ProfileEventSnapshot {
    pub name: &'static str,
    pub samples: u64,
    pub total: u64,
    pub average: u64,
    pub max: u64,
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static COALESCE_SMALL_FRAMES: AtomicBool = AtomicBool::new(false);
static SAMPLE_EVERY: AtomicU64 = AtomicU64::new(0);
static SAMPLE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static PROFILE_VARIABLES: LazyLock<ProfileVariables> = LazyLock::new(ProfileVariables::new);

pub fn configure(sample_every: u64) {
    let variables = LazyLock::force(&PROFILE_VARIABLES);
    variables.sample_every.set(sample_every.min(i64::MAX as u64) as i64);
    variables.enabled.set(i64::from(sample_every != 0));
    SAMPLE_EVERY.store(sample_every, Ordering::Relaxed);
    ENABLED.store(sample_every != 0, Ordering::Release);
}

pub fn configure_coalesce_small_frames(enabled: bool) {
    LazyLock::force(&PROFILE_VARIABLES).coalesce_small_frames.set(i64::from(enabled));
    COALESCE_SMALL_FRAMES.store(enabled, Ordering::Release);
}

#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

#[inline]
pub fn coalesce_small_frames_enabled() -> bool {
    COALESCE_SMALL_FRAMES.load(Ordering::Acquire)
}

#[inline]
pub fn sample_next() -> bool {
    let sample_every = SAMPLE_EVERY.load(Ordering::Relaxed);
    sample_every != 0
        && SAMPLE_SEQUENCE.fetch_add(1, Ordering::Relaxed).is_multiple_of(sample_every)
}

#[inline]
pub fn sample_key(key: u64) -> bool {
    let sample_every = SAMPLE_EVERY.load(Ordering::Relaxed);
    sample_every != 0 && key.is_multiple_of(sample_every)
}

#[inline]
pub fn start_if(sample: bool) -> Option<Instant> {
    sample.then(Instant::now)
}

#[inline]
pub fn record_elapsed(stage: ProfileStage, start: Instant) {
    record(stage, start.elapsed());
}

#[inline]
pub fn record(stage: ProfileStage, duration: Duration) {
    LazyLock::force(&PROFILE_VARIABLES).stages[stage as usize].record(duration);
}

#[inline]
pub fn record_event(event: ProfileEvent, value: u64) {
    LazyLock::force(&PROFILE_VARIABLES).events[event as usize].record(value);
}

pub fn snapshot() -> Vec<ProfileStageSnapshot> {
    let variables = LazyLock::force(&PROFILE_VARIABLES);
    STAGES
        .into_iter()
        .filter_map(|stage| {
            let snapshot = variables.stages[stage as usize].snapshot();
            if snapshot.count == 0 {
                return None;
            }
            Some(ProfileStageSnapshot {
                name: stage.name(),
                samples: snapshot.count,
                total: Duration::from_micros(snapshot.total_us),
                average: Duration::from_micros(snapshot.average_us),
                max: Duration::from_micros(snapshot.max_us),
            })
        })
        .collect()
}

pub fn event_snapshot() -> Vec<ProfileEventSnapshot> {
    let variables = LazyLock::force(&PROFILE_VARIABLES);
    EVENTS
        .into_iter()
        .filter_map(|event| {
            let variables = variables.events[event as usize];
            let samples = variables.samples.get();
            if samples == 0 {
                return None;
            }
            let total = variables.total.get();
            Some(ProfileEventSnapshot {
                name: event.name(),
                samples,
                total,
                average: total / samples,
                max: variables.max.get(),
            })
        })
        .collect()
}

fn expose_gauge(name: impl Into<String>, gauge: Gauge) {
    if let Err(error) = variables::expose_gauge(name, gauge) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_latency(name: impl Into<String>, latency: LatencyRecorder) {
    if let Err(error) = variables::expose_latency(name, latency) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_latency_window(name: impl Into<String>, latency: LatencyRecorder) {
    if let Err(error) = variables::expose_latency_window(name, latency, variables::DEFAULT_WINDOW) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_counter(name: impl Into<String>, counter: Counter) {
    if let Err(error) = variables::expose_counter(name, counter) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn expose_max(name: impl Into<String>, max: MaxRecorder) {
    if let Err(error) = variables::expose_max(name, max) {
        log::error!("failed to expose internal metric: {error}");
    }
}

fn variable_name(prefix: &str, name: &str, suffix: &str) -> String {
    let mut output = String::with_capacity(prefix.len() + name.len() + suffix.len() + 2);
    output.push_str(prefix);
    output.push('_');
    push_sanitized(&mut output, name);
    output.push('_');
    output.push_str(suffix);
    output
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
