// SPDX-License-Identifier: Apache-2.0

use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, Instant};
use std::{fmt, thread};

use bytes::{BufMut, Bytes};
use prost::Message;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};
use vaelo::__private::{
    Idempotency, MethodDescriptor, MethodKind, Protocol, ProtocolSet, RegisterService, RpcAcceptor,
    Server, ServiceRegistryBuilder, UnaryProstMethod,
};
use vaelo::{
    ConnectionMode, IoBufBuilder, Request, Response, RpcChannel as VaeloChannel, ServerBuilder,
    ShutdownPolicy, Status, http,
};

static ECHO_METHOD: MethodDescriptor =
    MethodDescriptor::new("perf.EchoService", "Echo", MethodKind::Unary, Idempotency::Idempotent);

struct PerfLogger;

impl log::Log for PerfLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target().starts_with("vaelo::") && metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            // Ignore output errors: diagnostics must not panic in an I/O continuation.
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr().lock(),
                "{} {}: {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

static PERF_LOGGER: PerfLogger = PerfLogger;

struct CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every operation is forwarded unchanged to the system allocator; the
// relaxed counters are observational and never participate in allocation.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() && COUNT_ALLOCATIONS.load(AtomicOrdering::Relaxed) {
            ALLOCATIONS.fetch_add(1, AtomicOrdering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, AtomicOrdering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() && COUNT_ALLOCATIONS.load(AtomicOrdering::Relaxed) {
            ALLOCATIONS.fetch_add(1, AtomicOrdering::Relaxed);
            ALLOCATED_BYTES.fetch_add(size as u64, AtomicOrdering::Relaxed);
        }
        pointer
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProtocolChoice {
    Brpc,
    Grpc,
    Http2,
    Tonic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    PingPong,
    Window,
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionPlan {
    Single,
    Tagged,
    Short,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunMode {
    Local,
    Server,
    Client,
}

#[derive(Clone, Debug)]
struct Config {
    mode: RunMode,
    protocol: ProtocolChoice,
    scenario: Scenario,
    connections: ConnectionPlan,
    server: ServerTarget,
    listen: SocketAddr,
    threads: usize,
    iters_per_thread: usize,
    window: usize,
    payload_size: usize,
    response_size: Option<usize>,
    warmup: usize,
    timeout: Option<Duration>,
    server_delay: Duration,
    server_threads: usize,
    profile_sample: u64,
    coalesce_small_frames: bool,
    count_allocations: bool,
    log_level: log::LevelFilter,
}

#[derive(Clone, Debug)]
enum ServerTarget {
    Local,
    Remote(SocketAddr),
}

#[derive(Clone, PartialEq, Message)]
struct EchoRequest {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
    #[prost(uint64, tag = "2")]
    response_size: u64,
}

#[derive(Clone, PartialEq, Message)]
struct EchoResponse {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
}

mod tonic_perf {
    tonic::include_proto!("perf");
}

struct TonicEchoService {
    delay: Duration,
}

#[tonic::async_trait]
impl tonic_perf::echo_service_server::EchoService for TonicEchoService {
    async fn echo(
        &self,
        request: TonicRequest<tonic_perf::EchoRequest>,
    ) -> Result<TonicResponse<tonic_perf::EchoResponse>, TonicStatus> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let request = request.into_inner();
        Ok(TonicResponse::new(tonic_perf::EchoResponse {
            payload: make_payload(request.response_size as usize),
        }))
    }
}

#[derive(Clone, Copy)]
struct EchoService {
    delay: Duration,
}

impl EchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(Response::new(EchoResponse {
            payload: make_payload(request.into_inner().response_size as usize),
        }))
    }
}

impl RegisterService for EchoService {
    fn register(self, registry: &mut ServiceRegistryBuilder) {
        registry.register(
            &ECHO_METHOD,
            UnaryProstMethod::new(
                std::sync::Arc::new(self),
                |service: std::sync::Arc<EchoService>, request| async move {
                    service.echo(request).await
                },
            ),
        );
    }
}

#[derive(Debug)]
struct ThreadReport {
    operations: usize,
    errors: usize,
    warmup_errors: usize,
    elapsed: Duration,
    success_latencies: Vec<u64>,
    failure_latencies: Vec<u64>,
    error_kinds: HashMap<String, usize>,
    allocations: u64,
    allocated_bytes: u64,
}

#[derive(Debug)]
struct LocalServer {
    address: SocketAddr,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl LocalServer {
    fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(std::env::args().skip(1))?;
    if config.help_requested() {
        println!("{}", usage());
        return Ok(());
    }
    config.validate()?;
    if config.log_level != log::LevelFilter::Off {
        log::set_logger(&PERF_LOGGER)
            .map_err(|error| format!("cannot install perf logger: {error}"))?;
        log::set_max_level(config.log_level);
    }

    match config.mode {
        RunMode::Local => run_local_mode(config)?,
        RunMode::Server => run_server_mode(config)?,
        RunMode::Client => run_client_mode(config)?,
    }
    Ok(())
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    config.threads = 0;
                    return Ok(config);
                }
                "--mode" => config.mode = parse_mode(&next_value(&arg, &mut args)?)?,
                "--protocol" => config.protocol = parse_protocol(&next_value(&arg, &mut args)?)?,
                "--scenario" => config.scenario = parse_scenario(&next_value(&arg, &mut args)?)?,
                "--connections" => {
                    config.connections = parse_connections(&next_value(&arg, &mut args)?)?
                }
                "--server" => config.server = parse_server(&next_value(&arg, &mut args)?)?,
                "--listen" => config.listen = next_value(&arg, &mut args)?.parse()?,
                "--threads" => config.threads = parse_usize(&arg, &mut args)?,
                "--iters" => config.iters_per_thread = parse_usize(&arg, &mut args)?,
                "--window" => config.window = parse_usize(&arg, &mut args)?,
                "--payload-size" => config.payload_size = parse_usize(&arg, &mut args)?,
                "--response-size" => config.response_size = Some(parse_usize(&arg, &mut args)?),
                "--warmup" => config.warmup = parse_usize(&arg, &mut args)?,
                "--timeout-ms" => {
                    config.timeout = Some(Duration::from_millis(parse_u64(&arg, &mut args)?))
                }
                "--server-delay-us" => {
                    config.server_delay = Duration::from_micros(parse_u64(&arg, &mut args)?)
                }
                "--server-threads" => config.server_threads = parse_usize(&arg, &mut args)?,
                "--profile-sample" => config.profile_sample = parse_u64(&arg, &mut args)?,
                "--coalesce-small-frames" => config.coalesce_small_frames = true,
                "--count-allocations" => config.count_allocations = true,
                "--log-level" => {
                    config.log_level = next_value(&arg, &mut args)?
                        .parse()
                        .map_err(|error| format!("invalid --log-level: {error}"))?;
                }
                unknown => return Err(format!("unknown argument `{unknown}`").into()),
            }
        }
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.server_threads == 0 {
            return Err("--server-threads must be greater than zero".into());
        }
        if self.mode == RunMode::Client && matches!(self.server, ServerTarget::Local) {
            return Err("--mode client requires --server HOST:PORT".into());
        }
        if self.mode == RunMode::Server {
            return Ok(());
        }
        if self.threads == 0 {
            return Err("--threads must be greater than zero".into());
        }
        if self.iters_per_thread == 0 {
            return Err("--iters must be greater than zero".into());
        }
        if self.count_allocations && self.threads != 1 {
            return Err("--count-allocations requires --threads 1 for an unambiguous delta".into());
        }
        if self.window == 0 {
            return Err("--window must be greater than zero".into());
        }
        if self.scenario == Scenario::Deadline && self.timeout.is_none() {
            return Err("--scenario deadline requires --timeout-ms".into());
        }
        if matches!(self.protocol, ProtocolChoice::Grpc | ProtocolChoice::Tonic)
            && self.connections == ConnectionPlan::Short
        {
            return Err("gRPC and tonic do not support short connections".into());
        }
        if self.mode == RunMode::Client && !self.server_delay.is_zero() {
            return Err("--server-delay-us does not apply to --mode client".into());
        }
        if self.mode == RunMode::Local
            && matches!(self.server, ServerTarget::Remote(_))
            && !self.server_delay.is_zero()
        {
            return Err("--server-delay-us only applies to --server local".into());
        }
        Ok(())
    }

    fn help_requested(&self) -> bool {
        self.threads == 0
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: RunMode::Local,
            protocol: ProtocolChoice::Brpc,
            scenario: Scenario::PingPong,
            connections: ConnectionPlan::Single,
            server: ServerTarget::Local,
            listen: "127.0.0.1:8002".parse().expect("default listen address is valid"),
            threads: std::thread::available_parallelism().map_or(1, usize::from),
            iters_per_thread: 1_000_000,
            window: 8,
            payload_size: 16,
            response_size: None,
            warmup: 1_000,
            timeout: None,
            server_delay: Duration::ZERO,
            server_threads: std::thread::available_parallelism().map_or(1, usize::from),
            profile_sample: 0,
            coalesce_small_frames: false,
            count_allocations: false,
            log_level: log::LevelFilter::Off,
        }
    }
}

fn run_local_mode(config: Config) -> Result<(), Box<dyn Error>> {
    configure_profile(&config);
    let local_server = match config.server {
        ServerTarget::Local => Some(start_local_server(&config)?),
        ServerTarget::Remote(_) => None,
    };
    let address = match (&config.server, &local_server) {
        (ServerTarget::Local, Some(server)) => server.address(),
        (ServerTarget::Remote(address), _) => *address,
        _ => unreachable!(),
    };

    print_run_header(&config, address);

    let reports = run_client_threads(config.clone(), address)?;
    if print_summary(&config, reports) != 0 {
        return Err("one or more measured RPCs failed".into());
    }
    print_profile_summary();

    drop(local_server);
    Ok(())
}

fn run_client_mode(config: Config) -> Result<(), Box<dyn Error>> {
    configure_profile(&config);
    let ServerTarget::Remote(address) = config.server else {
        unreachable!("validated by Config::validate")
    };
    print_run_header(&config, address);
    let reports = run_client_threads(config.clone(), address)?;
    if print_summary(&config, reports) != 0 {
        return Err("one or more measured RPCs failed".into());
    }
    print_profile_summary();
    Ok(())
}

fn run_server_mode(config: Config) -> Result<(), Box<dyn Error>> {
    configure_profile(&config);
    println!("protocol: {:?}", config.protocol);
    println!("listen: {}", config.listen);
    println!("server delay: {}", format_duration(config.server_delay));
    println!("server threads: {}", config.server_threads);
    println!("profile sample: {}", config.profile_sample);
    println!("coalesce small frames: {}", config.coalesce_small_frames);
    run_foreground_server(&config)
}

fn configure_profile(config: &Config) {
    vaelo::__private::profile::configure(config.profile_sample);
    vaelo::__private::profile::configure_coalesce_small_frames(config.coalesce_small_frames);
}

fn run_client_threads(
    config: Config,
    address: SocketAddr,
) -> Result<Vec<ThreadReport>, Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads)
        .enable_all()
        .build()?;
    let started = Instant::now();
    let reports = runtime
        .block_on(async {
            let shared = (config.connections == ConnectionPlan::Single)
                .then(|| rpc_channel(config.protocol, address, config.connections, 0));
            let mut joins = JoinSet::new();
            for thread_index in 0..config.threads {
                let thread_config = config.clone();
                let channel = shared.clone().unwrap_or_else(|| {
                    rpc_channel(
                        thread_config.protocol,
                        address,
                        thread_config.connections,
                        thread_index,
                    )
                });
                joins.spawn(run_client_worker(thread_config, channel));
            }

            let mut reports = Vec::with_capacity(config.threads);
            while let Some(result) = joins.join_next().await {
                reports
                    .push(result.map_err(|error| format!("client workload panicked: {error}"))??);
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(reports)
        })
        .map_err(|error| std::io::Error::other(error.to_string()))?;

    let wall = started.elapsed();
    let mut reports = reports;
    reports.sort_by_key(|report| report.elapsed);
    println!("client wall elapsed: {}", format_duration(wall));
    Ok(reports)
}

async fn run_client_worker(
    config: Config,
    channel: RpcChannel,
) -> Result<ThreadReport, Box<dyn Error + Send + Sync>> {
    let payload = make_payload(config.payload_size);
    let response_size = config.response_size.unwrap_or(config.payload_size);
    let expected = make_payload(response_size);

    let mut warmup_errors = 0;
    for _ in 0..config.warmup {
        if echo_once(
            channel.clone(),
            payload.clone(),
            expected.clone(),
            response_size,
            config.timeout,
        )
        .await
        .is_err()
        {
            warmup_errors += 1;
        }
    }

    let allocation_start = if config.count_allocations {
        let snapshot = allocation_snapshot();
        COUNT_ALLOCATIONS.store(true, AtomicOrdering::Release);
        Some(snapshot)
    } else {
        None
    };

    let mut report = match config.scenario {
        Scenario::PingPong => {
            run_ping_pong(
                channel,
                payload,
                expected,
                response_size,
                config.iters_per_thread,
                config.timeout,
            )
            .await?
        }
        Scenario::Window | Scenario::Deadline => {
            run_windowed(
                channel,
                payload,
                expected,
                response_size,
                config.iters_per_thread,
                config.window,
                config.timeout,
            )
            .await?
        }
    };
    if let Some((allocations, bytes)) = allocation_start {
        COUNT_ALLOCATIONS.store(false, AtomicOrdering::Release);
        let (end_allocations, end_bytes) = allocation_snapshot();
        report.allocations = end_allocations.saturating_sub(allocations);
        report.allocated_bytes = end_bytes.saturating_sub(bytes);
    }
    report.warmup_errors = warmup_errors;
    Ok(report)
}

async fn run_ping_pong(
    channel: RpcChannel,
    payload: Bytes,
    expected: Bytes,
    response_size: usize,
    iterations: usize,
    timeout: Option<Duration>,
) -> Result<ThreadReport, Box<dyn Error + Send + Sync>> {
    let started = Instant::now();
    let mut errors = 0;
    let mut success_latencies = Vec::with_capacity(iterations);
    let mut failure_latencies = Vec::new();
    let mut error_kinds = HashMap::new();

    for _ in 0..iterations {
        let call_started = Instant::now();
        match echo_once(channel.clone(), payload.clone(), expected.clone(), response_size, timeout)
            .await
        {
            Ok(()) => success_latencies.push(elapsed_nanos(call_started)),
            Err(status) => {
                errors += 1;
                *error_kinds.entry(status.to_string()).or_insert(0) += 1;
                failure_latencies.push(elapsed_nanos(call_started));
            }
        }
    }

    Ok(ThreadReport {
        operations: iterations,
        errors,
        warmup_errors: 0,
        elapsed: started.elapsed(),
        success_latencies,
        failure_latencies,
        error_kinds,
        allocations: 0,
        allocated_bytes: 0,
    })
}

async fn run_windowed(
    channel: RpcChannel,
    payload: Bytes,
    expected: Bytes,
    response_size: usize,
    iterations: usize,
    window: usize,
    timeout: Option<Duration>,
) -> Result<ThreadReport, Box<dyn Error + Send + Sync>> {
    let started = Instant::now();
    let mut errors = 0;
    let mut launched = 0;
    let mut completed = 0;
    let mut success_latencies = Vec::with_capacity(iterations);
    let mut failure_latencies = Vec::new();
    let mut error_kinds = HashMap::new();
    let mut joins = JoinSet::new();

    while launched < iterations.min(window) {
        let channel = channel.clone();
        let payload = payload.clone();
        let expected = expected.clone();
        joins.spawn(async move {
            let call_started = Instant::now();
            let result = echo_once(channel, payload, expected, response_size, timeout).await;
            (result, elapsed_nanos(call_started))
        });
        launched += 1;
    }

    while let Some(result) = joins.join_next().await {
        let (call, latency) = result?;
        match call {
            Ok(()) => success_latencies.push(latency),
            Err(status) => {
                errors += 1;
                failure_latencies.push(latency);
                *error_kinds.entry(status.to_string()).or_insert(0) += 1;
            }
        }
        completed += 1;
        if launched < iterations {
            let channel = channel.clone();
            let payload = payload.clone();
            let expected = expected.clone();
            joins.spawn(async move {
                let call_started = Instant::now();
                let result = echo_once(channel, payload, expected, response_size, timeout).await;
                (result, elapsed_nanos(call_started))
            });
            launched += 1;
        }
    }
    debug_assert_eq!(completed, iterations);

    Ok(ThreadReport {
        operations: iterations,
        errors,
        warmup_errors: 0,
        elapsed: started.elapsed(),
        success_latencies,
        failure_latencies,
        error_kinds,
        allocations: 0,
        allocated_bytes: 0,
    })
}

#[derive(Clone)]
enum RpcChannel {
    Brpc(VaeloChannel),
    Grpc(VaeloChannel),
    Http2(http::Client),
    Tonic(tonic::transport::Channel),
}

async fn echo_once(
    channel: RpcChannel,
    payload: Bytes,
    expected: Bytes,
    response_size: usize,
    timeout: Option<Duration>,
) -> Result<(), Status> {
    let request = EchoRequest { payload, response_size: response_size as u64 };
    match channel {
        RpcChannel::Brpc(channel) | RpcChannel::Grpc(channel) => {
            let mut call = channel.call::<_, EchoResponse>(&ECHO_METHOD, request.clone().into());
            if let Some(timeout) = timeout {
                call = call.timeout(timeout);
            }
            let response = call.await.map_err(Status::from)?;
            if response.payload == expected {
                return Ok(());
            }
            return Err(Status::new(vaelo::StatusCode::Internal, "echo payload mismatch"));
        }
        RpcChannel::Http2(client) => {
            use vaelo::http::BodyExt;

            let mut body = IoBufBuilder::new();
            body.put_slice(&request.payload);
            let request = http::Request::post("/echo")
                .header("x-response-size", response_size.to_string())
                .body(http::Full::new(body.finish()))
                .map_err(|error| Status::new(vaelo::StatusCode::Internal, error.to_string()))?;
            let mut call = client.request(request);
            if let Some(timeout) = timeout {
                call = call.timeout(timeout);
            }
            let response = call
                .await
                .map_err(|error| Status::new(vaelo::StatusCode::Unavailable, error.to_string()))?;
            let body =
                response.into_body().collect().await.map_err(|error| {
                    Status::new(vaelo::StatusCode::Unavailable, error.to_string())
                })?;
            if body.chunks().flatten().copied().eq(expected.iter().copied()) {
                return Ok(());
            }
            return Err(Status::new(vaelo::StatusCode::Internal, "echo payload mismatch"));
        }
        RpcChannel::Tonic(channel) => {
            let mut client = tonic_perf::echo_service_client::EchoServiceClient::new(channel);
            let tonic_response = client
                .echo(tonic_perf::EchoRequest {
                    payload: request.payload.clone(),
                    response_size: request.response_size,
                })
                .await
                .map_err(tonic_status)?
                .into_inner();
            if tonic_response.payload == expected {
                return Ok(());
            }
        }
    }
    Err(Status::new(vaelo::StatusCode::Internal, "echo payload mismatch"))
}

fn rpc_channel(
    protocol: ProtocolChoice,
    address: SocketAddr,
    connections: ConnectionPlan,
    thread_index: usize,
) -> RpcChannel {
    match protocol {
        ProtocolChoice::Brpc => {
            let (connection_mode, connection_tag) = match connections {
                ConnectionPlan::Single => (ConnectionMode::Single, vaelo::ConnectionTag::DEFAULT),
                ConnectionPlan::Tagged => {
                    (ConnectionMode::Single, vaelo::ConnectionTag::new(thread_index as u64 + 1))
                }
                ConnectionPlan::Short => (ConnectionMode::Short, vaelo::ConnectionTag::DEFAULT),
            };
            RpcChannel::Brpc(
                VaeloChannel::brpc(address)
                    .connection_mode(connection_mode)
                    .connection_tag(connection_tag)
                    .build()
                    .expect("performance channel options are valid"),
            )
        }
        ProtocolChoice::Grpc => RpcChannel::Grpc(
            VaeloChannel::grpc(address).build().expect("performance channel options are valid"),
        ),
        ProtocolChoice::Http2 => {
            let (connection_mode, connection_tag) = match connections {
                ConnectionPlan::Single => (ConnectionMode::Single, vaelo::ConnectionTag::DEFAULT),
                ConnectionPlan::Tagged => {
                    (ConnectionMode::Single, vaelo::ConnectionTag::new(thread_index as u64 + 1))
                }
                ConnectionPlan::Short => (ConnectionMode::Short, vaelo::ConnectionTag::DEFAULT),
            };
            RpcChannel::Http2(
                http::Client::http2(address)
                    .connection_mode(connection_mode)
                    .connection_tag(connection_tag)
                    .build()
                    .expect("performance HTTP/2 client options are valid"),
            )
        }
        ProtocolChoice::Tonic => {
            let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .expect("socket address forms a valid tonic endpoint")
                .tcp_nodelay(true);
            RpcChannel::Tonic(endpoint.connect_lazy())
        }
    }
}

fn tonic_status(status: TonicStatus) -> Status {
    let code =
        vaelo::StatusCode::from_i32(status.code() as i32).unwrap_or(vaelo::StatusCode::Unknown);
    Status::new(code, status.message().to_owned())
}

fn start_local_server(config: &Config) -> Result<LocalServer, Box<dyn Error>> {
    let (address_sender, address_receiver) = mpsc::sync_channel(1);
    let (stop_sender, stop_receiver) = mpsc::channel();
    let server_threads = config.server_threads;
    let protocol = config.protocol;
    let delay = config.server_delay;

    let thread = thread::spawn(move || {
        let result =
            run_local_server(server_threads, protocol, delay, address_sender, stop_receiver);
        if let Err(error) = result {
            eprintln!("local perf server failed: {error}");
        }
    });

    let address = address_receiver.recv()??;
    Ok(LocalServer { address, stop: Some(stop_sender), thread: Some(thread) })
}

fn run_local_server(
    server_threads: usize,
    protocol: ProtocolChoice,
    delay: Duration,
    address_sender: SyncSender<Result<SocketAddr, String>>,
    stop_receiver: mpsc::Receiver<()>,
) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(server_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        if protocol == ProtocolChoice::Http2 {
            let server = match ServerBuilder::new()
                .rpc_protocols([])
                .http_versions([http::Version::Http2])
                .shutdown_policy(ShutdownPolicy::graceful().with_timeout(Duration::from_millis(50)))
                .route(http::Method::POST, "/echo", move |request| async move {
                    use vaelo::http::BodyExt;

                    let response_size = request
                        .headers()
                        .get("x-response-size")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or_default();
                    let _ = request.into_body().collect().await?;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Ok(http::Response::new(http::Full::new(make_iobuf(response_size))))
                })
                .bind("127.0.0.1:0")
                .await
            {
                Ok(server) => server,
                Err(error) => {
                    let _ = address_sender.send(Err(error.to_string()));
                    return;
                }
            };
            let address = server.local_addr();
            let handle = server.handle();
            let _ = address_sender.send(Ok(address));
            let serving = tokio::spawn(server.serve());
            let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
            handle.shutdown();
            let _ = serving.await;
            return;
        }
        if protocol == ProtocolChoice::Tonic {
            let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                Ok(listener) => listener,
                Err(error) => {
                    let _ = address_sender.send(Err(error.to_string()));
                    return;
                }
            };
            let address = listener.local_addr().expect("bound listener has a local address");
            let _ = address_sender.send(Ok(address));
            let shutdown = async move {
                let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
            };
            let result = tonic::transport::Server::builder()
                .add_service(tonic_perf::echo_service_server::EchoServiceServer::new(
                    TonicEchoService { delay },
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                .await;
            if let Err(error) = result {
                eprintln!("local tonic perf server failed: {error}");
            }
            return;
        }
        let mut server = Server::new();
        let services = ServiceRegistryBuilder::default().add_service(EchoService { delay }).build();
        let protocols = match protocol {
            ProtocolChoice::Brpc => ProtocolSet::only(Protocol::Brpc),
            ProtocolChoice::Grpc => ProtocolSet::only(Protocol::Http2),
            ProtocolChoice::Http2 | ProtocolChoice::Tonic => unreachable!(),
        };
        let acceptor = RpcAcceptor::new(services).allow_protocols(protocols);
        let listener = match server.listen_tcp("127.0.0.1:0", acceptor).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = address_sender.send(Err(error.to_string()));
                return;
            }
        };
        let address = listener.local_addr();
        let _ = address_sender.send(Ok(address));
        let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
        server.shutdown();
    });
    Ok(())
}

fn run_foreground_server(config: &Config) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.server_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        if config.protocol == ProtocolChoice::Http2 {
            let delay = config.server_delay;
            let server = ServerBuilder::new()
                .rpc_protocols([])
                .http_versions([http::Version::Http2])
                .shutdown_policy(ShutdownPolicy::graceful().with_timeout(Duration::from_millis(50)))
                .route(http::Method::POST, "/echo", move |request| async move {
                    use vaelo::http::BodyExt;

                    let response_size = request
                        .headers()
                        .get("x-response-size")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or_default();
                    let _ = request.into_body().collect().await?;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Ok(http::Response::new(http::Full::new(make_iobuf(response_size))))
                })
                .bind(config.listen)
                .await?;
            println!("server: {}", server.local_addr());
            server
                .serve_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
            print_profile_summary();
            return Ok::<(), Box<dyn Error>>(());
        }
        if config.protocol == ProtocolChoice::Tonic {
            let listener = tokio::net::TcpListener::bind(config.listen).await?;
            println!("server: {}", listener.local_addr()?);
            tonic::transport::Server::builder()
                .add_service(tonic_perf::echo_service_server::EchoServiceServer::new(
                    TonicEchoService { delay: config.server_delay },
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
            return Ok::<(), Box<dyn Error>>(());
        }
        let mut server = Server::new();
        let services = ServiceRegistryBuilder::default()
            .add_service(EchoService { delay: config.server_delay })
            .build();
        let protocols = match config.protocol {
            ProtocolChoice::Brpc => ProtocolSet::only(Protocol::Brpc),
            ProtocolChoice::Grpc => ProtocolSet::only(Protocol::Http2),
            ProtocolChoice::Http2 | ProtocolChoice::Tonic => unreachable!(),
        };
        let acceptor = RpcAcceptor::new(services).allow_protocols(protocols);
        let listener = server.listen_tcp(config.listen, acceptor).await?;
        println!("server: {}", listener.local_addr());
        tokio::signal::ctrl_c().await?;
        server.shutdown();
        print_profile_summary();
        Ok::<(), Box<dyn Error>>(())
    })
}

fn make_payload(size: usize) -> Bytes {
    (0..size).map(|index| (index % 251) as u8).collect::<Vec<_>>().into()
}

fn allocation_snapshot() -> (u64, u64) {
    (ALLOCATIONS.load(AtomicOrdering::Acquire), ALLOCATED_BYTES.load(AtomicOrdering::Acquire))
}

fn make_iobuf(size: usize) -> vaelo::IoBuf {
    let payload = make_payload(size);
    let mut builder = IoBufBuilder::new();
    builder.put_slice(&payload);
    builder.finish()
}

fn print_run_header(config: &Config, address: SocketAddr) {
    println!("protocol: {:?}", config.protocol);
    println!("mode: {:?}", config.mode);
    println!("scenario: {:?}", config.scenario);
    println!("connections: {:?}", config.connections);
    println!("server: {address}");
    println!("threads: {}", config.threads);
    println!("iters/thread: {}", config.iters_per_thread);
    println!("window/thread: {}", config.window);
    println!("payload bytes: {}", config.payload_size);
    println!("response bytes: {}", config.response_size.unwrap_or(config.payload_size));
    println!("warmup/thread: {}", config.warmup);
    println!("timeout: {}", config.timeout.map_or_else(|| "none".to_owned(), format_duration));
    println!("server delay: {}", format_duration(config.server_delay));
    println!("server threads: {}", config.server_threads);
    println!("profile sample: {}", config.profile_sample);
    println!("coalesce small frames: {}", config.coalesce_small_frames);
    println!("count allocations: {}", config.count_allocations);
}

fn print_summary(config: &Config, mut reports: Vec<ThreadReport>) -> usize {
    let operations = reports.iter().map(|report| report.operations).sum::<usize>();
    let errors = reports.iter().map(|report| report.errors).sum::<usize>();
    let warmup_errors = reports.iter().map(|report| report.warmup_errors).sum::<usize>();
    let elapsed = reports.iter().map(|report| report.elapsed).max().unwrap_or_default();
    let throughput = operations as f64 / elapsed.as_secs_f64();
    let success = operations.saturating_sub(errors);

    let mut success_latencies = reports
        .iter_mut()
        .flat_map(|report| report.success_latencies.drain(..))
        .collect::<Vec<_>>();
    let mut failure_latencies = reports
        .iter_mut()
        .flat_map(|report| report.failure_latencies.drain(..))
        .collect::<Vec<_>>();
    success_latencies.sort_unstable();
    failure_latencies.sort_unstable();

    println!();
    println!("operations: {operations}");
    println!("success: {success}");
    println!("errors: {errors}");
    println!("warmup errors: {warmup_errors}");
    println!("measured elapsed: {}", format_duration(elapsed));
    println!("throughput: {:.2} ops/s", throughput);
    println!("avg latency: {}", format_duration(Duration::from_nanos(average(&success_latencies))));
    println!("min latency: {}", percentile(&success_latencies, 0.0));
    println!("p50 latency: {}", percentile(&success_latencies, 50.0));
    println!("p90 latency: {}", percentile(&success_latencies, 90.0));
    println!("p99 latency: {}", percentile(&success_latencies, 99.0));
    println!("p999 latency: {}", percentile(&success_latencies, 99.9));
    println!("max latency: {}", percentile(&success_latencies, 100.0));
    if config.count_allocations {
        let allocations = reports.iter().map(|report| report.allocations).sum::<u64>();
        let bytes = reports.iter().map(|report| report.allocated_bytes).sum::<u64>();
        println!("allocations: {allocations}");
        println!("allocations/op: {:.4}", allocations as f64 / operations as f64);
        println!("allocated bytes: {bytes}");
        println!("allocated bytes/op: {:.2}", bytes as f64 / operations as f64);
    }
    if !failure_latencies.is_empty() {
        println!("failure p50 latency: {}", percentile(&failure_latencies, 50.0));
        println!("failure p99 latency: {}", percentile(&failure_latencies, 99.0));
    }

    if config.scenario == Scenario::Deadline {
        println!("deadline error ratio: {:.4}", errors as f64 / operations as f64);
    }

    let mut error_kinds = HashMap::<String, usize>::new();
    for report in reports {
        for (kind, count) in report.error_kinds {
            *error_kinds.entry(kind).or_insert(0) += count;
        }
    }
    if !error_kinds.is_empty() {
        let mut error_kinds = error_kinds.into_iter().collect::<Vec<_>>();
        error_kinds.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        println!("error kinds:");
        for (kind, count) in error_kinds.into_iter().take(8) {
            println!("  {count}: {kind}");
        }
    }
    errors + warmup_errors
}

fn print_profile_summary() {
    let snapshots = vaelo::__private::profile::snapshot();
    let events = vaelo::__private::profile::event_snapshot();
    if snapshots.is_empty() && events.is_empty() {
        return;
    }

    if !snapshots.is_empty() {
        println!();
        println!("profile:");
        println!("  {:<30} {:>10} {:>12} {:>12} {:>12}", "stage", "samples", "avg", "max", "total");
        for snapshot in snapshots {
            println!(
                "  {:<30} {:>10} {:>12} {:>12} {:>12}",
                snapshot.name,
                snapshot.samples,
                format_duration(snapshot.average),
                format_duration(snapshot.max),
                format_duration(snapshot.total),
            );
        }
    }

    if !events.is_empty() {
        println!();
        println!("profile events:");
        println!("  {:<30} {:>10} {:>12} {:>12} {:>12}", "event", "samples", "avg", "max", "total");
        for event in events {
            println!(
                "  {:<30} {:>10} {:>12} {:>12} {:>12}",
                event.name, event.samples, event.average, event.max, event.total,
            );
        }
    }
}

fn average(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let sum = values.iter().map(|&value| value as u128).sum::<u128>();
    (sum / values.len() as u128) as u64
}

fn percentile(values: &[u64], percentile: f64) -> String {
    if values.is_empty() {
        return "n/a".to_owned();
    }
    let rank = match percentile.partial_cmp(&100.0) {
        Some(Ordering::Greater | Ordering::Equal) => values.len() - 1,
        _ => {
            let position = percentile / 100.0 * (values.len().saturating_sub(1)) as f64;
            position.round() as usize
        }
    };
    format_duration(Duration::from_nanos(values[rank]))
}

fn elapsed_nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn parse_mode(value: &str) -> Result<RunMode, Box<dyn Error>> {
    match value {
        "local" => Ok(RunMode::Local),
        "server" => Ok(RunMode::Server),
        "client" => Ok(RunMode::Client),
        _ => Err(format!("unknown mode `{value}`").into()),
    }
}

fn parse_protocol(value: &str) -> Result<ProtocolChoice, Box<dyn Error>> {
    match value {
        "brpc" => Ok(ProtocolChoice::Brpc),
        "grpc" => Ok(ProtocolChoice::Grpc),
        "http2" => Ok(ProtocolChoice::Http2),
        "tonic" => Ok(ProtocolChoice::Tonic),
        _ => Err(format!("unknown protocol `{value}`").into()),
    }
}

fn parse_scenario(value: &str) -> Result<Scenario, Box<dyn Error>> {
    match value {
        "ping-pong" => Ok(Scenario::PingPong),
        "window" => Ok(Scenario::Window),
        "deadline" => Ok(Scenario::Deadline),
        _ => Err(format!("unknown scenario `{value}`").into()),
    }
}

fn parse_connections(value: &str) -> Result<ConnectionPlan, Box<dyn Error>> {
    match value {
        "single" => Ok(ConnectionPlan::Single),
        "tagged" => Ok(ConnectionPlan::Tagged),
        "short" => Ok(ConnectionPlan::Short),
        _ => Err(format!("unknown connection mode `{value}`").into()),
    }
}

fn parse_server(value: &str) -> Result<ServerTarget, Box<dyn Error>> {
    match value {
        "local" => Ok(ServerTarget::Local),
        _ => Ok(ServerTarget::Remote(value.parse()?)),
    }
}

fn parse_usize(
    option: &str,
    args: &mut impl Iterator<Item = String>,
) -> Result<usize, Box<dyn Error>> {
    next_value(option, args)?.parse().map_err(Into::into)
}

fn parse_u64(option: &str, args: &mut impl Iterator<Item = String>) -> Result<u64, Box<dyn Error>> {
    next_value(option, args)?.parse().map_err(Into::into)
}

fn next_value(
    option: &str,
    args: &mut impl Iterator<Item = String>,
) -> Result<String, Box<dyn Error>> {
    args.next().ok_or_else(|| format!("{option} requires a value").into())
}

fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.2}us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

fn usage() -> impl fmt::Display {
    "\
vaelo-perf

USAGE:
  cargo run -p vaelo-perf --release -- [options]

OPTIONS:
  --mode local|server|client
  --protocol brpc|grpc|http2|tonic
  --scenario ping-pong|window|deadline
  --connections single|tagged|short
  --server local|HOST:PORT
  --listen HOST:PORT
  --threads N
  --iters N
  --window N
  --payload-size BYTES
  --response-size BYTES
  --warmup N
  --timeout-ms MS
  --server-delay-us US
  --server-threads N
  --profile-sample N
  --log-level off|error|warn|info|debug|trace
  --coalesce-small-frames
  --count-allocations

EXAMPLES:
  cargo run -p vaelo-perf --release -- --scenario ping-pong --threads 8 --iters 1000000
  cargo run -p vaelo-perf --release -- --mode server --listen 127.0.0.1:8002 --server-threads 8
  cargo run -p vaelo-perf --release -- --mode client --server 127.0.0.1:8002 --threads 8 --iters 1000000
  cargo run -p vaelo-perf --release -- --scenario ping-pong --profile-sample 100
  cargo run -p vaelo-perf --release -- --scenario ping-pong --coalesce-small-frames
  cargo run -p vaelo-perf --release -- --scenario window --threads 8 --window 16 --connections tagged
  cargo run -p vaelo-perf --release -- --scenario deadline --timeout-ms 1 --server-delay-us 5000
"
}
