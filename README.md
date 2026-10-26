# Vaelo

**A high-performance, low-latency, multi-protocol RPC framework for Rust.**

Vaelo supports bRPC/BAIDU_STD, gRPC, HTTP/1.1, and HTTP/2, allowing applications to serve multiple
client ecosystems without duplicating service implementations. Performance is treated as a core
design constraint and tracked through reproducible benchmarks and regression gates.

Vaelo is built for teams evolving their RPC infrastructure: keep existing bRPC services while
adopting the gRPC ecosystem, or expose RPC and HTTP interfaces through one service model.

> Vaelo is currently at version 0.1, and its public API is not yet stable. Core unary calls,
> multi-protocol serving, and tonic interoperability are covered by automated tests. Before using
> Vaelo in production, validate it against your own workloads and failure scenarios.

## Why Vaelo

- **Built for latency-sensitive workloads.** Performance is measured continuously with
  reproducible scenarios, managed baselines, and regression gates.
- **Support multiple protocols in one service.** Serve bRPC, gRPC, and HTTP clients from the same
  instance—even on the same port—while keeping one service implementation.
- **Evolve incrementally.** Run old and new protocols side by side instead of tying infrastructure
  upgrades to a big-bang application migration.
- **Keep the application interface simple.** Vaelo handles protocol detection, connection
  management, and timeouts so applications can focus on services and request handling.
- **Preserve protocol semantics.** Vaelo offers a consistent experience without hiding meaningful
  differences in errors, deadlines, or connection behavior.

## Capabilities

| Capability | Status |
| --- | --- |
| bRPC/BAIDU_STD unary | Client and server support, including protobuf and attachments |
| gRPC unary | Client and server support, tested for interoperability in both directions with tonic |
| gRPC streaming | Incremental client and server APIs for server/client/bidirectional streaming, with code generation and tonic interoperability |
| HTTP/1.1 | Client support, server routing, and RPC mapping |
| HTTP/2 | Server and gRPC paths; a general-purpose HTTP/2 client is not yet implemented |
| Multiple protocols on one port | bRPC, HTTP/1.1, HTTP/2, and gRPC over HTTP/2 |
| Connection strategies | Reuse, pooling, short connections, connection groups, connect timeout, one-shot endpoint resolution, and explicit cache eviction |
| Service discovery | Direct `SocketAddr`, built-in `dns://` and `list://` targets, pluggable naming schemes, TTL-bounded stale snapshots, and fail-fast unresolved calls |
| Load balancing | Lock-free round-robin selection across discovered endpoints |
| Observability | Logging, runtime metrics, performance sampling, and built-in health/status endpoints |

Vaelo does not yet promise support for TLS, load-balancing policies beyond round robin, or automatic
retries. Server shutdown stops new connection admission; applications remain responsible for
draining or cancelling their own long-lived business tasks.

## Performance

Performance claims should be reproducible. Vaelo includes benchmark suites for measuring latency
and throughput, comparing results with established baselines, and detecting regressions.

As a reference, the standard throughput scenarios produced the following results on a dual-socket
Intel Xeon Gold 5218 host (32 cores / 64 threads), running Debian 10 with Linux 5.15.120. Client and
server ran on the same host over loopback, with two client workers and two server workers. Each
client worker kept a closed-loop window of 16 in-flight RPCs, for a maximum of 32 concurrent RPCs;
a completed request was immediately replaced. Each round used 1,000 warmup and 10,000 measured
requests per client worker, and each scenario ran for three rounds.

| Request / response | Vaelo bRPC | Vaelo gRPC | Vaelo HTTP/2 |
| --- | ---: | ---: | ---: |
| 64 B / 64 B | 138.7k ops/s | 78.2k ops/s | 79.0k ops/s |
| 1 KiB / 1 KiB | 133.7k ops/s | 74.6k ops/s | 69.9k ops/s |
| 16 KiB / 16 KiB | 27.9k ops/s | 31.3k ops/s | 34.3k ops/s |
| 64 KiB / 64 KiB | 7.8k ops/s | 11.5k ops/s | 12.1k ops/s |
| 64 KiB / 64 B (upload) | 36.8k ops/s | 30.1k ops/s | 30.1k ops/s |
| 64 B / 64 KiB (download) | 10.6k ops/s | 14.0k ops/s | 13.2k ops/s |

The gRPC and HTTP/2 runs multiplexed all in-flight RPCs over one connection. bRPC used one tagged
connection per client worker for the 64 B and 1 KiB cases, and one shared connection for the other
cases. With these connection layouts, bRPC leads on small messages and upload-heavy traffic. gRPC
and HTTP/2 are faster once symmetric payloads reach 16 KiB, and also perform better in the
download-heavy case. These numbers are a snapshot from one machine, not a cross-platform
performance guarantee.

Run the quick performance suite:

```sh
cargo xtask perf run --suite quick --offline
```

The performance tooling also supports scenario filtering, result analysis, baseline management,
and regression gates. See [`perf/README.md`](perf/README.md) for details. Results are written to
`target/verification/` by default.

## Try It in a Minute

The project uses `nightly-2026-07-22` and requires Python 3.9+ and `protoc`.

Start the multi-protocol echo server:

```sh
cargo run -p demo --bin server
```

The server prints its listening address. Substitute its port for `PORT`, then reach the same
service over two different protocols:

```sh
cargo run -p demo --bin brpc-client -- 127.0.0.1:PORT "hello from bRPC"
cargo run -p demo --bin http-client -- 127.0.0.1:PORT "hello from HTTP"
```

See [`examples/multi-protocol`](examples/multi-protocol) for the complete example.

## Development and Verification

Run the complete project check:

```sh
cargo xtask check
```

This checks formatting, runs workspace tests and Clippy, and verifies code generation and gRPC
interoperability. Add `--offline` when the dependencies are already cached.

## License and Attribution

Vaelo is licensed under the [Apache License 2.0](LICENSE). Its BAIDU_STD compatibility uses the
published Apache bRPC wire schema and error-code constants; see [NOTICE](NOTICE) and
[THIRD_PARTY.md](THIRD_PARTY.md) for attribution and provenance.
