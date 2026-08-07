# Vaelo API design scenarios

This crate is a compile-time design fixture for the intended public API. The
network paths may still contain implementation placeholders; the examples
exist to evaluate ownership, lifecycle, backpressure and ergonomics before the
API is stabilized.

The scenarios cover:

- generated unary client and service code;
- bRPC and gRPC clients for one service;
- bind, ephemeral addresses, serving and shutdown;
- owned metadata and request context;
- bRPC request id, log id and segmented attachments;
- remote status details and execution certainty;
- timeout, absolute deadline and explicit cancellation;
- unstarted calls, eager start and detach;
- traffic tags, connection modes and optional observation;
- HTTP routes, bounded collection and streaming bodies;
- server-, client- and bidirectional-streaming RPC, including optional
  writable sessions with transport backpressure;
- segmented large payload processing without flattening;
- opt-in health, version, status and variables endpoints.

No scenario may import `vaelo::__private`. A scenario which requires that
module has identified a missing public API.
