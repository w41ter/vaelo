# Third-party provenance

Vaelo is an independent Rust implementation. It does not vendor source files
from the projects listed below. This document records the external material
used to implement wire compatibility and the exact references checked before
the repository was prepared for public release.

Unless noted below, the repository is Copyright 2026 w41ter and is licensed
under the Apache License, Version 2.0.

## Apache bRPC

- Project: <https://github.com/apache/brpc>
- License: Apache License, Version 2.0
- Reference revision: `87d498dd62d8b3b3562407cdf38ac0c475d312b3`
- Revision checked: 2026-10-22
- Upstream files:
  - `src/brpc/policy/baidu_rpc_meta.proto`
  - `src/brpc/errno.proto`
  - `LICENSE`
  - `NOTICE`

Vaelo's BAIDU_STD codec re-declares the protobuf field numbers and wire types
published in `baidu_rpc_meta.proto`. Its error translation uses selected
numeric constants published in `errno.proto`. The surrounding framing,
encoding, transport, client, and server implementations are native Rust code;
no Apache bRPC source file is included in this repository.

The Apache bRPC attribution required by its NOTICE file is reproduced in this
repository's [NOTICE](NOTICE).

## Standards references

The following standards define interoperable wire behavior rather than code
incorporated from another software project:

- HPACK and its canonical Huffman table: RFC 7541, especially Appendix B,
  <https://www.rfc-editor.org/rfc/rfc7541>
- HTTP/2: RFC 9113, <https://www.rfc-editor.org/rfc/rfc9113>
- gRPC over HTTP/2 protocol behavior:
  <https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md>

The HPACK table in `crates/vaelo/src/protocol/http2/hpack/huffman.rs` is the
normative code table from RFC 7541 Appendix B. The encoder and decoder around
that table are implemented in Rust in this repository.

## Maintenance

When code or data is copied, translated, or adapted from a new external
source, update this file and NOTICE in the same change. Record the upstream
project, immutable revision, source paths, license, and the local files that
use the material.
