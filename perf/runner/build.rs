// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/echo.proto");
    tonic_prost_build::configure()
        .bytes(".perf.EchoRequest.payload")
        .bytes(".perf.EchoResponse.payload")
        .compile_protos(&["proto/echo.proto"], &["proto"])?;
    Ok(())
}
