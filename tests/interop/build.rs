// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/echo.proto");
    tonic_prost_build::compile_protos("proto/echo.proto")?;
    Ok(())
}
