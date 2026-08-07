// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/echo.proto");
    println!("cargo:rerun-if-changed=proto/unsupported.proto");
    vaelo_build::configure().compile_protos(&["proto/echo.proto"], &["proto"])?;
    vaelo_build::configure().compile_protos(&["proto/unsupported.proto"], &["proto"])?;
    Ok(())
}
