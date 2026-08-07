// SPDX-License-Identifier: Apache-2.0

//! Cargo entry point; tools/dev/verify.py is the single workflow
//! implementation.
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .unwrap();
    match Command::new("python3")
        .arg(root.join("tools/dev/verify.py"))
        .args(std::env::args_os().skip(1))
        .current_dir(root)
        .status()
    {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
        Err(error) => {
            eprintln!("cannot start development workflow (Python 3.9+ required): {error}");
            ExitCode::FAILURE
        }
    }
}
