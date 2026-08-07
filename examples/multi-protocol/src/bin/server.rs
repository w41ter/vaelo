// SPDX-License-Identifier: Apache-2.0

use demo::start_server;

#[tokio::main]
async fn main() -> vaelo::Result<()> {
    let server = start_server().await?;
    println!("multi-protocol echo server listening on {}", server.address());
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| vaelo::Error::Channel(error.to_string().into()))?;
    server.shutdown();
    Ok(())
}
