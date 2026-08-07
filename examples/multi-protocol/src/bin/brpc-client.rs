// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use demo::brpc_echo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned())
        .parse::<SocketAddr>()?;
    let message = std::env::args().nth(2).unwrap_or_else(|| "hello brpc".to_owned());
    let response = brpc_echo(address, message).await?;
    println!("{response}");
    Ok(())
}
