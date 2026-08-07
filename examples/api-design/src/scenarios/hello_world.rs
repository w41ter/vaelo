use std::net::SocketAddr;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> vaelo::Result<()> {
    let channel = vaelo::RpcChannel::brpc(address).build()?;
    let client = ShowcaseClient::new(channel);
    let response = client.echo(EchoRequest { message: "hello".into() }).await?;
    println!("{}", response.message);
    Ok(())
}
