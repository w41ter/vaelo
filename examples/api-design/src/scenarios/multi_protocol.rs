use std::net::SocketAddr;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> vaelo::Result<()> {
    let brpc = ShowcaseClient::new(vaelo::RpcChannel::brpc(address).build()?);
    let grpc = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);

    let request = || EchoRequest { message: "same service".into() };
    brpc.echo(request()).await?;
    grpc.echo(request()).await?;
    Ok(())
}
