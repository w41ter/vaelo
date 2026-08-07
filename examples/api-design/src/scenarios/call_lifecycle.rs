use std::net::SocketAddr;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::brpc(address).build()?);

    let unsent = client.echo(EchoRequest { message: "not sent".into() });
    drop(unsent);

    let response = client.echo(EchoRequest { message: "await starts".into() }).await?;
    println!("{}", response.message);

    let handle = client.echo(EchoRequest { message: "explicit start".into() }).start();
    let _ = handle.await?;

    client.echo(EchoRequest { message: "fire and forget".into() }).start().detach();
    Ok(())
}
