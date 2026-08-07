use std::net::SocketAddr;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);
    let error = match client.fail(EchoRequest { message: "reject".into() }).await {
        Ok(_) => panic!("example returns an error"),
        Err(error) => error,
    };

    if let Some(status) = error.remote_status() {
        println!("{:?}: {}", status.code(), status.message());
    }
    println!("execution={:?}", error.execution_certainty());
    Ok(())
}
