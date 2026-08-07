use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);

    let _ = client
        .slow(EchoRequest { message: "timeout".into() })
        .timeout(Duration::from_millis(50))
        .await;

    let handle = client
        .slow(EchoRequest { message: "cancel".into() })
        .deadline(Instant::now() + Duration::from_secs(5))
        .start();
    handle.cancel();
    let _final_result = handle.await;
    Ok(())
}
