use std::net::SocketAddr;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);
    let mut request = vaelo::Request::new(EchoRequest { message: "context".into() });
    request.metadata_mut().insert(
        vaelo::MetadataKey::from_static("traceparent"),
        vaelo::MetadataValue::from_static(b"00-example"),
    )?;

    let response = client.echo(request).await?;
    assert_eq!(response.metadata().get("traceparent"), Some(b"00-example".as_slice()));
    Ok(())
}
