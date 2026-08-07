use std::net::SocketAddr;

use bytes::BufMut;
use vaelo::brpc::ResponseExt;

use crate::generated::EchoRequest;
use crate::generated::showcase_client::ShowcaseClient;

pub async fn run(address: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::brpc(address).build()?);

    let mut common = vaelo::Request::new(EchoRequest { message: "brpc".into() });
    common.metadata_mut().insert(
        vaelo::MetadataKey::from_static("traceparent"),
        vaelo::MetadataValue::from_static(b"00-brpc"),
    )?;

    let mut attachment = vaelo::IoBufBuilder::new();
    attachment.put_slice(b"segmented attachment");
    let request = vaelo::brpc::Request::from(common)
        .request_id("request-42")
        .log_id(1001)
        .attachment(attachment.finish());

    let response = client.echo(request).await?;
    assert!(!response.attachment().is_empty());
    Ok(())
}
