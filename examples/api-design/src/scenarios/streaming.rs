use std::net::SocketAddr;

use crate::generated::showcase_client::ShowcaseClient;
use crate::generated::{ChatMessage, Chunk, WatchRequest};

pub async fn server_streaming(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);
    let response = client.watch(WatchRequest { count: 10 }).await?;
    let _initial_metadata = response.metadata();
    let mut stream = response.into_inner();

    while let Some(message) = stream.message().await? {
        println!("{}", message.message);
    }
    let _trailers = stream.finish().await?;
    let _stats = stream.stats();
    Ok(())
}

pub async fn client_streaming(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);
    let mut upload = client.upload().start();
    upload.send(Chunk { data: b"first".to_vec() }).await?;
    upload.send(Chunk { data: b"second".to_vec() }).await?;
    let response = upload.finish().await?;
    println!("{}", response.bytes_received);
    Ok(())
}

pub async fn bidirectional(address: SocketAddr) -> vaelo::Result<()> {
    let client = ShowcaseClient::new(vaelo::RpcChannel::grpc(address).build()?);
    let (mut sender, response) = client.chat().start().into_parts();

    sender.send(ChatMessage { sender: "client".into(), text: "hello".into() }).await?;
    sender.finish().await?;

    let mut incoming = response.await?.into_inner();
    while let Some(reply) = incoming.message().await? {
        println!("{}: {}", reply.sender, reply.text);
    }
    incoming.finish().await?;
    Ok(())
}
