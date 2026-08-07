use std::net::SocketAddr;
use std::time::Duration;

use bytes::BufMut;
use vaelo::http::{BodyExt, ServerRequestExt};

use crate::ShowcaseService;
use crate::generated::showcase_server::ShowcaseServer;

pub fn server() -> vaelo::ServerBuilder {
    vaelo::ServerBuilder::new()
        .add_service(ShowcaseServer::new(ShowcaseService))
        .http_options(
            vaelo::http::ServerOptions::default()
                .enable_rpc_mapping(vaelo::http::RpcMapping::default()),
        )
        .route(
            vaelo::http::Method::POST,
            "/users/:id/echo",
            |request: vaelo::http::Request<vaelo::http::Incoming>| async move {
                let _peer = request.remote_addr();
                let id = request.path_param("id").unwrap_or("unknown").to_owned();
                let body = request.into_body().collect().limit(1024 * 1024).await?;
                let mut output = vaelo::IoBufBuilder::new();
                output.put_slice(id.as_bytes());
                output.put_slice(b":");
                for chunk in body.chunks() {
                    output.put_slice(chunk);
                }
                Ok(vaelo::http::Response::new(vaelo::http::Full::new(output.finish())))
            },
        )
        .route_service(
            vaelo::http::Method::POST,
            "/stateful",
            PrefixService { prefix: "server:".into() },
        )
}

struct PrefixService {
    prefix: String,
}

impl vaelo::http::Service for PrefixService {
    type ResponseBody = vaelo::http::Full;

    async fn call(
        &self,
        request: vaelo::http::Request<vaelo::http::Incoming>,
    ) -> Result<vaelo::http::Response<Self::ResponseBody>, vaelo::http::Error> {
        let body = request.into_body().collect().limit(1024 * 1024).await?;
        let mut output = vaelo::IoBufBuilder::new();
        output.put_slice(self.prefix.as_bytes());
        for chunk in body.chunks() {
            output.put_slice(chunk);
        }
        Ok(vaelo::http::Response::new(vaelo::http::Full::new(output.finish())))
    }
}

pub async fn client(address: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let client = vaelo::http::Client::http1(address)
        .connection_mode(vaelo::ConnectionMode::Single)
        .build()?;

    let mut body = vaelo::IoBufBuilder::new();
    body.put_slice(b"hello");
    let request = vaelo::http::Request::post("/users/42/echo")
        .header(http::header::HOST, "localhost")
        .body(vaelo::http::Full::new(body.finish()))?;

    let response = client.request(request).timeout(Duration::from_secs(1)).await?;
    let _body = response.into_body().collect().limit(1024 * 1024).await?;
    Ok(())
}

pub async fn streaming_upload(address: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let frames = tokio_stream::iter([
        Ok::<_, vaelo::http::Error>(vaelo::http::Frame::Data(iobuf(b"first"))),
        Ok(vaelo::http::Frame::Data(iobuf(b"second"))),
    ]);
    let body = vaelo::http::Stream::new(frames);
    let request = vaelo::http::Request::post("/upload").body(body)?;
    let client = vaelo::http::Client::http1(address).build()?;
    let _response = client.request(request).await?;
    Ok(())
}

fn iobuf(bytes: &[u8]) -> vaelo::IoBuf {
    let mut builder = vaelo::IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}
