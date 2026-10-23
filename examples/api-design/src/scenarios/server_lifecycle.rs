use crate::ShowcaseService;
use crate::generated::showcase_server::ShowcaseServer;

pub async fn bind() -> std::io::Result<vaelo::BoundServer> {
    vaelo::ServerBuilder::new()
        .rpc_protocols([vaelo::RpcProtocol::Brpc, vaelo::RpcProtocol::Grpc])
        .http_versions([vaelo::http::Version::Http1, vaelo::http::Version::Http2])
        .add_service(ShowcaseServer::new(ShowcaseService))
        .bind("127.0.0.1:0")
        .await
}

pub async fn serve() -> std::io::Result<()> {
    let server = bind().await?;
    println!("listening on {}", server.local_addr());
    let handle = server.handle();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("install ctrl-c handler");
        handle.shutdown();
    });

    server.serve().await
}
