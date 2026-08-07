// SPDX-License-Identifier: Apache-2.0

//! A real generated-code consumer. Compilation catches interface drift.
pub mod fixture {
    include!(concat!(env!("OUT_DIR"), "/fixture.rs"));
}

#[allow(dead_code)]
mod unsupported {
    include!(concat!(env!("OUT_DIR"), "/unsupported.rs"));
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use vaelo::__private::{RpcAcceptor, Server, ServiceRegistryBuilder};
    use vaelo::brpc::ServerRequestExt;
    use vaelo::{Request, Response, RpcChannel, RpcProtocol, Status, StatusCode};

    use super::fixture::echo_service_client::EchoServiceClient;
    use super::fixture::echo_service_server::{EchoService, EchoServiceServer};
    use super::fixture::{EchoRequest, EchoResponse};

    struct Echo;

    impl EchoService for Echo {
        async fn echo(
            &self,
            request: Request<EchoRequest>,
        ) -> Result<Response<EchoResponse>, Status> {
            if request.protocol() == Some(vaelo::RequestProtocol::Brpc) {
                assert_eq!(request.request_id(), Some("codegen-interop"));
                assert!(request.deadline().is_some());
            }
            let request = request.into_inner();
            if request.message == "reject" {
                return Err(Status::new(StatusCode::InvalidArgument, "rejected by fixture"));
            }
            Ok(Response::new(EchoResponse { message: request.message }))
        }
    }

    async fn round_trip(protocol: RpcProtocol) {
        let registry =
            ServiceRegistryBuilder::default().add_service(EchoServiceServer::new(Echo)).build();
        let mut server = Server::new();
        let listener = server.listen_tcp("127.0.0.1:0", RpcAcceptor::new(registry)).await.unwrap();
        let channel = RpcChannel::rpc(listener.local_addr(), protocol).build().unwrap();
        let client = EchoServiceClient::new(channel);
        let call = match protocol {
            RpcProtocol::Brpc => client.echo(
                vaelo::brpc::Request::new(EchoRequest { message: "generated client".into() })
                    .request_id("codegen-interop"),
            ),
            RpcProtocol::Grpc => client.echo(EchoRequest { message: "generated client".into() }),
            _ => unreachable!("test only exercises configured protocols"),
        };
        let response = call.timeout(Duration::from_secs(3)).await.unwrap();
        assert_eq!(response.into_inner().message, "generated client");
        let call = match protocol {
            RpcProtocol::Brpc => client.echo(
                vaelo::brpc::Request::new(EchoRequest { message: "reject".into() })
                    .request_id("codegen-interop"),
            ),
            RpcProtocol::Grpc => client.echo(EchoRequest { message: "reject".into() }),
            _ => unreachable!("test only exercises configured protocols"),
        };
        let error = call
            .timeout(Duration::from_secs(3))
            .await
            .err()
            .expect("service rejection should reach generated client");
        // bRPC's wire error mapping intentionally collapses application codes.
        let expected = match protocol {
            RpcProtocol::Brpc => StatusCode::Internal,
            RpcProtocol::Grpc => StatusCode::InvalidArgument,
            _ => unreachable!("test only exercises configured protocols"),
        };
        let status = error.remote_status().expect("service status");
        assert_eq!(status.code(), expected);
        assert_eq!(status.message(), "rejected by fixture");
        server.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generated_brpc_client_and_service_exchange_success_and_error() {
        tokio::time::timeout(Duration::from_secs(10), round_trip(RpcProtocol::Brpc)).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generated_grpc_client_and_service_exchange_success_and_error() {
        tokio::time::timeout(Duration::from_secs(10), round_trip(RpcProtocol::Grpc)).await.unwrap();
    }
}
