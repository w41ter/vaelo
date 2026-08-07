//! Compile-time API design scenarios.
//!
//! These examples intentionally describe the target public API before every
//! runtime path is implemented. They are not compatibility or performance
//! claims; they are executable design fixtures which must never depend on
//! vaelo::__private.

use std::time::Duration;

use tokio_stream::StreamExt;
use vaelo::brpc::ServerRequestExt;
use vaelo::{Request, Response, Status, StatusCode, StatusDetails, Streaming};

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/showcase.rs"));
}

pub mod scenarios;

use generated::showcase_server::Showcase;
use generated::{ChatMessage, Chunk, EchoRequest, EchoResponse, UploadSummary, WatchRequest};

#[derive(Default)]
pub struct ShowcaseService;

impl Showcase for ShowcaseService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let (context, message) = request.into_parts();
        let mut response = Response::new(EchoResponse { message: message.message });
        if let Some(traceparent) = context.metadata().get("traceparent") {
            response
                .metadata_mut()
                .insert("traceparent", vaelo::MetadataValue::copy_from_slice(traceparent))
                .expect("static metadata key");
        }
        if context.protocol() == Some(vaelo::RequestProtocol::Brpc) {
            use vaelo::brpc::ResponseExt;
            response.set_attachment(context.attachment().clone());
        }
        Ok(response)
    }

    async fn fail(&self, _request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Err(Status::new(StatusCode::InvalidArgument, "example rejection")
            .with_details(StatusDetails::from_static(b"structured-error")))
    }

    async fn slow(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let (context, message) = request.into_parts();
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                Ok(Response::new(EchoResponse { message: message.message }))
            }
            _ = context.cancelled() => Err(Status::new(StatusCode::Canceled, "cancelled")),
        }
    }

    type WatchStream = tokio_stream::Iter<std::vec::IntoIter<Result<EchoResponse, Status>>>;

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let count = request.into_inner().count;
        let messages = (0..count)
            .map(|value| Ok(EchoResponse { message: value.to_string() }))
            .collect::<Vec<_>>();
        Ok(Response::new(tokio_stream::iter(messages)))
    }

    async fn upload(
        &self,
        request: Request<Streaming<Chunk, Status>>,
    ) -> Result<Response<UploadSummary>, Status> {
        let mut stream = request.into_inner();
        let mut bytes_received = 0;
        while let Some(chunk) = stream.next().await {
            bytes_received += chunk?.data.len() as u64;
        }
        Ok(Response::new(UploadSummary { bytes_received }))
    }

    type ChatStream = tokio_stream::Iter<std::vec::IntoIter<Result<ChatMessage, Status>>>;

    async fn chat(
        &self,
        request: Request<Streaming<ChatMessage, Status>>,
    ) -> Result<Response<Self::ChatStream>, Status> {
        let mut input = request.into_inner();
        let mut replies = Vec::new();
        while let Some(message) = input.next().await {
            let message = message?;
            replies.push(Ok(ChatMessage {
                sender: "server".into(),
                text: format!("{}: {}", message.sender, message.text),
            }));
        }
        Ok(Response::new(tokio_stream::iter(replies)))
    }
}
