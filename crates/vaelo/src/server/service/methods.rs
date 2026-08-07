// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BufMut;
use tokio_stream::StreamExt;

use super::{RegisteredMethod, ServerCall};
use crate::server::ServerResponse;
use crate::server::http::{HttpRpcRequest, RpcBodyEncoding, RpcResponseEncoding};
use crate::{
    IoBuf, IoBufBuilder, Request, Response, ServerController, Status, StatusCode, Streaming,
};

/// Generic prost unary method. `Req`, `Resp`, and the service type remain known
/// here even though the registry stores this value as `dyn RegisteredMethod`.
pub struct UnaryProstMethod<S, Req, Resp, Invoke> {
    service: Arc<S>,
    invoke: Invoke,
    http_codec: Option<HttpMessageCodec<Req, Resp>>,
    _messages: PhantomData<fn(Req) -> Resp>,
}

impl<S, Req, Resp, Invoke> UnaryProstMethod<S, Req, Resp, Invoke> {
    pub fn new(service: Arc<S>, invoke: Invoke) -> Self {
        Self { service, invoke, http_codec: None, _messages: PhantomData }
    }
}

/// Adapter for an incremental server-streaming prost method.
///
/// The stable service contract returns a concrete `Stream`; the current
/// `Vec`-based implementation below is transitional and must be replaced
/// without changing the generated service signature.
pub struct ServerStreamingProstMethod<S, Req, Resp, Output, Invoke> {
    service: Arc<S>,
    invoke: Invoke,
    _messages: PhantomData<fn(Req) -> (Resp, Output)>,
}

impl<S, Req, Resp, Output, Invoke> ServerStreamingProstMethod<S, Req, Resp, Output, Invoke> {
    pub fn new(service: Arc<S>, invoke: Invoke) -> Self {
        Self { service, invoke, _messages: PhantomData }
    }
}

pub struct ClientStreamingProstMethod<S, Req, Resp, Invoke> {
    service: Arc<S>,
    invoke: Invoke,
    _messages: PhantomData<fn(Req) -> Resp>,
}

impl<S, Req, Resp, Invoke> ClientStreamingProstMethod<S, Req, Resp, Invoke> {
    pub fn new(service: Arc<S>, invoke: Invoke) -> Self {
        Self { service, invoke, _messages: PhantomData }
    }
}

pub struct BidiStreamingProstMethod<S, Req, Resp, Output, Invoke> {
    service: Arc<S>,
    invoke: Invoke,
    _messages: PhantomData<fn(Req) -> (Resp, Output)>,
}

impl<S, Req, Resp, Output, Invoke> BidiStreamingProstMethod<S, Req, Resp, Output, Invoke> {
    pub fn new(service: Arc<S>, invoke: Invoke) -> Self {
        Self { service, invoke, _messages: PhantomData }
    }
}

struct HttpMessageCodec<Req, Resp> {
    decode_json: fn(&[u8]) -> std::result::Result<Req, String>,
    decode_query: fn(serde_json::Value) -> std::result::Result<Req, String>,
    encode_json: fn(&Resp) -> std::result::Result<Vec<u8>, String>,
}

impl<S, Req, Resp, Invoke> UnaryProstMethod<S, Req, Resp, Invoke>
where
    Req: serde::de::DeserializeOwned,
    Resp: serde::Serialize,
{
    /// Enables the generated HTTP JSON/query adapter while leaving protobuf
    /// calls on the direct prost fast path.
    pub fn with_http_mapping(mut self) -> Self {
        self.http_codec = Some(HttpMessageCodec {
            decode_json: |bytes| serde_json::from_slice(bytes).map_err(|error| error.to_string()),
            decode_query: |value| serde_json::from_value(value).map_err(|error| error.to_string()),
            encode_json: |response| serde_json::to_vec(response).map_err(|error| error.to_string()),
        });
        self
    }
}

impl<S, Req, Resp, Invoke, Call> RegisteredMethod for UnaryProstMethod<S, Req, Resp, Invoke>
where
    S: Send + Sync + 'static,
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
    Invoke: Fn(Arc<S>, Request<Req>) -> Call + Send + Sync + 'static,
    Call: Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    fn start(&self, controller: ServerController, payload: IoBuf) -> ServerCall {
        let service = self.service.clone();
        let call = &self.invoke;
        let request = match Req::decode(payload) {
            Ok(request) => request,
            Err(error) => {
                return ServerCall::failed(Status::new(
                    StatusCode::Internal,
                    format!("invalid protobuf request: {error}"),
                ));
            }
        };
        // Construct the concrete future before erasing it into ServerCall.
        let future = call(service, controller.into_request(request));
        ServerCall::new(async move {
            match future.await {
                Ok(response) => encode_rpc_response(response),
                Err(status) => ServerResponse::failed(status),
            }
        })
    }

    fn start_http(&self, controller: ServerController, request: HttpRpcRequest) -> ServerCall {
        let response_encoding = request.response_encoding;
        let decoded = match request.body {
            RpcBodyEncoding::Protobuf(payload) => {
                Req::decode(payload).map_err(|error| error.to_string())
            }
            RpcBodyEncoding::Json(payload) => self
                .http_codec
                .as_ref()
                .ok_or_else(|| "JSON mapping is not enabled for this method".to_owned())
                .and_then(|codec| {
                    let bytes = payload_to_vec(&payload);
                    (codec.decode_json)(&bytes)
                }),
            RpcBodyEncoding::Query(value) => self
                .http_codec
                .as_ref()
                .ok_or_else(|| "query mapping is not enabled for this method".to_owned())
                .and_then(|codec| (codec.decode_query)(value)),
        };
        let request = match decoded {
            Ok(request) => request,
            Err(error) => {
                return ServerCall::failed(Status::new(
                    StatusCode::Unknown,
                    format!("invalid HTTP RPC request: {error}"),
                ));
            }
        };
        let service = self.service.clone();
        let future = (self.invoke)(service, controller.into_request(request));
        let encode_json = self.http_codec.as_ref().map(|codec| codec.encode_json);
        ServerCall::new(async move {
            match future.await {
                Ok(response) => {
                    let (message, attachment, metadata, trailing_metadata, _) =
                        response.into_parts();
                    let mut response = match response_encoding {
                        RpcResponseEncoding::Protobuf => encode_response(message),
                        RpcResponseEncoding::Json => match encode_json {
                            Some(encode) => match encode(&message) {
                                Ok(bytes) => ServerResponse::success(vec_to_iobuf(&bytes)),
                                Err(error) => ServerResponse::failed(Status::new(
                                    StatusCode::Internal,
                                    format!("encode JSON response: {error}"),
                                )),
                            },
                            None => ServerResponse::failed(Status::new(
                                StatusCode::Internal,
                                "JSON mapping is not enabled for this method",
                            )),
                        },
                    };
                    response.attachment = attachment;
                    response.metadata = metadata;
                    response.trailing_metadata = trailing_metadata;
                    response
                }
                Err(status) => ServerResponse::failed(status),
            }
        })
    }
}

impl<S, Req, Resp, Output, Invoke, Call> RegisteredMethod
    for ServerStreamingProstMethod<S, Req, Resp, Output, Invoke>
where
    S: Send + Sync + 'static,
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
    Output: tokio_stream::Stream<Item = Result<Resp, Status>> + Send + 'static,
    Invoke: Fn(Arc<S>, Request<Req>) -> Call + Send + Sync + 'static,
    Call: Future<Output = Result<Response<Output>, Status>> + Send + 'static,
{
    fn start(&self, controller: ServerController, payload: IoBuf) -> ServerCall {
        let service = self.service.clone();
        let call = &self.invoke;
        let request = match Req::decode(payload) {
            Ok(request) => request,
            Err(error) => {
                return ServerCall::failed(Status::new(
                    StatusCode::Internal,
                    format!("invalid protobuf request: {error}"),
                ));
            }
        };
        let future = call(service, controller.into_request(request));
        ServerCall::new(async move {
            match future.await {
                Ok(response) => encode_rpc_stream(response),
                Err(status) => ServerResponse::failed(status),
            }
        })
    }

    fn start_http(&self, _controller: ServerController, _request: HttpRpcRequest) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "server-streaming RPC is not available through HTTP mapping",
        ))
    }
}

impl<S, Req, Resp, Invoke, Call> RegisteredMethod
    for ClientStreamingProstMethod<S, Req, Resp, Invoke>
where
    S: Send + Sync + 'static,
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
    Invoke: Fn(Arc<S>, Request<Streaming<Req, Status>>) -> Call + Send + Sync + 'static,
    Call: Future<Output = Result<Response<Resp>, Status>> + Send + 'static,
{
    fn start(&self, _controller: ServerController, _payload: IoBuf) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "client-streaming RPC requires a streaming request",
        ))
    }

    fn start_stream(
        &self,
        controller: ServerController,
        request: crate::client::RawStreaming,
    ) -> ServerCall {
        let service = self.service.clone();
        let call = &self.invoke;
        let stream = Streaming::from_raw(request, |status| status, decode_error_status);
        let future = call(service, controller.into_request(stream));
        ServerCall::new(async move {
            match future.await {
                Ok(response) => encode_rpc_response(response),
                Err(status) => ServerResponse::failed(status),
            }
        })
    }

    fn start_http(&self, _controller: ServerController, _request: HttpRpcRequest) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "client-streaming RPC is not available through HTTP mapping",
        ))
    }
}

impl<S, Req, Resp, Output, Invoke, Call> RegisteredMethod
    for BidiStreamingProstMethod<S, Req, Resp, Output, Invoke>
where
    S: Send + Sync + 'static,
    Req: prost::Message + Default + Send + 'static,
    Resp: prost::Message + Send + 'static,
    Output: tokio_stream::Stream<Item = Result<Resp, Status>> + Send + 'static,
    Invoke: Fn(Arc<S>, Request<Streaming<Req, Status>>) -> Call + Send + Sync + 'static,
    Call: Future<Output = Result<Response<Output>, Status>> + Send + 'static,
{
    fn start(&self, _controller: ServerController, _payload: IoBuf) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "bidirectional-streaming RPC requires a streaming request",
        ))
    }

    fn start_stream(
        &self,
        controller: ServerController,
        request: crate::client::RawStreaming,
    ) -> ServerCall {
        let service = self.service.clone();
        let call = &self.invoke;
        let stream = Streaming::from_raw(request, |status| status, decode_error_status);
        let future = call(service, controller.into_request(stream));
        ServerCall::new(async move {
            match future.await {
                Ok(response) => encode_rpc_stream(response),
                Err(status) => ServerResponse::failed(status),
            }
        })
    }

    fn start_http(&self, _controller: ServerController, _request: HttpRpcRequest) -> ServerCall {
        ServerCall::failed(Status::new(
            StatusCode::Unimplemented,
            "bidirectional-streaming RPC is not available through HTTP mapping",
        ))
    }
}

fn encode_response(response: impl prost::Message) -> ServerResponse {
    let mut payload = IoBufBuilder::new();
    match response.encode(&mut payload) {
        Ok(()) => ServerResponse::success(payload.finish()),
        Err(error) => ServerResponse::failed(Status::new(
            StatusCode::Internal,
            format!("encode protobuf response: {error}"),
        )),
    }
}

fn encode_rpc_response(response: Response<impl prost::Message>) -> ServerResponse {
    let (message, attachment, metadata, trailing_metadata, _) = response.into_parts();
    let mut encoded = encode_response(message);
    encoded.attachment = attachment;
    encoded.metadata = metadata;
    encoded.trailing_metadata = trailing_metadata;
    encoded
}

fn encode_rpc_stream<M, S>(response: Response<S>) -> ServerResponse
where
    M: prost::Message + Send + 'static,
    S: tokio_stream::Stream<Item = Result<M, Status>> + Send + 'static,
{
    let (stream, attachment, metadata, trailing_metadata, _) = response.into_parts();
    let encoded_stream = stream.map(|item| {
        let message = item?;
        let mut payload = IoBufBuilder::new();
        message.encode(&mut payload).map_err(|error| {
            Status::new(StatusCode::Internal, format!("encode protobuf response: {error}"))
        })?;
        Ok(payload.finish())
    });
    let mut encoded = ServerResponse::incremental(Box::pin(encoded_stream));
    encoded.attachment = attachment;
    encoded.metadata = metadata;
    encoded.trailing_metadata = trailing_metadata;
    encoded
}

fn payload_to_vec(payload: &IoBuf) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len());
    for chunk in payload.chunks() {
        bytes.extend_from_slice(chunk);
    }
    bytes
}

fn vec_to_iobuf(bytes: &[u8]) -> IoBuf {
    let mut builder = IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}

fn decode_error_status(error: crate::Error) -> Status {
    Status::new(StatusCode::Internal, format!("invalid protobuf request: {error}"))
}
