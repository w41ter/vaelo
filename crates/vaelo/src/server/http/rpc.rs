// SPDX-License-Identifier: Apache-2.0

use http::{HeaderValue, Method, StatusCode as HttpStatusCode};
use serde_json::{Map, Value};

use super::{HttpRequest, HttpResponse, HttpRpcMapping, percent_decode};
use crate::http::{Body, BodyExt};
use crate::{IoBuf, ServiceRegistry, StatusCode};

pub struct HttpRpcRequest {
    pub body: RpcBodyEncoding,
    pub response_encoding: RpcResponseEncoding,
}

pub enum RpcBodyEncoding {
    Protobuf(IoBuf),
    Json(IoBuf),
    Query(Value),
}

#[derive(Clone, Copy)]
pub enum RpcResponseEncoding {
    Protobuf,
    Json,
}

pub(super) async fn dispatch(
    services: ServiceRegistry,
    mapping: HttpRpcMapping,
    request: HttpRequest,
) -> HttpResponse {
    let Some((service, method)) = rpc_path(&request.path) else {
        return HttpResponse::empty(HttpStatusCode::NOT_FOUND);
    };
    let service = service.to_owned();
    let method = method.to_owned();
    let remote_addr = request.remote_addr;
    let response_encoding = negotiate_response(&request);
    let body = match request_encoding(request, &mapping).await {
        Ok(body) => body,
        Err(status) => return HttpResponse::empty(status),
    };
    let Some(call) = services.start_http(
        remote_addr,
        &service,
        &method,
        HttpRpcRequest { body, response_encoding },
    ) else {
        return HttpResponse::empty(HttpStatusCode::NOT_FOUND);
    };
    let response = call.finish().await;
    let Some(status) = response.status else {
        let mut output = HttpResponse::new(HttpStatusCode::OK, response.body.into_unary());
        output.headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static(match response_encoding {
                RpcResponseEncoding::Protobuf => "application/protobuf",
                RpcResponseEncoding::Json => "application/json",
            }),
        );
        return output;
    };
    let code = match status.code() {
        StatusCode::Canceled => HttpStatusCode::REQUEST_TIMEOUT,
        StatusCode::DeadlineExceeded => HttpStatusCode::GATEWAY_TIMEOUT,
        StatusCode::Unavailable => HttpStatusCode::SERVICE_UNAVAILABLE,
        StatusCode::ResourceExhausted => HttpStatusCode::TOO_MANY_REQUESTS,
        StatusCode::InvalidArgument | StatusCode::OutOfRange => HttpStatusCode::BAD_REQUEST,
        StatusCode::NotFound => HttpStatusCode::NOT_FOUND,
        StatusCode::AlreadyExists | StatusCode::Aborted => HttpStatusCode::CONFLICT,
        StatusCode::PermissionDenied => HttpStatusCode::FORBIDDEN,
        StatusCode::Unauthenticated => HttpStatusCode::UNAUTHORIZED,
        StatusCode::Unimplemented => HttpStatusCode::NOT_IMPLEMENTED,
        StatusCode::FailedPrecondition => HttpStatusCode::PRECONDITION_FAILED,
        StatusCode::Unknown | StatusCode::Internal | StatusCode::DataLoss => {
            HttpStatusCode::INTERNAL_SERVER_ERROR
        }
        StatusCode::Ok => HttpStatusCode::OK,
    };
    let mut output = HttpResponse::new(code, text_body(status.message()));
    output
        .headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    output
}

fn rpc_path(path: &str) -> Option<(&str, &str)> {
    let mut segments = path.trim_matches('/').split('/');
    let service = segments.next()?;
    let method = segments.next()?;
    (segments.next().is_none() && !service.is_empty() && !method.is_empty())
        .then_some((service, method))
}

async fn request_encoding(
    request: HttpRequest,
    mapping: &HttpRpcMapping,
) -> Result<RpcBodyEncoding, HttpStatusCode> {
    let content_type = request
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if content_type.eq_ignore_ascii_case("application/protobuf")
        || content_type.eq_ignore_ascii_case("application/x-protobuf")
    {
        return mapping
            .allow_protobuf
            .then_some(request.body)
            .ok_or(HttpStatusCode::UNSUPPORTED_MEDIA_TYPE)?
            .collect()
            .await
            .map(RpcBodyEncoding::Protobuf)
            .map_err(|_| HttpStatusCode::BAD_REQUEST);
    }
    if content_type.eq_ignore_ascii_case("application/json") {
        return mapping
            .allow_json
            .then_some(request.body)
            .ok_or(HttpStatusCode::UNSUPPORTED_MEDIA_TYPE)?
            .collect()
            .await
            .map(RpcBodyEncoding::Json)
            .map_err(|_| HttpStatusCode::BAD_REQUEST);
    }
    if matches!(request.method, Method::GET | Method::DELETE)
        || request.body.size_hint().upper() == Some(0)
    {
        if !mapping.allow_query {
            return Err(HttpStatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        return query_value(request.query.as_deref().unwrap_or("")).map(RpcBodyEncoding::Query);
    }
    Err(HttpStatusCode::UNSUPPORTED_MEDIA_TYPE)
}

fn negotiate_response(request: &HttpRequest) -> RpcResponseEncoding {
    let accept = request
        .headers
        .get(http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if accept.split(',').any(|value| value.trim().starts_with("application/json")) {
        RpcResponseEncoding::Json
    } else {
        RpcResponseEncoding::Protobuf
    }
}

fn query_value(query: &str) -> Result<Value, HttpStatusCode> {
    let mut root = Map::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key);
        if key.is_empty() {
            return Err(HttpStatusCode::BAD_REQUEST);
        }
        insert_path(&mut root, &key, scalar_value(&percent_decode(value)))?;
    }
    Ok(Value::Object(root))
}

fn insert_path(
    root: &mut Map<String, Value>,
    path: &str,
    value: Value,
) -> Result<(), HttpStatusCode> {
    let parts: Vec<_> = path.split('.').collect();
    let mut object = root;
    for part in &parts[..parts.len() - 1] {
        let entry = object.entry((*part).to_owned()).or_insert_with(|| Value::Object(Map::new()));
        let Value::Object(next) = entry else {
            return Err(HttpStatusCode::BAD_REQUEST);
        };
        object = next;
    }
    let key = parts.last().unwrap().to_string();
    match object.entry(key) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(value);
        }
        serde_json::map::Entry::Occupied(mut entry) => match entry.get_mut() {
            Value::Array(values) => values.push(value),
            previous => {
                let old = std::mem::replace(previous, Value::Null);
                *previous = Value::Array(vec![old, value]);
            }
        },
    }
    Ok(())
}

fn scalar_value(value: &str) -> Value {
    match value {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => value
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| value.parse::<f64>().map(Value::from))
            .unwrap_or_else(|_| Value::String(value.to_owned())),
    }
}

fn text_body(value: &str) -> IoBuf {
    use bytes::BufMut;
    let mut builder = crate::IoBufBuilder::new();
    builder.put_slice(value.as_bytes());
    builder.finish()
}
