// SPDX-License-Identifier: Apache-2.0

//! Built-in HTTP endpoints for lightweight server introspection.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::BufMut;
use http::{HeaderValue, Method, StatusCode as HttpStatusCode};
use serde_json::json;

use super::{HttpRegistry, HttpRequest, HttpResponse, ServiceRegistry};
use crate::runtime::rpc_variables;
use crate::{IoBuf, IoBufBuilder, MethodDescriptor};

pub trait HealthCheck: Send + Sync + 'static {
    fn check(&self) -> bool;
}

impl<F> HealthCheck for F
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    fn check(&self) -> bool {
        self()
    }
}

struct AlwaysHealthy;

impl HealthCheck for AlwaysHealthy {
    fn check(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinExposure {
    LocalOnly,
    Any,
}

#[derive(Clone)]
pub struct BuiltinServices {
    health: bool,
    version: bool,
    status: bool,
    vars: bool,
    exposure: BuiltinExposure,
    application_version: Option<Arc<str>>,
    started_at: Instant,
    started_system_time: SystemTime,
    health_check: Arc<dyn HealthCheck>,
}

impl BuiltinServices {
    pub fn builder() -> BuiltinServicesBuilder {
        BuiltinServicesBuilder::default()
    }

    pub(crate) fn into_http_registry(self, services: ServiceRegistry) -> HttpRegistry {
        let state = Arc::new(BuiltinState { config: self, services });
        let mut builder = HttpRegistry::builder();
        if state.config.health {
            let state = state.clone();
            builder = builder.route(Method::GET, "/health", move |request| {
                let state = state.clone();
                async move { health(state, request) }
            });
        }
        if state.config.version {
            let state = state.clone();
            builder = builder.route(Method::GET, "/version", move |request| {
                let state = state.clone();
                async move { version(state, request) }
            });
        }
        if state.config.status {
            let state = state.clone();
            builder = builder.route(Method::GET, "/status", move |request| {
                let state = state.clone();
                async move { status(state, request) }
            });
        }
        if state.config.vars {
            let state = state.clone();
            builder = builder.route(Method::GET, "/vars", move |request| {
                let state = state.clone();
                async move { vars(state, request) }
            });
        }
        builder.build()
    }
}

impl Default for BuiltinServices {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            health: true,
            version: true,
            status: true,
            vars: true,
            exposure: BuiltinExposure::LocalOnly,
            application_version: None,
            started_at: Instant::now(),
            started_system_time: now,
            health_check: Arc::new(AlwaysHealthy),
        }
    }
}

#[derive(Default)]
pub struct BuiltinServicesBuilder {
    services: BuiltinServices,
}

impl BuiltinServicesBuilder {
    pub fn health(mut self, enabled: bool) -> Self {
        self.services.health = enabled;
        self
    }

    pub fn version(mut self, enabled: bool) -> Self {
        self.services.version = enabled;
        self
    }

    pub fn status(mut self, enabled: bool) -> Self {
        self.services.status = enabled;
        self
    }

    pub fn vars(mut self, enabled: bool) -> Self {
        self.services.vars = enabled;
        self
    }

    pub fn exposure(mut self, exposure: BuiltinExposure) -> Self {
        self.services.exposure = exposure;
        self
    }

    pub fn application_version(mut self, version: impl Into<Arc<str>>) -> Self {
        self.services.application_version = Some(version.into());
        self
    }

    pub fn health_check(mut self, check: impl HealthCheck) -> Self {
        self.services.health_check = Arc::new(check);
        self
    }

    pub fn build(self) -> BuiltinServices {
        self.services
    }
}

struct BuiltinState {
    config: BuiltinServices,
    services: ServiceRegistry,
}

fn health(state: Arc<BuiltinState>, request: HttpRequest) -> HttpResponse {
    if let Some(response) = forbidden(&state, &request) {
        return response;
    }
    if state.config.health_check.check() {
        text_response(HttpStatusCode::OK, "OK\n")
    } else {
        text_response(HttpStatusCode::SERVICE_UNAVAILABLE, "UNHEALTHY\n")
    }
}

fn version(state: Arc<BuiltinState>, request: HttpRequest) -> HttpResponse {
    if let Some(response) = forbidden(&state, &request) {
        return response;
    }
    json_response(json!({
        "framework": {
            "name": "vaelo",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "application_version": state.config.application_version.as_deref(),
        "started_unix_seconds": unix_seconds(state.config.started_system_time),
        "uptime_seconds": state.config.started_at.elapsed().as_secs(),
    }))
}

fn status(state: Arc<BuiltinState>, request: HttpRequest) -> HttpResponse {
    if let Some(response) = forbidden(&state, &request) {
        return response;
    }
    let methods = state.services.method_descriptors();
    let services = service_count(&methods);
    json_response(json!({
        "uptime_seconds": state.config.started_at.elapsed().as_secs(),
        "service_count": services,
        "method_count": methods.len(),
        "variables": rpc_variables::snapshot(),
        "methods": methods.iter().map(|method| {
            json!({
                "service": method.service,
                "method": method.method,
                "kind": format!("{:?}", method.kind),
                "idempotency": format!("{:?}", method.idempotency),
            })
        }).collect::<Vec<_>>(),
    }))
}

fn vars(state: Arc<BuiltinState>, request: HttpRequest) -> HttpResponse {
    if let Some(response) = forbidden(&state, &request) {
        return response;
    }
    json_response(json!(rpc_variables::snapshot()))
}

fn forbidden(state: &BuiltinState, request: &HttpRequest) -> Option<HttpResponse> {
    match state.config.exposure {
        BuiltinExposure::Any => None,
        BuiltinExposure::LocalOnly if request.remote_addr.ip().is_loopback() => None,
        BuiltinExposure::LocalOnly => Some(text_response(HttpStatusCode::FORBIDDEN, "forbidden\n")),
    }
}

fn service_count(methods: &[&'static MethodDescriptor]) -> usize {
    let mut count = 0;
    let mut previous = None;
    for method in methods {
        if previous != Some(method.service) {
            count += 1;
            previous = Some(method.service);
        }
    }
    count
}

fn unix_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).unwrap_or_default()
}

fn json_response(value: serde_json::Value) -> HttpResponse {
    match serde_json::to_vec(&value) {
        Ok(bytes) => {
            let mut response = HttpResponse::new(HttpStatusCode::OK, iobuf_from_bytes(&bytes));
            response
                .headers
                .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            response
        }
        Err(error) => text_response(
            HttpStatusCode::INTERNAL_SERVER_ERROR,
            &format!("encode builtin JSON response: {error}\n"),
        ),
    }
}

fn text_response(status: HttpStatusCode, text: &str) -> HttpResponse {
    let mut response = HttpResponse::new(status, iobuf_from_bytes(text.as_bytes()));
    response
        .headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    response
}

fn iobuf_from_bytes(bytes: &[u8]) -> IoBuf {
    let mut builder = IoBufBuilder::new();
    builder.put_slice(bytes);
    builder.finish()
}
