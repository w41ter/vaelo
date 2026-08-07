// SPDX-License-Identifier: Apache-2.0

//! Prost service generation for vaelo.

use std::fmt::Write;

use prost_build::{Service, ServiceGenerator};

/// Configure prost to generate vaelo clients, service traits, and
/// registrations.
pub fn configure() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.service_generator(Box::new(SrpcServiceGenerator));
    config
}

/// Configures vaelo generation and derives Serde for every generated message.
///
/// This is intended for APIs which explicitly expose protobuf messages through
/// JSON or query mappings. RPC-only schemas should use [`configure`] so well
/// known and externally mapped protobuf types do not need Serde
/// implementations.
pub fn configure_with_serde() -> prost_build::Config {
    let mut config = configure();
    config.type_attribute(
        ".",
        "#[derive(::vaelo::__private::Serialize, ::vaelo::__private::Deserialize)] #[serde(crate = \"::vaelo::__private::serde\")]",
    );
    config
}

struct SrpcServiceGenerator;

impl ServiceGenerator for SrpcServiceGenerator {
    fn generate(&mut self, service: Service, output: &mut String) {
        generate_descriptors(&service, output);
        generate_client(&service, output);
        generate_server(&service, output);
    }
}

fn generate_descriptors(service: &Service, output: &mut String) {
    let module = format!("{}_methods", snake(&service.name));
    writeln!(output, "#[doc(hidden)]\nmod {module} {{").unwrap();
    for method in &service.methods {
        let constant = screaming(&method.name);
        let kind = match (method.client_streaming, method.server_streaming) {
            (false, false) => "Unary",
            (false, true) => "ServerStreaming",
            (true, false) => "ClientStreaming",
            (true, true) => "BidirectionalStreaming",
        };
        let full_service = if service.package.is_empty() {
            service.proto_name.clone()
        } else {
            format!("{}.{}", service.package, service.proto_name)
        };
        writeln!(
            output,
            "pub static {constant}: ::vaelo::__private::MethodDescriptor = ::vaelo::__private::MethodDescriptor::new({full_service:?}, {:?}, ::vaelo::__private::MethodKind::{kind}, ::vaelo::__private::Idempotency::Unknown);",
            method.proto_name,
        )
        .unwrap();
    }
    writeln!(output, "}}").unwrap();
}

fn generate_client(service: &Service, output: &mut String) {
    let module = format!("{}_client", snake(&service.name));
    let client = format!("{}Client", service.name);
    let methods = format!("{}_methods", snake(&service.name));
    writeln!(
        output,
        "pub mod {module} {{\n    #[derive(Clone, Debug)]\n    pub struct {client} {{ channel: ::vaelo::RpcChannel }}\n    impl {client} {{\n        pub fn new(channel: ::vaelo::RpcChannel) -> Self {{ Self {{ channel }} }}\n        pub fn channel(&self) -> &::vaelo::RpcChannel {{ &self.channel }}"
    )
    .unwrap();
    for method in &service.methods {
        let constant = screaming(&method.name);
        match (method.client_streaming, method.server_streaming) {
            (false, false) => writeln!(
                output,
                "        pub fn {}(&self, request: impl ::vaelo::IntoRequest<super::{}>) -> ::vaelo::UnaryCall<super::{}, super::{}> {{ self.channel.call(&super::{methods}::{constant}, request.into_request()) }}",
                method.name, method.input_type, method.input_type, method.output_type
            ),
            (false, true) => writeln!(
                output,
                "        pub fn {}(&self, request: impl ::vaelo::IntoRequest<super::{}>) -> ::vaelo::ServerStreamingCall<super::{}, super::{}> {{ self.channel.server_streaming(&super::{methods}::{constant}, request.into_request()) }}",
                method.name, method.input_type, method.input_type, method.output_type
            ),
            (true, false) => writeln!(
                output,
                "        pub fn {}(&self) -> ::vaelo::ClientStreamingCall<super::{}, super::{}> {{ self.channel.client_streaming(&super::{methods}::{constant}) }}",
                method.name,
                method.input_type,
                method.output_type,
            ),
            (true, true) => writeln!(
                output,
                "        pub fn {}(&self) -> ::vaelo::BidiStreamingCall<super::{}, super::{}> {{ self.channel.bidirectional_streaming(&super::{methods}::{constant}) }}",
                method.name,
                method.input_type,
                method.output_type,
            ),
        }
        .unwrap();
    }
    writeln!(output, "    }}\n}}").unwrap();
}

fn generate_server(service: &Service, output: &mut String) {
    let module = format!("{}_server", snake(&service.name));
    let server = format!("{}Server", service.name);
    let methods = format!("{}_methods", snake(&service.name));
    writeln!(
        output,
        "pub mod {module} {{\n    pub trait {}: Send + Sync + 'static {{",
        service.name
    )
    .unwrap();
    for method in &service.methods {
        match (method.client_streaming, method.server_streaming) {
            (false, false) => writeln!(
                output,
                "        fn {}(&self, request: ::vaelo::Request<super::{}>) -> impl ::core::future::Future<Output = ::core::result::Result<::vaelo::Response<super::{}>, ::vaelo::Status>> + Send;",
                method.name, method.input_type, method.output_type
            ),
            (false, true) => {
                let stream = format!("{}Stream", method.proto_name);
                writeln!(
                    output,
                    "        type {stream}: ::vaelo::__private::Stream<Item = ::core::result::Result<super::{}, ::vaelo::Status>> + Send + 'static;\n        fn {}(&self, request: ::vaelo::Request<super::{}>) -> impl ::core::future::Future<Output = ::core::result::Result<::vaelo::Response<Self::{stream}>, ::vaelo::Status>> + Send;",
                    method.output_type, method.name, method.input_type
                )
            }
            (true, false) => writeln!(
                output,
                "        fn {}(&self, request: ::vaelo::Request<::vaelo::Streaming<super::{}, ::vaelo::Status>>) -> impl ::core::future::Future<Output = ::core::result::Result<::vaelo::Response<super::{}>, ::vaelo::Status>> + Send;",
                method.name, method.input_type, method.output_type
            ),
            (true, true) => {
                let stream = format!("{}Stream", method.proto_name);
                writeln!(
                    output,
                    "        type {stream}: ::vaelo::__private::Stream<Item = ::core::result::Result<super::{}, ::vaelo::Status>> + Send + 'static;\n        fn {}(&self, request: ::vaelo::Request<::vaelo::Streaming<super::{}, ::vaelo::Status>>) -> impl ::core::future::Future<Output = ::core::result::Result<::vaelo::Response<Self::{stream}>, ::vaelo::Status>> + Send;",
                    method.output_type, method.name, method.input_type
                )
            }
        }
        .unwrap();
    }
    writeln!(
        output,
        "    }}\n    pub struct {server}<T> {{ inner: ::std::sync::Arc<T> }}\n    impl<T> {server}<T> {{ pub fn new(inner: T) -> Self {{ Self {{ inner: ::std::sync::Arc::new(inner) }} }} }}\n    impl<T: {}> ::vaelo::__private::RegisterService for {server}<T> {{\n        fn register(self, registry: &mut ::vaelo::__private::ServiceRegistryBuilder) {{",
        service.name
    )
    .unwrap();
    for method in &service.methods {
        let constant = screaming(&method.name);
        let adapter = match (method.client_streaming, method.server_streaming) {
            (false, false) => format!(
                "::vaelo::__private::UnaryProstMethod::<T, super::{}, super::{}, _>",
                method.input_type, method.output_type
            ),
            (false, true) => format!(
                "::vaelo::__private::ServerStreamingProstMethod::<T, super::{}, super::{}, T::{}Stream, _>",
                method.input_type, method.output_type, method.proto_name
            ),
            (true, false) => format!(
                "::vaelo::__private::ClientStreamingProstMethod::<T, super::{}, super::{}, _>",
                method.input_type, method.output_type
            ),
            (true, true) => format!(
                "::vaelo::__private::BidiStreamingProstMethod::<T, super::{}, super::{}, T::{}Stream, _>",
                method.input_type, method.output_type, method.proto_name
            ),
        };
        writeln!(
            output,
            "            registry.register(&super::{methods}::{constant}, {adapter}::new(self.inner.clone(), |service: ::std::sync::Arc<T>, request| async move {{ service.{}(request).await }}));",
            method.name,
        )
        .unwrap();
    }
    writeln!(output, "        }}\n    }}\n}}").unwrap();
}

fn snake(name: &str) -> String {
    let mut output = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_uppercase() && index != 0 {
            output.push('_');
        }
        output.extend(character.to_lowercase());
    }
    output
}

fn screaming(name: &str) -> String {
    snake(name).to_uppercase()
}

#[cfg(test)]
mod tests {
    use prost_build::{Comments, Method, Service};

    use super::generate_client;

    #[test]
    fn generated_client_returns_unstarted_call_api() {
        let service = Service {
            name: "EchoService".into(),
            proto_name: "EchoService".into(),
            package: "demo".into(),
            comments: Comments::default(),
            methods: vec![Method {
                name: "echo".into(),
                proto_name: "Echo".into(),
                comments: Comments::default(),
                input_type: "EchoRequest".into(),
                output_type: "EchoResponse".into(),
                input_proto_type: ".demo.EchoRequest".into(),
                output_proto_type: ".demo.EchoResponse".into(),
                options: Default::default(),
                client_streaming: false,
                server_streaming: false,
            }],
            options: Default::default(),
        };
        let mut generated = String::new();

        generate_client(&service, &mut generated);

        assert!(generated.contains("channel: ::vaelo::RpcChannel"));
        assert!(
            generated.contains("-> ::vaelo::UnaryCall<super::EchoRequest, super::EchoResponse>")
        );
        assert!(generated.contains("self.channel.call"));
        assert!(!generated.contains("ServerController"));
    }

    #[test]
    fn generated_client_has_exactly_four_fixed_call_shapes() {
        fn method(
            name: &str,
            type_prefix: &str,
            client_streaming: bool,
            server_streaming: bool,
        ) -> Method {
            Method {
                name: name.into(),
                proto_name: type_prefix.into(),
                comments: Comments::default(),
                input_type: format!("{type_prefix}Request"),
                output_type: format!("{type_prefix}Response"),
                input_proto_type: format!(".demo.{type_prefix}Request"),
                output_proto_type: format!(".demo.{type_prefix}Response"),
                options: Default::default(),
                client_streaming,
                server_streaming,
            }
        }

        let service = Service {
            name: "ShapeService".into(),
            proto_name: "ShapeService".into(),
            package: "demo".into(),
            comments: Comments::default(),
            methods: vec![
                method("unary", "Unary", false, false),
                method("download", "Download", false, true),
                method("upload", "Upload", true, false),
                method("chat", "Chat", true, true),
            ],
            options: Default::default(),
        };
        let mut generated = String::new();

        generate_client(&service, &mut generated);

        assert!(
            generated.contains("-> ::vaelo::UnaryCall<super::UnaryRequest, super::UnaryResponse>")
        );
        assert!(generated.contains(
            "-> ::vaelo::ServerStreamingCall<super::DownloadRequest, super::DownloadResponse>"
        ));
        assert!(generated.contains(
            "pub fn upload(&self) -> ::vaelo::ClientStreamingCall<super::UploadRequest, super::UploadResponse>"
        ));
        assert!(generated.contains(
            "pub fn chat(&self) -> ::vaelo::BidiStreamingCall<super::ChatRequest, super::ChatResponse>"
        ));
        assert!(!generated.contains("StreamingSession"));
        assert!(!generated.contains("open_upload"));
        assert!(!generated.contains("open_chat"));
        assert!(!generated.contains("pub fn upload<S>"));
        assert!(!generated.contains("pub fn chat<S>"));
    }
}
