use std::sync::Arc;

use crate::ShowcaseService;
use crate::generated::showcase_server::ShowcaseServer;

pub fn server(ready: Arc<std::sync::atomic::AtomicBool>) -> vaelo::ServerBuilder {
    let health = move || ready.load(std::sync::atomic::Ordering::Acquire);
    let builtins = vaelo::BuiltinServices::builder()
        .health(true)
        .version(true)
        .status(true)
        .vars(true)
        .application_version(env!("CARGO_PKG_VERSION"))
        .health_check(health)
        .build();

    vaelo::ServerBuilder::new().add_service(ShowcaseServer::new(ShowcaseService)).builtins(builtins)
}
