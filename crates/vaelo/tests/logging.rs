// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;
use std::time::Duration;

use log::{Level, LevelFilter, Log, Metadata, Record};
use vaelo::__private::{AcceptedTcpSocket, Acceptor, Server};
use vaelo::{Error, Result};

struct TestLogger(Mutex<Vec<(Level, String, String)>>);

impl Log for TestLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("vaelo::")
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push((
                record.level(),
                record.target().to_owned(),
                record.args().to_string(),
            ));
        }
    }

    fn flush(&self) {}
}

static LOGGER: TestLogger = TestLogger(Mutex::new(Vec::new()));

struct RejectAcceptor {
    exhausted: bool,
}

impl Acceptor for RejectAcceptor {
    fn accept(&self, _socket: AcceptedTcpSocket) -> Result<()> {
        if self.exhausted {
            Err(Error::ResourceExhausted)
        } else {
            Err(Error::Channel("test admission rejection".into()))
        }
    }
}

// A separate integration-test process isolates the global facade logger from
// library tests and verifies diagnostics from an actual background accept task.
#[tokio::test]
async fn background_errors_and_listener_transitions_reach_application_logger() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(LevelFilter::Debug);
    let mut server = Server::new();
    let listener =
        server.listen_tcp("127.0.0.1:0", RejectAcceptor { exhausted: false }).await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr()).await.unwrap();
    let peer = client.local_addr().unwrap().to_string();
    let exhausted_listener =
        server.listen_tcp("127.0.0.1:0", RejectAcceptor { exhausted: true }).await.unwrap();
    let exhausted_client =
        tokio::net::TcpStream::connect(exhausted_listener.local_addr()).await.unwrap();
    let exhausted_peer = exhausted_client.local_addr().unwrap().to_string();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let found = LOGGER.0.lock().unwrap().iter().any(|(level, target, message)| {
                *level == Level::Warn
                    && target == "vaelo::server::acceptor"
                    && message.contains("test admission rejection")
                    && message.contains(&peer)
            });
            let system_failure = LOGGER.0.lock().unwrap().iter().any(|(level, target, message)| {
                *level == Level::Error
                    && target == "vaelo::server::acceptor"
                    && message.contains("call resources exhausted")
                    && message.contains(&exhausted_peer)
            });
            if found && system_failure {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    listener.shutdown();
    let records = LOGGER.0.lock().unwrap();
    for event in ["TCP listener started", "TCP socket accepted", "TCP listener shutdown requested"]
    {
        assert!(
            records
                .iter()
                .any(|(level, _, message)| { *level == Level::Debug && message.contains(event) }),
            "missing lifecycle log: {event}"
        );
    }
}
