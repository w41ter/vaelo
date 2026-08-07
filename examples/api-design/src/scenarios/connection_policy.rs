use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::time::Duration;

pub fn build(address: SocketAddr) -> vaelo::Result<(vaelo::RpcChannel, vaelo::RpcChannel)> {
    let ordinary = vaelo::RpcChannel::brpc(address)
        .connection_mode(vaelo::ConnectionMode::Pool { size: 4 })
        .connect_timeout(Duration::from_millis(100))
        .observation(vaelo::ObservationPolicy::SampledLatency {
            one_in: NonZeroU64::new(1024).expect("non-zero"),
        })
        .build()?;

    let bulk = vaelo::RpcChannel::brpc(address)
        .connection_mode(vaelo::ConnectionMode::Single)
        .connection_tag(vaelo::ConnectionTag::new(2))
        .build()?;

    Ok((ordinary, bulk))
}
