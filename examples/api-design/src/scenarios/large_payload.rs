use bytes::BufMut;
use vaelo::brpc::ResponseExt;

pub fn segmented_payload() -> vaelo::IoBuf {
    let mut first = vaelo::IoBufBuilder::new();
    first.put_slice(&vec![1_u8; 64 * 1024]);
    let mut payload = first.finish();

    let mut second = vaelo::IoBufBuilder::new();
    second.put_slice(&vec![2_u8; 64 * 1024]);
    let mut second = second.finish();
    payload.append_movable(&mut second);
    payload
}

pub fn take_attachment<T>(response: &mut vaelo::Response<T>) -> vaelo::IoBuf {
    response.take_attachment()
}

pub fn process_without_flattening(payload: &vaelo::IoBuf) -> usize {
    payload.chunks().map(<[u8]>::len).sum()
}
