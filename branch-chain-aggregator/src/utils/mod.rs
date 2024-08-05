mod key;

pub(crate) use key::*;

pub const SECP256K1: &str = "secp256k1_blake160";
pub const XUDT: &str = "xudt";
pub const REQUEST_LOCK: &str = "request_lock";
pub const QUEUE_TYPE: &str = "queue_type";

pub fn decode_udt_amount(data: &[u8]) -> Option<u128> {
    if data.len() < 16 {
        return None;
    }
    Some(u128::from_le_bytes(to_fixed_array(&data[0..16])))
}

pub fn encode_udt_amount(amount: u128) -> Vec<u8> {
    amount.to_le_bytes().to_vec()
}

pub fn to_fixed_array<const LEN: usize>(input: &[u8]) -> [u8; LEN] {
    assert_eq!(input.len(), LEN);
    let mut list = [0; LEN];
    list.copy_from_slice(input);
    list
}
