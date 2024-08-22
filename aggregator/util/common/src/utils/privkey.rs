//! Utility functions for aggregator

use crate::error::Error;

use ckb_hash::blake2b_256;
use ckb_types::{bytes::Bytes, h256, H256};

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

pub const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");

pub fn get_sighash_lock_args_from_privkey(
    key_path: PathBuf,
) -> Result<(Bytes, secp256k1::SecretKey), Error> {
    let pk = parse_file_to_h256(key_path).map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    let secret_key =
        secp256k1::SecretKey::from_slice(&pk).expect("impossible: fail to build secret key");
    let secp256k1: secp256k1::Secp256k1<secp256k1::All> = secp256k1::Secp256k1::new();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp256k1, &secret_key);
    let pubkey_compressed = &pubkey.serialize()[..];
    let pubkey_hash = blake2b_256(pubkey_compressed);
    let pubkey_hash = &pubkey_hash[0..20];
    let args = Bytes::from(pubkey_hash.to_vec());
    Ok((args, secret_key))
}

pub fn parse_file_to_h256(path: PathBuf) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path).map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    let mut data = vec![];
    file.read_to_end(&mut data)
        .map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    Ok(data)
}
