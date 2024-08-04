//! Utility functions for branch-chain-aggregator

use crate::error::Error;

use ckb_hash::blake2b_256;
use ckb_logger::info;
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    h256,
    packed::Script,
    prelude::{Builder, Entity, Pack},
    H160, H256,
};
use secp256k1;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

const SIGHASH_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");

pub(crate) fn get_sighash_script_from_privkey(key_path: PathBuf) -> Result<Script, Error> {
    let pk = parse_file_to_h256(key_path).map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    let secret_key =
        secp256k1::SecretKey::from_slice(&pk).expect("impossible: fail to build secret key");
    let secp256k1: secp256k1::Secp256k1<secp256k1::All> = secp256k1::Secp256k1::new();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp256k1, &secret_key);
    let pubkey_compressed = &pubkey.serialize()[..];
    let pubkey_hash = blake2b_256(pubkey_compressed);
    let pubkey_hash = &pubkey_hash[0..20];
    let args = Bytes::from(pubkey_hash.to_vec());
    let script = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(args.pack())
        .build();
    Ok(script)
}

fn parse_file_to_h256(path: PathBuf) -> Result<Vec<u8>, Error> {
    let mut file = File::open(&path).map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    let mut data = vec![];
    file.read_to_end(&mut data)
        .map_err(|e| Error::BinaryFileReadError(e.to_string()))?;
    Ok(data)
}
