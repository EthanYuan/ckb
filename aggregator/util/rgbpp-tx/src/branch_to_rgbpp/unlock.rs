use crate::schemas::leap::{
    Message, MessageUnion, Request, RequestContent, RequestLockArgs, Requests, Transfer,
};
use crate::{RgbppTxBuilder, CONFIRMATION_THRESHOLD, SIGHASH_TYPE_HASH};

use aggregator_common::{
    error::Error,
    types::RequestType,
    utils::{
        decode_udt_amount, encode_udt_amount, privkey::get_sighash_lock_args_from_privkey,
        QUEUE_TYPE, REQUEST_LOCK, SECP256K1, XUDT,
    },
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::{Cell, Order},
    rpc::CkbRpcClient as RpcClient,
    traits::{CellQueryOptions, LiveCell},
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        handler::HandlerContexts,
        input::{InputIterator, TransactionInput},
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup, Since, SinceType,
};
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, CellInput, CellOutput, Script, WitnessArgs},
    prelude::*,
    H256,
};
use molecule::prelude::Byte;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

impl RgbppTxBuilder {
    pub fn create_unlock_tx(&self) -> Result<H256, Error> {
        Ok(H256::default())
    }
}
