use crate::schemas::leap::{
    Message, MessageUnion, Request, RequestContent, RequestLockArgs, Requests, Transfer,
};
use crate::Aggregator;
use crate::{CONFIRMATION_THRESHOLD, SIGHASH_TYPE_HASH};

use aggregator_common::{
    error::Error,
    types::RequestType,
    utils::{
        decode_udt_amount, encode_udt_amount, privkey::get_sighash_lock_args_from_privkey,
        QUEUE_TYPE, REQUEST_LOCK, SECP256K1, TOKEN_MANAGER_TYPE,
    },
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::{Cell, Order},
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
use ckb_types::h256;
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, CellInput, Script, WitnessArgs},
    prelude::*,
    H256,
};
use molecule::prelude::Byte;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

impl Aggregator {
    pub fn collect_branch_requests(&self) -> Result<(), Error> {
        info!("Scan Branch requests ...");

        let stop: CancellationToken = new_tokio_exit_rx();

        let request_search_option = self.build_request_cell_search_option()?;
        let mut cursor = None;
        let limit = 10;

        loop {
            if stop.is_cancelled() {
                info!("Aggregator scan_rgbpp_request received exit signal, exiting now");
                return Ok(());
            }

            let request_cells = self
                .branch_rpc_client
                .get_cells(
                    request_search_option.clone().into(),
                    Order::Asc,
                    limit.into(),
                    cursor,
                )
                .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;

            if request_cells.objects.is_empty() {
                info!("No more request cells found");
                break;
            }
            cursor = Some(request_cells.last_cursor);

            info!("Found {} request cells", request_cells.objects.len());
            let tip = self
                .branch_rpc_client
                .get_tip_block_number()
                .map_err(|e| Error::RpcError(format!("get tip block number error: {}", e)))?
                .value();
            let cells_with_messge = self.check_request(request_cells.objects.clone(), tip);
            info!("Found {} valid request cells", cells_with_messge.len());
            if cells_with_messge.is_empty() {
                break;
            }
        }

        Ok(())
    }

    fn build_request_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let request_script = self._get_branch_script(REQUEST_LOCK)?;
        Ok(CellQueryOptions::new_lock(request_script))
    }

    fn check_request(&self, cells: Vec<Cell>, tip: u64) -> Vec<(Cell, Transfer)> {
        cells
            .into_iter()
            .filter_map(|cell| {
                let live_cell: LiveCell = cell.clone().into();
                RequestLockArgs::from_slice(&live_cell.output.lock().args().raw_data())
                    .ok()
                    .and_then(|args| {
                        let target_request_type_hash = args.request_type_hash();
                        info!("target_request_type_hash: {:?}", target_request_type_hash);

                        let timeout: u64 = args.timeout().unpack();
                        let since = Since::from_raw_value(timeout);
                        let since_check =
                            since.extract_metric().map_or(false, |(since_type, value)| {
                                match since_type {
                                    SinceType::BlockNumber => {
                                        let threshold = if since.is_absolute() {
                                            value
                                        } else {
                                            cell.block_number.value() + value
                                        };
                                        tip + CONFIRMATION_THRESHOLD < threshold
                                    }
                                    _ => false,
                                }
                            });

                        let content = args.content();
                        let target_chain_id: Bytes = content.target_chain_id().raw_data();
                        info!("target_chain_id: {:?}", target_chain_id);
                        let request_type = content.request_type();

                        let (check_message, transfer) = {
                            let message = content.message();
                            let message_union = message.to_enum();
                            match message_union {
                                MessageUnion::Transfer(transfer) => {
                                    let transfer_amount: u128 = transfer.amount().unpack();

                                    let check_asset = if cell.output.type_.is_some() {
                                        let check_amount = cell
                                            .clone()
                                            .output_data
                                            .and_then(|data| decode_udt_amount(data.as_bytes()))
                                            .map_or(false, |amount| {
                                                info!(
                                                    "original amount: {:?}, transfer amount: {:?}",
                                                    amount, transfer_amount
                                                );
                                                transfer_amount <= amount
                                            });

                                        let check_cell_type = {
                                            let type_args = cell.output.type_.clone().unwrap().args;
                                            let (token_manager_lock_args, _) =
                                                get_sighash_lock_args_from_privkey(
                                                    self.config
                                                        .branch_chain_token_manager_lock_key_path
                                                        .clone(),
                                                )
                                                .expect("get message queue outbox lock args");
                                            let token_manager_lock_hash = Script::new_builder()
                                                .code_hash(SIGHASH_TYPE_HASH.pack())
                                                .hash_type(ScriptHashType::Type.into())
                                                .args(token_manager_lock_args.pack())
                                                .build()
                                                .calc_script_hash();
                                            type_args.into_bytes()
                                                == token_manager_lock_hash.as_bytes()
                                        };

                                        let check_transfer_type = {
                                            let transfer_type_hash: H256 =
                                                transfer.asset_type().unpack();
                                            transfer_type_hash == h256!("0x37b6748d268d4aa62445d546bac1f90ccbc02cbbcecc7831aca3b77d70304e0f")
                                        };

                                        check_amount && check_cell_type && check_transfer_type
                                    } else {
                                        let check_amount = {
                                            let capacity: u64 = cell.output.capacity.into();
                                            transfer_amount <= capacity as u128
                                        };

                                        let check_transfer_type = {
                                            let transfer_type_hash: H256 =
                                                transfer.asset_type().unpack();
                                            transfer_type_hash == h256!("0x29b0b1a449b0e7fb08881e1d810a6abbedb119e9c4ffc76eebbc757fb214f091")
                                        };

                                        check_amount && check_transfer_type
                                    };

                                    let lock_hash: H256 = transfer.owner_lock_hash().unpack();
                                    let check_lock = self.asset_locks.contains_key(&lock_hash);

                                    (check_asset && check_lock, transfer)
                                }
                            }
                        };

                        let request_type_hash = self
                            .branch_scripts
                            .get(TOKEN_MANAGER_TYPE)
                            .map(|script_info| script_info.script.calc_script_hash());

                        if Some(target_request_type_hash) == request_type_hash
                            && request_type == Byte::new(RequestType::BranchToCkb as u8)
                            && check_message
                            && since_check
                        {
                            Some((cell, transfer))
                        } else {
                            None
                        }
                    })
            })
            .collect()
    }
}
