use crate::schemas::leap::{
    Message, MessageUnion, Request, RequestContent, RequestLockArgs, Requests, Transfer,
};
use crate::{wait_for_tx_confirmation, Aggregator, CONFIRMATION_THRESHOLD, SIGHASH_TYPE_HASH};

use aggregator_common::{
    error::Error,
    types::RequestType,
    utils::{
        decode_udt_amount, privkey::get_sighash_lock_args_from_privkey, REQUEST_LOCK, SECP256K1,
        TOKEN_MANAGER_TYPE, XUDT,
    },
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::{Cell, Order},
    traits::{CellQueryOptions, LiveCell},
    transaction::signer::{SignContexts, TransactionSigner},
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup, Since, SinceType,
};
use ckb_stop_handler::{new_tokio_exit_rx, CancellationToken};
use ckb_types::h256;
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, CellInput, CellOutput, Script, WitnessArgs},
    prelude::*,
    H256,
};
use molecule::prelude::Byte;

use std::collections::HashMap;
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
                debug!("No more request cells found");
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

            let burn_tx = self.create_burn_tx(cells_with_messge)?;
            match wait_for_tx_confirmation(
                self.branch_rpc_client.clone(),
                burn_tx,
                Duration::from_secs(15),
            ) {
                Ok(_) => info!("Transaction confirmed"),
                Err(e) => info!("{}", e.to_string()),
            }
        }

        Ok(())
    }

    pub fn create_burn_tx(&self, request_cells: Vec<(Cell, Transfer)>) -> Result<H256, Error> {
        // get queue cell
        let (queue_cell, queue_cell_data) = self.get_branch_queue_outbox_cell()?;

        // build new queue
        let mut request_ids = vec![];
        let mut requests = vec![];
        for (cell, transfer) in request_cells.clone() {
            let request_content = RequestContent::new_builder()
                .request_type(Byte::new(RequestType::BranchToCkb as u8))
                .message(
                    Message::new_builder()
                        .set(MessageUnion::Transfer(transfer))
                        .build(),
                )
                .build();
            let request = Request::new_builder()
                .request_cell(cell.out_point.into())
                .request_content(request_content)
                .build();
            let request_id = request.as_bytes().pack().calc_raw_data_hash();
            request_ids.push(request_id);
            requests.push(request);
        }

        if !queue_cell_data.outbox().is_empty() {
            return Err(Error::QueueOutboxHasUnprocessedRequests);
        }
        let existing_outbox = queue_cell_data
            .outbox()
            .as_builder()
            .extend(request_ids)
            .build();
        let queue_data = queue_cell_data.as_builder().outbox(existing_outbox).build();
        let queue_witness = Requests::new_builder().set(requests).build();

        // build capacity provider lock
        let (capacity_provider_lock_args, _) = get_sighash_lock_args_from_privkey(
            self.config.branch_chain_capacity_provider_key_path.clone(),
        )?;
        let capacity_provider_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(capacity_provider_lock_args.pack())
            .build();

        // build inputs
        let inputs: Vec<CellInput> = std::iter::once(queue_cell.out_point.clone())
            .chain(request_cells.iter().map(|(cell, _)| cell.out_point.clone()))
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let mut outputs = vec![queue_cell.output.clone().into()];
        let mut outputs_data = vec![queue_data.as_bytes().pack()];
        for (cell, _) in &request_cells {
            if cell.output.type_.is_some() {
                let output: CellOutput = cell.output.clone().into();
                let output = output
                    .as_builder()
                    .lock(capacity_provider_lock.clone())
                    .type_(None::<Script>.pack())
                    .build();
                outputs.push(output);

                outputs_data.push(PackedBytes::default());
            }
        }

        // cell deps
        let secp256k1_cell_dep = self.get_branch_cell_dep(SECP256K1)?;
        let xudt_cell_dep = self.get_branch_cell_dep(XUDT)?;
        let request_cell_dep = self.get_branch_cell_dep(REQUEST_LOCK)?;
        let queue_type_cell_dep = self.get_branch_cell_dep(TOKEN_MANAGER_TYPE)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![
                secp256k1_cell_dep,
                xudt_cell_dep,
                request_cell_dep,
                queue_type_cell_dep,
            ])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(
                WitnessArgs::new_builder()
                    .input_type(Some(queue_witness.as_bytes()).pack())
                    .build()
                    .as_bytes()
                    .pack(),
            )
            .witnesses(vec![PackedBytes::default(); request_cells.len()]);

        // group
        #[allow(clippy::mutable_key_type)]
        let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut type_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        {
            let lock_script: Script = queue_cell.output.lock.clone().into();
            lock_groups
                .entry(lock_script.calc_script_hash())
                .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                .input_indices
                .push(0);
            for (index, cell) in request_cells.iter().enumerate() {
                let lock_script: Script = cell.0.output.lock.clone().into();
                lock_groups
                    .entry(lock_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                    .input_indices
                    .push(index + 1);
            }
        }
        for (output_idx, output) in tx_builder.get_outputs().clone().iter().enumerate() {
            if let Some(type_script) = &output.type_().to_opt() {
                type_groups
                    .entry(type_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_type_script(type_script))
                    .output_indices
                    .push(output_idx);
            }
        }

        // build transaction
        let network_info = NetworkInfo::new(NetworkType::Testnet, self.config.branch_uri.clone());
        let script_groups: Vec<ScriptGroup> = lock_groups
            .into_values()
            .chain(type_groups.into_values())
            .collect();
        let tx_view = tx_builder.build();
        let mut tx_with_groups = TransactionWithScriptGroups::new(tx_view, script_groups);

        // sign
        let (_, message_queue_key) = get_sighash_lock_args_from_privkey(
            self.config
                .branch_chain_token_manager_outbox_lock_key_path
                .clone(),
        )?;
        TransactionSigner::new(&network_info)
            .sign_transaction(
                &mut tx_with_groups,
                &SignContexts::new_sighash(vec![message_queue_key]),
            )
            .map_err(|e| Error::TransactionSignError(e.to_string()))?;

        // send tx
        let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
        info!(
            "burn tx: {}",
            serde_json::to_string_pretty(&tx_json).unwrap()
        );
        let tx_hash = self
            .branch_rpc_client
            .send_transaction(tx_json.inner, None)
            .map_err(|e| Error::TransactionSendError(format!("send transaction error: {}", e)))?;
        info!("burn tx send: {:?}", tx_hash.pack());

        Ok(tx_hash)
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
