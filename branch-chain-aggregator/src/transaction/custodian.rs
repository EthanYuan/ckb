use crate::schemas::leap::{
    CrossChainQueue, Message, MessageUnion, Request, RequestContent, Requests, Transfer,
};
use crate::Aggregator;
use crate::Error;
use crate::{
    encode_udt_amount, get_sighash_script_from_privkey, RequestType, QUEUE_TYPE, REQUEST_LOCK,
    SECP256K1, XUDT,
};

use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::traits::LiveCell;
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::{Cell, Order},
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        handler::HandlerContexts,
        input::{InputIterator, TransactionInput},
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup,
};
use ckb_types::{
    bytes::Bytes,
    packed::{Byte32, Bytes as PackedBytes, CellInput, CellOutput, Script, WitnessArgs},
    prelude::*,
    H256,
};
use molecule::prelude::Byte;

use std::collections::HashMap;

impl Aggregator {
    pub(crate) fn create_custodian_tx(
        &self,
        request_cells: Vec<(Cell, Transfer)>,
    ) -> Result<H256, Error> {
        // get queue cell
        let queue_cell_search_option = self.build_message_queue_cell_search_option()?;
        let queue_cell = self
            .rpc_client
            .get_cells(queue_cell_search_option.into(), Order::Asc, 1.into(), None)
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if queue_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Queue cell found: {}",
                queue_cell.objects.len()
            )));
        }
        info!("Found {} queue cell", queue_cell.objects.len());
        let queue_cell = queue_cell.objects[0].clone();

        // build new queue
        let mut request_ids = vec![];
        let mut requests = vec![];
        for (cell, transfer) in request_cells.clone() {
            let request_content = RequestContent::new_builder()
                .request_type(Byte::new(RequestType::CkbToBranch as u8))
                .target_chain_id(self.chain_id.pack())
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

        let queue_live_cell: LiveCell = queue_cell.clone().into();
        let queue_data = queue_live_cell.output_data;
        let queue = CrossChainQueue::from_slice(&queue_data)
            .map_err(|e| Error::QueueCellDataDecodeError(e.to_string()))?;
        let existing_outbox = queue.outbox().as_builder().extend(request_ids).build();
        let queue_data = queue.as_builder().outbox(existing_outbox).build();
        let queue_witness = Requests::new_builder().set(requests).build();

        // build custodian lock
        let (custodian_lock, _) =
            get_sighash_script_from_privkey(self.config.rgbpp_custodian_lock_key_path.clone())?;

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
        for (cell, transfer) in &request_cells {
            let output: CellOutput = cell.output.clone().into();
            let output = output.as_builder().lock(custodian_lock.clone()).build();
            outputs.push(output);

            let udt_amount: u128 = transfer.amount().unpack();
            outputs_data.push(Bytes::from(encode_udt_amount(udt_amount)).pack());
        }

        // cell deps
        let secp256k1_cell_dep = self.get_rgbpp_cell_dep(SECP256K1)?;
        let xudt_cell_dep = self.get_rgbpp_cell_dep(XUDT)?;
        let request_cell_dep = self.get_rgbpp_cell_dep(REQUEST_LOCK)?;
        let queue_type_cell_dep = self.get_rgbpp_cell_dep(QUEUE_TYPE)?;

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

        // balance transaction
        let network_info = NetworkInfo::new(NetworkType::Testnet, self.config.rgbpp_uri.clone());
        let fee_rate = self.fee_rate()?;
        let configuration = {
            let mut config =
                TransactionBuilderConfiguration::new_with_network(network_info.clone())
                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
            config.fee_rate = fee_rate;
            config
        };
        let (capacity_provider_script, capacity_provider_key) =
            get_sighash_script_from_privkey(self.config.rgbpp_ckb_provider_key_path.clone())?;
        let mut change_builder =
            DefaultChangeBuilder::new(&configuration, capacity_provider_script.clone(), Vec::new());
        change_builder.init(&mut tx_builder);
        {
            let queue_cell_input = TransactionInput {
                live_cell: queue_cell.clone().into(),
                since: 0,
            };
            let _ = change_builder.check_balance(queue_cell_input, &mut tx_builder);
            for (cell, _) in &request_cells {
                let request_input = TransactionInput {
                    live_cell: cell.to_owned().into(),
                    since: 0,
                };
                let _ = change_builder.check_balance(request_input, &mut tx_builder);
            }
        };
        let contexts = HandlerContexts::default();
        let iterator = InputIterator::new(vec![capacity_provider_script], &network_info);
        let mut tx_with_groups = {
            let mut check_result = None;
            for (mut input_index, input) in iterator.enumerate() {
                input_index += 1 + request_cells.len(); // queue cell + request cells
                let input = input.map_err(|err| {
                    let msg = format!("failed to find {input_index}-th live cell since {err}");
                    Error::Other(msg)
                })?;
                tx_builder.input(input.cell_input());
                tx_builder.witness(PackedBytes::default());

                let previous_output = input.previous_output();
                let lock_script = previous_output.lock();
                lock_groups
                    .entry(lock_script.calc_script_hash())
                    .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                    .input_indices
                    .push(input_index);

                if change_builder.check_balance(input, &mut tx_builder) {
                    let mut script_groups: Vec<ScriptGroup> = lock_groups
                        .into_values()
                        .chain(type_groups.into_values())
                        .collect();
                    for script_group in script_groups.iter_mut() {
                        for handler in configuration.get_script_handlers() {
                            for context in &contexts.contexts {
                                if handler
                                    .build_transaction(
                                        &mut tx_builder,
                                        script_group,
                                        context.as_ref(),
                                    )
                                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?
                                {
                                    break;
                                }
                            }
                        }
                    }
                    let tx_view = change_builder.finalize(tx_builder);

                    check_result = Some(TransactionWithScriptGroups::new(tx_view, script_groups));
                    break;
                }
            }
            check_result
        }
        .ok_or_else(|| {
            let msg = "live cells are not enough".to_string();
            Error::Other(msg)
        })?;

        // sign
        let (_, message_queue_key) =
            get_sighash_script_from_privkey(self.config.rgbpp_queue_lock_key_path.clone())?;
        TransactionSigner::new(&network_info)
            .sign_transaction(
                &mut tx_with_groups,
                &SignContexts::new_sighash(vec![message_queue_key, capacity_provider_key]),
            )
            .map_err(|e| Error::TransactionSignError(e.to_string()))?;

        // send tx
        let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
        debug!(
            "custodian tx: {}",
            serde_json::to_string_pretty(&tx_json).unwrap()
        );
        let tx_hash = self
            .rpc_client
            .send_transaction(tx_json.inner, None)
            .map_err(|e| Error::TransactionSendError(format!("send transaction error: {}", e)))?;
        info!("custodian tx send: {:?}", tx_hash.pack());

        Ok(tx_hash)
    }
}
