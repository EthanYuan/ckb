use crate::schemas::leap::{MessageUnion, Requests};
use crate::{RgbppTxBuilder, SIGHASH_TYPE_HASH};

use aggregator_common::{
    error::Error,
    utils::{
        decode_udt_amount, encode_udt_amount, privkey::get_sighash_lock_args_from_privkey,
        QUEUE_TYPE, SECP256K1, XUDT,
    },
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::{debug, info};
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::ckb_indexer::Order,
    traits::{CellQueryOptions, LiveCell},
    transaction::{
        builder::{ChangeBuilder, DefaultChangeBuilder},
        input::{InputIterator, TransactionInput},
        signer::{SignContexts, TransactionSigner},
        TransactionBuilderConfiguration,
    },
    types::{NetworkInfo, NetworkType, TransactionWithScriptGroups},
    ScriptGroup,
};
use ckb_types::{
    bytes::Bytes,
    core::ScriptHashType,
    packed::{Byte32, Bytes as PackedBytes, CellInput, CellOutput, Script, WitnessArgs},
    prelude::*,
    H256,
};

use std::collections::HashMap;

impl RgbppTxBuilder {
    pub fn create_unlock_tx(&self) -> Result<H256, Error> {
        // get queue cell
        let (queue_cell, queue_cell_data) =
            self.get_rgbpp_queue_cell(self.rgbpp_queue_inbox_lock_key_path.clone())?;
        info!(
            "The queue inbox contains {} requests that need to be unlock.",
            queue_cell_data.inbox().len()
        );
        if queue_cell_data.inbox().is_empty() {
            return Ok(H256::default());
        }

        let (_, witness_input_type) = self.get_tx_witness_input_type(
            queue_cell.out_point.clone(),
            self.rgbpp_rpc_client.clone(),
        )?;
        let requests = Requests::from_slice(&witness_input_type.raw_data()).map_err(|e| {
            Error::TransactionParseError(format!("get requests from witness error: {}", e))
        })?;
        info!("Found {} requests in witness", requests.len());

        // build new queue
        let request_ids = vec![];
        let existing_outbox = queue_cell_data
            .outbox()
            .as_builder()
            .set(request_ids.clone())
            .build();
        let existing_inbox = queue_cell_data
            .inbox()
            .as_builder()
            .set(request_ids)
            .build();
        let queue_data = queue_cell_data
            .as_builder()
            .inbox(existing_inbox)
            .outbox(existing_outbox)
            .build();

        // build inputs
        let inputs: Vec<CellInput> = std::iter::once(queue_cell.clone().out_point)
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let mut outputs: Vec<CellOutput> = vec![queue_cell.clone().output.into()];
        let mut outputs_data = vec![queue_data.as_bytes().pack()];
        let mut needed_xudt_amount: HashMap<H256, u128> = HashMap::new();
        for request in requests {
            let (owner_lock_hash, asset_id, transfer_amount) = {
                let request_content = request.request_content();
                let message = request_content.message();
                let message_union = message.to_enum();
                match message_union {
                    MessageUnion::Transfer(transfer) => {
                        let transfer_amount: u128 = transfer.amount().unpack();
                        let owner_lock_hash: H256 = transfer.owner_lock_hash().unpack();
                        let asset_id: H256 = transfer.asset_type().unpack();
                        (owner_lock_hash, asset_id, transfer_amount)
                    }
                }
            };
            let lock = self
                .asset_locks
                .get(&owner_lock_hash)
                .ok_or(Error::LockNotFound(owner_lock_hash.to_string()))?;
            let type_ = self
                .asset_types
                .get(&asset_id)
                .ok_or(Error::AssetTypeNotFound(asset_id.to_string()))?
                .script
                .to_owned();
            let output = CellOutput::new_builder()
                .capacity(142_0000_0000.pack())
                .lock(lock.to_owned())
                .type_(Some(type_.to_owned()).pack())
                .build();
            outputs.push(output);
            outputs_data.push(Bytes::from(encode_udt_amount(transfer_amount)).pack());
            let entry = needed_xudt_amount.entry(asset_id).or_insert(0);
            *entry += transfer_amount;
        }

        // cell deps
        let secp256k1_cell_dep = self.get_rgbpp_cell_dep(SECP256K1)?;
        let xudt_cell_dep = self.get_rgbpp_cell_dep(XUDT)?;
        let queue_type_cell_dep = self.get_rgbpp_cell_dep(QUEUE_TYPE)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![secp256k1_cell_dep, xudt_cell_dep, queue_type_cell_dep])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(WitnessArgs::new_builder().build().as_bytes().pack());

        // group
        #[allow(clippy::mutable_key_type)]
        let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut type_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        {
            let lock_script: Script = queue_cell.clone().output.lock.into();
            lock_groups
                .entry(lock_script.calc_script_hash())
                .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                .input_indices
                .push(0);
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

        // custodian lock script
        let (custodian_lock_script_args, custodian_lock_key) =
            get_sighash_lock_args_from_privkey(self.rgbpp_custodian_lock_key_path.clone())?;
        let custodian_lock_script = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(custodian_lock_script_args.pack())
            .build();

        // balance xudt
        let custodian_search_option = CellQueryOptions::new_lock(custodian_lock_script);
        let mut cursor = None;
        let mut input_index = 1;
        let mut input_custodian_cells = vec![];
        loop {
            let custodian_cells = self
                .rgbpp_rpc_client
                .get_cells(
                    custodian_search_option.clone().into(),
                    Order::Asc,
                    1.into(),
                    cursor,
                )
                .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;

            if custodian_cells.objects.is_empty() {
                debug!("No more request cells found");
                break;
            }
            cursor = Some(custodian_cells.last_cursor);

            let custodian_cell = custodian_cells.objects[0].clone();
            let input: LiveCell = custodian_cell.into();
            if input.output.type_().is_none() {
                continue;
            }
            let type_ = input.output.type_().to_opt().unwrap();
            let asset_id = type_.calc_script_hash().unpack();
            let needed_amount = needed_xudt_amount.get(&asset_id);
            if needed_amount.is_none() {
                continue;
            }
            let needed_amount = needed_amount.unwrap();
            if needed_amount == &0 {
                continue;
            }
            let udt_amount = decode_udt_amount(&input.output_data);
            if udt_amount.is_none() || udt_amount == Some(0) {
                continue;
            }
            let udt_amount = udt_amount.unwrap();
            if udt_amount < *needed_amount {
                // output amount set to 0
                let output = input.output.clone();
                let output_data = Bytes::from(encode_udt_amount(0)).pack();
                tx_builder.output(output);
                tx_builder.output_data(output_data);

                // needed substracted by udt_amount
                let entry = needed_xudt_amount.entry(asset_id).or_insert(0);
                *entry -= udt_amount;
            } else {
                // output amount set to udt_amount - needed_amount
                let output = input.output.clone();
                let output_data =
                    Bytes::from(encode_udt_amount(udt_amount - *needed_amount)).pack();
                tx_builder.output(output);
                tx_builder.output_data(output_data);

                // needed set to 0
                let entry = needed_xudt_amount.entry(asset_id).or_insert(0);
                *entry = 0;
            };
            tx_builder.input(
                CellInput::new_builder()
                    .previous_output(input.out_point.clone())
                    .build(),
            );
            input_custodian_cells.push(input.clone());
            tx_builder.witness(PackedBytes::default());
            let previous_output = input.output;
            let lock_script = previous_output.lock();
            lock_groups
                .entry(lock_script.calc_script_hash())
                .or_insert_with(|| ScriptGroup::from_lock_script(&lock_script))
                .input_indices
                .push(input_index);
            input_index += 1;

            if needed_xudt_amount.values().all(|&value| value == 0) {
                break;
            }
        }

        if !needed_xudt_amount.values().all(|&value| value == 0) {
            let msg = "Insufficient balance of UDT asset".to_string();
            return Err(Error::InsufficientXUDTtoUnlock(msg));
        }

        // balance capacity
        let network_info = NetworkInfo::new(NetworkType::Testnet, self.rgbpp_uri.clone());
        let fee_rate = self.fee_rate()?;
        let configuration = {
            let mut config =
                TransactionBuilderConfiguration::new_with_network(network_info.clone())
                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
            config.fee_rate = fee_rate;
            config
        };
        let (capacity_provider_script_args, capacity_provider_key) =
            get_sighash_lock_args_from_privkey(self.rgbpp_ckb_provider_key_path.clone())?;
        let capacity_provider_script = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(capacity_provider_script_args.pack())
            .build();

        let mut change_builder =
            DefaultChangeBuilder::new(&configuration, capacity_provider_script.clone(), Vec::new());
        change_builder.init(&mut tx_builder);
        {
            let queue_cell_input = TransactionInput {
                live_cell: queue_cell.clone().into(),
                since: 0,
            };
            let _ = change_builder.check_balance(queue_cell_input, &mut tx_builder);
            for cell in &input_custodian_cells {
                let request_input = TransactionInput {
                    live_cell: cell.to_owned(),
                    since: 0,
                };
                let _ = change_builder.check_balance(request_input, &mut tx_builder);
            }
        };
        let iterator = InputIterator::new(vec![capacity_provider_script], &network_info);
        let mut tx_with_groups = {
            let mut check_result = None;
            for (mut input_index, input) in iterator.enumerate() {
                input_index += 1 + input_custodian_cells.len(); // queue cell + custodian cells
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
                    let script_groups: Vec<ScriptGroup> = lock_groups
                        .into_values()
                        .chain(type_groups.into_values())
                        .collect();

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
        let (_, message_queue_inbox_key) =
            get_sighash_lock_args_from_privkey(self.rgbpp_queue_inbox_lock_key_path.clone())?;
        TransactionSigner::new(&network_info)
            .sign_transaction(
                &mut tx_with_groups,
                &SignContexts::new_sighash(vec![
                    message_queue_inbox_key,
                    custodian_lock_key,
                    capacity_provider_key,
                ]),
            )
            .map_err(|e| Error::TransactionSignError(e.to_string()))?;

        // send tx
        let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
        info!(
            "unlock tx: {}",
            serde_json::to_string_pretty(&tx_json).unwrap()
        );
        let tx_hash = self
            .rgbpp_rpc_client
            .send_transaction(tx_json.inner, None)
            .map_err(|e| Error::TransactionSendError(format!("send transaction error: {}", e)))?;
        info!("unlock tx send: {:?}", tx_hash.pack());

        Ok(tx_hash)
    }
}
