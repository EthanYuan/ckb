use crate::schemas::leap::{MessageUnion, Request};
use crate::transaction::SIGHASH_TYPE_HASH;
use crate::{encode_udt_amount, Aggregator};

use aggregator_common::{
    error::Error,
    utils::{privkey::get_sighash_lock_args_from_privkey, SECP256K1, XUDT},
};
use ckb_jsonrpc_types::TransactionView;
use ckb_logger::info;
use ckb_sdk::CkbRpcClient;
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::{
        ckb_indexer::{Cell, Order},
        ResponseFormatGetter,
    },
    traits::{CellQueryOptions, MaturityOption, PrimaryScriptType, QueryOrder},
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
    packed::{
        Byte32, Bytes as PackedBytes, CellInput, CellOutput, OutPoint, Script, Transaction,
        WitnessArgs,
    },
    prelude::*,
    H256,
};
use molecule::prelude::Entity;

use std::collections::HashMap;

impl Aggregator {
    pub(crate) fn create_leap_tx(
        &self,
        requests: Vec<Request>,
        queue_cell: OutPoint,
    ) -> Result<H256, Error> {
        if requests.is_empty() {
            info!("No requests to mint, skip leap tx");
            return Ok(H256::default());
        }

        // Check if the requests of the last leap tx are duplicated, and if so, return immediately.
        let witness = self.get_last_leap_tx_witness();
        if let Ok((witness, tx_hash)) = witness {
            if witness == queue_cell {
                info!(
                    "The queue request has already been minted through the last leap tx: {}",
                    tx_hash
                );
                return Ok(tx_hash);
            }
        }

        // build inputs
        info!("Search Token Manager Cell ...");
        let token_manager_cell = self.get_token_manager_cell()?;
        let inputs: Vec<CellInput> = std::iter::once(token_manager_cell.out_point.clone())
            .map(|out_point| {
                CellInput::new_builder()
                    .previous_output(out_point.into())
                    .build()
            })
            .collect();

        // build outputs
        let mut outputs: Vec<CellOutput> = vec![token_manager_cell.output.clone().into()];
        let mut outputs_data: Vec<PackedBytes> = vec![token_manager_cell
            .output_data
            .clone()
            .unwrap_or_default()
            .into()];
        for request in &requests {
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
            let is_capacity = if let Some(asset) = self.rgbpp_assets.get(&asset_id) {
                asset.is_capacity
            } else {
                false
            };
            if is_capacity {
                let lock = self
                    .rgbpp_locks
                    .get(&owner_lock_hash)
                    .ok_or(Error::LockNotFound(owner_lock_hash.to_string()))?;
                let output = CellOutput::new_builder()
                    .capacity((transfer_amount as u64).pack())
                    .lock(lock.to_owned())
                    .build();
                outputs.push(output);
                outputs_data.push(PackedBytes::default());
            } else {
                let lock = self
                    .rgbpp_locks
                    .get(&owner_lock_hash)
                    .ok_or(Error::LockNotFound(owner_lock_hash.to_string()))?;
                let xudt = &self
                    .branch_scripts
                    .get(XUDT)
                    .ok_or(Error::BranchScriptNotFound(XUDT.to_string()))?
                    .script;
                let owner_lock: Script = token_manager_cell.output.lock.clone().into();
                let type_ = xudt
                    .clone()
                    .as_builder()
                    .args(owner_lock.calc_script_hash().raw_data().pack())
                    .build();
                let output = CellOutput::new_builder()
                    .capacity(200u64.pack())
                    .lock(lock.to_owned())
                    .type_(Some(type_.to_owned()).pack())
                    .build();
                outputs.push(output);
                outputs_data.push(Bytes::from(encode_udt_amount(transfer_amount)).pack());
            }
        }

        // cell deps
        let secp256k1_cell_dep = self.get_branch_cell_dep(SECP256K1)?;
        let xudt_cell_dep = self.get_branch_cell_dep(XUDT)?;

        // build transaction
        let mut tx_builder = TransactionBuilder::default();
        tx_builder
            .cell_deps(vec![secp256k1_cell_dep, xudt_cell_dep])
            .inputs(inputs)
            .outputs(outputs)
            .outputs_data(outputs_data)
            .witness(
                WitnessArgs::new_builder()
                    .input_type(Some(queue_cell.as_bytes()).pack())
                    .build()
                    .as_bytes()
                    .pack(),
            );

        // group
        #[allow(clippy::mutable_key_type)]
        let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        #[allow(clippy::mutable_key_type)]
        let mut type_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
        {
            let lock_script: Script = token_manager_cell.output.lock.clone().into();
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

        // balance transaction
        let network_info = NetworkInfo::new(NetworkType::Testnet, self.config.branch_uri.clone());
        let fee_rate = self.fee_rate()?;
        let configuration = {
            let mut config =
                TransactionBuilderConfiguration::new_with_network(network_info.clone())
                    .map_err(|e| Error::TransactionBuildError(e.to_string()))?;
            config.fee_rate = fee_rate;
            config
        };
        let (capacity_provider_script_args, capacity_provider_key) =
            get_sighash_lock_args_from_privkey(
                self.config.branch_chain_capacity_provider_key_path.clone(),
            )?;
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
                live_cell: token_manager_cell.clone().into(),
                since: 0,
            };
            let _ = change_builder.check_balance(queue_cell_input, &mut tx_builder);
        };

        let iterator = InputIterator::new(vec![capacity_provider_script], &network_info);
        let mut tx_with_groups = {
            let mut check_result = None;
            for (mut input_index, input) in iterator.enumerate() {
                input_index += 1;
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
        let (_, token_manager_key) = get_sighash_lock_args_from_privkey(
            self.config.branch_chain_token_manager_lock_key_path.clone(),
        )?;
        TransactionSigner::new(&network_info)
            .sign_transaction(
                &mut tx_with_groups,
                &SignContexts::new_sighash(vec![capacity_provider_key, token_manager_key]),
            )
            .map_err(|e| Error::TransactionSignError(e.to_string()))?;

        // send tx
        let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
        info!(
            "leap tx: {}",
            serde_json::to_string_pretty(&tx_json).unwrap()
        );
        let tx_hash = self
            .branch_rpc_client
            .send_transaction(tx_json.inner, None)
            .map_err(|e| Error::TransactionSendError(format!("send transaction error: {}", e)))?;
        info!("leap tx send: {:?}", tx_hash.pack());

        Ok(tx_hash)
    }

    fn get_last_leap_tx_witness(&self) -> Result<(OutPoint, H256), Error> {
        let token_manager_cell = self.get_token_manager_cell()?;
        let (tx_hash, witness_input_type) = self.get_tx_witness_input_type(
            token_manager_cell.out_point,
            self.branch_rpc_client.clone(),
        )?;
        let queue_out_point =
            OutPoint::from_slice(&witness_input_type.raw_data()).map_err(|e| {
                Error::TransactionParseError(format!("get queue from witness error: {}", e))
            })?;
        Ok((queue_out_point, tx_hash))
    }

    pub(crate) fn get_tx_witness_input_type(
        &self,
        out_point: ckb_jsonrpc_types::OutPoint,
        rpc_client: CkbRpcClient,
    ) -> Result<(H256, PackedBytes), Error> {
        let tx_hash = out_point.tx_hash;
        let index: u32 = out_point.index.into();
        let tx = rpc_client
            .get_transaction(tx_hash.clone())
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .transaction
            .ok_or(Error::RpcError("get transaction error: None".to_string()))?
            .get_value()
            .map_err(|e| Error::RpcError(format!("get transaction error: {}", e)))?
            .inner;
        let tx: Transaction = tx.into();
        let witness = tx
            .witnesses()
            .get(index as usize)
            .ok_or(Error::TransactionParseError(
                "get witness error: None".to_string(),
            ))?;
        let witness_input_type = WitnessArgs::from_slice(&witness.raw_data())
            .map_err(|e| Error::TransactionParseError(format!("get witness error: {}", e)))?
            .input_type()
            .to_opt()
            .ok_or(Error::TransactionParseError(
                "get witness input type error: None".to_string(),
            ))?;
        Ok((tx.calc_tx_hash().unpack(), witness_input_type))
    }

    fn get_token_manager_cell(&self) -> Result<Cell, Error> {
        let token_manager_cell_search_option = self.build_token_manager_cell_search_option()?;
        let token_manager_cell = self
            .branch_rpc_client
            .get_cells(
                token_manager_cell_search_option.into(),
                Order::Asc,
                1.into(),
                None,
            )
            .map_err(|e| Error::LiveCellNotFound(e.to_string()))?;
        if token_manager_cell.objects.len() != 1 {
            return Err(Error::LiveCellNotFound(format!(
                "Token Manager cell found: {}",
                token_manager_cell.objects.len()
            )));
        }
        info!(
            "Found {} Token Manager cell",
            token_manager_cell.objects.len()
        );
        let token_manager_cell = token_manager_cell.objects[0].clone();

        Ok(token_manager_cell)
    }

    fn build_token_manager_cell_search_option(&self) -> Result<CellQueryOptions, Error> {
        let (token_manager_lock_args, _) = get_sighash_lock_args_from_privkey(
            self.config.branch_chain_token_manager_lock_key_path.clone(),
        )?;
        let token_manager_lock = Script::new_builder()
            .code_hash(SIGHASH_TYPE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(token_manager_lock_args.pack())
            .build();

        let cell_query_option = CellQueryOptions {
            primary_script: token_manager_lock,
            primary_type: PrimaryScriptType::Lock,
            with_data: Some(true),
            secondary_script: None,
            secondary_script_len_range: None,
            data_len_range: None,
            capacity_range: None,
            block_range: None,
            order: QueryOrder::Asc,
            limit: Some(1),
            maturity: MaturityOption::Mature,
            min_total_capacity: 1,
            script_search_mode: None,
        };
        Ok(cell_query_option)
    }
}
