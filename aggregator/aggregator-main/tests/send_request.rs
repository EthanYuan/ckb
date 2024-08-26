use aggregator_common::types::RequestType;
use aggregator_main::{
    schemas::leap::{self, MessageUnion, RequestContent, Transfer},
    ScriptInfo, SIGHASH_TYPE_HASH,
};
use ckb_app_config::ScriptConfig;
use ckb_hash::blake2b_256;
use ckb_jsonrpc_types::TransactionView;
use ckb_sdk::{
    core::TransactionBuilder,
    rpc::CkbRpcClient as RpcClient,
    traits::LiveCell,
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
    h160, h256,
    packed::{
        Byte, Byte32, Bytes as PackedBytes, CellDep, CellInput, CellOutput, OutPoint, Script,
    },
    prelude::*,
    H256,
};

use std::collections::HashMap;

#[test]
#[ignore]
fn send_request() {
    let ckb_provider_lock_privkey =
        h256!("0x6b726167f8d0b2b8722b9a3f0cfdc67d43f1622666a6fda6b32da76a3824e52e");
    let ckb_provider_lock_args = h160!("0xa0c73e84d827b87073a2a0e6448921870d114d02").as_bytes();

    let branch_uri = "http://localhost:8114";

    let message_queue_type_id =
        h256!("0xd1affd2e1d88d8f44ddace885e740687b6b7fc82545410dac513362942bf34be");
    let request_lock_code_hash =
        h256!("0x1a1e4fef34f5982906f745b048fe7b1089647e82346074e0f32c2ece26cf6b1e");
    let xudt_code_hash =
        h256!("0x6283a479a3cf5d4276cd93594de9f1827ab9b55c7b05b3d28e4c2e0a696cfefd");

    // input
    let out_point = OutPoint::new(
        h256!("0x32f7ef35ea6fda741ff7613d992c84115096bae05e6ffa47c6c054f8a0b2cbda").pack(),
        0,
    );
    let input = CellInput::new_builder()
        .previous_output(out_point.clone().clone())
        .build();
    let branch_rpc_client = RpcClient::new(branch_uri);
    let input_cell = branch_rpc_client
        .get_live_cell(out_point.clone().into(), true)
        .unwrap();
    println!("input_cell: {:?}", input_cell);
    let input_cell = input_cell.cell.unwrap();
    let tx_input = TransactionInput {
        live_cell: LiveCell {
            output: input_cell.output.clone().into(),
            output_data: input_cell.data.unwrap().content.into_bytes(),
            out_point: input.previous_output(),
            block_number: 14318072,
            tx_index: 2,
        },
        since: 0,
    };

    let user_lock = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(Bytes::from(ckb_provider_lock_args).pack())
        .build();
    let token_script = Script::new_builder()
        .code_hash(xudt_code_hash.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(user_lock.calc_script_hash().as_bytes().pack())
        .build();

    // output: create request
    let branch_chain_id = Bytes::from(b"mocked-branch-chain-id".to_vec());
    let message = leap::Message::new_builder()
        .set(MessageUnion::Transfer(
            Transfer::new_builder()
                .owner_lock_hash(user_lock.calc_script_hash())
                .amount(1000u128.pack())
                .asset_type(token_script.calc_script_hash())
                .build(),
        ))
        .build();
    let request_content = RequestContent::new_builder()
        .request_type(Byte::new(RequestType::BranchToCkb as u8))
        .target_chain_id(branch_chain_id.pack())
        .message(message)
        .build();
    let (request_cell, request_cell_data) = build_request(
        request_lock_code_hash.pack(),
        message_queue_type_id.pack(),
        user_lock.calc_script_hash(),
        request_content.clone(),
        token_script.clone(),
        1000,
    );

    // cell deps
    let scripts = prepare_scripts();
    let secp256k1_cell_dep = get_branch_cell_dep("secp256k1_blake160", &scripts);
    let xudt_cell_dep = get_branch_cell_dep("xudt", &scripts);
    let request_cell_dep = get_branch_cell_dep("request_lock", &scripts);
    let queue_type_cell_dep = get_branch_cell_dep("queue_type", &scripts);

    // create transaction
    let mut tx_builder = TransactionBuilder::default();
    tx_builder
        .cell_deps(vec![
            secp256k1_cell_dep,
            xudt_cell_dep,
            request_cell_dep,
            queue_type_cell_dep,
        ])
        .input(input)
        .output(request_cell)
        .output_data(request_cell_data.pack());

    // group
    let mut lock_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
    let mut type_groups: HashMap<Byte32, ScriptGroup> = HashMap::default();
    {
        lock_groups
            .entry(user_lock.calc_script_hash())
            .or_insert_with(|| ScriptGroup::from_lock_script(&user_lock))
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
    let network_info = NetworkInfo::new(NetworkType::Testnet, branch_uri.to_string());
    let fee_rate = 3000;
    let configuration = {
        let mut config =
            TransactionBuilderConfiguration::new_with_network(network_info.clone()).unwrap();
        config.fee_rate = fee_rate;
        config
    };
    let (capacity_provider_script_args, capacity_provider_key) =
        get_sighash_lock_args_from_privkey(ckb_provider_lock_privkey);
    let capacity_provider_script = Script::new_builder()
        .code_hash(SIGHASH_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(capacity_provider_script_args.pack())
        .build();
    let mut change_builder =
        DefaultChangeBuilder::new(&configuration, capacity_provider_script.clone(), Vec::new());
    change_builder.init(&mut tx_builder);
    {
        let _ = change_builder.check_balance(tx_input, &mut tx_builder);
    };
    let iterator = InputIterator::new(vec![user_lock], &network_info);
    let mut tx_with_groups = {
        let mut check_result = None;
        for (mut input_index, input) in iterator.enumerate() {
            println!("{:?}", input);
            input_index += 1;
            let input = input.unwrap();
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
    .unwrap();

    // sign
    TransactionSigner::new(&network_info)
        .sign_transaction(
            &mut tx_with_groups,
            &SignContexts::new_sighash(vec![capacity_provider_key]),
        )
        .unwrap();

    // send tx
    let tx_json = TransactionView::from(tx_with_groups.get_tx_view().clone());
    println!(
        "request tx: {}",
        serde_json::to_string_pretty(&tx_json).unwrap()
    );
    let tx_hash = branch_rpc_client
        .send_transaction(tx_json.inner, None)
        .unwrap();
    println!("request tx send: {:?}", tx_hash.pack());
}

fn build_request(
    request_lock_code_hash: Byte32,
    request_type_hash: Byte32,
    owner_lock_hash: Byte32,
    content: RequestContent,
    token_script: Script,
    amount: u128,
) -> (CellOutput, Bytes) {
    let lock_args = leap::RequestLockArgs::new_builder()
        .request_type_hash(request_type_hash)
        .owner_lock_hash(owner_lock_hash)
        .content(content)
        .build();
    let request_lock = Script::new_builder()
        .code_hash(request_lock_code_hash)
        .hash_type(ScriptHashType::Type.into())
        .args(lock_args.as_bytes().pack())
        .build();
    let cell = CellOutput::new_builder()
        .lock(request_lock)
        .type_(Some(token_script).pack())
        .capacity(1_0000_0000.pack())
        .build();
    let buf: [u8; 16] = amount.to_le_bytes();
    let data = buf.to_vec().into();
    (cell, data)
}

pub(crate) fn prepare_scripts() -> HashMap<String, ScriptInfo> {
    let mut rgbpp_script_config: Vec<ScriptConfig> = Vec::new();
    let xudt_script = r#"
    {
        "args": "0x",
        "code_hash": "0x6283a479a3cf5d4276cd93594de9f1827ab9b55c7b05b3d28e4c2e0a696cfefd",
        "hash_type": "type"
    }
    "#;
    let xudt_out_point = r#"
    {
        "dep_type": "code",
        "out_point": {
            "index": "0x5",
            "tx_hash": "0x1c21f865d6564c7b54e5a616adee365378b41b92e5c941cdf45c572c0c9e5811"
        }
    }
    "#;
    rgbpp_script_config.push(ScriptConfig {
        script_name: "xudt".to_string(),
        script: xudt_script.to_string(),
        cell_dep: xudt_out_point.to_string(),
    });
    let request_lock = r#"
    {
        "args": "0x",
        "code_hash": "0x1a1e4fef34f5982906f745b048fe7b1089647e82346074e0f32c2ece26cf6b1e",
        "hash_type": "type"
    }
    "#;
    let request_out_point = r#"
    {
        "dep_type": "code",
        "out_point": {
            "index": "0x6",
            "tx_hash": "0x1c21f865d6564c7b54e5a616adee365378b41b92e5c941cdf45c572c0c9e5811"
        }
    }
    "#;
    rgbpp_script_config.push(ScriptConfig {
        script_name: "request_lock".to_string(),
        script: request_lock.to_string(),
        cell_dep: request_out_point.to_string(),
    });
    let queue_type_script = r#"
    {
        "args": "0x4242",
        "code_hash": "0x9c6933d977360f115a3e9cd5a2e0e475853681b80d775d93ad0f8969da343e56",
        "hash_type": "type"
    }
    "#;
    let queue_type_out_point = r#"
    {
        "dep_type": "code",
        "out_point": {
            "index": "0x7",
            "tx_hash": "0x1c21f865d6564c7b54e5a616adee365378b41b92e5c941cdf45c572c0c9e5811"
        }
    }
    "#;
    rgbpp_script_config.push(ScriptConfig {
        script_name: "queue_type".to_string(),
        script: queue_type_script.to_string(),
        cell_dep: queue_type_out_point.to_string(),
    });
    let secp256k1_blake160_script = r#"
    {
        "args": "0x",
        "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
        "hash_type": "type"
    }
    "#;
    let secp256k1_blake160_out_point = r#"
    {
        "dep_type": "dep_group",
        "out_point": {
            "index": "0x0",
            "tx_hash": "0xde836a3233cbd4904f60ec9bfb5a2055d15314b751c9c0d4acff37d5e402f003"
        }
    }
    "#;
    rgbpp_script_config.push(ScriptConfig {
        script_name: "secp256k1_blake160".to_string(),
        script: secp256k1_blake160_script.to_string(),
        cell_dep: secp256k1_blake160_out_point.to_string(),
    });

    get_script_map(rgbpp_script_config)
}

pub(crate) fn get_branch_cell_dep(
    script_name: &str,
    branch_scripts: &HashMap<String, ScriptInfo>,
) -> CellDep {
    branch_scripts
        .get(script_name)
        .map(|script_info| script_info.cell_dep.clone())
        .unwrap()
}

fn get_script_map(scripts: Vec<ScriptConfig>) -> HashMap<String, ScriptInfo> {
    scripts
        .iter()
        .map(|s| {
            (
                s.script_name.clone(),
                ScriptInfo {
                    script: serde_json::from_str::<ckb_jsonrpc_types::Script>(&s.script)
                        .expect("config string to script")
                        .into(),
                    cell_dep: serde_json::from_str::<ckb_jsonrpc_types::CellDep>(&s.cell_dep)
                        .expect("config string to cell dep")
                        .into(),
                },
            )
        })
        .collect()
}

pub fn get_sighash_lock_args_from_privkey(key: H256) -> (Bytes, secp256k1::SecretKey) {
    let secret_key = secp256k1::SecretKey::from_slice(key.as_bytes())
        .expect("impossible: fail to build secret key");
    let secp256k1: secp256k1::Secp256k1<secp256k1::All> = secp256k1::Secp256k1::new();
    let pubkey = secp256k1::PublicKey::from_secret_key(&secp256k1, &secret_key);
    let pubkey_compressed = &pubkey.serialize()[..];
    let pubkey_hash = blake2b_256(pubkey_compressed);
    let pubkey_hash = &pubkey_hash[0..20];
    let args = Bytes::from(pubkey_hash.to_vec());
    (args, secret_key)
}
