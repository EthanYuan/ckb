# Branch Chain

A branch chain is a BTC Layer-2 chain that actually runs smart contracts. It is characterized by high TPS (Transactions Per Second), low fees, fewer block-producing nodes, and a challenge-based security model. 

For more information, see the [branch chain](https://github.com/ckb-cell/utxo-stack-doc/blob/master/docs/branch-chain.md).

## Features

- BTC as cell capacity token, 1 satoshi can be used for 1 byte of on-chain space
- Single sequencer
- Blocks produced every 2 seconds

---

## Quick Start

### Build from Source

Source code:

```shell
# get ckb source code
git clone https://github.com/EthanYuan/ckb.git
cd ckb
git checkout v0.116.1-branch-chain
```

Build:

```shell
make prod
```

For reference, see the [build from source](https://docs-old.nervos.org/docs/basics/guides/get-ckb#build-from-source)

### Init

```shell
mkdir branch-dev
cd branch-dev
```

```shell
ckb init --chain dev --genesis-message branch-dev
```

### Configure 

The following settings are used to configure the `block_assembler` in the `ckb.toml` file:

```toml
[block_assembler]
code_hash = "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8"
args = "0x470dcdc5e44064909650113a274b3b36aecb6dc7"
hash_type = "type"
message = "0x"
```

The `args` value in the `block_assembler` configuration is directly taken from the `args` specified in the `lock script` of a pre-defined cell in the genesis configuration (`[genesis.issued_cells]`). This allows for a simplified setup, as you can reuse the existing parameters from the development chain's specification.

**Please NOTE that this setup is intended for testing purposes only and should not be used in a production environment.**

### Start Node with Indexer

```shell
ckb run --indexer
```
Restarting in the same directory will reuse the data.

## Use RPC

Find RPC port in the log output, the following command assumes 8114 is used:

```shell
curl -d '{"id": 1, "jsonrpc": "2.0", "method":"get_tip_header","params": []}' \
  -H 'content-type:application/json' 'http://localhost:8114'
```

For more RPC commands, please refer to the [RPC documentation](rpc/README.md).

---

## Documentations

- [RGBPP Layer](rgbpp-layer.md)
