#[macro_use]
extern crate log;

use anyhow::{Context, Result};
use bitcoin::{Address, Amount, BlockHash, OutPoint, Script, Txid};

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use electrs_index::*;

fn load_funding(
    syncer: &mut Syncer,
    blockhashes: &[BlockHash],
    script: &Script,
) -> Result<HashMap<OutPoint, Amount>> {
    let mut funding = HashMap::new();
    syncer.for_blocks(blockhashes, |block| {
        for tx in block.txdata {
            for (txo, vout) in tx.output.iter().zip(0u32..) {
                if txo.script_pubkey == *script {
                    let outpoint = OutPoint::new(tx.txid(), vout);
                    assert_eq!(
                        funding.insert(outpoint, Amount::from_sat(txo.value)),
                        None,
                        "double-fund of {}",
                        outpoint
                    );
                }
            }
        }
    })?;
    Ok(funding)
}

fn load_spending(
    syncer: &mut Syncer,
    blockhashes: &[BlockHash],
    outpoint: &OutPoint,
) -> Result<Option<Txid>> {
    let mut txid = None;
    syncer.for_blocks(blockhashes, |block| {
        for tx in block.txdata {
            for txi in &tx.input {
                if txi.previous_output == *outpoint {
                    assert_eq!(txid, None, "double-spend of {}", outpoint);
                    txid = Some(tx.txid());
                }
            }
        }
    })?;
    Ok(txid)
}

fn get_balance(
    a: &Address,
    funding: HashMap<OutPoint, Amount>,
    spending: HashMap<OutPoint, Txid>,
) -> Amount {
    let mut total = Amount::ZERO;
    for (outpoint, value) in funding {
        match spending.get(&outpoint) {
            None => {
                info!("{}: {} has {}", a, outpoint, value,);
                total += value;
            }
            Some(txid) => debug!("{}: {} had {}, spent by {}", a, outpoint, value, txid,),
        }
    }
    total
}

fn main() -> Result<()> {
    let config = Config::from_args();

    let metrics = Metrics::new(config.monitoring_addr)?;
    let store = DBStore::open(Path::new(&config.db_path), config.low_memory)?;
    let mut index = Index::new(store, &metrics).context("failed to open index")?;

    let addresses: Vec<Address> = config
        .args
        .iter()
        .map(|a| Address::from_str(a).expect("invalid address"))
        .collect();

    let mut syncer = Syncer::new(config.network)?;
    let mut chain = index.chain(&mut syncer);
    loop {
        index.sync(&mut syncer, &mut chain)?;
        index.flush();

        let mut total = Amount::ZERO;
        for a in &addresses {
            let script = a.script_pubkey();
            let sh = ScriptHash::new(&script);

            let blockhashes = index.lookup_by_scripthash(sh, &chain);
            let funding = load_funding(&mut syncer, &blockhashes, &script)?;

            let mut spending = HashMap::new();
            for outpoint in funding.keys() {
                let blockhashes = index.lookup_by_spending(*outpoint, &chain);
                load_spending(&mut syncer, &blockhashes, outpoint)?
                    .map(|txid| spending.insert(*outpoint, txid));
            }
            total += get_balance(a, funding, spending);
        }
        info!("total: {}", total);
        syncer.wait_for_inv()?;
    }
}
