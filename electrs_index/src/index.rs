use anyhow::Result;
use bitcoin::consensus::{deserialize, serialize};
use bitcoin::{Block, BlockHash, OutPoint, Txid};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    chain::Chain,
    db,
    metrics::{Histogram, Metrics},
    sync::Syncer,
    types::{HeaderRow, ScriptHash, ScriptHashRow, SpendingPrefixRow, TxidRow},
};

struct StopFlag {
    flag: AtomicBool,
}

impl StopFlag {
    fn new() -> Self {
        StopFlag {
            flag: AtomicBool::new(false),
        }
    }

    fn set(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    fn bail_if_set(&self, msg: &str) -> Result<()> {
        if self.flag.load(Ordering::Relaxed) {
            bail!("interrupted {}", msg);
        }
        Ok(())
    }
}

pub struct Index {
    store: db::DBStore,
    stats: Stats,
    stop_flag: StopFlag,
}

#[derive(Clone)]
struct Stats {
    update_duration: Histogram,
    update_size: Histogram,
    lookup_duration: Histogram,
}

impl Stats {
    fn new(metrics: &Metrics) -> Self {
        Self {
            update_duration: metrics.histogram_vec(
                "index_update_duration",
                "Index update duration (in seconds)",
                &["step"],
            ),
            update_size: metrics.histogram_vec(
                "index_update_size",
                "Index update size (in bytes)",
                &["step"],
            ),
            lookup_duration: metrics.histogram_vec(
                "index_lookup_duration",
                "Index lookup duration (in seconds)",
                &["step"],
            ),
        }
    }

    fn report_stats(&self, batch: &db::WriteBatch) {
        self.update_size
            .observe_size("write_funding_rows", db_rows_size(&batch.funding_rows));
        self.update_size
            .observe_size("write_spending_rows", db_rows_size(&batch.spending_rows));
        self.update_size
            .observe_size("write_txid_rows", db_rows_size(&batch.txid_rows));
        self.update_size
            .observe_size("write_header_rows", db_rows_size(&batch.header_rows));
        debug!(
            "writing {} funding and {} spending rows from {} transactions, {} blocks",
            batch.funding_rows.len(),
            batch.spending_rows.len(),
            batch.txid_rows.len(),
            batch.header_rows.len()
        );
    }
}

struct IndexResult {
    header_row: HeaderRow,
    funding_rows: Vec<ScriptHashRow>,
    spending_rows: Vec<SpendingPrefixRow>,
    txid_rows: Vec<TxidRow>,
}

impl IndexResult {
    fn extend(&self, batch: &mut db::WriteBatch) {
        let funding_rows = self.funding_rows.iter().map(ScriptHashRow::to_db_row);
        let spending_rows = self.spending_rows.iter().map(SpendingPrefixRow::to_db_row);
        let txid_rows = self.txid_rows.iter().map(TxidRow::to_db_row);

        batch.funding_rows.extend(funding_rows);
        batch.spending_rows.extend(spending_rows);
        batch.txid_rows.extend(txid_rows);
        batch.header_rows.push(self.header_row.to_db_row());
        batch.tip_row = serialize(&self.header_row.header.block_hash()).into_boxed_slice();
    }
}

impl Index {
    pub fn new(store: db::DBStore, metrics: &Metrics) -> Result<Self> {
        Ok(Index {
            store,
            stats: Stats::new(metrics),
            stop_flag: StopFlag::new(),
        })
    }

    pub fn stop(&self) {
        self.stop_flag.set();
    }

    pub fn chain(&self, syncer: &Syncer) -> Chain {
        let tip: BlockHash = match self.store.get_tip() {
            Some(row) => deserialize(&row).expect("invalid tip"),
            None => return syncer.new_chain(),
        };
        let headers = self
            .store
            .read_headers()
            .into_iter()
            .map(|row| HeaderRow::from_db_row(&row).header)
            .collect();

        syncer.load_chain(headers, tip)
    }

    pub fn lookup_by_txid(&self, txid: Txid, chain: &Chain) -> Vec<BlockHash> {
        self.store
            .iter_txid(&TxidRow::scan_prefix(txid))
            .map(|row| TxidRow::from_db_row(&row).height())
            .filter_map(|height| chain.get_block_hash(height))
            .collect()
    }

    pub fn lookup_by_scripthash(&self, script_hash: ScriptHash, chain: &Chain) -> Vec<BlockHash> {
        self.store
            .iter_funding(&ScriptHashRow::scan_prefix(script_hash))
            .map(|row| ScriptHashRow::from_db_row(&row).height())
            .filter_map(|height| chain.get_block_hash(height))
            .collect()
    }

    pub fn lookup_by_spending(&self, outpoint: OutPoint, chain: &Chain) -> Vec<BlockHash> {
        self.store
            .iter_spending(&SpendingPrefixRow::scan_prefix(outpoint))
            .map(|row| SpendingPrefixRow::from_db_row(&row).height())
            .filter_map(|height| chain.get_block_hash(height))
            .collect()
    }

    pub fn sync(&self, syncer: &mut Syncer, chain: &mut Chain) -> Result<()> {
        loop {
            let new_headers = syncer.get_new_headers(chain)?;
            if new_headers.is_empty() {
                break;
            }
            info!(
                "indexing {} blocks: [{}..{}]",
                new_headers.len(),
                new_headers.first().unwrap().height(),
                new_headers.last().unwrap().height()
            );
            let blockhashes: Vec<BlockHash> = new_headers.iter().map(|h| h.hash()).collect();
            let mut heights_map: HashMap<BlockHash, usize> =
                new_headers.iter().map(|h| (h.hash(), h.height())).collect();

            let mut batch = db::WriteBatch::default();

            syncer.for_blocks(&blockhashes, |block| {
                let height = heights_map
                    .remove(&block.block_hash())
                    .expect("unexpected block");
                let result = index_single_block(block, height);
                result.extend(&mut batch);
            })?;
            assert!(heights_map.is_empty(), "some blocks were not indexed");
            batch.sort();
            self.stats.report_stats(&batch);
            self.store.write(batch);
            chain.update(new_headers);
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        self.store.flush()
    }
}

fn db_rows_size(rows: &[db::Row]) -> usize {
    rows.iter().map(|key| key.len()).sum()
}

fn index_single_block(block: Block, height: usize) -> IndexResult {
    let mut funding_rows = vec![];
    let mut spending_rows = vec![];
    let mut txid_rows = Vec::with_capacity(block.txdata.len());

    for tx in &block.txdata {
        txid_rows.push(TxidRow::new(tx.txid(), height));

        funding_rows.extend(
            tx.output
                .iter()
                .filter(|txo| !txo.script_pubkey.is_provably_unspendable())
                .map(|txo| {
                    let scripthash = ScriptHash::new(&txo.script_pubkey);
                    ScriptHashRow::new(scripthash, height)
                }),
        );

        if tx.is_coin_base() {
            continue; // coinbase doesn't have inputs
        }
        spending_rows.extend(
            tx.input
                .iter()
                .map(|txin| SpendingPrefixRow::new(txin.previous_output, height)),
        );
    }
    IndexResult {
        funding_rows,
        spending_rows,
        txid_rows,
        header_row: HeaderRow::new(block.header),
    }
}
