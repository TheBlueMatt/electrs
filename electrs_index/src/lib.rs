#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

extern crate configure_me;

// export specific versions of rust-bitcoin crates
pub use bitcoin;
// pub use bitcoincore_rpc;

mod chain;
mod config;
mod db;
mod index;
mod metrics;
mod sync;
mod types;

pub use {
    config::Config,
    db::DBStore,
    index::Index,
    metrics::{Gauge, GaugeVec, Histogram, Metrics},
    sync::Syncer,
    types::ScriptHash,
};
