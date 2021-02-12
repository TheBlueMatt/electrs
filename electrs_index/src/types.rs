use anyhow::Result;

use std::convert::TryInto;

use bitcoin::{
    consensus::encode::{deserialize, serialize, Decodable, Encodable},
    hashes::{borrow_slice_impl, hash_newtype, hex_fmt_impl, index_impl, serde_impl, sha256, Hash},
    BlockHeader, OutPoint, Script, Txid,
};

use crate::db;

macro_rules! impl_consensus_encoding {
    ($thing:ident, $($field:ident),+) => (
        impl Encodable for $thing {
            #[inline]
            fn consensus_encode<S: ::std::io::Write>(
                &self,
                mut s: S,
            ) -> Result<usize, std::io::Error> {
                let mut len = 0;
                $(len += self.$field.consensus_encode(&mut s)?;)+
                Ok(len)
            }
        }

        impl Decodable for $thing {
            #[inline]
            fn consensus_decode<D: ::std::io::Read>(
                mut d: D,
            ) -> Result<$thing, bitcoin::consensus::encode::Error> {
                Ok($thing {
                    $($field: Decodable::consensus_decode(&mut d)?),+
                })
            }
        }
    );
}

hash_newtype!(
    ScriptHash,
    sha256::Hash,
    32,
    doc = "SHA256(scriptPubkey)",
    true
);

impl ScriptHash {
    pub fn new(script: &Script) -> Self {
        ScriptHash::hash(&script[..])
    }

    fn prefix(&self) -> ScriptHashPrefix {
        let mut prefix = [0u8; PREFIX_LEN];
        prefix.copy_from_slice(&self.0[..PREFIX_LEN]);
        ScriptHashPrefix { prefix }
    }
}

const PREFIX_LEN: usize = 8;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ScriptHashPrefix {
    prefix: [u8; PREFIX_LEN],
}

impl_consensus_encoding!(ScriptHashPrefix, prefix);

pub type Height = u32;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct ScriptHashRow {
    prefix: ScriptHashPrefix,
    height: Height, // transaction confirmed height
}

impl_consensus_encoding!(ScriptHashRow, prefix, height);

impl ScriptHashRow {
    pub fn scan_prefix(script_hash: ScriptHash) -> Box<[u8]> {
        script_hash.0[..PREFIX_LEN].to_vec().into_boxed_slice()
    }

    pub fn new(script_hash: ScriptHash, height: usize) -> Self {
        Self {
            prefix: script_hash.prefix(),
            height: height.try_into().expect("invalid height"),
        }
    }

    pub fn to_db_row(&self) -> db::Row {
        serialize(self).into_boxed_slice()
    }

    pub fn from_db_row(row: &[u8]) -> Self {
        deserialize(&row).expect("bad ScriptHashRow")
    }

    pub fn height(&self) -> usize {
        self.height.try_into().expect("invalid height")
    }
}

// ***************************************************************************

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SpendingPrefix {
    prefix: [u8; PREFIX_LEN],
}

impl_consensus_encoding!(SpendingPrefix, prefix);

fn spending_prefix(prev: OutPoint) -> SpendingPrefix {
    let txid_prefix = &prev.txid[..PREFIX_LEN];
    let value = u64::from_be_bytes(txid_prefix.try_into().unwrap());
    let value = value.wrapping_add(prev.vout.into());
    SpendingPrefix {
        prefix: value.to_be_bytes(),
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct SpendingPrefixRow {
    prefix: SpendingPrefix,
    height: Height, // transaction confirmed height
}

impl_consensus_encoding!(SpendingPrefixRow, prefix, height);

impl SpendingPrefixRow {
    pub fn scan_prefix(outpoint: OutPoint) -> Box<[u8]> {
        Box::new(spending_prefix(outpoint).prefix)
    }

    pub fn new(outpoint: OutPoint, height: usize) -> Self {
        Self {
            prefix: spending_prefix(outpoint),
            height: height.try_into().expect("invalid height"),
        }
    }

    pub fn to_db_row(&self) -> db::Row {
        serialize(self).into_boxed_slice()
    }

    pub fn from_db_row(row: &[u8]) -> Self {
        deserialize(&row).expect("bad SpendingPrefixRow")
    }

    pub fn height(&self) -> usize {
        self.height.try_into().expect("invalid height")
    }
}

// ***************************************************************************

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TxidPrefix {
    prefix: [u8; PREFIX_LEN],
}

impl_consensus_encoding!(TxidPrefix, prefix);

fn txid_prefix(txid: &Txid) -> TxidPrefix {
    let mut prefix = [0u8; PREFIX_LEN];
    prefix.copy_from_slice(&txid[..PREFIX_LEN]);
    TxidPrefix { prefix }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct TxidRow {
    prefix: TxidPrefix,
    height: Height, // transaction confirmed height
}

impl_consensus_encoding!(TxidRow, prefix, height);

impl TxidRow {
    pub fn scan_prefix(txid: Txid) -> Box<[u8]> {
        Box::new(txid_prefix(&txid).prefix)
    }

    pub fn new(txid: Txid, height: usize) -> Self {
        Self {
            prefix: txid_prefix(&txid),
            height: height.try_into().expect("invalid height"),
        }
    }

    pub fn to_db_row(&self) -> db::Row {
        serialize(self).into_boxed_slice()
    }

    pub fn from_db_row(row: &[u8]) -> Self {
        deserialize(&row).expect("bad TxidRow")
    }

    pub fn height(&self) -> usize {
        self.height.try_into().expect("invalid height")
    }
}

// ***************************************************************************

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HeaderRow {
    pub header: BlockHeader,
}

impl_consensus_encoding!(HeaderRow, header);

impl HeaderRow {
    pub fn new(header: BlockHeader) -> Self {
        Self { header }
    }

    pub fn to_db_row(&self) -> db::Row {
        serialize(self).into_boxed_slice()
    }

    pub fn from_db_row(row: &[u8]) -> Self {
        deserialize(&row).expect("bad HeaderRow")
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{spending_prefix, ScriptHash, ScriptHashRow, SpendingPrefix};
    use bitcoin::{hashes::hex::ToHex, Address, OutPoint, Txid};
    use serde_json::{from_str, json};

    use std::str::FromStr;

    #[test]
    fn test_script_hash_serde() {
        let hex = "\"4b3d912c1523ece4615e91bf0d27381ca72169dbf6b1c2ffcc9f92381d4984a3\"";
        let script_hash: ScriptHash = from_str(&hex).unwrap();
        assert_eq!(format!("\"{}\"", script_hash), hex);
        assert_eq!(json!(script_hash).to_string(), hex);
    }

    #[test]
    fn test_script_hash_row() {
        let hex = "\"4b3d912c1523ece4615e91bf0d27381ca72169dbf6b1c2ffcc9f92381d4984a3\"";
        let script_hash: ScriptHash = from_str(&hex).unwrap();
        let row1 = ScriptHashRow::new(script_hash, 123456);
        let db_row = row1.to_db_row();
        assert_eq!(db_row[..].to_hex(), "a384491d38929fcc341278563412");
        let row2 = ScriptHashRow::from_db_row(&db_row);
        assert_eq!(row1, row2);
    }

    #[test]
    fn test_script_hash() {
        let addr = Address::from_str("1KVNjD3AAnQ3gTMqoTKcWFeqSFujq9gTBT").unwrap();
        let script_hash = ScriptHash::new(&addr.script_pubkey());
        assert_eq!(
            script_hash.to_hex(),
            "00dfb264221d07712a144bda338e89237d1abd2db4086057573895ea2659766a"
        );
    }

    #[test]
    fn test_spending_prefix() {
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let txid = Txid::from_str(hex).unwrap();

        assert_eq!(
            spending_prefix(OutPoint { txid, vout: 0 }),
            SpendingPrefix {
                prefix: [31, 30, 29, 28, 27, 26, 25, 24]
            }
        );
        assert_eq!(
            spending_prefix(OutPoint { txid, vout: 10 }),
            SpendingPrefix {
                prefix: [31, 30, 29, 28, 27, 26, 25, 34]
            }
        );
        assert_eq!(
            spending_prefix(OutPoint { txid, vout: 255 }),
            SpendingPrefix {
                prefix: [31, 30, 29, 28, 27, 26, 26, 23]
            }
        );
        assert_eq!(
            spending_prefix(OutPoint { txid, vout: 256 }),
            SpendingPrefix {
                prefix: [31, 30, 29, 28, 27, 26, 26, 24]
            }
        );
    }
}
