#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate log;

extern crate bitcoin;

use anyhow::{Context, Result};

use std::collections::HashMap;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, process};

use bitcoin::consensus::encode;
use bitcoin::hashes::hex::FromHex;
use bitcoin::network::stream_reader::StreamReader;
use bitcoin::network::{
    address, constants,
    message::{self, NetworkMessage},
    message_blockdata::{GetHeadersMessage, Inventory},
    message_network,
};
use bitcoin::secp256k1;
use bitcoin::secp256k1::rand::Rng;
use bitcoin::{Block, BlockHash, BlockHeader};

struct Client {
    stream: TcpStream,
    reader: StreamReader<TcpStream>,
    network: constants::Network,
}

impl Client {
    fn connect(addr: SocketAddr, network: constants::Network) -> Result<Client> {
        let stream = TcpStream::connect(addr).context("p2p failed to connect")?;
        let reader = StreamReader::new(
            stream.try_clone().context("p2p failed to clone")?,
            /*buffer_size*/ Some(1 << 20),
        );
        let mut client = Client {
            stream,
            reader,
            network,
        };
        client.send(build_version_message(addr))?;
        if let NetworkMessage::GetHeaders(_) = client.recv()? {
            client.send(NetworkMessage::Headers(vec![]))?;
        }
        Ok(client)
    }

    fn send(&mut self, msg: NetworkMessage) -> Result<()> {
        let raw_msg = message::RawNetworkMessage {
            magic: self.network.magic(),
            payload: msg,
        };
        self.stream
            .write_all(encode::serialize(&raw_msg).as_slice())
            .context("p2p failed to send")
    }

    fn recv(&mut self) -> Result<NetworkMessage> {
        loop {
            let raw_msg: message::RawNetworkMessage =
                self.reader.read_next().context("p2p failed to recv")?;

            // println!("Received: {:?}", raw_msg.payload);
            match raw_msg.payload {
                NetworkMessage::Version(_) => {
                    self.send(NetworkMessage::Verack)?;
                    continue;
                }
                NetworkMessage::Ping(nonce) => {
                    self.send(NetworkMessage::Pong(nonce))?;
                    continue;
                }
                NetworkMessage::Verack | NetworkMessage::Alert(_) | NetworkMessage::Addr(_) => {
                    continue
                }
                payload => return Ok(payload),
            }
        }
    }

    fn recv_skip_inv(&mut self) -> Result<NetworkMessage> {
        loop {
            match self.recv()? {
                NetworkMessage::Inv(_) => continue,
                msg => return Ok(msg),
            }
        }
    }

    fn shutdown(self) -> Result<()> {
        self.stream
            .shutdown(Shutdown::Both)
            .context("p2p failed to shutdown")
    }
}

fn build_version_message(address: SocketAddr) -> NetworkMessage {
    let my_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
    let services = constants::ServiceFlags::NETWORK;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time error")
        .as_secs() as i64;

    let addr_recv = address::Address::new(&address, constants::ServiceFlags::NETWORK);
    let addr_from = address::Address::new(&my_address, constants::ServiceFlags::NETWORK);
    let nonce = secp256k1::rand::thread_rng().gen();
    let user_agent = String::from("rust-example");
    let start_height = 0i32;

    NetworkMessage::Version(message_network::VersionMessage::new(
        services,
        timestamp,
        addr_recv,
        addr_from,
        nonce,
        user_agent,
        start_height,
    ))
}

struct NewHeader {
    header: BlockHeader,
    hash: BlockHash,
    height: usize,
}

impl NewHeader {
    fn from((header, height): (BlockHeader, usize)) -> Self {
        Self {
            header,
            hash: header.block_hash(),
            height,
        }
    }
}

struct Chain {
    headers: Vec<(BlockHash, BlockHeader)>,
    heights: HashMap<BlockHash, usize>,
}

impl Chain {
    fn new(genesis: BlockHeader) -> Self {
        assert_eq!(genesis.prev_blockhash, BlockHash::default());
        Self {
            headers: vec![(genesis.block_hash(), genesis)],
            heights: std::iter::once((genesis.block_hash(), 0)).collect(),
        }
    }

    pub(crate) fn update(&mut self, headers: Vec<NewHeader>) {
        if let Some(first_height) = headers.first().map(|h| h.height) {
            for (hash, _header) in self.headers.drain(first_height..) {
                assert!(self.heights.remove(&hash).is_some());
            }
            for (h, height) in headers.into_iter().zip(first_height..) {
                assert_eq!(h.height, height);
                assert_eq!(h.hash, h.header.block_hash());
                assert!(self.heights.insert(h.hash, h.height).is_none());
                self.headers.push((h.hash, h.header));
            }
        }
    }

    fn locator(&self) -> Vec<BlockHash> {
        let mut result = vec![];
        let mut index = self.headers.len() - 1;
        let mut step = 1;
        while index > 0 {
            if result.len() >= 10 {
                step *= 2;
            }
            result.push(self.headers[index].0);
            index = index.saturating_sub(step);
        }
        assert_eq!(index, 0);
        result.push(self.headers[index].0);
        println!("locator: {:?}", result);
        result
    }
}

struct Syncer {
    chain: Chain,
    client: Client,
}

impl Syncer {
    fn new(network: constants::Network) -> Result<Self> {
        let (address, genesis_hash) = match network {
            constants::Network::Bitcoin => (
                "127.0.0.1:8333",
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            ),
            constants::Network::Testnet => (
                "127.0.0.1:18333",
                "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943",
            ),
            constants::Network::Regtest => (
                "127.0.0.1:18444",
                "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
            ),
            constants::Network::Signet => (
                "127.0.0.1:38333",
                "00000008819873e925422c1ff0f99f7cc9bbb232af63a077a480a3633bee1ef6",
            ),
        };
        let genesis_hash = BlockHash::from_hex(genesis_hash).expect("invalid genesis block hash");
        let mut client = Client::connect(address.parse()?, network)?;
        let genesis_block = match get_blocks(&mut client, &[genesis_hash])?.next() {
            Some(Ok(block)) => block,
            _ => bail!("no genesis block"),
        };
        let chain = Chain::new(genesis_block.header);
        Ok(Syncer { client, chain })
    }

    pub(crate) fn get_new_headers(&mut self) -> Result<Vec<NewHeader>> {
        let msg = GetHeadersMessage::new(self.chain.locator(), BlockHash::default());
        self.client.send(NetworkMessage::GetHeaders(msg))?;
        let headers = match self.client.recv_skip_inv()? {
            NetworkMessage::Headers(headers) => headers,
            msg => bail!("unexpected {:?}", msg),
        };
        debug!("got {} new headers", headers.len());
        let prev_blockhash = match headers.first().map(|h| h.prev_blockhash) {
            None => return Ok(vec![]),
            Some(prev_blockhash) => prev_blockhash,
        };
        let new_heights = match self.chain.heights.get(&prev_blockhash) {
            Some(last_height) => (last_height + 1)..,
            None => bail!("missing prev_blockhash: {}", prev_blockhash),
        };
        Ok(headers
            .into_iter()
            .zip(new_heights)
            .map(NewHeader::from)
            .collect())
    }

    fn wait_for_inv(&mut self) -> Result<()> {
        loop {
            match self.client.recv()? {
                NetworkMessage::Inv(_) => return Ok(()),
                msg => bail!("unexpected: {:?}", msg),
            }
        }
    }

    pub(crate) fn get_blocks<'a>(
        &'a mut self,
        blockhashes: &'a [BlockHash],
    ) -> Result<BlockDownload<'a>> {
        get_blocks(&mut self.client, blockhashes)
    }
}

fn get_blocks<'a>(
    client: &'a mut Client,
    blockhashes: &'a [BlockHash],
) -> Result<BlockDownload<'a>> {
    client.send(NetworkMessage::GetData(
        blockhashes
            .iter()
            .map(|h| Inventory::WitnessBlock(*h))
            .collect(),
    ))?;
    Ok(BlockDownload {
        client,
        blockhashes,
        index: 0,
    })
}

struct BlockDownload<'a> {
    client: &'a mut Client,
    blockhashes: &'a [BlockHash],
    index: usize,
}

impl<'a> BlockDownload<'a> {
    fn recv_next_block(&mut self) -> Result<Block> {
        loop {
            match self.client.recv_skip_inv()? {
                NetworkMessage::Block(block) => {
                    assert_eq!(block.block_hash(), self.blockhashes[self.index]);
                    self.index += 1;
                    return Ok(block);
                }
                _ => continue,
            }
        }
    }
}

impl<'a> Iterator for BlockDownload<'a> {
    type Item = Result<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.blockhashes.len() {
            return None;
        }
        Some(self.recv_next_block())
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("not enough arguments");
        process::exit(1);
    }
    let network_str: &str = &args[1];
    let network = match network_str {
        "bitcoin" => constants::Network::Bitcoin,
        "testnet" => constants::Network::Testnet,
        "regtest" => constants::Network::Regtest,
        "signet" => constants::Network::Signet,
        _ => bail!("unsupported network {}", network_str),
    };

    let mut syncer = Syncer::new(network)?;
    let mut size = 0;
    let mut blocks = 0;
    let mut txs = 0;
    loop {
        let headers = syncer.get_new_headers()?;
        if headers.is_empty() {
            syncer.wait_for_inv()?;
            continue;
        }
        let blockhashes: Vec<_> = headers.iter().map(|h| h.hash).collect();
        for result in syncer.get_blocks(&blockhashes)? {
            let block = result?;
            size += block.get_size();
            txs += block.txdata.len();
            blocks += 1;
        }
        syncer.update(headers);
        println!(
            "blocks: {}, txs: {}, size: {:.3} MB",
            blocks,
            txs,
            size as f64 / 1e6
        );
    }
    // syncer.client.shutdown()
}
