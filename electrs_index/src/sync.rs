use anyhow::{Context, Result};

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};

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

use crate::chain::{Chain, NewHeader};

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
        if let NetworkMessage::GetHeaders(_) = client.recv_skip_inv()? {
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
    let user_agent = String::from("electrs");
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

pub struct Syncer {
    genesis_header: BlockHeader,
    client: Client,
}

impl Syncer {
    pub fn new(network: constants::Network) -> Result<Self> {
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

        client.send(NetworkMessage::GetData(vec![Inventory::WitnessBlock(
            genesis_hash,
        )]))?;
        let genesis_header = match client.recv_skip_inv()? {
            NetworkMessage::Block(b) => b.header,
            msg => bail!("unexpected {:?}", msg),
        };

        Ok(Syncer {
            client,
            genesis_header,
        })
    }

    pub(crate) fn new_chain(&self) -> Chain {
        Chain::new(self.genesis_header)
    }

    pub(crate) fn load_chain(&self, headers: Vec<BlockHeader>, tip: BlockHash) -> Chain {
        let mut result = self.new_chain();
        result.load(headers, tip);
        result
    }

    pub(crate) fn get_new_headers(&mut self, chain: &Chain) -> Result<Vec<NewHeader>> {
        let msg = GetHeadersMessage::new(chain.locator(), BlockHash::default());
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
        let new_heights = match chain.get_block_height(&prev_blockhash) {
            Some(last_height) => (last_height + 1)..,
            None => bail!("missing prev_blockhash: {}", prev_blockhash),
        };
        Ok(headers
            .into_iter()
            .zip(new_heights)
            .map(NewHeader::from)
            .collect())
    }

    pub fn wait_for_inv(&mut self) -> Result<()> {
        loop {
            match self.client.recv()? {
                NetworkMessage::Inv(_) => return Ok(()),
                msg => bail!("unexpected: {:?}", msg),
            }
        }
    }

    pub fn for_blocks<F>(&mut self, blockhashes: &[BlockHash], mut f: F) -> Result<()>
    where
        F: FnMut(Block),
    {
        self.client.send(NetworkMessage::GetData(
            blockhashes
                .iter()
                .map(|h| Inventory::WitnessBlock(*h))
                .collect(),
        ))?;
        for h in blockhashes {
            match self.client.recv_skip_inv().expect("failed to recv block") {
                NetworkMessage::Block(block) => {
                    assert_eq!(block.block_hash(), *h);
                    f(block);
                }
                msg => panic!("unexpected {:?}", msg),
            };
        }
        Ok(())
    }
}
