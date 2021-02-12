#!/bin/bash
set -eux
cd `dirname $0`

cargo fmt
cargo build -p electrs_query --release

NETWORK=$1
shift

QUERY="target/release/electrs_query --network $NETWORK --db-dir ./db2 --daemon-dir $HOME/.bitcoin"
export RUST_LOG=${RUST_LOG-info}
$QUERY $*

# use SIGINT to quit
