#!/bin/sh
# Build both test peers before starting any network processes.
set -eu
cd "$(dirname "$0")/.."
mode=${1:-all}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cargo build --locked -p rpc-interop
(cd interop/go && GOWORK=off go build -mod=readonly -o "$tmp/go-peer" .)
rust_bin=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"]+"/debug/rpc-interop")')
python3 interop/run.py "$mode" "$tmp/go-peer" "$rust_bin" "$tmp"
