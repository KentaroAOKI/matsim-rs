#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="/home/kentaro/repos/matsim-example-project/scenarios/equil/config-benchmark.xml"
RUST_OUT="/home/kentaro/repos/matsim-example-project/scenarios/equil/output-benchmark"
JAVA_OUT="/home/kentaro/repos/matsim-example-project/output-benchmark-inmem"

cd "$ROOT"
cargo run -p matsim-cli -- run --config "$CONFIG"
cargo run -p matsim-cli -- compare --left "$JAVA_OUT" --right "$RUST_OUT"
