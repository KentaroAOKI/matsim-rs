# matsim-rs

`matsim-rs` is a new Rust workspace for rebuilding the MATSim core incrementally.

Current scope:

- read MATSim `config.xml`, `network.xml`, and `plans.xml(.gz)`
- load selected plans into a Rust internal model
- run a deterministic single-iteration summary over the loaded scenario
- write `scorestats.csv`, `modestats.csv`, and `traveldistancestats.csv`

This is not yet a full MATSim replacement. The current implementation is the first executable milestone for the `equil` scenario.

## Usage

```bash
cargo run -p matsim-cli -- run --config /path/to/config.xml
```

## Example

The default comparison target is the `equil` scenario shipped in `matsim-libs`.

```bash
./scripts/compare-with-java.sh
```

