# parquet-linter

A linter for Parquet files. Analyzes metadata to detect suboptimal encoding, compression, and configuration, then optionally rewrites files with fixes applied.

## Usage

```bash
# Check a local file 
parquet-linter data.parquet

# Check a remote file
parquet-linter https://huggingface.co/datasets/open-r1/OpenR1-Math-220k/resolve/main/data/train-00003-of-00010.parquet 

# Fix issues
parquet-linter fix data.parquet -o fixed.parquet

# Dry run 
parquet-linter fix data.parquet -o fixed.parquet --dry-run
```

## Build

```bash
cargo build --release
```
