# parquet-linter

Unleash the performance potential of your Parquet files.

## Usage

```bash
# Lint a local file 
parquet-linter data.parquet

# Lint a remote file
parquet-linter https://huggingface.co/datasets/open-r1/OpenR1-Math-220k/resolve/main/data/train-00003-of-00010.parquet 

# Rewrite using lint results
parquet-linter rewrite data.parquet -o fixed.parquet

# Dry run 
parquet-linter rewrite data.parquet -o fixed.parquet --dry-run
```

## Prescriptions

`parquet-linter` use a little DSL (prescription) to describe what optimizations to apply:

```
set column stack_trace.list.item compression lz4_raw
set column submit_ts_ns compression uncompressed
set column next_pid compression zstd(3)
set column ts_ns dictionary_page_size_limit 2097152
set file max_row_group_size 65536
set file data_page_size_limit 1048576
```

This allows you to sample a file, and then apply the prescription to other files.

You can do this by:
```bash
parquet-linter data.parquet --export-prescription prescription.txt
```

Then apply it to another file:
```bash
parquet-linter rewrite other.parquet -o rewritten.parquet --from-prescription prescription.txt
```

## Build

```bash
cargo build --release
```

## Leaderboard

Let's see how much we can improve existing parquet files, we define cost as `cost = loading_time_ms + file_size_mb`

Loading time is the time it takes to convert parquet file to arrow RecordBatch.
File size is the size of the parquet file as reported by file system.

#### Benchmark `parquet-linter`

```bash
cargo build --release --package parquet-linter-leaderboard
./target/release/parquet-leaderboard --from-linter --iterations 3
```

#### Benchmark your own prescriptions

Create a directory of numbered files (`0.prescription`, `1.prescription`, ...) matching the manifest order, then run:

```bash
./target/release/parquet-leaderboard --from-custom-prescription prescriptions --iterations 3
```

### Current Results (By `parquet-linter`)

| File | Before Cost | New Cost | Change |
|---|---:|---:|---:|
| 0 | 440.30 | 461.93 | +4.91% |
| 1 | 453.76 | 352.71 | -22.27% |
| 2 | 391.16 | 313.62 | -19.82% |
| 3 | 123.37 | 139.48 | +13.05% |
| 4 | 433.62 | 382.74 | -11.73% |
| 5 | 151.27 | 130.96 | -13.43% |
| 6 | 644.52 | 622.04 | -3.49% |
| total | 2638.01 | 2403.48 | -8.89% |
