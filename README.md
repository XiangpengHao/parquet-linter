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

## Leaderboard

We define `cost = loading_time_ms + file_size_mb`

Loading time is the time it takes to convert parquet file to arrow RecordBatch.
File size is the size of the parquet file as reported by file system.

Parquet files are listed in `doc/parquet_files.txt`. They are unmodified Parquet datasets from Hugging Face.

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

All rules enabled. Page statistics are enabled (required).

| File | Old cost | New cost | Change |
|---|---:|---:|---:|
| 0 | 447.24 | 440.95 | -1.41% |
| 1 | 461.83 | 362.99 | -21.40% |
| 2 | 399.16 | 305.73 | -23.41% |
| 3 | 123.56 | 119.83 | -3.02% |
| 4 | 446.94 | 375.05 | -16.08% |
| 5 | 150.86 | 114.19 | -24.31% |
| 6 | 642.86 | 594.70 | -7.49% |
| total | 2672.45 | 2313.43 | -13.43% |

### Limitations

The current leaderboard does not show filter-pushdown results, therefore not reflecting benefits of zone maps and smaller page sizes.
