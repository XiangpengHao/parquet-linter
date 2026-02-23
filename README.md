# parquet-linter

[![Crates.io](https://img.shields.io/crates/v/parquet-linter)](https://crates.io/crates/parquet-linter)
[![docs.rs](https://img.shields.io/docsrs/parquet-linter)](https://docs.rs/parquet-linter)

Unleash the performance potential of your Parquet files. 
Checkout our [blog post](https://blog.xiangpeng.systems/posts/parquet-linter/) for more details.

<img src="./doc/teaser.png" width="60%">

## Install

```bash
cargo install parquet-linter-cli
```

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

### Apply at write time

You can also apply a prescription when initially writing Parquet files, avoiding a rewrite entirely. 

```rust
use parquet::file::properties::WriterProperties;
use parquet_linter::prescription::LinterPrescriptionExt;

let props = WriterProperties::builder()
    .apply_prescription("set file compression zstd(3)\nset column user_id encoding delta_binary_packed")
    .expect("valid prescription")
    .build();

// Use `props` with ArrowWriter, AsyncArrowWriter, etc.
```

## Leaderboard

We track two metrics separately:

- `decode time (ms)`: time to convert a Parquet file into Arrow `RecordBatch`es
- `file size (MB)`: size of the Parquet file on disk

Parquet files are listed in `doc/parquet_files.txt`. They are unmodified Parquet datasets from Hugging Face.

### Current Results

All rules enabled. Page statistics are enabled (required). Can you do better?

#### File Size Leaderboard (MB, lower is better)

| | File 0 | File 1 | File 2 | File 3 | File 4 | File 5 | File 6 | **Total** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| HuggingFace default | 171.38 | 137.88 | 105.55 | 88.78 | 145.97 | 62.59 | 247.95 | **960.10** |
| **parquet-linter** | 171.35 (-0.02%) | 128.31 (-6.94%) | 90.10 (-14.64%) | 71.70 (-19.24%) | 141.41 (-3.12%) | 58.08 (-7.21%) | 244.73 (-1.30%) | **905.68 (-5.67%)** |

#### Decode Time Leaderboard (ms, lower is better)

| | File 0 | File 1 | File 2 | File 3 | File 4 | File 5 | File 6 | **Total** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| HuggingFace default | 243.87 | 3291.04 | 2393.66 | 159.81 | 2652.84 | 1220.01 | 8262.75 | **18223.97** |
| **parquet-linter** | 240.18 (-1.51%) | 2533.81 (-23.01%) | 1959.87 (-18.12%) | 22.52 (-85.91%) | 1887.88 (-28.84%) | 951.93 (-21.97%) | 7576.31 (-8.31%) | **15172.50 (-16.74%)** |

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


### Limitations

The current leaderboard does not show filter-pushdown results, therefore not reflecting benefits of zone maps and smaller page sizes.
