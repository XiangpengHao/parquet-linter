# parquet-lint

A linter for Parquet files. Analyzes metadata to detect suboptimal encoding, compression, and configuration, then optionally rewrites files with fixes applied.

## Usage

```bash
# Check for issues
parquet-lint check data.parquet

# Check with filters
parquet-lint check data.parquet --severity warning --rules low-compression-ratio,float-byte-stream-split

# Fix issues (rewrite file)
parquet-lint fix data.parquet -o fixed.parquet

# Preview fixes without writing
parquet-lint fix data.parquet -o fixed.parquet --dry-run
```

## Rules

| Rule | Severity | What it detects |
|------|----------|-----------------|
| `low-compression-ratio` | warning | Compressed size > 95% of uncompressed |
| `missing-page-statistics` | warning | No page-level column index |
| `vector-embedding-page-size` | warning | Float list columns with high values/row (embeddings) using large pages |
| `dictionary-encoding-cardinality` | warning/info | Dictionary fallback on high cardinality, or missing dictionary on low cardinality |
| `page-row-group-size` | warning | Row groups < 32 MB or > 512 MB |
| `float-byte-stream-split` | info | Scalar float columns using PLAIN instead of BYTE_STREAM_SPLIT |
| `sorted-integer-delta` | info | Sorted integer columns not using DELTA_BINARY_PACKED |
| `bloom-filter-recommendation` | info | High-cardinality byte array / UUID columns without bloom filters |
| `compression-codec-upgrade` | info | GZIP or deprecated LZ4 instead of ZSTD |
| `timestamp-delta-encoding` | info | Timestamp/date columns using PLAIN instead of DELTA_BINARY_PACKED |
| `oversized-string-statistics` | warning | Untruncated string statistics > 64 bytes |

## Build

```bash
cargo build --release
```
