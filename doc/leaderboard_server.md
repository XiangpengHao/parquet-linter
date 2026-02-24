# Leaderboard Benchmark Server

`parquet-leaderboard` supports a server mode for LLM-driven prescription evaluation.

In this mode, the process exposes APIs that:
- list available parquet benchmark files
- return file column metadata (`ColumnContext`-like JSON view)
- evaluate a prescription on a selected file

The server is intended to be the execution backend for a model loop:
1. client fetches file info
2. model generates a prescription
3. server rewrites parquet in memory and returns benchmark results

## Start Server

```bash
cargo run -p parquet-linter-leaderboard -- --server --listen 127.0.0.1:3000
```

Notes:
- file list comes from `doc/parquet_files.txt` (or `--parquet-manifest`)
- parquet files are cached under `--data-dir` (default: `data`)
- decode benchmark runs `--iterations` times (default: `3`) and returns the minimum time

## API

### `GET /list`

Returns all parquet files in the manifest.

Each item includes:
- `id`
- `url`
- `downloaded`
- `local_size_bytes` (if downloaded)

Example:

```bash
curl http://127.0.0.1:3000/list
```

### `GET /info/:id`

Returns metadata for one parquet file, including a JSON-friendly view of column contexts.

Behavior:
- lazily downloads the parquet file if missing locally
- returns per-column stats (types, null/distinct counts, sizes, ratios, summarized `type_stats`)

Example:

```bash
curl http://127.0.0.1:3000/info/0
```

### `POST /eval`

Evaluates a prescription for a parquet file.

Request body:

```json
{
  "id": 0,
  "prescription": "set file compression snappy"
}
```

Behavior:
- parses the prescription
- rewrites the parquet in memory (no output file written)
- validates Arrow schema is unchanged
- measures rewritten size
- runs decode benchmark 3 times (or `--iterations`) and returns min decode time

Response includes:
- `size_bytes`
- `size_mb`
- `min_decoding_time_ms`
- `cost` (`size_mb + min_decoding_time_ms`)
- `directive_count`
- `conflict_warning` (present when directives conflict; last directive still wins)

Example:

```bash
curl -X POST http://127.0.0.1:3000/eval \
  -H 'content-type: application/json' \
  -d '{"id":0,"prescription":"set file compression snappy"}'
```

## Errors

- `400 Bad Request`: invalid prescription syntax
- `404 Not Found`: unknown parquet file id
- `500 Internal Server Error`: download/rewrite/benchmark/schema-check failures
