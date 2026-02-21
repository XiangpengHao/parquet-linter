# Cardinality Estimation

This document describes the current `src/cardinality.rs` behavior.

## Goals

- Keep estimation lightweight and fast.
- Avoid full-file scans unless absolutely necessary.
- Provide stable enough signals for lint rules (not exact NDV).

## Current Heuristic (3 Tiers)

For each column, we estimate cardinality using exactly one sampled row group.

### Row group selection

- Pick the first row group with `num_rows > 0`.
- If none match, fall back to row group `0`.

### Tier 1: Column statistics distinct count

- Read `statistics.distinct_count` from the sampled row group column.
- If present, treat as sampled-row-group NDV.
- Scale to file-level NDV using ratio:
  - `sample_distinct / sample_total_values`
  - multiplied by file-level `total_values` for the column.

This is the cheapest path and preferred when available.

### Tier 2: Dictionary-page inference

- If Tier 1 is missing, inspect the sampled row group column pages.
- If a dictionary page is found, use dictionary entry count as sampled NDV.
- Stop once data pages are reached.
- Scale the sampled NDV to file level (same ratio scaling as Tier 1).

This is still lightweight and avoids materializing values.

### Tier 3: Value sampling fallback

- Used only for unresolved columns.
- Only for flat schemas (same existing constraint as before).
- Read up to `SAMPLE_ROWS` (currently `16_384`) from the sampled row group.
- Hash sampled values and count unique hashes.
- Scale sampled distinct ratio to file-level total values.

This is the most expensive tier, so it is intentionally late and narrow.

## Conservatism / Final Fallback

- If a column still cannot be estimated, assume all values are distinct:
  - `distinct_count = total_count`.

This avoids false low-cardinality recommendations.

## Why one-row-group sampling

- Predictable and bounded cost.
- Fits linter usage where speed matters more than exact NDV.
- Captures enough signal for rules like dictionary, bloom filter, and encoding recommendations.

## Known limitations

- If row groups are highly non-uniform, one-row-group scaling can misestimate file-level NDV.
- Dictionary/page metadata can be missing or writer-dependent.
- Hash-based Tier 3 counts hash-distinct values (low collision risk, not exact).

## Tuning knobs

- `SAMPLE_ROWS` controls Tier 3 cost/quality.
- Row group selection strategy can be changed later (e.g., largest non-empty row group) if needed.
