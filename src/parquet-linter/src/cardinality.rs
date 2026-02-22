use anyhow::Result;
use arrow_array::*;
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::column::page::Page;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::rule;

pub struct ColumnCardinality {
    pub distinct_count: u64,
    pub non_null_count: u64,
}

impl ColumnCardinality {
    pub fn ratio(&self) -> f64 {
        if self.non_null_count == 0 {
            0.0
        } else {
            self.distinct_count as f64 / self.non_null_count as f64
        }
    }
}

const SAMPLE_ROWS: usize = 16_384;

/// Estimate per-column cardinality using a lightweight 3-tier approach:
/// 1. Distinct count from one row group's column statistics
/// 2. Distinct count inferred from one row group's dictionary page
/// 3. Sample values from one row group and estimate file-level ratio
pub async fn estimate(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
) -> Result<Vec<ColumnCardinality>> {
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    if metadata.num_row_groups() == 0 {
        return Ok(Vec::new());
    }

    let sample_rg_idx = pick_sample_row_group(metadata);
    let totals = total_non_null_values_per_column(metadata, num_cols);
    let mut result: Vec<Option<ColumnCardinality>> = (0..num_cols).map(|_| None).collect();

    for col_idx in 0..num_cols {
        let total_non_null = totals[col_idx];
        let sample_col = metadata.row_group(sample_rg_idx).column(col_idx);
        let sample_non_null = column_non_null_count(sample_col);
        if sample_non_null == 0 {
            continue;
        }

        // Tier 1: statistics
        if let Some(dc) = sample_col
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .map(|dc| dc.min(sample_non_null))
        {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: scale_distinct(dc, sample_non_null, total_non_null),
                non_null_count: total_non_null,
            });
            continue;
        }

        // Tier 2: dictionary page (fetches only this column chunk's bytes)
        if let Some(dc) = dictionary_distinct_count(reader, metadata, sample_rg_idx, col_idx).await
        {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: scale_distinct(
                    dc.min(sample_non_null),
                    sample_non_null,
                    total_non_null,
                )
                .max(dc.min(total_non_null)),
                non_null_count: total_non_null,
            });
        }
    }

    // Tier 3: sample unresolved flat columns only.
    let schema = metadata.file_metadata().schema_descr();
    let is_flat = schema.root_schema().get_fields().len() == num_cols;
    if is_flat {
        let unresolved: Vec<usize> = (0..num_cols).filter(|&i| result[i].is_none()).collect();
        if !unresolved.is_empty() {
            sample_cardinalities(
                reader,
                metadata,
                &totals,
                sample_rg_idx,
                &unresolved,
                &mut result,
            )
            .await?;
        }
    }

    // Fill unknowns: assume all unique (conservative)
    Ok(result
        .into_iter()
        .enumerate()
        .map(|(col_idx, card)| {
            let total = totals[col_idx];
            card.unwrap_or(ColumnCardinality {
                distinct_count: total,
                non_null_count: total,
            })
        })
        .collect())
}

fn pick_sample_row_group(metadata: &ParquetMetaData) -> usize {
    metadata
        .row_groups()
        .iter()
        .position(|rg| rg.num_rows() > 0)
        .unwrap_or(0)
}

fn total_non_null_values_per_column(metadata: &ParquetMetaData, num_cols: usize) -> Vec<u64> {
    let mut totals = vec![0u64; num_cols];
    for rg in metadata.row_groups() {
        for (col_idx, total) in totals.iter_mut().enumerate() {
            *total += column_non_null_count(rg.column(col_idx));
        }
    }
    totals
}

fn column_non_null_count(col: &ColumnChunkMetaData) -> u64 {
    if col.num_values() <= 0 {
        return 0;
    }

    let total = col.num_values() as u64;
    let null_count = col
        .statistics()
        .and_then(|statistics| statistics.null_count_opt())
        .unwrap_or(0)
        .min(total);
    total.saturating_sub(null_count)
}

fn scale_distinct(sample_distinct: u64, sample_total: u64, full_total: u64) -> u64 {
    if sample_total == 0 || full_total == 0 {
        return full_total;
    }

    let ratio = sample_distinct as f64 / sample_total as f64;
    ((ratio * full_total as f64) as u64)
        .max(sample_distinct)
        .min(full_total)
}

/// Read one row group's dictionary page via a targeted byte-range fetch.
async fn dictionary_distinct_count(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
    rg_idx: usize,
    col_idx: usize,
) -> Option<u64> {
    let mut page_reader = rule::column_page_reader(reader, metadata, rg_idx, col_idx)
        .await
        .ok()?;
    use parquet::column::page::PageReader;
    if let Ok(Some(page)) = page_reader.get_next_page() {
        match page {
            Page::DictionaryPage { num_values, .. } => return Some(num_values as u64),
            Page::DataPage { .. } | Page::DataPageV2 { .. } => {}
        }
    }
    None
}

async fn sample_cardinalities(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
    non_null_totals: &[u64],
    sample_rg_idx: usize,
    columns: &[usize],
    result: &mut [Option<ColumnCardinality>],
) -> Result<()> {
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

    let builder = ParquetRecordBatchStreamBuilder::new(reader.clone())
        .await?
        .with_row_groups(vec![sample_rg_idx])
        .with_batch_size(SAMPLE_ROWS)
        .with_limit(SAMPLE_ROWS);

    // Project only the columns we need
    let mask = parquet::arrow::ProjectionMask::leaves(
        metadata.file_metadata().schema_descr(),
        columns.iter().copied(),
    );
    let mut stream = builder.with_projection(mask).build()?;

    let mut sets: Vec<HashSet<u64>> = vec![HashSet::new(); columns.len()];
    let mut sample_non_null_counts = vec![0u64; columns.len()];

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        // Projected batch columns are in order of `columns`
        for (i, _col_idx) in columns.iter().enumerate() {
            let array = batch.column(i).as_ref();
            hash_array_values(array, &mut sets[i]);
            sample_non_null_counts[i] += (array.len() - array.null_count()) as u64;
        }
    }

    if sample_non_null_counts.iter().all(|count| *count == 0) {
        return Ok(());
    }

    for (i, &col_idx) in columns.iter().enumerate() {
        let sample_non_null = sample_non_null_counts[i];
        if sample_non_null == 0 {
            continue;
        }

        let sample_distinct = sets[i].len() as u64;
        let total_non_null = non_null_totals[col_idx];
        let estimated = scale_distinct(sample_distinct, sample_non_null, total_non_null);

        let sampled = estimated.max(sample_distinct).min(total_non_null);
        if let Some(existing) = result[col_idx].as_mut() {
            existing.distinct_count = existing.distinct_count.max(sampled).min(total_non_null);
        } else {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: sampled,
                non_null_count: total_non_null,
            });
        }
    }

    Ok(())
}

fn hash_array_values(array: &dyn Array, set: &mut HashSet<u64>) {
    for i in 0..array.len() {
        if array.is_null(i) {
            continue;
        }
        let mut hasher = DefaultHasher::new();
        hash_value(array, i, &mut hasher);
        set.insert(hasher.finish());
    }
}

fn hash_value(array: &dyn Array, i: usize, hasher: &mut impl Hasher) {
    let any = array.as_any();

    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Int8Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Int16Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Int32Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<UInt8Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<UInt16Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<UInt32Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<UInt64Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Float32Array>() {
        a.value(i).to_bits().hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Float64Array>() {
        a.value(i).to_bits().hash(hasher);
    } else if let Some(a) = any.downcast_ref::<StringArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<LargeStringArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<LargeBinaryArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Date32Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Date64Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<TimestampSecondArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<Decimal128Array>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<FixedSizeBinaryArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<StringViewArray>() {
        a.value(i).hash(hasher);
    } else if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
        a.value(i).hash(hasher);
    } else {
        // Unknown type: treat each value as unique
        i.hash(hasher);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_is_zero_when_column_has_no_non_null_values() {
        let card = ColumnCardinality {
            distinct_count: 0,
            non_null_count: 0,
        };
        assert_eq!(card.ratio(), 0.0);
    }

    #[test]
    fn sampling_distinct_ignores_null_values() {
        let array = StringArray::from(vec![Some("a"), None, Some("a"), None]);
        let mut set = HashSet::new();
        hash_array_values(&array, &mut set);
        assert_eq!(set.len(), 1);
    }
}
