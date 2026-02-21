use anyhow::Result;
use arrow_array::*;
use parquet::column::page::Page;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::HashSet;
use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;

pub struct ColumnCardinality {
    pub distinct_count: u64,
    pub total_count: u64,
}

impl ColumnCardinality {
    pub fn ratio(&self) -> f64 {
        if self.total_count == 0 {
            1.0
        } else {
            self.distinct_count as f64 / self.total_count as f64
        }
    }
}

const SAMPLE_ROWS: usize = 16_384;

/// Estimate per-column cardinality using a lightweight 3-tier approach:
/// 1. Distinct count from one row group's column statistics
/// 2. Distinct count inferred from one row group's dictionary page
/// 3. Sample values from one row group and estimate file-level ratio
pub fn estimate(
    path: &Path,
    reader: &SerializedFileReader<File>,
    metadata: &ParquetMetaData,
) -> Result<Vec<ColumnCardinality>> {
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    if metadata.num_row_groups() == 0 {
        return Ok(Vec::new());
    }

    let sample_rg_idx = pick_sample_row_group(metadata);
    let totals = total_values_per_column(metadata, num_cols);
    let mut result: Vec<Option<ColumnCardinality>> = (0..num_cols).map(|_| None).collect();

    for col_idx in 0..num_cols {
        let total = totals[col_idx];
        let sample_col = metadata.row_group(sample_rg_idx).column(col_idx);
        let sample_total = sample_col.num_values() as u64;
        if sample_total == 0 {
            continue;
        }

        // Tier 1: statistics
        if let Some(dc) = sample_col
            .statistics()
            .and_then(|s| s.distinct_count_opt())
            .map(|dc| dc.min(sample_total))
        {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: scale_distinct(dc, sample_total, total),
                total_count: total,
            });
            continue;
        }

        // Tier 2: dictionary page
        if let Some(dc) = dictionary_distinct_count(reader, sample_rg_idx, col_idx) {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: scale_distinct(dc.min(sample_total), sample_total, total).max(dc),
                total_count: total,
            });
        }
    }

    // Tier 3: sample unresolved flat columns only.
    let schema = metadata.file_metadata().schema_descr();
    let is_flat = schema.root_schema().get_fields().len() == num_cols;
    if is_flat {
        let unresolved: Vec<usize> = (0..num_cols).filter(|&i| result[i].is_none()).collect();
        if !unresolved.is_empty() {
            sample_cardinalities(path, &totals, sample_rg_idx, &unresolved, &mut result)?;
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
                total_count: total,
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

fn total_values_per_column(metadata: &ParquetMetaData, num_cols: usize) -> Vec<u64> {
    let mut totals = vec![0u64; num_cols];
    for rg in metadata.row_groups() {
        for (col_idx, total) in totals.iter_mut().enumerate() {
            *total += rg.column(col_idx).num_values() as u64;
        }
    }
    totals
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

/// Read one row group's dictionary page and return dictionary entry count.
fn dictionary_distinct_count(
    reader: &SerializedFileReader<File>,
    rg_idx: usize,
    col_idx: usize,
) -> Option<u64> {
    let rg_reader = reader.get_row_group(rg_idx).ok()?;
    let mut page_reader = rg_reader.get_column_page_reader(col_idx).ok()?;

    while let Ok(Some(page)) = page_reader.get_next_page() {
        match page {
            Page::DictionaryPage { num_values, .. } => return Some(num_values as u64),
            Page::DataPage { .. } | Page::DataPageV2 { .. } => break,
        }
    }
    None
}

fn sample_cardinalities(
    path: &Path,
    totals: &[u64],
    sample_rg_idx: usize,
    columns: &[usize],
    result: &mut [Option<ColumnCardinality>],
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder
        .with_row_groups(vec![sample_rg_idx])
        .with_batch_size(SAMPLE_ROWS)
        .with_limit(SAMPLE_ROWS)
        .build()?;

    let mut sets: Vec<HashSet<u64>> = vec![HashSet::new(); columns.len()];
    let mut sample_rows = 0u64;

    for batch_result in reader {
        let batch = batch_result?;
        for (i, &col_idx) in columns.iter().enumerate() {
            hash_array_values(batch.column(col_idx).as_ref(), &mut sets[i]);
        }
        sample_rows += batch.num_rows() as u64;
    }

    if sample_rows == 0 {
        return Ok(());
    }

    for (i, &col_idx) in columns.iter().enumerate() {
        let sample_distinct = sets[i].len() as u64;
        let total = totals[col_idx];
        let estimated = scale_distinct(sample_distinct, sample_rows, total);

        let sampled = estimated.max(sample_distinct).min(total);
        if let Some(existing) = result[col_idx].as_mut() {
            existing.distinct_count = existing.distinct_count.max(sampled).min(total);
        } else {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: sampled,
                total_count: total,
            });
        }
    }

    Ok(())
}

fn hash_array_values(array: &dyn Array, set: &mut HashSet<u64>) {
    for i in 0..array.len() {
        if array.is_null(i) {
            set.insert(u64::MAX);
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
