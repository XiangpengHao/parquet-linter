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

/// Estimate per-column cardinality using a 3-tier approach:
/// 1. Parquet statistics `distinct_count` (exact when available)
/// 2. Dictionary page entry count (exact for that row group, lower bound for file)
/// 3. Sample first 16k rows and count distinct values
pub fn estimate(
    path: &Path,
    reader: &SerializedFileReader<File>,
    metadata: &ParquetMetaData,
) -> Result<Vec<ColumnCardinality>> {
    let num_cols = metadata.file_metadata().schema_descr().num_columns();
    let mut result: Vec<Option<ColumnCardinality>> = (0..num_cols).map(|_| None).collect();

    for col_idx in 0..num_cols {
        let total = total_values(metadata, col_idx);

        // Tier 1: statistics
        let dc = metadata
            .row_groups()
            .iter()
            .filter_map(|rg| rg.column(col_idx).statistics()?.distinct_count_opt())
            .max();
        if let Some(dc) = dc {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: dc,
                total_count: total,
            });
            continue;
        }

        // Tier 2: dictionary page entry count
        if let Some(dc) = dictionary_distinct_count(reader, metadata, col_idx) {
            result[col_idx] = Some(ColumnCardinality {
                distinct_count: dc,
                total_count: total,
            });
        }
    }

    // Tier 3: sample flat columns that still need estimation
    let schema = metadata.file_metadata().schema_descr();
    let is_flat = schema.root_schema().get_fields().len() == num_cols;
    if is_flat {
        let needs_sampling: Vec<usize> = (0..num_cols)
            .filter(|&i| result[i].is_none())
            .collect();
        if !needs_sampling.is_empty() {
            sample_cardinalities(path, metadata, &needs_sampling, &mut result)?;
        }
    }

    // Fill unknowns: assume all unique (conservative)
    Ok(result
        .into_iter()
        .enumerate()
        .map(|(col_idx, card)| {
            let total = total_values(metadata, col_idx);
            card.unwrap_or(ColumnCardinality {
                distinct_count: total,
                total_count: total,
            })
        })
        .collect())
}

fn total_values(metadata: &ParquetMetaData, col_idx: usize) -> u64 {
    metadata
        .row_groups()
        .iter()
        .map(|rg| rg.column(col_idx).num_values() as u64)
        .sum()
}

/// Read dictionary page from the first row group that has one.
/// Returns the number of dictionary entries (= distinct values in that row group).
fn dictionary_distinct_count(
    reader: &SerializedFileReader<File>,
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> Option<u64> {
    for rg_idx in 0..metadata.num_row_groups() {
        let col = metadata.row_group(rg_idx).column(col_idx);
        if col.dictionary_page_offset().is_none() {
            continue;
        }
        let rg_reader = reader.get_row_group(rg_idx).ok()?;
        let mut page_reader = rg_reader.get_column_page_reader(col_idx).ok()?;
        if let Some(Page::DictionaryPage { num_values, .. }) = page_reader.get_next_page().ok()? {
            return Some(num_values as u64);
        }
    }
    None
}

fn sample_cardinalities(
    path: &Path,
    metadata: &ParquetMetaData,
    columns: &[usize],
    result: &mut [Option<ColumnCardinality>],
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder
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
        let total = total_values(metadata, col_idx);
        // Use sample ratio directly (no extrapolation) — robust for classification
        let ratio = sample_distinct as f64 / sample_rows as f64;
        let estimated = (ratio * total as f64).min(total as f64) as u64;

        result[col_idx] = Some(ColumnCardinality {
            distinct_count: estimated.max(sample_distinct),
            total_count: total,
        });
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
