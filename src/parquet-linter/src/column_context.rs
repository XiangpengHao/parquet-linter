use arrow_schema::DataType;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::basic::{LogicalType, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

use crate::cardinality;

/// Per-leaf-column context combining type information and statistics
/// extracted from Parquet metadata.
pub struct ColumnContext {
    /// Parquet physical storage type.
    pub physical_type: PhysicalType,
    /// Parquet logical type annotation (if any).
    pub logical_type: Option<LogicalType>,
    /// Corresponding Arrow data type.
    pub arrow_type: DataType,

    /// Total number of values (including nulls) across all row groups.
    pub num_values: u64,
    /// Total null count across all row groups.
    pub null_count: u64,
    /// Estimated number of distinct non-null values (file-level).
    pub distinct_count: u64,

    /// Total uncompressed byte size across all row groups.
    pub uncompressed_size: i64,
    /// Total compressed byte size across all row groups.
    pub compressed_size: i64,

    /// Type-specific statistics extracted from column-chunk metadata.
    pub type_stats: TypeStats,
}

impl ColumnContext {
    pub fn non_null_count(&self) -> u64 {
        self.num_values.saturating_sub(self.null_count)
    }

    pub fn null_ratio(&self) -> f64 {
        if self.num_values == 0 {
            0.0
        } else {
            self.null_count as f64 / self.num_values as f64
        }
    }

    pub fn cardinality_ratio(&self) -> f64 {
        let nn = self.non_null_count();
        if nn == 0 {
            0.0
        } else {
            self.distinct_count as f64 / nn as f64
        }
    }
}

pub enum TypeStats {
    Boolean(BooleanStats),
    Int(IntStats),
    Float(FloatStats),
    String(StringStats),
    Binary(BinaryStats),
    FixedLenBinary(FixedLenBinaryStats),
    Unknown,
}

pub struct BooleanStats {
    /// Global minimum across all row groups.
    pub min: Option<bool>,
    /// Global maximum across all row groups.
    pub max: Option<bool>,
}

pub struct IntStats {
    /// Logical bit width (8, 16, 32, or 64); defaults to physical width.
    pub bit_width: u8,
    /// Whether the logical type indicates signed integers.
    pub is_signed: bool,
    /// Global minimum across all row groups (INT32 values widened to i64).
    pub min: Option<i64>,
    /// Global maximum across all row groups.
    pub max: Option<i64>,
}

pub struct FloatStats {
    /// Physical bit width (32 or 64).
    pub bit_width: u8,
    /// Global minimum across all row groups (FLOAT widened to f64).
    pub min: Option<f64>,
    /// Global maximum across all row groups.
    pub max: Option<f64>,
}

pub struct ByteLengthStats {
    /// Minimum byte length observed in sample.
    pub min: usize,
    /// Maximum byte length observed in sample.
    pub max: usize,
    /// Average byte length observed in sample.
    pub avg: f64,
}

pub struct StringStats {
    /// Global minimum string value (only from exact statistics).
    pub min_value: Option<String>,
    /// Global maximum string value (only from exact statistics).
    pub max_value: Option<String>,
    /// Length statistics from sampling one row group.
    pub lengths: Option<ByteLengthStats>,
}

pub struct BinaryStats {
    /// Global minimum value (only from exact statistics).
    pub min_value: Option<Vec<u8>>,
    /// Global maximum value (only from exact statistics).
    pub max_value: Option<Vec<u8>>,
    /// Length statistics from sampling one row group.
    pub lengths: Option<ByteLengthStats>,
}

pub struct FixedLenBinaryStats {
    /// Fixed byte length from the Parquet type.
    pub type_length: i32,
}

/// Build per-column contexts from metadata and cardinality estimation.
pub async fn build(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
) -> anyhow::Result<Vec<ColumnContext>> {
    let cardinalities = cardinality::estimate(reader, metadata).await?;
    let schema = metadata.file_metadata().schema_descr();
    let num_cols = schema.num_columns();
    let arrow_types = derive_arrow_types(metadata);

    let mut columns = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let descr = schema.column(col_idx);
        let physical_type = descr.physical_type();
        let logical_type = descr.logical_type_ref().cloned();

        let mut num_values = 0u64;
        let mut null_count = 0u64;
        let mut uncompressed_size = 0i64;
        let mut compressed_size = 0i64;

        for rg in metadata.row_groups() {
            let col = rg.column(col_idx);
            num_values += col.num_values() as u64;
            null_count += col
                .statistics()
                .and_then(|s| s.null_count_opt())
                .unwrap_or(0);
            uncompressed_size += col.uncompressed_size();
            compressed_size += col.compressed_size();
        }

        let type_stats = extract_type_stats(
            physical_type,
            logical_type.as_ref(),
            &descr,
            metadata,
            col_idx,
        );

        let card = &cardinalities[col_idx];

        columns.push(ColumnContext {
            physical_type,
            logical_type,
            arrow_type: arrow_types[col_idx].clone(),
            num_values,
            null_count,
            distinct_count: card.distinct_count,
            uncompressed_size,
            compressed_size,
            type_stats,
        });
    }

    // Sample byte lengths for all BYTE_ARRAY columns.
    let byte_array_cols: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.physical_type == PhysicalType::BYTE_ARRAY)
        .map(|(i, _)| i)
        .collect();
    if !byte_array_cols.is_empty() {
        let mut lengths = sample_byte_lengths(reader, metadata, &byte_array_cols).await?;
        for (i, col_idx) in byte_array_cols.into_iter().enumerate() {
            match &mut columns[col_idx].type_stats {
                TypeStats::String(s) => s.lengths = lengths[i].take(),
                TypeStats::Binary(b) => b.lengths = lengths[i].take(),
                _ => {}
            }
        }
    }

    Ok(columns)
}

fn derive_arrow_types(metadata: &ParquetMetaData) -> Vec<DataType> {
    let schema_descr = metadata.file_metadata().schema_descr();
    let key_value_metadata = metadata.file_metadata().key_value_metadata();
    let num_cols = schema_descr.num_columns();

    // For flat schemas, parquet_to_arrow_schema gives a 1:1 mapping.
    let is_flat = schema_descr.root_schema().get_fields().len() == num_cols;
    if is_flat {
        if let Ok(arrow_schema) =
            parquet::arrow::parquet_to_arrow_schema(schema_descr, key_value_metadata)
        {
            return arrow_schema
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect();
        }
    }

    // Fallback: derive per-column from physical/logical type.
    (0..num_cols)
        .map(|i| arrow_type_from_descriptor(&schema_descr.column(i)))
        .collect()
}

fn arrow_type_from_descriptor(descr: &ColumnDescriptor) -> DataType {
    match descr.physical_type() {
        PhysicalType::BOOLEAN => DataType::Boolean,
        PhysicalType::INT32 => match descr.logical_type_ref() {
            Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }) => DataType::Int8,
            Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }) => DataType::Int16,
            Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }) => DataType::UInt8,
            Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }) => DataType::UInt16,
            Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }) => DataType::UInt32,
            Some(LogicalType::Date) => DataType::Date32,
            Some(LogicalType::Decimal { precision, scale }) => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            Some(LogicalType::Time {
                unit: TimeUnit::MILLIS,
                ..
            }) => DataType::Time32(arrow_schema::TimeUnit::Millisecond),
            _ => DataType::Int32,
        },
        PhysicalType::INT64 => match descr.logical_type_ref() {
            Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }) => DataType::UInt64,
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            }) => {
                let tu = match unit {
                    TimeUnit::MILLIS => arrow_schema::TimeUnit::Millisecond,
                    TimeUnit::MICROS => arrow_schema::TimeUnit::Microsecond,
                    TimeUnit::NANOS => arrow_schema::TimeUnit::Nanosecond,
                };
                DataType::Timestamp(
                    tu,
                    if *is_adjusted_to_u_t_c {
                        Some("UTC".into())
                    } else {
                        None
                    },
                )
            }
            Some(LogicalType::Time { unit, .. }) => match unit {
                TimeUnit::MICROS => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
                TimeUnit::NANOS => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),
                _ => DataType::Int64,
            },
            Some(LogicalType::Decimal { precision, scale }) => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            _ => DataType::Int64,
        },
        PhysicalType::INT96 => DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        PhysicalType::FLOAT => DataType::Float32,
        PhysicalType::DOUBLE => DataType::Float64,
        PhysicalType::BYTE_ARRAY => match descr.logical_type_ref() {
            Some(LogicalType::String | LogicalType::Enum | LogicalType::Json) => DataType::Utf8,
            _ => DataType::Binary,
        },
        PhysicalType::FIXED_LEN_BYTE_ARRAY => match descr.logical_type_ref() {
            Some(LogicalType::Decimal { precision, scale }) => {
                DataType::Decimal128(*precision as u8, *scale as i8)
            }
            _ => DataType::FixedSizeBinary(descr.type_length()),
        },
    }
}

fn extract_type_stats(
    physical_type: PhysicalType,
    logical_type: Option<&LogicalType>,
    descr: &ColumnDescriptor,
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> TypeStats {
    match physical_type {
        PhysicalType::BOOLEAN => {
            let (min, max) = aggregate_bool_minmax(metadata, col_idx);
            TypeStats::Boolean(BooleanStats { min, max })
        }
        PhysicalType::INT32 => {
            let (is_signed, bit_width) = int_type_info(logical_type, 32);
            let (min, max) = aggregate_int32_minmax(metadata, col_idx);
            TypeStats::Int(IntStats {
                bit_width,
                is_signed,
                min: min.map(i64::from),
                max: max.map(i64::from),
            })
        }
        PhysicalType::INT64 => {
            let (is_signed, bit_width) = int_type_info(logical_type, 64);
            let (min, max) = aggregate_int64_minmax(metadata, col_idx);
            TypeStats::Int(IntStats {
                bit_width,
                is_signed,
                min,
                max,
            })
        }
        PhysicalType::FLOAT => {
            let (min, max) = aggregate_float_minmax(metadata, col_idx);
            TypeStats::Float(FloatStats {
                bit_width: 32,
                min: min.map(f64::from),
                max: max.map(f64::from),
            })
        }
        PhysicalType::DOUBLE => {
            let (min, max) = aggregate_double_minmax(metadata, col_idx);
            TypeStats::Float(FloatStats {
                bit_width: 64,
                min,
                max,
            })
        }
        PhysicalType::BYTE_ARRAY => {
            let is_string = matches!(
                logical_type,
                Some(
                    LogicalType::String
                        | LogicalType::Json
                        | LogicalType::Enum
                        | LogicalType::Bson
                )
            );
            if is_string {
                let (min_value, max_value) = aggregate_string_minmax(metadata, col_idx);
                TypeStats::String(StringStats {
                    min_value,
                    max_value,
                    lengths: None,
                })
            } else {
                let (min_value, max_value) = aggregate_binary_minmax(metadata, col_idx);
                TypeStats::Binary(BinaryStats {
                    min_value,
                    max_value,
                    lengths: None,
                })
            }
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            TypeStats::FixedLenBinary(FixedLenBinaryStats {
                type_length: descr.type_length(),
            })
        }
        _ => TypeStats::Unknown,
    }
}

fn int_type_info(logical_type: Option<&LogicalType>, physical_bits: u8) -> (bool, u8) {
    match logical_type {
        Some(LogicalType::Integer {
            bit_width,
            is_signed,
        }) => (*is_signed, *bit_width as u8),
        _ => (true, physical_bits),
    }
}

fn aggregate_bool_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<bool>, Option<bool>) {
    let mut global_min: Option<bool> = None;
    let mut global_max: Option<bool> = None;
    for rg in metadata.row_groups() {
        if let Some(Statistics::Boolean(stats)) = rg.column(col_idx).statistics() {
            if let Some(&v) = stats.min_opt() {
                global_min = Some(global_min.map_or(v, |cur| cur && v));
            }
            if let Some(&v) = stats.max_opt() {
                global_max = Some(global_max.map_or(v, |cur| cur || v));
            }
        }
    }
    (global_min, global_max)
}

fn aggregate_int32_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<i32>, Option<i32>) {
    let mut global_min: Option<i32> = None;
    let mut global_max: Option<i32> = None;
    for rg in metadata.row_groups() {
        if let Some(Statistics::Int32(stats)) = rg.column(col_idx).statistics() {
            if let Some(&v) = stats.min_opt() {
                global_min = Some(global_min.map_or(v, |cur| cur.min(v)));
            }
            if let Some(&v) = stats.max_opt() {
                global_max = Some(global_max.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    (global_min, global_max)
}

fn aggregate_int64_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<i64>, Option<i64>) {
    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;
    for rg in metadata.row_groups() {
        if let Some(Statistics::Int64(stats)) = rg.column(col_idx).statistics() {
            if let Some(&v) = stats.min_opt() {
                global_min = Some(global_min.map_or(v, |cur| cur.min(v)));
            }
            if let Some(&v) = stats.max_opt() {
                global_max = Some(global_max.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    (global_min, global_max)
}

fn aggregate_float_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<f32>, Option<f32>) {
    let mut global_min: Option<f32> = None;
    let mut global_max: Option<f32> = None;
    for rg in metadata.row_groups() {
        if let Some(Statistics::Float(stats)) = rg.column(col_idx).statistics() {
            if let Some(&v) = stats.min_opt() {
                global_min = Some(global_min.map_or(v, |cur| cur.min(v)));
            }
            if let Some(&v) = stats.max_opt() {
                global_max = Some(global_max.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    (global_min, global_max)
}

fn aggregate_double_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<f64>, Option<f64>) {
    let mut global_min: Option<f64> = None;
    let mut global_max: Option<f64> = None;
    for rg in metadata.row_groups() {
        if let Some(Statistics::Double(stats)) = rg.column(col_idx).statistics() {
            if let Some(&v) = stats.min_opt() {
                global_min = Some(global_min.map_or(v, |cur| cur.min(v)));
            }
            if let Some(&v) = stats.max_opt() {
                global_max = Some(global_max.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    (global_min, global_max)
}

fn aggregate_string_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<String>, Option<String>) {
    let mut global_min: Option<Vec<u8>> = None;
    let mut global_max: Option<Vec<u8>> = None;
    for rg in metadata.row_groups() {
        let col = rg.column(col_idx);
        let Some(stats) = col.statistics() else {
            continue;
        };
        if stats.min_is_exact() {
            if let Some(min_bytes) = stats.min_bytes_opt() {
                global_min = Some(match global_min {
                    Some(cur) if cur.as_slice() <= min_bytes => cur,
                    _ => min_bytes.to_vec(),
                });
            }
        }
        if stats.max_is_exact() {
            if let Some(max_bytes) = stats.max_bytes_opt() {
                global_max = Some(match global_max {
                    Some(cur) if cur.as_slice() >= max_bytes => cur,
                    _ => max_bytes.to_vec(),
                });
            }
        }
    }
    (
        global_min.and_then(|b| String::from_utf8(b).ok()),
        global_max.and_then(|b| String::from_utf8(b).ok()),
    )
}

fn aggregate_binary_minmax(
    metadata: &ParquetMetaData,
    col_idx: usize,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut global_min: Option<Vec<u8>> = None;
    let mut global_max: Option<Vec<u8>> = None;
    for rg in metadata.row_groups() {
        let col = rg.column(col_idx);
        let Some(stats) = col.statistics() else {
            continue;
        };
        if stats.min_is_exact() {
            if let Some(min_bytes) = stats.min_bytes_opt() {
                global_min = Some(match global_min {
                    Some(cur) if cur.as_slice() <= min_bytes => cur,
                    _ => min_bytes.to_vec(),
                });
            }
        }
        if stats.max_is_exact() {
            if let Some(max_bytes) = stats.max_bytes_opt() {
                global_max = Some(match global_max {
                    Some(cur) if cur.as_slice() >= max_bytes => cur,
                    _ => max_bytes.to_vec(),
                });
            }
        }
    }
    (global_min, global_max)
}

const SAMPLE_ROWS: usize = 16_384;

/// Sample one row group to compute byte-length statistics for BYTE_ARRAY columns.
async fn sample_byte_lengths(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
    byte_array_cols: &[usize],
) -> anyhow::Result<Vec<Option<ByteLengthStats>>> {
    use futures::StreamExt;
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

    if byte_array_cols.is_empty() || metadata.num_row_groups() == 0 {
        return Ok((0..byte_array_cols.len()).map(|_| None).collect());
    }

    let sample_rg_idx = cardinality::pick_sample_row_group(metadata);
    let mask = parquet::arrow::ProjectionMask::leaves(
        metadata.file_metadata().schema_descr(),
        byte_array_cols.iter().copied(),
    );
    let builder = ParquetRecordBatchStreamBuilder::new(reader.clone())
        .await?
        .with_row_groups(vec![sample_rg_idx])
        .with_batch_size(SAMPLE_ROWS)
        .with_limit(SAMPLE_ROWS)
        .with_projection(mask);
    let mut stream = builder.build()?;

    let mut min_lens = vec![usize::MAX; byte_array_cols.len()];
    let mut max_lens = vec![0usize; byte_array_cols.len()];
    let mut total_lens = vec![0u64; byte_array_cols.len()];
    let mut counts = vec![0u64; byte_array_cols.len()];

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        for (i, _) in byte_array_cols.iter().enumerate() {
            let array = batch.column(i);
            let (non_null, total_len, batch_min, batch_max) = measure_byte_lengths(array.as_ref());
            counts[i] += non_null;
            total_lens[i] += total_len;
            if non_null > 0 {
                min_lens[i] = min_lens[i].min(batch_min);
                max_lens[i] = max_lens[i].max(batch_max);
            }
        }
    }

    Ok((0..byte_array_cols.len())
        .map(|i| {
            if counts[i] == 0 {
                return None;
            }
            Some(ByteLengthStats {
                min: min_lens[i],
                max: max_lens[i],
                avg: total_lens[i] as f64 / counts[i] as f64,
            })
        })
        .collect())
}

/// Measure byte lengths from an Arrow array, returning (non_null_count, total_length, min_length, max_length).
fn measure_byte_lengths(array: &dyn arrow_array::Array) -> (u64, u64, usize, usize) {
    use arrow_array::{Array, BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray};

    let any = array.as_any();

    macro_rules! measure {
        ($arr:expr) => {{
            let a = $arr;
            let mut count = 0u64;
            let mut total = 0u64;
            let mut min = usize::MAX;
            let mut max = 0usize;
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                let len = a.value(i).len();
                count += 1;
                total += len as u64;
                min = min.min(len);
                max = max.max(len);
            }
            (count, total, min, max)
        }};
    }

    if let Some(a) = any.downcast_ref::<StringArray>() {
        measure!(a)
    } else if let Some(a) = any.downcast_ref::<LargeStringArray>() {
        measure!(a)
    } else if let Some(a) = any.downcast_ref::<StringViewArray>() {
        measure!(a)
    } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
        measure!(a)
    } else if let Some(a) = any.downcast_ref::<LargeBinaryArray>() {
        measure!(a)
    } else if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
        measure!(a)
    } else {
        (0, 0, usize::MAX, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_null_count_saturates() {
        let ctx = ColumnContext {
            physical_type: PhysicalType::INT32,
            logical_type: None,
            arrow_type: DataType::Int32,
            num_values: 10,
            null_count: 100,
            distinct_count: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            type_stats: TypeStats::Unknown,
        };
        assert_eq!(ctx.non_null_count(), 0);
    }

    #[test]
    fn cardinality_ratio_zero_when_all_null() {
        let ctx = ColumnContext {
            physical_type: PhysicalType::INT32,
            logical_type: None,
            arrow_type: DataType::Int32,
            num_values: 100,
            null_count: 100,
            distinct_count: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            type_stats: TypeStats::Unknown,
        };
        assert_eq!(ctx.cardinality_ratio(), 0.0);
    }

    #[test]
    fn cardinality_ratio_computes_correctly() {
        let ctx = ColumnContext {
            physical_type: PhysicalType::INT32,
            logical_type: None,
            arrow_type: DataType::Int32,
            num_values: 1000,
            null_count: 0,
            distinct_count: 100,
            uncompressed_size: 0,
            compressed_size: 0,
            type_stats: TypeStats::Unknown,
        };
        assert!((ctx.cardinality_ratio() - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn int_type_info_uses_logical_type() {
        let (is_signed, bit_width) = int_type_info(
            Some(&LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }),
            32,
        );
        assert!(!is_signed);
        assert_eq!(bit_width, 16);
    }

    #[test]
    fn int_type_info_defaults_to_physical() {
        let (is_signed, bit_width) = int_type_info(None, 64);
        assert!(is_signed);
        assert_eq!(bit_width, 64);
    }
}
