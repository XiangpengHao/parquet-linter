use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{Codec, Directive, Prescription};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Compression, LogicalType, Type as PhysicalType};

pub struct CompressionCodecRule;

const LARGE_UNCOMPRESSED_COLUMN_BYTES: i64 = 4 * 1024 * 1024; // 4 MB
const MIN_COLUMN_BYTES_FOR_CODEC_CHANGE: i64 = 8 * 1024 * 1024; // 8 MB
const MIN_SINGLE_ROW_GROUP_BYTES_FOR_ZSTD: i64 = 32 * 1024 * 1024; // 32 MB
const MIN_TEXT_BYTES_FOR_LZ4_UPGRADE: i64 = 32 * 1024 * 1024; // 32 MB
const MIN_TOTAL_BYTES_FOR_SMALL_CHUNK_LZ4: i64 = 64 * 1024 * 1024; // 64 MB
const MIN_ROW_GROUPS_FOR_SMALL_CHUNK_LZ4: usize = 64;
const MAX_AVG_UNCOMPRESSED_CHUNK_BYTES_FOR_LZ4: i64 = 1024 * 1024; // 1 MB
const MIN_RATIO_FOR_SMALL_CHUNK_LZ4: f64 = 0.55;
const MAX_RATIO_FOR_SMALL_CHUNK_LZ4: f64 = 0.85;
const MAX_RATIO_FOR_ZSTD_UPGRADE_FROM_SNAPPY: f64 = 0.90;
const LOW_COMPRESSION_RATIO_SKIP_ZSTD: f64 = 0.95;
const LOW_COMPRESSION_RATIO_SKIP_LZ4: f64 = 0.98;
const TARGET_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodecRecommendation {
    ZstdLevel3,
    Lz4,
}

impl CodecRecommendation {
    fn target(self) -> Codec {
        match self {
            CodecRecommendation::ZstdLevel3 => Codec::Zstd(TARGET_ZSTD_LEVEL),
            CodecRecommendation::Lz4 => Codec::Lz4Raw,
        }
    }

    fn advice(self) -> &'static str {
        match self {
            CodecRecommendation::ZstdLevel3 => "recommend switching to ZSTD level 3",
            CodecRecommendation::Lz4 => "recommend switching to LZ4 for faster decompression",
        }
    }
}

fn classify_codec_issue(
    compression: Compression,
    uncompressed_size: i64,
) -> Option<(CodecRecommendation, &'static str)> {
    let is_target_zstd = matches!(compression, Compression::ZSTD(level) if level.compression_level() == TARGET_ZSTD_LEVEL);
    let speed_sensitive = uncompressed_size > LARGE_UNCOMPRESSED_COLUMN_BYTES;

    if speed_sensitive && matches!(compression, Compression::SNAPPY) {
        return Some((
            CodecRecommendation::Lz4,
            "large column chunks are decompression-sensitive",
        ));
    }

    if is_target_zstd {
        None
    } else {
        Some((
            CodecRecommendation::ZstdLevel3,
            "default compression policy prefers ZSTD level 3",
        ))
    }
}

fn supports_zstd_upgrade_by_type(
    physical_type: PhysicalType,
    logical_type: Option<&LogicalType>,
) -> bool {
    match physical_type {
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => true,
        PhysicalType::BOOLEAN => false,
        PhysicalType::INT32 | PhysicalType::INT64 => matches!(
            logical_type,
            Some(LogicalType::String)
                | Some(LogicalType::Json)
                | Some(LogicalType::Bson)
                | Some(LogicalType::Enum)
        ),
        PhysicalType::FLOAT | PhysicalType::DOUBLE => false,
        _ => false,
    }
}

fn is_text_logical_type(logical_type: Option<&LogicalType>) -> bool {
    matches!(
        logical_type,
        Some(LogicalType::String | LogicalType::Json | LogicalType::Enum)
    )
}

fn prefer_lz4_for_many_small_snappy_byte_array_chunks(
    physical_type: PhysicalType,
    non_empty_groups: usize,
    total_uncompressed: i64,
    avg_chunk_uncompressed: i64,
    aggregated_ratio: Option<f64>,
    sample_compression: Option<Compression>,
) -> bool {
    if physical_type != PhysicalType::BYTE_ARRAY {
        return false;
    }
    if !matches!(sample_compression, Some(Compression::SNAPPY)) {
        return false;
    }
    if non_empty_groups < MIN_ROW_GROUPS_FOR_SMALL_CHUNK_LZ4 {
        return false;
    }
    if total_uncompressed < MIN_TOTAL_BYTES_FOR_SMALL_CHUNK_LZ4 {
        return false;
    }
    if avg_chunk_uncompressed <= 0
        || avg_chunk_uncompressed > MAX_AVG_UNCOMPRESSED_CHUNK_BYTES_FOR_LZ4
    {
        return false;
    }
    let Some(ratio) = aggregated_ratio else {
        return false;
    };
    (MIN_RATIO_FOR_SMALL_CHUNK_LZ4..=MAX_RATIO_FOR_SMALL_CHUNK_LZ4).contains(&ratio)
}

#[async_trait::async_trait]
impl Rule for CompressionCodecRule {
    fn name(&self) -> &'static str {
        "compression-codec-upgrade"
    }

    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let col0 = row_groups[0].column(col_idx);
            let descr = col0.column_descr();
            let physical_type = descr.physical_type();
            let logical_type = descr.logical_type_ref();

            let mut total_uncompressed = 0i64;
            let mut total_compressed = 0i64;
            let mut non_empty_groups = 0usize;
            let mut zstd_groups = 0usize;
            let mut lz4_groups = 0usize;
            let mut zstd_sample = None;
            let mut lz4_sample = None;
            let mut sample_compression = None;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let compression = col.compression();
                let uncompressed_size = col.uncompressed_size();
                let compressed_size = col.compressed_size();
                if uncompressed_size > 0 {
                    total_uncompressed += uncompressed_size;
                    non_empty_groups += 1;
                    sample_compression.get_or_insert(compression);
                }
                if compressed_size > 0 {
                    total_compressed += compressed_size;
                }
                if let Some((recommendation, reason)) =
                    classify_codec_issue(compression, uncompressed_size)
                {
                    match recommendation {
                        CodecRecommendation::ZstdLevel3 => {
                            zstd_groups += 1;
                            zstd_sample.get_or_insert((compression, reason));
                        }
                        CodecRecommendation::Lz4 => {
                            lz4_groups += 1;
                            lz4_sample.get_or_insert((compression, reason));
                        }
                    }
                }
            }

            if total_uncompressed < MIN_COLUMN_BYTES_FOR_CODEC_CHANGE {
                zstd_groups = 0;
                zstd_sample = None;
            }

            let aggregated_ratio = if total_uncompressed > 0 && total_compressed > 0 {
                Some(total_compressed as f64 / total_uncompressed as f64)
            } else {
                None
            };
            let avg_chunk_uncompressed = if non_empty_groups > 0 {
                total_uncompressed / non_empty_groups as i64
            } else {
                0
            };

            if !supports_zstd_upgrade_by_type(physical_type, logical_type) {
                zstd_groups = 0;
                zstd_sample = None;
            }

            if row_groups.len() == 1 && total_uncompressed < MIN_SINGLE_ROW_GROUP_BYTES_FOR_ZSTD {
                zstd_groups = 0;
                zstd_sample = None;
            }

            if matches!(col0.compression(), Compression::SNAPPY)
                && let Some(ratio) = aggregated_ratio
                && ratio >= MAX_RATIO_FOR_ZSTD_UPGRADE_FROM_SNAPPY
            {
                zstd_groups = 0;
                zstd_sample = None;
            }

            if let Some(ratio) = aggregated_ratio {
                // Let low-compression-ratio rule handle nearly incompressible columns.
                if ratio > LOW_COMPRESSION_RATIO_SKIP_ZSTD {
                    zstd_groups = 0;
                    zstd_sample = None;
                }
            }

            if is_text_logical_type(logical_type)
                && total_uncompressed < MIN_TEXT_BYTES_FOR_LZ4_UPGRADE
            {
                lz4_groups = 0;
                lz4_sample = None;
            }
            if let Some(ratio) = aggregated_ratio
                && ratio > LOW_COMPRESSION_RATIO_SKIP_LZ4
            {
                lz4_groups = 0;
                lz4_sample = None;
            }

            let prefer_lz4_many_small_chunks = prefer_lz4_for_many_small_snappy_byte_array_chunks(
                physical_type,
                non_empty_groups,
                total_uncompressed,
                avg_chunk_uncompressed,
                aggregated_ratio,
                sample_compression,
            );

            let chosen = if prefer_lz4_many_small_chunks {
                Some((
                    CodecRecommendation::Lz4,
                    non_empty_groups,
                    (
                        Compression::SNAPPY,
                        "many small SNAPPY byte-array chunks are scan/decompression-sensitive",
                    ),
                ))
            } else if lz4_groups > zstd_groups {
                lz4_sample.map(|sample| (CodecRecommendation::Lz4, lz4_groups, sample))
            } else {
                zstd_sample
                    .map(|sample| (CodecRecommendation::ZstdLevel3, zstd_groups, sample))
                    .or_else(|| {
                        lz4_sample.map(|sample| (CodecRecommendation::Lz4, lz4_groups, sample))
                    })
            };

            if let Some((recommendation, problematic_groups, (compression, reason))) = chosen {
                let path = col0.column_path().clone();
                let mut prescription = Prescription::new();
                prescription.push(Directive::SetColumnCompression(
                    path.clone(),
                    recommendation.target(),
                ));
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: match recommendation {
                        CodecRecommendation::ZstdLevel3 => Severity::Suggestion,
                        CodecRecommendation::Lz4 => Severity::Warning,
                    },
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "using {:?} in {problematic_groups}/{} row groups; {}; {} \
                         (column size {:.1}MB)",
                        compression,
                        row_groups.len(),
                        reason,
                        recommendation.advice(),
                        total_uncompressed as f64 / (1024.0 * 1024.0),
                    ),
                    prescription,
                });
            }
        }
        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{GzipLevel, ZstdLevel};

    #[test]
    fn classify_gzip_as_zstd_level_3() {
        let got = classify_codec_issue(Compression::GZIP(GzipLevel::default()), 1);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::ZstdLevel3,
                "default compression policy prefers ZSTD level 3",
            ))
        );
    }

    #[test]
    fn do_not_classify_large_uncompressed_as_lz4() {
        let got = classify_codec_issue(
            Compression::UNCOMPRESSED,
            LARGE_UNCOMPRESSED_COLUMN_BYTES + 1,
        );
        assert_eq!(
            got,
            Some((
                CodecRecommendation::ZstdLevel3,
                "default compression policy prefers ZSTD level 3",
            ))
        );
    }

    #[test]
    fn prefer_lz4_for_many_small_snappy_byte_array_chunks_matches_file6_like_pattern() {
        let prefer = prefer_lz4_for_many_small_snappy_byte_array_chunks(
            PhysicalType::BYTE_ARRAY,
            240,
            99_480_415,
            414_501,
            Some(0.636),
            Some(Compression::SNAPPY),
        );
        assert!(prefer);
    }

    #[test]
    fn prefer_lz4_for_many_small_snappy_byte_array_chunks_rejects_too_compressible() {
        let prefer = prefer_lz4_for_many_small_snappy_byte_array_chunks(
            PhysicalType::BYTE_ARRAY,
            240,
            99_480_415,
            414_501,
            Some(0.30),
            Some(Compression::SNAPPY),
        );
        assert!(!prefer);
    }

    #[test]
    fn classify_large_snappy_as_lz4() {
        let got = classify_codec_issue(Compression::SNAPPY, LARGE_UNCOMPRESSED_COLUMN_BYTES + 1);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::Lz4,
                "large column chunks are decompression-sensitive",
            ))
        );
    }

    #[test]
    fn classify_small_uncompressed_as_zstd_level_3() {
        let got = classify_codec_issue(Compression::UNCOMPRESSED, LARGE_UNCOMPRESSED_COLUMN_BYTES);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::ZstdLevel3,
                "default compression policy prefers ZSTD level 3",
            ))
        );
    }

    #[test]
    fn ignore_zstd_level_3() {
        let got = classify_codec_issue(
            Compression::ZSTD(ZstdLevel::try_new(TARGET_ZSTD_LEVEL).expect("valid zstd level")),
            1,
        );
        assert_eq!(got, None);
    }
}
