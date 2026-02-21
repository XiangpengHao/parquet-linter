use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Compression, ZstdLevel};

pub struct CompressionCodecRule;

const LARGE_UNCOMPRESSED_COLUMN_BYTES: i64 = 4 * 1024 * 1024; // 4 MB
const TARGET_ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodecRecommendation {
    ZstdLevel3,
    Lz4,
}

impl CodecRecommendation {
    fn target(self) -> Compression {
        match self {
            CodecRecommendation::ZstdLevel3 => {
                Compression::ZSTD(ZstdLevel::try_new(TARGET_ZSTD_LEVEL).expect("valid zstd level"))
            }
            CodecRecommendation::Lz4 => Compression::LZ4,
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
    let target_zstd = CodecRecommendation::ZstdLevel3.target();
    let speed_sensitive = uncompressed_size > LARGE_UNCOMPRESSED_COLUMN_BYTES;

    if speed_sensitive && matches!(compression, Compression::UNCOMPRESSED | Compression::SNAPPY) {
        return Some((
            CodecRecommendation::Lz4,
            "large column chunks are decompression-sensitive",
        ));
    }

    if compression == target_zstd {
        None
    } else {
        Some((
            CodecRecommendation::ZstdLevel3,
            "default compression policy prefers ZSTD level 3",
        ))
    }
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
            let mut zstd_groups = 0usize;
            let mut lz4_groups = 0usize;
            let mut zstd_sample = None;
            let mut lz4_sample = None;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let compression = col.compression();
                let uncompressed_size = col.uncompressed_size();
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

            let chosen = if lz4_groups > zstd_groups {
                lz4_sample.map(|sample| (CodecRecommendation::Lz4, lz4_groups, sample))
            } else {
                zstd_sample
                    .map(|sample| (CodecRecommendation::ZstdLevel3, zstd_groups, sample))
                    .or_else(|| {
                        lz4_sample
                            .map(|sample| (CodecRecommendation::Lz4, lz4_groups, sample))
                    })
            };

            if let Some((recommendation, problematic_groups, (compression, reason))) = chosen {
                let path = row_groups[0].column(col_idx).column_path().clone();
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
                        "using {:?} in {problematic_groups}/{} row groups; {}; \
                         {}",
                        compression,
                        row_groups.len(),
                        reason,
                        recommendation.advice()
                    ),
                    fixes: vec![FixAction::SetColumnCompression(path, recommendation.target())],
                });
            }
        }
        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::GzipLevel;

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
    fn classify_large_uncompressed_as_lz4() {
        let got =
            classify_codec_issue(Compression::UNCOMPRESSED, LARGE_UNCOMPRESSED_COLUMN_BYTES + 1);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::Lz4,
                "large column chunks are decompression-sensitive",
            ))
        );
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
