use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Compression;

pub struct CompressionCodecRule;

const LARGE_UNCOMPRESSED_COLUMN_BYTES: i64 = 4 * 1024 * 1024; // 4 MB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodecRecommendation {
    Zstd,
    Snappy,
}

impl CodecRecommendation {
    fn target(self) -> Compression {
        match self {
            CodecRecommendation::Zstd => Compression::ZSTD(Default::default()),
            CodecRecommendation::Snappy => Compression::SNAPPY,
        }
    }

    fn advice(self) -> &'static str {
        match self {
            CodecRecommendation::Zstd => "consider upgrading to ZSTD",
            CodecRecommendation::Snappy => "consider enabling SNAPPY compression",
        }
    }
}

fn classify_codec_issue(
    compression: Compression,
    uncompressed_size: i64,
) -> Option<(CodecRecommendation, &'static str)> {
    match compression {
        Compression::GZIP(_) => Some((
            CodecRecommendation::Zstd,
            "GZIP has worse decompression speed than ZSTD at similar ratios",
        )),
        Compression::LZ4 => Some((
            CodecRecommendation::Zstd,
            "LZ4 codec is deprecated; use ZSTD instead",
        )),
        Compression::UNCOMPRESSED if uncompressed_size > LARGE_UNCOMPRESSED_COLUMN_BYTES => Some((
            CodecRecommendation::Snappy,
            "UNCOMPRESSED column chunks are larger than 4MB",
        )),
        _ => None,
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
            let mut snappy_groups = 0usize;
            let mut zstd_sample = None;
            let mut snappy_sample = None;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let compression = col.compression();
                let uncompressed_size = col.uncompressed_size();
                if let Some((recommendation, reason)) =
                    classify_codec_issue(compression, uncompressed_size)
                {
                    match recommendation {
                        CodecRecommendation::Zstd => {
                            zstd_groups += 1;
                            zstd_sample.get_or_insert((compression, reason));
                        }
                        CodecRecommendation::Snappy => {
                            snappy_groups += 1;
                            snappy_sample.get_or_insert((compression, reason));
                        }
                    }
                }
            }

            let chosen = if snappy_groups > zstd_groups {
                snappy_sample.map(|sample| (CodecRecommendation::Snappy, snappy_groups, sample))
            } else {
                zstd_sample
                    .map(|sample| (CodecRecommendation::Zstd, zstd_groups, sample))
                    .or_else(|| {
                        snappy_sample
                            .map(|sample| (CodecRecommendation::Snappy, snappy_groups, sample))
                    })
            };

            if let Some((recommendation, problematic_groups, (compression, reason))) = chosen {
                let path = row_groups[0].column(col_idx).column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: match recommendation {
                        CodecRecommendation::Zstd => Severity::Info,
                        CodecRecommendation::Snappy => Severity::Warning,
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
    fn classify_gzip_as_zstd_upgrade() {
        let got = classify_codec_issue(Compression::GZIP(GzipLevel::default()), 1);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::Zstd,
                "GZIP has worse decompression speed than ZSTD at similar ratios",
            ))
        );
    }

    #[test]
    fn classify_large_uncompressed_as_snappy() {
        let got =
            classify_codec_issue(Compression::UNCOMPRESSED, LARGE_UNCOMPRESSED_COLUMN_BYTES + 1);
        assert_eq!(
            got,
            Some((
                CodecRecommendation::Snappy,
                "UNCOMPRESSED column chunks are larger than 4MB",
            ))
        );
    }

    #[test]
    fn ignore_small_uncompressed() {
        let got = classify_codec_issue(Compression::UNCOMPRESSED, LARGE_UNCOMPRESSED_COLUMN_BYTES);
        assert_eq!(got, None);
    }
}
