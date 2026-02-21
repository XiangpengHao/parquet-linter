use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Compression;

pub struct CompressionRatioRule;

#[async_trait::async_trait]
impl Rule for CompressionRatioRule {
    fn name(&self) -> &'static str {
        "low-compression-ratio"
    }

    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let mut compressed_sum = 0i64;
            let mut uncompressed_sum = 0i64;
            let mut compressed_groups = 0usize;
            let mut sample_compression = None;

            for rg in row_groups {
                let col = rg.column(col_idx);
                if matches!(col.compression(), Compression::UNCOMPRESSED) {
                    continue;
                }
                let uncompressed = col.uncompressed_size();
                if uncompressed <= 0 {
                    continue;
                }
                compressed_sum += col.compressed_size();
                uncompressed_sum += uncompressed;
                compressed_groups += 1;
                sample_compression = Some(col.compression());
            }

            if uncompressed_sum <= 0 {
                continue;
            }

            let ratio = compressed_sum as f64 / uncompressed_sum as f64;
            if ratio > 0.95 {
                let Some(compression) = sample_compression else {
                    continue;
                };
                let path = row_groups[0].column(col_idx).column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "aggregated compression ratio is {ratio:.2} ({:?}) across \
                         {compressed_groups}/{} row groups; data is nearly incompressible",
                        compression,
                        row_groups.len()
                    ),
                    fixes: vec![FixAction::SetColumnCompression(
                        path,
                        Compression::UNCOMPRESSED,
                    )],
                });
            }
        }
        diagnostics
    }
}
