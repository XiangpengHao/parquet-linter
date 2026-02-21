use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Compression;

pub struct CompressionCodecRule;

impl Rule for CompressionCodecRule {
    fn name(&self) -> &'static str {
        "compression-codec-upgrade"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let mut problematic_groups = 0usize;
            let mut sample = None;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let compression = col.compression();
                let suggest = match compression {
                    Compression::GZIP(_) => {
                        Some("GZIP has worse decompression speed than ZSTD at similar ratios")
                    }
                    Compression::LZ4 => Some("LZ4 codec is deprecated; use ZSTD instead"),
                    _ => None,
                };
                if let Some(reason) = suggest {
                    problematic_groups += 1;
                    sample = Some((compression, reason));
                }
            }

            if let Some((compression, reason)) = sample {
                let path = row_groups[0].column(col_idx).column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Info,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "using {:?} in {problematic_groups}/{} row groups; {}; \
                         consider upgrading to ZSTD",
                        compression,
                        row_groups.len(),
                        reason
                    ),
                    fixes: vec![FixAction::SetColumnCompression(
                        path,
                        Compression::ZSTD(Default::default()),
                    )],
                });
            }
        }
        diagnostics
    }
}
