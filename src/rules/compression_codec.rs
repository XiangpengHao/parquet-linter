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
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let compression = col.compression();
                let suggest = match compression {
                    Compression::GZIP(_) => Some("GZIP has worse decompression speed than ZSTD at similar ratios"),
                    Compression::LZ4 => Some("LZ4 codec is deprecated; use ZSTD instead"),
                    _ => None,
                };
                if let Some(reason) = suggest {
                    let path = col.column_path().clone();
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Info,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message: format!(
                            "using {:?}; {reason}; consider upgrading to ZSTD",
                            compression
                        ),
                        fixes: vec![FixAction::SetColumnCompression(
                            path,
                            Compression::ZSTD(Default::default()),
                        )],
                    });
                }
            }
        }
        diagnostics
    }
}
