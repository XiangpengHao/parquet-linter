use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Compression;

pub struct CompressionRatioRule;

impl Rule for CompressionRatioRule {
    fn name(&self) -> &'static str {
        "low-compression-ratio"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if matches!(col.compression(), Compression::UNCOMPRESSED) {
                    continue;
                }
                let compressed = col.compressed_size();
                let uncompressed = col.uncompressed_size();
                if uncompressed <= 0 {
                    continue;
                }
                let ratio = compressed as f64 / uncompressed as f64;
                if ratio > 0.95 {
                    let path = col.column_path().clone();
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Warning,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message: format!(
                            "compression ratio is {ratio:.2} ({:?}), data is nearly incompressible",
                            col.compression()
                        ),
                        fixes: vec![FixAction::SetColumnCompression(
                            path,
                            Compression::UNCOMPRESSED,
                        )],
                    });
                }
            }
        }
        diagnostics
    }
}
