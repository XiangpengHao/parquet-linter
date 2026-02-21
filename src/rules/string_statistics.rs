use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Type as PhysicalType;

pub struct StringStatisticsRule;

const MAX_STAT_LENGTH: usize = 64;

impl Rule for StringStatisticsRule {
    fn name(&self) -> &'static str {
        "oversized-string-statistics"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if !matches!(
                    col.column_descr().physical_type(),
                    PhysicalType::BYTE_ARRAY
                ) {
                    continue;
                }

                let stats = match col.statistics() {
                    Some(s) => s,
                    None => continue,
                };

                if !stats.min_is_exact() {
                    continue;
                }

                let min_len = stats.min_bytes_opt().map_or(0, |b| b.len());
                let max_len = stats.max_bytes_opt().map_or(0, |b| b.len());
                let oversized = min_len > MAX_STAT_LENGTH || max_len > MAX_STAT_LENGTH;

                if oversized {
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
                            "string statistics are large (min: {min_len}B, max: {max_len}B) \
                             and untruncated; consider truncating to {MAX_STAT_LENGTH} bytes"
                        ),
                        fixes: vec![FixAction::SetStatisticsTruncateLength(Some(
                            MAX_STAT_LENGTH,
                        ))],
                    });
                }
            }
        }
        diagnostics
    }
}
