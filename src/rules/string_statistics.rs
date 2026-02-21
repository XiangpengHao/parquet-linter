use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Type as PhysicalType;

pub struct StringStatisticsRule;

const MAX_STAT_LENGTH: usize = 64;

#[async_trait::async_trait]
impl Rule for StringStatisticsRule {
    fn name(&self) -> &'static str {
        "oversized-string-statistics"
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
            if !matches!(
                col0.column_descr().physical_type(),
                PhysicalType::BYTE_ARRAY
            ) {
                continue;
            }

            let mut affected_groups = 0usize;
            let mut peak_min_len = 0usize;
            let mut peak_max_len = 0usize;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let Some(stats) = col.statistics() else {
                    continue;
                };
                if !stats.min_is_exact() {
                    continue;
                }

                let min_len = stats.min_bytes_opt().map_or(0, |b| b.len());
                let max_len = stats.max_bytes_opt().map_or(0, |b| b.len());
                let oversized = min_len > MAX_STAT_LENGTH || max_len > MAX_STAT_LENGTH;
                if oversized {
                    affected_groups += 1;
                    peak_min_len = peak_min_len.max(min_len);
                    peak_max_len = peak_max_len.max(max_len);
                }
            }

            if affected_groups > 0 {
                let path = col0.column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "string statistics are large (up to min: {peak_min_len}B, max: {peak_max_len}B) \
                         in {affected_groups}/{} row groups and untruncated; \
                         consider truncating to {MAX_STAT_LENGTH} bytes",
                        row_groups.len()
                    ),
                    fixes: vec![FixAction::SetStatisticsTruncateLength(Some(MAX_STAT_LENGTH))],
                });
            }
        }
        diagnostics
    }
}
