use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::file::properties::EnabledStatistics;

pub struct PageStatisticsRule;

impl Rule for PageStatisticsRule {
    fn name(&self) -> &'static str {
        "missing-page-statistics"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if col.column_index_offset().is_none() {
                    let path = col.column_path().clone();
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Warning,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message: "no page-level column index found; page statistics are missing"
                            .to_string(),
                        fixes: vec![FixAction::SetColumnStatisticsEnabled(
                            path,
                            EnabledStatistics::Page,
                        )],
                    });
                }
            }
        }
        diagnostics
    }
}
