use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{Directive, Prescription, StatisticsConfig};
use crate::rule::{Rule, RuleContext};

pub struct PageStatisticsRule;

#[async_trait::async_trait]
impl Rule for PageStatisticsRule {
    fn name(&self) -> &'static str {
        "missing-page-statistics"
    }

    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let missing_groups = row_groups
                .iter()
                .filter(|rg| rg.column(col_idx).column_index_offset().is_none())
                .count();
            if missing_groups > 0 {
                let path = row_groups[0].column(col_idx).column_path().clone();
                let mut prescription = Prescription::new();
                prescription.push(Directive::SetColumnStatistics(
                    path.clone(),
                    StatisticsConfig::Page,
                ));
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "no page-level column index found in {missing_groups}/{} row groups; \
                         page statistics are missing",
                        row_groups.len()
                    ),
                    prescription,
                });
            }
        }
        diagnostics
    }
}
