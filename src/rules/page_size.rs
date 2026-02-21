use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};

pub struct PageSizeRule;

const TARGET_ROW_GROUP_SIZE: i64 = 128 * 1024 * 1024; // 128 MB
const MIN_ROW_GROUP_SIZE: i64 = 16 * 1024 * 1024; // 16 MB
const MAX_ROW_GROUP_SIZE: i64 = 512 * 1024 * 1024; // 512 MB

impl Rule for PageSizeRule {
    fn name(&self) -> &'static str {
        "page-row-group-size"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            let size = rg.compressed_size();
            if size < MIN_ROW_GROUP_SIZE {
                let num_rows = rg.num_rows();
                let target_rows =
                    (num_rows as f64 * TARGET_ROW_GROUP_SIZE as f64 / size as f64) as usize;
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::RowGroup { index: rg_idx },
                    message: format!(
                        "row group compressed size is {:.1} MB (< 16 MB), consider larger row groups by increasing rows per row group",
                        size as f64 / 1024.0 / 1024.0
                    ),
                    fixes: vec![FixAction::SetMaxRowGroupSize(target_rows)],
                });
            } else if size > MAX_ROW_GROUP_SIZE {
                let num_rows = rg.num_rows();
                let target_rows =
                    (num_rows as f64 * TARGET_ROW_GROUP_SIZE as f64 / size as f64) as usize;
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::RowGroup { index: rg_idx },
                    message: format!(
                        "row group compressed size is {:.1} MB (> 512 MB), consider smaller row groups",
                        size as f64 / 1024.0 / 1024.0
                    ),
                    fixes: vec![FixAction::SetMaxRowGroupSize(target_rows)],
                });
            }
        }
        diagnostics
    }
}
