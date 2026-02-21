use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Type as PhysicalType;

pub struct VectorEmbeddingRule;

const SMALL_PAGE_SIZE: usize = 8192;
const MIN_ELEMENTS_PER_ROW: i64 = 64;

impl Rule for VectorEmbeddingRule {
    fn name(&self) -> &'static str {
        "vector-embedding-page-size"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            let num_rows = rg.num_rows();
            if num_rows <= 0 {
                continue;
            }
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let descr = col.column_descr();
                let is_float = matches!(
                    descr.physical_type(),
                    PhysicalType::FLOAT | PhysicalType::DOUBLE
                );
                let is_repeated = descr.max_rep_level() > 0;
                if !is_float || !is_repeated {
                    continue;
                }
                let avg_values = col.num_values() / num_rows;
                if avg_values >= MIN_ELEMENTS_PER_ROW {
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
                            "column looks like a vector embedding ({avg_values} values/row), \
                             consider smaller page size for random-access lookups"
                        ),
                        fixes: vec![FixAction::SetDataPageSizeLimit(SMALL_PAGE_SIZE)],
                    });
                }
            }
        }
        diagnostics
    }
}
