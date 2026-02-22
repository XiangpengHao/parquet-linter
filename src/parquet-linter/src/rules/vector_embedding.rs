use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{Directive, Prescription};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Type as PhysicalType;

pub struct VectorEmbeddingRule;

// 8KB pages are good for point lookups but often hurt full scans. Use a
// moderate page size to preserve most benefits without over-fragmenting data.
const SMALL_PAGE_SIZE: usize = 256 * 1024; // 256 KB
const MIN_ELEMENTS_PER_ROW: i64 = 64;

#[async_trait::async_trait]
impl Rule for VectorEmbeddingRule {
    fn name(&self) -> &'static str {
        "vector-embedding-page-size"
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
            let descr = col0.column_descr();
            let is_float = matches!(
                descr.physical_type(),
                PhysicalType::FLOAT | PhysicalType::DOUBLE
            );
            let is_repeated = descr.max_rep_level() > 0;
            if !is_float || !is_repeated {
                continue;
            }

            let mut total_rows = 0i64;
            let mut total_values = 0i64;
            for rg in row_groups {
                let num_rows = rg.num_rows();
                if num_rows <= 0 {
                    continue;
                }
                total_rows += num_rows;
                total_values += rg.column(col_idx).num_values();
            }

            if total_rows <= 0 {
                continue;
            }

            let avg_values = total_values / total_rows;
            if avg_values >= MIN_ELEMENTS_PER_ROW {
                let path = col0.column_path().clone();
                let mut prescription = Prescription::new();
                prescription.push(Directive::SetFileDataPageSizeLimit(SMALL_PAGE_SIZE));
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "column looks like a vector embedding ({avg_values} values/row on average), \
                         consider smaller page size for random-access lookups"
                    ),
                    prescription,
                });
            }
        }
        diagnostics
    }
}
