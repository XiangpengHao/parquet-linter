use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, Type as PhysicalType};

pub struct SortedIntegersRule;

impl Rule for SortedIntegersRule {
    fn name(&self) -> &'static str {
        "sorted-integer-delta"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let descr = row_groups[0].column(col_idx).column_descr();
            if !matches!(
                descr.physical_type(),
                PhysicalType::INT32 | PhysicalType::INT64
            ) {
                continue;
            }

            // Check if already using delta encoding
            let already_delta = row_groups.iter().all(|rg| {
                rg.column(col_idx)
                    .encodings()
                    .any(|e| matches!(e, Encoding::DELTA_BINARY_PACKED))
            });
            if already_delta {
                continue;
            }

            // Check if column appears in sorting_columns
            let in_sorting = row_groups.iter().any(|rg| {
                rg.sorting_columns()
                    .is_some_and(|sc| sc.iter().any(|s| s.column_idx as usize == col_idx))
            });

            // Check if min stats are monotonically non-decreasing across row groups
            let monotonic = if row_groups.len() >= 2 {
                let mins: Vec<Option<&[u8]>> = row_groups
                    .iter()
                    .map(|rg| {
                        rg.column(col_idx)
                            .statistics()
                            .and_then(|s| s.min_bytes_opt())
                    })
                    .collect();
                mins.windows(2).all(|w| match (w[0], w[1]) {
                    (Some(a), Some(b)) => a <= b,
                    _ => false,
                })
            } else {
                false
            };

            if in_sorting || monotonic {
                let path = row_groups[0].column(col_idx).column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Info,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: if in_sorting {
                        "sorted integer column; DELTA_BINARY_PACKED encoding is more efficient"
                            .to_string()
                    } else {
                        "integer column appears sorted across row groups; \
                         DELTA_BINARY_PACKED encoding is more efficient"
                            .to_string()
                    },
                    fixes: vec![FixAction::SetColumnEncoding(
                        path,
                        Encoding::DELTA_BINARY_PACKED,
                    )],
                });
            }
        }
        diagnostics
    }
}
