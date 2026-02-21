use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, LogicalType, Type as PhysicalType};

pub struct TimestampEncodingRule;

#[async_trait::async_trait]
impl Rule for TimestampEncodingRule {
    fn name(&self) -> &'static str {
        "timestamp-delta-encoding"
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
            let is_int = matches!(
                descr.physical_type(),
                PhysicalType::INT32 | PhysicalType::INT64
            );
            if !is_int {
                continue;
            }

            let is_temporal = matches!(
                descr.logical_type_ref(),
                Some(&LogicalType::Timestamp { .. } | &LogicalType::Date)
            );
            if !is_temporal {
                continue;
            }

            let non_empty_groups = row_groups
                .iter()
                .filter(|rg| rg.column(col_idx).num_values() > 0)
                .count();
            if non_empty_groups == 0 {
                continue;
            }

            let plain_without_delta_groups = row_groups
                .iter()
                .filter(|rg| {
                    let col = rg.column(col_idx);
                    if col.num_values() == 0 {
                        return false;
                    }

                    let encodings: Vec<Encoding> = col.encodings().collect();
                    let uses_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
                    let uses_delta = encodings
                        .iter()
                        .any(|e| matches!(e, Encoding::DELTA_BINARY_PACKED));
                    uses_plain && !uses_delta
                })
                .count();

            if plain_without_delta_groups > 0 {
                let path = col0.column_path().clone();
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Info,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "timestamp/date column uses PLAIN without DELTA_BINARY_PACKED in \
                         {plain_without_delta_groups}/{non_empty_groups} row groups; \
                         DELTA_BINARY_PACKED is typically more efficient for temporal data"
                    ),
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
