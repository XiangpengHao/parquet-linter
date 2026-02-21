use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, LogicalType, Type as PhysicalType};

pub struct TimestampEncodingRule;

impl Rule for TimestampEncodingRule {
    fn name(&self) -> &'static str {
        "timestamp-delta-encoding"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let descr = col.column_descr();
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

                let encodings: Vec<Encoding> = col.encodings().collect();
                let uses_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
                let uses_delta = encodings
                    .iter()
                    .any(|e| matches!(e, Encoding::DELTA_BINARY_PACKED));

                if uses_plain && !uses_delta {
                    let path = col.column_path().clone();
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Info,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message:
                            "timestamp/date column using PLAIN encoding; \
                             DELTA_BINARY_PACKED is typically more efficient for temporal data"
                                .to_string(),
                        fixes: vec![FixAction::SetColumnEncoding(
                            path,
                            Encoding::DELTA_BINARY_PACKED,
                        )],
                    });
                }
            }
        }
        diagnostics
    }
}
