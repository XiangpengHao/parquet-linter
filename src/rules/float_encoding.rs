use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, Type as PhysicalType};

pub struct FloatEncodingRule;

impl Rule for FloatEncodingRule {
    fn name(&self) -> &'static str {
        "float-byte-stream-split"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let descr = col.column_descr();
                let is_scalar_float = matches!(
                    descr.physical_type(),
                    PhysicalType::FLOAT | PhysicalType::DOUBLE
                ) && descr.max_rep_level() == 0;

                if !is_scalar_float {
                    continue;
                }

                let encodings: Vec<Encoding> = col.encodings().collect();
                let uses_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
                let uses_bss = encodings
                    .iter()
                    .any(|e| matches!(e, Encoding::BYTE_STREAM_SPLIT));

                if uses_plain && !uses_bss {
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
                            "scalar float column using PLAIN encoding; \
                             BYTE_STREAM_SPLIT typically compresses 2-4x better"
                                .to_string(),
                        fixes: vec![FixAction::SetColumnEncoding(
                            path,
                            Encoding::BYTE_STREAM_SPLIT,
                        )],
                    });
                }
            }
        }
        diagnostics
    }
}
