use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{DataEncoding, Directive, Prescription};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, Type as PhysicalType};

pub struct FloatEncodingRule;

/// Below this ratio, dictionary encoding is better than BYTE_STREAM_SPLIT.
const LOW_CARDINALITY_RATIO: f64 = 0.1;

#[async_trait::async_trait]
impl Rule for FloatEncodingRule {
    fn name(&self) -> &'static str {
        "float-byte-stream-split"
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
            let is_scalar_float = matches!(
                descr.physical_type(),
                PhysicalType::FLOAT | PhysicalType::DOUBLE
            ) && descr.max_rep_level() == 0;

            if !is_scalar_float {
                continue;
            }

            // Low cardinality floats are better served by dictionary encoding
            if ctx.cardinalities[col_idx].ratio() < LOW_CARDINALITY_RATIO {
                continue;
            }

            let non_empty_groups = row_groups
                .iter()
                .filter(|rg| rg.column(col_idx).num_values() > 0)
                .count();
            if non_empty_groups == 0 {
                continue;
            }

            let plain_without_bss_groups = row_groups
                .iter()
                .filter(|rg| {
                    let col = rg.column(col_idx);
                    if col.num_values() == 0 {
                        return false;
                    }
                    let encodings: Vec<Encoding> = col.encodings().collect();
                    let uses_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
                    let uses_bss = encodings
                        .iter()
                        .any(|e| matches!(e, Encoding::BYTE_STREAM_SPLIT));
                    uses_plain && !uses_bss
                })
                .count();

            if plain_without_bss_groups > 0 {
                let path = col0.column_path().clone();
                let mut prescription = Prescription::new();
                prescription.push(Directive::SetColumnEncoding(
                    path.clone(),
                    DataEncoding::ByteStreamSplit,
                ));
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Suggestion,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: format!(
                        "scalar float column uses PLAIN without BYTE_STREAM_SPLIT in \
                         {plain_without_bss_groups}/{non_empty_groups} row groups; \
                         BYTE_STREAM_SPLIT typically compresses 2-4x better"
                    ),
                    prescription,
                });
            }
        }
        diagnostics
    }
}
