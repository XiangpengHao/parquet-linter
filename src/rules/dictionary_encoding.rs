use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Encoding;

pub struct DictionaryEncodingRule;

impl Rule for DictionaryEncodingRule {
    fn name(&self) -> &'static str {
        "dictionary-encoding-cardinality"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let encodings: Vec<Encoding> = col.encodings().collect();
                let has_dict = encodings
                    .iter()
                    .any(|e| matches!(e, Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY));
                let has_fallback = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));

                // Case A: dictionary fallback occurred (high cardinality + dict)
                if has_dict && has_fallback {
                    let path = col.column_path().clone();
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Warning,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message:
                            "dictionary encoding fell back to plain (high cardinality); \
                             consider disabling dictionary encoding"
                                .to_string(),
                        fixes: vec![FixAction::SetColumnDictionaryEnabled(path, false)],
                    });
                    continue;
                }

                // Case B: low cardinality without dictionary
                if !has_dict {
                    if let Some(stats) = col.statistics() {
                        if let Some(distinct) = stats.distinct_count_opt() {
                            let num_values = col.num_values() as u64;
                            if num_values > 0 && distinct * 10 < num_values {
                                let path = col.column_path().clone();
                                diagnostics.push(Diagnostic {
                                    rule_name: self.name(),
                                    severity: Severity::Info,
                                    location: Location::Column {
                                        row_group: rg_idx,
                                        column: col_idx,
                                        path: path.clone(),
                                    },
                                    message: format!(
                                        "low cardinality ({distinct} distinct / {num_values} values), \
                                         consider enabling dictionary encoding"
                                    ),
                                    fixes: vec![FixAction::SetColumnDictionaryEnabled(path, true)],
                                });
                            }
                        }
                    }
                }
            }
        }
        diagnostics
    }
}
