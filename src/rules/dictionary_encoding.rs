use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Encoding;

pub struct DictionaryEncodingRule;

/// Above this ratio (distinct / num_values), dictionary encoding is not worthwhile.
const HIGH_CARDINALITY_RATIO: f64 = 0.5;
/// Below this ratio, dictionary encoding is clearly beneficial.
const LOW_CARDINALITY_RATIO: f64 = 0.1;
const LARGE_DICT_PAGE_SIZE: usize = 2 * 1024 * 1024; // 2 MB

impl Rule for DictionaryEncodingRule {
    fn name(&self) -> &'static str {
        "dictionary-encoding-cardinality"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if col.num_values() == 0 {
                    continue;
                }

                let encodings: Vec<Encoding> = col.encodings().collect();
                let has_dict = encodings
                    .iter()
                    .any(|e| matches!(e, Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY));
                let has_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));

                let card = &ctx.cardinalities[col_idx];
                let ratio = card.ratio();
                let path = col.column_path().clone();
                let location = Location::Column {
                    row_group: rg_idx,
                    column: col_idx,
                    path: path.clone(),
                };

                // Dictionary fallback: both dictionary and plain pages present.
                // Note: some writers list PLAIN in encodings even without true fallback
                // (e.g., the dictionary page itself uses PLAIN encoding). We use estimated
                // cardinality to decide the right fix rather than relying solely on this signal.
                if has_dict && has_plain {
                    if ratio > HIGH_CARDINALITY_RATIO {
                        diagnostics.push(Diagnostic {
                            rule_name: self.name(),
                            severity: Severity::Warning,
                            location,
                            message: format!(
                                "dictionary fell back to plain; estimated cardinality is high \
                                 (~{} distinct / {} total = {:.0}%), \
                                 dictionary encoding is not beneficial",
                                card.distinct_count, card.total_count, ratio * 100.0
                            ),
                            fixes: vec![FixAction::SetColumnDictionaryEnabled(path, false)],
                        });
                    } else {
                        diagnostics.push(Diagnostic {
                            rule_name: self.name(),
                            severity: Severity::Warning,
                            location,
                            message: format!(
                                "dictionary fell back to plain; estimated cardinality is moderate \
                                 (~{} distinct / {} total = {:.0}%), \
                                 dictionary page size may be too small",
                                card.distinct_count, card.total_count, ratio * 100.0
                            ),
                            fixes: vec![FixAction::SetColumnDictionaryPageSizeLimit(
                                path,
                                LARGE_DICT_PAGE_SIZE,
                            )],
                        });
                    }
                    continue;
                }

                // No dictionary, but cardinality is low → suggest enabling.
                if !has_dict && ratio < LOW_CARDINALITY_RATIO {
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Info,
                        location,
                        message: format!(
                            "low cardinality (~{} distinct / {} total = {:.0}%), \
                             consider enabling dictionary encoding",
                            card.distinct_count, card.total_count, ratio * 100.0
                        ),
                        fixes: vec![FixAction::SetColumnDictionaryEnabled(path, true)],
                    });
                }
            }
        }
        diagnostics
    }
}
