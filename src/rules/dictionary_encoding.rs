use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::column::page::Page;
use parquet::basic::Encoding;
use parquet::file::reader::FileReader;

pub struct DictionaryEncodingRule;

/// Above this ratio (distinct / num_values), dictionary encoding is not worthwhile.
const HIGH_CARDINALITY_RATIO: f64 = 0.5;
/// Below this ratio, dictionary encoding is clearly beneficial.
const LOW_CARDINALITY_RATIO: f64 = 0.1;
const LARGE_DICT_PAGE_SIZE: usize = 2 * 1024 * 1024; // 2 MB

/// Dictionary fallback detection notes:
/// - Dictionary page itself is encoded with `PLAIN` in Parquet.
/// - Old writers encode dictionary data pages as `PLAIN_DICTIONARY`.
/// - Modern writers encode dictionary data pages as `RLE_DICTIONARY`.
/// - Therefore, column-chunk encoding metadata containing both dictionary encoding and `PLAIN`
///   is ambiguous and does not prove fallback by itself.
/// - True fallback means value data pages contain both dictionary encoding and `PLAIN`.
///
/// Flow chart (per row group / column chunk):
///
///   [num_values == 0] -> skip
///            |
///            v
///   [metadata has dict encoding?]
///      | no
///      v
///   classify as no-dictionary
///      |
///      +---------------------------------------------+
///                                                    |
///   [metadata has plain encoding?]                   |
///      | no                                          |
///      v                                             |
///   classify as dictionary-only (fast path)          |
///      |                                             |
///      +---------------------------------------------+
///                                                    |
///      | yes (ambiguous)                             |
///      v                                             |
///   inspect data pages only                          |
///      |
///      +--> data pages include dict + plain -> fallback
///      |
///      +--> data pages include dict only  -> dictionary-only
///      |
///      +--> data pages include plain only -> no-dictionary
///      |
///      +--> inspection failed / no data page -> unknown (do not warn)
#[derive(Default, Clone, Copy)]
struct DataPageEncodingSummary {
    has_plain_data_pages: bool,
    has_dictionary_data_pages: bool,
}

impl DataPageEncodingSummary {
    fn is_fallback(self) -> bool {
        self.has_plain_data_pages && self.has_dictionary_data_pages
    }
}

fn summarize_data_page_encodings(
    ctx: &RuleContext,
    row_group_idx: usize,
    col_idx: usize,
) -> Option<DataPageEncodingSummary> {
    let rg_reader = ctx.reader.get_row_group(row_group_idx).ok()?;
    let mut page_reader = rg_reader.get_column_page_reader(col_idx).ok()?;
    let mut summary = DataPageEncodingSummary::default();
    let mut seen_data_page = false;

    while let Ok(Some(page)) = page_reader.get_next_page() {
        let encoding = match page {
            Page::DataPage { encoding, .. } | Page::DataPageV2 { encoding, .. } => {
                seen_data_page = true;
                encoding
            }
            Page::DictionaryPage { .. } => continue,
        };

        match encoding {
            Encoding::PLAIN => summary.has_plain_data_pages = true,
            Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                summary.has_dictionary_data_pages = true
            }
            _ => {}
        }

        if summary.is_fallback() {
            break;
        }
    }

    if seen_data_page { Some(summary) } else { None }
}

impl Rule for DictionaryEncodingRule {
    fn name(&self) -> &'static str {
        "dictionary-encoding-cardinality"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let row_groups = ctx.metadata.row_groups();
        if row_groups.is_empty() {
            return diagnostics;
        }

        let num_columns = row_groups[0].num_columns();
        for col_idx in 0..num_columns {
            let path = row_groups[0].column(col_idx).column_path().clone();
            let mut non_empty_groups = 0usize;
            let mut fallback_groups = 0usize;
            let mut no_dict_groups = 0usize;

            for rg_idx in 0..row_groups.len() {
                let col = row_groups[rg_idx].column(col_idx);
                if col.num_values() == 0 {
                    continue;
                }

                non_empty_groups += 1;
                let encodings: Vec<Encoding> = col.encodings().collect();
                let metadata_has_dictionary = encodings
                    .iter()
                    .any(|e| matches!(e, Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY));
                if !metadata_has_dictionary {
                    no_dict_groups += 1;
                    continue;
                }

                let metadata_has_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
                if !metadata_has_plain {
                    // Dictionary data pages present and no plain in metadata:
                    // treat as dictionary-only and skip heavy page inspection.
                    continue;
                }

                // Ambiguous in metadata, inspect actual value data page encodings.
                // If inspection fails, stay conservative and avoid warning.
                if let Some(summary) = summarize_data_page_encodings(ctx, rg_idx, col_idx) {
                    if summary.is_fallback() {
                        fallback_groups += 1;
                    } else if !summary.has_dictionary_data_pages {
                        no_dict_groups += 1;
                    }
                }
            }

            if non_empty_groups == 0 {
                continue;
            }

            let card = &ctx.cardinalities[col_idx];
            let ratio = card.ratio();
            let location = Location::Column {
                column: col_idx,
                path: path.clone(),
            };

            // Dictionary fallback: both dictionary and plain pages present.
            // We only warn when value data pages confirm both dictionary and plain encodings.
            if fallback_groups > 0 {
                if ratio > HIGH_CARDINALITY_RATIO {
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Warning,
                        location,
                        message: format!(
                            "dictionary data pages fell back to PLAIN in {fallback_groups}/{non_empty_groups} row groups; \
                             estimated cardinality is high (~{} distinct / {} total = {:.0}%), \
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
                            "dictionary data pages fell back to PLAIN in {fallback_groups}/{non_empty_groups} row groups; \
                             estimated cardinality is moderate (~{} distinct / {} total = {:.0}%), \
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
            if no_dict_groups > 0 && ratio < LOW_CARDINALITY_RATIO {
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Info,
                    location,
                    message: format!(
                        "low cardinality (~{} distinct / {} total = {:.0}%) and no dictionary in \
                         {no_dict_groups}/{non_empty_groups} row groups; consider enabling dictionary encoding",
                        card.distinct_count, card.total_count, ratio * 100.0
                    ),
                    fixes: vec![FixAction::SetColumnDictionaryEnabled(path, true)],
                });
            }
        }
        diagnostics
    }
}
