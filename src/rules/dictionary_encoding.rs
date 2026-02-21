use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::Encoding;
use parquet::basic::PageType;
use parquet::column::page::Page;
use parquet::file::metadata::{ColumnChunkMetaData, PageEncodingStats};
use parquet::file::reader::FileReader;

pub struct DictionaryEncodingRule;

/// Above this ratio (distinct / num_values), dictionary encoding is not worthwhile.
const HIGH_CARDINALITY_RATIO: f64 = 0.5;
/// Below this ratio, dictionary encoding is clearly beneficial.
const LOW_CARDINALITY_RATIO: f64 = 0.1;
const LARGE_DICT_PAGE_SIZE: usize = 2 * 1024 * 1024; // 2 MB
const AMBIGUOUS_GROUP_SAMPLE_RATIO: f64 = 0.05;

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
///   [has page_encoding_stats(_mask)?]                |
///      | yes                                         |
///      v                                             |
///   classify from data-page encodings in metadata    |
///      |                                             |
///      +---------------------------------------------+
///                                                    |
///      | no                                          |
///      v                                             |
///   [metadata has plain encoding?]                   |
///      | no                                          |
///      v                                             |
///   classify as dictionary-only (fast path)          |
///      |                                             |
///      +---------------------------------------------+
///                                                    |
///      | yes (ambiguous)                             |
///      v                                             |
///   inspect data pages only for sampled 5% of        |
///   ambiguous row groups                             |
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
    fn classify(self) -> ChunkDictionaryState {
        match (self.has_dictionary_data_pages, self.has_plain_data_pages) {
            (true, true) => ChunkDictionaryState::Fallback,
            (true, false) => ChunkDictionaryState::DictionaryOnly,
            (false, true) => ChunkDictionaryState::NoDictionary,
            (false, false) => ChunkDictionaryState::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkDictionaryState {
    Fallback,
    DictionaryOnly,
    NoDictionary,
    Unknown,
}

fn summarize_metadata_page_encodings(
    page_stats: &[PageEncodingStats],
) -> Option<DataPageEncodingSummary> {
    let mut summary = DataPageEncodingSummary::default();
    let mut saw_data_page = false;

    for stat in page_stats {
        if !matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2) {
            continue;
        }
        saw_data_page = true;
        match stat.encoding {
            Encoding::PLAIN => summary.has_plain_data_pages = true,
            Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                summary.has_dictionary_data_pages = true
            }
            _ => {}
        }
        if matches!(summary.classify(), ChunkDictionaryState::Fallback) {
            break;
        }
    }

    saw_data_page.then_some(summary)
}

fn classify_from_metadata(col: &ColumnChunkMetaData) -> ChunkDictionaryState {
    let encodings: Vec<Encoding> = col.encodings().collect();
    let metadata_has_dictionary = encodings
        .iter()
        .any(|e| matches!(e, Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY));
    if !metadata_has_dictionary {
        return ChunkDictionaryState::NoDictionary;
    }

    if let Some(mask) = col.page_encoding_stats_mask() {
        let has_plain = mask.is_set(Encoding::PLAIN);
        let has_dictionary =
            mask.is_set(Encoding::PLAIN_DICTIONARY) || mask.is_set(Encoding::RLE_DICTIONARY);
        return match (has_dictionary, has_plain) {
            (true, true) => ChunkDictionaryState::Fallback,
            (true, false) => ChunkDictionaryState::DictionaryOnly,
            (false, true) => ChunkDictionaryState::NoDictionary,
            (false, false) => ChunkDictionaryState::Unknown,
        };
    }

    if let Some(page_stats) = col.page_encoding_stats() {
        if let Some(summary) = summarize_metadata_page_encodings(page_stats) {
            return summary.classify();
        }
    }

    let metadata_has_plain = encodings.iter().any(|e| matches!(e, Encoding::PLAIN));
    if metadata_has_plain {
        ChunkDictionaryState::Unknown
    } else {
        ChunkDictionaryState::DictionaryOnly
    }
}

fn choose_sample_row_groups(indices: &[usize]) -> Vec<usize> {
    if indices.is_empty() {
        return Vec::new();
    }

    let target = ((indices.len() as f64) * AMBIGUOUS_GROUP_SAMPLE_RATIO).ceil() as usize;
    let sample_count = target.max(1).min(indices.len());
    if sample_count == indices.len() {
        return indices.to_vec();
    }

    (0..sample_count)
        .map(|i| {
            let pos = i * indices.len() / sample_count;
            indices[pos]
        })
        .collect()
}

fn classify_from_sampled_pages(
    ctx: &RuleContext,
    row_group_idx: usize,
    col_idx: usize,
) -> ChunkDictionaryState {
    let Some(summary) = summarize_data_page_encodings(ctx, row_group_idx, col_idx) else {
        return ChunkDictionaryState::Unknown;
    };
    summary.classify()
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

        if matches!(summary.classify(), ChunkDictionaryState::Fallback) {
            break;
        }
    }

    seen_data_page.then_some(summary)
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
            let mut ambiguous_groups = Vec::new();

            for rg_idx in 0..row_groups.len() {
                let col = row_groups[rg_idx].column(col_idx);
                if col.num_values() == 0 {
                    continue;
                }

                non_empty_groups += 1;
                match classify_from_metadata(col) {
                    ChunkDictionaryState::Fallback => fallback_groups += 1,
                    ChunkDictionaryState::NoDictionary => no_dict_groups += 1,
                    ChunkDictionaryState::DictionaryOnly => {}
                    ChunkDictionaryState::Unknown => ambiguous_groups.push(rg_idx),
                }
            }

            if non_empty_groups == 0 {
                continue;
            }

            let sampled_ambiguous_groups = choose_sample_row_groups(&ambiguous_groups);
            let mut sampled_fallback_groups = 0usize;
            let mut sampled_no_dict_groups = 0usize;
            for rg_idx in sampled_ambiguous_groups.iter().copied() {
                match classify_from_sampled_pages(ctx, rg_idx, col_idx) {
                    ChunkDictionaryState::Fallback => sampled_fallback_groups += 1,
                    ChunkDictionaryState::NoDictionary => sampled_no_dict_groups += 1,
                    ChunkDictionaryState::DictionaryOnly | ChunkDictionaryState::Unknown => {}
                }
            }

            fallback_groups += sampled_fallback_groups;
            no_dict_groups += sampled_no_dict_groups;

            let card = &ctx.cardinalities[col_idx];
            let ratio = card.ratio();
            let location = Location::Column {
                column: col_idx,
                path: path.clone(),
            };

            // Dictionary fallback: both dictionary and plain pages present.
            // Detection is exact when page-encoding metadata is present, and sampled otherwise.
            if fallback_groups > 0 {
                let sampled_suffix = if sampled_ambiguous_groups.is_empty() {
                    String::new()
                } else {
                    format!(
                        " (plus {} sampled ambiguous row groups)",
                        sampled_ambiguous_groups.len()
                    )
                };
                if ratio > HIGH_CARDINALITY_RATIO {
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Warning,
                        location,
                        message: format!(
                            "dictionary data pages fell back to PLAIN in {fallback_groups}/{non_empty_groups} row groups{sampled_suffix}; \
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
                            "dictionary data pages fell back to PLAIN in {fallback_groups}/{non_empty_groups} row groups{sampled_suffix}; \
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
