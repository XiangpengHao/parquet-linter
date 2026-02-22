use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{DataEncoding, Directive, Prescription};
use crate::rule::{Rule, RuleContext};
use parquet::basic::{Encoding, LogicalType, Type as PhysicalType};

pub struct StringEncodingRule;

const MIN_TOTAL_BYTES: i64 = 32 * 1024 * 1024; // 32 MB
const MIN_NON_EMPTY_GROUPS: usize = 2;
const MAX_NON_EMPTY_GROUPS: usize = 32;
const MIN_AVG_CHUNK_BYTES: i64 = 4 * 1024 * 1024; // 4 MB
const MIN_RATIO: f64 = 0.35;
const MAX_RATIO: f64 = 0.75;
const SMALL_CHUNK_MIN_TOTAL_BYTES: i64 = 64 * 1024 * 1024; // 64 MB
const SMALL_CHUNK_MIN_GROUPS: usize = 64;
const SMALL_CHUNK_MAX_AVG_CHUNK_BYTES: i64 = 1024 * 1024; // 1 MB
const SMALL_CHUNK_MIN_RATIO: f64 = 0.55;
const SMALL_CHUNK_MAX_RATIO: f64 = 0.85;

#[derive(Debug, Clone, Copy, PartialEq)]
struct StringColumnSummary {
    total_uncompressed: i64,
    total_compressed: i64,
    non_empty_groups: usize,
}

impl StringColumnSummary {
    fn aggregated_ratio(self) -> Option<f64> {
        if self.total_uncompressed > 0 && self.total_compressed > 0 {
            Some(self.total_compressed as f64 / self.total_uncompressed as f64)
        } else {
            None
        }
    }

    fn avg_chunk_uncompressed(self) -> i64 {
        if self.non_empty_groups == 0 {
            0
        } else {
            self.total_uncompressed / self.non_empty_groups as i64
        }
    }
}

fn looks_text_column(logical_type: Option<&LogicalType>, path: &str) -> bool {
    if matches!(
        logical_type,
        Some(LogicalType::String | LogicalType::Json | LogicalType::Enum | LogicalType::Bson)
    ) {
        return true;
    }

    let path = path.to_ascii_lowercase();
    !(path.contains("bytes") || path.contains("embedding") || path.contains("image"))
}

fn should_prefer_delta_length_byte_array(
    summary: StringColumnSummary,
    logical_type: Option<&LogicalType>,
    path: &str,
    has_plain: bool,
    has_dictionary: bool,
    already_delta_byte_array: bool,
) -> bool {
    if already_delta_byte_array || !has_plain || !has_dictionary {
        return false;
    }
    if !looks_text_column(logical_type, path) {
        return false;
    }
    let Some(ratio) = summary.aggregated_ratio() else {
        return false;
    };
    let avg_chunk = summary.avg_chunk_uncompressed();

    let moderate_multi_group_large_chunks = summary.total_uncompressed >= MIN_TOTAL_BYTES
        && (MIN_NON_EMPTY_GROUPS..=MAX_NON_EMPTY_GROUPS).contains(&summary.non_empty_groups)
        && avg_chunk >= MIN_AVG_CHUNK_BYTES
        && (MIN_RATIO..=MAX_RATIO).contains(&ratio);

    let many_small_chunks = summary.total_uncompressed >= SMALL_CHUNK_MIN_TOTAL_BYTES
        && summary.non_empty_groups >= SMALL_CHUNK_MIN_GROUPS
        && avg_chunk > 0
        && avg_chunk <= SMALL_CHUNK_MAX_AVG_CHUNK_BYTES
        && (SMALL_CHUNK_MIN_RATIO..=SMALL_CHUNK_MAX_RATIO).contains(&ratio);

    moderate_multi_group_large_chunks || many_small_chunks
}

#[async_trait::async_trait]
impl Rule for StringEncodingRule {
    fn name(&self) -> &'static str {
        "string-byte-array-encoding"
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
            if descr.physical_type() != PhysicalType::BYTE_ARRAY {
                continue;
            }

            let path = col0.column_path().string();
            let logical_type = descr.logical_type_ref();

            let mut summary = StringColumnSummary {
                total_uncompressed: 0,
                total_compressed: 0,
                non_empty_groups: 0,
            };
            let mut has_plain = false;
            let mut has_dictionary = false;
            let mut already_delta_byte_array = false;

            for rg in row_groups {
                let col = rg.column(col_idx);
                let unc = col.uncompressed_size();
                let comp = col.compressed_size();
                if unc > 0 {
                    summary.total_uncompressed += unc;
                    summary.non_empty_groups += 1;
                }
                if comp > 0 {
                    summary.total_compressed += comp;
                }
                for enc in col.encodings() {
                    match enc {
                        Encoding::PLAIN => has_plain = true,
                        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                            has_dictionary = true
                        }
                        Encoding::DELTA_BYTE_ARRAY | Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                            already_delta_byte_array = true
                        }
                        _ => {}
                    }
                }
            }

            if !should_prefer_delta_length_byte_array(
                summary,
                logical_type,
                &path,
                has_plain,
                has_dictionary,
                already_delta_byte_array,
            ) {
                continue;
            }

            let ratio = summary.aggregated_ratio().unwrap_or(0.0);
            let mut prescription = Prescription::new();
            let path_obj = col0.column_path().clone();
            prescription.push(Directive::SetColumnDictionary(path_obj.clone(), false));
            prescription.push(Directive::SetColumnEncoding(
                path_obj.clone(),
                DataEncoding::DeltaLengthByteArray,
            ));
            diagnostics.push(Diagnostic {
                rule_name: self.name(),
                severity: Severity::Suggestion,
                location: Location::Column {
                    column: col_idx,
                    path: path_obj.clone(),
                },
                message: format!(
                    "text column ({:.1}MB across {}/{} row groups, ratio {:.2}) uses dictionary/plain \
                     pages; try DELTA_LENGTH_BYTE_ARRAY and disable dictionary",
                    summary.total_uncompressed as f64 / (1024.0 * 1024.0),
                    summary.non_empty_groups,
                    row_groups.len(),
                    ratio,
                ),
                prescription,
            });
        }

        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_file4_like_large_multi_group_text() {
        let summary = StringColumnSummary {
            total_uncompressed: 163_000_000,
            total_compressed: 72_000_000,
            non_empty_groups: 5,
        };
        assert!(should_prefer_delta_length_byte_array(
            summary,
            Some(&LogicalType::String),
            "reasoning_content",
            true,
            true,
            false
        ));
    }

    #[test]
    fn rejects_single_row_group_giant_text() {
        let summary = StringColumnSummary {
            total_uncompressed: 520_000_000,
            total_compressed: 110_000_000,
            non_empty_groups: 1,
        };
        assert!(!should_prefer_delta_length_byte_array(
            summary,
            Some(&LogicalType::String),
            "content",
            true,
            true,
            false
        ));
    }

    #[test]
    fn selects_file6_like_many_tiny_chunk_text() {
        let summary = StringColumnSummary {
            total_uncompressed: 99_000_000,
            total_compressed: 63_000_000,
            non_empty_groups: 240,
        };
        assert!(should_prefer_delta_length_byte_array(
            summary,
            Some(&LogicalType::String),
            "content",
            true,
            true,
            false
        ));
    }
}
