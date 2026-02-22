use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::{Directive, Prescription};
use crate::rule::{Rule, RuleContext};

pub struct PageSizeRule;

const MAX_ROWS_PER_ROW_GROUP: usize = 64 * 1024; // 64K rows
const MAX_ROW_GROUP_SIZE_BYTES: i64 = 256 * 1024 * 1024; // 256 MB
const HARD_MAX_DATA_PAGE_SIZE_LIMIT: usize = 4 * 1024 * 1024; // 4 MB
const IDEAL_DATA_PAGE_SIZE_LIMIT: usize = 1024 * 1024; // 1 MB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowGroupSuggestion {
    target_max_rows: usize,
    oversized_rows_groups: usize,
    oversized_size_groups: usize,
}

fn compute_row_group_suggestion(row_groups: &[(i64, i64)]) -> Option<RowGroupSuggestion> {
    let mut oversized_rows_groups = 0usize;
    let mut oversized_size_groups = 0usize;
    let mut target_max_rows = MAX_ROWS_PER_ROW_GROUP;

    for (num_rows, compressed_size) in row_groups {
        if *num_rows > MAX_ROWS_PER_ROW_GROUP as i64 {
            oversized_rows_groups += 1;
        }

        if *compressed_size > MAX_ROW_GROUP_SIZE_BYTES {
            oversized_size_groups += 1;
            if *num_rows > 0 {
                // Reduce rows proportionally so compressed size trends toward <= 256MB.
                let scaled = ((*num_rows as f64) * (MAX_ROW_GROUP_SIZE_BYTES as f64)
                    / (*compressed_size as f64))
                    .floor() as usize;
                target_max_rows = target_max_rows.min(scaled.max(1));
            }
        }
    }

    if oversized_rows_groups == 0 && oversized_size_groups == 0 {
        return None;
    }

    Some(RowGroupSuggestion {
        target_max_rows,
        oversized_rows_groups,
        oversized_size_groups,
    })
}

fn build_policy_message(suggestion: RowGroupSuggestion, total_row_groups: usize) -> String {
    let mut parts = Vec::new();
    if suggestion.oversized_rows_groups > 0 {
        parts.push(format!(
            "{}/{} row group(s) exceed {}K rows",
            suggestion.oversized_rows_groups,
            total_row_groups,
            MAX_ROWS_PER_ROW_GROUP / 1024
        ));
    }
    if suggestion.oversized_size_groups > 0 {
        parts.push(format!(
            "{}/{} row group(s) exceed {}MB compressed",
            suggestion.oversized_size_groups,
            total_row_groups,
            MAX_ROW_GROUP_SIZE_BYTES / 1024 / 1024
        ));
    }

    format!(
        "{}; set max_row_group_size={} ({}K rows). Recommended data_page_size_limit={}MB (hard max {}MB).",
        parts.join("; "),
        suggestion.target_max_rows,
        MAX_ROWS_PER_ROW_GROUP / 1024,
        IDEAL_DATA_PAGE_SIZE_LIMIT / 1024 / 1024,
        HARD_MAX_DATA_PAGE_SIZE_LIMIT / 1024 / 1024,
    )
}

#[async_trait::async_trait]
impl Rule for PageSizeRule {
    fn name(&self) -> &'static str {
        "page-row-group-size"
    }

    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let row_groups: Vec<_> = ctx
            .metadata
            .row_groups()
            .iter()
            .map(|rg| (rg.num_rows(), rg.compressed_size()))
            .collect();

        let Some(suggestion) = compute_row_group_suggestion(&row_groups) else {
            return Vec::new();
        };

        let mut prescription = Prescription::new();
        prescription.push(Directive::SetFileMaxRowGroupSize(
            suggestion.target_max_rows,
        ));
        prescription.push(Directive::SetFileDataPageSizeLimit(
            IDEAL_DATA_PAGE_SIZE_LIMIT,
        ));

        vec![Diagnostic {
            rule_name: self.name(),
            severity: Severity::Warning,
            location: Location::File,
            message: build_policy_message(suggestion, row_groups.len()),
            prescription,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_violation_returns_none() {
        let row_groups = vec![(10_000, 64 * 1024 * 1024), (20_000, 128 * 1024 * 1024)];
        assert_eq!(compute_row_group_suggestion(&row_groups), None);
    }

    #[test]
    fn rows_violation_caps_at_64k() {
        let row_groups = vec![(70_000, 128 * 1024 * 1024)];
        assert_eq!(
            compute_row_group_suggestion(&row_groups),
            Some(RowGroupSuggestion {
                target_max_rows: 64 * 1024,
                oversized_rows_groups: 1,
                oversized_size_groups: 0,
            })
        );
    }

    #[test]
    fn size_violation_scales_rows_down() {
        let row_groups = vec![(100_000, 512 * 1024 * 1024)];
        assert_eq!(
            compute_row_group_suggestion(&row_groups),
            Some(RowGroupSuggestion {
                target_max_rows: 50_000,
                oversized_rows_groups: 1,
                oversized_size_groups: 1,
            })
        );
    }

    #[test]
    fn message_mentions_only_violated_constraints() {
        let msg = build_policy_message(
            RowGroupSuggestion {
                target_max_rows: 64 * 1024,
                oversized_rows_groups: 226,
                oversized_size_groups: 0,
            },
            226,
        );
        assert!(msg.contains("226/226 row group(s) exceed 64K rows"));
        assert!(!msg.contains("exceed 256MB compressed"));
        assert!(msg.contains("data_page_size_limit=1MB"));
    }
}
