use parquet::column::page::PageReader;
use parquet::schema::types::ColumnPath;

use crate::diagnostic::{Diagnostic, Location, Severity};
use crate::prescription::Prescription;
use crate::rule::{self, Rule, RuleContext};

pub struct GpuPageCountRule;

const RECOMMENDED_PAGES_PER_ROW_GROUP: usize = 100;

#[derive(Clone, Copy)]
struct RowGroupPageCount {
    total_pages: usize,
    non_empty_columns: usize,
}

struct ColumnPageCount {
    col_idx: usize,
    path: ColumnPath,
    pages: usize,
}

struct ColumnPageAggregate {
    path: ColumnPath,
    total_pages: usize,
    non_empty_row_groups: usize,
}

async fn row_group_column_page_counts(
    ctx: &RuleContext,
    row_group_idx: usize,
) -> Option<Vec<ColumnPageCount>> {
    let row_group = ctx.metadata.row_group(row_group_idx);

    // Fast path: use page locations from offset index when the file has page index metadata.
    if let Some(offset_index) = ctx.metadata.offset_index() {
        let mut counts = Vec::new();
        for col_idx in 0..row_group.num_columns() {
            let col = row_group.column(col_idx);
            if col.num_values() == 0 {
                continue;
            }
            let mut pages = offset_index[row_group_idx][col_idx].page_locations().len();
            if col.dictionary_page_offset().is_some() {
                pages += 1;
            }
            counts.push(ColumnPageCount {
                col_idx,
                path: col.column_path().clone(),
                pages,
            });
        }
        return Some(counts);
    }

    let mut counts = Vec::new();
    // Fallback: scan pages in each column chunk when offset index is unavailable.
    for col_idx in 0..row_group.num_columns() {
        let col = row_group.column(col_idx);
        if col.num_values() == 0 {
            continue;
        }

        let mut pages = 0usize;
        let mut page_reader =
            rule::column_page_reader(&ctx.reader, &ctx.metadata, row_group_idx, col_idx)
                .await
                .ok()?;
        while let Ok(Some(_page)) = page_reader.get_next_page() {
            pages += 1;
        }
        counts.push(ColumnPageCount {
            col_idx,
            path: col.column_path().clone(),
            pages,
        });
    }
    Some(counts)
}

#[async_trait::async_trait]
impl Rule for GpuPageCountRule {
    fn name(&self) -> &'static str {
        "gpu-page-count"
    }

    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        if !ctx.gpu {
            return Vec::new();
        }

        let total_row_groups = ctx.metadata.num_row_groups();
        let mut first_bad: Option<(usize, RowGroupPageCount)> = None;
        let mut column_aggregates: Vec<Option<ColumnPageAggregate>> = Vec::new();
        let mut read_failed_row_groups = 0usize;

        for rg_idx in 0..total_row_groups {
            let row_group = ctx.metadata.row_group(rg_idx);
            if row_group.num_rows() <= 0 {
                continue;
            }

            let Some(column_counts) = row_group_column_page_counts(ctx, rg_idx).await else {
                read_failed_row_groups += 1;
                continue;
            };
            if column_aggregates.is_empty() {
                column_aggregates.resize_with(row_group.num_columns(), || None);
            }

            let mut page_count = RowGroupPageCount {
                total_pages: 0,
                non_empty_columns: 0,
            };
            // Aggregate column page counts across row groups.
            for column in column_counts {
                page_count.total_pages += column.pages;
                page_count.non_empty_columns += 1;

                let slot = &mut column_aggregates[column.col_idx];
                match slot {
                    Some(agg) => {
                        agg.total_pages += column.pages;
                        agg.non_empty_row_groups += 1;
                    }
                    None => {
                        *slot = Some(ColumnPageAggregate {
                            path: column.path,
                            total_pages: column.pages,
                            non_empty_row_groups: 1,
                        });
                    }
                }
            }

            if page_count.total_pages >= RECOMMENDED_PAGES_PER_ROW_GROUP {
                continue;
            }

            if first_bad.is_none() {
                first_bad = Some((rg_idx, page_count));
            }
        }

        let Some((first_bad_idx, first_bad)) = first_bad else {
            if read_failed_row_groups > 0 {
                return vec![Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::File,
                    message: format!(
                        "page count check could not read page counts for {read_failed_row_groups} row groups; results may be incomplete"
                    ),
                    prescription: Prescription::new(),
                }];
            }
            return Vec::new();
        };

        let mut diagnostics = Vec::new();

        for (col_idx, aggregate) in column_aggregates.into_iter().enumerate() {
            if let Some(aggregate) = aggregate {
                let avg_pages =
                    aggregate.total_pages as f64 / aggregate.non_empty_row_groups.max(1) as f64;
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Warning,
                    location: Location::Column {
                        column: col_idx,
                        path: aggregate.path.clone(),
                    },
                    message: format!(
                        "page count detail: this column averages {:.1} pages per non-empty row group ({} total pages across {}/{} row groups); for GPU scans, Parquet is recommended to have more than {} pages in each row group",
                        avg_pages,
                        aggregate.total_pages,
                        aggregate.non_empty_row_groups,
                        total_row_groups,
                        RECOMMENDED_PAGES_PER_ROW_GROUP
                    ),
                    prescription: Prescription::new(),
                });
            }
        }

        if diagnostics.is_empty() {
            diagnostics.push(Diagnostic {
                rule_name: self.name(),
                severity: Severity::Warning,
                location: Location::RowGroup {
                    index: first_bad_idx,
                },
                message: format!(
                    "page count check failed for row_group[{first_bad_idx}] ({} total pages across {} non-empty column chunks); column averages unavailable",
                    first_bad.total_pages, first_bad.non_empty_columns
                ),
                prescription: Prescription::new(),
            });
        }

        if read_failed_row_groups > 0 {
            diagnostics.push(Diagnostic {
                rule_name: self.name(),
                severity: Severity::Warning,
                location: Location::File,
                message: format!(
                    "page count check could not read page counts for {read_failed_row_groups} row groups; column averages may be incomplete"
                ),
                prescription: Prescription::new(),
            });
        }

        diagnostics
    }
}
