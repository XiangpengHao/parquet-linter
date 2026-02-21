use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::LogicalType;
use parquet::basic::Type as PhysicalType;

pub struct BloomFilterRule;

impl Rule for BloomFilterRule {
    fn name(&self) -> &'static str {
        "bloom-filter-recommendation"
    }

    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for (rg_idx, rg) in ctx.metadata.row_groups().iter().enumerate() {
            for (col_idx, col) in rg.columns().iter().enumerate() {
                if col.bloom_filter_offset().is_some() {
                    continue;
                }

                let descr = col.column_descr();
                let physical = descr.physical_type();
                let is_byte_array = matches!(
                    physical,
                    PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
                );
                if !is_byte_array {
                    continue;
                }

                let is_uuid =
                    matches!(descr.logical_type_ref(), Some(&LogicalType::Uuid));

                let high_cardinality = col
                    .statistics()
                    .and_then(|s| s.distinct_count_opt())
                    .is_some_and(|dc| {
                        let nv = col.num_values() as u64;
                        nv > 0 && dc * 2 > nv
                    });

                if is_uuid || high_cardinality {
                    let path = col.column_path().clone();
                    let ndv = col
                        .statistics()
                        .and_then(|s| s.distinct_count_opt())
                        .unwrap_or(10_000);
                    diagnostics.push(Diagnostic {
                        rule_name: self.name(),
                        severity: Severity::Info,
                        location: Location::Column {
                            row_group: rg_idx,
                            column: col_idx,
                            path: path.clone(),
                        },
                        message: if is_uuid {
                            "UUID column without bloom filter; bloom filters enable fast point lookups".to_string()
                        } else {
                            "high-cardinality byte array column without bloom filter".to_string()
                        },
                        fixes: vec![
                            FixAction::SetColumnBloomFilterEnabled(path.clone(), true),
                            FixAction::SetColumnBloomFilterNdv(path, ndv),
                        ],
                    });
                }
            }
        }
        diagnostics
    }
}
