use crate::diagnostic::{Diagnostic, FixAction, Location, Severity};
use crate::rule::{Rule, RuleContext};
use parquet::basic::LogicalType;
use parquet::basic::Type as PhysicalType;

pub struct BloomFilterRule;

const HIGH_CARDINALITY_RATIO: f64 = 0.5;

#[async_trait::async_trait]
impl Rule for BloomFilterRule {
    fn name(&self) -> &'static str {
        "bloom-filter-recommendation"
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
            let is_byte_array = matches!(
                descr.physical_type(),
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
            );
            if !is_byte_array {
                continue;
            }

            let non_empty_groups = row_groups
                .iter()
                .filter(|rg| rg.column(col_idx).num_values() > 0)
                .count();
            if non_empty_groups == 0 {
                continue;
            }

            let missing_bloom_groups = row_groups
                .iter()
                .filter(|rg| {
                    let col = rg.column(col_idx);
                    col.num_values() > 0 && col.bloom_filter_offset().is_none()
                })
                .count();
            if missing_bloom_groups == 0 {
                continue;
            }

            let is_uuid = matches!(descr.logical_type_ref(), Some(&LogicalType::Uuid));
            let card = &ctx.cardinalities[col_idx];
            let high_cardinality = card.ratio() > HIGH_CARDINALITY_RATIO;

            if is_uuid || high_cardinality {
                let path = col0.column_path().clone();
                let ndv = card.distinct_count;
                diagnostics.push(Diagnostic {
                    rule_name: self.name(),
                    severity: Severity::Suggestion,
                    location: Location::Column {
                        column: col_idx,
                        path: path.clone(),
                    },
                    message: if is_uuid {
                        format!(
                            "UUID column missing bloom filters in {missing_bloom_groups}/{non_empty_groups} row groups; \
                             bloom filters enable fast point lookups"
                        )
                    } else {
                        format!(
                            "high-cardinality byte array column missing bloom filters in \
                             {missing_bloom_groups}/{non_empty_groups} row groups \
                             (~{ndv} estimated distinct values)"
                        )
                    },
                    fixes: vec![
                        FixAction::SetColumnBloomFilterEnabled(path.clone(), true),
                        FixAction::SetColumnBloomFilterNdv(path, ndv),
                    ],
                });
            }
        }
        diagnostics
    }
}
