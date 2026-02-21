use crate::cardinality::ColumnCardinality;
use crate::diagnostic::Diagnostic;
use parquet::file::metadata::ParquetMetaData;

pub struct RuleContext<'a> {
    pub metadata: &'a ParquetMetaData,
    pub cardinalities: &'a [ColumnCardinality],
}

pub trait Rule: Send + Sync {
    fn name(&self) -> &'static str;
    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic>;
}
