use crate::diagnostic::Diagnostic;
use parquet::file::metadata::ParquetMetaData;

pub struct RuleContext<'a> {
    pub metadata: &'a ParquetMetaData,
}

pub trait Rule: Send + Sync {
    fn name(&self) -> &'static str;
    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic>;
}
