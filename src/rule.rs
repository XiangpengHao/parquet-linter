use crate::cardinality::ColumnCardinality;
use crate::diagnostic::Diagnostic;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;

pub struct RuleContext<'a> {
    pub metadata: &'a ParquetMetaData,
    pub cardinalities: &'a [ColumnCardinality],
    pub reader: &'a SerializedFileReader<File>,
}

pub trait Rule: Send + Sync {
    fn name(&self) -> &'static str;
    fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic>;
}
