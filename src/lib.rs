pub mod cardinality;
pub mod diagnostic;
pub mod fix;
pub mod loader;
pub mod prescription;
pub mod rule;
pub mod rules;

use std::sync::Arc;

use diagnostic::{Diagnostic, Severity};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use rule::RuleContext;

pub async fn lint(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    rule_names: Option<&[String]>,
) -> anyhow::Result<Vec<Diagnostic>> {
    let reader = ParquetObjectReader::new(store, path);
    lint_reader(reader, rule_names).await
}

async fn lint_reader(
    reader: ParquetObjectReader,
    rule_names: Option<&[String]>,
) -> anyhow::Result<Vec<Diagnostic>> {
    use parquet::arrow::async_reader::AsyncFileReader;
    let metadata = reader.clone().get_metadata(None).await?;
    let cardinalities = cardinality::estimate(&reader, &metadata).await?;
    let ctx = RuleContext {
        metadata,
        cardinalities,
        reader,
    };
    let rules = rules::get_rules(rule_names);
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    for r in &rules {
        diagnostics.extend(r.check(&ctx).await);
    }
    diagnostics.sort_by_key(|d| d.severity);
    Ok(diagnostics)
}

pub fn has_warnings_or_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|d| matches!(d.severity, Severity::Warning | Severity::Error))
}
