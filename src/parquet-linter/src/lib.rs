pub mod cardinality;
pub mod column_context;
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

#[derive(Debug, Clone, Copy, Default)]
pub struct LintOptions {
    pub gpu: bool,
}

pub async fn lint(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    rule_names: Option<&[String]>,
) -> anyhow::Result<Vec<Diagnostic>> {
    lint_with_options(store, path, rule_names, LintOptions::default()).await
}

pub async fn lint_with_options(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    rule_names: Option<&[String]>,
    options: LintOptions,
) -> anyhow::Result<Vec<Diagnostic>> {
    let reader = ParquetObjectReader::new(store, path);
    lint_reader(reader, rule_names, options).await
}

async fn lint_reader(
    reader: ParquetObjectReader,
    rule_names: Option<&[String]>,
    options: LintOptions,
) -> anyhow::Result<Vec<Diagnostic>> {
    use parquet::arrow::async_reader::AsyncFileReader;
    let metadata = if options.gpu {
        let arrow_options = parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true);
        reader.clone().get_metadata(Some(&arrow_options)).await?
    } else {
        reader.clone().get_metadata(None).await?
    };
    let columns = column_context::build(&reader, &metadata).await?;
    let ctx = RuleContext {
        metadata,
        columns,
        reader,
        gpu: options.gpu,
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
