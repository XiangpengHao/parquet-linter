pub mod cardinality;
pub mod diagnostic;
pub mod fix;
pub mod rule;
pub mod rules;

use diagnostic::{Diagnostic, Severity};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use rule::RuleContext;
use std::path::Path;

pub fn lint(path: &Path, rule_names: Option<&[String]>) -> anyhow::Result<Vec<Diagnostic>> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata().clone();
    let cardinalities = cardinality::estimate(path, &reader, &metadata)?;
    let ctx = RuleContext {
        metadata: &metadata,
        cardinalities: &cardinalities,
        reader: &reader,
    };
    let rules = rules::get_rules(rule_names);
    let mut diagnostics: Vec<Diagnostic> = rules.iter().flat_map(|r| r.check(&ctx)).collect();
    diagnostics.sort_by_key(|d| d.severity);
    Ok(diagnostics)
}

pub fn has_warnings_or_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|d| matches!(d.severity, Severity::Warning | Severity::Error))
}
