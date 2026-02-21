pub mod diagnostic;
pub mod fix;
pub mod metadata;
pub mod rule;
pub mod rules;

use diagnostic::{Diagnostic, Severity};
use rule::RuleContext;
use std::path::Path;

pub fn lint(path: &Path, rule_names: Option<&[String]>) -> anyhow::Result<Vec<Diagnostic>> {
    let metadata = metadata::read_metadata(path)?;
    let ctx = RuleContext { metadata: &metadata };
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
