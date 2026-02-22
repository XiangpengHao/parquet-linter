use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use std::fs;
use std::path::PathBuf;
use std::process;

use parquet_linter::diagnostic::Severity;
use parquet_linter::prescription::Prescription;

#[derive(Parser)]
#[command(
    name = "parquet-linter",
    about = "Lint and rewrite parquet files",
    args_conflicts_with_subcommands = true,
    arg_required_else_help = true
)]
struct Cli {
    /// File path or URL (local, s3://, https://)
    #[arg(value_name = "FILE")]
    file: Option<String>,
    /// Only run specific rules (comma-separated)
    #[arg(long, value_delimiter = ',')]
    rules: Option<Vec<String>>,
    /// Minimum severity to display
    #[arg(long)]
    severity: Option<Severity>,
    /// Write merged prescription DSL from lint results to a text file
    #[arg(long, value_name = "FILE")]
    export_prescription: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Rewrite a parquet file using lint results or a prescription
    Rewrite {
        /// File path or URL (local, s3://, https://)
        file: Option<String>,
        /// Output file path
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Only apply fixes from specific rules (comma-separated)
        #[arg(long, value_delimiter = ',')]
        rules: Option<Vec<String>>,
        /// Apply a prescription DSL file directly (without running lint)
        #[arg(long, value_name = "FILE")]
        from_prescription: Option<PathBuf>,
        /// Show what would be fixed without writing
        #[arg(long)]
        dry_run: bool,
        /// Write merged prescription DSL to a text file
        #[arg(long, value_name = "FILE")]
        export_prescription: Option<PathBuf>,
    },
}

fn write_prescription(path: &PathBuf, prescription: &Prescription) -> Result<()> {
    let mut text = prescription.to_string();
    if !text.ends_with('\n') {
        text.push('\n');
    }
    fs::write(path, text)?;
    let msg = format!("Wrote prescription to {}", path.display());
    println!("{}", msg.cyan().bold());
    Ok(())
}

fn read_prescription(path: &PathBuf) -> Result<Prescription> {
    let text = fs::read_to_string(path)?;
    Prescription::parse(&text).map_err(Into::into)
}

fn warn_if_conflicting_for_apply(prescription: &Prescription) {
    if let Err(conflict) = prescription.validate() {
        let msg = format!(
            "Conflicting directives detected; continuing with last directive wins: {conflict}"
        );
        println!("{}", msg.yellow().bold());
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        None => {
            let file = cli
                .file
                .ok_or_else(|| anyhow::anyhow!("missing FILE argument for check mode"))?;
            let severity = cli.severity.unwrap_or(Severity::Suggestion);
            let rules = cli.rules;
            let export_prescription = cli.export_prescription;

            let (store, path) = parquet_linter::loader::parse(&file)?;
            let diagnostics = parquet_linter::lint(store, path, rules.as_deref()).await?;
            let filtered: Vec<_> = diagnostics
                .iter()
                .filter(|d| d.severity >= severity)
                .collect();

            if export_prescription.is_some() {
                let mut prescription = Prescription::new();
                for diagnostic in &filtered {
                    prescription.extend(diagnostic.prescription.clone());
                }
                if let Err(conflict) = prescription.validate() {
                    let msg = format!(
                        "Prescription contains conflicting directives (exporting for review anyway): {conflict}"
                    );
                    println!("{}", msg.yellow().bold());
                }

                if let Some(path) = &export_prescription {
                    write_prescription(path, &prescription)?;
                }
            }

            if filtered.is_empty() {
                println!("{}", "No issues found. ✓".green().bold());
            } else {
                for d in &filtered {
                    d.print_colored();
                    println!();
                }
                let summary = format!("{} issue(s) found.", filtered.len());
                println!("{}", summary.yellow().bold());
            }

            if parquet_linter::has_warnings_or_errors(&diagnostics) {
                process::exit(1);
            }
        }
        Some(Command::Rewrite {
            file,
            output,
            rules,
            from_prescription,
            dry_run,
            export_prescription,
        }) => {
            let file =
                file.ok_or_else(|| anyhow::anyhow!("missing FILE argument for rewrite mode"))?;
            let output =
                output.ok_or_else(|| anyhow::anyhow!("missing --output for rewrite mode"))?;

            if let Some(prescription_path) = from_prescription {
                if rules.is_some() {
                    return Err(anyhow::anyhow!(
                        "--rules cannot be used with --from-prescription"
                    ));
                }

                let prescription = read_prescription(&prescription_path)?;
                if prescription.is_empty() {
                    println!("{}", "No directives to apply. ✓".green().bold());
                    return Ok(());
                }
                warn_if_conflicting_for_apply(&prescription);

                if let Some(path) = &export_prescription {
                    write_prescription(path, &prescription)?;
                }

                if dry_run {
                    let msg = format!(
                        "Dry run: {} directive(s) loaded from {}:",
                        prescription.directives().len(),
                        prescription_path.display()
                    );
                    println!("{}", msg.cyan().bold());
                    println!("{prescription}");
                } else {
                    let (store, path) = parquet_linter::loader::parse(&file)?;
                    parquet_linter::fix::rewrite(store, path, &output, &prescription).await?;
                    let msg = format!(
                        "Applied {} directive(s) from {}, wrote {}",
                        prescription.directives().len(),
                        prescription_path.display(),
                        output.display()
                    );
                    println!("{}", msg.green().bold());
                }
            } else {
                let (store, path) = parquet_linter::loader::parse(&file)?;
                let diagnostics =
                    parquet_linter::lint(store.clone(), path.clone(), rules.as_deref()).await?;
                let mut prescription = Prescription::new();
                for diagnostic in &diagnostics {
                    prescription.extend(diagnostic.prescription.clone());
                }

                if prescription.is_empty() {
                    println!("{}", "No fixes to apply. ✓".green().bold());
                    return Ok(());
                }

                warn_if_conflicting_for_apply(&prescription);

                for diagnostic in &diagnostics {
                    if diagnostic.prescription.is_empty() {
                        continue;
                    }
                    diagnostic.print_colored();
                    println!();
                }

                if let Some(path) = &export_prescription {
                    write_prescription(path, &prescription)?;
                }

                if dry_run {
                    let msg = format!(
                        "Dry run: {} directive(s) would be applied:",
                        prescription.directives().len()
                    );
                    println!("{}", msg.cyan().bold());
                    println!("{prescription}");
                } else {
                    parquet_linter::fix::rewrite(store, path, &output, &prescription).await?;
                    let msg = format!(
                        "Applied {} directive(s), wrote {}",
                        prescription.directives().len(),
                        output.display()
                    );
                    println!("{}", msg.green().bold());
                }
            }
        }
    }
    Ok(())
}
