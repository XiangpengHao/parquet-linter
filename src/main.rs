use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process;

use parquet_linter::diagnostic::Severity;

#[derive(Parser)]
#[command(name = "parquet-lint", about = "Lint and fix parquet files")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check a parquet file for issues
    Check {
        file: PathBuf,
        /// Only run specific rules (comma-separated)
        #[arg(long, value_delimiter = ',')]
        rules: Option<Vec<String>>,
        /// Minimum severity to display
        #[arg(long, default_value = "info")]
        severity: Severity,
    },
    /// Fix issues by rewriting the parquet file
    Fix {
        file: PathBuf,
        /// Output file path
        #[arg(short, long)]
        output: PathBuf,
        /// Only apply fixes from specific rules (comma-separated)
        #[arg(long, value_delimiter = ',')]
        rules: Option<Vec<String>>,
        /// Show what would be fixed without writing
        #[arg(long)]
        dry_run: bool,
    },
    /// Print parquet file metadata
    Info { file: PathBuf },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Check {
            file,
            rules,
            severity,
        } => {
            let diagnostics =
                parquet_linter::lint(&file, rules.as_deref())?;
            let filtered: Vec<_> = diagnostics
                .iter()
                .filter(|d| d.severity >= severity)
                .collect();

            if filtered.is_empty() {
                println!("No issues found.");
            } else {
                for d in &filtered {
                    println!("{d}");
                    println!();
                }
                println!("{} issue(s) found.", filtered.len());
            }

            if parquet_linter::has_warnings_or_errors(&diagnostics) {
                process::exit(1);
            }
        }
        Command::Fix {
            file,
            output,
            rules,
            dry_run,
        } => {
            let diagnostics =
                parquet_linter::lint(&file, rules.as_deref())?;
            let all_fixes: Vec<_> = diagnostics.iter().flat_map(|d| d.fixes.clone()).collect();

            if all_fixes.is_empty() {
                println!("No fixes to apply.");
                return Ok(());
            }

            for d in &diagnostics {
                if !d.fixes.is_empty() {
                    println!("{d}");
                    println!();
                }
            }

            if dry_run {
                println!("Dry run: {} fix action(s) would be applied.", all_fixes.len());
            } else {
                parquet_linter::fix::rewrite_file(&file, &output, &all_fixes)?;
                println!(
                    "Applied {} fix action(s), wrote {}",
                    all_fixes.len(),
                    output.display()
                );
            }
        }
        Command::Info { file } => {
            parquet_linter::metadata::print_info(&file)?;
        }
    }
    Ok(())
}
