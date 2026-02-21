use colored::Colorize;
use parquet::schema::types::ColumnPath;
use std::fmt;

use crate::prescription::Prescription;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Suggestion,
    Warning,
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Suggestion => write!(f, "suggestion"),
            Severity::Warning => write!(f, "warning"),
            Severity::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for Severity {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "suggestion" => Ok(Severity::Suggestion),
            "warning" => Ok(Severity::Warning),
            "error" => Ok(Severity::Error),
            _ => Err(format!("unknown severity: {s}")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Location {
    File,
    RowGroup { index: usize },
    Column { column: usize, path: ColumnPath },
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Location::File => write!(f, "file"),
            Location::RowGroup { index } => write!(f, "row_group[{index}]"),
            Location::Column { column, path } => {
                write!(f, "column[{column}]({path})")
            }
        }
    }
}

pub struct Diagnostic {
    pub rule_name: &'static str,
    pub severity: Severity,
    pub location: Location,
    pub message: String,
    pub prescription: Prescription,
}

impl Diagnostic {
    pub fn print_colored(&self) {
        let severity_str = match self.severity {
            Severity::Suggestion => "suggestion".blue().bold(),
            Severity::Warning => "warning".yellow().bold(),
            Severity::Error => "error".red().bold(),
        };
        let rule = self.rule_name.dimmed();
        let location = format!("{}", self.location).cyan();
        println!("{severity_str} {rule}");
        println!("  {} {location}", "-->".dimmed());
        println!("  {}", self.message);
        for directive in self.prescription.directives() {
            println!("  {} {directive}", "fix:".green().bold());
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} @ {}: {}",
            self.severity, self.rule_name, self.location, self.message
        )?;
        for directive in self.prescription.directives() {
            write!(f, "\n  fix: {directive}")?;
        }
        Ok(())
    }
}
