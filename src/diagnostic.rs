use parquet::basic::{Compression, Encoding};
use parquet::file::properties::EnabledStatistics;
use parquet::schema::types::ColumnPath;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warning,
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Info => write!(f, "info"),
            Severity::Warning => write!(f, "warning"),
            Severity::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for Severity {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "info" => Ok(Severity::Info),
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
    Column { row_group: usize, column: usize, path: ColumnPath },
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Location::File => write!(f, "file"),
            Location::RowGroup { index } => write!(f, "row_group[{index}]"),
            Location::Column { row_group, column, path } => {
                write!(f, "row_group[{row_group}].column[{column}]({path})")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum FixAction {
    SetDataPageSizeLimit(usize),
    SetMaxRowGroupSize(usize),
    SetColumnCompression(ColumnPath, Compression),
    SetColumnEncoding(ColumnPath, Encoding),
    SetColumnDictionaryEnabled(ColumnPath, bool),
    SetColumnDictionaryPageSizeLimit(ColumnPath, usize),
    SetColumnStatisticsEnabled(ColumnPath, EnabledStatistics),
    SetColumnBloomFilterEnabled(ColumnPath, bool),
    SetColumnBloomFilterNdv(ColumnPath, u64),
    SetStatisticsTruncateLength(Option<usize>),
    SetCompression(Compression),
}

impl fmt::Display for FixAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FixAction::SetDataPageSizeLimit(v) => write!(f, "set data_page_size_limit={v}"),
            FixAction::SetMaxRowGroupSize(v) => write!(f, "set max_row_group_size={v}"),
            FixAction::SetColumnCompression(col, c) => write!(f, "set {col} compression={c:?}"),
            FixAction::SetColumnEncoding(col, e) => write!(f, "set {col} encoding={e:?}"),
            FixAction::SetColumnDictionaryEnabled(col, v) => {
                write!(f, "set {col} dictionary_enabled={v}")
            }
            FixAction::SetColumnDictionaryPageSizeLimit(col, v) => {
                write!(f, "set {col} dictionary_page_size_limit={v}")
            }
            FixAction::SetColumnStatisticsEnabled(col, v) => {
                write!(f, "set {col} statistics_enabled={v:?}")
            }
            FixAction::SetColumnBloomFilterEnabled(col, v) => {
                write!(f, "set {col} bloom_filter_enabled={v}")
            }
            FixAction::SetColumnBloomFilterNdv(col, v) => {
                write!(f, "set {col} bloom_filter_ndv={v}")
            }
            FixAction::SetStatisticsTruncateLength(v) => {
                write!(f, "set statistics_truncate_length={v:?}")
            }
            FixAction::SetCompression(c) => write!(f, "set compression={c:?}"),
        }
    }
}

pub struct Diagnostic {
    pub rule_name: &'static str,
    pub severity: Severity,
    pub location: Location,
    pub message: String,
    pub fixes: Vec<FixAction>,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {} @ {}: {}", self.severity, self.rule_name, self.location, self.message)?;
        for fix in &self.fixes {
            write!(f, "\n  fix: {fix}")?;
        }
        Ok(())
    }
}
