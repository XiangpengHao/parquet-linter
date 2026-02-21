use std::collections::HashMap;
use std::fmt;

use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

/// Compression codec - excludes deprecated LZ4 and unsupported LZO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Uncompressed,
    Snappy,
    Gzip(u8),
    Brotli(u8),
    Zstd(i32),
    Lz4Raw,
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Codec::Uncompressed => write!(f, "uncompressed"),
            Codec::Snappy => write!(f, "snappy"),
            Codec::Gzip(level) => write!(f, "gzip({level})"),
            Codec::Brotli(level) => write!(f, "brotli({level})"),
            Codec::Zstd(level) => write!(f, "zstd({level})"),
            Codec::Lz4Raw => write!(f, "lz4_raw"),
        }
    }
}

impl From<Codec> for Compression {
    fn from(value: Codec) -> Self {
        match value {
            Codec::Uncompressed => Compression::UNCOMPRESSED,
            Codec::Snappy => Compression::SNAPPY,
            Codec::Gzip(level) => Compression::GZIP(
                GzipLevel::try_new(level.into()).expect("Codec::Gzip level must be in 0..=9"),
            ),
            Codec::Brotli(level) => Compression::BROTLI(
                BrotliLevel::try_new(level.into())
                    .expect("Codec::Brotli level must be in 0..=11"),
            ),
            Codec::Zstd(level) => Compression::ZSTD(
                ZstdLevel::try_new(level).expect("Codec::Zstd level must be in 1..=22"),
            ),
            Codec::Lz4Raw => Compression::LZ4_RAW,
        }
    }
}

/// Data encoding - excludes dictionary/level encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataEncoding {
    Plain,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
}

impl fmt::Display for DataEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataEncoding::Plain => write!(f, "plain"),
            DataEncoding::DeltaBinaryPacked => write!(f, "delta_binary_packed"),
            DataEncoding::DeltaLengthByteArray => write!(f, "delta_length_byte_array"),
            DataEncoding::DeltaByteArray => write!(f, "delta_byte_array"),
            DataEncoding::ByteStreamSplit => write!(f, "byte_stream_split"),
        }
    }
}

impl From<DataEncoding> for Encoding {
    fn from(value: DataEncoding) -> Self {
        match value {
            DataEncoding::Plain => Encoding::PLAIN,
            DataEncoding::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
            DataEncoding::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            DataEncoding::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
            DataEncoding::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
        }
    }
}

/// Statistics level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsConfig {
    None,
    Chunk,
    Page,
}

impl fmt::Display for StatisticsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatisticsConfig::None => write!(f, "none"),
            StatisticsConfig::Chunk => write!(f, "chunk"),
            StatisticsConfig::Page => write!(f, "page"),
        }
    }
}

impl From<StatisticsConfig> for EnabledStatistics {
    fn from(value: StatisticsConfig) -> Self {
        match value {
            StatisticsConfig::None => EnabledStatistics::None,
            StatisticsConfig::Chunk => EnabledStatistics::Chunk,
            StatisticsConfig::Page => EnabledStatistics::Page,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Directive {
    // File-scope
    SetFileCompression(Codec),
    SetFileMaxRowGroupSize(usize),
    SetFileDataPageSizeLimit(usize),
    SetFileStatisticsTruncateLength(Option<usize>),

    // Column-scope
    SetColumnCompression(ColumnPath, Codec),
    SetColumnEncoding(ColumnPath, DataEncoding),
    SetColumnDictionary(ColumnPath, bool),
    SetColumnDictionaryPageSizeLimit(ColumnPath, usize),
    SetColumnStatistics(ColumnPath, StatisticsConfig),
    SetColumnBloomFilter(ColumnPath, bool),
    SetColumnBloomFilterNdv(ColumnPath, u64),
    SetColumnBloomFilterFpp(ColumnPath, f64),
}

impl Directive {
    fn column_text(column: &ColumnPath) -> String {
        column.string()
    }

    fn conflict_key(&self) -> String {
        match self {
            Directive::SetFileCompression(_) => "file compression".to_string(),
            Directive::SetFileMaxRowGroupSize(_) => "file max_row_group_size".to_string(),
            Directive::SetFileDataPageSizeLimit(_) => "file data_page_size_limit".to_string(),
            Directive::SetFileStatisticsTruncateLength(_) => {
                "file statistics_truncate_length".to_string()
            }
            Directive::SetColumnCompression(col, _) => {
                format!("column {} compression", Self::column_text(col))
            }
            Directive::SetColumnEncoding(col, _) => {
                format!("column {} encoding", Self::column_text(col))
            }
            Directive::SetColumnDictionary(col, _) => {
                format!("column {} dictionary", Self::column_text(col))
            }
            Directive::SetColumnDictionaryPageSizeLimit(col, _) => {
                format!("column {} dictionary_page_size_limit", Self::column_text(col))
            }
            Directive::SetColumnStatistics(col, _) => {
                format!("column {} statistics", Self::column_text(col))
            }
            Directive::SetColumnBloomFilter(col, _) => {
                format!("column {} bloom_filter", Self::column_text(col))
            }
            Directive::SetColumnBloomFilterNdv(col, _) => {
                format!("column {} bloom_filter_ndv", Self::column_text(col))
            }
            Directive::SetColumnBloomFilterFpp(col, _) => {
                format!("column {} bloom_filter_fpp", Self::column_text(col))
            }
        }
    }

    fn conflict_value(&self) -> String {
        match self {
            Directive::SetFileCompression(v) => v.to_string(),
            Directive::SetFileMaxRowGroupSize(v) => v.to_string(),
            Directive::SetFileDataPageSizeLimit(v) => v.to_string(),
            Directive::SetFileStatisticsTruncateLength(v) => match v {
                Some(v) => v.to_string(),
                None => "none".to_string(),
            },
            Directive::SetColumnCompression(_, v) => v.to_string(),
            Directive::SetColumnEncoding(_, v) => v.to_string(),
            Directive::SetColumnDictionary(_, v) => v.to_string(),
            Directive::SetColumnDictionaryPageSizeLimit(_, v) => v.to_string(),
            Directive::SetColumnStatistics(_, v) => v.to_string(),
            Directive::SetColumnBloomFilter(_, v) => v.to_string(),
            Directive::SetColumnBloomFilterNdv(_, v) => v.to_string(),
            Directive::SetColumnBloomFilterFpp(_, v) => v.to_string(),
        }
    }
}

impl fmt::Display for Directive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Directive::SetFileCompression(c) => write!(f, "set file compression {c}"),
            Directive::SetFileMaxRowGroupSize(n) => write!(f, "set file max_row_group_size {n}"),
            Directive::SetFileDataPageSizeLimit(n) => {
                write!(f, "set file data_page_size_limit {n}")
            }
            Directive::SetFileStatisticsTruncateLength(Some(n)) => {
                write!(f, "set file statistics_truncate_length {n}")
            }
            Directive::SetFileStatisticsTruncateLength(None) => {
                write!(f, "set file statistics_truncate_length none")
            }
            Directive::SetColumnCompression(col, c) => {
                write!(f, "set column {} compression {c}", Self::column_text(col))
            }
            Directive::SetColumnEncoding(col, e) => {
                write!(f, "set column {} encoding {e}", Self::column_text(col))
            }
            Directive::SetColumnDictionary(col, v) => {
                write!(f, "set column {} dictionary {v}", Self::column_text(col))
            }
            Directive::SetColumnDictionaryPageSizeLimit(col, n) => {
                write!(
                    f,
                    "set column {} dictionary_page_size_limit {n}",
                    Self::column_text(col)
                )
            }
            Directive::SetColumnStatistics(col, stats) => {
                write!(f, "set column {} statistics {stats}", Self::column_text(col))
            }
            Directive::SetColumnBloomFilter(col, enabled) => {
                write!(
                    f,
                    "set column {} bloom_filter {enabled}",
                    Self::column_text(col)
                )
            }
            Directive::SetColumnBloomFilterNdv(col, ndv) => {
                write!(
                    f,
                    "set column {} bloom_filter_ndv {ndv}",
                    Self::column_text(col)
                )
            }
            Directive::SetColumnBloomFilterFpp(col, fpp) => {
                write!(
                    f,
                    "set column {} bloom_filter_fpp {fpp}",
                    Self::column_text(col)
                )
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Prescription(Vec<Directive>);

impl Prescription {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, directive: Directive) {
        self.0.push(directive);
    }

    pub fn directives(&self) -> &[Directive] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn extend(&mut self, other: Prescription) {
        self.0.extend(other.0);
    }

    pub fn validate(&self) -> Result<(), ConflictError> {
        let mut seen: HashMap<String, (String, String)> = HashMap::new();

        for directive in &self.0 {
            let key = directive.conflict_key();
            let value = directive.conflict_value();
            let text = directive.to_string();

            if let Some((first_value, first_text)) = seen.get(&key) {
                if first_value != &value {
                    return Err(ConflictError {
                        key,
                        first: first_text.clone(),
                        second: text,
                    });
                }
            } else {
                seen.insert(key, (value, text));
            }
        }

        Ok(())
    }

    pub fn apply(&self, mut builder: WriterPropertiesBuilder) -> WriterPropertiesBuilder {
        for directive in &self.0 {
            builder = match directive {
                Directive::SetFileCompression(codec) => builder.set_compression((*codec).into()),
                Directive::SetFileMaxRowGroupSize(rows) => builder.set_max_row_group_size(*rows),
                Directive::SetFileDataPageSizeLimit(bytes) => {
                    builder.set_data_page_size_limit(*bytes)
                }
                Directive::SetFileStatisticsTruncateLength(length) => {
                    builder.set_statistics_truncate_length(*length)
                }
                Directive::SetColumnCompression(col, codec) => {
                    builder.set_column_compression(col.clone(), (*codec).into())
                }
                Directive::SetColumnEncoding(col, encoding) => {
                    builder.set_column_encoding(col.clone(), (*encoding).into())
                }
                Directive::SetColumnDictionary(col, enabled) => {
                    builder.set_column_dictionary_enabled(col.clone(), *enabled)
                }
                Directive::SetColumnDictionaryPageSizeLimit(col, size_limit) => {
                    builder.set_column_dictionary_page_size_limit(col.clone(), *size_limit)
                }
                Directive::SetColumnStatistics(col, stats) => {
                    builder.set_column_statistics_enabled(col.clone(), (*stats).into())
                }
                Directive::SetColumnBloomFilter(col, enabled) => {
                    builder.set_column_bloom_filter_enabled(col.clone(), *enabled)
                }
                Directive::SetColumnBloomFilterNdv(col, ndv) => {
                    builder.set_column_bloom_filter_ndv(col.clone(), *ndv)
                }
                Directive::SetColumnBloomFilterFpp(col, fpp) => {
                    builder.set_column_bloom_filter_fpp(col.clone(), *fpp)
                }
            }
        }
        builder
    }
}

impl fmt::Display for Prescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, directive) in self.0.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }
            write!(f, "{directive}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConflictError {
    pub key: String,
    pub first: String,
    pub second: String,
}

impl fmt::Display for ConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "conflicting directives for {}: '{}' conflicts with '{}'",
            self.key, self.second, self.first
        )
    }
}

impl std::error::Error for ConflictError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directive_display_covers_all_variants() {
        let column = ColumnPath::from("user_id");
        let cases = vec![
            (
                Directive::SetFileCompression(Codec::Zstd(3)),
                "set file compression zstd(3)",
            ),
            (
                Directive::SetFileMaxRowGroupSize(65_536),
                "set file max_row_group_size 65536",
            ),
            (
                Directive::SetFileDataPageSizeLimit(1_048_576),
                "set file data_page_size_limit 1048576",
            ),
            (
                Directive::SetFileStatisticsTruncateLength(Some(64)),
                "set file statistics_truncate_length 64",
            ),
            (
                Directive::SetFileStatisticsTruncateLength(None),
                "set file statistics_truncate_length none",
            ),
            (
                Directive::SetColumnCompression(column.clone(), Codec::Snappy),
                "set column user_id compression snappy",
            ),
            (
                Directive::SetColumnEncoding(column.clone(), DataEncoding::ByteStreamSplit),
                "set column user_id encoding byte_stream_split",
            ),
            (
                Directive::SetColumnDictionary(column.clone(), false),
                "set column user_id dictionary false",
            ),
            (
                Directive::SetColumnDictionaryPageSizeLimit(column.clone(), 2_097_152),
                "set column user_id dictionary_page_size_limit 2097152",
            ),
            (
                Directive::SetColumnStatistics(column.clone(), StatisticsConfig::Page),
                "set column user_id statistics page",
            ),
            (
                Directive::SetColumnBloomFilter(column.clone(), true),
                "set column user_id bloom_filter true",
            ),
            (
                Directive::SetColumnBloomFilterNdv(column.clone(), 50_000),
                "set column user_id bloom_filter_ndv 50000",
            ),
            (
                Directive::SetColumnBloomFilterFpp(column.clone(), 0.01),
                "set column user_id bloom_filter_fpp 0.01",
            ),
        ];

        for (directive, expected) in cases {
            assert_eq!(directive.to_string(), expected);
        }
    }

    #[test]
    fn validate_detects_conflict_for_same_key_different_values() {
        let mut prescription = Prescription::new();
        prescription.push(Directive::SetColumnCompression(
            ColumnPath::from("user_id"),
            Codec::Zstd(3),
        ));
        prescription.push(Directive::SetColumnCompression(
            ColumnPath::from("user_id"),
            Codec::Snappy,
        ));

        let error = prescription.validate().expect_err("should conflict");
        assert_eq!(error.key, "column user_id compression");
        assert_eq!(error.first, "set column user_id compression zstd(3)");
        assert_eq!(error.second, "set column user_id compression snappy");
    }

    #[test]
    fn validate_allows_duplicate_identical_directives() {
        let mut prescription = Prescription::new();
        prescription.push(Directive::SetFileDataPageSizeLimit(8_192));
        prescription.push(Directive::SetFileDataPageSizeLimit(8_192));

        assert!(prescription.validate().is_ok());
    }

    #[test]
    fn validate_allows_non_conflicting_directives_on_different_columns() {
        let mut prescription = Prescription::new();
        prescription.push(Directive::SetColumnCompression(
            ColumnPath::from("col_a"),
            Codec::Zstd(3),
        ));
        prescription.push(Directive::SetColumnCompression(
            ColumnPath::from("col_b"),
            Codec::Snappy,
        ));

        assert!(prescription.validate().is_ok());
    }

    #[test]
    fn apply_builds_writer_properties() {
        let mut prescription = Prescription::new();
        prescription.push(Directive::SetFileCompression(Codec::Lz4Raw));
        prescription.push(Directive::SetFileMaxRowGroupSize(65_536));
        prescription.push(Directive::SetFileDataPageSizeLimit(1_048_576));
        prescription.push(Directive::SetFileStatisticsTruncateLength(None));
        prescription.push(Directive::SetColumnCompression(
            ColumnPath::from("user_id"),
            Codec::Zstd(3),
        ));
        prescription.push(Directive::SetColumnEncoding(
            ColumnPath::from("user_id"),
            DataEncoding::ByteStreamSplit,
        ));
        prescription.push(Directive::SetColumnDictionary(
            ColumnPath::from("user_id"),
            false,
        ));
        prescription.push(Directive::SetColumnDictionaryPageSizeLimit(
            ColumnPath::from("user_id"),
            2_097_152,
        ));
        prescription.push(Directive::SetColumnStatistics(
            ColumnPath::from("user_id"),
            StatisticsConfig::Page,
        ));
        prescription.push(Directive::SetColumnBloomFilter(
            ColumnPath::from("user_id"),
            true,
        ));
        prescription.push(Directive::SetColumnBloomFilterNdv(
            ColumnPath::from("user_id"),
            50_000,
        ));
        prescription.push(Directive::SetColumnBloomFilterFpp(
            ColumnPath::from("user_id"),
            0.01,
        ));

        let properties = prescription.apply(parquet::file::properties::WriterProperties::builder()).build();

        assert_eq!(properties.max_row_group_size(), 65_536);
        assert_eq!(properties.data_page_size_limit(), 1_048_576);
        assert_eq!(properties.statistics_truncate_length(), None);
        assert_eq!(
            properties.compression(&ColumnPath::from("other_column")),
            Compression::LZ4_RAW
        );
        assert_eq!(
            properties.compression(&ColumnPath::from("user_id")),
            Compression::ZSTD(ZstdLevel::try_new(3).expect("valid level"))
        );
        assert_eq!(
            properties.encoding(&ColumnPath::from("user_id")),
            Some(Encoding::BYTE_STREAM_SPLIT)
        );
        assert!(!properties.dictionary_enabled(&ColumnPath::from("user_id")));
        assert_eq!(
            properties.statistics_enabled(&ColumnPath::from("user_id")),
            EnabledStatistics::Page
        );
        let bloom_filter = properties
            .bloom_filter_properties(&ColumnPath::from("user_id"))
            .expect("bloom filter configured");
        assert_eq!(bloom_filter.ndv, 50_000);
        assert!((bloom_filter.fpp - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn from_codec_covers_all_variants() {
        assert_eq!(Compression::from(Codec::Uncompressed), Compression::UNCOMPRESSED);
        assert_eq!(Compression::from(Codec::Snappy), Compression::SNAPPY);
        assert_eq!(
            Compression::from(Codec::Gzip(6)),
            Compression::GZIP(GzipLevel::try_new(6).expect("valid gzip level"))
        );
        assert_eq!(
            Compression::from(Codec::Brotli(4)),
            Compression::BROTLI(BrotliLevel::try_new(4).expect("valid brotli level"))
        );
        assert_eq!(
            Compression::from(Codec::Zstd(3)),
            Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level"))
        );
        assert_eq!(Compression::from(Codec::Lz4Raw), Compression::LZ4_RAW);
    }

    #[test]
    fn from_data_encoding_covers_all_variants() {
        assert_eq!(Encoding::from(DataEncoding::Plain), Encoding::PLAIN);
        assert_eq!(
            Encoding::from(DataEncoding::DeltaBinaryPacked),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            Encoding::from(DataEncoding::DeltaLengthByteArray),
            Encoding::DELTA_LENGTH_BYTE_ARRAY
        );
        assert_eq!(
            Encoding::from(DataEncoding::DeltaByteArray),
            Encoding::DELTA_BYTE_ARRAY
        );
        assert_eq!(
            Encoding::from(DataEncoding::ByteStreamSplit),
            Encoding::BYTE_STREAM_SPLIT
        );
    }
}
