use anyhow::Result;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

use crate::diagnostic::FixAction;

pub fn build_writer_properties(actions: &[FixAction]) -> WriterProperties {
    let mut builder = WriterProperties::builder();
    for action in actions {
        builder = match action.clone() {
            FixAction::SetDataPageSizeLimit(v) => builder.set_data_page_size_limit(v),
            FixAction::SetMaxRowGroupSize(v) => builder.set_max_row_group_size(v),
            FixAction::SetColumnCompression(col, c) => builder.set_column_compression(col, c),
            FixAction::SetColumnEncoding(col, e) => builder.set_column_encoding(col, e),
            FixAction::SetColumnDictionaryEnabled(col, v) => {
                builder.set_column_dictionary_enabled(col, v)
            }
            FixAction::SetColumnDictionaryPageSizeLimit(col, v) => {
                builder.set_column_dictionary_page_size_limit(col, v)
            }
            FixAction::SetColumnStatisticsEnabled(col, v) => {
                builder.set_column_statistics_enabled(col, v)
            }
            FixAction::SetColumnBloomFilterEnabled(col, v) => {
                builder.set_column_bloom_filter_enabled(col, v)
            }
            FixAction::SetColumnBloomFilterNdv(col, v) => {
                builder.set_column_bloom_filter_ndv(col, v)
            }
            FixAction::SetStatisticsTruncateLength(v) => {
                builder.set_statistics_truncate_length(v)
            }
            FixAction::SetCompression(c) => builder.set_compression(c),
        };
    }
    builder.build()
}

pub fn rewrite_file(input: &Path, output: &Path, actions: &[FixAction]) -> Result<()> {
    let props = build_writer_properties(actions);

    let input_file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    let output_file = File::create(output)?;
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}
