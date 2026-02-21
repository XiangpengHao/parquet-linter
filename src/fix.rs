use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::{ParquetMetaData, SortingColumn};
use parquet::file::properties::{
    EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
};

use crate::diagnostic::FixAction;

pub fn build_writer_properties(actions: &[FixAction]) -> WriterProperties {
    apply_fix_actions(WriterProperties::builder(), actions).build()
}

fn build_writer_properties_with_base(
    metadata: &ParquetMetaData,
    actions: &[FixAction],
) -> WriterProperties {
    let base = infer_writer_properties(metadata);
    apply_fix_actions(base.into_builder(), actions).build()
}

fn apply_fix_actions(
    mut builder: WriterPropertiesBuilder,
    actions: &[FixAction],
) -> WriterPropertiesBuilder {
    for action in actions {
        builder = match action {
            FixAction::SetDataPageSizeLimit(v) => builder.set_data_page_size_limit(*v),
            FixAction::SetMaxRowGroupSize(v) => builder.set_max_row_group_size(*v),
            FixAction::SetColumnCompression(col, c) => {
                builder.set_column_compression(col.clone(), *c)
            }
            FixAction::SetColumnEncoding(col, e) => builder.set_column_encoding(col.clone(), *e),
            FixAction::SetColumnDictionaryEnabled(col, v) => {
                builder.set_column_dictionary_enabled(col.clone(), *v)
            }
            FixAction::SetColumnDictionaryPageSizeLimit(col, v) => {
                builder.set_column_dictionary_page_size_limit(col.clone(), *v)
            }
            FixAction::SetColumnStatisticsEnabled(col, v) => {
                builder.set_column_statistics_enabled(col.clone(), *v)
            }
            FixAction::SetColumnBloomFilterEnabled(col, v) => {
                builder.set_column_bloom_filter_enabled(col.clone(), *v)
            }
            FixAction::SetColumnBloomFilterNdv(col, v) => {
                builder.set_column_bloom_filter_ndv(col.clone(), *v)
            }
            FixAction::SetStatisticsTruncateLength(v) => builder.set_statistics_truncate_length(*v),
            FixAction::SetCompression(c) => builder.set_compression(*c),
        };
    }
    builder
}

fn infer_writer_properties(metadata: &ParquetMetaData) -> WriterProperties {
    let mut builder = WriterProperties::builder();
    let file_meta = metadata.file_metadata();

    if let Some(version) = infer_writer_version(file_meta.version()) {
        builder = builder.set_writer_version(version);
    }
    if let Some(created_by) = file_meta.created_by() {
        builder = builder.set_created_by(created_by.to_string());
    }
    if let Some(key_value_metadata) = file_meta.key_value_metadata() {
        builder = builder.set_key_value_metadata(Some(key_value_metadata.clone()));
    }
    if let Some(sorting_columns) = infer_sorting_columns(metadata) {
        builder = builder.set_sorting_columns(Some(sorting_columns));
    }
    if let Some(max_row_group_size) = infer_max_row_group_size(metadata) {
        builder = builder.set_max_row_group_size(max_row_group_size);
    }

    let schema = file_meta.schema_descr();
    for column_idx in 0..schema.num_columns() {
        let column_path = schema.column(column_idx).path().clone();

        if let Some(compression) = infer_column_compression(metadata, column_idx) {
            builder = builder.set_column_compression(column_path.clone(), compression);
        }
        if let Some(encoding) = infer_column_encoding(metadata, column_idx) {
            builder = builder.set_column_encoding(column_path.clone(), encoding);
        }
        if let Some(dictionary_enabled) = infer_column_dictionary_enabled(metadata, column_idx) {
            builder =
                builder.set_column_dictionary_enabled(column_path.clone(), dictionary_enabled);
        }
        if let Some(statistics_enabled) = infer_column_statistics_enabled(metadata, column_idx) {
            builder =
                builder.set_column_statistics_enabled(column_path.clone(), statistics_enabled);
        }
        if let Some(bloom_filter_enabled) = infer_column_bloom_filter_enabled(metadata, column_idx)
        {
            builder = builder.set_column_bloom_filter_enabled(column_path, bloom_filter_enabled);
        }
    }

    builder.build()
}

fn infer_writer_version(version: i32) -> Option<WriterVersion> {
    match version {
        1 => Some(WriterVersion::PARQUET_1_0),
        2 => Some(WriterVersion::PARQUET_2_0),
        _ => None,
    }
}

fn infer_sorting_columns(metadata: &ParquetMetaData) -> Option<Vec<SortingColumn>> {
    let mut inferred: Option<Vec<SortingColumn>> = None;

    for row_group in metadata.row_groups() {
        match (inferred.as_ref(), row_group.sorting_columns()) {
            (_, None) => return None,
            (None, Some(columns)) => inferred = Some(columns.clone()),
            (Some(existing), Some(columns)) if existing == columns => {}
            _ => return None,
        }
    }

    inferred
}

fn infer_max_row_group_size(metadata: &ParquetMetaData) -> Option<usize> {
    metadata
        .row_groups()
        .iter()
        .map(|row_group| row_group.num_rows())
        .max()
        .and_then(|rows| usize::try_from(rows).ok())
        .filter(|rows| *rows > 0)
}

fn infer_column_compression(metadata: &ParquetMetaData, column_idx: usize) -> Option<Compression> {
    most_frequent(
        metadata
            .row_groups()
            .iter()
            .map(|rg| rg.column(column_idx).compression()),
    )
}

fn infer_column_encoding(metadata: &ParquetMetaData, column_idx: usize) -> Option<Encoding> {
    let encodings = metadata
        .row_groups()
        .iter()
        .flat_map(|rg| rg.column(column_idx).encodings())
        .filter(|encoding| !is_level_encoding(*encoding) && !is_dictionary_encoding(*encoding));
    most_frequent(encodings)
}

fn infer_column_dictionary_enabled(metadata: &ParquetMetaData, column_idx: usize) -> Option<bool> {
    let mut saw_row_group = false;
    let mut used_dictionary = false;
    for row_group in metadata.row_groups() {
        saw_row_group = true;
        let column = row_group.column(column_idx);
        if column.dictionary_page_offset().is_some()
            || column.encodings().any(is_dictionary_encoding)
        {
            used_dictionary = true;
            break;
        }
    }
    saw_row_group.then_some(used_dictionary)
}

fn infer_column_statistics_enabled(
    metadata: &ParquetMetaData,
    column_idx: usize,
) -> Option<EnabledStatistics> {
    let mut saw_row_group = false;
    let mut has_page_stats = false;
    let mut has_chunk_stats = false;
    for row_group in metadata.row_groups() {
        saw_row_group = true;
        let column = row_group.column(column_idx);
        if column.column_index_offset().is_some() && column.column_index_length().is_some() {
            has_page_stats = true;
        }
        if column.statistics().is_some() {
            has_chunk_stats = true;
        }
    }

    if !saw_row_group {
        None
    } else if has_page_stats {
        Some(EnabledStatistics::Page)
    } else if has_chunk_stats {
        Some(EnabledStatistics::Chunk)
    } else {
        Some(EnabledStatistics::None)
    }
}

fn infer_column_bloom_filter_enabled(
    metadata: &ParquetMetaData,
    column_idx: usize,
) -> Option<bool> {
    let mut saw_row_group = false;
    let mut has_bloom = false;
    for row_group in metadata.row_groups() {
        saw_row_group = true;
        if row_group.column(column_idx).bloom_filter_offset().is_some() {
            has_bloom = true;
            break;
        }
    }
    saw_row_group.then_some(has_bloom)
}

#[allow(deprecated)]
fn is_level_encoding(encoding: Encoding) -> bool {
    matches!(encoding, Encoding::RLE | Encoding::BIT_PACKED)
}

fn is_dictionary_encoding(encoding: Encoding) -> bool {
    matches!(
        encoding,
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
    )
}

fn most_frequent<T: Copy + Eq>(values: impl IntoIterator<Item = T>) -> Option<T> {
    let mut counts: Vec<(T, usize)> = Vec::new();

    for value in values {
        if let Some((_, count)) = counts.iter_mut().find(|(seen, _)| *seen == value) {
            *count += 1;
        } else {
            counts.push((value, 1));
        }
    }

    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(value, _)| value)
}

pub async fn rewrite(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    output: &Path,
    actions: &[FixAction],
) -> Result<()> {
    let reader = ParquetObjectReader::new(store, path);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let props = build_writer_properties_with_base(builder.metadata(), actions);
    let schema = builder.schema().clone();
    let mut stream = builder.build()?;

    let output_file = File::create(output)?;
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::basic::{GzipLevel, ZstdLevel};
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    fn write_two_column_file(path: &Path, props: WriterProperties) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )?;

        let output = File::create(path)?;
        let mut writer = ArrowWriter::try_new(output, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn read_column_compression(path: &Path, column_idx: usize) -> Result<Compression> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let input = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
        let metadata = builder.metadata();
        let compression = metadata.row_group(0).column(column_idx).compression();
        Ok(compression)
    }

    #[tokio::test]
    async fn rewrite_preserves_untouched_column_compression() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let input = tempdir.path().join("input.parquet");
        let output = tempdir.path().join("output.parquet");
        let input_props = WriterProperties::builder()
            .set_column_compression(
                ColumnPath::from("a"),
                Compression::ZSTD(ZstdLevel::default()),
            )
            .set_column_compression(ColumnPath::from("b"), Compression::SNAPPY)
            .build();

        write_two_column_file(&input, input_props)?;

        let fixes = vec![FixAction::SetColumnCompression(
            ColumnPath::from("a"),
            Compression::GZIP(GzipLevel::default()),
        )];
        let (store, path) = crate::loader::parse(input.to_str().unwrap())?;
        rewrite(store, path, &output, &fixes).await?;

        assert_eq!(
            read_column_compression(&output, 0)?,
            Compression::GZIP(GzipLevel::default())
        );
        assert_eq!(read_column_compression(&output, 1)?, Compression::SNAPPY);
        Ok(())
    }
}
