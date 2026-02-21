use std::sync::Arc;

use bytes::{Buf, Bytes};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};

use crate::cardinality::ColumnCardinality;
use crate::diagnostic::Diagnostic;

pub struct RuleContext {
    pub metadata: Arc<ParquetMetaData>,
    pub cardinalities: Vec<ColumnCardinality>,
    pub reader: ParquetObjectReader,
}

#[async_trait::async_trait]
pub trait Rule: Send + Sync {
    fn name(&self) -> &'static str;
    async fn check(&self, ctx: &RuleContext) -> Vec<Diagnostic>;
}

/// A byte slice from a column chunk, implementing `ChunkReader` so that
/// `SerializedPageReader` can iterate pages without loading the entire file.
pub struct ColumnChunk {
    data: Bytes,
    start: u64,
}

impl ColumnChunk {
    pub fn new(data: Bytes, start: u64) -> Self {
        Self { data, start }
    }
}

impl Length for ColumnChunk {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for ColumnChunk {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let local = (start - self.start) as usize;
        Ok(self.data.slice(local..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let local = (start - self.start) as usize;
        Ok(self.data.slice(local..local + length))
    }
}

/// Fetch a column chunk's bytes and create a page reader.
pub async fn column_page_reader(
    reader: &ParquetObjectReader,
    metadata: &ParquetMetaData,
    rg_idx: usize,
    col_idx: usize,
) -> anyhow::Result<SerializedPageReader<ColumnChunk>> {
    use parquet::arrow::async_reader::AsyncFileReader;

    let rg = metadata.row_group(rg_idx);
    let col = rg.column(col_idx);
    let (offset, length) = col.byte_range();
    let bytes = reader
        .clone()
        .get_bytes(offset..(offset + length))
        .await?;
    let chunk = ColumnChunk::new(bytes, offset);
    Ok(SerializedPageReader::new(
        Arc::new(chunk),
        col,
        rg.num_rows() as usize,
        None,
    )?)
}
