use std::fs;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Clone, Copy)]
pub struct Measurement {
    pub loading_time_ms: f64,
    pub file_size_mb: f64,
    pub cost: f64,
}

pub fn measure(path: &Path, batch_size: usize, iterations: usize) -> Result<Measurement> {
    let iterations = iterations.max(1);
    let mut best_loading_time_ms = f64::INFINITY;

    for _ in 0..iterations {
        let input = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
        let reader = builder.with_batch_size(batch_size).build()?;

        let start = Instant::now();
        let mut total_rows = 0usize;
        for batch in reader {
            total_rows += batch?.num_rows();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let _ = total_rows;

        best_loading_time_ms = best_loading_time_ms.min(elapsed_ms);
    }

    let file_size_mb = fs::metadata(path)?.len() as f64 / (1024.0 * 1024.0);
    Ok(Measurement {
        loading_time_ms: best_loading_time_ms,
        file_size_mb,
        cost: best_loading_time_ms + file_size_mb,
    })
}
