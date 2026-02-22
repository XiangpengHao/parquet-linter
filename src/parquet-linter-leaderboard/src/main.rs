mod benchmark;
mod download;
mod report;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_linter::prescription::Prescription;

use crate::benchmark::Measurement;

#[derive(Parser, Debug)]
#[command(name = "parquet-leaderboard", about = "Benchmark prescription rewrites on parquet files")]
struct Cli {
    #[arg(value_name = "PRESCRIPTION_DIR")]
    prescription_dir: PathBuf,

    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 3)]
    iterations: usize,

    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Download missing parquet files into --data-dir before benchmarking
    #[arg(long)]
    download: bool,
}

#[derive(Debug)]
struct LoadedPrescription {
    index: usize,
    path: PathBuf,
    prescription: Prescription,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let urls = parse_urls(include_str!("../../../doc/parquet_files.txt"));
    ensure!(!urls.is_empty(), "no parquet URLs found in doc/parquet_files.txt");
    ensure!(cli.batch_size > 0, "--batch-size must be > 0");
    ensure!(cli.iterations > 0, "--iterations must be > 0");

    fs::create_dir_all(&cli.data_dir)?;
    fs::create_dir_all(&cli.output_dir)?;

    let prescriptions = load_prescriptions(&cli.prescription_dir, urls.len())?;
    if prescriptions.is_empty() {
        bail!(
            "no prescription files found in {} (expected files like 0.prescription)",
            cli.prescription_dir.display()
        );
    }

    let missing_inputs: Vec<_> = prescriptions
        .iter()
        .filter_map(|item| {
            let path = cli.data_dir.join(format!("{}.parquet", item.index));
            (!path.exists()).then_some(item.index)
        })
        .collect();

    if !missing_inputs.is_empty() {
        if !cli.download {
            bail!(
                "missing {} input parquet file(s) in {} (e.g. {}). Re-run with --download to fetch required files.",
                missing_inputs.len(),
                cli.data_dir.display(),
                cli.data_dir.join(format!("{}.parquet", missing_inputs[0])).display()
            );
        }

        for item in &prescriptions {
            let data_path = cli.data_dir.join(format!("{}.parquet", item.index));
            if data_path.exists() {
                continue;
            }
            let url = urls.get(item.index).ok_or_else(|| {
                anyhow::anyhow!("missing URL for prescription index {}", item.index)
            })?;
            download::download_if_missing(item.index, url, &data_path).await?;
        }
    }

    let mut results = Vec::with_capacity(prescriptions.len());
    for item in prescriptions {
        let input_path = cli.data_dir.join(format!("{}.parquet", item.index));
        let output_path = cli.output_dir.join(format!("{}.parquet", item.index));

        println!(
            "Applying prescription #{}, {} -> {}",
            item.index,
            item.path.display(),
            output_path.display()
        );
        let (store, object_path) = parquet_linter::loader::parse(
            input_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non-utf8 path: {}", input_path.display()))?,
        )?;
        parquet_linter::fix::rewrite(store, object_path, &output_path, &item.prescription).await?;

        validate_schema_match(&input_path, &output_path)
            .with_context(|| format!("schema mismatch for file #{}", item.index))?;

        let original = benchmark::measure(&input_path, cli.batch_size, cli.iterations)?;
        let output = benchmark::measure(&output_path, cli.batch_size, cli.iterations)?;
        print_file_summary(item.index, original, output);
        results.push(report::FileResult {
            index: item.index,
            original,
            output,
        });
    }

    println!();
    report::print(&results);
    Ok(())
}

fn parse_urls(text: &str) -> Vec<&str> {
    text.lines()
        .filter_map(|line| {
            let line = line.split('#').next()?.trim();
            (!line.is_empty()).then_some(line)
        })
        .collect()
}

fn load_prescriptions(dir: &Path, url_count: usize) -> Result<Vec<LoadedPrescription>> {
    let mut by_index: BTreeMap<usize, PathBuf> = BTreeMap::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("prescription") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(index) = stem.parse::<usize>() else {
            continue;
        };
        ensure!(
            index < url_count,
            "prescription index {} out of range ({} URLs)",
            index,
            url_count
        );
        by_index.insert(index, path);
    }

    let mut loaded = Vec::with_capacity(by_index.len());
    for (index, path) in by_index {
        let text = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let prescription =
            Prescription::parse(&text).with_context(|| format!("failed to parse {}", path.display()))?;
        if let Err(conflict) = prescription.validate() {
            println!(
                "Warning: conflicting directives in {} (continuing with last directive wins): {}",
                path.display(),
                conflict
            );
        }
        loaded.push(LoadedPrescription {
            index,
            path,
            prescription,
        });
    }
    Ok(loaded)
}

fn validate_schema_match(original_path: &Path, rewritten_path: &Path) -> Result<()> {
    let original = read_arrow_schema(original_path)?;
    let rewritten = read_arrow_schema(rewritten_path)?;
    ensure!(
        original.as_ref() == rewritten.as_ref(),
        "Arrow schema changed after rewrite"
    );
    Ok(())
}

fn read_arrow_schema(path: &Path) -> Result<std::sync::Arc<arrow_schema::Schema>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

fn print_file_summary(index: usize, original: Measurement, output: Measurement) {
    let delta_cost_pct = if original.cost == 0.0 {
        0.0
    } else {
        (output.cost - original.cost) / original.cost * 100.0
    };
    println!(
        "File #{index}: cost {:.2} -> {:.2} ({:+.2}%), size {:.2}MB -> {:.2}MB, time {:.2}ms -> {:.2}ms",
        original.cost,
        output.cost,
        delta_cost_pct,
        original.file_size_mb,
        output.file_size_mb,
        original.loading_time_ms,
        output.loading_time_ms
    );
}
