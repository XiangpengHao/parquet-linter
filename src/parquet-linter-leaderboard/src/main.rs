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
#[command(
    name = "parquet-leaderboard",
    about = "Benchmark parquet optimizations using linter-generated or custom prescriptions"
)]
struct Cli {
    /// Generate prescriptions from parquet-linter for each file in the manifest, then benchmark
    #[arg(long, conflicts_with = "from_custom_prescription")]
    from_linter: bool,

    /// Benchmark using a directory of numbered prescription files (0.prescription, 1.prescription, ...)
    #[arg(long, value_name = "DIR", conflicts_with = "from_linter")]
    from_custom_prescription: Option<PathBuf>,

    /// Optional parquet URL manifest file (defaults to doc/parquet_files.txt)
    #[arg(long, value_name = "FILE")]
    parquet_manifest: Option<PathBuf>,

    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 3)]
    iterations: usize,

    #[arg(long, default_value_t = 8192)]
    batch_size: usize,
}

#[derive(Debug)]
struct LoadedPrescription {
    index: usize,
    path: Option<PathBuf>,
    prescription: Prescription,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    ensure!(cli.batch_size > 0, "--batch-size must be > 0");
    ensure!(cli.iterations > 0, "--iterations must be > 0");

    let manifest_text = match &cli.parquet_manifest {
        Some(path) => fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?,
        None => include_str!("../../../doc/parquet_files.txt").to_string(),
    };
    let urls = parse_urls(&manifest_text);
    ensure!(
        !urls.is_empty(),
        "no parquet URLs found in doc/parquet_files.txt"
    );

    match (cli.from_linter, cli.from_custom_prescription.as_deref()) {
        (true, None) => {
            run_from_linter(
                &urls,
                &cli.data_dir,
                &cli.output_dir,
                cli.batch_size,
                cli.iterations,
            )
            .await
        }
        (false, Some(prescription_dir)) => {
            run_from_custom_prescription(
                &urls,
                &cli.data_dir,
                &cli.output_dir,
                prescription_dir,
                cli.batch_size,
                cli.iterations,
            )
            .await
        }
        _ => bail!("must pass exactly one mode: --from-linter or --from-custom-prescription <DIR>"),
    }
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
        let prescription = Prescription::parse(&text)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        if let Err(conflict) = prescription.validate() {
            println!(
                "Warning: conflicting directives in {} (continuing with last directive wins): {}",
                path.display(),
                conflict
            );
        }
        loaded.push(LoadedPrescription {
            index,
            path: Some(path),
            prescription,
        });
    }
    Ok(loaded)
}

async fn run_from_linter(
    urls: &[&str],
    data_dir: &Path,
    output_dir: &Path,
    batch_size: usize,
    iterations: usize,
) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    fs::create_dir_all(output_dir)?;

    ensure_inputs_for_indexes(urls, data_dir, 0..urls.len()).await?;

    let mut prescriptions = Vec::with_capacity(urls.len());
    for (index, _) in urls.iter().enumerate() {
        let input_path = data_dir.join(format!("{index}.parquet"));
        let diagnostics = lint_local_file(&input_path).await?;

        let mut prescription = Prescription::new();
        for diagnostic in &diagnostics {
            prescription.extend(diagnostic.prescription.clone());
        }
        if let Err(conflict) = prescription.validate() {
            println!(
                "Warning: conflicting directives for linter-generated prescription #{} (continuing with last directive wins): {}",
                index, conflict
            );
        }
        println!(
            "Linter #{}: {} diagnostics, {} directives",
            index,
            diagnostics.len(),
            prescription.directives().len()
        );
        prescriptions.push(LoadedPrescription {
            index,
            path: None,
            prescription,
        });
    }

    run_benchmark(
        urls,
        data_dir,
        output_dir,
        prescriptions,
        batch_size,
        iterations,
    )
    .await
}

async fn run_from_custom_prescription(
    urls: &[&str],
    data_dir: &Path,
    output_dir: &Path,
    prescription_dir: &Path,
    batch_size: usize,
    iterations: usize,
) -> Result<()> {
    let prescriptions = load_prescriptions(prescription_dir, urls.len())?;
    if prescriptions.is_empty() {
        bail!(
            "no prescription files found in {} (expected files like 0.prescription)",
            prescription_dir.display()
        );
    }
    run_benchmark(
        urls,
        data_dir,
        output_dir,
        prescriptions,
        batch_size,
        iterations,
    )
    .await
}

async fn run_benchmark(
    urls: &[&str],
    data_dir: &Path,
    output_dir: &Path,
    prescriptions: Vec<LoadedPrescription>,
    batch_size: usize,
    iterations: usize,
) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    fs::create_dir_all(output_dir)?;

    let missing_inputs: Vec<_> = prescriptions
        .iter()
        .filter_map(|item| {
            let path = data_dir.join(format!("{}.parquet", item.index));
            (!path.exists()).then_some(item.index)
        })
        .collect();

    if !missing_inputs.is_empty() {
        println!(
            "Warning: missing {} input parquet file(s) in {}. Downloading required files now; existing files will be reused.",
            missing_inputs.len(),
            data_dir.display()
        );
        ensure_inputs_for_indexes(urls, data_dir, missing_inputs.iter().copied()).await?;
    }

    let mut results = Vec::with_capacity(prescriptions.len());
    for item in prescriptions {
        let input_path = data_dir.join(format!("{}.parquet", item.index));
        let output_path = output_dir.join(format!("{}.parquet", item.index));

        println!(
            "Applying prescription #{}, {} -> {}",
            item.index,
            item.path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<linter-generated>".to_string()),
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

        let original = benchmark::measure(&input_path, batch_size, iterations)?;
        let output = benchmark::measure(&output_path, batch_size, iterations)?;
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

async fn ensure_inputs_for_indexes(
    urls: &[&str],
    data_dir: &Path,
    indexes: impl IntoIterator<Item = usize>,
) -> Result<()> {
    for index in indexes {
        let data_path = data_dir.join(format!("{index}.parquet"));
        if data_path.exists() {
            continue;
        }
        let url = urls
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("missing URL for file index {}", index))?;
        download::download_if_missing(index, url, &data_path).await?;
    }
    Ok(())
}

async fn lint_local_file(path: &Path) -> Result<Vec<parquet_linter::diagnostic::Diagnostic>> {
    let (store, object_path) = parquet_linter::loader::parse(
        path.to_str()
            .ok_or_else(|| anyhow::anyhow!("non-utf8 path: {}", path.display()))?,
    )?;
    parquet_linter::lint(store, object_path, None).await
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
