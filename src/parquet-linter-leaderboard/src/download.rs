use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use futures::TryStreamExt;

pub async fn download_if_missing(index: usize, url: &str, destination: &Path) -> Result<()> {
    if destination.exists() {
        println!("Skipping #{index}: {} (exists)", destination.display());
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    println!("Downloading #{index}: {url}");
    let (store, path) = parquet_linter::loader::parse(url)
        .with_context(|| format!("failed to parse URL for file #{index}: {url}"))?;
    let stream = store
        .get(&path)
        .await
        .with_context(|| format!("failed to GET file #{index}: {url}"))?
        .into_stream();

    let mut file = File::create(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;

    futures::pin_mut!(stream);
    while let Some(chunk) = stream.try_next().await? {
        file.write_all(&chunk)?;
    }
    file.flush()?;

    println!("Downloaded #{index} -> {}", destination.display());
    Ok(())
}
