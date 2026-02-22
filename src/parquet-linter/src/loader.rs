use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

/// Parse a location string into an object store and path.
///
/// Accepts local paths (`./file.parquet`, `/tmp/file.parquet`),
/// S3 URLs (`s3://bucket/key.parquet`), or HTTP URLs
/// (`https://example.com/file.parquet`).
pub fn parse(location: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let url = parse_location(location)?;
    let (store, path) = object_store::parse_url(&url)
        .with_context(|| format!("unsupported location: {location}"))?;
    Ok((Arc::from(store), path))
}

fn parse_location(location: &str) -> Result<url::Url> {
    match url::Url::parse(location) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let abs = std::path::Path::new(location)
                .canonicalize()
                .with_context(|| format!("file not found: {location}"))?;
            url::Url::from_file_path(&abs)
                .map_err(|_| anyhow::anyhow!("invalid file path: {location}"))
        }
        Err(e) => Err(e.into()),
    }
}
