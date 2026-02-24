use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow, ensure};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet_linter::column_context::{ColumnContext, TypeStats};
use parquet_linter::prescription::Prescription;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::benchmark;
use crate::download;

#[derive(Clone)]
struct AppState {
    urls: Arc<Vec<String>>,
    data_dir: Arc<PathBuf>,
    batch_size: usize,
    iterations: usize,
}

#[derive(Serialize)]
struct ListResponse {
    files: Vec<FileEntry>,
}

#[derive(Serialize)]
struct FileEntry {
    id: usize,
    url: String,
    downloaded: bool,
    local_size_bytes: Option<u64>,
}

#[derive(Serialize)]
struct InfoResponse {
    id: usize,
    url: String,
    columns: Vec<ColumnContextView>,
}

#[derive(Serialize)]
struct ColumnContextView {
    column_index: usize,
    column_path: String,
    physical_type: String,
    logical_type: Option<String>,
    arrow_type: String,
    num_values: u64,
    null_count: u64,
    distinct_count: u64,
    uncompressed_size: i64,
    compressed_size: i64,
    non_null_count: u64,
    null_ratio: f64,
    cardinality_ratio: f64,
    type_stats: String,
}

#[derive(Deserialize)]
struct EvalRequest {
    id: usize,
    prescription: String,
}

#[derive(Serialize)]
struct EvalResponse {
    id: usize,
    size_bytes: usize,
    size_mb: f64,
    min_decoding_time_ms: f64,
    cost: f64,
    directive_count: usize,
    conflict_warning: Option<String>,
}

pub async fn run(
    urls: Vec<String>,
    data_dir: PathBuf,
    batch_size: usize,
    iterations: usize,
    listen: std::net::SocketAddr,
) -> Result<()> {
    ensure!(batch_size > 0, "--batch-size must be > 0");
    ensure!(iterations > 0, "--iterations must be > 0");

    std::fs::create_dir_all(&data_dir)?;

    let state = AppState {
        urls: Arc::new(urls),
        data_dir: Arc::new(data_dir),
        batch_size,
        iterations,
    };

    let app = Router::new()
        .route("/list", get(list))
        .route("/info/:id", get(info))
        .route("/eval", post(eval))
        .with_state(state);

    let listener = TcpListener::bind(listen).await?;
    println!("Leaderboard benchmark server listening on http://{listen}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn list(State(state): State<AppState>) -> Result<Json<ListResponse>, ApiError> {
    let mut files = Vec::with_capacity(state.urls.len());
    for (id, url) in state.urls.iter().enumerate() {
        let path = file_path(&state, id);
        let metadata = std::fs::metadata(&path).ok();
        files.push(FileEntry {
            id,
            url: url.clone(),
            downloaded: metadata.is_some(),
            local_size_bytes: metadata.map(|m| m.len()),
        });
    }
    Ok(Json(ListResponse { files }))
}

async fn info(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<usize>,
) -> Result<Json<InfoResponse>, ApiError> {
    let path = ensure_local_input(&state, id).await?;
    let (url, columns) = build_info(&state, id, &path)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(InfoResponse { id, url, columns }))
}

async fn eval(
    State(state): State<AppState>,
    Json(req): Json<EvalRequest>,
) -> Result<Json<EvalResponse>, ApiError> {
    let path = ensure_local_input(&state, req.id).await?;

    let prescription = Prescription::parse(&req.prescription)
        .map_err(|e| ApiError::bad_request(format!("invalid prescription: {e}")))?;
    let conflict_warning = prescription.validate().err().map(|e| e.to_string());

    let (store, object_path) = parquet_linter::loader::parse(
        path.to_str()
            .ok_or_else(|| anyhow!("non-utf8 path: {}", path.display()))
            .map_err(ApiError::internal)?,
    )
    .map_err(ApiError::internal)?;
    let rewritten = parquet_linter::fix::rewrite_to_bytes(store, object_path, &prescription)
        .await
        .map_err(ApiError::internal)?;

    validate_schema_match_bytes(&path, &rewritten).map_err(ApiError::internal)?;

    let measurement = benchmark::measure_bytes(&rewritten, state.batch_size, state.iterations)
        .map_err(ApiError::internal)?;

    Ok(Json(EvalResponse {
        id: req.id,
        size_bytes: rewritten.len(),
        size_mb: measurement.file_size_mb,
        min_decoding_time_ms: measurement.loading_time_ms,
        cost: measurement.cost,
        directive_count: prescription.directives().len(),
        conflict_warning,
    }))
}

async fn build_info(
    state: &AppState,
    id: usize,
    path: &Path,
) -> Result<(String, Vec<ColumnContextView>)> {
    let (store, object_path) = parquet_linter::loader::parse(
        path.to_str()
            .ok_or_else(|| anyhow!("non-utf8 path: {}", path.display()))?,
    )?;
    let reader = ParquetObjectReader::new(store, object_path);
    let metadata = reader.clone().get_metadata(None).await?;
    let contexts = parquet_linter::column_context::build(&reader, &metadata).await?;
    let schema = metadata.file_metadata().schema_descr();

    let columns = contexts
        .into_iter()
        .enumerate()
        .map(|(column_index, ctx)| {
            column_context_view(
                column_index,
                schema.column(column_index).path().string(),
                ctx,
            )
        })
        .collect();

    Ok((state.urls[id].clone(), columns))
}

fn column_context_view(
    column_index: usize,
    column_path: String,
    ctx: ColumnContext,
) -> ColumnContextView {
    ColumnContextView {
        column_index,
        column_path,
        physical_type: format!("{:?}", ctx.physical_type),
        logical_type: ctx.logical_type.as_ref().map(|t| format!("{:?}", t)),
        arrow_type: format!("{:?}", ctx.arrow_type),
        num_values: ctx.num_values,
        null_count: ctx.null_count,
        distinct_count: ctx.distinct_count,
        uncompressed_size: ctx.uncompressed_size,
        compressed_size: ctx.compressed_size,
        non_null_count: ctx.non_null_count(),
        null_ratio: ctx.null_ratio(),
        cardinality_ratio: ctx.cardinality_ratio(),
        type_stats: type_stats_summary(&ctx.type_stats),
    }
}

fn type_stats_summary(stats: &TypeStats) -> String {
    match stats {
        TypeStats::Boolean(s) => format!("boolean(min={:?}, max={:?})", s.min, s.max),
        TypeStats::Int(s) => format!(
            "int(bit_width={}, signed={}, min={:?}, max={:?})",
            s.bit_width, s.is_signed, s.min, s.max
        ),
        TypeStats::Float(s) => format!(
            "float(bit_width={}, min={:?}, max={:?})",
            s.bit_width, s.min, s.max
        ),
        TypeStats::String(s) => format!(
            "string(min={:?}, max={:?}, lengths={})",
            s.min_value,
            s.max_value,
            match &s.lengths {
                Some(l) => format!("min={}, max={}, avg={:.2}", l.min, l.max, l.avg),
                None => "none".to_string(),
            }
        ),
        TypeStats::Binary(s) => format!(
            "binary(min_len={:?}, max_len={:?}, lengths={})",
            s.min_value.as_ref().map(|v| v.len()),
            s.max_value.as_ref().map(|v| v.len()),
            match &s.lengths {
                Some(l) => format!("min={}, max={}, avg={:.2}", l.min, l.max, l.avg),
                None => "none".to_string(),
            }
        ),
        TypeStats::FixedLenBinary(s) => format!("fixed_len_binary(type_length={})", s.type_length),
        TypeStats::Unknown => "unknown".to_string(),
    }
}

async fn ensure_local_input(state: &AppState, id: usize) -> Result<PathBuf, ApiError> {
    let url = state
        .urls
        .get(id)
        .ok_or_else(|| ApiError::not_found(format!("unknown parquet file id: {id}")))?;
    let path = file_path(state, id);
    if !path.exists() {
        download::download_if_missing(id, url, &path)
            .await
            .map_err(ApiError::internal)?;
    }
    Ok(path)
}

fn file_path(state: &AppState, id: usize) -> PathBuf {
    state.data_dir.join(format!("{id}.parquet"))
}

fn validate_schema_match_bytes(original_path: &Path, rewritten_bytes: &[u8]) -> Result<()> {
    let original = read_arrow_schema_from_file(original_path)?;
    let rewritten = read_arrow_schema_from_bytes(rewritten_bytes)?;
    ensure!(
        original.as_ref() == rewritten.as_ref(),
        "Arrow schema changed after rewrite"
    );
    Ok(())
}

fn read_arrow_schema_from_file(path: &Path) -> Result<std::sync::Arc<arrow_schema::Schema>> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

fn read_arrow_schema_from_bytes(bytes: &[u8]) -> Result<std::sync::Arc<arrow_schema::Schema>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    Ok(builder.schema().clone())
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn not_found(message: String) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message,
        }
    }

    fn internal(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("{err:#}"),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}
