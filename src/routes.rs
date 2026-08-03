//! HTTP handlers. OpenAI-compatible transcription + SSE stream + async jobs + ops.

use crate::history::{HistoryRecord, SOURCE_API};
use crate::pipeline::{self, STREAM_CHUNK_SECS};
use crate::transcript::{self, TranscriptOutput};
use crate::{audio, error::AppError, state::AppState};
use anyhow::anyhow;
use axum::{
    extract::{Multipart, Path, Query, State},
    http::{header, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::path::Path as FsPath;
use std::time::Instant;
use transcribe_rs::TranscriptionResult;

/// `GET /v1/history` page size.
const HISTORY_LIMIT_DEFAULT: usize = 100;
const HISTORY_LIMIT_MAX: usize = 500;

// ── multipart form ────────────────────────────────────────────────────────────

struct Form {
    file: Vec<u8>,
    /// Uploaded filename, when the client sent one.
    filename: Option<String>,
    response_format: String,
}

impl Form {
    /// Name to store this upload under in the history.
    fn name(&self) -> String {
        self.filename.clone().unwrap_or_else(|| "upload".to_string())
    }
}

async fn parse_form(mut mp: Multipart) -> Result<Form, AppError> {
    let mut file = None;
    let mut filename = None;
    let mut response_format = "json".to_string();

    while let Some(field) = mp
        .next_field()
        .await
        .map_err(|e| AppError(anyhow!("bad multipart: {e}")))?
    {
        match field.name().unwrap_or("") {
            "file" => {
                filename = field.file_name().map(|n| n.to_string()).filter(|n| !n.is_empty());
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| AppError(anyhow!("reading 'file': {e}")))?;
                file = Some(bytes.to_vec());
            }
            "response_format" => {
                if let Ok(v) = field.text().await {
                    response_format = v.trim().to_lowercase();
                }
            }
            // `model`, `language`, `temperature`, etc. are accepted and ignored
            // (Parakeet is configured server-side). Drain the field.
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    let file = file.ok_or_else(|| AppError(anyhow!("missing 'file' field")))?;
    if file.is_empty() {
        return Err(AppError(anyhow!("'file' field is empty")));
    }
    Ok(Form {
        file,
        filename,
        response_format,
    })
}

/// Reject transcription requests early (with an actionable message) when ffmpeg
/// is not available to decode audio.
fn ensure_ffmpeg(st: &AppState) -> Result<(), AppError> {
    if st.ffmpeg.available {
        Ok(())
    } else {
        Err(AppError(anyhow!(
            "ffmpeg is not installed on the server, so audio cannot be decoded. Install it and restart: {}",
            crate::ffmpeg::Ffmpeg::install_hint()
        )))
    }
}

// ── POST /v1/audio/transcriptions ─────────────────────────────────────────────

pub async fn transcriptions(
    State(st): State<AppState>,
    mp: Multipart,
) -> Result<Response, AppError> {
    let form = parse_form(mp).await?;
    ensure_ffmpeg(&st)?;
    let started = Instant::now();
    let samples = audio::decode_to_pcm(&st.ffmpeg.bin, &form.file).await?;
    let out = pipeline::transcribe_samples(&st.engine, samples, pipeline::CHUNK_SECS).await?;
    st.metrics.record(started.elapsed().as_millis() as u64);

    // Persist on success only — a failure already reports over HTTP, and saving it
    // would only clutter the UI's history. Archiving the upload can mean writing
    // hundreds of MB, so it goes to the blocking pool; awaited so the recording is
    // already listable by the time the client can call /v1/history.
    let record = HistoryRecord::done(form.name(), SOURCE_API, &out);
    let history = st.history.clone();
    let bytes = form.file;
    if let Err(e) = tokio::task::spawn_blocking(move || history.save(record, Some(&bytes))).await {
        tracing::warn!("history: save task failed: {e}");
    }

    Ok(render(&out, &form.response_format))
}

fn render(out: &TranscriptOutput, format: &str) -> Response {
    match format {
        "text" => out.text.clone().into_response(),
        "verbose_json" => Json(json!({
            "task": "transcribe",
            "duration": out.duration,
            "text": out.text,
            "segments": out.segments,
        }))
        .into_response(),
        "srt" => with_type(transcript::to_srt(out), SRT_CONTENT_TYPE),
        "vtt" => with_type(transcript::to_vtt(out), VTT_CONTENT_TYPE),
        // "json" and anything unrecognized → minimal OpenAI shape.
        _ => Json(json!({ "text": out.text })).into_response(),
    }
}

const SRT_CONTENT_TYPE: &str = "application/x-subrip; charset=utf-8";
const VTT_CONTENT_TYPE: &str = "text/vtt; charset=utf-8";
const TXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";

fn with_type(body: String, ct: &'static str) -> Response {
    ([(header::CONTENT_TYPE, ct)], body).into_response()
}

fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
}

// ── POST /v1/audio/transcriptions/stream (SSE) ────────────────────────────────

/// Streams partial text per chunk. Deliberately **not** persisted to the history:
/// the handler never assembles a final `TranscriptOutput` (that is the point of
/// streaming), and stitching one together just to save it would double the work
/// and could store a truncated transcript when the client disconnects mid-stream.
pub async fn transcriptions_stream(
    State(st): State<AppState>,
    mp: Multipart,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, AppError> {
    let form = parse_form(mp).await?;
    ensure_ffmpeg(&st)?;
    let samples = audio::decode_to_pcm(&st.ffmpeg.bin, &form.file).await?;
    let engine = st.engine.clone();
    let metrics = st.metrics.clone();
    let started = Instant::now();
    let chunks = audio::chunk(&samples, STREAM_CHUNK_SECS);
    let total = chunks.len();

    let stream = async_stream::stream! {
        for (i, (buf, offset)) in chunks.into_iter().enumerate() {
            let is_last = i + 1 == total;
            match engine.transcribe(buf).await {
                Ok(res) => {
                    let (start, end) = bounds(&res, offset);
                    let payload = json!({
                        "text": res.text.trim(),
                        "chunk_index": i,
                        "total_chunks": total,
                        "start": start,
                        "end": end,
                        "final": is_last,
                    });
                    yield Ok(Event::default().data(payload.to_string()));
                }
                Err(e) => {
                    let payload = json!({
                        "error": e.to_string(),
                        "chunk_index": i,
                        "final": true,
                    });
                    yield Ok(Event::default().data(payload.to_string()));
                    break;
                }
            }
        }
        metrics.record(started.elapsed().as_millis() as u64);
    };

    Ok(Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default()))
}

fn bounds(res: &TranscriptionResult, offset: f32) -> (f32, f32) {
    match &res.segments {
        Some(segs) if !segs.is_empty() => (
            segs.first().map(|s| s.start).unwrap_or(0.0) + offset,
            segs.last().map(|s| s.end).unwrap_or(0.0) + offset,
        ),
        _ => (offset, offset),
    }
}

// ── POST /v1/audio/transcriptions/async  +  GET /v1/audio/jobs/{id} ───────────

pub async fn transcriptions_async(
    State(st): State<AppState>,
    mp: Multipart,
) -> Result<Response, AppError> {
    let form = parse_form(mp).await?;
    ensure_ffmpeg(&st)?;
    let job_id = st.jobs.submit(form.file, form.filename).await;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "job_id": job_id, "status": "queued" })),
    )
        .into_response())
}

pub async fn job_status(
    State(st): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Response, AppError> {
    match st.jobs.get(&job_id) {
        Some(job) => Ok(Json(job).into_response()),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "job not found" })),
        )
            .into_response()),
    }
}

// ── ops ───────────────────────────────────────────────────────────────────────

pub async fn ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("../static/index.html"))
}

pub async fn docs() -> axum::response::Html<&'static str> {
    axum::response::Html(crate::docs::html())
}

pub async fn index(State(st): State<AppState>) -> Response {
    Json(json!({
        "service": "parakeet-local-asr-rust",
        "model": st.model_id,
        "device": st.device,
        "ffmpeg": st.ffmpeg.available,
        "ui": "/ui",
        "openai_base_url": "/v1",
        "endpoints": [
            "POST /v1/audio/transcriptions",
            "POST /v1/audio/transcriptions/stream",
            "POST /v1/audio/transcriptions/async",
            "GET /v1/audio/jobs/{id}",
            "GET /v1/history",
            "GET /v1/history/{id}",
            "GET /v1/history/{id}/audio",
            "GET /v1/history/{id}/download",
            "DELETE /v1/history/{id}",
            "GET /v1/watcher",
            "GET /health",
            "GET /metrics"
        ]
    }))
    .into_response()
}

pub async fn health(State(st): State<AppState>) -> Response {
    let engine_alive = st.engine.is_alive();
    let ffmpeg_ok = st.ffmpeg.available;
    // 200 as long as the engine is up (so the UI loads and can surface the ffmpeg
    // banner); 503 only when the engine itself is dead.
    let code = if engine_alive {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(json!({
            "status": if engine_alive && ffmpeg_ok { "ok" } else { "degraded" },
            "engine": engine_alive,
            "ffmpeg": ffmpeg_ok,
            "ffmpeg_source": st.ffmpeg.source,
            "ffmpeg_install": crate::ffmpeg::Ffmpeg::install_hint(),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "model": st.model_id,
            "device": st.device,
            "history_count": st.history.count(),
            "watcher_enabled": st.watcher.is_enabled(),
        })),
    )
        .into_response()
}

pub async fn metrics(State(st): State<AppState>) -> Response {
    let (total, avg) = st.metrics.snapshot();
    Json(json!({
        "queue_depth": st.jobs.depth(),
        "total_requests": total,
        "avg_latency_ms": avg,
    }))
    .into_response()
}

// ── persistent history (/v1/history) ──────────────────────────────────────────

#[derive(Deserialize)]
pub struct ListQuery {
    limit: Option<usize>,
}

/// `GET /v1/history?limit=N` → newest-first metadata + text, without segments.
pub async fn history_list(State(st): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let limit = q.limit.unwrap_or(HISTORY_LIMIT_DEFAULT).min(HISTORY_LIMIT_MAX);
    let history = st.history.clone();
    // Reads one small file per record — off the reactor.
    let items = match tokio::task::spawn_blocking(move || history.list(limit)).await {
        Ok(items) => items,
        Err(e) => {
            tracing::warn!("history: list task failed: {e}");
            Vec::new()
        }
    };
    Json(json!({ "items": items })).into_response()
}

/// `GET /v1/history/{id}` → the full record, segments included.
pub async fn history_get(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match st.history.get(&id) {
        Some(record) => Json(record).into_response(),
        None => not_found("recording not found"),
    }
}

/// `GET /v1/history/{id}/audio` → the archived original audio.
pub async fn history_audio(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(path) = st.history.audio_path(&id) else {
        return not_found("audio not found");
    };
    match tokio::fs::read(&path).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, audio_content_type(&path))], bytes).into_response(),
        Err(e) => {
            tracing::warn!("history: cannot read {}: {e}", path.display());
            not_found("audio not found")
        }
    }
}

fn audio_content_type(path: &FsPath) -> &'static str {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        // .opus files are Ogg-contained too (WhatsApp exports both spellings).
        Some("ogg") | Some("opus") => "audio/ogg",
        Some("wav") => "audio/wav",
        Some("mp3") => "audio/mpeg",
        Some("m4a") => "audio/mp4",
        Some("webm") => "audio/webm",
        Some("flac") => "audio/flac",
        _ => "application/octet-stream",
    }
}

#[derive(Deserialize)]
pub struct DownloadQuery {
    format: Option<String>,
}

/// `GET /v1/history/{id}/download?format=txt|srt|vtt|json` → attachment.
pub async fn history_download(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<DownloadQuery>,
) -> Response {
    let Some(record) = st.history.get(&id) else {
        return not_found("recording not found");
    };
    let out = TranscriptOutput {
        text: record.text.clone(),
        duration: record.duration,
        segments: record.segments.clone(),
    };
    let (body, content_type, ext) = match q.format.unwrap_or_default().to_lowercase().as_str() {
        "srt" => (transcript::to_srt(&out), SRT_CONTENT_TYPE, "srt"),
        "vtt" => (transcript::to_vtt(&out), VTT_CONTENT_TYPE, "vtt"),
        "json" => (
            serde_json::to_string_pretty(&record).unwrap_or_else(|_| "{}".into()),
            JSON_CONTENT_TYPE,
            "json",
        ),
        // "txt" and anything unrecognized.
        _ => (record.text.clone(), TXT_CONTENT_TYPE, "txt"),
    };
    // `record.id` passed the store's id validation, so it is safe in a header.
    (
        [
            (header::CONTENT_TYPE, content_type.to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}.{ext}\"", record.id),
            ),
        ],
        body,
    )
        .into_response()
}

/// `DELETE /v1/history/{id}` → removes record, text and archived audio.
pub async fn history_delete(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    Json(json!({ "deleted": st.history.delete(&id) })).into_response()
}

/// `GET /v1/watcher` → folder-watcher status and counters.
pub async fn watcher_status(State(st): State<AppState>) -> Response {
    Json(st.watcher.snapshot()).into_response()
}
