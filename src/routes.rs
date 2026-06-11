//! HTTP handlers. OpenAI-compatible transcription + SSE stream + async jobs + ops.

use crate::pipeline::{self, STREAM_CHUNK_SECS};
use crate::transcript::TranscriptOutput;
use crate::{audio, error::AppError, state::AppState};
use anyhow::anyhow;
use axum::{
    extract::{Multipart, Path, State},
    http::{header, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde_json::json;
use std::convert::Infallible;
use std::time::Instant;
use transcribe_rs::TranscriptionResult;

// ── multipart form ────────────────────────────────────────────────────────────

struct Form {
    file: Vec<u8>,
    response_format: String,
}

async fn parse_form(mut mp: Multipart) -> Result<Form, AppError> {
    let mut file = None;
    let mut response_format = "json".to_string();

    while let Some(field) = mp
        .next_field()
        .await
        .map_err(|e| AppError(anyhow!("bad multipart: {e}")))?
    {
        match field.name().unwrap_or("") {
            "file" => {
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
    let samples = audio::decode_to_pcm(&st.ffmpeg.bin, form.file).await?;
    let out = pipeline::transcribe_samples(&st.engine, samples, pipeline::CHUNK_SECS).await?;
    st.metrics.record(started.elapsed().as_millis() as u64);
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
        "srt" => with_type(to_srt(out), "application/x-subrip; charset=utf-8"),
        "vtt" => with_type(to_vtt(out), "text/vtt; charset=utf-8"),
        // "json" and anything unrecognized → minimal OpenAI shape.
        _ => Json(json!({ "text": out.text })).into_response(),
    }
}

fn with_type(body: String, ct: &'static str) -> Response {
    ([(header::CONTENT_TYPE, ct)], body).into_response()
}

// ── POST /v1/audio/transcriptions/stream (SSE) ────────────────────────────────

pub async fn transcriptions_stream(
    State(st): State<AppState>,
    mp: Multipart,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, AppError> {
    let form = parse_form(mp).await?;
    ensure_ffmpeg(&st)?;
    let samples = audio::decode_to_pcm(&st.ffmpeg.bin, form.file).await?;
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
    let job_id = st.jobs.submit(form.file).await;
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

// ── subtitle formats ──────────────────────────────────────────────────────────

fn fmt_ts(t: f32, sep: char) -> String {
    let ms = (t.max(0.0) * 1000.0).round() as i64;
    let h = ms / 3_600_000;
    let m = (ms % 3_600_000) / 60_000;
    let s = (ms % 60_000) / 1000;
    let milli = ms % 1000;
    format!("{h:02}:{m:02}:{s:02}{sep}{milli:03}")
}

fn to_srt(out: &TranscriptOutput) -> String {
    let mut s = String::new();
    for (i, seg) in out.segments.iter().enumerate() {
        s.push_str(&format!(
            "{}\n{} --> {}\n{}\n\n",
            i + 1,
            fmt_ts(seg.start, ','),
            fmt_ts(seg.end, ','),
            seg.text.trim()
        ));
    }
    s
}

fn to_vtt(out: &TranscriptOutput) -> String {
    let mut s = String::from("WEBVTT\n\n");
    for seg in &out.segments {
        s.push_str(&format!(
            "{} --> {}\n{}\n\n",
            fmt_ts(seg.start, '.'),
            fmt_ts(seg.end, '.'),
            seg.text.trim()
        ));
    }
    s
}
