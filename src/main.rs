mod audio;
mod docs;
mod engine;
mod error;
mod ffmpeg;
mod history;
mod jobs;
mod metrics;
mod model;
mod pipeline;
mod ras;
mod routes;
mod state;
mod transcript;
mod update;
mod watcher;

use anyhow::Result;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use state::AppState;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

// Allow large audio uploads (default axum limit is 2 MB). Each upload is buffered
// in RAM, so keep this bounded for modest hardware.
const MAX_UPLOAD_BYTES: usize = 512 * 1024 * 1024; // 512 MB

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    // Everything after the (optional) `serve` subcommand is a serve flag.
    let serve_args: &[String] = match argv.first().map(String::as_str) {
        Some("update") => return update::run().await,
        Some("version") | Some("--version") | Some("-V") => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_help();
            return Ok(());
        }
        Some("serve") => &argv[1..],
        None => &[],
        // `serve` is the default, so flags may be given without naming it.
        Some(flag) if flag.starts_with('-') => &argv[..],
        Some(other) => {
            eprintln!("unknown command: {other}\n");
            print_help();
            std::process::exit(2);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "parakeet_local_asr_rust=info,tower_http=warn".into()),
        )
        .init();

    let watch_cfg = match watcher::WatchConfig::parse(serve_args) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{e}\n");
            print_help();
            std::process::exit(2);
        }
    };

    let model_id = std::env::var("ASR_MODEL").unwrap_or_else(|_| "parakeet-tdt-0.6b-v3".into());
    let ras_home = ras::home();
    if let Err(e) = std::fs::create_dir_all(&ras_home) {
        tracing::warn!("cannot create data home {}: {e}", ras_home.display());
    }
    tracing::info!("data home: {}", ras_home.display());
    let models_dir = resolve_models_dir(&ras_home, &model_id);
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);
    let device = std::env::var("ASR_DEVICE").unwrap_or_else(|_| "cpu".into());

    tracing::info!("ensuring model '{model_id}' under {}", models_dir.display());
    let model_dir = model::ensure_model(&models_dir, &model_id).await?;

    tracing::info!("loading engine (first load can take a few seconds)...");
    let engine = engine::EngineHandle::spawn(model_dir)?;

    let ffmpeg = Arc::new(ffmpeg::detect());
    if ffmpeg.available {
        tracing::info!("ffmpeg found (source: {})", ffmpeg.source);
    } else {
        tracing::warn!(
            "ffmpeg NOT found — audio decoding will fail until installed: {}",
            ffmpeg::Ffmpeg::install_hint()
        );
    }

    let metrics = Arc::new(metrics::Metrics::default());
    let history = Arc::new(history::HistoryStore::new(&ras_home));
    let jobs = jobs::JobQueue::start(
        engine.clone(),
        metrics.clone(),
        ffmpeg.bin.clone(),
        history.clone(),
    );

    if !watch_cfg.dirs.is_empty() && !ffmpeg.available {
        tracing::warn!("watcher: ffmpeg is missing — watched files will fail to decode");
    }
    let watcher = watcher::start(
        watch_cfg,
        engine.clone(),
        history.clone(),
        ffmpeg.bin.clone(),
        &ras_home,
    );

    let state = AppState {
        engine,
        jobs,
        metrics,
        ffmpeg,
        model_id,
        device,
        history,
        watcher,
    };

    let app = Router::new()
        .route("/", get(routes::index))
        .route("/ui", get(routes::ui))
        .route("/docs", get(routes::docs))
        .route("/health", get(routes::health))
        .route("/metrics", get(routes::metrics))
        .route("/v1/audio/transcriptions", post(routes::transcriptions))
        .route(
            "/v1/audio/transcriptions/stream",
            post(routes::transcriptions_stream),
        )
        .route(
            "/v1/audio/transcriptions/async",
            post(routes::transcriptions_async),
        )
        .route("/v1/audio/jobs/:job_id", get(routes::job_status))
        .route("/v1/history", get(routes::history_list))
        .route(
            "/v1/history/:id",
            get(routes::history_get).delete(routes::history_delete),
        )
        .route("/v1/history/:id/audio", get(routes::history_audio))
        .route("/v1/history/:id/download", get(routes::history_download))
        .route("/v1/watcher", get(routes::watcher_status))
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("parakeet-local-asr-rust listening on http://{addr}");

    // Open the UI in the default browser (best-effort; set ASR_NO_OPEN=1 to skip,
    // e.g. on a headless server / in Docker). The listener is already bound, so the
    // browser's connection is queued until the accept loop below picks it up.
    if std::env::var_os("ASR_NO_OPEN").is_none() {
        let url = format!("http://localhost:{port}/ui");
        tracing::info!("opening {url}");
        let _ = open::that_detached(&url);
    }

    axum::serve(listener, app).await?;
    Ok(())
}

/// Where to keep (and look for) the model bundle:
///
/// 1. `MODELS_DIR` when set — Docker mounts a volume at `/models`.
/// 2. `./models/<model>` when it already exists — don't orphan an existing
///    install by silently switching to the data home.
/// 3. `$RAS_HOME/models` — the default, shared across working directories.
fn resolve_models_dir(ras_home: &Path, model_id: &str) -> PathBuf {
    if let Some(raw) = std::env::var_os("MODELS_DIR") {
        if !raw.is_empty() {
            return PathBuf::from(raw);
        }
    }
    let ras_models = ras::models_dir(ras_home);
    let legacy = PathBuf::from("models");
    let legacy_has_model = model::dir_name(model_id)
        .map(|name| legacy.join(name).is_dir())
        .unwrap_or(false);
    if legacy_has_model {
        tracing::info!(
            "using the model in ./models (legacy layout) — move it to {} to share one \
             cache across working directories",
            ras_models.display()
        );
        return legacy;
    }
    ras_models
}

fn print_help() {
    println!(
        "parakeet-local-asr-rust {} — OpenAI-compatible Parakeet ASR server\n\n\
USAGE:\n  \
parakeet-local-asr-rust [serve] [OPTIONS]   start the server (default)\n  \
parakeet-local-asr-rust update              update to the latest release (checksum-verified)\n  \
parakeet-local-asr-rust version             print the version\n  \
parakeet-local-asr-rust help                show this help\n\n\
SERVE OPTIONS:\n  \
--watch <dir>       transcribe audio files that appear in <dir> (repeatable)\n  \
--watch-ext <ext>   extensions the watcher picks up, default .ogg (repeatable)\n  \
--no-notify         do not send desktop notifications for watched files\n\n\
EXAMPLE:\n  \
parakeet-local-asr-rust serve --watch ~/Downloads --watch-ext .ogg\n\n\
ENV:\n  \
PORT (8090) · ASR_MODEL · MODELS_DIR · ASR_DEVICE · FFMPEG_PATH · RUST_LOG\n  \
RAS_HOME (~/.ras) — transcripts, audio and model cache live here\n  \
ASR_WATCH_DIRS · ASR_WATCH_EXTS (comma-separated) · ASR_NO_NOTIFY\n  \
ASR_NO_HISTORY — do not persist transcripts · ASR_NO_OPEN — do not open the browser",
        env!("CARGO_PKG_VERSION")
    );
}
