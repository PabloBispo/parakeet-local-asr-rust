mod audio;
mod docs;
mod engine;
mod error;
mod ffmpeg;
mod jobs;
mod metrics;
mod model;
mod pipeline;
mod routes;
mod state;
mod transcript;
mod update;

use anyhow::Result;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use state::AppState;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

// Allow large audio uploads (default axum limit is 2 MB). Each upload is buffered
// in RAM, so keep this bounded for modest hardware.
const MAX_UPLOAD_BYTES: usize = 512 * 1024 * 1024; // 512 MB

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("update") => return update::run().await,
        Some("version") | Some("--version") | Some("-V") => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_help();
            return Ok(());
        }
        None | Some("serve") => {}
        Some(other) => {
            eprintln!("unknown command: {other}\n");
            print_help();
            std::process::exit(2);
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "parakeet_local_asr_rust=info,tower_http=warn".into()),
        )
        .init();

    let model_id = std::env::var("ASR_MODEL").unwrap_or_else(|_| "parakeet-tdt-0.6b-v3".into());
    let models_dir = PathBuf::from(std::env::var("MODELS_DIR").unwrap_or_else(|_| "models".into()));
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
    let jobs = jobs::JobQueue::start(engine.clone(), metrics.clone(), ffmpeg.bin.clone());

    let state = AppState {
        engine,
        jobs,
        metrics,
        ffmpeg,
        model_id,
        device,
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

fn print_help() {
    println!(
        "parakeet-local-asr-rust {} — OpenAI-compatible Parakeet ASR server\n\n\
USAGE:\n  \
parakeet-local-asr-rust [serve]   start the server (default)\n  \
parakeet-local-asr-rust update    update to the latest release (checksum-verified)\n  \
parakeet-local-asr-rust version   print the version\n  \
parakeet-local-asr-rust help      show this help\n\n\
ENV: PORT (8090) · ASR_MODEL · MODELS_DIR · ASR_DEVICE · FFMPEG_PATH · RUST_LOG",
        env!("CARGO_PKG_VERSION")
    );
}
