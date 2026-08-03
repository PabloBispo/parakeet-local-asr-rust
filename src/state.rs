use crate::engine::EngineHandle;
use crate::ffmpeg::Ffmpeg;
use crate::history::HistoryStore;
use crate::jobs::JobQueue;
use crate::metrics::Metrics;
use crate::watcher::WatcherStatus;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
    pub jobs: JobQueue,
    pub metrics: Arc<Metrics>,
    pub ffmpeg: Arc<Ffmpeg>,
    pub model_id: String,
    pub device: String,
    /// Persistent transcript history under `$RAS_HOME`.
    pub history: Arc<HistoryStore>,
    /// Live folder-watcher counters (disabled placeholder when not watching).
    pub watcher: Arc<WatcherStatus>,
}
