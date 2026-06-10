use crate::engine::EngineHandle;
use crate::ffmpeg::Ffmpeg;
use crate::jobs::JobQueue;
use crate::metrics::Metrics;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
    pub jobs: JobQueue,
    pub metrics: Arc<Metrics>,
    pub ffmpeg: Arc<Ffmpeg>,
    pub model_id: String,
    pub device: String,
}
