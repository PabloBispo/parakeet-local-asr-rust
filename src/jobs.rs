//! Async job queue for long audio. Submissions go onto a bounded channel and a
//! single background worker drains them through the (serialized) engine.
//!
//! The in-memory store is bounded: finished jobs are evicted after a TTL, and a
//! hard cap drops the oldest finished jobs if the map still grows too large. The
//! store uses a non-poisoning `parking_lot::Mutex` so a panic elsewhere can never
//! turn `/metrics`, `/async`, or `/jobs/{id}` into permanent failures.

use crate::engine::EngineHandle;
use crate::history::{HistoryRecord, HistoryStore, SOURCE_API_ASYNC};
use crate::metrics::Metrics;
use crate::pipeline;
use crate::transcript::TranscriptOutput;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// How long a finished job is retained before eviction.
const JOB_TTL: Duration = Duration::from_secs(3600);
/// Hard cap on stored jobs; oldest finished jobs are dropped beyond this.
const MAX_JOBS: usize = 1000;

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Processing,
    Done,
    Failed,
}

#[derive(Clone, serde::Serialize)]
pub struct Job {
    pub job_id: String,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<TranscriptOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip)]
    created_at: Instant,
}

type JobStore = Arc<Mutex<HashMap<String, Job>>>;

struct JobRequest {
    job_id: String,
    /// Upload filename, used as the history record's name.
    name: String,
    audio_bytes: Vec<u8>,
}

/// Cheap-to-clone handle to the job queue + store.
#[derive(Clone)]
pub struct JobQueue {
    tx: mpsc::Sender<JobRequest>,
    store: JobStore,
}

impl JobQueue {
    pub fn start(
        engine: EngineHandle,
        metrics: Arc<Metrics>,
        ffmpeg_bin: PathBuf,
        history: Arc<HistoryStore>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<JobRequest>(128);
        let store: JobStore = Arc::new(Mutex::new(HashMap::new()));

        let worker_store = store.clone();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                set_status(&worker_store, &req.job_id, JobStatus::Processing);
                let started = Instant::now();
                let outcome = process(&engine, &ffmpeg_bin, &req.audio_bytes).await;
                metrics.record(started.elapsed().as_millis() as u64);

                // Only successful transcriptions are persisted: a failed API call
                // reports back over HTTP, so saving it would just litter the UI.
                if let Ok(out) = &outcome {
                    let record = HistoryRecord::done(&req.name, SOURCE_API_ASYNC, out);
                    let history = history.clone();
                    let bytes = req.audio_bytes;
                    if let Err(e) =
                        tokio::task::spawn_blocking(move || history.save(record, Some(&bytes)))
                            .await
                    {
                        tracing::warn!("history: save task failed: {e}");
                    }
                }

                let mut guard = worker_store.lock();
                if let Some(job) = guard.get_mut(&req.job_id) {
                    match outcome {
                        Ok(out) => {
                            job.status = JobStatus::Done;
                            job.result = Some(out);
                        }
                        Err(e) => {
                            job.status = JobStatus::Failed;
                            job.error = Some(e.to_string());
                        }
                    }
                }
            }
        });

        Self { tx, store }
    }

    /// Enqueue audio for async transcription; returns the new job id.
    /// `name` is the upload filename (used for the history record).
    pub async fn submit(&self, audio_bytes: Vec<u8>, name: Option<String>) -> String {
        let job_id = uuid::Uuid::new_v4().to_string();
        {
            let mut guard = self.store.lock();
            evict(&mut guard);
            guard.insert(
                job_id.clone(),
                Job {
                    job_id: job_id.clone(),
                    status: JobStatus::Queued,
                    result: None,
                    error: None,
                    created_at: Instant::now(),
                },
            );
        }
        // Awaits if the channel is full — natural back-pressure.
        if self
            .tx
            .send(JobRequest {
                job_id: job_id.clone(),
                name: name.unwrap_or_else(|| "upload".to_string()),
                audio_bytes,
            })
            .await
            .is_err()
        {
            // Worker task is gone — don't leave the job stuck in Queued forever.
            if let Some(job) = self.store.lock().get_mut(&job_id) {
                job.status = JobStatus::Failed;
                job.error = Some("job worker unavailable".into());
            }
        }
        job_id
    }

    pub fn get(&self, job_id: &str) -> Option<Job> {
        self.store.lock().get(job_id).cloned()
    }

    pub fn depth(&self) -> usize {
        self.store
            .lock()
            .values()
            .filter(|j| matches!(j.status, JobStatus::Queued | JobStatus::Processing))
            .count()
    }
}

async fn process(
    engine: &EngineHandle,
    ffmpeg_bin: &Path,
    bytes: &[u8],
) -> anyhow::Result<TranscriptOutput> {
    let samples = crate::audio::decode_to_pcm(ffmpeg_bin, bytes).await?;
    pipeline::transcribe_samples(engine, samples, pipeline::CHUNK_SECS).await
}

fn set_status(store: &JobStore, job_id: &str, status: JobStatus) {
    if let Some(job) = store.lock().get_mut(job_id) {
        job.status = status;
    }
}

/// Evict finished jobs older than the TTL; if still over `MAX_JOBS`, drop the
/// oldest finished jobs. In-flight (Queued/Processing) jobs are never evicted.
fn evict(store: &mut HashMap<String, Job>) {
    store.retain(|_, j| {
        let finished = matches!(j.status, JobStatus::Done | JobStatus::Failed);
        !(finished && j.created_at.elapsed() > JOB_TTL)
    });

    if store.len() <= MAX_JOBS {
        return;
    }
    let mut finished: Vec<(String, Instant)> = store
        .iter()
        .filter(|(_, j)| matches!(j.status, JobStatus::Done | JobStatus::Failed))
        .map(|(k, j)| (k.clone(), j.created_at))
        .collect();
    finished.sort_by_key(|(_, t)| *t);
    let to_remove = store.len().saturating_sub(MAX_JOBS);
    for (k, _) in finished.into_iter().take(to_remove) {
        store.remove(&k);
    }
}
