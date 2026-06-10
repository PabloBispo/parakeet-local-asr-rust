//! Async job queue for long audio. Submissions go onto a bounded channel and a
//! single background worker drains them through the (serialized) engine.

use crate::engine::EngineHandle;
use crate::pipeline;
use crate::transcript::TranscriptOutput;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

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
}

type JobStore = Arc<Mutex<HashMap<String, Job>>>;

struct JobRequest {
    job_id: String,
    audio_bytes: Vec<u8>,
}

/// Cheap-to-clone handle to the job queue + store.
#[derive(Clone)]
pub struct JobQueue {
    tx: mpsc::Sender<JobRequest>,
    store: JobStore,
}

impl JobQueue {
    pub fn start(engine: EngineHandle) -> Self {
        let (tx, mut rx) = mpsc::channel::<JobRequest>(128);
        let store: JobStore = Arc::new(Mutex::new(HashMap::new()));

        let worker_store = store.clone();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                set_status(&worker_store, &req.job_id, JobStatus::Processing);
                let outcome = process(&engine, &req.audio_bytes).await;
                let mut guard = worker_store.lock().unwrap();
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
    pub async fn submit(&self, audio_bytes: Vec<u8>) -> String {
        let job_id = uuid::Uuid::new_v4().to_string();
        self.store.lock().unwrap().insert(
            job_id.clone(),
            Job {
                job_id: job_id.clone(),
                status: JobStatus::Queued,
                result: None,
                error: None,
            },
        );
        // Awaits if the channel is full — natural back-pressure.
        let _ = self
            .tx
            .send(JobRequest {
                job_id: job_id.clone(),
                audio_bytes,
            })
            .await;
        job_id
    }

    pub fn get(&self, job_id: &str) -> Option<Job> {
        self.store.lock().unwrap().get(job_id).cloned()
    }

    pub fn depth(&self) -> usize {
        self.store
            .lock()
            .unwrap()
            .values()
            .filter(|j| matches!(j.status, JobStatus::Queued | JobStatus::Processing))
            .count()
    }
}

async fn process(engine: &EngineHandle, bytes: &[u8]) -> anyhow::Result<TranscriptOutput> {
    let samples = crate::audio::decode_to_pcm(bytes).await?;
    pipeline::transcribe_samples(engine, samples, pipeline::CHUNK_SECS).await
}

fn set_status(store: &JobStore, job_id: &str, status: JobStatus) {
    if let Some(job) = store.lock().unwrap().get_mut(job_id) {
        job.status = status;
    }
}
