//! ASR engine actor.
//!
//! `ParakeetModel` wraps ONNX Runtime sessions and is **not** `Send`/`Sync`-safe to
//! move between threads mid-flight. We pin it to a single dedicated OS thread (the
//! "actor") and talk to it over channels. Inference is serialized — one transcription
//! at a time — which also bounds peak RAM (important on small machines / containers).

use anyhow::{anyhow, Result};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::thread;
use tokio::sync::{mpsc, oneshot};
use transcribe_rs::onnx::parakeet::{ParakeetModel, ParakeetParams, TimestampGranularity};
use transcribe_rs::onnx::Quantization;
use transcribe_rs::TranscriptionResult;

struct EngineMsg {
    audio: Vec<f32>,
    reply: oneshot::Sender<Result<TranscriptionResult>>,
}

/// Cheap-to-clone handle to the engine actor. Safe to store in shared app state.
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineMsg>,
}

impl EngineHandle {
    /// Spawn the engine thread and load the model. Blocks until the model is
    /// loaded (or returns the load error).
    pub fn spawn(model_dir: PathBuf) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<EngineMsg>(64);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();

        thread::Builder::new()
            .name("asr-engine".into())
            .spawn(move || {
                let mut model = match ParakeetModel::load(&model_dir, &Quantization::Int8) {
                    Ok(m) => {
                        let _ = ready_tx.send(Ok(()));
                        m
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(anyhow!("failed to load model: {e}")));
                        return;
                    }
                };

                // Serialized inference loop.
                while let Some(msg) = rx.blocking_recv() {
                    let params = ParakeetParams {
                        language: None,
                        timestamp_granularity: Some(TimestampGranularity::Segment),
                    };
                    // Isolate inference panics (e.g. deep in ORT / FFI) so one bad
                    // request becomes an error reply instead of killing the actor
                    // and silently bricking the server.
                    let res = catch_unwind(AssertUnwindSafe(|| {
                        model.transcribe_with(&msg.audio, &params)
                    }))
                    .map_err(|_| anyhow!("engine panicked during transcription"))
                    .and_then(|r| r.map_err(|e| anyhow!("transcription failed: {e}")));
                    let _ = msg.reply.send(res);
                }
                tracing::info!("engine thread shutting down");
            })
            .map_err(|e| anyhow!("failed to spawn engine thread: {e}"))?;

        ready_rx
            .recv()
            .map_err(|_| anyhow!("engine thread died during startup"))??;
        Ok(Self { tx })
    }

    /// Whether the engine actor thread is still alive (its receiver is open).
    /// Goes `false` only if the thread exited (which catch_unwind now prevents
    /// on per-request panics).
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Transcribe one buffer of 16 kHz mono f32 samples. Awaits its turn on the
    /// engine thread, then the result.
    pub async fn transcribe(&self, audio: Vec<f32>) -> Result<TranscriptionResult> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineMsg { audio, reply })
            .await
            .map_err(|_| anyhow!("engine channel closed"))?;
        rx.await.map_err(|_| anyhow!("engine dropped the reply"))?
    }
}
