//! End-to-end transcription pipeline: chunk -> engine -> assemble.

use crate::audio;
use crate::engine::EngineHandle;
use crate::transcript::{assemble, TranscriptOutput};
use anyhow::Result;

/// Chunk size for the sync/async endpoints — large, to keep full context while
/// still bounding RAM on very long audio.
pub const CHUNK_SECS: f32 = 240.0;

/// Smaller chunk size for SSE streaming so partial text arrives quickly.
pub const STREAM_CHUNK_SECS: f32 = 20.0;

/// Transcribe a full decoded buffer, chunking as needed.
pub async fn transcribe_samples(
    engine: &EngineHandle,
    samples: Vec<f32>,
    chunk_secs: f32,
) -> Result<TranscriptOutput> {
    let duration = audio::duration_secs(&samples);
    let chunks = audio::chunk(&samples, chunk_secs);
    let mut results = Vec::with_capacity(chunks.len());
    for (buf, offset) in chunks {
        let res = engine.transcribe(buf).await?;
        results.push((res, offset));
    }
    Ok(assemble(results, duration))
}
