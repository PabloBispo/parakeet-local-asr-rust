//! Audio decoding (via ffmpeg) and chunking.

use anyhow::{anyhow, Result};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

pub const SAMPLE_RATE: usize = 16_000;

/// Decode arbitrary audio bytes (wav/mp3/m4a/ogg/opus/flac/...) to 16 kHz mono f32
/// by shelling out to ffmpeg (`ffmpeg_bin`). Borrows `input` so callers that also
/// archive the original bytes (history persistence) don't need a second copy.
///
/// The input is written to a temp file instead of being piped via stdin:
/// seekable containers (MP4/m4a) keep their index (`moov` atom) at the END of the
/// file, which ffmpeg cannot reach over a non-seekable pipe — piping silently
/// yields empty output. A real file path decodes every format reliably.
pub async fn decode_to_pcm(ffmpeg_bin: &Path, input: &[u8]) -> Result<Vec<f32>> {
    let tmp = std::env::temp_dir().join(format!("parakeet-asr-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&tmp, input)
        .await
        .map_err(|e| anyhow!("failed to write temp input file: {e}"))?;

    let result = run_ffmpeg(ffmpeg_bin, &tmp).await;
    let _ = tokio::fs::remove_file(&tmp).await; // best-effort cleanup
    result
}

async fn run_ffmpeg(ffmpeg_bin: &Path, input_path: &Path) -> Result<Vec<f32>> {
    let output = Command::new(ffmpeg_bin)
        .args(["-nostdin", "-loglevel", "error", "-i"])
        .arg(input_path)
        .args(["-ac", "1", "-ar", "16000", "-f", "f32le", "pipe:1"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Kill the child if this future is dropped (e.g. client disconnects).
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| anyhow!("failed to spawn ffmpeg (is it installed?): {e}"))?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffmpeg failed: {}", err.trim()));
    }

    let bytes = output.stdout;
    if bytes.len() % 4 != 0 {
        return Err(anyhow!("ffmpeg produced misaligned PCM output"));
    }
    // Surface a decode that produced nothing as an error instead of returning an
    // empty (but "successful") transcript.
    if bytes.is_empty() {
        return Err(anyhow!(
            "ffmpeg decoded no audio from the input (unsupported, empty, or corrupt file?)"
        ));
    }
    let samples = bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .collect();
    Ok(samples)
}

/// Split samples into chunks of at most `chunk_secs` seconds.
/// Returns `(chunk_samples, start_offset_secs)` pairs. A buffer shorter than one
/// chunk is returned as a single `(buffer, 0.0)` pair.
pub fn chunk(samples: &[f32], chunk_secs: f32) -> Vec<(Vec<f32>, f32)> {
    let chunk_len = (chunk_secs * SAMPLE_RATE as f32) as usize;
    if chunk_len == 0 || samples.len() <= chunk_len {
        return vec![(samples.to_vec(), 0.0)];
    }
    samples
        .chunks(chunk_len)
        .enumerate()
        .map(|(i, c)| (c.to_vec(), (i * chunk_len) as f32 / SAMPLE_RATE as f32))
        .collect()
}

pub fn duration_secs(samples: &[f32]) -> f32 {
    samples.len() as f32 / SAMPLE_RATE as f32
}
