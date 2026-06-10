//! Audio decoding (via ffmpeg) and chunking.

use anyhow::{anyhow, Result};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub const SAMPLE_RATE: usize = 16_000;

/// Decode arbitrary audio bytes (wav/mp3/m4a/ogg/opus/flac/...) to 16 kHz mono f32
/// by shelling out to ffmpeg. ffmpeg must be on PATH. Takes ownership of `input`
/// to avoid copying large uploads.
pub async fn decode_to_pcm(input: Vec<u8>) -> Result<Vec<f32>> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "f32le",
            "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Kill the child if this future is dropped (e.g. client disconnects
        // mid-transcription) so we don't orphan ffmpeg processes.
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow!("failed to spawn ffmpeg (is it installed?): {e}"))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("no ffmpeg stdin"))?;
    // Feed stdin concurrently with draining stdout to avoid a pipe deadlock on
    // large inputs. Dropping `stdin` at the end of the task closes the pipe (EOF).
    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&input).await;
    });

    let output = child.wait_with_output().await?;
    let _ = writer.await;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffmpeg failed: {}", err.trim()));
    }

    let bytes = output.stdout;
    if bytes.len() % 4 != 0 {
        return Err(anyhow!("ffmpeg produced misaligned PCM output"));
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
