//! Transcript output type + assembly of per-chunk engine results into one transcript.

use serde::Serialize;
use transcribe_rs::TranscriptionResult;

#[derive(Clone, Serialize)]
pub struct Segment {
    pub id: usize,
    pub start: f32,
    pub end: f32,
    pub text: String,
}

#[derive(Clone, Serialize)]
pub struct TranscriptOutput {
    pub text: String,
    pub duration: f32,
    pub segments: Vec<Segment>,
}

/// Merge chunk results into one transcript. Each entry is `(result, offset_secs)`
/// where `offset_secs` is the chunk's start position in the full audio. The engine
/// already returns chunk-relative timestamps; we add the offset to make them absolute.
pub fn assemble(chunks: Vec<(TranscriptionResult, f32)>, duration: f32) -> TranscriptOutput {
    let mut text = String::new();
    let mut segments = Vec::new();

    for (res, offset) in chunks {
        let trimmed = res.text.trim();
        if !trimmed.is_empty() {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(trimmed);
        }
        if let Some(segs) = res.segments {
            for s in segs {
                segments.push(Segment {
                    id: segments.len(),
                    start: s.start + offset,
                    end: s.end + offset,
                    text: s.text,
                });
            }
        }
    }

    TranscriptOutput {
        text,
        duration,
        segments,
    }
}
