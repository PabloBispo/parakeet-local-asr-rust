//! Transcript output type + assembly of per-chunk engine results into one transcript.

use serde::{Deserialize, Serialize};
use transcribe_rs::TranscriptionResult;

/// `Deserialize` is needed to read segments back out of the saved history.
#[derive(Clone, Serialize, Deserialize)]
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

// ── subtitle formats ──────────────────────────────────────────────────────────
// Shared by the transcription endpoints (`response_format=srt|vtt`) and the
// history download endpoint.

/// `HH:MM:SS<sep>mmm` — `sep` is `,` for SRT and `.` for WebVTT.
pub fn fmt_ts(t: f32, sep: char) -> String {
    let ms = (t.max(0.0) * 1000.0).round() as i64;
    let h = ms / 3_600_000;
    let m = (ms % 3_600_000) / 60_000;
    let s = (ms % 60_000) / 1000;
    let milli = ms % 1000;
    format!("{h:02}:{m:02}:{s:02}{sep}{milli:03}")
}

pub fn to_srt(out: &TranscriptOutput) -> String {
    let mut s = String::new();
    for (i, seg) in out.segments.iter().enumerate() {
        s.push_str(&format!(
            "{}\n{} --> {}\n{}\n\n",
            i + 1,
            fmt_ts(seg.start, ','),
            fmt_ts(seg.end, ','),
            seg.text.trim()
        ));
    }
    s
}

pub fn to_vtt(out: &TranscriptOutput) -> String {
    let mut s = String::from("WEBVTT\n\n");
    for seg in &out.segments {
        s.push_str(&format!(
            "{} --> {}\n{}\n\n",
            fmt_ts(seg.start, '.'),
            fmt_ts(seg.end, '.'),
            seg.text.trim()
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TranscriptOutput {
        TranscriptOutput {
            text: "one two".into(),
            duration: 3675.5,
            segments: vec![
                Segment {
                    id: 0,
                    start: 0.0,
                    end: 1.25,
                    text: " one ".into(),
                },
                Segment {
                    id: 1,
                    start: 3600.5,
                    end: 3675.5,
                    text: "two".into(),
                },
            ],
        }
    }

    #[test]
    fn timestamps_are_zero_padded_and_hour_aware() {
        assert_eq!(fmt_ts(0.0, ','), "00:00:00,000");
        assert_eq!(fmt_ts(-1.0, ','), "00:00:00,000");
        assert_eq!(fmt_ts(1.25, ','), "00:00:01,250");
        assert_eq!(fmt_ts(3675.5, '.'), "01:01:15.500");
    }

    #[test]
    fn srt_and_vtt_render_every_segment() {
        let srt = to_srt(&sample());
        assert!(srt.starts_with("1\n00:00:00,000 --> 00:00:01,250\none\n\n"));
        assert!(srt.contains("2\n01:00:00,500 --> 01:01:15,500\ntwo\n"));

        let vtt = to_vtt(&sample());
        assert!(vtt.starts_with("WEBVTT\n\n00:00:00.000 --> 00:00:01.250\none\n"));
        assert!(vtt.contains("01:00:00.500 --> 01:01:15.500\ntwo\n"));
    }
}
