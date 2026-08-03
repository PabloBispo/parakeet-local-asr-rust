//! Persistent transcript history under `$RAS_HOME`.
//!
//! One file per record, no central index. That choice is deliberate: there is no
//! index to corrupt or lock, a crash can lose at most the record being written,
//! records are readable with `cat`, and pruning is `rm`. Listing a few hundred
//! small JSON files is far cheaper than the coordination an index would need.
//!
//! ```text
//! $RAS_HOME/transcripts/<uuid>.json   the full record (metadata + text + segments)
//! $RAS_HOME/transcripts/<uuid>.txt    plain text — what a notification copies
//! $RAS_HOME/audio/<uuid>-<name>       the original audio, byte-for-byte
//! ```
//!
//! **Persistence must never break transcription.** Every IO error here is logged
//! and swallowed; nothing propagates into the request path.

use crate::ras;
use crate::transcript::{Segment, TranscriptOutput};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SOURCE_API: &str = "api";
pub const SOURCE_API_ASYNC: &str = "api-async";
pub const SOURCE_WATCHER: &str = "watcher";

pub const STATUS_DONE: &str = "done";
pub const STATUS_FAILED: &str = "failed";

/// Cap on the sanitized part of an archived audio filename (the `<uuid>-` prefix
/// is what makes it unique, so truncating the tail is safe).
const MAX_NAME_LEN: usize = 80;

/// One transcription, as stored on disk and returned by `/v1/history`.
#[derive(Clone, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub id: String,
    pub name: String,
    /// `"api"` | `"api-async"` | `"watcher"`.
    pub source: String,
    pub created_at_ms: u64,
    pub duration: f32,
    /// `"done"` | `"failed"`.
    pub status: String,
    pub error: Option<String>,
    pub text: String,
    pub segments: Vec<Segment>,
    /// Filename inside `$RAS_HOME/audio/`, if the audio was archived.
    pub audio_file: Option<String>,
}

impl HistoryRecord {
    pub fn done(name: impl Into<String>, source: &str, out: &TranscriptOutput) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            source: source.to_string(),
            created_at_ms: now_ms(),
            duration: out.duration,
            status: STATUS_DONE.to_string(),
            error: None,
            text: out.text.clone(),
            segments: out.segments.clone(),
            audio_file: None,
        }
    }

    pub fn failed(name: impl Into<String>, source: &str, error: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            source: source.to_string(),
            created_at_ms: now_ms(),
            duration: 0.0,
            status: STATUS_FAILED.to_string(),
            error: Some(error.into()),
            text: String::new(),
            segments: Vec::new(),
            audio_file: None,
        }
    }
}

/// Reader/writer for the on-disk history. Cheap to clone behind an `Arc`.
pub struct HistoryStore {
    transcripts: PathBuf,
    audio: PathBuf,
    enabled: bool,
}

impl HistoryStore {
    /// Store rooted at `ras_home`. Disabled (all writes become no-ops) when
    /// `ASR_NO_HISTORY` is set.
    pub fn new(ras_home: &Path) -> Self {
        Self::with_enabled(ras_home, !ras::env_flag("ASR_NO_HISTORY"))
    }

    pub fn with_enabled(ras_home: &Path, enabled: bool) -> Self {
        let transcripts = ras::transcripts_dir(ras_home);
        let audio = ras::audio_dir(ras_home);
        if enabled {
            for dir in [&transcripts, &audio] {
                if let Err(e) = fs::create_dir_all(dir) {
                    tracing::warn!("history: cannot create {}: {e}", dir.display());
                }
            }
        } else {
            tracing::info!("history: persistence disabled (ASR_NO_HISTORY)");
        }
        Self {
            transcripts,
            audio,
            enabled,
        }
    }

    /// Persist a record (plus, optionally, the original audio) and return its id.
    ///
    /// The id is returned even when persistence is disabled or a write failed, so
    /// callers can reference the transcription in logs/notifications regardless.
    pub fn save(&self, mut record: HistoryRecord, audio_bytes: Option<&[u8]>) -> String {
        let id = record.id.clone();
        if !self.enabled {
            return id;
        }

        // Audio first: the JSON record has to carry the final filename.
        if let Some(bytes) = audio_bytes {
            let file_name = format!("{id}-{}", sanitize_name(&record.name));
            match ras::write_atomic(&self.audio.join(&file_name), bytes) {
                Ok(()) => record.audio_file = Some(file_name),
                Err(e) => tracing::warn!("history: cannot archive audio for {id}: {e}"),
            }
        }

        if let Err(e) = ras::write_atomic(&self.txt_path(&id), record.text.as_bytes()) {
            tracing::warn!("history: cannot write transcript text for {id}: {e}");
        }
        // The JSON record goes last: it is what makes the recording visible in
        // `/v1/history`, so by the time it appears the audio and text are there.
        match serde_json::to_string_pretty(&record) {
            Ok(json) => {
                if let Err(e) = ras::write_atomic(&self.json_path(&id), json.as_bytes()) {
                    tracing::warn!("history: cannot write record {id}: {e}");
                }
            }
            Err(e) => tracing::warn!("history: cannot serialize record {id}: {e}"),
        }
        id
    }

    /// Newest-first records, capped at `limit`. `segments` is cleared — the list
    /// view only needs metadata + text; callers fetch details by id.
    pub fn list(&self, limit: usize) -> Vec<HistoryRecord> {
        let mut items = Vec::new();
        let entries = match fs::read_dir(&self.transcripts) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return items,
            Err(e) => {
                tracing::warn!("history: cannot list {}: {e}", self.transcripts.display());
                return items;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Some(mut record) = read_record(&path) {
                record.segments.clear();
                items.push(record);
            }
        }
        items.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));
        items.truncate(limit);
        items
    }

    pub fn get(&self, id: &str) -> Option<HistoryRecord> {
        if !valid_id(id) {
            tracing::warn!("history: rejected malformed id {id:?}");
            return None;
        }
        read_record(&self.json_path(id))
    }

    /// Remove the record, its text file and its archived audio.
    /// Returns whether anything was actually removed.
    pub fn delete(&self, id: &str) -> bool {
        if !valid_id(id) {
            tracing::warn!("history: rejected malformed id {id:?}");
            return false;
        }
        let mut removed = fs::remove_file(self.json_path(id)).is_ok();
        removed |= fs::remove_file(self.txt_path(id)).is_ok();
        if let Some(path) = self.audio_path(id) {
            removed |= fs::remove_file(path).is_ok();
        }
        removed
    }

    /// Path of the archived audio for `id` (the suffix depends on the upload name).
    pub fn audio_path(&self, id: &str) -> Option<PathBuf> {
        if !valid_id(id) {
            return None;
        }
        let prefix = format!("{id}-");
        fs::read_dir(&self.audio)
            .ok()?
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
            })
    }

    pub fn count(&self) -> usize {
        match fs::read_dir(&self.transcripts) {
            Ok(entries) => entries
                .flatten()
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                .count(),
            Err(_) => 0,
        }
    }

    fn json_path(&self, id: &str) -> PathBuf {
        self.transcripts.join(format!("{id}.json"))
    }

    /// Plain-text transcript path. Public because the watcher hands it to
    /// `terminal-notifier` so clicking the notification copies the transcript.
    pub fn txt_path(&self, id: &str) -> PathBuf {
        self.transcripts.join(format!("{id}.txt"))
    }
}

fn read_record(path: &Path) -> Option<HistoryRecord> {
    let raw = match fs::read_to_string(path) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!("history: cannot read {}: {e}", path.display());
            return None;
        }
    };
    match serde_json::from_str(&raw) {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!("history: skipping corrupt record {}: {e}", path.display());
            None
        }
    }
}

/// Ids are uuids we generated. Accept only `[A-Za-z0-9-]` so a request can never
/// escape the transcripts directory (`..`, `/`, `\` are all rejected).
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// Make an upload name safe to use as a filename component.
fn sanitize_name(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    // Every char is ASCII at this point, so truncating bytes is char-safe.
    out.truncate(MAX_NAME_LEN);
    if out.is_empty() {
        return "audio".to_string();
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn out(text: &str) -> TranscriptOutput {
        TranscriptOutput {
            text: text.to_string(),
            duration: 1.5,
            segments: vec![Segment {
                id: 0,
                start: 0.0,
                end: 1.5,
                text: text.to_string(),
            }],
        }
    }

    #[test]
    fn sanitize_keeps_safe_chars_and_replaces_the_rest() {
        assert_eq!(sanitize_name("audio-01_final.ogg"), "audio-01_final.ogg");
        assert_eq!(sanitize_name("a b/c\\d:e"), "a_b_c_d_e");
        assert_eq!(sanitize_name("../../etc/passwd"), ".._.._etc_passwd");
        assert_eq!(sanitize_name("áudio ção.ogg"), "_udio___o.ogg");
        assert_eq!(sanitize_name(""), "audio");
        assert_eq!(sanitize_name(&"x".repeat(300)).len(), MAX_NAME_LEN);
    }

    #[test]
    fn ids_with_path_traversal_are_rejected() {
        assert!(valid_id("2f1c9d3e-0b1a-4c2d-8e5f-6a7b8c9d0e1f"));
        assert!(valid_id("abc123"));
        assert!(!valid_id(""));
        assert!(!valid_id(".."));
        assert!(!valid_id("../../etc/passwd"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id("a\\b"));
        assert!(!valid_id("a.b"));
        assert!(!valid_id("id with space"));
        assert!(!valid_id(&"a".repeat(65)));
    }

    #[test]
    fn store_roundtrip_save_get_list_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), true);
        assert_eq!(store.count(), 0);
        assert!(store.list(10).is_empty());

        let id = store.save(
            HistoryRecord::done("voice note.ogg", SOURCE_API, &out("hello there")),
            Some(b"RIFFfake"),
        );

        // Files landed where the layout says they should.
        assert!(tmp.path().join("transcripts").join(format!("{id}.json")).is_file());
        let txt = tmp.path().join("transcripts").join(format!("{id}.txt"));
        assert_eq!(std::fs::read_to_string(&txt).unwrap(), "hello there");
        let audio = store.audio_path(&id).expect("audio archived");
        assert_eq!(std::fs::read(&audio).unwrap(), b"RIFFfake");
        assert_eq!(
            audio.file_name().unwrap().to_str().unwrap(),
            format!("{id}-voice_note.ogg")
        );

        // get() returns the full record, list() strips segments but keeps text.
        let got = store.get(&id).expect("record readable");
        assert_eq!(got.name, "voice note.ogg");
        assert_eq!(got.source, SOURCE_API);
        assert_eq!(got.status, STATUS_DONE);
        assert_eq!(got.segments.len(), 1);
        assert_eq!(got.audio_file.as_deref(), Some(&*format!("{id}-voice_note.ogg")));

        let listed = store.list(10);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].text, "hello there");
        assert!(listed[0].segments.is_empty());
        assert_eq!(store.count(), 1);

        assert!(store.delete(&id));
        assert!(store.get(&id).is_none());
        assert!(store.audio_path(&id).is_none());
        assert_eq!(store.count(), 0);
        assert!(!store.delete(&id));
    }

    #[test]
    fn list_is_newest_first_and_respects_the_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), true);

        for (name, ts) in [("old", 1_000u64), ("newest", 3_000), ("middle", 2_000)] {
            let mut rec = HistoryRecord::done(name, SOURCE_WATCHER, &out(name));
            rec.created_at_ms = ts;
            store.save(rec, None);
        }

        let names: Vec<String> = store.list(10).into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["newest", "middle", "old"]);
        let top: Vec<String> = store.list(2).into_iter().map(|r| r.name).collect();
        assert_eq!(top, ["newest", "middle"]);
    }

    #[test]
    fn corrupt_records_are_skipped_not_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), true);
        store.save(HistoryRecord::done("good.ogg", SOURCE_API, &out("ok")), None);
        std::fs::write(tmp.path().join("transcripts").join("broken.json"), "{not json").unwrap();
        std::fs::write(tmp.path().join("transcripts").join("notes.md"), "ignored").unwrap();

        let listed = store.list(10);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "good.ogg");
    }

    /// The atomic-write temp files must be invisible to every lookup path.
    #[test]
    fn stray_temp_files_are_never_mistaken_for_records() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), true);
        let id = store.save(
            HistoryRecord::done("a.ogg", SOURCE_API, &out("hi")),
            Some(b"bytes"),
        );

        // Simulate a crash between the temp write and the rename.
        std::fs::write(tmp.path().join("transcripts").join(".tmp-leftover"), "{}").unwrap();
        std::fs::write(tmp.path().join("audio").join(".tmp-leftover"), "junk").unwrap();

        assert_eq!(store.count(), 1);
        assert_eq!(store.list(10).len(), 1);
        assert_eq!(
            store.audio_path(&id).unwrap().file_name().unwrap(),
            std::ffi::OsStr::new(&format!("{id}-a.ogg"))
        );
    }

    #[test]
    fn failed_records_carry_the_error_and_no_audio() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), true);
        let id = store.save(
            HistoryRecord::failed("bad.ogg", SOURCE_WATCHER, "ffmpeg exploded"),
            None,
        );
        let got = store.get(&id).unwrap();
        assert_eq!(got.status, STATUS_FAILED);
        assert_eq!(got.error.as_deref(), Some("ffmpeg exploded"));
        assert!(got.audio_file.is_none());
        assert!(store.audio_path(&id).is_none());
    }

    #[test]
    fn disabled_store_writes_nothing_but_still_yields_an_id() {
        let tmp = tempfile::tempdir().unwrap();
        let store = HistoryStore::with_enabled(tmp.path(), false);
        let id = store.save(
            HistoryRecord::done("x.ogg", SOURCE_API, &out("hi")),
            Some(b"bytes"),
        );
        assert!(!id.is_empty());
        assert!(store.get(&id).is_none());
        assert_eq!(store.count(), 0);
        assert!(!tmp.path().join("transcripts").exists());
    }
}
