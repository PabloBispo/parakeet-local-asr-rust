//! In-process folder watcher: drop an audio file into a watched directory and it
//! is transcribed, saved to the history, and announced with a desktop
//! notification. Built for the "WhatsApp voice note → Downloads → transcript on
//! the clipboard" loop.
//!
//! Design notes:
//!
//! - **Own sequential worker, not the async job queue.** Watched files are a
//!   background courtesy; they must never occupy the queue slots (or the
//!   `/metrics` counters) that API clients are polling.
//! - **Stability wait.** A create event fires when a file appears, not when it is
//!   finished. We wait for its size+mtime to hold still before reading it.
//! - **Persistent dedup.** `$RAS_HOME/watcher_state.json` remembers (size, mtime)
//!   per path, so a restart does not re-transcribe a whole Downloads folder, and
//!   the create+rename pair browsers emit only produces one transcript.

use crate::engine::EngineHandle;
use crate::history::{HistoryRecord, HistoryStore, SOURCE_WATCHER};
use crate::transcript::TranscriptOutput;
use crate::{audio, pipeline, ras};
use notify::{EventKind, RecursiveMode, Watcher};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Picked up when `--watch` is given without `--watch-ext` (WhatsApp voice notes).
const DEFAULT_EXT: &str = ".ogg";

/// Suffixes downloaders use while a file is still being written. Never candidates
/// — the real file appears when the downloader renames it.
const PARTIAL_SUFFIXES: [&str; 4] = [".part", ".crdownload", ".download", ".tmp"];

/// A file must keep the same size+mtime for this long before we read it.
const STABLE_FOR: Duration = Duration::from_millis(1_500);
const POLL_EVERY: Duration = Duration::from_millis(500);
/// Give up on a file that never settles (a partial transcript is worse than none).
const STABLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Raw fs events buffered between the notify thread and our worker.
const EVENT_QUEUE: usize = 1024;

/// Characters of transcript shown in the notification body.
const PREVIEW_CHARS: usize = 120;

/// Above this, prune dedup entries whose file no longer exists.
const MAX_STATE_ENTRIES: usize = 5_000;

// ── configuration ─────────────────────────────────────────────────────────────

/// Watcher configuration from `serve` flags, with env fallbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchConfig {
    pub dirs: Vec<PathBuf>,
    /// Normalized, lowercase, dot-prefixed (e.g. `.ogg`).
    pub exts: Vec<String>,
    /// Send desktop notifications when a watched file is transcribed.
    pub notify: bool,
}

impl WatchConfig {
    /// Parse `serve` flags: `--watch <dir>`, `--watch-ext <ext>`, `--no-notify`
    /// (all repeatable, `--flag=value` also accepted). Falls back to
    /// `ASR_WATCH_DIRS` / `ASR_WATCH_EXTS` / `ASR_NO_NOTIFY` when a flag is absent.
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut exts: Vec<String> = Vec::new();
        let mut notify = true;

        let mut i = 0;
        while i < args.len() {
            let (key, inline) = match args[i].split_once('=') {
                Some((k, v)) => (k, Some(v.to_string())),
                None => (args[i].as_str(), None),
            };
            match key {
                "--watch" => dirs.push(ras::expand_tilde(&take_value(args, &mut i, inline, key)?)),
                "--watch-ext" => exts.push(take_value(args, &mut i, inline, key)?),
                "--no-notify" => notify = false,
                other => {
                    return Err(format!(
                        "unknown option for `serve`: {other} \
                         (expected --watch, --watch-ext or --no-notify)"
                    ))
                }
            }
            i += 1;
        }

        if dirs.is_empty() {
            dirs = ras::env_list("ASR_WATCH_DIRS")
                .iter()
                .map(|d| ras::expand_tilde(d))
                .collect();
        }
        if exts.is_empty() {
            exts = ras::env_list("ASR_WATCH_EXTS");
        }
        if ras::env_flag("ASR_NO_NOTIFY") {
            notify = false;
        }

        let mut exts: Vec<String> = exts
            .iter()
            .map(|e| normalize_ext(e))
            .filter(|e| e.len() > 1)
            .collect();
        exts.sort();
        exts.dedup();
        if exts.is_empty() {
            exts.push(DEFAULT_EXT.to_string());
        }

        Ok(Self { dirs, exts, notify })
    }
}

/// Value of `--flag value` or `--flag=value`; advances the cursor for the former.
fn take_value(
    args: &[String],
    i: &mut usize,
    inline: Option<String>,
    flag: &str,
) -> Result<String, String> {
    if let Some(v) = inline {
        if v.is_empty() {
            return Err(format!("{flag} needs a value"));
        }
        return Ok(v);
    }
    *i += 1;
    args.get(*i)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

/// `OGG` / `.OGG` / `*.ogg` → `.ogg`
fn normalize_ext(raw: &str) -> String {
    let e = raw.trim().trim_start_matches('*').to_ascii_lowercase();
    if e.starts_with('.') {
        e
    } else {
        format!(".{e}")
    }
}

/// Should this path be transcribed? Extension match (case-insensitive), not a
/// dotfile, not an in-progress download.
fn is_candidate(path: &Path, exts: &[String]) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if name.starts_with('.') {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    if PARTIAL_SUFFIXES.iter().any(|s| lower.ends_with(s)) {
        return false;
    }
    exts.iter().any(|e| lower.ends_with(e.as_str()))
}

/// Creates and rename-to only. Browsers write `foo.ogg.crdownload` and rename it,
/// so the rename is often the *only* event naming the final file.
fn is_interesting(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(notify::event::ModifyKind::Name(_))
    )
}

// ── shared status (GET /v1/watcher, the UI header panel) ──────────────────────

#[derive(Clone, Serialize)]
pub struct LastFile {
    pub name: String,
    pub history_id: String,
    pub at_ms: u64,
}

#[derive(Clone, Default, Serialize)]
pub struct WatcherSnapshot {
    pub enabled: bool,
    pub dirs: Vec<String>,
    pub exts: Vec<String>,
    pub started_at_ms: u64,
    pub files_seen: u64,
    pub files_processed: u64,
    pub last_file: Option<LastFile>,
}

/// Live watcher counters. Non-poisoning mutex: a panic anywhere must not turn
/// `/v1/watcher` or `/health` into permanent failures.
pub struct WatcherStatus(Mutex<WatcherSnapshot>);

impl WatcherStatus {
    fn new(snapshot: WatcherSnapshot) -> Arc<Self> {
        Arc::new(Self(Mutex::new(snapshot)))
    }

    pub fn disabled() -> Arc<Self> {
        Self::new(WatcherSnapshot::default())
    }

    pub fn snapshot(&self) -> WatcherSnapshot {
        self.0.lock().clone()
    }

    pub fn is_enabled(&self) -> bool {
        self.0.lock().enabled
    }

    /// A settled, not-yet-seen file is about to be transcribed.
    fn note_seen(&self) {
        self.0.lock().files_seen += 1;
    }

    /// Transcription finished — `done` or `failed`, both count as processed.
    fn note_processed(&self, name: &str, history_id: String) {
        let mut g = self.0.lock();
        g.files_processed += 1;
        g.last_file = Some(LastFile {
            name: name.to_string(),
            history_id,
            at_ms: now_ms(),
        });
    }
}

// ── startup ───────────────────────────────────────────────────────────────────

/// Start watching. Returns a disabled status (server keeps running) when no
/// usable directory was configured — the watcher is an add-on, never a blocker.
pub fn start(
    cfg: WatchConfig,
    engine: EngineHandle,
    history: Arc<HistoryStore>,
    ffmpeg_bin: PathBuf,
    ras_home: &Path,
) -> Arc<WatcherStatus> {
    if cfg.dirs.is_empty() {
        return WatcherStatus::disabled();
    }

    let existing: Vec<PathBuf> = cfg
        .dirs
        .iter()
        .filter_map(|d| {
            if !d.is_dir() {
                tracing::warn!("watcher: {} is not a directory — skipping", d.display());
                return None;
            }
            // Canonicalize so dedup keys and logs are stable regardless of how
            // the path was spelled (relative, symlinked, trailing slash).
            Some(std::fs::canonicalize(d).unwrap_or_else(|_| d.clone()))
        })
        .collect();
    if existing.is_empty() {
        tracing::warn!("watcher: no watchable directory found — folder watching is off");
        return WatcherStatus::disabled();
    }

    let (tx, rx) = mpsc::channel::<PathBuf>(EVENT_QUEUE);
    let mut fs_watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        match res {
            Ok(event) => {
                if !is_interesting(&event.kind) {
                    return;
                }
                for path in event.paths {
                    // try_send, not blocking_send: stalling the fs callback would
                    // make the OS drop events wholesale. A full queue means
                    // thousands of pending files, which the log should say.
                    if let Err(e) = tx.try_send(path) {
                        tracing::warn!("watcher: dropped a filesystem event ({e})");
                    }
                }
            }
            Err(e) => tracing::warn!("watcher: filesystem event error: {e}"),
        }
    }) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("watcher: cannot initialize filesystem watcher: {e}");
            return WatcherStatus::disabled();
        }
    };

    let mut watched = Vec::new();
    for dir in &existing {
        // Non-recursive: watching ~/Downloads should not crawl every subtree.
        match fs_watcher.watch(dir, RecursiveMode::NonRecursive) {
            Ok(()) => watched.push(dir.clone()),
            Err(e) => tracing::warn!("watcher: cannot watch {}: {e}", dir.display()),
        }
    }
    if watched.is_empty() {
        tracing::warn!("watcher: no directory could be watched — folder watching is off");
        return WatcherStatus::disabled();
    }

    let status = WatcherStatus::new(WatcherSnapshot {
        enabled: true,
        dirs: watched.iter().map(|d| d.display().to_string()).collect(),
        exts: cfg.exts.clone(),
        started_at_ms: now_ms(),
        ..Default::default()
    });

    tracing::info!(
        "watcher: watching {} for {} (notifications: {})",
        watched
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(", "),
        cfg.exts.join(" "),
        if cfg.notify { "on" } else { "off" }
    );

    let worker = Worker {
        exts: cfg.exts,
        notify: cfg.notify,
        engine,
        history,
        ffmpeg_bin,
        state_path: ras::watcher_state_path(ras_home),
        status: status.clone(),
    };
    // `fs_watcher` moves into the task: dropping it would stop the watch.
    tokio::spawn(worker.run(rx, fs_watcher));

    status
}

// ── worker ────────────────────────────────────────────────────────────────────

struct Worker {
    exts: Vec<String>,
    notify: bool,
    engine: EngineHandle,
    history: Arc<HistoryStore>,
    ffmpeg_bin: PathBuf,
    state_path: PathBuf,
    status: Arc<WatcherStatus>,
}

impl Worker {
    /// Drain fs events one at a time. Sequential on purpose: inference is
    /// serialized anyway, and concurrency here would only starve API requests.
    async fn run(self, mut rx: mpsc::Receiver<PathBuf>, _fs_watcher: notify::RecommendedWatcher) {
        let mut state = load_state(&self.state_path);
        // Guards the create+rename pair a single event batch can contain. The loop
        // is sequential, so this is a batch-level dedup, not cross-thread locking.
        let mut in_flight: HashSet<PathBuf> = HashSet::new();

        while let Some(path) = rx.recv().await {
            if !is_candidate(&path, &self.exts) {
                continue;
            }
            if !in_flight.insert(path.clone()) {
                continue;
            }
            self.handle(&path, &mut state).await;
            in_flight.remove(&path);
        }
        tracing::info!("watcher: event channel closed — folder watching stopped");
    }

    async fn handle(&self, path: &Path, state: &mut StateMap) {
        let Some(mark) = wait_until_stable(path).await else {
            return;
        };
        let key = path.to_string_lossy().to_string();
        if state.get(&key) == Some(&mark) {
            tracing::debug!("watcher: {} already transcribed — skipping", path.display());
            return;
        }

        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "audio".to_string());
        // Counted here rather than on the raw event: this is the first point where
        // we know the file is real, settled, and not a duplicate.
        self.status.note_seen();
        tracing::info!("watcher: transcribing {}", path.display());

        let id = match self.transcribe(path).await {
            Ok((bytes, out)) => {
                let preview = preview_of(&out.text);
                let id = self.history.save(
                    HistoryRecord::done(&name, SOURCE_WATCHER, &out),
                    Some(&bytes),
                );
                tracing::info!("watcher: {name} -> {id} ({:.1}s audio)", out.duration);
                if self.notify {
                    let txt = self.history.txt_path(&id);
                    dispatch(&format!("Transcrito: {name}"), &preview, Some(&txt));
                }
                id
            }
            Err(e) => {
                tracing::warn!("watcher: {} failed: {e:#}", path.display());
                let reason = format!("{e:#}");
                // Failures are persisted (unlike API failures) because nobody is
                // watching an HTTP response here — the UI is the only feedback.
                let id = self.history.save(
                    HistoryRecord::failed(&name, SOURCE_WATCHER, reason.clone()),
                    None,
                );
                if self.notify {
                    dispatch(
                        &format!("Falha na transcrição: {name}"),
                        &preview_of(&reason),
                        None,
                    );
                }
                id
            }
        };

        self.status.note_processed(&name, id);
        // Recorded even on failure: a file that cannot be decoded would otherwise
        // be retried on every restart.
        state.insert(key, mark);
        save_state(&self.state_path, state);
    }

    async fn transcribe(&self, path: &Path) -> anyhow::Result<(Vec<u8>, TranscriptOutput)> {
        let bytes = tokio::fs::read(path).await?;
        let samples = audio::decode_to_pcm(&self.ffmpeg_bin, &bytes).await?;
        let out = pipeline::transcribe_samples(&self.engine, samples, pipeline::CHUNK_SECS).await?;
        Ok((bytes, out))
    }
}

/// Wait for `path` to stop changing; `None` if it vanished, is empty, or never
/// settled within `STABLE_TIMEOUT`.
async fn wait_until_stable(path: &Path) -> Option<FileMark> {
    let start = Instant::now();
    let mut last: Option<FileMark> = None;
    let mut unchanged_since = Instant::now();

    loop {
        match tokio::fs::metadata(path).await {
            Ok(meta) if meta.is_file() => {
                let mark = FileMark {
                    size: meta.len(),
                    mtime_ms: mtime_ms(&meta),
                };
                if last.as_ref() == Some(&mark) {
                    if unchanged_since.elapsed() >= STABLE_FOR {
                        if mark.size == 0 {
                            tracing::debug!("watcher: {} is empty — skipping", path.display());
                            return None;
                        }
                        return Some(mark);
                    }
                } else {
                    last = Some(mark);
                    unchanged_since = Instant::now();
                }
            }
            Ok(_) => return None, // replaced by a directory/symlink
            Err(_) => {
                tracing::debug!(
                    "watcher: {} disappeared before it settled",
                    path.display()
                );
                return None;
            }
        }

        if start.elapsed() >= STABLE_TIMEOUT {
            tracing::warn!(
                "watcher: {} is still changing after {}s — skipping it (a partial \
                 transcript would be worse than none)",
                path.display(),
                STABLE_TIMEOUT.as_secs()
            );
            return None;
        }
        tokio::time::sleep(POLL_EVERY).await;
    }
}

// ── persistent dedup state ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FileMark {
    size: u64,
    mtime_ms: u64,
}

type StateMap = HashMap<String, FileMark>;

fn load_state(path: &Path) -> StateMap {
    match std::fs::read_to_string(path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
            tracing::warn!(
                "watcher: {} is unreadable ({e}) — starting with an empty dedup state",
                path.display()
            );
            StateMap::new()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => StateMap::new(),
        Err(e) => {
            tracing::warn!("watcher: cannot read {}: {e}", path.display());
            StateMap::new()
        }
    }
}

/// Rewrite the dedup state. Small file, written once per processed file — plain
/// blocking IO is cheaper than moving it onto the blocking pool.
fn save_state(path: &Path, state: &mut StateMap) {
    if state.len() > MAX_STATE_ENTRIES {
        state.retain(|p, _| Path::new(p).exists());
    }
    match serde_json::to_string_pretty(state) {
        Ok(json) => {
            // Atomic: a crash mid-write must not leave an unparseable state file
            // (which would make the watcher re-transcribe everything).
            if let Err(e) = ras::write_atomic(path, json.as_bytes()) {
                tracing::warn!("watcher: cannot write {}: {e}", path.display());
            }
        }
        Err(e) => tracing::warn!("watcher: cannot serialize dedup state: {e}"),
    }
}

fn mtime_ms(meta: &std::fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn preview_of(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "(no speech detected)".to_string();
    }
    let mut out: String = trimmed.chars().take(PREVIEW_CHARS).collect();
    if trimmed.chars().count() > PREVIEW_CHARS {
        out.push('…');
    }
    out
}

// ── desktop notifications ─────────────────────────────────────────────────────
//
// All best-effort and all via subprocess: a native notification/clipboard crate
// would add C dependencies to four release targets for a cosmetic feature.
// Failures are logged at debug level and never surface to the user.

fn dispatch(title: &str, body: &str, copy_from: Option<&Path>) {
    #[cfg(target_os = "macos")]
    notify_macos(title, body, copy_from);

    #[cfg(target_os = "linux")]
    notify_linux(title, body, copy_from);

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = copy_from;
        tracing::info!("{title} — {body}");
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
mod notify_shell {
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    /// Extra directories checked after `PATH`: a server started by launchd/systemd
    /// often has a minimal `PATH` that misses the package manager's bindir.
    const EXTRA_DIRS: [&str; 3] = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"];

    pub fn which(bin: &str) -> Option<PathBuf> {
        let from_path = std::env::var_os("PATH")
            .map(|p| std::env::split_paths(&p).collect::<Vec<_>>())
            .unwrap_or_default();
        from_path
            .into_iter()
            .chain(EXTRA_DIRS.iter().map(PathBuf::from))
            .map(|dir| dir.join(bin))
            .find(|candidate| candidate.is_file())
    }

    /// Run to completion, discarding output. `true` on a successful exit.
    pub fn run(cmd: &mut Command) -> bool {
        match cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(s) if s.success() => true,
            Ok(s) => {
                tracing::debug!("watcher: notification helper exited with {s}");
                false
            }
            Err(e) => {
                tracing::debug!("watcher: notification helper failed to run: {e}");
                false
            }
        }
    }

    /// Feed `data` to `bin` on stdin (clipboard helpers read stdin).
    pub fn pipe_stdin(bin: &str, args: &[&str], data: &[u8]) {
        let child = Command::new(bin)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match child {
            Ok(mut c) => {
                if let Some(mut stdin) = c.stdin.take() {
                    let _ = stdin.write_all(data);
                }
                let _ = c.wait();
            }
            Err(e) => tracing::debug!("watcher: cannot run {bin}: {e}"),
        }
    }

    pub fn read_text(path: &Path) -> Option<Vec<u8>> {
        std::fs::read(path).ok()
    }

    /// Single-quote a path for a `sh -c` style command line.
    pub fn shell_quote(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', r"'\''"))
    }
}

#[cfg(target_os = "macos")]
fn notify_macos(title: &str, body: &str, copy_from: Option<&Path>) {
    use notify_shell::*;
    use std::process::Command;

    // Preferred: terminal-notifier gives a *clickable* notification, and the click
    // copies the full transcript — the whole point of the watcher workflow.
    if let Some(bin) = which("terminal-notifier") {
        let mut cmd = Command::new(bin);
        cmd.args(["-title", title, "-message", body]);
        if let Some(path) = copy_from {
            cmd.args(["-execute", &format!("cat {} | pbcopy", shell_quote(path))]);
        }
        if run(&mut cmd) {
            return;
        }
    }

    // Fallback: osascript cannot attach a click action, so copy eagerly instead.
    if let Some(path) = copy_from {
        if let Some(text) = read_text(path) {
            pipe_stdin("pbcopy", &[], &text);
        }
    }
    let script = format!(
        "display notification \"{}\" with title \"{}\"",
        applescript_escape(body),
        applescript_escape(title)
    );
    run(Command::new("osascript").args(["-e", &script]));
}

#[cfg(target_os = "macos")]
fn applescript_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "linux")]
fn notify_linux(title: &str, body: &str, copy_from: Option<&Path>) {
    use notify_shell::*;
    use std::process::Command;

    if let Some(path) = copy_from {
        if which("xclip").is_some() {
            if let Some(text) = read_text(path) {
                pipe_stdin("xclip", &["-selection", "clipboard"], &text);
            }
        }
    }
    if which("notify-send").is_some() {
        run(Command::new("notify-send").args([title, body]));
    } else {
        tracing::info!("{title} — {body}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exts(list: &[&str]) -> Vec<String> {
        list.iter().map(|e| e.to_string()).collect()
    }

    #[test]
    fn extension_filter_is_case_insensitive() {
        let e = exts(&[".ogg", ".m4a"]);
        assert!(is_candidate(Path::new("/w/note.ogg"), &e));
        assert!(is_candidate(Path::new("/w/NOTE.OGG"), &e));
        assert!(is_candidate(Path::new("/w/audio message.m4a"), &e));
        assert!(!is_candidate(Path::new("/w/report.pdf"), &e));
        assert!(!is_candidate(Path::new("/w/noext"), &e));
        // Extension-only name is a dotfile, not a match.
        assert!(!is_candidate(Path::new("/w/.ogg"), &e));
    }

    #[test]
    fn partial_downloads_and_dotfiles_are_ignored() {
        let e = exts(&[".ogg"]);
        assert!(!is_candidate(Path::new("/w/note.ogg.part"), &e));
        assert!(!is_candidate(Path::new("/w/note.ogg.crdownload"), &e));
        assert!(!is_candidate(Path::new("/w/note.ogg.download"), &e));
        assert!(!is_candidate(Path::new("/w/note.ogg.tmp"), &e));
        assert!(!is_candidate(Path::new("/w/.hidden.ogg"), &e));
        // ...and the finished rename target is picked up.
        assert!(is_candidate(Path::new("/w/note.ogg"), &e));
    }

    #[test]
    fn only_create_and_rename_events_are_interesting() {
        use notify::event::{CreateKind, DataChange, ModifyKind, RemoveKind, RenameMode};
        assert!(is_interesting(&EventKind::Create(CreateKind::File)));
        assert!(is_interesting(&EventKind::Modify(ModifyKind::Name(
            RenameMode::To
        ))));
        assert!(is_interesting(&EventKind::Modify(ModifyKind::Name(
            RenameMode::Any
        ))));
        assert!(!is_interesting(&EventKind::Modify(ModifyKind::Data(
            DataChange::Content
        ))));
        assert!(!is_interesting(&EventKind::Remove(RemoveKind::File)));
        assert!(!is_interesting(&EventKind::Access(
            notify::event::AccessKind::Read
        )));
    }

    #[test]
    fn extensions_are_normalized_and_deduped() {
        assert_eq!(normalize_ext("ogg"), ".ogg");
        assert_eq!(normalize_ext(".OGG"), ".ogg");
        assert_eq!(normalize_ext("*.M4a"), ".m4a");
        assert_eq!(normalize_ext("  wav  "), ".wav");

        let args: Vec<String> = ["--watch", "/tmp", "--watch-ext", "OGG", "--watch-ext", ".ogg"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let cfg = WatchConfig::parse(&args).unwrap();
        assert_eq!(cfg.dirs, vec![PathBuf::from("/tmp")]);
        assert_eq!(cfg.exts, exts(&[".ogg"]));
    }

    #[test]
    fn flags_parse_including_inline_values_and_repeats() {
        let args: Vec<String> = [
            "--watch",
            "/tmp/a",
            "--watch=/tmp/b",
            "--watch-ext=m4a",
            "--no-notify",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let cfg = WatchConfig::parse(&args).unwrap();
        assert_eq!(
            cfg.dirs,
            vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]
        );
        assert_eq!(cfg.exts, exts(&[".m4a"]));
        assert!(!cfg.notify);
    }

    #[test]
    fn bad_flags_are_rejected() {
        let missing: Vec<String> = vec!["--watch".to_string()];
        assert!(WatchConfig::parse(&missing).is_err());
        let unknown: Vec<String> = vec!["--nope".to_string()];
        assert!(WatchConfig::parse(&unknown).is_err());
    }

    #[test]
    fn dedup_state_roundtrips_and_survives_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("watcher_state.json");

        assert!(load_state(&path).is_empty()); // missing file is not an error

        let mut state = StateMap::new();
        state.insert(
            "/w/note.ogg".to_string(),
            FileMark {
                size: 42,
                mtime_ms: 1_700_000_000_000,
            },
        );
        save_state(&path, &mut state);
        assert_eq!(load_state(&path), state);

        std::fs::write(&path, "{ truncated").unwrap();
        assert!(load_state(&path).is_empty());
    }

    #[test]
    fn preview_truncates_and_handles_silence() {
        assert_eq!(preview_of("  hello  "), "hello");
        assert_eq!(preview_of("   "), "(no speech detected)");
        let long = "á".repeat(PREVIEW_CHARS + 50);
        let p = preview_of(&long);
        assert_eq!(p.chars().count(), PREVIEW_CHARS + 1);
        assert!(p.ends_with('…'));
    }

    #[test]
    fn status_counters_track_seen_and_processed() {
        let status = WatcherStatus::new(WatcherSnapshot {
            enabled: true,
            ..Default::default()
        });
        assert!(status.is_enabled());
        status.note_seen();
        let mid = status.snapshot();
        assert_eq!((mid.files_seen, mid.files_processed), (1, 0));
        assert!(mid.last_file.is_none());

        status.note_processed("note.ogg", "abc-123".to_string());
        let done = status.snapshot();
        assert_eq!((done.files_seen, done.files_processed), (1, 1));
        let last = done.last_file.unwrap();
        assert_eq!(last.name, "note.ogg");
        assert_eq!(last.history_id, "abc-123");
        assert!(last.at_ms > 0);

        assert!(!WatcherStatus::disabled().is_enabled());
    }
}
