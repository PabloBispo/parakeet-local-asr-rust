//! In-process folder watcher: drop an audio file into a watched directory and it
//! is transcribed, saved to the history, and announced with a desktop
//! notification. Built for the "WhatsApp voice note → Downloads → transcript on
//! the clipboard" loop.
//!
//! Design notes:
//!
//! - **Two tasks, not one.** A *control* task owns the `notify` watcher and
//!   answers config commands (add/remove folder, change extensions); a
//!   *transcription* task drains filesystem events one at a time. They are split
//!   because transcribing a 30-minute recording takes minutes, and the UI's
//!   "add folder" button must answer immediately — a single task would queue the
//!   command behind the current transcription.
//! - **Own sequential worker, not the async job queue.** Watched files are a
//!   background courtesy; they must never occupy the queue slots (or the
//!   `/metrics` counters) that API clients are polling.
//! - **Stability wait.** A create event fires when a file appears, not when it is
//!   finished. We wait for its size+mtime to hold still before reading it.
//! - **Persistent dedup.** `$RAS_HOME/watcher_state.json` remembers (size, mtime)
//!   per path, so a restart does not re-transcribe a whole Downloads folder, and
//!   the create+rename pair browsers emit only produces one transcript.
//! - **Persistent config.** `$RAS_HOME/watcher_config.json` remembers the folders
//!   and extensions, so what the user set in the UI survives a restart.

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
use tokio::sync::{mpsc, oneshot};

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

/// Config commands buffered between HTTP handlers and the control task. Each one
/// is applied in microseconds, so this only needs to absorb a burst of clicks.
const COMMAND_QUEUE: usize = 32;

/// Characters of transcript shown in the notification body.
const PREVIEW_CHARS: usize = 120;

/// Above this, prune dedup entries whose file no longer exists.
const MAX_STATE_ENTRIES: usize = 5_000;

// ── configuration ─────────────────────────────────────────────────────────────

/// Watcher configuration from `serve` flags, with env fallbacks. Merged with the
/// persisted config at startup (see [`merge_startup`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchConfig {
    pub dirs: Vec<PathBuf>,
    /// Normalized, lowercase, dot-prefixed (e.g. `.ogg`). **Empty means "the user
    /// did not say"** — the default is applied by the startup merge, which has to
    /// tell "unspecified" apart from an explicit choice.
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

        // Left empty when nothing was given: the startup merge decides between the
        // persisted list and the default.
        let exts = if exts.is_empty() {
            Vec::new()
        } else {
            normalize_exts(&exts)?
        };

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

// ── runtime validation (shared by the CLI parser and PUT/POST/DELETE handlers) ──
//
// Every message here is user-facing: it is returned verbatim as
// `{"error": "..."}` and shown in the UI, hence pt-BR and no technical detail.

const ERR_EMPTY_PATH: &str = "informe o caminho da pasta";
const ERR_NOT_FOUND: &str = "pasta não encontrada";
const ERR_NOT_A_DIR: &str = "caminho não é uma pasta";
const ERR_NOT_WATCHED: &str = "pasta não monitorada";
const ERR_NO_EXTS: &str = "informe ao menos uma extensão";

/// Normalize a user-supplied extension list: lowercase, dot-prefixed, deduped with
/// the caller's order preserved (the UI shows the list back, so shuffling it would
/// look like the server ignored the input).
///
/// Rejects rather than silently drops: a typo that quietly disappears would leave
/// the user watching for an extension they never asked for. A valid suffix is a
/// single non-empty token — no whitespace, no path separators, not just dots.
fn normalize_exts(raw: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    for item in raw {
        let ext = normalize_ext(item);
        let body = &ext[1..]; // normalize_ext always returns a leading '.'
        let invalid = body.is_empty()
            || body.chars().all(|c| c == '.')
            || body
                .chars()
                .any(|c| c.is_whitespace() || c == '/' || c == '\\');
        if invalid {
            return Err(format!("extensão inválida: {}", item.trim()));
        }
        if !out.contains(&ext) {
            out.push(ext);
        }
    }
    if out.is_empty() {
        return Err(ERR_NO_EXTS.to_string());
    }
    Ok(out)
}

/// A user-supplied folder, ready to watch: `~` expanded, existing, a directory,
/// canonicalized.
///
/// Canonicalizing matters beyond tidiness — the same folder spelled two ways
/// (`~/Downloads`, `/Users/me/Downloads`, a symlink) must collapse to one entry,
/// or `unwatch` and the duplicate check would both miss.
fn validate_dir(raw: &str) -> Result<PathBuf, String> {
    if raw.trim().is_empty() {
        return Err(ERR_EMPTY_PATH.to_string());
    }
    check_dir(&ras::expand_tilde(raw.trim()))
}

fn check_dir(path: &Path) -> Result<PathBuf, String> {
    let meta = std::fs::metadata(path).map_err(|_| ERR_NOT_FOUND.to_string())?;
    if !meta.is_dir() {
        return Err(ERR_NOT_A_DIR.to_string());
    }
    Ok(std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
}

/// Find `wanted` in the watched list. Matches the exact stored path first (that is
/// the string `GET /v1/watcher` shows, so it is what the UI sends back), then falls
/// back to comparing canonical paths for hand-written input. Both are needed: a
/// folder that has since been deleted cannot be canonicalized at all, and only the
/// exact-string match can still remove it.
fn match_dir(dirs: &[PathBuf], wanted: &Path) -> Option<usize> {
    if let Some(i) = dirs.iter().position(|d| d == wanted) {
        return Some(i);
    }
    let canonical = std::fs::canonicalize(wanted).ok()?;
    dirs.iter().position(|d| {
        *d == canonical
            || std::fs::canonicalize(d)
                .map(|c| c == canonical)
                .unwrap_or(false)
    })
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

// ── persisted config ($RAS_HOME/watcher_config.json) ──────────────────────────

/// What the user configured, as written to disk:
/// `{"dirs":["/Users/me/Downloads"],"exts":[".ogg"]}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StoredConfig {
    #[serde(default)]
    dirs: Vec<String>,
    #[serde(default)]
    exts: Vec<String>,
}

/// Read the persisted config. Anything unreadable or unparseable starts empty with
/// a warning: losing the folder list is annoying, refusing to boot is worse.
fn load_config(path: &Path) -> StoredConfig {
    match std::fs::read_to_string(path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|e| {
            tracing::warn!(
                "watcher: {} is unreadable ({e}) — starting with an empty configuration",
                path.display()
            );
            StoredConfig::default()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => StoredConfig::default(),
        Err(e) => {
            tracing::warn!("watcher: cannot read {}: {e}", path.display());
            StoredConfig::default()
        }
    }
}

/// Persist the config. Best-effort on purpose: the in-memory state has already
/// changed and the folder is already being watched, so a read-only `$RAS_HOME` must
/// degrade to "works until restart", not to a failed request.
fn save_config(path: &Path, dirs: &[PathBuf], exts: &[String]) {
    let stored = StoredConfig {
        dirs: dirs.iter().map(|d| d.display().to_string()).collect(),
        exts: exts.to_vec(),
    };
    match serde_json::to_string_pretty(&stored) {
        Ok(json) => {
            if let Err(e) = ras::write_atomic(path, json.as_bytes()) {
                tracing::warn!(
                    "watcher: cannot write {} ({e}) — this change is lost on restart",
                    path.display()
                );
            }
        }
        Err(e) => tracing::warn!("watcher: cannot serialize the configuration: {e}"),
    }
}

/// Merge the persisted config with what the CLI/env asked for, at startup.
///
/// - **dirs: union, persisted first.** `--watch` is additive so that starting the
///   server with a flag once does not silently drop the folders the user added from
///   the UI, and dropping the flag later does not stop watching them (that is what
///   `DELETE /v1/watcher/dirs` is for). Persisted folders that no longer exist are
///   dropped with a warning — the caller rewrites the file, so they stop coming
///   back.
/// - **exts: CLI/env wins when given, else persisted, else `.ogg`.** Extensions are
///   one setting rather than a set of independent items: an explicit
///   `--watch-ext m4a` must not leave a stale `.ogg` behind.
fn merge_startup(stored: &StoredConfig, cfg: &WatchConfig) -> (Vec<PathBuf>, Vec<String>) {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let candidates = stored
        .dirs
        .iter()
        .map(|d| (ras::expand_tilde(d), true))
        .chain(cfg.dirs.iter().map(|d| (d.clone(), false)));
    for (path, from_disk) in candidates {
        match check_dir(&path) {
            Ok(dir) => {
                if !dirs.contains(&dir) {
                    dirs.push(dir);
                }
            }
            Err(reason) => tracing::warn!(
                "watcher: skipping {} ({reason}){}",
                path.display(),
                if from_disk {
                    " — removing it from the saved configuration"
                } else {
                    ""
                }
            ),
        }
    }

    let exts = if !cfg.exts.is_empty() {
        cfg.exts.clone()
    } else if !stored.exts.is_empty() {
        normalize_exts(&stored.exts).unwrap_or_else(|e| {
            tracing::warn!("watcher: saved extensions are invalid ({e}) — using {DEFAULT_EXT}");
            vec![DEFAULT_EXT.to_string()]
        })
    } else {
        vec![DEFAULT_EXT.to_string()]
    };

    (dirs, exts)
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
    /// At least one directory is currently being watched. Folders can be added and
    /// removed at runtime, so this flips during the process' life — it is not a
    /// "was the watcher configured at startup" flag.
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

    pub fn snapshot(&self) -> WatcherSnapshot {
        self.0.lock().clone()
    }

    pub fn is_enabled(&self) -> bool {
        self.0.lock().enabled
    }

    /// Extensions as of right now. The transcription task re-reads them per event
    /// instead of caching them, so `PUT /v1/watcher/exts` takes effect immediately
    /// (and without a second source of truth to keep in sync).
    fn exts(&self) -> Vec<String> {
        self.0.lock().exts.clone()
    }

    /// Publish the watched directories. `enabled` is derived here — the two can
    /// never disagree.
    fn set_dirs(&self, dirs: &[PathBuf]) {
        let mut g = self.0.lock();
        g.dirs = dirs.iter().map(|d| d.display().to_string()).collect();
        g.enabled = !g.dirs.is_empty();
    }

    fn set_exts(&self, exts: &[String]) {
        self.0.lock().exts = exts.to_vec();
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

// ── runtime configuration (POST/DELETE /v1/watcher/dirs, PUT /v1/watcher/exts) ─

/// A config change, sent to the control task. Each carries its own reply channel:
/// the caller is an HTTP handler holding a client connection open, so it needs the
/// answer to *this* command, not a broadcast.
enum WatchCommand {
    AddDir {
        /// Already validated and canonicalized by [`WatcherHandle::add_dir`].
        path: PathBuf,
        reply: oneshot::Sender<Result<(), String>>,
    },
    RemoveDir {
        /// Tilde-expanded only: the folder may have been deleted since it was added,
        /// and it must still be removable.
        path: PathBuf,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SetExts {
        /// Already normalized by [`WatcherHandle::set_exts`].
        exts: Vec<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// Returned to a caller when there is no control task at all (the platform's
/// filesystem watcher could not be created). The server still serves everything
/// else, so this is a per-request error rather than a startup failure.
const ERR_UNAVAILABLE: &str = "monitoramento de pastas indisponível neste servidor";

/// Handle on the running watcher: read its status, or change what it watches.
///
/// Cloneable because it lives in `AppState`. Mutations are `async` but never wait
/// on transcription — they are served by the control task, which does nothing but
/// apply config (see the module docs).
#[derive(Clone)]
pub struct WatcherHandle {
    status: Arc<WatcherStatus>,
    /// `None` = no control task: mutations fail with [`ERR_UNAVAILABLE`].
    commands: Option<mpsc::Sender<WatchCommand>>,
}

impl WatcherHandle {
    pub fn snapshot(&self) -> WatcherSnapshot {
        self.status.snapshot()
    }

    pub fn is_enabled(&self) -> bool {
        self.status.is_enabled()
    }

    /// Start watching `raw` (a path, `~` allowed). Idempotent: adding a folder that
    /// is already watched succeeds and changes nothing.
    pub async fn add_dir(&self, raw: &str) -> Result<WatcherSnapshot, String> {
        let path = validate_dir(raw)?;
        self.send(move |reply| WatchCommand::AddDir { path, reply })
            .await
    }

    /// Stop watching `raw`, given either the exact string from the snapshot or any
    /// spelling of the same path.
    pub async fn remove_dir(&self, raw: &str) -> Result<WatcherSnapshot, String> {
        if raw.trim().is_empty() {
            return Err(ERR_EMPTY_PATH.to_string());
        }
        let path = ras::expand_tilde(raw.trim());
        self.send(move |reply| WatchCommand::RemoveDir { path, reply })
            .await
    }

    /// Replace the watched extensions (not additive — this is the whole list).
    pub async fn set_exts(&self, exts: Vec<String>) -> Result<WatcherSnapshot, String> {
        let exts = normalize_exts(&exts)?;
        self.send(move |reply| WatchCommand::SetExts { exts, reply })
            .await
    }

    /// Round-trip one command and return the resulting snapshot, so every mutating
    /// endpoint answers with exactly the body of `GET /v1/watcher`.
    async fn send<F>(&self, build: F) -> Result<WatcherSnapshot, String>
    where
        F: FnOnce(oneshot::Sender<Result<(), String>>) -> WatchCommand,
    {
        let commands = self
            .commands
            .as_ref()
            .ok_or_else(|| ERR_UNAVAILABLE.to_string())?;
        let (reply_tx, reply_rx) = oneshot::channel();
        commands
            .send(build(reply_tx))
            .await
            .map_err(|_| ERR_UNAVAILABLE.to_string())?;
        // The control task sets the status before replying, so reading it here is
        // guaranteed to show this command's effect.
        reply_rx.await.map_err(|_| ERR_UNAVAILABLE.to_string())??;
        Ok(self.status.snapshot())
    }
}

// ── control task ──────────────────────────────────────────────────────────────

/// Owns the platform watcher and the authoritative directory list. Single-threaded
/// by construction, so no locking is needed around `watch`/`unwatch`.
struct Control {
    fs_watcher: notify::RecommendedWatcher,
    dirs: Vec<PathBuf>,
    exts: Vec<String>,
    status: Arc<WatcherStatus>,
    config_path: PathBuf,
}

impl Control {
    /// Every arm is a handful of syscalls, so commands never queue behind real work
    /// — that is the whole reason this is not the transcription task.
    async fn run(mut self, mut rx: mpsc::Receiver<WatchCommand>) {
        while let Some(cmd) = rx.recv().await {
            let (result, reply) = match cmd {
                WatchCommand::AddDir { path, reply } => (self.add_dir(path), reply),
                WatchCommand::RemoveDir { path, reply } => (self.remove_dir(&path), reply),
                WatchCommand::SetExts { exts, reply } => (self.set_exts(exts), reply),
            };
            // Send error = the caller's request was cancelled; nothing to undo.
            let _ = reply.send(result);
        }
        tracing::debug!("watcher: control channel closed");
    }

    fn add_dir(&mut self, dir: PathBuf) -> Result<(), String> {
        if self.dirs.contains(&dir) {
            return Ok(());
        }
        // Non-recursive: watching ~/Downloads should not crawl every subtree.
        self.fs_watcher
            .watch(&dir, RecursiveMode::NonRecursive)
            .map_err(|e| format!("não foi possível monitorar a pasta: {e}"))?;
        tracing::info!("watcher: now watching {}", dir.display());
        self.dirs.push(dir);
        self.publish();
        Ok(())
    }

    fn remove_dir(&mut self, wanted: &Path) -> Result<(), String> {
        let idx = match_dir(&self.dirs, wanted).ok_or_else(|| ERR_NOT_WATCHED.to_string())?;
        let dir = self.dirs.remove(idx);
        if let Err(e) = self.fs_watcher.unwatch(&dir) {
            // The OS handle can already be gone (deleted folder). The user's intent
            // — stop transcribing files from here — is satisfied either way.
            tracing::warn!("watcher: unwatch {} failed: {e}", dir.display());
        }
        tracing::info!("watcher: stopped watching {}", dir.display());
        self.publish();
        Ok(())
    }

    fn set_exts(&mut self, exts: Vec<String>) -> Result<(), String> {
        tracing::info!("watcher: extensions set to {}", exts.join(" "));
        self.exts = exts;
        self.publish();
        Ok(())
    }

    /// Make the change visible (status) and durable (config file), in that order:
    /// the HTTP reply must never report a state the snapshot does not have yet.
    fn publish(&self) {
        self.status.set_dirs(&self.dirs);
        self.status.set_exts(&self.exts);
        save_config(&self.config_path, &self.dirs, &self.exts);
    }
}

// ── startup ───────────────────────────────────────────────────────────────────

/// Start the watcher machinery. Always starts it — even with zero directories —
/// because folders are added at runtime; an empty config just means `enabled:
/// false` until the first `POST /v1/watcher/dirs`.
///
/// Never fails: the watcher is an add-on, never a blocker. A platform watcher that
/// cannot be created yields a handle that reports `enabled: false` and rejects
/// mutations with a clear message.
pub fn start(
    cfg: WatchConfig,
    engine: EngineHandle,
    history: Arc<HistoryStore>,
    ffmpeg_bin: PathBuf,
    ras_home: &Path,
) -> WatcherHandle {
    let config_path = ras::watcher_config_path(ras_home);
    let (dirs, exts) = merge_startup(&load_config(&config_path), &cfg);

    let (files_tx, files_rx) = mpsc::channel::<PathBuf>(EVENT_QUEUE);
    let mut fs_watcher = match new_fs_watcher(files_tx) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("watcher: cannot initialize filesystem watcher: {e}");
            // Deliberately not persisting here: rewriting the config with an empty
            // dirs list would erase the user's folders over a transient platform
            // failure.
            return WatcherHandle {
                status: WatcherStatus::new(WatcherSnapshot {
                    exts,
                    started_at_ms: now_ms(),
                    ..Default::default()
                }),
                commands: None,
            };
        }
    };

    let mut watched = Vec::new();
    for dir in &dirs {
        match fs_watcher.watch(dir, RecursiveMode::NonRecursive) {
            Ok(()) => watched.push(dir.clone()),
            Err(e) => tracing::warn!("watcher: cannot watch {}: {e}", dir.display()),
        }
    }

    let status = WatcherStatus::new(WatcherSnapshot {
        enabled: !watched.is_empty(),
        dirs: watched.iter().map(|d| d.display().to_string()).collect(),
        exts: exts.clone(),
        started_at_ms: now_ms(),
        ..Default::default()
    });

    // Write the merged result back so `--watch <dir>` given once sticks, and so
    // folders that no longer exist stop being retried on every boot.
    save_config(&config_path, &watched, &exts);

    if watched.is_empty() {
        tracing::info!(
            "watcher: no folder configured — add one from the UI or with \
             POST /v1/watcher/dirs"
        );
    } else {
        tracing::info!(
            "watcher: watching {} for {} (notifications: {})",
            watched
                .iter()
                .map(|d| d.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            exts.join(" "),
            if cfg.notify { "on" } else { "off" }
        );
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<WatchCommand>(COMMAND_QUEUE);
    // `fs_watcher` moves into the control task: dropping it would stop the watch.
    tokio::spawn(
        Control {
            fs_watcher,
            dirs: watched,
            exts,
            status: status.clone(),
            config_path,
        }
        .run(cmd_rx),
    );
    tokio::spawn(
        Worker {
            notify: cfg.notify,
            engine,
            history,
            ffmpeg_bin,
            state_path: ras::watcher_state_path(ras_home),
            status: status.clone(),
        }
        .run(files_rx),
    );

    WatcherHandle {
        status,
        commands: Some(cmd_tx),
    }
}

fn new_fs_watcher(tx: mpsc::Sender<PathBuf>) -> notify::Result<notify::RecommendedWatcher> {
    notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
        Ok(event) => {
            if !is_interesting(&event.kind) {
                return;
            }
            for path in event.paths {
                // try_send, not blocking_send: stalling the fs callback would make
                // the OS drop events wholesale. A full queue means thousands of
                // pending files, which the log should say.
                if let Err(e) = tx.try_send(path) {
                    tracing::warn!("watcher: dropped a filesystem event ({e})");
                }
            }
        }
        Err(e) => tracing::warn!("watcher: filesystem event error: {e}"),
    })
}

// ── transcription task ────────────────────────────────────────────────────────

struct Worker {
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
    async fn run(self, mut rx: mpsc::Receiver<PathBuf>) {
        let mut state = load_state(&self.state_path);
        // Guards the create+rename pair a single event batch can contain. The loop
        // is sequential, so this is a batch-level dedup, not cross-thread locking.
        let mut in_flight: HashSet<PathBuf> = HashSet::new();

        while let Some(path) = rx.recv().await {
            // Read the extensions per event, not once: they change at runtime. A
            // file already queued when the list changed is judged by the new list,
            // which is what the user just asked for.
            if !is_candidate(&path, &self.status.exts()) {
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
            // Without a UTF-8 locale pbcopy decodes stdin as Mac Roman; harmless
            // for byte-transparent tools like xclip.
            .env("LC_CTYPE", "UTF-8")
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
            // The click action runs in terminal-notifier's launchd context, which
            // has no locale set — pbcopy then decodes stdin as Mac Roman and
            // mangles UTF-8 ("ã" becomes "√£"). Force UTF-8 on pbcopy itself.
            cmd.args([
                "-execute",
                &format!("cat {} | LC_CTYPE=UTF-8 pbcopy", shell_quote(path)),
            ]);
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

        // No --watch-ext: left empty so the startup merge can tell "unspecified"
        // apart from an explicit choice.
        let bare: Vec<String> = vec!["--watch".into(), "/tmp".into()];
        assert!(WatchConfig::parse(&bare).unwrap().exts.is_empty());
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
    }

    #[test]
    fn enabled_follows_the_watched_directory_list() {
        let status = WatcherStatus::new(WatcherSnapshot::default());
        assert!(!status.is_enabled());

        status.set_dirs(&[PathBuf::from("/w/a")]);
        assert!(status.is_enabled());
        assert_eq!(status.snapshot().dirs, vec!["/w/a".to_string()]);

        status.set_exts(&exts(&[".m4a"]));
        assert_eq!(status.exts(), exts(&[".m4a"]));

        status.set_dirs(&[]);
        assert!(!status.is_enabled());
        assert!(status.snapshot().dirs.is_empty());
    }

    // ── runtime configuration ────────────────────────────────────────────────

    #[test]
    fn ext_lists_are_normalized_deduped_and_order_preserving() {
        assert_eq!(
            normalize_exts(&exts(&["M4A", ".ogg", "*.WAV", " opus "])).unwrap(),
            exts(&[".m4a", ".ogg", ".wav", ".opus"])
        );
        // Dedup keeps the caller's order (the UI echoes the list back).
        assert_eq!(
            normalize_exts(&exts(&["ogg", ".OGG", "*.ogg"])).unwrap(),
            exts(&[".ogg"])
        );

        assert_eq!(normalize_exts(&[]).unwrap_err(), ERR_NO_EXTS);
        for bad in ["  ", "a b", "og g", "au/dio", "au\\dio", ".", ".."] {
            let err = normalize_exts(&exts(&[bad])).unwrap_err();
            assert!(
                err.starts_with("extensão inválida"),
                "{bad:?} should be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn config_file_roundtrips_and_survives_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("watcher_config.json");

        assert_eq!(load_config(&path), StoredConfig::default()); // missing file

        save_config(
            &path,
            &[PathBuf::from("/w/a"), PathBuf::from("/w/b")],
            &exts(&[".ogg", ".m4a"]),
        );
        assert_eq!(
            load_config(&path),
            StoredConfig {
                dirs: vec!["/w/a".to_string(), "/w/b".to_string()],
                exts: exts(&[".ogg", ".m4a"]),
            }
        );

        std::fs::write(&path, "{ truncated").unwrap();
        assert_eq!(load_config(&path), StoredConfig::default());

        // A partial file (only `dirs`) is still usable.
        std::fs::write(&path, r#"{"dirs":["/w/a"]}"#).unwrap();
        assert_eq!(load_config(&path).dirs, vec!["/w/a".to_string()]);
        assert!(load_config(&path).exts.is_empty());
    }

    fn cfg(dirs: &[&Path], exts: &[&str]) -> WatchConfig {
        WatchConfig {
            dirs: dirs.iter().map(|d| d.to_path_buf()).collect(),
            exts: exts.iter().map(|e| e.to_string()).collect(),
            notify: true,
        }
    }

    #[test]
    fn startup_merges_persisted_config_with_flags() {
        let tmp = tempfile::tempdir().unwrap();
        let saved = tmp.path().join("saved");
        let flagged = tmp.path().join("flagged");
        std::fs::create_dir(&saved).unwrap();
        std::fs::create_dir(&flagged).unwrap();
        let canon = |p: &Path| std::fs::canonicalize(p).unwrap();

        let stored = StoredConfig {
            dirs: vec![saved.display().to_string()],
            exts: exts(&[".m4a"]),
        };

        // Union of both sources, persisted first; flags do not replace the saved set.
        let (dirs, ext_list) = merge_startup(&stored, &cfg(&[&flagged], &[]));
        assert_eq!(dirs, vec![canon(&saved), canon(&flagged)]);
        // No --watch-ext given → the persisted list wins over the default.
        assert_eq!(ext_list, exts(&[".m4a"]));

        // Explicit --watch-ext replaces the persisted list entirely.
        let (_, ext_list) = merge_startup(&stored, &cfg(&[], &[".wav"]));
        assert_eq!(ext_list, exts(&[".wav"]));

        // Nothing anywhere → the default.
        let (dirs, ext_list) = merge_startup(&StoredConfig::default(), &cfg(&[], &[]));
        assert!(dirs.is_empty());
        assert_eq!(ext_list, exts(&[DEFAULT_EXT]));

        // The same folder from both sources is listed once.
        let (dirs, _) = merge_startup(&stored, &cfg(&[&saved], &[]));
        assert_eq!(dirs, vec![canon(&saved)]);

        // A persisted folder that no longer exists is dropped (and the caller
        // rewrites the file without it).
        let gone = StoredConfig {
            dirs: vec![tmp.path().join("deleted").display().to_string()],
            exts: Vec::new(),
        };
        let (dirs, ext_list) = merge_startup(&gone, &cfg(&[], &[]));
        assert!(dirs.is_empty());
        assert_eq!(ext_list, exts(&[DEFAULT_EXT]));
    }

    #[test]
    fn directory_validation_rejects_missing_paths_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("watched");
        std::fs::create_dir(&dir).unwrap();
        let file = tmp.path().join("note.ogg");
        std::fs::write(&file, b"x").unwrap();

        assert_eq!(
            validate_dir(&dir.display().to_string()).unwrap(),
            std::fs::canonicalize(&dir).unwrap()
        );
        // Trailing slash and `.` segments collapse to the same canonical path.
        assert_eq!(
            validate_dir(&format!("{}/", dir.display())).unwrap(),
            std::fs::canonicalize(&dir).unwrap()
        );

        assert_eq!(
            validate_dir(&tmp.path().join("nope").display().to_string()).unwrap_err(),
            ERR_NOT_FOUND
        );
        assert_eq!(
            validate_dir(&file.display().to_string()).unwrap_err(),
            ERR_NOT_A_DIR
        );
        assert_eq!(validate_dir("   ").unwrap_err(), ERR_EMPTY_PATH);
    }

    #[test]
    fn watched_dirs_match_by_exact_string_and_by_canonical_path() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("watched");
        std::fs::create_dir(&dir).unwrap();
        let canonical = std::fs::canonicalize(&dir).unwrap();
        let watched = vec![canonical.clone()];

        // Exactly what GET /v1/watcher showed.
        assert_eq!(match_dir(&watched, &canonical), Some(0));
        // A different spelling of the same folder (uncanonicalized temp path on
        // macOS is /var/... while the canonical one is /private/var/...).
        assert_eq!(match_dir(&watched, &dir), Some(0));
        assert_eq!(match_dir(&watched, &dir.join(".")), Some(0));
        // Unrelated, and unknown-but-nonexistent.
        assert_eq!(match_dir(&watched, tmp.path()), None);
        assert_eq!(match_dir(&watched, &tmp.path().join("gone")), None);
        assert_eq!(match_dir(&[], &canonical), None);
    }

    #[test]
    fn control_task_applies_and_persists_config_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a");
        let b = tmp.path().join("b");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        let config_path = tmp.path().join("watcher_config.json");

        let (tx, _rx) = mpsc::channel::<PathBuf>(4);
        let Ok(fs_watcher) = new_fs_watcher(tx) else {
            eprintln!("skipping: this host has no usable filesystem watcher");
            return;
        };
        let status = WatcherStatus::new(WatcherSnapshot::default());
        let mut control = Control {
            fs_watcher,
            dirs: Vec::new(),
            exts: exts(&[DEFAULT_EXT]),
            status: status.clone(),
            config_path: config_path.clone(),
        };

        let canon_a = std::fs::canonicalize(&a).unwrap();
        control.add_dir(canon_a.clone()).unwrap();
        assert!(status.is_enabled());
        assert_eq!(status.snapshot().dirs, vec![canon_a.display().to_string()]);

        // Idempotent: adding the same folder twice is a no-op success.
        control.add_dir(canon_a.clone()).unwrap();
        assert_eq!(status.snapshot().dirs.len(), 1);

        control.add_dir(std::fs::canonicalize(&b).unwrap()).unwrap();
        control.set_exts(exts(&[".m4a"])).unwrap();
        assert_eq!(
            load_config(&config_path),
            StoredConfig {
                dirs: status.snapshot().dirs,
                exts: exts(&[".m4a"]),
            }
        );

        // Removing by the un-canonicalized spelling still works.
        control.remove_dir(&b).unwrap();
        assert_eq!(status.snapshot().dirs, vec![canon_a.display().to_string()]);
        // ...and by the exact string the snapshot shows.
        control.remove_dir(&canon_a).unwrap();
        assert!(!status.is_enabled());
        assert_eq!(control.remove_dir(&canon_a).unwrap_err(), ERR_NOT_WATCHED);

        // The emptied list is persisted, not forgotten.
        assert_eq!(load_config(&config_path).dirs, Vec::<String>::new());
    }
}
