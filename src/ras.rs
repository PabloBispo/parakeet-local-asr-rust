//! The data home — `$RAS_HOME`, default `~/.ras`.
//!
//! Everything the server keeps between runs lives under one directory, so a user
//! can back it up, inspect it, or delete it in a single move:
//!
//! ```text
//! $RAS_HOME/
//! ├── models/               model cache (unless MODELS_DIR overrides it)
//! ├── transcripts/          <uuid>.json + <uuid>.txt per transcription
//! ├── audio/                <uuid>-<name> — the original audio, verbatim
//! ├── watcher_config.json   folder-watcher config (folders + extensions)
//! └── watcher_state.json    folder-watcher dedup state
//! ```
//!
//! Resolution order for the home itself: `RAS_HOME` → `~/.ras` → `./.ras`
//! (last resort, with a warning — a home directory is essentially always there).

use std::path::{Path, PathBuf};

/// Directory created inside the user's home when `RAS_HOME` is not set.
const HOME_DIR_NAME: &str = ".ras";

/// Resolve the data home. Does not create anything.
pub fn home() -> PathBuf {
    if let Some(raw) = std::env::var_os("RAS_HOME") {
        if !raw.is_empty() {
            return expand_tilde(&raw.to_string_lossy());
        }
    }
    match dirs::home_dir() {
        Some(h) => h.join(HOME_DIR_NAME),
        None => {
            tracing::warn!(
                "could not determine a home directory — falling back to ./{HOME_DIR_NAME} \
                 (set RAS_HOME to choose explicitly)"
            );
            PathBuf::from(".").join(HOME_DIR_NAME)
        }
    }
}

pub fn models_dir(ras_home: &Path) -> PathBuf {
    ras_home.join("models")
}

pub fn transcripts_dir(ras_home: &Path) -> PathBuf {
    ras_home.join("transcripts")
}

pub fn audio_dir(ras_home: &Path) -> PathBuf {
    ras_home.join("audio")
}

pub fn watcher_state_path(ras_home: &Path) -> PathBuf {
    ras_home.join("watcher_state.json")
}

/// Watched folders and extensions, as configured at runtime from the UI/API.
pub fn watcher_config_path(ras_home: &Path) -> PathBuf {
    ras_home.join("watcher_config.json")
}

/// Expand a leading `~` in a user-supplied path. Shells do this for us on the
/// command line, but not for values coming from env vars or quoted arguments.
pub fn expand_tilde(raw: &str) -> PathBuf {
    let rest = match raw {
        "~" => return dirs::home_dir().unwrap_or_else(|| PathBuf::from(raw)),
        r if r.starts_with("~/") || r.starts_with("~\\") => &r[2..],
        other => return PathBuf::from(other),
    };
    match dirs::home_dir() {
        Some(h) => h.join(rest),
        None => PathBuf::from(raw),
    }
}

/// Boolean env var: true when set to anything other than "", "0" or "false".
pub fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            !(v.is_empty() || v == "0" || v == "false")
        }
        Err(_) => false,
    }
}

/// Write `data` to `path` atomically: fill a sibling temp file, then rename.
///
/// A plain `fs::write` truncates first, so a concurrent reader (`GET /v1/history`
/// polls constantly) can observe a half-written record — and silently drop it as
/// corrupt. Rename inside the same directory is atomic, so a reader sees either
/// the old file or the complete new one.
///
/// The temp name is dot-prefixed and extension-less so that neither the history
/// listing (`*.json`) nor the audio lookup (`<id>-*`) can ever pick it up.
pub fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&tmp, data)?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Comma-separated env var → trimmed, non-empty items.
pub fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_tilde_only_touches_a_leading_tilde() {
        let home = dirs::home_dir().expect("test host has a home dir");
        assert_eq!(expand_tilde("~"), home);
        assert_eq!(expand_tilde("~/Downloads"), home.join("Downloads"));
        assert_eq!(expand_tilde("/tmp/x"), PathBuf::from("/tmp/x"));
        // A tilde in the middle is a legitimate filename character.
        assert_eq!(expand_tilde("/tmp/a~b"), PathBuf::from("/tmp/a~b"));
        assert_eq!(expand_tilde("rel/dir"), PathBuf::from("rel/dir"));
    }

    #[test]
    fn write_atomic_replaces_content_and_leaves_no_temp_file() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("state.json");

        write_atomic(&target, b"first").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"first");
        write_atomic(&target, b"second").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"second");

        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n != "state.json")
            .collect();
        assert!(leftovers.is_empty(), "temp files left behind: {leftovers:?}");
    }

    #[test]
    fn layout_hangs_off_the_home() {
        let h = PathBuf::from("/data/.ras");
        assert_eq!(models_dir(&h), PathBuf::from("/data/.ras/models"));
        assert_eq!(
            transcripts_dir(&h),
            PathBuf::from("/data/.ras/transcripts")
        );
        assert_eq!(audio_dir(&h), PathBuf::from("/data/.ras/audio"));
        assert_eq!(
            watcher_state_path(&h),
            PathBuf::from("/data/.ras/watcher_state.json")
        );
        assert_eq!(
            watcher_config_path(&h),
            PathBuf::from("/data/.ras/watcher_config.json")
        );
    }
}
