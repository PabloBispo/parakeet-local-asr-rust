//! ffmpeg detection. Instruct-only: we locate a working ffmpeg, but never
//! download one — ffmpeg is universally available via the system package
//! manager, and fetching executables at runtime is both a security smell and
//! unnecessary. When it's missing the server still starts (so the UI loads and
//! can guide the user); transcription endpoints return a clear install hint.

use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Outcome of resolving ffmpeg at startup.
pub struct Ffmpeg {
    /// Command used to invoke ffmpeg. Always set (falls back to `"ffmpeg"`), but
    /// only meaningful when `available` is true.
    pub bin: PathBuf,
    /// Whether a working ffmpeg was found.
    pub available: bool,
    /// Where it was found: `"FFMPEG_PATH"` | `"path"` | `"missing"`.
    pub source: &'static str,
}

impl Ffmpeg {
    /// OS-appropriate install command shown to the user when ffmpeg is missing.
    pub fn install_hint() -> &'static str {
        match std::env::consts::OS {
            "macos" => "brew install ffmpeg",
            "linux" => "sudo apt install ffmpeg   # or: dnf / pacman / apk install ffmpeg",
            "windows" => "winget install ffmpeg   # or: choco install ffmpeg",
            _ => "install ffmpeg and ensure it is on your PATH",
        }
    }
}

/// Try `bin -version`; true if it runs and exits successfully.
fn works(bin: &str) -> bool {
    Command::new(bin)
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Resolve ffmpeg: explicit `FFMPEG_PATH` first, then PATH. Never downloads.
pub fn detect() -> Ffmpeg {
    if let Ok(p) = std::env::var("FFMPEG_PATH") {
        if !p.is_empty() && works(&p) {
            return Ffmpeg {
                bin: PathBuf::from(p),
                available: true,
                source: "FFMPEG_PATH",
            };
        }
        if !p.is_empty() {
            tracing::warn!("FFMPEG_PATH='{p}' does not run; falling back to PATH lookup");
        }
    }
    if works("ffmpeg") {
        return Ffmpeg {
            bin: PathBuf::from("ffmpeg"),
            available: true,
            source: "path",
        };
    }
    Ffmpeg {
        bin: PathBuf::from("ffmpeg"),
        available: false,
        source: "missing",
    }
}
