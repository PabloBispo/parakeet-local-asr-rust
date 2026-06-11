//! `update` subcommand: self-update the binary from the latest GitHub Release.
//!
//! Flow (the security gate is the SHA-256 check on the downloaded archive):
//!   1. resolve the latest release via the GitHub API
//!   2. download the platform archive + `SHA256SUMS`
//!   3. verify the archive against `SHA256SUMS` (abort on mismatch)
//!   4. extract the new binary and compare its SHA-256 to the running binary
//!   5. if they differ, atomically replace the running binary
//!
//! The decision to update is made by **binary content hash**, not the version
//! string — so a rebuilt-but-same-version release is detected, and an identical
//! binary is never needlessly rewritten.

use anyhow::{anyhow, bail, Result};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use tar::Archive;

const REPO: &str = "PabloBispo/parakeet-local-asr-rust";

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

impl Release {
    fn asset_url(&self, name: &str) -> Option<&str> {
        self.assets
            .iter()
            .find(|a| a.name == name)
            .map(|a| a.browser_download_url.as_str())
    }
}

pub async fn run() -> Result<()> {
    println!("current version: v{}", env!("CARGO_PKG_VERSION"));

    let (archive_name, is_zip) = platform_archive()?;
    if is_zip {
        bail!(
            "automatic update isn't supported on Windows yet.\n\
             Download the latest release manually:\n  https://github.com/{REPO}/releases/latest"
        );
    }

    let client = reqwest::Client::builder()
        .user_agent(concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    // 1. latest release
    let rel = latest_release(&client).await?;
    println!("latest release:  {}", rel.tag_name);

    let archive_url = rel.asset_url(&archive_name).ok_or_else(|| {
        anyhow!(
            "no prebuilt binary for this platform ({archive_name}) in {}. \
             Build from source: cargo build --release",
            rel.tag_name
        )
    })?;
    let sums_url = rel
        .asset_url("SHA256SUMS")
        .ok_or_else(|| anyhow!("release {} has no SHA256SUMS", rel.tag_name))?;

    // 2. download
    println!("downloading {archive_name} ...");
    let archive_bytes = download(&client, archive_url).await?;
    let sums = String::from_utf8_lossy(&download(&client, sums_url).await?).into_owned();

    // 3. verify the archive (security gate)
    let expected = sum_for(&sums, &archive_name)
        .ok_or_else(|| anyhow!("{archive_name} is not listed in SHA256SUMS"))?;
    let actual = sha256_hex(&archive_bytes);
    if actual != expected {
        bail!("checksum mismatch for {archive_name}: expected {expected}, got {actual}");
    }
    println!("checksum verified \u{2713}");

    // 4. extract the new binary, compare by hash to the running one
    let new_bin = extract_binary(&archive_bytes)?;
    let current_exe = std::env::current_exe()?;
    let current_bytes = std::fs::read(&current_exe)?;
    if sha256_hex(&current_bytes) == sha256_hex(&new_bin) {
        println!(
            "already up to date \u{2713}  (binary hash matches {}).",
            rel.tag_name
        );
        return Ok(());
    }

    // 5. atomically replace
    replace_binary(&current_exe, &new_bin)?;
    println!(
        "updated to {} \u{2713}  ({})",
        rel.tag_name,
        current_exe.display()
    );
    println!("restart the server to run the new version.");
    Ok(())
}

fn platform_archive() -> Result<(String, bool)> {
    let pkg = env!("CARGO_PKG_NAME");
    let (triple, is_zip) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => ("aarch64-apple-darwin", false),
        ("linux", "x86_64") => ("x86_64-unknown-linux-gnu", false),
        ("linux", "aarch64") => ("aarch64-unknown-linux-gnu", false),
        ("windows", "x86_64") => ("x86_64-pc-windows-msvc", true),
        ("macos", "x86_64") => {
            bail!("no prebuilt binary for Intel Macs — build from source: cargo build --release")
        }
        (os, arch) => bail!("unsupported platform {os}/{arch} — build from source"),
    };
    let ext = if is_zip { "zip" } else { "tar.gz" };
    Ok((format!("{pkg}-{triple}.{ext}"), is_zip))
}

async fn latest_release(client: &reqwest::Client) -> Result<Release> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let bytes = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    serde_json::from_slice(&bytes).map_err(|e| anyhow!("failed to parse GitHub API response: {e}"))
}

async fn download(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let resp = client.get(url).send().await?.error_for_status()?;
    Ok(resp.bytes().await?.to_vec())
}

/// Find the SHA-256 for `name` in a `SHA256SUMS` body (`<hash>  <file>` per line;
/// matched by basename, tolerating a leading `*`).
fn sum_for(sums: &str, name: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let mut it = line.split_whitespace();
        let hash = it.next()?;
        let file = it.next()?;
        let base = file.trim_start_matches('*').rsplit('/').next().unwrap_or(file);
        (base == name).then(|| hash.to_string())
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

fn extract_binary(archive: &[u8]) -> Result<Vec<u8>> {
    let bin_name = env!("CARGO_PKG_NAME");
    let mut ar = Archive::new(GzDecoder::new(archive));
    for entry in ar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.file_name().and_then(|s| s.to_str()) == Some(bin_name) {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            return Ok(buf);
        }
    }
    Err(anyhow!("binary '{bin_name}' not found in the release archive"))
}

/// Write the new binary next to the current one and rename over it. On Unix a
/// rename over a running executable is fine (the live process keeps its inode).
fn replace_binary(current_exe: &Path, new_bytes: &[u8]) -> Result<()> {
    let dir = current_exe
        .parent()
        .ok_or_else(|| anyhow!("executable has no parent directory"))?;
    let fname = current_exe
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("parakeet-local-asr-rust");
    let tmp = dir.join(format!(".{fname}.update"));

    std::fs::write(&tmp, new_bytes)
        .map_err(|e| anyhow!("failed to write {} (need write permission?): {e}", tmp.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }

    std::fs::rename(&tmp, current_exe).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        anyhow!(
            "failed to replace {} (need write permission to the install dir?): {e}",
            current_exe.display()
        )
    })?;
    Ok(())
}
