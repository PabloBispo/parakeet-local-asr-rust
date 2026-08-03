//! Model auto-download + extraction.
//!
//! Parakeet int8 model bundles are hosted as `.tar.gz` archives on the Handy CDN
//! (same artifacts the Voxia/Handy desktop apps use). On first run we download,
//! verify the SHA-256, and extract into `MODELS_DIR/<dir_name>/`.

use anyhow::{anyhow, Result};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use tar::Archive;

struct ModelSpec {
    dir_name: &'static str,
    url: &'static str,
    sha256: &'static str,
}

fn spec(model_id: &str) -> Result<ModelSpec> {
    match model_id {
        "parakeet-tdt-0.6b-v3" | "v3" | "default" => Ok(ModelSpec {
            dir_name: "parakeet-tdt-0.6b-v3-int8",
            // First-party mirror on our own GitHub Releases (models-v1) — no
            // dependency on a third-party CDN for the default model.
            url: "https://github.com/PabloBispo/parakeet-local-asr-rust/releases/download/models-v1/parakeet-v3-int8.tar.gz",
            sha256: "734f0c00ab60687d14bc1560319209af82b71e8b0be064dcc2fae46a70f0df7d",
        }),
        // v2 is secondary and still pulled from the upstream Handy CDN (not
        // mirrored). The default path (v3 above) is fully first-party.
        "parakeet-tdt-0.6b-v2" | "v2" => Ok(ModelSpec {
            dir_name: "parakeet-tdt-0.6b-v2-int8",
            url: "https://blob.handy.computer/parakeet-v2-int8.tar.gz",
            sha256: "ac9b9429984dd565b25097337a887bb7f0f8ac393573661c651f0e7d31563991",
        }),
        other => Err(anyhow!(
            "unknown model id '{other}' (expected parakeet-tdt-0.6b-v3 or -v2)"
        )),
    }
}

/// Directory name a model extracts into, e.g. `parakeet-tdt-0.6b-v3-int8`.
/// `None` for an unknown id. Used to detect an existing legacy `./models` install.
pub fn dir_name(model_id: &str) -> Option<&'static str> {
    spec(model_id).ok().map(|s| s.dir_name)
}

/// Ensure the model is present under `models_dir`, downloading + extracting if absent.
/// Returns the path to the model directory ready for `ParakeetModel::load`.
pub async fn ensure_model(models_dir: &Path, model_id: &str) -> Result<PathBuf> {
    let spec = spec(model_id)?;
    let model_dir = models_dir.join(spec.dir_name);
    if model_dir.is_dir() {
        tracing::info!("model already present: {}", model_dir.display());
        return Ok(model_dir);
    }

    std::fs::create_dir_all(models_dir)?;
    let tar_path = models_dir.join(format!("{}.tar.gz", spec.dir_name));
    let part_path = models_dir.join(format!("{}.tar.gz.part", spec.dir_name));

    tracing::info!("downloading model '{model_id}' from {}", spec.url);
    // Download to a .part file, then rename — so a killed/partial download never
    // looks like a complete archive.
    download(spec.url, &part_path).await?;
    std::fs::rename(&part_path, &tar_path)?;

    tracing::info!("verifying checksum");
    verify_sha256(&tar_path, spec.sha256)?;

    tracing::info!("extracting archive");
    extract(&tar_path, models_dir, &model_dir)?;
    let _ = std::fs::remove_file(&tar_path);

    if !model_dir.is_dir() {
        return Err(anyhow!(
            "extraction did not produce {}",
            model_dir.display()
        ));
    }
    tracing::info!("model ready: {}", model_dir.display());
    Ok(model_dir)
}

async fn download(url: &str, dest: &Path) -> Result<()> {
    // Fail fast on a dead/stalled CDN instead of hanging server startup forever.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(1800))
        .build()?;
    let resp = client.get(url).send().await?.error_for_status()?;
    let mut file = File::create(dest)?;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk)?;
    }
    file.flush()?;
    Ok(())
}

fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    let actual = format!("{:x}", hasher.finalize());
    if actual != expected {
        let _ = std::fs::remove_file(path);
        return Err(anyhow!(
            "checksum mismatch: expected {expected}, got {actual} (deleted corrupt download)"
        ));
    }
    Ok(())
}

fn extract(tar_path: &Path, models_dir: &Path, final_dir: &Path) -> Result<()> {
    let tmp = models_dir.join(".extracting");
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    std::fs::create_dir_all(&tmp)?;

    let tar_gz = File::open(tar_path)?;
    let mut archive = Archive::new(GzDecoder::new(tar_gz));
    archive.unpack(&tmp)?;

    // The archive may wrap everything in a single top-level directory; flatten it
    // so the model files land directly in `final_dir`.
    let dirs: Vec<_> = std::fs::read_dir(&tmp)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    if final_dir.exists() {
        std::fs::remove_dir_all(final_dir)?;
    }
    if dirs.len() == 1 {
        std::fs::rename(dirs[0].path(), final_dir)?;
        let _ = std::fs::remove_dir_all(&tmp);
    } else {
        std::fs::rename(&tmp, final_dir)?;
    }
    Ok(())
}
