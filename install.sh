#!/bin/sh
# install.sh — parakeet-local-asr-rust installer
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/PabloBispo/parakeet-local-asr-rust/master/install.sh | sh
#
# Environment overrides:
#   VERSION=v0.2.0   — install a specific release tag instead of latest
#   INSTALL_DIR=...  — install binary somewhere other than ~/.local/bin
#
# This script will:
#   1. Detect your OS and CPU architecture
#   2. Fetch the latest release version from GitHub (or use $VERSION)
#   3. Download the release tarball + SHA256SUMS
#   4. Verify the checksum — aborts loudly on mismatch
#   5. Extract the binary to $INSTALL_DIR (default: ~/.local/bin), no sudo required
#   6. Check that ffmpeg is on PATH and print install instructions if not
#
# Nothing is downloaded silently. You can pipe this through `less` before running it.

set -eu

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO="PabloBispo/parakeet-local-asr-rust"
RELEASES_URL="https://github.com/${REPO}/releases"
API_URL="https://api.github.com/repos/${REPO}/releases/latest"
BINARY_NAME="parakeet-local-asr-rust"
INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/bin}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
error() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Require a command to be on PATH or abort with a helpful message.
need() {
    command -v "$1" >/dev/null 2>&1 || error "Required tool not found: $1. Please install it and re-run."
}

# Download a URL to a local file. Prefers curl; falls back to wget.
download() {
    url="$1"; dest="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$dest"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$dest"
    else
        error "Neither curl nor wget found. Install one of them and re-run."
    fi
}

# ---------------------------------------------------------------------------
# Temp dir — cleaned up automatically on exit
# ---------------------------------------------------------------------------

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

printf '\n'
printf '  ██████╗  █████╗ ██████╗  █████╗ ██╗  ██╗███████╗███████╗████████╗\n'
printf '  ██╔══██╗██╔══██╗██╔══██╗██╔══██╗██║ ██╔╝██╔════╝██╔════╝╚══██╔══╝\n'
printf '  ██████╔╝███████║██████╔╝███████║█████╔╝ █████╗  █████╗     ██║   \n'
printf '  ██╔═══╝ ██╔══██║██╔══██╗██╔══██║██╔═██╗ ██╔══╝  ██╔══╝     ██║   \n'
printf '  ██║     ██║  ██║██║  ██║██║  ██║██║  ██╗███████╗███████╗   ██║   \n'
printf '  ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝   ╚═╝   \n'
printf '                                                                      \n'
printf '  parakeet-local-asr-rust — local speech-to-text server (OpenAI-compatible)\n'
printf '\n'

# ---------------------------------------------------------------------------
# Detect OS + arch
# ---------------------------------------------------------------------------

OS="$(uname -s)"
ARCH="$(uname -m)"

info "Detected: ${OS} / ${ARCH}"

case "${OS}" in
    Darwin)
        case "${ARCH}" in
            arm64)  ASSET_SUFFIX="aarch64-apple-darwin.tar.gz" ;;
            x86_64)
                printf '\n'
                warn "No prebuilt binary for Intel (x86_64) Macs."
                printf '\n'
                printf '  Apple Silicon (arm64) Macs are covered. On an Intel Mac, build from source:\n'
                printf '    git clone https://github.com/PabloBispo/parakeet-local-asr-rust\n'
                printf '    cd parakeet-local-asr-rust && cargo build --release\n'
                printf '  (needs the Rust toolchain — https://rustup.rs), or use Docker (see the README).\n'
                printf '\n'
                exit 0
                ;;
            *)      error "Unsupported macOS architecture: ${ARCH}. See ${RELEASES_URL} to download manually." ;;
        esac
        CHECKSUM_CMD="shasum -a 256"
        ;;
    Linux)
        case "${ARCH}" in
            x86_64)          ASSET_SUFFIX="x86_64-unknown-linux-gnu.tar.gz" ;;
            aarch64|arm64)   ASSET_SUFFIX="aarch64-unknown-linux-gnu.tar.gz" ;;
            *)               error "Unsupported Linux architecture: ${ARCH}. See ${RELEASES_URL} to download manually." ;;
        esac
        CHECKSUM_CMD="sha256sum"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        printf '\n'
        warn "Windows detected. This sh script cannot install on Windows."
        printf '\n'
        printf '  Please download the .zip release manually from:\n'
        printf '  %s\n' "${RELEASES_URL}"
        printf '\n'
        printf '  Look for: parakeet-local-asr-rust-x86_64-pc-windows-msvc.zip\n'
        printf '  Extract it and add the folder to your PATH.\n'
        printf '\n'
        printf '  Alternatively, use Git Bash or WSL and re-run this script.\n'
        printf '\n'
        exit 0
        ;;
    *)
        error "Unsupported operating system: ${OS}. See ${RELEASES_URL} for manual downloads."
        ;;
esac

ASSET_NAME="${BINARY_NAME}-${ASSET_SUFFIX}"

# ---------------------------------------------------------------------------
# Resolve version
# ---------------------------------------------------------------------------

if [ -n "${VERSION:-}" ]; then
    info "Using specified version: ${VERSION}"
else
    info "Fetching latest release version from GitHub..."
    need grep
    need sed
    RELEASE_JSON="${TMP_DIR}/release.json"
    download "${API_URL}" "${RELEASE_JSON}"
    # Parse "tag_name": "v0.1.0" — no jq required
    VERSION="$(grep '"tag_name"' "${RELEASE_JSON}" | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
    if [ -z "${VERSION}" ]; then
        error "Could not parse release version from GitHub API. Check ${RELEASES_URL} and set VERSION= manually."
    fi
    ok "Latest release: ${VERSION}"
fi

BASE_URL="${RELEASES_URL}/download/${VERSION}"

# ---------------------------------------------------------------------------
# Download tarball + checksum file
# ---------------------------------------------------------------------------

TARBALL="${TMP_DIR}/${ASSET_NAME}"
SUMS_FILE="${TMP_DIR}/SHA256SUMS"

info "Downloading ${ASSET_NAME} ..."
download "${BASE_URL}/${ASSET_NAME}" "${TARBALL}"

info "Downloading SHA256SUMS ..."
download "${BASE_URL}/SHA256SUMS" "${SUMS_FILE}"

# ---------------------------------------------------------------------------
# Verify checksum — abort loudly on mismatch
# ---------------------------------------------------------------------------

info "Verifying checksum..."

# Extract the expected hash for this specific asset from the SUMS file
EXPECTED_LINE="$(grep "${ASSET_NAME}" "${SUMS_FILE}" || true)"
if [ -z "${EXPECTED_LINE}" ]; then
    error "No checksum entry found for '${ASSET_NAME}' in SHA256SUMS. Cannot verify download."
fi

# Write a single-entry checksum file pointing at the downloaded tarball
# so we can use the tool in check mode without changing directories.
VERIFY_FILE="${TMP_DIR}/verify.sha256"
EXPECTED_HASH="$(printf '%s' "${EXPECTED_LINE}" | awk '{print $1}')"
printf '%s  %s\n' "${EXPECTED_HASH}" "${TARBALL}" > "${VERIFY_FILE}"

ACTUAL_HASH="$(${CHECKSUM_CMD} "${TARBALL}" | awk '{print $1}')"

if [ "${ACTUAL_HASH}" != "${EXPECTED_HASH}" ]; then
    printf '\n'
    error "CHECKSUM MISMATCH — download is corrupt or tampered with.
  Expected: ${EXPECTED_HASH}
  Got:      ${ACTUAL_HASH}
  File:     ${TARBALL}
  Aborting. Do NOT proceed."
fi

ok "Checksum verified."

# ---------------------------------------------------------------------------
# Install binary
# ---------------------------------------------------------------------------

if [ ! -d "${INSTALL_DIR}" ]; then
    info "Creating install directory: ${INSTALL_DIR}"
    mkdir -p "${INSTALL_DIR}"
fi

info "Extracting to ${INSTALL_DIR} ..."
tar -xzf "${TARBALL}" -C "${INSTALL_DIR}" "${BINARY_NAME}"
chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

ok "Installed: ${INSTALL_DIR}/${BINARY_NAME}"

# ---------------------------------------------------------------------------
# PATH check
# ---------------------------------------------------------------------------

printf '\n'
case ":${PATH}:" in
    *":${INSTALL_DIR}:"*)
        ok "${INSTALL_DIR} is already on your PATH."
        ;;
    *)
        warn "${INSTALL_DIR} is NOT on your PATH."
        printf '\n'
        printf '  Add it by running the appropriate command for your shell:\n'
        printf '\n'
        printf '  bash / zsh:     echo '"'"'export PATH="%s:$PATH"'"'"' >> ~/.bashrc\n' "${INSTALL_DIR}"
        printf '  zsh (alt):      echo '"'"'export PATH="%s:$PATH"'"'"' >> ~/.zshrc\n' "${INSTALL_DIR}"
        printf '  fish:           fish_add_path %s\n' "${INSTALL_DIR}"
        printf '\n'
        printf '  Then restart your shell or run:  export PATH="%s:$PATH"\n' "${INSTALL_DIR}"
        printf '\n'
        ;;
esac

# ---------------------------------------------------------------------------
# ffmpeg check — parakeet-local-asr-rust needs ffmpeg at runtime
# ---------------------------------------------------------------------------

printf '\n'
if command -v ffmpeg >/dev/null 2>&1; then
    ok "ffmpeg found: $(command -v ffmpeg)"
else
    warn "ffmpeg was not found on PATH."
    printf '\n'
    printf '  parakeet-local-asr-rust uses ffmpeg to decode audio files.\n'
    printf '  Install it with:\n'
    printf '\n'
    case "${OS}" in
        Darwin)
            printf '    brew install ffmpeg\n'
            ;;
        Linux)
            printf '    Debian/Ubuntu:  sudo apt install ffmpeg\n'
            printf '    Fedora/RHEL:    sudo dnf install ffmpeg\n'
            printf '    Arch Linux:     sudo pacman -S ffmpeg\n'
            printf '    Alpine:         sudo apk add ffmpeg\n'
            ;;
    esac
    printf '\n'
    printf '  After installing ffmpeg, parakeet-local-asr-rust will work normally.\n'
    printf '\n'
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

printf '\n'
printf '  ────────────────────────────────────────────────────────────\n'
printf '  Installation complete!\n'
printf '\n'
printf '  Run the server:\n'
printf '    %s\n' "${BINARY_NAME}"
printf '\n'
printf '  On first run it will download the ML model (~456 MB) to ./models/\n'
printf '  and then serve on http://localhost:8090\n'
printf '\n'
printf '  Web UI:     http://localhost:8090/ui\n'
printf '  API:        POST http://localhost:8090/v1/audio/transcriptions\n'
printf '  ────────────────────────────────────────────────────────────\n'
printf '\n'
