# Parakeet ASR — Rust

An **OpenAI-compatible** speech-to-text server in Rust. Drop-in replacement for
`POST /v1/audio/transcriptions` — point any OpenAI client at it and transcribe.

Powered by NVIDIA **Parakeet TDT 0.6B** (v3, 25 European languages incl. Portuguese)
via [`transcribe-rs`](https://github.com/cjpais/transcribe-rs) + ONNX Runtime — the
same engine the Voxia / Handy desktop apps ship in production. Small static binary,
low RAM, CPU int8 runs ~20–30× realtime. No Python, no PyTorch.

Sibling of the Python [`parakeet-asr-server`](../parakeet-asr-server) — **same API**.

---

## Install (one-liner)

Prebuilt, self-contained binary (macOS & Linux). First-party script + GitHub
Releases, **SHA-256 verified**, installs to `~/.local/bin` (no sudo):

```bash
curl -fsSL https://raw.githubusercontent.com/PabloBispo/parakeet-local-asr-rust/master/install.sh | sh
```

Audit it first if you like: `curl -fsSL .../install.sh | less`. Then run
`parakeet-local-asr-rust` and open **http://localhost:8090/ui**. The server detects
`ffmpeg` and, if it's missing, tells you the exact install command (and the web UI
shows a banner). Windows: grab the `.zip` from the [Releases](https://github.com/PabloBispo/parakeet-local-asr-rust/releases) page.

> **No prebuilt binary for your platform** (e.g. an older Intel Mac)? Build it
> yourself in one command — `cargo build --release` (needs the Rust toolchain) —
> or just use Docker below. The binary is self-contained (ONNX Runtime is statically
> linked); you only need `ffmpeg` at runtime.

### Updating

```bash
parakeet-local-asr-rust update
```

Self-updates from the latest GitHub Release. It downloads the platform archive,
**verifies it against the published `SHA256SUMS`**, and only replaces the binary if
its **content hash** differs from what you're running (so it's a no-op when you're
already current). `parakeet-local-asr-rust version` prints the installed version.

---

## Run with Docker

```bash
docker compose up --build
```

That's it. First boot downloads the model (~456 MB, cached in a volume) and serves on
**http://localhost:8090**. Check it:

```bash
curl http://localhost:8090/health
# {"status":"ok","model":"parakeet-tdt-0.6b-v3","device":"cpu"}
```

Transcribe a file:

```bash
curl -s http://localhost:8090/v1/audio/transcriptions \
  -F file=@audio.mp3 \
  -F model=parakeet-tdt-0.6b-v3
# {"text":"..."}
```

Any ffmpeg-readable format works (wav, mp3, m4a, ogg/opus, flac…). Audio is decoded
to 16 kHz mono internally.

### Web UI

On startup the server **opens the UI in your default browser** automatically
(set `ASR_NO_OPEN=1` to disable, e.g. on a headless box). Or open
**http://localhost:8090/ui** yourself — a built-in, single-file UI (embedded in the
binary, no separate build, works offline). A **`docs ↗`** button in the header opens
the rendered API/integration docs at `/docs`. Drag-drop multiple files, auto-transcribe, playback
with click-to-seek segment timestamps, full-text search, WhatsApp-audio grouping, and
copy / download `.srt` / `.txt`. The browser keeps its own drag-drop library in
IndexedDB; everything the **server** transcribed is listed separately from `~/.ras`.

### Folder watcher

Point the server at a folder and every audio file that lands there is transcribed
automatically — save a WhatsApp voice note to `~/Downloads` and the transcript is
waiting for you:

```bash
parakeet-local-asr-rust serve --watch ~/Downloads --watch-ext .ogg
```

- `--watch <dir>` and `--watch-ext <ext>` are repeatable (`--watch-ext` defaults to
  `.ogg`); `--no-notify` turns the desktop notification off.
- Partial downloads (`.crdownload`, `.part`, …) are ignored until the downloader
  renames them, and a file is only read once its size has stopped changing.
- Each result is saved to the history (visible in the UI) and announced with a
  desktop notification. On macOS with
  [`terminal-notifier`](https://github.com/julienXX/terminal-notifier) installed
  (`brew install terminal-notifier`), **clicking the notification copies the
  transcript** to the clipboard; otherwise it is copied immediately via `pbcopy`.
  On Linux it uses `notify-send` + `xclip` when present.
- Already-transcribed files are remembered in `~/.ras/watcher_state.json`, so a
  restart does not re-transcribe the whole folder.

#### Configurable at runtime — from the UI or the API

No flags and no restart needed: **folders and extensions can be changed while the
server runs**, from the watcher panel in the UI or over HTTP. Changes are saved to
`~/.ras/watcher_config.json` and restored on the next start.

```bash
# start watching a folder (~ is expanded server-side)
curl -s -X POST http://localhost:8090/v1/watcher/dirs \
  -H 'Content-Type: application/json' -d '{"path":"~/Downloads"}'

# replace the extension list (the whole list, not a delta; a missing dot is added)
curl -s -X PUT http://localhost:8090/v1/watcher/exts \
  -H 'Content-Type: application/json' -d '{"exts":[".ogg","m4a"]}'

# stop watching a folder
curl -s -X DELETE http://localhost:8090/v1/watcher/dirs \
  -H 'Content-Type: application/json' -d '{"path":"/Users/me/Downloads"}'
```

All three answer with the same body as `GET /v1/watcher` (so a client just
re-renders), or `400 {"error":"pasta não encontrada"}` on bad input. `enabled` in
that body means "at least one folder is being watched" — it flips as folders are
added and removed.

Changes take effect immediately, including while a long file is being transcribed:
the watcher runs a control task (owns the folder subscriptions) separately from its
transcription task, so a config call never queues behind a 30-minute recording.

> **Origin guard.** The server listens on `0.0.0.0` with permissive CORS so local
> tools and the browser extension can post audio from anywhere. These three
> endpoints are the exception: they choose which folders on your machine get read,
> so a request carrying a non-loopback `Origin` header is refused with
> `403 {"error":"origem não permitida"}`. Requests with no `Origin` (curl, scripts,
> SDKs) and browser requests from `localhost` / `127.0.0.1` / `[::1]` on **any**
> port are allowed. The `GET` endpoints and `/v1/audio/*` are unaffected.

### Run without Docker

Needs `ffmpeg` on PATH and a Rust toolchain (≥ 1.83):

```bash
cargo run --release        # downloads model to ./models on first run, serves :8090
```

---

## Getting started — integrate in 30 seconds

Base URL `http://localhost:8090/v1`, **no API key needed** (pass any dummy string —
SDKs require one, the server ignores it). Full recipes (LangGraph, Agno, streaming,
async jobs) in [`docs/integrations.md`](docs/integrations.md).

### OpenAI SDK (Python)

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")

with open("audio.mp3", "rb") as f:
    r = client.audio.transcriptions.create(
        model="parakeet-tdt-0.6b-v3",
        file=f,
        response_format="verbose_json",   # text + per-segment timestamps
    )
print(r.text)
```

### LangChain (Python)

```python
from langchain_core.tools import tool
from openai import OpenAI

asr = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")

@tool
def transcribe_audio(path: str) -> str:
    """Transcribe an audio file to text."""
    with open(path, "rb") as f:
        return asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3", file=f
        ).text

# bind `transcribe_audio` to any LangChain agent as a tool
```

### Agno

```python
from agno.agent import Agent
from agno.models.openai import OpenAIChat       # your real chat LLM
from openai import OpenAI

asr = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")

def transcribe_audio(path: str) -> str:
    """Transcribe an audio file and return its text."""
    with open(path, "rb") as f:
        return asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3", file=f
        ).text

agent = Agent(model=OpenAIChat(id="gpt-4o-mini"), tools=[transcribe_audio])
agent.print_response("Transcribe meeting.mp3 and summarize the decisions.")
```

→ See [`docs/integrations.md`](docs/integrations.md) for **LangGraph** (`StateGraph`
transcribe→summarize), JS/TS, SSE streaming, and async job polling.

---

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/audio/transcriptions` | OpenAI-compatible. multipart `file` + `model` + `response_format` (`json`\|`text`\|`verbose_json`\|`srt`\|`vtt`) |
| `POST` | `/v1/audio/transcriptions/stream` | SSE — one event per ~20 s chunk, partial text as it's ready |
| `POST` | `/v1/audio/transcriptions/async` | → `202 {job_id}` for very long audio |
| `GET`  | `/v1/audio/jobs/{id}` | poll job: `queued\|processing\|done\|failed` + result |
| `GET`  | `/v1/history?limit=N` | saved recordings, newest first (`limit` default 100, max 500; no `segments`) |
| `GET`  | `/v1/history/{id}` | one recording, `segments[]` included |
| `GET`  | `/v1/history/{id}/audio` | the archived original audio |
| `GET`  | `/v1/history/{id}/download?format=` | `txt` (default) \| `srt` \| `vtt` \| `json` as an attachment |
| `DELETE` | `/v1/history/{id}` | delete record + text + audio → `{"deleted": true}` |
| `GET`  | `/v1/watcher` | folder-watcher status, config + counters |
| `POST` | `/v1/watcher/dirs` | `{"path":"~/Downloads"}` → start watching it; answers with the `/v1/watcher` body |
| `DELETE` | `/v1/watcher/dirs` | `{"path":"/Users/me/Downloads"}` → stop watching it (path in the body) |
| `PUT`  | `/v1/watcher/exts` | `{"exts":[".ogg","m4a"]}` → replace the watched extensions |
| `GET`  | `/health` | `{status, model, device, history_count, watcher_enabled}` |
| `GET`  | `/metrics` | `{queue_depth, total_requests, avg_latency_ms}` |

`verbose_json` adds `duration` and `segments[]` (`id`, `start`, `end`, `text`;
timestamps in seconds, absolute).

### Config (env)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8090` | listen port |
| `ASR_MODEL` | `parakeet-tdt-0.6b-v3` | `parakeet-tdt-0.6b-v3` (25 EU langs) or `parakeet-tdt-0.6b-v2` (English) |
| `MODELS_DIR` | `$RAS_HOME/models` (`/models` in Docker) | model cache dir |
| `ASR_DEVICE` | `cpu` | reported in `/health` (info only) |
| `RAS_HOME` | `~/.ras` | data home: transcripts, archived audio, model cache, watcher config + state |
| `ASR_WATCH_DIRS` | — | comma-separated folders to watch (same as `--watch`; merged into the saved config) |
| `ASR_WATCH_EXTS` | `.ogg` | comma-separated extensions the watcher picks up (overrides the saved list) |
| `ASR_NO_NOTIFY` | — | `1` disables desktop notifications |
| `ASR_NO_HISTORY` | — | `1` disables persistence — nothing is written to `~/.ras` |
| `ASR_NO_OPEN` | — | `1` does not open the browser on startup |
| `RUST_LOG` | `parakeet_local_asr_rust=info` | log level |

---

## How it works

- **Axum + Tokio** HTTP layer.
- **Engine actor**: `ParakeetModel` (ONNX Runtime sessions) is pinned to one dedicated
  thread and fed over a channel. Inference is serialized — bounds peak RAM, never moves
  the non-`Send` model across threads.
- **ffmpeg** subprocess decodes any input to 16 kHz mono f32.
- Long audio is **chunked** (240 s for sync/async, 20 s for streaming) and timestamps
  are stitched back to absolute time.
- Model is **auto-downloaded + SHA-256 verified** on first run (Handy CDN artifacts).
- **Folder watcher** (FSEvents / inotify) feeds its own sequential worker, so watched
  files never compete for the async job queue that API clients poll. It is split in
  two tasks — a control task owning the folder subscriptions and a transcription task
  draining events — so runtime config changes answer in milliseconds even while a
  long recording is being transcribed.

### Data home — `~/.ras`

Everything the server keeps between runs lives in one directory (override with
`RAS_HOME`, disable persistence entirely with `ASR_NO_HISTORY=1`):

```
~/.ras/
├── models/                     model cache (unless MODELS_DIR is set)
├── transcripts/
│   ├── <uuid>.json             full record: metadata + text + segments
│   └── <uuid>.txt              plain text (what a notification copies)
├── audio/<uuid>-<name>         the original audio, byte-for-byte
├── watcher_config.json         watched folders + extensions, as set from the UI/API
└── watcher_state.json          per-file (size, mtime) so nothing is transcribed twice
```

One file per record, no index: nothing to corrupt or lock, `cat`-readable, and
prunable with `rm`. Writes are atomic (temp file + rename), so a concurrent
`/v1/history` read never sees a half-written record. Successful API transcriptions
and every watched file are saved; failed API calls are not (they answer over HTTP),
while failed watched files are (the UI is their only feedback channel). The SSE
streaming endpoint never persists — it never assembles a final transcript.

An existing `./models/<model>` directory is still used when present, so upgrading
from an earlier version does not orphan an already-downloaded model. In Docker the
model cache is `/models` (a volume) and the data home falls back to `/root/.ras`
inside the container — set `RAS_HOME` to a mounted path if you want the saved
transcripts to survive `docker compose down`.

### Layout

```
src/
├── main.rs        # app wiring, router, config, CLI flags
├── engine.rs      # ParakeetModel actor thread
├── audio.rs       # ffmpeg decode + chunking
├── pipeline.rs    # decode → chunk → engine → assemble
├── transcript.rs  # output type + segment assembly + srt/vtt rendering
├── routes.rs      # handlers (transcribe / stream / async / history / watcher / ops)
├── jobs.rs        # async job queue + worker
├── watcher.rs     # folder watcher + desktop notifications
├── history.rs     # persistent transcript store (~/.ras)
├── ras.rs         # data-home resolution + atomic writes
├── model.rs       # download + extract + verify
├── metrics.rs     # counters
├── state.rs       # shared AppState
└── error.rs       # JSON error responses
static/index.html      # built-in web UI (embedded via include_str!)
docs/integrations.md   # openai / langchain / langgraph / agno recipes
```

---

## License

PoC / evaluation code. Model weights are NVIDIA's (CC-BY-4.0). See the
[Parakeet model card](https://huggingface.co/nvidia/parakeet-tdt-0.6b-v3).
