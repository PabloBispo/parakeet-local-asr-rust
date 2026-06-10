# Parakeet ASR — Rust

An **OpenAI-compatible** speech-to-text server in Rust. Drop-in replacement for
`POST /v1/audio/transcriptions` — point any OpenAI client at it and transcribe.

Powered by NVIDIA **Parakeet TDT 0.6B** (v3, 25 European languages incl. Portuguese)
via [`transcribe-rs`](https://github.com/cjpais/transcribe-rs) + ONNX Runtime — the
same engine the Voxia / Handy desktop apps ship in production. Small static binary,
low RAM, CPU int8 runs ~20–30× realtime. No Python, no PyTorch.

Sibling of the Python [`parakeet-asr-server`](../parakeet-asr-server) — **same API**.

---

## Run it (1 command)

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

Open **http://localhost:8090/ui** — a built-in, single-file UI (embedded in the binary,
no separate build, works offline). Drag-drop multiple files, auto-transcribe, playback
with click-to-seek segment timestamps, full-text search, WhatsApp-audio grouping, and
copy / download `.srt` / `.txt`. Your library is kept client-side (IndexedDB) — nothing
is stored server-side.

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
| `GET`  | `/health` | `{status, model, device}` |
| `GET`  | `/metrics` | `{queue_depth, total_requests, avg_latency_ms}` |

`verbose_json` adds `duration` and `segments[]` (`id`, `start`, `end`, `text`;
timestamps in seconds, absolute).

### Config (env)

| Var | Default | Meaning |
|---|---|---|
| `PORT` | `8090` | listen port |
| `ASR_MODEL` | `parakeet-tdt-0.6b-v3` | `parakeet-tdt-0.6b-v3` (25 EU langs) or `parakeet-tdt-0.6b-v2` (English) |
| `MODELS_DIR` | `models` (`/models` in Docker) | model cache dir |
| `ASR_DEVICE` | `cpu` | reported in `/health` (info only) |
| `RUST_LOG` | `parakeet_asr_rust=info` | log level |

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

### Layout

```
src/
├── main.rs        # app wiring, router, config
├── engine.rs      # ParakeetModel actor thread
├── audio.rs       # ffmpeg decode + chunking
├── pipeline.rs    # decode → chunk → engine → assemble
├── transcript.rs  # output type + segment assembly
├── routes.rs      # handlers (transcribe / stream / async / health / metrics)
├── jobs.rs        # async job queue + worker
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
