# Integrations

The Rust ASR server exposes an **OpenAI-compatible** transcription API. Any tool that
speaks the OpenAI Audio API works against it by overriding `base_url` to point at this
server — no client code changes beyond that, and no real API key is needed (pass any
non-empty string; the server ignores it).

- **Base URL:** `http://localhost:8090/v1`
- **Model id:** `parakeet-tdt-0.6b-v3` (also accepts `parakeet-tdt-0.6b-v2`)
- **Auth:** none — pass `"not-needed"` or any non-empty string; the server ignores it
- **Audio formats:** anything ffmpeg decodes (wav, mp3, m4a, ogg/opus, flac, webm, …);
  decoded internally to 16 kHz mono

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/v1/audio/transcriptions` | OpenAI-compat sync transcription (multipart form) |
| `POST` | `/v1/audio/transcriptions/stream` | SSE — one event per audio chunk; last has `"final": true` |
| `POST` | `/v1/audio/transcriptions/async` | Enqueue long audio → `202 {"job_id": "...", "status": "queued"}` |
| `GET`  | `/v1/audio/jobs/{job_id}` | Poll async job — `{"status": "queued\|processing\|done\|failed", "result": ...}` |
| `GET`  | `/v1/history?limit=N` | Saved recordings, newest first (see below) |
| `GET`  | `/v1/history/{id}` | One recording with `segments[]` |
| `GET`  | `/v1/history/{id}/audio` | The archived original audio bytes |
| `GET`  | `/v1/history/{id}/download?format=txt\|srt\|vtt\|json` | Transcript as a file attachment |
| `DELETE` | `/v1/history/{id}` | Delete record + text + audio → `{"deleted": true}` |
| `GET`  | `/v1/watcher` | Folder-watcher status and counters |
| `GET`  | `/health` | `{"status": "ok", "model": ..., "device": ..., "history_count": N, "watcher_enabled": bool}` |
| `GET`  | `/metrics` | `{"queue_depth": N, "total_requests": N, "avg_latency_ms": N}` |

`response_format` (sync endpoint): `json` (default, `{"text": "..."}`), `text`,
`verbose_json` (adds `duration` and `segments[]` each with `id`, `start`, `end`, `text`
in absolute seconds), `srt`, `vtt`.

---

## Raw HTTP — curl

Basic transcription:

```bash
# Returns {"text": "..."}
curl -s http://localhost:8090/v1/audio/transcriptions \
  -F "file=@audio.mp3" \
  -F "model=parakeet-tdt-0.6b-v3" \
  -F "response_format=json" | jq .text
```

Timestamped segments:

```bash
curl -s http://localhost:8090/v1/audio/transcriptions \
  -F "file=@audio.mp3" \
  -F "model=parakeet-tdt-0.6b-v3" \
  -F "response_format=verbose_json" \
  | jq '.segments[] | {start, end, text}'
```

SSE streaming (one JSON event per audio chunk):

```bash
# -N disables output buffering so events print as they arrive
curl -N -s -X POST http://localhost:8090/v1/audio/transcriptions/stream \
  -F "file=@audio.mp3" \
  -F "model=parakeet-tdt-0.6b-v3"
# Each line: data: {"text":"...","chunk_index":0,"total_chunks":3,"start":0.0,"end":8.4,"final":false}
# Last line:  data: {"text":"...","chunk_index":2,"total_chunks":3,"start":16.1,"end":22.3,"final":true}
```

---

## Folder watcher & persistent history

The server can watch folders and transcribe audio files as they land, saving every
result under a data home (`$RAS_HOME`, default `~/.ras`) that the `/v1/history`
endpoints expose.

```bash
# transcribe every .ogg (WhatsApp voice notes) that appears in ~/Downloads
parakeet-local-asr-rust serve --watch ~/Downloads --watch-ext .ogg
```

| Flag | Env | Meaning |
|------|-----|---------|
| `--watch <dir>` (repeatable) | `ASR_WATCH_DIRS` (comma-separated) | folders to watch, non-recursive |
| `--watch-ext <ext>` (repeatable) | `ASR_WATCH_EXTS` (comma-separated) | extensions to pick up, default `.ogg` |
| `--no-notify` | `ASR_NO_NOTIFY=1` | no desktop notification per transcript |
| — | `RAS_HOME` | data home, default `~/.ras` |
| — | `ASR_NO_HISTORY=1` | do not persist anything |

```text
$RAS_HOME/
├── models/                model cache (unless MODELS_DIR is set)
├── transcripts/<uuid>.json + <uuid>.txt
├── audio/<uuid>-<name>    the original audio, verbatim
└── watcher_state.json     dedup state (size + mtime per path)
```

In-progress downloads (`.crdownload`, `.part`, `.download`, `.tmp`) and dotfiles are
ignored, and a file is only read once its size has stopped changing — so the rename a
browser performs at the end of a download is what triggers the transcription. On
macOS the notification copies the transcript to the clipboard (on click with
`terminal-notifier` installed, immediately otherwise); on Linux via `notify-send` +
`xclip`.

Recordings are saved for successful API transcriptions and for every watched file
(including failures, with `"status": "failed"`). The SSE streaming endpoint does not
persist.

```bash
# list the most recent recordings (segments omitted; fetch one by id for those)
curl -s "http://localhost:8090/v1/history?limit=20" | jq '.items[] | {id, name, source, status, duration}'

# one record, with timestamps
curl -s http://localhost:8090/v1/history/<id> | jq '.segments[] | {start, end, text}'

# subtitles / plain text as a download, and the original audio
curl -s -OJ "http://localhost:8090/v1/history/<id>/download?format=srt"
curl -s http://localhost:8090/v1/history/<id>/audio -o voice-note.ogg

curl -s -X DELETE http://localhost:8090/v1/history/<id>      # {"deleted":true}

# watcher status
curl -s http://localhost:8090/v1/watcher | jq
# {"enabled":true,"dirs":["/Users/me/Downloads"],"exts":[".ogg"],"started_at_ms":...,
#  "files_seen":3,"files_processed":3,
#  "last_file":{"name":"voice.ogg","history_id":"<uuid>","at_ms":...}}
```

A `HistoryRecord` is:

```json
{
  "id": "<uuid>",
  "name": "voice note.ogg",
  "source": "api | api-async | watcher",
  "created_at_ms": 1730000000000,
  "duration": 14.4,
  "status": "done | failed",
  "error": null,
  "text": "...",
  "segments": [{ "id": 0, "start": 0.0, "end": 3.7, "text": "..." }],
  "audio_file": "<uuid>-voice_note.ogg"
}
```

---

## OpenAI SDK — Python

> Shows basic transcription plus segment iteration with `verbose_json`.

```python
# pip install openai
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8090/v1",
    api_key="not-needed",  # required by the SDK; ignored by the server
)

# --- plain text result ---
with open("audio.mp3", "rb") as f:
    result = client.audio.transcriptions.create(
        model="parakeet-tdt-0.6b-v3",
        file=f,
        response_format="json",
    )
print(result.text)

# --- timestamped segments (verbose_json) ---
with open("audio.mp3", "rb") as f:
    result = client.audio.transcriptions.create(
        model="parakeet-tdt-0.6b-v3",
        file=f,
        response_format="verbose_json",
    )
print(f"duration: {result.duration:.1f}s")
for seg in result.segments:
    print(f"[{seg.start:.1f}-{seg.end:.1f}] {seg.text}")
```

---

## OpenAI SDK — JS/TS

> Drop-in: only `baseURL` differs from the official OpenAI endpoint.

```ts
// npm install openai
import OpenAI from "openai";
import fs from "fs";

const client = new OpenAI({
  baseURL: "http://localhost:8090/v1",
  apiKey: "not-needed", // required by the SDK; ignored by the server
});

const result = await client.audio.transcriptions.create({
  model: "parakeet-tdt-0.6b-v3",
  file: fs.createReadStream("audio.mp3"),
  response_format: "json",
});

console.log(result.text);
```

---

## LangChain (Python)

> Transcribes with the OpenAI client pointed at this server, then feeds the text into
> a LangChain chain. The ASR server is the transcription backend; `ChatOpenAI` (or any
> OpenAI-compatible chat model) handles reasoning separately.

```python
# pip install langchain langchain-openai openai
from openai import OpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

asr = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")


def transcribe(path: str) -> str:
    with open(path, "rb") as f:
        return asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3",
            file=f,
            response_format="text",
        )


transcript = transcribe("audio.mp3")

chain = (
    ChatPromptTemplate.from_template("Extract action items from this transcript:\n\n{transcript}")
    | ChatOpenAI(model="gpt-4o-mini")   # point base_url here too for a local LLM
    | StrOutputParser()
)
print(chain.invoke({"transcript": transcript}))
```

Expose transcription as a **LangChain tool** for use inside an agent:

```python
from langchain_core.tools import tool

@tool
def transcribe_audio(path: str) -> str:
    """Transcribe a local audio file (mp3/wav/ogg/m4a/flac/...) to text."""
    with open(path, "rb") as f:
        return asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3",
            file=f,
            response_format="text",
        )
```

---

## LangGraph (Python)

> A `StateGraph` with three typed fields: `audio_path` → `transcribe` node (calls this
> server) → `summarize` node (any LLM) → done. Shows the full wiring pattern.

```python
# pip install langgraph langchain-openai openai
from typing import Optional
from typing_extensions import TypedDict

from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langgraph.graph import START, END, StateGraph

asr = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")
llm = ChatOpenAI(model="gpt-4o-mini")  # replace with any chat-compatible LLM


# --- State definition ---

class PipelineState(TypedDict):
    audio_path: str
    transcript: Optional[str]
    summary: Optional[str]


# --- Nodes ---

def transcribe(state: PipelineState) -> dict:
    """Call the Rust ASR server and store the full transcript."""
    with open(state["audio_path"], "rb") as f:
        text = asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3",
            file=f,
            response_format="text",
        )
    return {"transcript": text}


def summarize(state: PipelineState) -> dict:
    """Summarize the transcript with an LLM."""
    chain = (
        ChatPromptTemplate.from_template("Summarize the following transcript in 3 bullet points:\n\n{transcript}")
        | llm
        | StrOutputParser()
    )
    summary = chain.invoke({"transcript": state["transcript"]})
    return {"summary": summary}


# --- Graph assembly ---

graph = StateGraph(PipelineState)
graph.add_node("transcribe", transcribe)
graph.add_node("summarize", summarize)

graph.add_edge(START, "transcribe")
graph.add_edge("transcribe", "summarize")
graph.add_edge("summarize", END)

pipeline = graph.compile()

# --- Run ---
result = pipeline.invoke({"audio_path": "audio.mp3"})
print(result["transcript"])
print(result["summary"])
```

---

## Agno

> `OpenAILike` is for **chat** models, not audio. The correct pattern is to expose
> transcription as a plain Python **tool** that the agent can call; the agent's own
> reasoning model can be anything (OpenAI, a local OpenAI-compatible server, etc.).

```python
# pip install agno openai
from openai import OpenAI
from agno.agent import Agent
from agno.models.openai.like import OpenAILike
from agno.tools import tool

asr = OpenAI(base_url="http://localhost:8090/v1", api_key="not-needed")


@tool
def transcribe_audio(path: str) -> str:
    """Transcribe a local audio file to text.

    Args:
        path (str): Path to an audio file (mp3, wav, ogg/opus, m4a, flac, webm).
    """
    with open(path, "rb") as f:
        return asr.audio.transcriptions.create(
            model="parakeet-tdt-0.6b-v3",
            file=f,
            response_format="text",
        )


agent = Agent(
    # The reasoning model is a separate chat LLM — point it wherever your chat model lives.
    # OpenAILike works for any OpenAI-compatible *chat* endpoint; use OpenAI(...) for the real API.
    model=OpenAILike(
        id="gpt-4o-mini",
        base_url="https://api.openai.com/v1",
        api_key="sk-...",
    ),
    tools=[transcribe_audio],
    markdown=True,
)

agent.print_response("Transcribe ./audio.mp3 and list the key topics mentioned.")
```

---

## SSE Streaming (Python)

> Receives partial results as each audio chunk is processed — useful for long files or
> progress UIs. Uses `httpx` for streaming HTTP.

```python
# pip install httpx
import json
import httpx

with httpx.stream(
    "POST",
    "http://localhost:8090/v1/audio/transcriptions/stream",
    files={"file": open("audio.mp3", "rb")},
    data={"model": "parakeet-tdt-0.6b-v3"},
    timeout=None,
) as r:
    for line in r.iter_lines():
        if line.startswith("data:"):
            ev = json.loads(line[5:])
            print(f"[{ev['chunk_index']}/{ev['total_chunks']}] "
                  f"{ev['start']:.1f}s–{ev['end']:.1f}s  {ev['text']}")
            if ev.get("final"):
                break
```

---

## Async Jobs (Python)

> Submit and poll — avoids HTTP timeouts on very long files (30+ min recordings).

```python
# pip install httpx
import time
import httpx

BASE = "http://localhost:8090"

# Submit
resp = httpx.post(
    f"{BASE}/v1/audio/transcriptions/async",
    files={"file": open("audio.mp3", "rb")},
    data={"model": "parakeet-tdt-0.6b-v3"},
)
resp.raise_for_status()
job = resp.json()          # {"job_id": "<uuid>", "status": "queued"}
job_id = job["job_id"]
print(f"queued as {job_id}")

# Poll
while True:
    r = httpx.get(f"{BASE}/v1/audio/jobs/{job_id}").json()
    print(f"status: {r['status']}")
    if r["status"] == "done":
        # result has the same shape as verbose_json
        print(r["result"])
        break
    if r["status"] == "failed":
        raise RuntimeError(r.get("error", "transcription failed"))
    time.sleep(3)
```
