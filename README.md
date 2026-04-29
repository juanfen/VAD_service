# xiaozhi-vad-service

Silero VAD standalone service with:

- external protocol over `WebSocket`
- internal inference fan-out over `multiprocessing` IPC
- session-affinity routing for better concurrency behavior

This keeps connection handling in the FastAPI process while moving decode and VAD inference work into dedicated worker processes.

## Endpoints

### WebSocket

- `GET /ws/v1/vad`

Query parameters:

- `session_id` (required)
- `threshold`
- `threshold_low`
- `min_silence_duration_ms`
- `frame_window_threshold`

Protocol:

1. The client streams binary frames.
2. Each binary frame is one Opus packet, matching the original HTTP infer input mode.
3. The server replies with the same VAD JSON fields returned by `POST /v1/vad/infer`.
4. Optional text commands:
   - `{"type":"ping"}`
   - `{"type":"delete_session"}`

The websocket path changes the transport only. The per-packet payload and inference response shape stay aligned with the original HTTP behavior.

### HTTP compatibility

The service still keeps:

- `GET /health`
- `POST /v1/vad/infer`
- `DELETE /v1/vad/sessions/{session_id}`

`POST /v1/vad/infer` now uses the same IPC worker pool as the WebSocket path.

## Concurrency model

1. FastAPI accepts WebSocket and HTTP traffic.
2. A stable hash of `session_id` pins the same session to the same worker.
3. Each worker owns its model, decoder, and in-memory session state.
4. The main process talks to workers through `multiprocessing.Queue`.

This reduces pressure on a single Python process in high-concurrency scenarios.
To reduce queue churn under bursty websocket traffic, concurrent `infer` calls bound for the same worker are also micro-batched before crossing the IPC boundary.

## Environment variables

- `VAD_HOST`: default `0.0.0.0`
- `VAD_PORT`: default `8866`
- `VAD_MODEL_DIR`
- `VAD_SESSION_TTL_SECONDS`: default `300`
- `VAD_SESSION_CLEANUP_INTERVAL_SECONDS`: default `60`
- `VAD_DEFAULT_THRESHOLD`: default `0.5`
- `VAD_DEFAULT_THRESHOLD_LOW`: default `0.3`
- `VAD_DEFAULT_MIN_SILENCE_DURATION_MS`: default `200`
- `VAD_DEFAULT_FRAME_WINDOW_THRESHOLD`: default `3`
- `VAD_API_KEY`: optional Bearer token
- `VAD_WORKER_COUNT`: default `min(CPU count, 4)`
- `VAD_WORKER_REQUEST_TIMEOUT_SECONDS`: default `10`
- `VAD_TORCH_NUM_THREADS`: default `1`
- `VAD_IPC_BATCH_MAX_SIZE`: default `16`
- `VAD_IPC_BATCH_WAIT_MS`: default `1.5`

## Start with Docker Compose

```bash
docker compose up --build -d
```

Health check:

```bash
curl http://127.0.0.1:8866/health
```

## Start locally

PowerShell:

```powershell
pip install -r requirements.txt
$env:VAD_MODEL_DIR=".\snakers4_silero-vad"
python app.py
```

Linux or macOS:

```bash
pip install -r requirements.txt
env VAD_MODEL_DIR=./snakers4_silero-vad python app.py
```

## Go WebSocket tester

A ready-to-run Go tester is included:

- [tools/go-vad-ws-tester/main.go](/C:/Users/admin/Desktop/work/xiaozhi-vad-service/tools/go-vad-ws-tester/main.go)

It can drive both the legacy HTTP endpoint and the new WebSocket endpoint while preserving the original `Opus packet` payload mode.

It supports:

- `single` mode for functional verification
- `load` mode for concurrent websocket pressure testing
- `ws` transport for the new websocket service
- `http` transport for the old HTTP service or the compatibility endpoint

### WAV requirements

- 16kHz
- mono
- 16-bit PCM
- `.wav`

### Install Go if needed

1. Open [go.dev/dl](https://go.dev/dl/)
2. Install Go for your platform
3. Re-open your shell
4. Verify with:

```powershell
go version
```

### Run the tester

```bash
cd tools/go-vad-ws-tester
go mod tidy
go run . -wav D:\audio\test.wav
```

Common examples:

```bash
go run . -url ws://192.168.1.20:8866/ws/v1/vad -wav D:\audio\test.wav
go run . -wav D:\audio\test.wav -api-key your_api_key
go run . -wav D:\audio\test.wav -verbose
go run . -wav D:\audio\test.wav -realtime=false
```

Legacy HTTP single-session example:

```bash
go run . -transport http -url http://127.0.0.1:8766 -wav D:\audio\test.wav
```

### Run a load test

Example with 50 concurrent websocket sessions and 5 sessions per goroutine:

```bash
go run . -mode load -wav D:\audio\test.wav -concurrency 50 -iterations 5 -realtime=false
```

Example with time-based load for 60 seconds:

```bash
go run . -mode load -wav D:\audio\test.wav -concurrency 100 -duration 60s -realtime=false
```

Load mode prints:

- progress snapshots while running
- session success and failure counts
- frame success and failure counts
- session throughput
- frame throughput
- latency `avg`, `p50`, `p95`, `p99`, `max`

Legacy HTTP load test example:

```bash
go run . -mode load -transport http -url http://127.0.0.1:8766 -wav D:\audio\test.wav -concurrency 50 -duration 60s -realtime=false
```

Notes:

- `transport=ws` defaults to `audio-format=opus`
- `transport=http` defaults to `audio-format=opus`
- `transport=ws` is meant to preserve the original Opus packet mode from the HTTP service

## Example WebSocket URLs

Opus:

```text
ws://127.0.0.1:8866/ws/v1/vad?session_id=test-002&audio_format=opus
```
