# xiaozhi-vad-service

Silero VAD standalone HTTP service for `xiaozhi-server`.

## Endpoints

- `GET /health`
- `POST /v1/vad/infer`
- `DELETE /v1/vad/sessions/{session_id}`

The service keeps per-session VAD state in memory, so run a single replica unless you add sticky routing or external state.

## Start with Docker Compose

From this directory:

```bash
docker compose up --build -d
```

Health check:

```bash
curl http://192.168.99.103:8766/health
```

## Start with Docker

```bash
docker build -t xiaozhi-vad-service .
docker run -d \
  --name xiaozhi-vad-service \
  -p 8766:8766 \
  -e VAD_PORT=8766 \
  -e VAD_MODEL_DIR=/app/models/snakers4_silero-vad \
  -v ../xiaozhi-server/models/snakers4_silero-vad:/app/models/snakers4_silero-vad:ro \
  xiaozhi-vad-service
```

## Start locally

PowerShell:

```powershell
pip install -r requirements.txt
$env:VAD_MODEL_DIR="..\xiaozhi-server\models\snakers4_silero-vad"
python app.py
```

On Linux or macOS:

```bash
export VAD_MODEL_DIR=../xiaozhi-server/models/snakers4_silero-vad
python app.py
```

## xiaozhi-server config

Use the new VAD config block:

```yaml
selected_module:
  VAD: SileroVADService

VAD:
  SileroVADService:
    type: remote_http
    base_url: http://192.168.99.103:8766
    timeout_ms: 1500
    threshold: 0.5
    threshold_low: 0.3
    min_silence_duration_ms: 200
    frame_window_threshold: 3
    api_key: ""
```

If you use `manager-api`, run its latest migration and then select `SileroVAD 服务版` in the role or agent model configuration page.
