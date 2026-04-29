import asyncio
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import opuslib_next
import torch
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request

MODEL_DIR = os.getenv("VAD_MODEL_DIR", "/app/models/snakers4_silero-vad")
SERVICE_HOST = os.getenv("VAD_HOST", "0.0.0.0")
SERVICE_PORT = int(os.getenv("VAD_PORT", "8766"))
SESSION_TTL_SECONDS = int(os.getenv("VAD_SESSION_TTL_SECONDS", "300"))
SESSION_CLEANUP_INTERVAL_SECONDS = int(
    os.getenv("VAD_SESSION_CLEANUP_INTERVAL_SECONDS", "60")
)
API_KEY = os.getenv("VAD_API_KEY", "").strip()
DEFAULT_THRESHOLD = float(os.getenv("VAD_DEFAULT_THRESHOLD", "0.5"))
DEFAULT_THRESHOLD_LOW = float(os.getenv("VAD_DEFAULT_THRESHOLD_LOW", "0.3"))
DEFAULT_MIN_SILENCE_DURATION_MS = int(
    os.getenv("VAD_DEFAULT_MIN_SILENCE_DURATION_MS", "200")
)
DEFAULT_FRAME_WINDOW_THRESHOLD = int(
    os.getenv("VAD_DEFAULT_FRAME_WINDOW_THRESHOLD", "3")
)
TORCH_NUM_THREADS = os.getenv("VAD_TORCH_NUM_THREADS")

if TORCH_NUM_THREADS:
    torch.set_num_threads(max(int(TORCH_NUM_THREADS), 1))

MODEL = None
SESSION_STORE: Dict[str, "SessionState"] = {}
SESSION_STORE_LOCK = threading.Lock()


@dataclass
class SessionState:
    decoder: opuslib_next.Decoder = field(
        default_factory=lambda: opuslib_next.Decoder(16000, 1)
    )
    audio_buffer: bytearray = field(default_factory=bytearray)
    voice_window: deque = field(default_factory=lambda: deque(maxlen=5))
    client_have_voice: bool = False
    last_is_voice: bool = False
    last_activity_time_ms: float = 0.0
    last_access_time: float = field(default_factory=time.monotonic)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


def verify_api_key(authorization: Optional[str]) -> None:
    if not API_KEY:
        return
    expected = f"Bearer {API_KEY}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="invalid api key")


def load_model():
    global MODEL
    if MODEL is not None:
        return MODEL
    model, _ = torch.hub.load(
        repo_or_dir=MODEL_DIR,
        source="local",
        model="silero_vad",
        force_reload=False,
    )
    model.eval()
    MODEL = model
    return MODEL


def get_or_create_session(session_id: str) -> SessionState:
    with SESSION_STORE_LOCK:
        session = SESSION_STORE.get(session_id)
        if session is None:
            session = SessionState()
            SESSION_STORE[session_id] = session
        session.last_access_time = time.monotonic()
        return session


def delete_session(session_id: str) -> bool:
    with SESSION_STORE_LOCK:
        return SESSION_STORE.pop(session_id, None) is not None


def cleanup_expired_sessions() -> int:
    now = time.monotonic()
    expired_ids = []
    with SESSION_STORE_LOCK:
        for session_id, session in SESSION_STORE.items():
            if now - session.last_access_time > SESSION_TTL_SECONDS:
                expired_ids.append(session_id)
        for session_id in expired_ids:
            SESSION_STORE.pop(session_id, None)
    return len(expired_ids)


async def cleanup_loop():
    while True:
        await asyncio.sleep(SESSION_CLEANUP_INTERVAL_SECONDS)
        cleanup_expired_sessions()


def infer_packet(
    session: SessionState,
    opus_packet: bytes,
    threshold: float,
    threshold_low: float,
    min_silence_duration_ms: int,
    frame_window_threshold: int,
) -> dict:
    model = load_model()
    with session.lock:
        pcm_frame = session.decoder.decode(opus_packet, 960)
        session.audio_buffer.extend(pcm_frame)
        session.last_access_time = time.monotonic()

        have_voice = False
        client_voice_stop = False

        while len(session.audio_buffer) >= 512 * 2:
            chunk = session.audio_buffer[: 512 * 2]
            del session.audio_buffer[: 512 * 2]

            audio_int16 = np.frombuffer(chunk, dtype=np.int16)
            audio_float32 = audio_int16.astype(np.float32) / 32768.0
            audio_tensor = torch.from_numpy(audio_float32)

            with torch.no_grad():
                speech_prob = model(audio_tensor, 16000).item()

            if speech_prob >= threshold:
                is_voice = True
            elif speech_prob <= threshold_low:
                is_voice = False
            else:
                is_voice = session.last_is_voice

            session.last_is_voice = is_voice
            session.voice_window.append(is_voice)
            have_voice = session.voice_window.count(True) >= frame_window_threshold

            if session.client_have_voice and not have_voice:
                stop_duration = time.time() * 1000 - session.last_activity_time_ms
                if stop_duration >= min_silence_duration_ms:
                    client_voice_stop = True

            if have_voice:
                session.client_have_voice = True
                session.last_activity_time_ms = time.time() * 1000

        return {
            "have_voice": have_voice,
            "client_have_voice": session.client_have_voice,
            "client_voice_stop": client_voice_stop,
            "last_is_voice": session.last_is_voice,
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    task = asyncio.create_task(cleanup_loop())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="xiaozhi-vad-service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    load_model()
    with SESSION_STORE_LOCK:
        session_count = len(SESSION_STORE)
    return {
        "status": "healthy",
        "model_dir": MODEL_DIR,
        "session_count": session_count,
    }


@app.post("/v1/vad/infer")
async def infer(
    request: Request,
    session_id: str,
    threshold: float = DEFAULT_THRESHOLD,
    threshold_low: float = DEFAULT_THRESHOLD_LOW,
    min_silence_duration_ms: int = DEFAULT_MIN_SILENCE_DURATION_MS,
    frame_window_threshold: int = DEFAULT_FRAME_WINDOW_THRESHOLD,
    authorization: Optional[str] = Header(default=None),
):
    verify_api_key(authorization)
    opus_packet = await request.body()
    if not opus_packet:
        raise HTTPException(status_code=400, detail="empty audio packet")

    try:
        session = get_or_create_session(session_id)
        return infer_packet(
            session=session,
            opus_packet=opus_packet,
            threshold=threshold,
            threshold_low=threshold_low,
            min_silence_duration_ms=min_silence_duration_ms,
            frame_window_threshold=frame_window_threshold,
        )
    except opuslib_next.OpusError as exc:
        raise HTTPException(status_code=400, detail=f"opus decode failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"vad infer failed: {exc}") from exc


@app.delete("/v1/vad/sessions/{session_id}")
async def delete_vad_session(
    session_id: str,
    authorization: Optional[str] = Header(default=None),
):
    verify_api_key(authorization)
    deleted = delete_session(session_id)
    return {"deleted": deleted, "session_id": session_id}


if __name__ == "__main__":
    uvicorn.run("app:app", host=SERVICE_HOST, port=SERVICE_PORT, reload=False)
