import asyncio
import hashlib
import json
import multiprocessing as mp
import os
import queue
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import numpy as np
import opuslib_next
import torch
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from starlette import status

MODEL_DIR = os.getenv("VAD_MODEL_DIR", "/app/models/snakers4_silero-vad")
SERVICE_HOST = os.getenv("VAD_HOST", "0.0.0.0")
SERVICE_PORT = int(os.getenv("VAD_PORT", "8866"))
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
DEFAULT_WORKER_COUNT = max(1, min(os.cpu_count() or 1, 4))
WORKER_COUNT = int(os.getenv("VAD_WORKER_COUNT", str(DEFAULT_WORKER_COUNT)))
WORKER_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("VAD_WORKER_REQUEST_TIMEOUT_SECONDS", "10")
)
TORCH_NUM_THREADS = max(int(os.getenv("VAD_TORCH_NUM_THREADS", "1")), 1)
IPC_BATCH_MAX_SIZE = max(int(os.getenv("VAD_IPC_BATCH_MAX_SIZE", "16")), 1)
IPC_BATCH_WAIT_MS = max(float(os.getenv("VAD_IPC_BATCH_WAIT_MS", "1.5")), 0.0)
PCM_SAMPLE_RATE = 16000
PCM_FRAME_SIZE = 512
PCM_SAMPLE_WIDTH_BYTES = 2
PCM_CHUNK_BYTES = PCM_FRAME_SIZE * PCM_SAMPLE_WIDTH_BYTES
SUPPORTED_AUDIO_FORMATS = {"opus", "pcm_s16le"}

BACKEND: Optional["VADBackend"] = None
WORKER_MODEL = None


@dataclass
class SessionState:
    audio_format: str
    decoder: Optional[opuslib_next.Decoder] = None
    audio_buffer: bytearray = field(default_factory=bytearray)
    voice_window: deque = field(default_factory=lambda: deque(maxlen=5))
    client_have_voice: bool = False
    last_is_voice: bool = False
    last_activity_time_ms: float = 0.0
    last_access_time: float = field(default_factory=time.monotonic)


class WorkerRequestError(Exception):
    def __init__(self, message: str, error_type: str = "WorkerError") -> None:
        super().__init__(message)
        self.message = message
        self.error_type = error_type


class BackendUnavailableError(RuntimeError):
    pass


def verify_api_key(authorization: Optional[str]) -> None:
    if not API_KEY:
        return
    expected = f"Bearer {API_KEY}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="invalid api key")


def validate_audio_format(audio_format: str) -> str:
    normalized = audio_format.strip().lower()
    if normalized not in SUPPORTED_AUDIO_FORMATS:
        supported = ", ".join(sorted(SUPPORTED_AUDIO_FORMATS))
        raise ValueError(f"unsupported audio_format: {audio_format}, supported: {supported}")
    return normalized


def stable_worker_index(session_id: str, worker_count: int) -> int:
    digest = hashlib.sha1(session_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % worker_count


def load_worker_model():
    global WORKER_MODEL
    if WORKER_MODEL is not None:
        return WORKER_MODEL

    torch.set_num_threads(TORCH_NUM_THREADS)
    model, _ = torch.hub.load(
        repo_or_dir=MODEL_DIR,
        source="local",
        model="silero_vad",
        force_reload=False,
    )
    model.eval()
    WORKER_MODEL = model
    return WORKER_MODEL


def cleanup_expired_sessions(session_store: Dict[str, SessionState]) -> int:
    now = time.monotonic()
    expired_ids = [
        session_id
        for session_id, session in session_store.items()
        if now - session.last_access_time > SESSION_TTL_SECONDS
    ]
    for session_id in expired_ids:
        session_store.pop(session_id, None)
    return len(expired_ids)


def ensure_voice_window_capacity(session: SessionState, frame_window_threshold: int) -> None:
    if session.voice_window.maxlen >= frame_window_threshold:
        return
    session.voice_window = deque(
        session.voice_window, maxlen=max(5, frame_window_threshold)
    )


def get_or_create_worker_session(
    session_store: Dict[str, SessionState],
    session_id: str,
    audio_format: str,
) -> SessionState:
    session = session_store.get(session_id)
    if session is None:
        decoder = opuslib_next.Decoder(PCM_SAMPLE_RATE, 1) if audio_format == "opus" else None
        session = SessionState(audio_format=audio_format, decoder=decoder)
        session_store[session_id] = session
    elif session.audio_format != audio_format:
        raise ValueError(
            f"session {session_id} already uses audio_format={session.audio_format}, "
            f"cannot switch to {audio_format}"
        )

    session.last_access_time = time.monotonic()
    return session


def append_audio_bytes(session: SessionState, audio_chunk: bytes) -> None:
    if session.audio_format == "opus":
        if session.decoder is None:
            session.decoder = opuslib_next.Decoder(PCM_SAMPLE_RATE, 1)
        pcm_frame = session.decoder.decode(audio_chunk, 960)
        session.audio_buffer.extend(pcm_frame)
        return

    session.audio_buffer.extend(audio_chunk)


def infer_audio_chunk(
    session: SessionState,
    audio_chunk: bytes,
    threshold: float,
    threshold_low: float,
    min_silence_duration_ms: int,
    frame_window_threshold: int,
) -> dict[str, Any]:
    model = load_worker_model()
    append_audio_bytes(session, audio_chunk)
    ensure_voice_window_capacity(session, frame_window_threshold)

    have_voice = False
    client_voice_stop = False
    processed_chunks = 0

    while len(session.audio_buffer) >= PCM_CHUNK_BYTES:
        chunk = session.audio_buffer[:PCM_CHUNK_BYTES]
        del session.audio_buffer[:PCM_CHUNK_BYTES]
        processed_chunks += 1

        audio_int16 = np.frombuffer(chunk, dtype=np.int16)
        audio_float32 = audio_int16.astype(np.float32) / 32768.0
        audio_tensor = torch.from_numpy(audio_float32)

        with torch.inference_mode():
            speech_prob = model(audio_tensor, PCM_SAMPLE_RATE).item()

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
        "processed_chunks": processed_chunks,
        "pending_buffer_bytes": len(session.audio_buffer),
    }


def execute_worker_request(
    session_store: Dict[str, SessionState],
    request: Dict[str, Any],
    worker_id: int,
) -> Dict[str, Any]:
    request_type = request.get("type")
    if request_type == "infer":
        session_id = request["session_id"]
        audio_format = validate_audio_format(request["audio_format"])
        session = get_or_create_worker_session(
            session_store=session_store,
            session_id=session_id,
            audio_format=audio_format,
        )
        return infer_audio_chunk(
            session=session,
            audio_chunk=request["payload"],
            threshold=request["threshold"],
            threshold_low=request["threshold_low"],
            min_silence_duration_ms=request["min_silence_duration_ms"],
            frame_window_threshold=request["frame_window_threshold"],
        )
    if request_type == "delete_session":
        deleted = session_store.pop(request["session_id"], None) is not None
        return {"deleted": deleted, "session_id": request["session_id"]}
    if request_type == "health":
        expired = cleanup_expired_sessions(session_store)
        return {
            "worker_id": worker_id,
            "session_count": len(session_store),
            "expired_sessions": expired,
        }
    raise ValueError(f"unsupported worker request type: {request_type}")


def build_worker_response(request_id: Optional[str], result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "request_id": request_id,
        "ok": True,
        "result": result,
    }


def build_worker_error_response(
    request_id: Optional[str],
    exc: Exception,
) -> Dict[str, Any]:
    return {
        "request_id": request_id,
        "ok": False,
        "error": str(exc),
        "error_type": exc.__class__.__name__,
    }


def dispatch_worker_request(
    session_store: Dict[str, SessionState],
    request: Dict[str, Any],
    worker_id: int,
) -> Dict[str, Any]:
    request_id = request.get("request_id")
    try:
        result = execute_worker_request(
            session_store=session_store,
            request=request,
            worker_id=worker_id,
        )
        return build_worker_response(request_id, result)
    except Exception as exc:
        return build_worker_error_response(request_id, exc)


def worker_main(
    worker_id: int,
    request_queue: mp.Queue,
    response_queue: mp.Queue,
) -> None:
    session_store: Dict[str, SessionState] = {}
    load_worker_model()

    while True:
        try:
            request = request_queue.get(timeout=SESSION_CLEANUP_INTERVAL_SECONDS)
        except queue.Empty:
            cleanup_expired_sessions(session_store)
            continue
        except (EOFError, OSError):
            break

        if request is None:
            break

        request_id = request.get("request_id")
        request_type = request.get("type")

        if request_type == "shutdown":
            break

        if request_type == "infer_batch":
            responses = [
                dispatch_worker_request(
                    session_store=session_store,
                    request=batch_request,
                    worker_id=worker_id,
                )
                for batch_request in request.get("requests", [])
            ]
            response_queue.put({"type": "batch", "responses": responses})
            continue

        response_queue.put(
            dispatch_worker_request(
                session_store=session_store,
                request=request,
                worker_id=worker_id,
            )
        )


class IPCWorkerClient:
    def __init__(
        self,
        worker_id: int,
        request_queue: mp.Queue,
        response_queue: mp.Queue,
        process: mp.Process,
    ) -> None:
        self.worker_id = worker_id
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.process = process
        self.pending: Dict[str, asyncio.Future] = {}
        self.pending_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.router_thread: Optional[threading.Thread] = None
        self.sender_thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.outbound_queue: queue.Queue = queue.Queue()
        self.deferred_queue: deque[Dict[str, Any]] = deque()

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.sender_thread = threading.Thread(
            target=self._send_requests,
            name=f"vad-worker-sender-{self.worker_id}",
            daemon=True,
        )
        self.router_thread = threading.Thread(
            target=self._route_responses,
            name=f"vad-worker-router-{self.worker_id}",
            daemon=True,
        )
        self.sender_thread.start()
        self.router_thread.start()

    def _next_outbound_request(self, timeout: float = 0.5) -> Optional[Dict[str, Any]]:
        if self.deferred_queue:
            return self.deferred_queue.popleft()

        try:
            request = self.outbound_queue.get(timeout=timeout)
        except queue.Empty:
            return None

        if request is None:
            self.stop_event.set()
            return None
        return request

    def _send_requests(self) -> None:
        batch_wait_seconds = IPC_BATCH_WAIT_MS / 1000.0
        while not self.stop_event.is_set():
            request = self._next_outbound_request()
            if request is None:
                continue

            if request.get("type") != "infer" or IPC_BATCH_MAX_SIZE <= 1:
                self.request_queue.put(request)
                continue

            batch = [request]
            deadline = time.perf_counter() + batch_wait_seconds

            while len(batch) < IPC_BATCH_MAX_SIZE:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break

                next_request = self._next_outbound_request(timeout=remaining)
                if next_request is None:
                    break

                if next_request.get("type") != "infer":
                    self.deferred_queue.appendleft(next_request)
                    break

                batch.append(next_request)

            if len(batch) == 1:
                self.request_queue.put(batch[0])
                continue

            self.request_queue.put({"type": "infer_batch", "requests": batch})

    def _deliver_response(self, response: Dict[str, Any]) -> None:
        request_id = response.get("request_id")
        if not request_id or self.loop is None:
            return

        with self.pending_lock:
            future = self.pending.pop(request_id, None)

        if future is None:
            return

        def deliver() -> None:
            if future.cancelled() or future.done():
                return
            if response.get("ok"):
                future.set_result(response.get("result"))
                return

            future.set_exception(
                WorkerRequestError(
                    response.get("error", "worker request failed"),
                    response.get("error_type", "WorkerError"),
                )
            )

        self.loop.call_soon_threadsafe(deliver)

    def _route_responses(self) -> None:
        while not self.stop_event.is_set():
            try:
                response = self.response_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            except (EOFError, OSError):
                break

            if response.get("type") == "batch":
                for item in response.get("responses", []):
                    self._deliver_response(item)
                continue

            self._deliver_response(response)

    async def submit(self, payload: Dict[str, Any], timeout: float) -> Dict[str, Any]:
        if self.loop is None:
            raise BackendUnavailableError("worker loop is not running")

        request_id = uuid.uuid4().hex
        future = self.loop.create_future()

        with self.pending_lock:
            self.pending[request_id] = future

        payload = dict(payload)
        payload["request_id"] = request_id
        self.outbound_queue.put(payload)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            with self.pending_lock:
                self.pending.pop(request_id, None)
            future.cancel()
            raise

    def shutdown(self) -> None:
        self.stop_event.set()
        with suppress(Exception):
            self.request_queue.put({"type": "shutdown"})
        with suppress(Exception):
            self.request_queue.put(None)
        with self.pending_lock:
            pending = list(self.pending.values())
            self.pending.clear()

        for future in pending:
            if future.cancelled() or future.done():
                continue
            if self.loop is None:
                continue
            def deliver_shutdown_error(target_future: asyncio.Future = future) -> None:
                if target_future.cancelled() or target_future.done():
                    return
                target_future.set_exception(
                    BackendUnavailableError("worker is shutting down")
                )

            self.loop.call_soon_threadsafe(deliver_shutdown_error)

        if self.router_thread is not None:
            self.router_thread.join(timeout=2)
        if self.sender_thread is not None:
            self.outbound_queue.put(None)
            self.sender_thread.join(timeout=2)

        self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=2)


class VADBackend:
    def __init__(self, worker_count: int) -> None:
        self.worker_count = max(worker_count, 1)
        self.ctx = mp.get_context("spawn")
        self.workers: list[IPCWorkerClient] = []

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        for worker_id in range(self.worker_count):
            request_queue = self.ctx.Queue()
            response_queue = self.ctx.Queue()
            process = self.ctx.Process(
                target=worker_main,
                args=(worker_id, request_queue, response_queue),
                daemon=True,
            )
            process.start()

            worker = IPCWorkerClient(
                worker_id=worker_id,
                request_queue=request_queue,
                response_queue=response_queue,
                process=process,
            )
            worker.start(loop)
            self.workers.append(worker)

        await self.health()

    async def shutdown(self) -> None:
        for worker in self.workers:
            worker.shutdown()
        self.workers.clear()

    def select_worker(self, session_id: str) -> IPCWorkerClient:
        if not self.workers:
            raise BackendUnavailableError("vad backend is not initialized")
        worker_index = stable_worker_index(session_id, len(self.workers))
        return self.workers[worker_index]

    async def infer(
        self,
        session_id: str,
        audio_format: str,
        payload: bytes,
        threshold: float,
        threshold_low: float,
        min_silence_duration_ms: int,
        frame_window_threshold: int,
    ) -> Dict[str, Any]:
        worker = self.select_worker(session_id)
        return await worker.submit(
            {
                "type": "infer",
                "session_id": session_id,
                "audio_format": audio_format,
                "payload": payload,
                "threshold": threshold,
                "threshold_low": threshold_low,
                "min_silence_duration_ms": min_silence_duration_ms,
                "frame_window_threshold": frame_window_threshold,
            },
            timeout=WORKER_REQUEST_TIMEOUT_SECONDS,
        )

    async def delete_session(self, session_id: str) -> Dict[str, Any]:
        worker = self.select_worker(session_id)
        return await worker.submit(
            {
                "type": "delete_session",
                "session_id": session_id,
            },
            timeout=WORKER_REQUEST_TIMEOUT_SECONDS,
        )

    async def health(self) -> Dict[str, Any]:
        if not self.workers:
            raise BackendUnavailableError("vad backend is not initialized")

        worker_reports = await asyncio.gather(
            *[
                worker.submit({"type": "health"}, timeout=WORKER_REQUEST_TIMEOUT_SECONDS)
                for worker in self.workers
            ]
        )

        total_sessions = sum(report["session_count"] for report in worker_reports)
        return {
            "status": "healthy",
            "model_dir": MODEL_DIR,
            "worker_count": len(self.workers),
            "torch_num_threads": TORCH_NUM_THREADS,
            "ipc_batch_max_size": IPC_BATCH_MAX_SIZE,
            "ipc_batch_wait_ms": IPC_BATCH_WAIT_MS,
            "total_sessions": total_sessions,
            "workers": worker_reports,
        }


def get_backend() -> VADBackend:
    if BACKEND is None:
        raise BackendUnavailableError("vad backend is not initialized")
    return BACKEND


def to_http_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, asyncio.TimeoutError):
        return HTTPException(status_code=504, detail="vad worker timeout")
    if isinstance(exc, WorkerRequestError):
        if exc.error_type in {"ValueError", "OpusError"}:
            return HTTPException(status_code=400, detail=exc.message)
        return HTTPException(status_code=500, detail=exc.message)
    if isinstance(exc, BackendUnavailableError):
        return HTTPException(status_code=503, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global BACKEND
    BACKEND = VADBackend(worker_count=WORKER_COUNT)
    await BACKEND.start()
    try:
        yield
    finally:
        if BACKEND is not None:
            await BACKEND.shutdown()
        BACKEND = None


app = FastAPI(title="xiaozhi-vad-service", version="2.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    backend = get_backend()
    return await backend.health()


@app.post("/v1/vad/infer")
async def infer(
    request: Request,
    session_id: str,
    threshold: float = DEFAULT_THRESHOLD,
    threshold_low: float = DEFAULT_THRESHOLD_LOW,
    min_silence_duration_ms: int = DEFAULT_MIN_SILENCE_DURATION_MS,
    frame_window_threshold: int = DEFAULT_FRAME_WINDOW_THRESHOLD,
    audio_format: str = "opus",
    authorization: Optional[str] = Header(default=None),
):
    verify_api_key(authorization)
    try:
        audio_format = validate_audio_format(audio_format)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = await request.body()
    if not payload:
        raise HTTPException(status_code=400, detail="empty audio payload")

    try:
        backend = get_backend()
        return await backend.infer(
            session_id=session_id,
            audio_format=audio_format,
            payload=payload,
            threshold=threshold,
            threshold_low=threshold_low,
            min_silence_duration_ms=min_silence_duration_ms,
            frame_window_threshold=frame_window_threshold,
        )
    except Exception as exc:
        raise to_http_exception(exc) from exc


@app.websocket("/ws/v1/vad")
async def vad_websocket(
    websocket: WebSocket,
    session_id: str,
    threshold: float = DEFAULT_THRESHOLD,
    threshold_low: float = DEFAULT_THRESHOLD_LOW,
    min_silence_duration_ms: int = DEFAULT_MIN_SILENCE_DURATION_MS,
    frame_window_threshold: int = DEFAULT_FRAME_WINDOW_THRESHOLD,
):
    audio_format = "opus"
    try:
        verify_api_key(websocket.headers.get("authorization"))
        validate_audio_format(audio_format)
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="invalid api key")
        return

    await websocket.accept()
    backend = get_backend()

    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break

            if message.get("bytes") is not None:
                payload = message["bytes"]
                if not payload:
                    await websocket.send_json({"detail": "empty audio payload"})
                    continue

                try:
                    result = await backend.infer(
                        session_id=session_id,
                        audio_format=audio_format,
                        payload=payload,
                        threshold=threshold,
                        threshold_low=threshold_low,
                        min_silence_duration_ms=min_silence_duration_ms,
                        frame_window_threshold=frame_window_threshold,
                    )
                except Exception as exc:
                    error = to_http_exception(exc)
                    await websocket.send_json({"detail": error.detail})
                    continue

                await websocket.send_json(result)
                continue

            if message.get("text") is None:
                continue

            try:
                command = json.loads(message["text"])
            except json.JSONDecodeError:
                await websocket.send_json(
                    {"detail": "websocket text frame must be valid JSON"}
                )
                continue

            command_type = command.get("type")
            if command_type == "ping":
                await websocket.send_json({"pong": True})
                continue

            if command_type == "delete_session":
                result = await backend.delete_session(session_id)
                await websocket.send_json(result)
                break

            await websocket.send_json(
                {"detail": f"unsupported command type: {command_type}"}
            )
    except WebSocketDisconnect:
        pass
    finally:
        with suppress(Exception):
            await backend.delete_session(session_id)
        with suppress(Exception):
            await websocket.close()


@app.delete("/v1/vad/sessions/{session_id}")
async def delete_vad_session(
    session_id: str,
    authorization: Optional[str] = Header(default=None),
):
    verify_api_key(authorization)
    try:
        backend = get_backend()
        return await backend.delete_session(session_id)
    except Exception as exc:
        raise to_http_exception(exc) from exc


if __name__ == "__main__":
    uvicorn.run("app:app", host=SERVICE_HOST, port=SERVICE_PORT, reload=False)
