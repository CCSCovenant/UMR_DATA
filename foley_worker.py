#!/usr/bin/env python3
import argparse
import gc
import json
import os
import queue
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import parse


TASKS: Dict[str, Dict[str, Any]] = {}
TASKS_LOCK = threading.Lock()
TASK_QUEUE: "queue.Queue[str]" = queue.Queue()
ACTIVE_TASK_ID: Optional[str] = None
HTTP_SERVER: Optional[ThreadingHTTPServer] = None

ARGS: Any = None
DEVICE: Any = None
MODEL_DICT: Any = None
CFG: Any = None
MODEL_LOCK = threading.Lock()
IMPORTS_READY = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def stage_label_for(stage: str) -> str:
    mapping = {
        "queued": "等待 worker 调度",
        "loading_models": "加载 Foley 模型",
        "extracting_features": "提取视频/文本特征",
        "denoising": "扩散采样中",
        "saving_audio": "写出音频",
        "merging": "合并回视频",
        "completed": "已完成",
        "failed": "执行失败",
    }
    return mapping.get(stage, stage or "未知阶段")


def format_duration(seconds: float) -> str:
    if not seconds or seconds <= 0:
        return ""
    seconds = int(seconds)
    minutes, sec = divmod(seconds, 60)
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def read_json_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def update_task(task_id: str, **updates: Any) -> Dict[str, Any]:
    with TASKS_LOCK:
        task = TASKS[task_id]
        task.update(updates)
        return dict(task)


def get_task(task_id: str) -> Dict[str, Any]:
    with TASKS_LOCK:
        if task_id not in TASKS:
            raise FileNotFoundError(f"Worker task not found: {task_id}")
        task = dict(TASKS[task_id])
        started_at = task.get("started_at") or task.get("created_at")
        if started_at:
            try:
                started = datetime.fromisoformat(started_at)
                task["elapsed_seconds"] = max(
                    0.0,
                    (datetime.now(timezone.utc) - started).total_seconds(),
                )
            except Exception:
                task["elapsed_seconds"] = 0.0
        else:
            task["elapsed_seconds"] = 0.0
        return task


def append_task_log(task_id: str, line: str) -> None:
    text = line.strip()
    if not text:
        return
    with TASKS_LOCK:
        task = TASKS[task_id]
        log_lines = task.setdefault("log_lines", [])
        log_lines.append(text)
        task["latest_log_line"] = text
        task["log_tail"] = "\n".join(log_lines[-80:])


def set_stage(task_id: str, stage: str, **extra: Any) -> None:
    update_task(task_id, stage=stage, stage_label=stage_label_for(stage), **extra)


def log_sink(message: Any) -> None:
    global ACTIVE_TASK_ID
    text = message.record["message"]
    sys.stderr.write(f"[foley-worker] {text}\n")
    if ACTIVE_TASK_ID:
        append_task_log(ACTIVE_TASK_ID, text)


def setup_runtime_imports() -> None:
    global IMPORTS_READY
    global torch
    global torchaudio
    global logger
    global load_model
    global denoise_process
    global feature_process
    global merge_audio_video
    global model_utils

    if IMPORTS_READY:
        return

    project_dir = Path(ARGS.project_dir).resolve()
    if str(project_dir) not in sys.path:
        sys.path.insert(0, str(project_dir))
    os.chdir(project_dir)

    import torch  # type: ignore
    import torchaudio  # type: ignore
    from loguru import logger  # type: ignore
    from hunyuanvideo_foley.utils.model_utils import load_model, denoise_process  # type: ignore
    from hunyuanvideo_foley.utils.feature_utils import feature_process  # type: ignore
    from hunyuanvideo_foley.utils.media_utils import merge_audio_video  # type: ignore
    import hunyuanvideo_foley.utils.model_utils as model_utils  # type: ignore

    logger.remove()
    logger.add(log_sink, level="INFO")
    globals().update(
        {
            "torch": torch,
            "torchaudio": torchaudio,
            "logger": logger,
            "load_model": load_model,
            "denoise_process": denoise_process,
            "feature_process": feature_process,
            "merge_audio_video": merge_audio_video,
            "model_utils": model_utils,
        }
    )
    IMPORTS_READY = True


def select_device() -> Any:
    setup_runtime_imports()
    if torch.cuda.is_available():
        torch.cuda.set_device(ARGS.gpu_id)
        return torch.device(f"cuda:{ARGS.gpu_id}")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def progress_tqdm(iterable: Any, total: Optional[int] = None, desc: str = "") -> Any:
    global ACTIVE_TASK_ID
    start_time = time.time()
    total_steps = total or 0
    current = 0
    for current, item in enumerate(iterable, start=1):
        if ACTIVE_TASK_ID:
            elapsed = time.time() - start_time
            eta_seconds = ((elapsed / current) * (total_steps - current)) if total_steps and current else 0.0
            update_task(
                ACTIVE_TASK_ID,
                stage="denoising",
                stage_label=stage_label_for("denoising"),
                progress_current=current,
                progress_total=total_steps,
                progress_percent=int(current * 100 / max(total_steps, 1)) if total_steps else 0,
                eta_hint=format_duration(eta_seconds),
                elapsed_hint=format_duration(elapsed),
            )
        yield item


def ensure_model_loaded(task_id: str) -> None:
    global MODEL_DICT, CFG, DEVICE
    if MODEL_DICT is not None and CFG is not None and DEVICE is not None:
        return

    with MODEL_LOCK:
        if MODEL_DICT is not None and CFG is not None and DEVICE is not None:
            return
        set_stage(task_id, "loading_models")
        setup_runtime_imports()
        DEVICE = select_device()
        model_utils.tqdm = progress_tqdm
        MODEL_DICT, CFG = load_model(
            ARGS.model_path,
            resolve_config_path(),
            DEVICE,
            enable_offload=ARGS.enable_offload,
            model_size=ARGS.model_size,
        )


def resolve_config_path() -> str:
    project_dir = Path(ARGS.project_dir).resolve()
    config_mapping = {
        "xl": project_dir / "configs" / "hunyuanvideo-foley-xl.yaml",
        "xxl": project_dir / "configs" / "hunyuanvideo-foley-xxl.yaml",
    }
    return str(config_mapping[ARGS.model_size])


def process_task(task_id: str) -> None:
    global ACTIVE_TASK_ID
    task = get_task(task_id)
    clip_path = Path(task["clip_path"]).resolve()
    output_dir = Path(task["output_dir"]).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    audio_path = output_dir / "editing_clip_generated.wav"
    video_path = output_dir / "editing_clip_with_audio.mp4"

    ACTIVE_TASK_ID = task_id
    update_task(task_id, status="running", started_at=utc_now_iso())
    try:
        ensure_model_loaded(task_id)
        set_stage(task_id, "extracting_features")
        visual_feats, text_feats, audio_len_in_s = feature_process(
            str(clip_path),
            task["prompt"],
            MODEL_DICT,
            CFG,
        )
        set_stage(
            task_id,
            "denoising",
            progress_current=0,
            progress_total=int(task["num_inference_steps"]),
            progress_percent=0,
            eta_hint="",
            elapsed_hint="",
        )
        audio, sample_rate = denoise_process(
            visual_feats,
            text_feats,
            audio_len_in_s,
            MODEL_DICT,
            CFG,
            guidance_scale=float(task["guidance_scale"]),
            num_inference_steps=int(task["num_inference_steps"]),
            batch_size=1,
        )

        set_stage(task_id, "saving_audio", progress_current=1, progress_total=1, progress_percent=100)
        # denoise_process returns [batch, channels, samples]; torchaudio.save expects [channels, samples]
        audio_to_save = audio[0] if getattr(audio, "ndim", 0) == 3 else audio
        torchaudio.save(str(audio_path), audio_to_save, sample_rate)

        if task.get("save_video", True):
            set_stage(task_id, "merging")
            merge_audio_video(str(audio_path), str(clip_path), str(video_path))

        update_task(
            task_id,
            status="completed",
            finished_at=utc_now_iso(),
            stage="completed",
            stage_label=stage_label_for("completed"),
            audio_path=str(audio_path),
            merged_video_path=str(video_path) if video_path.exists() else "",
        )
    except Exception as exc:
        append_task_log(task_id, traceback.format_exc())
        update_task(
            task_id,
            status="failed",
            finished_at=utc_now_iso(),
            stage="failed",
            stage_label=stage_label_for("failed"),
            error=str(exc),
        )
    finally:
        ACTIVE_TASK_ID = None
        gc.collect()
        if "torch" in globals() and torch.cuda.is_available():
            torch.cuda.empty_cache()


def worker_loop() -> None:
    while True:
        task_id = TASK_QUEUE.get()
        try:
            process_task(task_id)
        finally:
            TASK_QUEUE.task_done()


class WorkerHandler(BaseHTTPRequestHandler):
    server_version = "UMRMFoleyWorker/0.1"

    def do_GET(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if parsed.path == "/health":
                active_task = ACTIVE_TASK_ID
                self.send_json(
                    {
                        "ok": True,
                        "loaded": MODEL_DICT is not None,
                        "busy": active_task is not None,
                        "active_task_id": active_task or "",
                        "project_dir": str(Path(ARGS.project_dir).resolve()),
                        "model_path": str(Path(ARGS.model_path).resolve()),
                        "gpu_id": ARGS.gpu_id,
                        "model_size": ARGS.model_size,
                        "enable_offload": bool(ARGS.enable_offload),
                    }
                )
                return

            if parsed.path == "/task":
                task_id = (parse.parse_qs(parsed.query).get("id") or [""])[0]
                self.send_json({"task": get_task(task_id)})
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint.")
        except Exception as exc:
            self.handle_exception(exc)

    def do_POST(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if parsed.path == "/run":
                payload = read_json_body(self)
                task_id = uuid.uuid4().hex[:12]
                task = {
                    "task_id": task_id,
                    "status": "queued",
                    "created_at": utc_now_iso(),
                    "clip_path": str(payload.get("clip_path", "")).strip(),
                    "prompt": str(payload.get("prompt", "")).strip(),
                    "output_dir": str(payload.get("output_dir", "")).strip(),
                    "guidance_scale": float(payload.get("guidance_scale", 4.5)),
                    "num_inference_steps": int(payload.get("num_inference_steps", 50)),
                    "save_video": bool(payload.get("save_video", True)),
                    "audio_path": "",
                    "merged_video_path": "",
                    "error": "",
                    "latest_log_line": "",
                    "log_tail": "",
                    "log_lines": [],
                    "stage": "queued",
                    "stage_label": stage_label_for("queued"),
                    "progress_current": 0,
                    "progress_total": 0,
                    "progress_percent": 0,
                    "eta_hint": "",
                    "elapsed_hint": "",
                }
                if not task["clip_path"] or not Path(task["clip_path"]).exists():
                    raise FileNotFoundError(f"Clip not found: {task['clip_path']}")
                if not task["prompt"]:
                    raise ValueError("prompt is required")
                if not task["output_dir"]:
                    raise ValueError("output_dir is required")
                with TASKS_LOCK:
                    TASKS[task_id] = task
                TASK_QUEUE.put(task_id)
                self.send_json({"ok": True, "task": get_task(task_id)})
                return

            if parsed.path == "/shutdown":
                self.send_json({"ok": True})
                threading.Thread(target=shutdown_server, daemon=True).start()
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint.")
        except Exception as exc:
            self.handle_exception(exc)

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_exception(self, exc: Exception) -> None:
        traceback.print_exc()
        status = HTTPStatus.BAD_REQUEST
        if isinstance(exc, FileNotFoundError):
            status = HTTPStatus.NOT_FOUND
        elif isinstance(exc, RuntimeError):
            status = HTTPStatus.BAD_GATEWAY
        self.send_json({"ok": False, "error": str(exc), "type": exc.__class__.__name__}, status=status)

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), format % args))


def parse_args() -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--model-size", choices=["xl", "xxl"], default="xxl")
    parser.add_argument("--gpu-id", type=int, default=0)
    parser.add_argument("--enable-offload", action="store_true")
    return parser.parse_args()


def shutdown_server() -> None:
    global HTTP_SERVER
    time.sleep(0.2)
    if HTTP_SERVER is not None:
        HTTP_SERVER.shutdown()


def main() -> None:
    global ARGS, HTTP_SERVER
    ARGS = parse_args()
    threading.Thread(target=worker_loop, daemon=True).start()
    server = ThreadingHTTPServer((ARGS.host, ARGS.port), WorkerHandler)
    HTTP_SERVER = server
    print(f"Foley worker listening on http://{ARGS.host}:{ARGS.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
