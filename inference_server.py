#!/usr/bin/env python3
import hashlib
import json
import mimetypes
import os
import shutil
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

import server as core


INFER_ROOT = Path(os.environ.get("UMRM_INFER_VIDEO_ROOT", str(core.DEFAULT_VIDEO_ROOT))).resolve()
INFER_HOST = os.environ.get("UMRM_INFER_HOST", "0.0.0.0")
INFER_PORT = int(os.environ.get("UMRM_INFER_PORT", "8877"))
INFER_TOKEN = os.environ.get("UMRM_INFER_TOKEN", "").strip()

INFER_RUNS_DIR = core.BASE_DIR / "inference_runs"

VIDEO_INDEX: Dict[str, str] = {}
VIDEO_INDEX_LOCK = threading.Lock()


def json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_video_id(relative_path: str, size_bytes: int, mtime_ns: int) -> str:
    raw = f"{relative_path}|{size_bytes}|{mtime_ns}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def build_video_index() -> Dict[str, str]:
    index: Dict[str, str] = {}
    if not INFER_ROOT.exists() or not INFER_ROOT.is_dir():
        return index
    for path in sorted(INFER_ROOT.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in core.VIDEO_EXTS:
            continue
        stat = path.stat()
        rel = str(path.relative_to(INFER_ROOT)).replace("\\", "/")
        video_id = compute_video_id(rel, stat.st_size, stat.st_mtime_ns)
        index[video_id] = str(path.resolve())
    return index


def refresh_video_index() -> None:
    with VIDEO_INDEX_LOCK:
        VIDEO_INDEX.clear()
        VIDEO_INDEX.update(build_video_index())


def resolve_video_path(video_id: str) -> Path:
    with VIDEO_INDEX_LOCK:
        path = VIDEO_INDEX.get(video_id, "")
    if path:
        resolved = Path(path).resolve()
        if resolved.exists():
            return resolved
    refresh_video_index()
    with VIDEO_INDEX_LOCK:
        refreshed = VIDEO_INDEX.get(video_id, "")
    if not refreshed:
        raise FileNotFoundError(f"Video not found by video_id: {video_id}")
    resolved = Path(refreshed).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Video path disappeared: {resolved}")
    return resolved


def update_task(task_id: str, **updates: Any) -> Dict[str, Any]:
    with core.EDITING_TASKS_LOCK:
        task = core.EDITING_TASKS[task_id]
        task.update(updates)
        return dict(task)


def get_task(task_id: str) -> Dict[str, Any]:
    with core.EDITING_TASKS_LOCK:
        if task_id not in core.EDITING_TASKS:
            raise FileNotFoundError(f"Editing task not found: {task_id}")
        task = dict(core.EDITING_TASKS[task_id])
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


def mirror_worker_task(local_task_id: str, remote_task: Dict[str, Any]) -> None:
    update_task(
        local_task_id,
        worker_task_id=remote_task.get("task_id", ""),
        stage=remote_task.get("stage", ""),
        stage_label=remote_task.get("stage_label", core.stage_label_for(remote_task.get("stage", ""))),
        progress_current=remote_task.get("progress_current", 0),
        progress_total=remote_task.get("progress_total", 0),
        progress_percent=remote_task.get("progress_percent", 0),
        eta_hint=remote_task.get("eta_hint", ""),
        elapsed_hint=remote_task.get("elapsed_hint", ""),
        latest_log_line=remote_task.get("latest_log_line", ""),
        log_tail=remote_task.get("log_tail", ""),
    )


def run_foley_task(task_id: str) -> None:
    task = get_task(task_id)
    source_video = resolve_video_path(task["video_id"])
    prompt = task["editing_prompt"]
    start_s = float(task["editing_start"])
    end_s = float(task["editing_end"])
    run_dir = Path(task["run_dir"]).resolve()
    clip_path = run_dir / "editing_clip.mp4"
    output_dir = run_dir / "foley_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        update_task(task_id, status="clipping", started_at=utc_now_iso())
        core.clip_video_segment(source_video, clip_path, start_s, end_s)

        foley_cfg = core.get_foley_config()
        project_dir = Path(foley_cfg["project_dir"]).resolve()
        if not project_dir.exists():
            raise FileNotFoundError(f"Foley project not found: {project_dir}")
        if not Path(foley_cfg["model_path"]).exists():
            raise FileNotFoundError(
                f"Foley model path not found: {foley_cfg['model_path']}"
            )

        update_task(
            task_id,
            status="running",
            stage="loading_models",
            stage_label=core.stage_label_for("loading_models"),
            clip_path=str(clip_path),
            foley_config=foley_cfg,
            source_video_path=str(source_video),
        )

        core.FOLEY_WORKER_MANAGER.ensure_worker(foley_cfg)
        remote_payload = core.FOLEY_WORKER_MANAGER.request(
            "POST",
            "/run",
            payload={
                "clip_path": str(clip_path),
                "prompt": prompt,
                "output_dir": str(output_dir),
                "guidance_scale": foley_cfg["guidance_scale"],
                "num_inference_steps": foley_cfg["num_inference_steps"],
                "save_video": False,
            },
            timeout=30,
        )
        remote_task_id = remote_payload["task"]["task_id"]
        mirror_worker_task(task_id, remote_payload["task"])

        while True:
            polled = core.FOLEY_WORKER_MANAGER.request(
                "GET",
                f"/task?id={parse.quote(remote_task_id)}",
                timeout=10,
            )
            remote_task = polled["task"]
            mirror_worker_task(task_id, remote_task)

            if remote_task["status"] == "completed":
                audio_path = Path(remote_task["audio_path"]).resolve()
                if not audio_path.exists():
                    raise FileNotFoundError(f"Audio not found: {audio_path}")
                update_task(
                    task_id,
                    status="completed",
                    stage="completed",
                    stage_label=core.stage_label_for("completed"),
                    finished_at=utc_now_iso(),
                    audio_path=str(audio_path),
                    audio_filename=audio_path.name,
                    audio_download_url=f"/api/infer/audio?id={parse.quote(task_id)}",
                    merged_video_path="",
                    merged_video_media_url="",
                    merged_video_preview_url="",
                )
                break

            if remote_task["status"] == "failed":
                raise RuntimeError(
                    remote_task.get("error")
                    or "Foley worker reported failure."
                )

            time.sleep(2)
    except Exception as exc:
        update_task(
            task_id,
            status="failed",
            stage="failed",
            stage_label=core.stage_label_for("failed"),
            finished_at=utc_now_iso(),
            error=str(exc),
        )


def start_editing_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    video_id = str(payload.get("video_id", "")).strip()
    editing_prompt = str(payload.get("editing_prompt", "")).strip()
    if not video_id:
        raise ValueError("video_id is required.")
    if not editing_prompt:
        raise ValueError("editing_prompt is required.")

    editing_start = float(payload.get("editing_start", 0))
    editing_end = float(payload.get("editing_end", 0))
    if editing_end <= editing_start:
        raise ValueError("editing_end must be greater than editing_start.")

    task_id = uuid.uuid4().hex[:12]
    run_dir = INFER_RUNS_DIR / task_id
    run_dir.mkdir(parents=True, exist_ok=True)
    task = {
        "task_id": task_id,
        "status": "queued",
        "created_at": utc_now_iso(),
        "video_id": video_id,
        "editing_prompt": editing_prompt,
        "editing_start": editing_start,
        "editing_end": editing_end,
        "run_dir": str(run_dir),
        "error": "",
        "audio_path": "",
        "audio_filename": "",
        "audio_download_url": "",
        "merged_video_path": "",
        "merged_video_media_url": "",
        "merged_video_preview_url": "",
        "clip_path": "",
        "log_tail": "",
        "log_lines": [],
        "latest_log_line": "",
        "stage": "queued",
        "stage_label": core.stage_label_for("queued"),
        "progress_current": 0,
        "progress_total": 0,
        "progress_percent": 0,
        "eta_hint": "",
        "elapsed_hint": "",
    }
    with core.EDITING_TASKS_LOCK:
        core.EDITING_TASKS[task_id] = task

    thread = threading.Thread(target=run_foley_task, args=(task_id,), daemon=True)
    thread.start()
    return get_task(task_id)


def is_request_authorized(handler: BaseHTTPRequestHandler, parsed: parse.ParseResult) -> bool:
    if not INFER_TOKEN:
        return True
    query_token = (parse.parse_qs(parsed.query).get("token") or [""])[0].strip()
    header_token = handler.headers.get("X-UMRM-Token", "").strip()
    return INFER_TOKEN in {query_token, header_token}


class InferenceHandler(BaseHTTPRequestHandler):
    server_version = "UMRMInferenceServer/0.1"

    def do_GET(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if not is_request_authorized(self, parsed):
                self.send_json({"ok": False, "error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if parsed.path == "/api/infer/health":
                with VIDEO_INDEX_LOCK:
                    video_count = len(VIDEO_INDEX)
                self.send_json(
                    {
                        "ok": True,
                        "video_root": str(INFER_ROOT),
                        "video_count": video_count,
                    }
                )
                return

            if parsed.path == "/api/infer/editing-task":
                query = parse.parse_qs(parsed.query)
                task_id = (query.get("id") or [""])[0]
                self.send_json({"task": get_task(task_id)})
                return

            if parsed.path == "/api/infer/audio":
                query = parse.parse_qs(parsed.query)
                task_id = (query.get("id") or [""])[0]
                task = get_task(task_id)
                audio_path = Path(task.get("audio_path", "")).expanduser().resolve()
                if not audio_path.exists() or not audio_path.is_file():
                    self.send_error(HTTPStatus.NOT_FOUND, "Audio file not found.")
                    return
                self.serve_file(audio_path)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint.")
        except Exception as exc:
            self.handle_exception(exc)

    def do_POST(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if not is_request_authorized(self, parsed):
                self.send_json({"ok": False, "error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if parsed.path == "/api/infer/editing":
                payload = core.read_json_body(self)
                task = start_editing_task(payload)
                self.send_json({"ok": True, "task": task})
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint.")
        except Exception as exc:
            self.handle_exception(exc)

    def serve_file(self, file_path: Path) -> None:
        file_size = file_path.stat().st_size
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        range_header = self.headers.get("Range")

        if range_header:
            match = core.re.match(r"bytes=(\d*)-(\d*)", range_header)
            if not match:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            start_raw, end_raw = match.groups()
            start = int(start_raw) if start_raw else 0
            end = int(end_raw) if end_raw else file_size - 1
            if start >= file_size or end < start:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            end = min(end, file_size - 1)
            length = end - start + 1

            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            with file_path.open("rb") as fh:
                fh.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = fh.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        with file_path.open("rb") as fh:
            shutil.copyfileobj(fh, self.wfile)

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
        self.send_json(
            {
                "ok": False,
                "error": str(exc),
                "type": exc.__class__.__name__,
            },
            status=status,
        )

    def log_message(self, format: str, *args: Any) -> None:
        core.sys.stderr.write(
            "[%s] %s\n"
            % (self.log_date_time_string(), format % args)
        )


def run_server() -> None:
    core.ensure_dirs()
    INFER_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    refresh_video_index()
    server = ThreadingHTTPServer((INFER_HOST, INFER_PORT), InferenceHandler)
    print(f"UMRM inference server running at http://{INFER_HOST}:{INFER_PORT}")
    print(f"Inference video root: {INFER_ROOT}")
    print(f"Indexed videos: {len(VIDEO_INDEX)}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
