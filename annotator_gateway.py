#!/usr/bin/env python3
import hashlib
import json
import os
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import parse, request, error

import server as core


INFER_BASE_URL = (os.environ.get("UMRM_INFER_BASE_URL", "http://127.0.0.1:8877").rstrip("/"))
INFER_TOKEN = os.environ.get("UMRM_INFER_TOKEN", "").strip()
INFER_REQUEST_TIMEOUT = int(os.environ.get("UMRM_INFER_REQUEST_TIMEOUT", "20"))
GATEWAY_EDITING_RUNS_DIR = core.BASE_DIR / "gateway_editing_runs"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_video_id(relative_path: str, size_bytes: int, mtime_ns: int) -> str:
    raw = f"{relative_path}|{size_bytes}|{mtime_ns}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def list_videos_with_id(root: Path) -> list[Dict[str, Any]]:
    if not root.exists():
        raise FileNotFoundError(f"Video root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Video root is not a directory: {root}")

    videos: list[Dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in core.VIDEO_EXTS:
            continue
        stat = path.stat()
        rel_path = str(path.relative_to(root)).replace("\\", "/")
        video_id = compute_video_id(rel_path, stat.st_size, stat.st_mtime_ns)
        videos.append(
            {
                "video_id": video_id,
                "name": path.name,
                "relative_path": rel_path,
                "absolute_path": str(path),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
                "media_url": core.media_url_for_path(path),
                "preview_media_url": core.preview_url_for_path(path),
                "preview_cached": core.preview_cache_path(path).exists(),
            }
        )
    return videos


def inference_request(
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
) -> Dict[str, Any]:
    url = f"{INFER_BASE_URL}{path}"
    data = None
    headers: Dict[str, str] = {}
    if INFER_TOKEN:
        headers["X-UMRM-Token"] = INFER_TOKEN
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout or INFER_REQUEST_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Inference server error {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Cannot reach inference server: {exc.reason}") from exc


def download_audio(task_id: str, source_url: str, filename_hint: str = "generated.wav") -> Path:
    run_dir = GATEWAY_EDITING_RUNS_DIR / task_id / "foley_output"
    run_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(filename_hint).suffix or ".wav"
    target = run_dir / f"editing_clip_generated{suffix}"

    absolute_url = source_url if source_url.startswith("http://") or source_url.startswith("https://") else f"{INFER_BASE_URL}{source_url}"
    req = request.Request(absolute_url, headers={"X-UMRM-Token": INFER_TOKEN} if INFER_TOKEN else {})
    with request.urlopen(req, timeout=max(30, INFER_REQUEST_TIMEOUT)) as resp:
        data = resp.read()
    with target.open("wb") as fh:
        fh.write(data)
    return target


def mirror_remote_task(local_task_id: str, remote_task: Dict[str, Any]) -> None:
    core.update_task(
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


def run_remote_editing_task(task_id: str) -> None:
    task = core.get_task(task_id)
    try:
        core.update_task(
            task_id,
            status="running",
            stage="queued",
            stage_label=core.stage_label_for("queued"),
            started_at=utc_now_iso(),
        )
        remote_payload = inference_request(
            "POST",
            "/api/infer/editing",
            payload={
                "video_id": task["video_id"],
                "editing_start": task["editing_start"],
                "editing_end": task["editing_end"],
                "editing_prompt": task["editing_prompt"],
            },
            timeout=max(30, INFER_REQUEST_TIMEOUT),
        )
        remote_task = remote_payload["task"]
        remote_task_id = remote_task["task_id"]
        mirror_remote_task(task_id, remote_task)

        while True:
            polled = inference_request(
                "GET",
                f"/api/infer/editing-task?id={parse.quote(remote_task_id)}",
                timeout=INFER_REQUEST_TIMEOUT,
            )
            remote_task = polled["task"]
            mirror_remote_task(task_id, remote_task)

            if remote_task.get("status") == "completed":
                audio_url = str(remote_task.get("audio_download_url", "")).strip()
                if not audio_url:
                    raise RuntimeError("Inference task completed without audio_download_url.")
                local_audio = download_audio(
                    task_id,
                    audio_url,
                    filename_hint=str(remote_task.get("audio_filename", "generated.wav")),
                )
                core.update_task(
                    task_id,
                    status="completed",
                    stage="completed",
                    stage_label=core.stage_label_for("completed"),
                    finished_at=utc_now_iso(),
                    audio_path=str(local_audio),
                    audio_media_url=core.media_url_for_path(local_audio),
                    merged_video_path="",
                    merged_video_media_url="",
                    merged_video_preview_url="",
                )
                break

            if remote_task.get("status") == "failed":
                raise RuntimeError(
                    remote_task.get("error")
                    or "Inference server reported failure."
                )

            time.sleep(2)
    except Exception as exc:
        traceback.print_exc()
        core.update_task(
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

    clip_start = payload.get("clip_start")
    clip_end = payload.get("clip_end")
    if clip_start is not None and clip_end is not None:
        clip_start_f = float(clip_start)
        clip_end_f = float(clip_end)
        if editing_start < clip_start_f or editing_end > clip_end_f:
            raise ValueError("Editing interval must stay within the selected clip.")

    task_id = uuid.uuid4().hex[:12]
    run_dir = GATEWAY_EDITING_RUNS_DIR / task_id
    run_dir.mkdir(parents=True, exist_ok=True)
    task = {
        "task_id": task_id,
        "status": "queued",
        "created_at": utc_now_iso(),
        "video_id": video_id,
        "video_path": str(payload.get("video_path", "")).strip(),
        "video_name": str(payload.get("video_name", "")).strip(),
        "editing_prompt": editing_prompt,
        "editing_start": editing_start,
        "editing_end": editing_end,
        "clip_start": clip_start,
        "clip_end": clip_end,
        "run_dir": str(run_dir),
        "error": "",
        "audio_path": "",
        "audio_media_url": "",
        "merged_video_path": "",
        "merged_video_media_url": "",
        "merged_video_preview_url": "",
        "clip_path": "",
        "clip_media_url": "",
        "clip_preview_url": "",
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

    thread = threading.Thread(target=run_remote_editing_task, args=(task_id,), daemon=True)
    thread.start()
    return core.get_task(task_id)


def patch_core() -> None:
    core.list_videos = list_videos_with_id
    core.start_editing_task = start_editing_task


def run_server() -> None:
    patch_core()
    core.ensure_dirs()
    GATEWAY_EDITING_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    core.run_server()


if __name__ == "__main__":
    run_server()
