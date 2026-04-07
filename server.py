#!/usr/bin/env python3
import json
import math
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid
import hashlib
from datetime import datetime, timezone
from email.parser import BytesParser
from email.policy import default as email_policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, parse, request


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = BASE_DIR / "uploads"
ANNOTATION_FILE = DATA_DIR / "annotations.jsonl"
VIDEO_STATUS_FILE = DATA_DIR / "video_statuses.json"
SETTINGS_FILE = DATA_DIR / "runtime_settings.json"
EDITING_RUNS_DIR = BASE_DIR / "editing_runs"
PREVIEW_CACHE_DIR = BASE_DIR / "preview_cache"
HLS_CACHE_DIR = PREVIEW_CACHE_DIR / "hls"
DEFAULT_VIDEO_ROOT = Path(
    os.environ.get("UMRM_VIDEO_ROOT", "/data/UMRM/data/EGO/videos/full_scale")
).resolve()
VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v"}
MAX_FRAME_COUNT = 10
PREVIEW_ENABLED = os.environ.get("UMRM_PREVIEW_ENABLED", "1") != "0"
PREVIEW_HEIGHT = max(0, int(os.environ.get("UMRM_PREVIEW_HEIGHT", "540")))
PREVIEW_VIDEO_BITRATE = os.environ.get("UMRM_PREVIEW_VIDEO_BITRATE", "900k").strip() or "900k"
PREVIEW_AUDIO_BITRATE = os.environ.get("UMRM_PREVIEW_AUDIO_BITRATE", "96k").strip() or "96k"
LOW_STREAM_ENABLED = os.environ.get("UMRM_LOW_STREAM_ENABLED", "1") != "0"
LOW_STREAM_HEIGHT = max(0, int(os.environ.get("UMRM_LOW_STREAM_HEIGHT", "360")))
LOW_STREAM_VIDEO_BITRATE = os.environ.get("UMRM_LOW_STREAM_VIDEO_BITRATE", "450k").strip() or "450k"
LOW_STREAM_AUDIO_BITRATE = os.environ.get("UMRM_LOW_STREAM_AUDIO_BITRATE", "64k").strip() or "64k"
LOW_STREAM_FPS = max(8, int(os.environ.get("UMRM_LOW_STREAM_FPS", "20")))
HLS_ENABLED = os.environ.get("UMRM_HLS_ENABLED", "1") != "0"
HLS_SEGMENT_DURATION = max(
    1, int(os.environ.get("UMRM_HLS_SEGMENT_DURATION", "1"))
)
HLS_CACHE_VERSION = os.environ.get("UMRM_HLS_CACHE_VERSION", "v3").strip() or "v3"
UI_SHARED_TOKEN = os.environ.get("UMRM_SHARED_TOKEN", "").strip()
CLIP_MIN_DURATION = 2.0
CLIP_MAX_DURATION = 15.0
CLIP_DURATION_EPS = 1e-6
VIDEO_CLAIM_TIMEOUT_SECONDS = 30 * 60
VIDEO_STATUS_UNCLAIMED = "unclaimed"
VIDEO_STATUS_CLAIMED = "claimed"
VIDEO_STATUS_COMPLETED_UNVERIFIED = "completed_unverified"
VIDEO_STATUS_VERIFIED = "verified"
VALID_VIDEO_STATUSES = {
    VIDEO_STATUS_UNCLAIMED,
    VIDEO_STATUS_CLAIMED,
    VIDEO_STATUS_COMPLETED_UNVERIFIED,
    VIDEO_STATUS_VERIFIED,
}

DEFAULT_VLM_BASE = os.environ.get("OPENAI_BASE_URL") or os.environ.get(
    "OPENAI_API_BASE", "https://aihubmix.com/v1"
)
DEFAULT_FOLEY_PROJECT_DIR = Path(
    os.environ.get("UMRM_FOLEY_PROJECT_DIR", "/data/cws/Project/HunyuanVideo-Foley")
).resolve()
DEFAULT_FOLEY_MODEL_PATH = Path(
    os.environ.get("UMRM_FOLEY_MODEL_PATH", str(DEFAULT_FOLEY_PROJECT_DIR / "ckpt"))
).resolve()
DEFAULT_FOLEY_FFMPEG = Path(
    os.environ.get("UMRM_FOLEY_FFMPEG", "/data/cws/miniconda3/envs/v2a/bin/ffmpeg")
).resolve()
FOLEY_WORKER_SCRIPT = BASE_DIR / "foley_worker.py"
FOLEY_WORKER_HOST = os.environ.get("UMRM_FOLEY_WORKER_HOST", "127.0.0.1")
FOLEY_WORKER_PORT = int(os.environ.get("UMRM_FOLEY_WORKER_PORT", "8766"))
NO_PROXY_OPENER = request.build_opener(request.ProxyHandler({}))

EDITING_TASKS: Dict[str, Dict[str, Any]] = {}
EDITING_TASKS_LOCK = threading.Lock()
PREVIEW_LOCKS: Dict[str, threading.Lock] = {}
PREVIEW_LOCKS_GUARD = threading.Lock()
HLS_LOCKS: Dict[str, threading.Lock] = {}
HLS_LOCKS_GUARD = threading.Lock()
HLS_JOBS: Dict[str, subprocess.Popen] = {}
VIDEO_DURATION_CACHE: Dict[str, float] = {}
VIDEO_DURATION_CACHE_LOCK = threading.Lock()
VIDEO_STATUS_LOCK = threading.Lock()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EDITING_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    PREVIEW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    HLS_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def is_client_disconnect_error(exc: Exception) -> bool:
    return isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError))


def read_json_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length else b"{}"
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON body: {exc}") from exc


def read_multipart_form(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    content_type = handler.headers.get("Content-Type", "")
    if "multipart/form-data" not in content_type:
        raise ValueError("Content-Type must be multipart/form-data.")

    content_length = int(handler.headers.get("Content-Length", "0"))
    if content_length <= 0:
        raise ValueError("Empty multipart body.")

    body = handler.rfile.read(content_length)
    header = (
        f"Content-Type: {content_type}\r\n"
        "MIME-Version: 1.0\r\n\r\n"
    ).encode("utf-8")
    message = BytesParser(policy=email_policy).parsebytes(header + body)

    fields: Dict[str, str] = {}
    files: Dict[str, Dict[str, Any]] = {}
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        filename = part.get_filename()
        payload = part.get_payload(decode=True) or b""
        if filename:
            files[name] = {"filename": filename, "content": payload}
        else:
            fields[name] = payload.decode("utf-8", errors="replace")
    return {"fields": fields, "files": files}


def resolve_root(root_value: Optional[str]) -> Path:
    if root_value:
        return Path(root_value).expanduser().resolve()
    return DEFAULT_VIDEO_ROOT


def resolve_annotation_file(path_value: Optional[str], annotator_id: str = "") -> Path:
    raw = str(path_value or "").strip()
    owner = sanitize_annotator_id(annotator_id)
    if not raw:
        target = ANNOTATION_FILE
    else:
        target = Path(raw).expanduser().resolve()
    suffix = target.suffix.lower()
    if suffix not in {".json", ".jsonl"}:
        raise ValueError("标注保存路径必须是 .json 或 .jsonl 文件。")
    if not owner:
        return target
    if target.parent.name == owner:
        return target
    return target.parent / owner / target.name
    return target


def parse_iso_datetime(raw_value: Any) -> Optional[datetime]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_video_status_table() -> Dict[str, Dict[str, Any]]:
    if not VIDEO_STATUS_FILE.exists():
        return {}
    with VIDEO_STATUS_FILE.open("r", encoding="utf-8") as fh:
        raw = fh.read().strip()
        if not raw:
            return {}
        payload = json.loads(raw)
    if not isinstance(payload, dict):
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            result[str(key)] = dict(value)
    return result


def save_video_status_table(table: Dict[str, Dict[str, Any]]) -> None:
    ensure_dirs()
    VIDEO_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = VIDEO_STATUS_FILE.with_suffix(f"{VIDEO_STATUS_FILE.suffix}.tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(table, fh, ensure_ascii=False, indent=2)
    temp_path.replace(VIDEO_STATUS_FILE)


def sanitize_annotator_id(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    text = re.sub(r"[^A-Za-z0-9._:-]+", "_", text)
    return text[:80] if text else ""


def get_request_annotator_id(
    handler: BaseHTTPRequestHandler,
    payload: Optional[Dict[str, Any]] = None,
    allow_fallback: bool = True,
) -> str:
    header_value = sanitize_annotator_id(handler.headers.get("X-UMRM-Annotator-ID", ""))
    payload_value = sanitize_annotator_id((payload or {}).get("annotator_id", ""))
    if header_value:
        return header_value
    if payload_value:
        return payload_value
    if not allow_fallback:
        return ""
    fallback_ip = sanitize_annotator_id((handler.client_address or ["unknown"])[0])
    return f"ip:{fallback_ip or 'unknown'}"


def get_required_annotator_id(
    handler: BaseHTTPRequestHandler, payload: Optional[Dict[str, Any]] = None
) -> str:
    annotator_id = get_request_annotator_id(handler, payload, allow_fallback=False)
    if annotator_id:
        return annotator_id
    raise ValueError("请先输入用户名后再操作。")


def new_video_status_entry(video_path: str, video_relative_path: str = "") -> Dict[str, Any]:
    now = utc_now_iso()
    return {
        "video_path": video_path,
        "video_relative_path": video_relative_path,
        "status": VIDEO_STATUS_UNCLAIMED,
        "claimed_by": "",
        "claimed_at": "",
        "claim_expires_at": "",
        "completed_at": "",
        "completed_by": "",
        "verified_at": "",
        "updated_at": now,
    }


def normalize_video_status_entry(video_path: str, video_relative_path: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    merged = new_video_status_entry(video_path, video_relative_path)
    merged.update({k: entry.get(k, merged[k]) for k in merged.keys()})
    merged["video_path"] = video_path
    if video_relative_path:
        merged["video_relative_path"] = video_relative_path
    status_value = str(merged.get("status", "")).strip()
    if status_value not in VALID_VIDEO_STATUSES:
        status_value = VIDEO_STATUS_UNCLAIMED
    if status_value == VIDEO_STATUS_CLAIMED:
        status_value = VIDEO_STATUS_UNCLAIMED
    merged["status"] = status_value
    merged["claimed_by"] = sanitize_annotator_id(merged.get("claimed_by", ""))
    merged["completed_by"] = sanitize_annotator_id(merged.get("completed_by", ""))
    return merged


def cleanup_expired_claims(table: Dict[str, Dict[str, Any]]) -> bool:
    changed = False
    now = datetime.now(timezone.utc)
    for video_path, entry in list(table.items()):
        normalized = normalize_video_status_entry(
            str(video_path),
            str(entry.get("video_relative_path", "")),
            entry if isinstance(entry, dict) else {},
        )
        expires_at = parse_iso_datetime(normalized.get("claim_expires_at"))
        has_active_claim_owner = bool(normalized.get("claimed_by"))
        if has_active_claim_owner and (expires_at is None or expires_at <= now):
            normalized["claimed_by"] = ""
            normalized["claimed_at"] = ""
            normalized["claim_expires_at"] = ""
            normalized["updated_at"] = utc_now_iso()
            changed = True
        if normalized != entry:
            table[video_path] = normalized
            changed = True
    return changed


def resolve_status_entry(
    table: Dict[str, Dict[str, Any]],
    video_path: str,
    video_relative_path: str = "",
) -> Dict[str, Any]:
    current = table.get(video_path)
    if not isinstance(current, dict):
        entry = new_video_status_entry(video_path, video_relative_path)
        table[video_path] = entry
        return entry
    normalized = normalize_video_status_entry(video_path, video_relative_path, current)
    table[video_path] = normalized
    return normalized


def apply_video_status_for_videos(videos: List[Dict[str, Any]]) -> None:
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        changed = cleanup_expired_claims(table)
        for video in videos:
            path = str(video["absolute_path"])
            rel = str(video.get("relative_path", ""))
            entry = resolve_status_entry(table, path, rel)
            if entry.get("video_relative_path") != rel:
                entry["video_relative_path"] = rel
                entry["updated_at"] = utc_now_iso()
                changed = True
            video["video_status"] = entry
        if changed:
            save_video_status_table(table)


def list_videos(root: Path) -> List[Dict[str, Any]]:
    if not root.exists():
        raise FileNotFoundError(f"Video root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Video root is not a directory: {root}")

    videos: List[Dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in VIDEO_EXTS:
            continue
        stat = path.stat()
        rel_path = str(path.relative_to(root))
        videos.append(
            {
                "name": path.name,
                "relative_path": rel_path,
                "absolute_path": str(path),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat(),
                "media_url": media_url_for_path(path),
                "hls_media_url": hls_url_for_path(path) if HLS_ENABLED else None,
                "stream_media_url": low_stream_url_for_path(path),
                "preview_media_url": preview_url_for_path(path),
                "preview_cached": preview_cache_path(path).exists(),
            }
        )
    apply_video_status_for_videos(videos)
    return videos


def claim_video(video_path: str, video_relative_path: str, annotator_id: str) -> Dict[str, Any]:
    normalized_path = str(Path(video_path).expanduser().resolve())
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    expires_iso = datetime.fromtimestamp(
        now.timestamp() + VIDEO_CLAIM_TIMEOUT_SECONDS,
        tz=timezone.utc,
    ).isoformat()
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        changed = cleanup_expired_claims(table)
        entry = resolve_status_entry(table, normalized_path, video_relative_path)
        status_value = entry["status"]
        if status_value == VIDEO_STATUS_VERIFIED:
            raise ValueError("该视频已是 verified 状态，不可再次领取。")
        current_owner = sanitize_annotator_id(entry.get("claimed_by", ""))
        if current_owner and current_owner != annotator_id:
            raise ValueError(f"该视频已被 {current_owner} 领取，请稍后重试。")
        entry["claimed_by"] = annotator_id
        entry["claimed_at"] = now_iso
        entry["claim_expires_at"] = expires_iso
        entry["updated_at"] = now_iso
        table[normalized_path] = entry
        save_video_status_table(table)
        return {"status_entry": entry, "table_changed": changed}


def release_video_claim(video_path: str, annotator_id: str) -> Dict[str, Any]:
    normalized_path = str(Path(video_path).expanduser().resolve())
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        cleanup_expired_claims(table)
        entry = resolve_status_entry(table, normalized_path)
        if entry.get("claimed_by") == annotator_id:
            entry["claimed_by"] = ""
            entry["claimed_at"] = ""
            entry["claim_expires_at"] = ""
            entry["updated_at"] = utc_now_iso()
            table[normalized_path] = entry
            save_video_status_table(table)
        return entry


def heartbeat_video_claim(video_path: str, annotator_id: str) -> Dict[str, Any]:
    normalized_path = str(Path(video_path).expanduser().resolve())
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    expires_iso = datetime.fromtimestamp(
        now.timestamp() + VIDEO_CLAIM_TIMEOUT_SECONDS,
        tz=timezone.utc,
    ).isoformat()
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        cleanup_expired_claims(table)
        entry = resolve_status_entry(table, normalized_path)
        if entry.get("claimed_by") != annotator_id:
            raise ValueError("当前视频未被你领取，无法续约。")
        entry["claim_expires_at"] = expires_iso
        entry["updated_at"] = now_iso
        table[normalized_path] = entry
        save_video_status_table(table)
        return entry


def update_video_status(video_path: str, status: str, annotator_id: str) -> Dict[str, Any]:
    normalized_path = str(Path(video_path).expanduser().resolve())
    target_status = str(status or "").strip().lower()
    if target_status not in {VIDEO_STATUS_COMPLETED_UNVERIFIED, VIDEO_STATUS_VERIFIED}:
        raise ValueError("仅支持更新为 completed_unverified 或 verified。")
    now_iso = utc_now_iso()
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        cleanup_expired_claims(table)
        entry = resolve_status_entry(table, normalized_path)
        current_status = entry["status"]
        if target_status == VIDEO_STATUS_COMPLETED_UNVERIFIED:
            if entry.get("claimed_by") != annotator_id:
                raise ValueError("仅当前领取者可标记为 completed_unverified。")
            entry["status"] = VIDEO_STATUS_COMPLETED_UNVERIFIED
            entry["completed_at"] = now_iso
            entry["completed_by"] = annotator_id
            entry["claimed_at"] = ""
            entry["claim_expires_at"] = ""
            entry["claimed_by"] = ""
        if target_status == VIDEO_STATUS_VERIFIED:
            if current_status not in {VIDEO_STATUS_COMPLETED_UNVERIFIED, VIDEO_STATUS_VERIFIED}:
                raise ValueError("仅 completed_unverified 状态可标记为 verified。")
            entry["status"] = VIDEO_STATUS_VERIFIED
            entry["verified_at"] = now_iso
        entry["updated_at"] = now_iso
        table[normalized_path] = entry
        save_video_status_table(table)
        return entry


def get_video_statuses(video_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        changed = cleanup_expired_claims(table)
        result: Dict[str, Dict[str, Any]] = {}
        for item in video_items:
            path = str(Path(item.get("video_path", "")).expanduser().resolve())
            if not path:
                continue
            rel = str(item.get("video_relative_path", ""))
            entry = resolve_status_entry(table, path, rel)
            if rel and entry.get("video_relative_path") != rel:
                entry["video_relative_path"] = rel
                entry["updated_at"] = utc_now_iso()
                changed = True
            result[path] = entry
        if changed:
            save_video_status_table(table)
        return result


def get_video_statuses_for_root(root: Path) -> Dict[str, Dict[str, Any]]:
    resolved_root = root.resolve()
    root_str = str(resolved_root)
    root_prefix = f"{root_str}{os.sep}"
    with VIDEO_STATUS_LOCK:
        table = load_video_status_table()
        changed = cleanup_expired_claims(table)
        result: Dict[str, Dict[str, Any]] = {}
        for video_path, entry in table.items():
            if video_path == root_str or video_path.startswith(root_prefix):
                result[video_path] = entry
        if changed:
            save_video_status_table(table)
        return result


def media_url_for_path(path: Path) -> str:
    return f"/media?path={parse.quote(str(path.resolve()))}"


def preview_url_for_path(path: Path) -> str:
    return f"/preview?path={parse.quote(str(path.resolve()))}"


def low_stream_url_for_path(path: Path) -> str:
    return f"/stream-low?path={parse.quote(str(path.resolve()))}"


def hls_cache_key(path: Path) -> str:
    resolved = path.resolve()
    stat = resolved.stat()
    signature = (
        f"{HLS_CACHE_VERSION}|{HLS_SEGMENT_DURATION}|"
        f"{resolved}|{stat.st_size}|{stat.st_mtime_ns}"
    )
    return hashlib.sha1(signature.encode("utf-8")).hexdigest()


def video_duration_cache_key(path: Path) -> str:
    resolved = path.resolve()
    stat = resolved.stat()
    signature = f"{resolved}|{stat.st_size}|{stat.st_mtime_ns}"
    return hashlib.sha1(signature.encode("utf-8")).hexdigest()


def probe_video_duration_seconds(path: Path) -> Optional[float]:
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"Media file not found: {resolved}")
    cache_key = video_duration_cache_key(resolved)
    with VIDEO_DURATION_CACHE_LOCK:
        cached = VIDEO_DURATION_CACHE.get(cache_key)
        if cached is not None:
            return cached
    ffprobe_bin = shutil.which("ffprobe")
    if not ffprobe_bin:
        return None
    result = subprocess.run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(resolved),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        timeout=20,
    )
    if result.returncode != 0:
        return None
    raw_duration = result.stdout.strip()
    if not raw_duration:
        return None
    try:
        duration_seconds = float(raw_duration)
    except ValueError:
        return None
    if not math.isfinite(duration_seconds) or duration_seconds <= 0:
        return None
    with VIDEO_DURATION_CACHE_LOCK:
        VIDEO_DURATION_CACHE[cache_key] = duration_seconds
    return duration_seconds


def hls_url_for_path(path: Path) -> str:
    resolved = path.resolve()
    key = hls_cache_key(resolved)
    return f"/hls/{key}/index.m3u8?path={parse.quote(str(resolved))}"


def resolve_ffmpeg_bin() -> Path:
    if DEFAULT_FOLEY_FFMPEG.exists():
        return DEFAULT_FOLEY_FFMPEG
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        return Path(ffmpeg_path).resolve()
    raise FileNotFoundError("ffmpeg not found for preview generation.")


def preview_cache_path(path: Path) -> Path:
    resolved = path.resolve()
    stat = resolved.stat()
    signature = f"{resolved}|{stat.st_size}|{stat.st_mtime_ns}"
    digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    return PREVIEW_CACHE_DIR / f"{digest}.mp4"


def preview_lock_for(cache_key: str) -> threading.Lock:
    with PREVIEW_LOCKS_GUARD:
        lock = PREVIEW_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            PREVIEW_LOCKS[cache_key] = lock
        return lock


def hls_lock_for(cache_key: str) -> threading.Lock:
    with HLS_LOCKS_GUARD:
        lock = HLS_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            HLS_LOCKS[cache_key] = lock
        return lock


def hls_cache_dir_for_key(cache_key: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{40}", cache_key):
        raise ValueError("Invalid hls cache key.")
    return HLS_CACHE_DIR / cache_key


def is_hls_playlist_ready(playlist_path: Path, output_dir: Path) -> bool:
    if not playlist_path.exists():
        return False
    try:
        lines = playlist_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return False
    target_duration = 0
    has_positive_extinf = False
    has_existing_segment = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#EXT-X-TARGETDURATION:"):
            raw_value = stripped.split(":", 1)[1].strip()
            try:
                target_duration = int(raw_value)
            except ValueError:
                target_duration = 0
        elif stripped.startswith("#EXTINF:"):
            raw_value = stripped.split(":", 1)[1].split(",", 1)[0].strip()
            try:
                if float(raw_value) > 0:
                    has_positive_extinf = True
            except ValueError:
                pass
        elif stripped and not stripped.startswith("#"):
            if (output_dir / stripped).exists():
                has_existing_segment = True
    return target_duration >= 1 and has_positive_extinf and has_existing_segment


def ensure_hls_available(source_path: Path, cache_key: str) -> Path:
    resolved = source_path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"Media file not found: {resolved}")
    if hls_cache_key(resolved) != cache_key:
        raise ValueError("HLS cache key mismatch.")

    output_dir = hls_cache_dir_for_key(cache_key)
    playlist_path = output_dir / "index.m3u8"
    if playlist_path.exists():
        return playlist_path

    lock = hls_lock_for(cache_key)
    with lock:
        if playlist_path.exists():
            return playlist_path
        ffmpeg_bin = resolve_ffmpeg_bin()
        output_dir.mkdir(parents=True, exist_ok=True)
        segment_pattern = output_dir / "seg_%05d.m4s"
        log_path = output_dir / "ffmpeg.log"
        existing_job = HLS_JOBS.get(cache_key)
        if existing_job and existing_job.poll() is None:
            job = existing_job
        else:
            if existing_job:
                HLS_JOBS.pop(cache_key, None)
            cmd = [
                str(ffmpeg_bin),
                "-y",
                "-i",
                str(resolved),
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-pix_fmt",
                "yuv420p",
                "-b:v",
                LOW_STREAM_VIDEO_BITRATE,
                "-maxrate",
                LOW_STREAM_VIDEO_BITRATE,
                "-bufsize",
                LOW_STREAM_VIDEO_BITRATE,
                "-r",
                str(LOW_STREAM_FPS),
                "-g",
                str(LOW_STREAM_FPS),
                "-keyint_min",
                str(LOW_STREAM_FPS),
                "-sc_threshold",
                "0",
            ]
            if LOW_STREAM_HEIGHT > 0:
                cmd.extend(["-vf", f"scale=-2:min({LOW_STREAM_HEIGHT}\\,ih):flags=bilinear"])
            cmd.extend(
                [
                    "-c:a",
                    "aac",
                    "-b:a",
                    LOW_STREAM_AUDIO_BITRATE,
                    "-f",
                    "hls",
                    "-hls_time",
                    str(HLS_SEGMENT_DURATION),
                    "-hls_list_size",
                    "0",
                    "-hls_playlist_type",
                    "event",
                    "-hls_flags",
                    "independent_segments+append_list",
                    "-hls_segment_type",
                    "fmp4",
                    "-hls_fmp4_init_filename",
                    "init.mp4",
                    "-hls_segment_filename",
                    str(segment_pattern),
                    str(playlist_path),
                ]
            )
            with log_path.open("w", encoding="utf-8") as log_file:
                job = subprocess.Popen(
                    cmd,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                )
            HLS_JOBS[cache_key] = job

    wait_started_at = time.perf_counter()
    while not is_hls_playlist_ready(playlist_path, output_dir):
        job = HLS_JOBS.get(cache_key)
        if job is None:
            break
        if job.poll() is not None:
            HLS_JOBS.pop(cache_key, None)
            if not is_hls_playlist_ready(playlist_path, output_dir):
                ffmpeg_log = ""
                if log_path.exists():
                    ffmpeg_log = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
                raise RuntimeError(f"HLS generation failed:\n{ffmpeg_log}")
            break
        if (time.perf_counter() - wait_started_at) > 60.0:
            raise RuntimeError("HLS generation is taking too long, please retry.")
        time.sleep(0.1)

    if is_hls_playlist_ready(playlist_path, output_dir):
        return playlist_path

    ffmpeg_log = ""
    if log_path.exists():
        ffmpeg_log = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
    raise RuntimeError(f"HLS playlist unavailable:\n{ffmpeg_log}")


def ensure_preview_available(source_path: Path) -> Path:
    if not PREVIEW_ENABLED:
        return source_path

    resolved = source_path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"Media file not found: {resolved}")

    cached_path = preview_cache_path(resolved)
    if cached_path.exists():
        return cached_path

    lock = preview_lock_for(str(cached_path))
    with lock:
        if cached_path.exists():
            return cached_path

        ffmpeg_bin = resolve_ffmpeg_bin()
        temp_path = cached_path.with_suffix(".tmp.mp4")
        cmd = [
            str(ffmpeg_bin),
            "-y",
            "-i",
            str(resolved),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-b:v",
            PREVIEW_VIDEO_BITRATE,
            "-maxrate",
            PREVIEW_VIDEO_BITRATE,
            "-bufsize",
            PREVIEW_VIDEO_BITRATE,
            "-r",
            "24",
        ]
        if PREVIEW_HEIGHT > 0:
            cmd.extend(["-vf", f"scale=-2:min({PREVIEW_HEIGHT}\\,ih):flags=lanczos"])
        cmd.extend(
            [
                "-c:a",
                "aac",
                "-b:a",
                PREVIEW_AUDIO_BITRATE,
                "-movflags",
                "+faststart",
                str(temp_path),
            ]
        )
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise RuntimeError(f"Preview generation failed:\
{result.stdout[-4000:]}")
        temp_path.replace(cached_path)
    return cached_path


def is_request_authorized(handler: BaseHTTPRequestHandler, parsed: parse.ParseResult) -> bool:
    if not UI_SHARED_TOKEN:
        return True
    query_token = (parse.parse_qs(parsed.query).get("token") or [""])[0].strip()
    header_token = handler.headers.get("X-UMRM-Token", "").strip()
    auth_header = handler.headers.get("Authorization", "").strip()
    bearer_token = ""
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()
    return UI_SHARED_TOKEN in {query_token, header_token, bearer_token}


def request_needs_auth(parsed: parse.ParseResult) -> bool:
    return (
        parsed.path.startswith("/api/")
        or parsed.path in {"/media", "/preview", "/stream-low"}
        or parsed.path.startswith("/hls/")
    )


def normalize_chat_completion_url(api_base: str) -> str:
    base = api_base.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


def urlopen_no_proxy(target: Any, timeout: int):
    return NO_PROXY_OPENER.open(target, timeout=timeout)


def call_chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float = 0.2,
    timeout: int = 180,
) -> Dict[str, Any]:
    if not api_base or not model:
        raise RuntimeError("API base and model must be configured.")

    url = normalize_chat_completion_url(api_base)
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urlopen_no_proxy(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Model request failed with {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Model request failed: {exc.reason}") from exc


def extract_message_text(response_payload: Dict[str, Any]) -> str:
    choices = response_payload.get("choices") or []
    if not choices:
        raise RuntimeError("Model response does not contain choices.")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
        return "\n".join(part for part in parts if part).strip()
    raise RuntimeError("Model response content is empty.")


def try_parse_json_block(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("Model output is not valid JSON.")


def default_runtime_settings() -> Dict[str, Any]:
    return {
        "vlm": {
            "api_base": DEFAULT_VLM_BASE,
            "model": "",
            "api_key": "",
            "translate_to_zh": True,
        },
        "foley": {
            "project_dir": str(DEFAULT_FOLEY_PROJECT_DIR),
            "model_path": str(DEFAULT_FOLEY_MODEL_PATH),
            "conda_env": os.environ.get("UMRM_FOLEY_CONDA_ENV", "v2a"),
            "gpu_id": int(os.environ.get("UMRM_FOLEY_GPU_ID", "0")),
            "model_size": os.environ.get("UMRM_FOLEY_MODEL_SIZE", "xl"),
            "guidance_scale": float(os.environ.get("UMRM_FOLEY_GUIDANCE_SCALE", "4.5")),
            "num_inference_steps": int(
                os.environ.get("UMRM_FOLEY_NUM_INFERENCE_STEPS", "50")
            ),
            "enable_offload": os.environ.get("UMRM_FOLEY_ENABLE_OFFLOAD", "0") == "1",
        },
    }


def mask_secret(secret: str) -> str:
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}...{secret[-4:]}"


def load_runtime_settings() -> Dict[str, Any]:
    settings = default_runtime_settings()
    if SETTINGS_FILE.exists():
        with SETTINGS_FILE.open("r", encoding="utf-8") as fh:
            stored = json.load(fh)
        if isinstance(stored, dict):
            if isinstance(stored.get("vlm"), dict):
                settings["vlm"].update(stored["vlm"])
            if isinstance(stored.get("foley"), dict):
                settings["foley"].update(stored["foley"])
    return settings


def save_runtime_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    settings = load_runtime_settings()
    vlm_payload = payload.get("vlm") or {}
    foley_payload = payload.get("foley") or {}

    if "api_base" in vlm_payload:
        settings["vlm"]["api_base"] = str(vlm_payload["api_base"]).strip()
    if "model" in vlm_payload:
        settings["vlm"]["model"] = str(vlm_payload["model"]).strip()
    if "api_key" in vlm_payload and str(vlm_payload["api_key"]).strip():
        settings["vlm"]["api_key"] = str(vlm_payload["api_key"]).strip()
    if "translate_to_zh" in vlm_payload:
        settings["vlm"]["translate_to_zh"] = bool(vlm_payload["translate_to_zh"])

    if "project_dir" in foley_payload:
        settings["foley"]["project_dir"] = str(foley_payload["project_dir"]).strip()
    if "model_path" in foley_payload:
        settings["foley"]["model_path"] = str(foley_payload["model_path"]).strip()
    if "conda_env" in foley_payload:
        settings["foley"]["conda_env"] = str(foley_payload["conda_env"]).strip()
    if "gpu_id" in foley_payload:
        settings["foley"]["gpu_id"] = int(foley_payload["gpu_id"])
    if "model_size" in foley_payload:
        settings["foley"]["model_size"] = str(foley_payload["model_size"]).strip() or "xxl"
    if "guidance_scale" in foley_payload:
        settings["foley"]["guidance_scale"] = float(foley_payload["guidance_scale"])
    if "num_inference_steps" in foley_payload:
        settings["foley"]["num_inference_steps"] = int(
            foley_payload["num_inference_steps"]
        )
    if "enable_offload" in foley_payload:
        settings["foley"]["enable_offload"] = bool(foley_payload["enable_offload"])

    with SETTINGS_FILE.open("w", encoding="utf-8") as fh:
        json.dump(settings, fh, ensure_ascii=False, indent=2)
    os.chmod(SETTINGS_FILE, 0o600)
    return settings


def get_public_settings() -> Dict[str, Any]:
    settings = load_runtime_settings()
    return {
        "vlm": {
            "api_base": settings["vlm"].get("api_base", ""),
            "model": settings["vlm"].get("model", ""),
            "api_key_set": bool(settings["vlm"].get("api_key", "")),
            "api_key_masked": mask_secret(settings["vlm"].get("api_key", "")),
            "translate_to_zh": bool(settings["vlm"].get("translate_to_zh", True)),
        },
        "foley": settings["foley"],
    }


def get_vlm_config() -> Dict[str, str]:
    settings = load_runtime_settings()
    return {
        "api_base": os.environ.get("VLM_API_BASE")
        or os.environ.get("OPENAI_BASE_URL")
        or os.environ.get("OPENAI_API_BASE")
        or str(settings["vlm"].get("api_base", "")).strip(),
        "api_key": os.environ.get("VLM_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or str(settings["vlm"].get("api_key", "")).strip(),
        "model": os.environ.get("VLM_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or str(settings["vlm"].get("model", "")).strip(),
    }


def get_translation_config() -> Dict[str, str]:
    return {
        "api_base": os.environ.get("TRANSLATE_API_BASE")
        or os.environ.get("OPENAI_BASE_URL")
        or os.environ.get("OPENAI_API_BASE")
        or get_vlm_config()["api_base"],
        "api_key": os.environ.get("TRANSLATE_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or get_vlm_config()["api_key"],
        "model": os.environ.get("TRANSLATE_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or get_vlm_config()["model"],
    }


def get_foley_config() -> Dict[str, Any]:
    settings = load_runtime_settings()
    foley = settings["foley"]
    return {
        "project_dir": str(foley.get("project_dir", DEFAULT_FOLEY_PROJECT_DIR)).strip(),
        "model_path": str(foley.get("model_path", DEFAULT_FOLEY_MODEL_PATH)).strip(),
        "conda_env": str(foley.get("conda_env", "v2a")).strip() or "v2a",
        "gpu_id": int(foley.get("gpu_id", 0)),
        "model_size": str(foley.get("model_size", "xxl")).strip() or "xxl",
        "guidance_scale": float(foley.get("guidance_scale", 4.5)),
        "num_inference_steps": int(foley.get("num_inference_steps", 50)),
        "enable_offload": bool(foley.get("enable_offload", False)),
    }


def translate_text_between(text: str, source_lang: str, target_lang: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Text to translate cannot be empty.")
    source_lang_norm = source_lang.strip().lower()
    target_lang_norm = target_lang.strip().lower()
    if source_lang_norm == "zh" and target_lang_norm == "en":
        llm_prompt = (
            "Translate the following Chinese text into concise, natural English. "
            "Return translated English only. Do not add notes, quotes, or explanations.\n\n"
            f"{cleaned}"
        )
        google_sl = "zh-CN"
        google_tl = "en"
    elif source_lang_norm == "en" and target_lang_norm == "zh":
        llm_prompt = (
            "Translate the following English text into concise, natural Simplified Chinese. "
            "Return translated Chinese only. Do not add notes, quotes, or explanations.\n\n"
            f"{cleaned}"
        )
        google_sl = "en"
        google_tl = "zh-CN"
    else:
        raise ValueError("Unsupported translation direction.")

    cfg = get_translation_config()
    if cfg["api_base"] and cfg["model"]:
        response = call_chat_completion(
            api_base=cfg["api_base"],
            api_key=cfg["api_key"],
            model=cfg["model"],
            messages=[{"role": "user", "content": llm_prompt}],
            temperature=0.1,
            timeout=120,
        )
        return {
            "translated_text": extract_message_text(response),
            "provider": "llm",
        }

    google_url = (
        "https://translate.googleapis.com/translate_a/single?"
        + parse.urlencode(
            {
                "client": "gtx",
                "sl": google_sl,
                "tl": google_tl,
                "dt": "t",
                "q": cleaned,
            }
        )
    )
    try:
        with urlopen_no_proxy(google_url, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except error.URLError as exc:
        raise RuntimeError(
            "翻译服务不可达：当前环境无法访问 translate.googleapis.com。"
        ) from exc
    translated = "".join(item[0] for item in payload[0] if item and item[0])
    return {"translated_text": translated, "provider": "google-gtx"}


def translate_text(text: str) -> Dict[str, Any]:
    return translate_text_between(text, "zh", "en")


def translate_text_en_to_zh(text: str) -> Dict[str, Any]:
    return translate_text_between(text, "en", "zh")


def build_vlm_messages(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    frames = payload.get("frames") or []
    if not isinstance(frames, list) or not frames:
        raise ValueError("At least one sampled frame is required.")
    if len(frames) > MAX_FRAME_COUNT:
        raise ValueError(f"Too many frames. Max supported: {MAX_FRAME_COUNT}.")

    clip_start = float(payload.get("clip_start", 0))
    clip_end = float(payload.get("clip_end", 0))
    current_state = str(payload.get("current_state", "")).strip()
    custom_prompt = str(payload.get("vlm_prompt", "")).strip()
    video_name = str(payload.get("video_name", "")).strip()

    settings = load_runtime_settings()
    output_zh = bool(settings.get("vlm", {}).get("translate_to_zh", True))
    output_instruction = "Write concise Simplified Chinese." if output_zh else "Write concise English."
    instruction = (
        "You are helping a human annotator write a current state description for a selected "
        "video clip. Base everything only on visible evidence in the frames.\n"
        f"{output_instruction}\n"
        "Focus on:\n"
        "- who or what is present\n"
        "- what is happening in the selected clip\n"
        "- scene state that matters for downstream motion/reaction labeling\n"
        "- do not invent hidden causes, backstory, or emotions\n"
        "Return JSON only in this format:\n"
        "{\n"
        '  "clip_summary": "...",\n'
        '  "current_state": "..."\n'
        "}\n\n"
        f"Video name: {video_name or 'unknown'}\n"
        f"Selected clip: {clip_start:.2f}s to {clip_end:.2f}s\n"
        f"Existing current state from annotator: {current_state or '(empty)'}\n"
    )
    if custom_prompt:
        instruction += f"Additional instruction from annotator: {custom_prompt}\n"

    content: List[Dict[str, Any]] = [{"type": "text", "text": instruction}]
    for frame in frames:
        content.append({"type": "image_url", "image_url": {"url": frame}})

    return [{"role": "user", "content": content}]


def run_vlm_understanding(payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg = get_vlm_config()
    if not cfg["api_base"] or not cfg["model"]:
        raise RuntimeError(
            "VLM is not configured. Please set VLM base URL and model in the UI settings."
        )

    response = call_chat_completion(
        api_base=cfg["api_base"],
        api_key=cfg["api_key"],
        model=cfg["model"],
        messages=build_vlm_messages(payload),
        temperature=0.2,
        timeout=180,
    )
    raw_text = extract_message_text(response)
    parsed = try_parse_json_block(raw_text)
    return {
        "clip_summary": str(parsed.get("clip_summary", "")).strip(),
        "current_state": str(parsed.get("current_state", "")).strip(),
        "raw_response": raw_text,
    }


def validate_annotation_payload(payload: Dict[str, Any]) -> None:
    clip_start = float(payload.get("clip_start", 0))
    clip_end = float(payload.get("clip_end", 0))
    if clip_end <= clip_start:
        raise ValueError("结束时间必须大于开始时间。")
    duration = clip_end - clip_start
    if duration < CLIP_MIN_DURATION - CLIP_DURATION_EPS:
        raise ValueError("标注片段长度不能小于 2 秒。")
    if duration > CLIP_MAX_DURATION + CLIP_DURATION_EPS:
        raise ValueError("标注片段长度不能大于 15 秒。")
    payload["clip_start"] = clip_start
    payload["clip_end"] = clip_end
    payload["clip_duration"] = round(duration, 6)
    payload["mental_reasoning"] = str(payload.get("mental_reasoning", "")).strip()


def load_annotations(annotation_file: Path) -> List[Dict[str, Any]]:
    if not annotation_file.exists():
        return []
    suffix = annotation_file.suffix.lower()
    if suffix == ".json":
        with annotation_file.open("r", encoding="utf-8") as fh:
            raw = fh.read().strip()
            if not raw:
                return []
            data = json.loads(raw)
            if not isinstance(data, list):
                raise ValueError("标注文件格式错误：.json 文件应为数组。")
            return [item for item in data if isinstance(item, dict)]
    annotations: List[Dict[str, Any]] = []
    with annotation_file.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            annotations.append(json.loads(line))
    return annotations


def append_annotation(payload: Dict[str, Any], annotation_file: Path) -> None:
    ensure_dirs()
    annotation_file.parent.mkdir(parents=True, exist_ok=True)
    if annotation_file.suffix.lower() == ".json":
        annotations = load_annotations(annotation_file)
        annotations.append(payload)
        with annotation_file.open("w", encoding="utf-8") as fh:
            json.dump(annotations, fh, ensure_ascii=False, indent=2)
        return
    with annotation_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def annotation_identity_key(item: Dict[str, Any]) -> str:
    existing = str(item.get("annotation_id", "")).strip()
    if existing:
        return existing
    seed = "|".join(
        [
            str(item.get("video_path", "")).strip(),
            str(item.get("clip_start", "")).strip(),
            str(item.get("clip_end", "")).strip(),
            str(item.get("saved_at", "")).strip(),
            str(item.get("ui_created_at", "")).strip(),
            str(item.get("annotator_id", "")).strip(),
            str(item.get("reaction", "")).strip(),
            str(item.get("motion_prompt", "")).strip(),
        ]
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def normalize_annotations_for_output(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        clone = dict(item)
        clone["annotation_id"] = annotation_identity_key(clone)
        normalized.append(clone)
    return normalized


def normalize_path_string(path_value: str) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(parse.unquote(raw)).expanduser().resolve())
    except Exception:
        return raw


def delete_annotation_by_id(annotation_file: Path, annotation_id: str, annotator_id: str) -> int:
    target_id = str(annotation_id or "").strip()
    if not target_id:
        raise ValueError("annotation_id 不能为空。")
    annotations = load_annotations(annotation_file)
    remaining: List[Dict[str, Any]] = []
    deleted_count = 0
    for item in annotations:
        if not isinstance(item, dict):
            continue
        item_id = annotation_identity_key(item)
        owner = sanitize_annotator_id(item.get("annotator_id", ""))
        owner_match = (not owner) or owner == annotator_id
        if item_id == target_id and owner_match:
            deleted_count += 1
            continue
        remaining.append(item)
    if deleted_count <= 0:
        return 0
    annotation_file.parent.mkdir(parents=True, exist_ok=True)
    if annotation_file.suffix.lower() == ".json":
        with annotation_file.open("w", encoding="utf-8") as fh:
            json.dump(remaining, fh, ensure_ascii=False, indent=2)
        return deleted_count
    with annotation_file.open("w", encoding="utf-8") as fh:
        for item in remaining:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    return deleted_count


def sanitize_filename(name: str) -> str:
    base = os.path.basename(name).strip()
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return base or "upload.mp4"


class FoleyWorkerManager:
    def __init__(self) -> None:
        self.process: Optional[subprocess.Popen[str]] = None
        self.signature: Optional[tuple] = None
        self.lock = threading.Lock()
        self.log_thread: Optional[threading.Thread] = None

    def _build_signature(self, foley_cfg: Dict[str, Any]) -> tuple:
        return (
            foley_cfg["conda_env"],
            foley_cfg["project_dir"],
            foley_cfg["model_path"],
            foley_cfg["gpu_id"],
            foley_cfg["model_size"],
            bool(foley_cfg["enable_offload"]),
        )

    def _reader(self, process: subprocess.Popen[str]) -> None:
        if process.stdout is None:
            return
        for line in process.stdout:
            text = line.rstrip()
            if text:
                sys.stderr.write(f"[FOLEY_WORKER] {text}\n")

    def _health_matches(self, payload: Dict[str, Any], foley_cfg: Dict[str, Any]) -> bool:
        return (
            str(payload.get("project_dir", "")) == str(foley_cfg["project_dir"])
            and str(payload.get("model_path", "")) == str(foley_cfg["model_path"])
            and int(payload.get("gpu_id", -1)) == int(foley_cfg["gpu_id"])
            and str(payload.get("model_size", "")) == str(foley_cfg["model_size"])
            and bool(payload.get("enable_offload", False)) == bool(foley_cfg["enable_offload"])
        )

    def _stop_locked(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None
        self.signature = None

    def _start_locked(self, foley_cfg: Dict[str, Any]) -> None:
        if not FOLEY_WORKER_SCRIPT.exists():
            raise FileNotFoundError(f"Foley worker script not found: {FOLEY_WORKER_SCRIPT}")

        command = [
            "conda",
            "run",
            "-n",
            foley_cfg["conda_env"],
            "python",
            str(FOLEY_WORKER_SCRIPT),
            "--host",
            FOLEY_WORKER_HOST,
            "--port",
            str(FOLEY_WORKER_PORT),
            "--project-dir",
            foley_cfg["project_dir"],
            "--model-path",
            foley_cfg["model_path"],
            "--model-size",
            foley_cfg["model_size"],
            "--gpu-id",
            str(foley_cfg["gpu_id"]),
        ]
        if foley_cfg["enable_offload"]:
            command.append("--enable-offload")

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        self.process = subprocess.Popen(
            command,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        self.signature = self._build_signature(foley_cfg)
        self.log_thread = threading.Thread(
            target=self._reader, args=(self.process,), daemon=True
        )
        self.log_thread.start()
        self._wait_until_ready_locked()

    def _wait_until_ready_locked(self, timeout_s: float = 60.0) -> None:
        deadline = time.time() + timeout_s
        last_error = ""
        while time.time() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError("Foley worker exited during startup.")
            try:
                payload = self.request("GET", "/health", timeout=2, ensure_worker=False)
                if payload.get("ok"):
                    return
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise RuntimeError(f"Foley worker did not become ready: {last_error}")

    def ensure_worker(self, foley_cfg: Dict[str, Any]) -> None:
        with self.lock:
            signature = self._build_signature(foley_cfg)
            try:
                health = self.request("GET", "/health", timeout=2, ensure_worker=False)
                if health.get("ok") and self._health_matches(health, foley_cfg):
                    self.signature = signature
                    return
                if health.get("ok"):
                    try:
                        self.request("POST", "/shutdown", payload={}, timeout=2, ensure_worker=False)
                        time.sleep(1.0)
                    except Exception:
                        pass
            except Exception:
                pass
            needs_restart = (
                self.process is None
                or self.process.poll() is not None
                or self.signature != signature
            )
            if needs_restart:
                self._stop_locked()
                self._start_locked(foley_cfg)

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = 10,
        ensure_worker: bool = True,
    ) -> Dict[str, Any]:
        if ensure_worker:
            self.ensure_worker(get_foley_config())
        url = f"http://{FOLEY_WORKER_HOST}:{FOLEY_WORKER_PORT}{path}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=data, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Foley worker error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Cannot reach Foley worker: {exc.reason}") from exc


FOLEY_WORKER_MANAGER = FoleyWorkerManager()


def make_task_public(task: Dict[str, Any]) -> Dict[str, Any]:
    public = dict(task)
    return public


def update_task(task_id: str, **updates: Any) -> Dict[str, Any]:
    with EDITING_TASKS_LOCK:
        task = EDITING_TASKS[task_id]
        task.update(updates)
        return dict(task)


def get_task(task_id: str) -> Dict[str, Any]:
    with EDITING_TASKS_LOCK:
        if task_id not in EDITING_TASKS:
            raise FileNotFoundError(f"Editing task not found: {task_id}")
        task = dict(EDITING_TASKS[task_id])
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


def stage_label_for(stage: str) -> str:
    mapping = {
        "queued": "等待启动",
        "clipping": "裁剪 editing 子视频",
        "loading_models": "加载 Foley 模型",
        "extracting_features": "提取视频/文本特征",
        "denoising": "扩散采样中",
        "saving_audio": "写出音频",
        "merging": "合并回视频",
        "completed": "已完成",
        "failed": "执行失败",
        "running": "处理中",
    }
    return mapping.get(stage, stage or "未知阶段")


def parse_denoising_progress(line: str) -> Dict[str, Any]:
    match = re.search(
        r"Denoising steps:\s*(?P<pct>\d+)%.*?(?P<cur>\d+)/(?P<total>\d+)\s*\[(?P<elapsed>[^<]+)<(?P<eta>[^,\]]+)",
        line,
    )
    if not match:
        return {}
    total = int(match.group("total"))
    current = int(match.group("cur"))
    percent = int(match.group("pct")) if match.group("pct") else int(current * 100 / max(total, 1))
    return {
        "stage": "denoising",
        "stage_label": stage_label_for("denoising"),
        "progress_current": current,
        "progress_total": total,
        "progress_percent": percent,
        "eta_hint": match.group("eta").strip(),
        "elapsed_hint": match.group("elapsed").strip(),
    }


def append_task_log(task_id: str, line: str) -> None:
    text = line.strip()
    if not text:
        return

    updates: Dict[str, Any] = {"latest_log_line": text}
    progress = parse_denoising_progress(text)
    if progress:
        updates.update(progress)
    else:
        if "Loading models" in text or "Starting model loading process" in text:
            updates.update({"stage": "loading_models", "stage_label": stage_label_for("loading_models")})
        elif "Loading " in text and "model" in text:
            updates.update({"stage": "loading_models", "stage_label": stage_label_for("loading_models")})
        elif "Processing single video" in text or "Text prompt" in text:
            updates.update({"stage": "extracting_features", "stage_label": stage_label_for("extracting_features")})
        elif "Releasing feature extraction models" in text:
            updates.update({"stage": "extracting_features", "stage_label": stage_label_for("extracting_features")})
        elif "Audio saved to:" in text:
            updates.update(
                {
                    "stage": "saving_audio",
                    "stage_label": stage_label_for("saving_audio"),
                    "progress_current": 1,
                    "progress_total": 1,
                    "progress_percent": 100,
                }
            )
        elif "Merging audio" in text:
            updates.update({"stage": "merging", "stage_label": stage_label_for("merging")})
        elif "Processing completed!" in text:
            updates.update({"stage": "completed", "stage_label": stage_label_for("completed")})

    with EDITING_TASKS_LOCK:
        task = EDITING_TASKS[task_id]
        log_lines = task.setdefault("log_lines", [])
        log_lines.append(text)
        task["log_tail"] = "\n".join(log_lines[-80:])
        task.update(updates)


def iter_process_lines(stream: Any) -> List[str]:
    buffer = ""
    while True:
        chunk = stream.read(1)
        if chunk == "":
            if buffer.strip():
                yield buffer
            break
        if chunk in {"\n", "\r"}:
            if buffer.strip():
                yield buffer
            buffer = ""
            continue
        buffer += chunk


def clip_video_segment(
    input_path: Path, output_path: Path, start_s: float, end_s: float
) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Video file not found: {input_path}")
    duration = end_s - start_s
    if duration <= 0:
        raise ValueError("Editing interval must have positive duration.")
    ffmpeg_bin = DEFAULT_FOLEY_FFMPEG
    if not ffmpeg_bin.exists():
        raise FileNotFoundError(f"ffmpeg not found: {ffmpeg_bin}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(ffmpeg_bin),
        "-y",
        "-ss",
        f"{start_s:.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Clip extraction failed:\n{result.stdout[-4000:]}")


def build_foley_command(
    clip_path: Path,
    prompt: str,
    output_dir: Path,
    foley_cfg: Dict[str, Any],
) -> List[str]:
    command = [
        "conda",
        "run",
        "-n",
        foley_cfg["conda_env"],
        "python",
        "infer.py",
        "--model_path",
        foley_cfg["model_path"],
        "--model_size",
        foley_cfg["model_size"],
        "--gpu_id",
        str(foley_cfg["gpu_id"]),
        "--single_video",
        str(clip_path),
        "--single_prompt",
        prompt,
        "--output_dir",
        str(output_dir),
        "--guidance_scale",
        str(foley_cfg["guidance_scale"]),
        "--num_inference_steps",
        str(foley_cfg["num_inference_steps"]),
    ]
    if foley_cfg["enable_offload"]:
        command.append("--enable_offload")
    return command


def mirror_worker_task(local_task_id: str, remote_task: Dict[str, Any]) -> None:
    update_task(
        local_task_id,
        worker_task_id=remote_task.get("task_id", ""),
        stage=remote_task.get("stage", ""),
        stage_label=remote_task.get("stage_label", stage_label_for(remote_task.get("stage", ""))),
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
    source_video = Path(task["video_path"]).resolve()
    prompt = task["editing_prompt"]
    start_s = float(task["editing_start"])
    end_s = float(task["editing_end"])
    run_dir = Path(task["run_dir"]).resolve()
    clip_path = run_dir / "editing_clip.mp4"
    output_dir = run_dir / "foley_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        update_task(task_id, status="clipping", started_at=utc_now_iso())
        clip_video_segment(source_video, clip_path, start_s, end_s)

        foley_cfg = get_foley_config()
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
            stage_label=stage_label_for("loading_models"),
            clip_path=str(clip_path),
            clip_media_url=media_url_for_path(clip_path),
            clip_preview_url=preview_url_for_path(clip_path),
            foley_config=foley_cfg,
        )

        FOLEY_WORKER_MANAGER.ensure_worker(foley_cfg)
        remote_payload = FOLEY_WORKER_MANAGER.request(
            "POST",
            "/run",
            payload={
                "clip_path": str(clip_path),
                "prompt": prompt,
                "output_dir": str(output_dir),
                "guidance_scale": foley_cfg["guidance_scale"],
                "num_inference_steps": foley_cfg["num_inference_steps"],
                "save_video": True,
            },
            timeout=30,
        )
        remote_task_id = remote_payload["task"]["task_id"]
        mirror_worker_task(task_id, remote_payload["task"])

        while True:
            polled = FOLEY_WORKER_MANAGER.request(
                "GET",
                f"/task?id={parse.quote(remote_task_id)}",
                timeout=10,
            )
            remote_task = polled["task"]
            mirror_worker_task(task_id, remote_task)

            if remote_task["status"] == "completed":
                audio_path = Path(remote_task["audio_path"])
                video_path = Path(remote_task["merged_video_path"]) if remote_task.get("merged_video_path") else None
                update_task(
                    task_id,
                    status="completed",
                    stage="completed",
                    stage_label=stage_label_for("completed"),
                    finished_at=utc_now_iso(),
                    audio_path=str(audio_path),
                    audio_media_url=media_url_for_path(audio_path),
                    merged_video_path=str(video_path) if video_path and video_path.exists() else "",
                    merged_video_media_url=media_url_for_path(video_path)
                    if video_path and video_path.exists()
                    else "",
                    merged_video_preview_url=preview_url_for_path(video_path)
                    if video_path and video_path.exists()
                    else "",
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
            stage_label=stage_label_for("failed"),
            finished_at=utc_now_iso(),
            error=str(exc),
        )


def start_editing_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    video_path = str(payload.get("video_path", "")).strip()
    editing_prompt = str(payload.get("editing_prompt", "")).strip()
    if not video_path:
        raise ValueError("video_path is required.")
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
    run_dir = EDITING_RUNS_DIR / task_id
    run_dir.mkdir(parents=True, exist_ok=True)
    task = {
        "task_id": task_id,
        "status": "queued",
        "created_at": utc_now_iso(),
        "video_path": video_path,
        "video_name": os.path.basename(video_path),
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
        "stage_label": stage_label_for("queued"),
        "progress_current": 0,
        "progress_total": 0,
        "progress_percent": 0,
        "eta_hint": "",
        "elapsed_hint": "",
    }
    with EDITING_TASKS_LOCK:
        EDITING_TASKS[task_id] = task

    thread = threading.Thread(target=run_foley_task, args=(task_id,), daemon=True)
    thread.start()
    return get_task(task_id)


class UIRequestHandler(BaseHTTPRequestHandler):
    server_version = "UMRMLabelUI/0.2"

    def do_GET(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if request_needs_auth(parsed) and not is_request_authorized(self, parsed):
                self.send_json({"ok": False, "error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if parsed.path == "/api/config":
                settings = get_public_settings()
                vlm_cfg = get_vlm_config()
                annotator_id = get_request_annotator_id(self, allow_fallback=False)
                annotation_file = resolve_annotation_file(None, annotator_id)
                self.send_json(
                    {
                        "default_video_root": str(DEFAULT_VIDEO_ROOT),
                        "upload_dir": str(UPLOAD_DIR),
                        "annotation_file": str(annotation_file),
                        "vlm_configured": bool(vlm_cfg["api_base"] and vlm_cfg["model"]),
                        "translation_configured": bool(
                            get_translation_config()["api_base"]
                            and get_translation_config()["model"]
                        ),
                        "preview_enabled": PREVIEW_ENABLED,
                        "preview_height": PREVIEW_HEIGHT,
                        "low_stream_enabled": LOW_STREAM_ENABLED,
                        "low_stream_height": LOW_STREAM_HEIGHT,
                        "low_stream_video_bitrate": LOW_STREAM_VIDEO_BITRATE,
                        "low_stream_audio_bitrate": LOW_STREAM_AUDIO_BITRATE,
                        "shared_access_enabled": bool(UI_SHARED_TOKEN),
                        "settings": settings,
                    }
                )
                return

            if parsed.path == "/api/settings":
                self.send_json({"settings": get_public_settings()})
                return

            if parsed.path == "/api/videos":
                query = parse.parse_qs(parsed.query)
                root = resolve_root((query.get("root") or [""])[0])
                videos = list_videos(root)
                self.send_json({"root": str(root), "videos": videos})
                return

            if parsed.path == "/api/video-metadata":
                query = parse.parse_qs(parsed.query)
                raw_path = (query.get("path") or [""])[0]
                source_path = Path(parse.unquote(raw_path)).expanduser().resolve()
                duration_seconds = probe_video_duration_seconds(source_path)
                self.send_json(
                    {
                        "path": str(source_path),
                        "duration_seconds": duration_seconds,
                    }
                )
                return

            if parsed.path == "/api/video-statuses":
                query = parse.parse_qs(parsed.query)
                root = resolve_root((query.get("root") or [""])[0])
                statuses = get_video_statuses_for_root(root)
                self.send_json({"root": str(root), "statuses": statuses})
                return

            if parsed.path == "/api/annotations":
                query = parse.parse_qs(parsed.query)
                annotator_id = get_required_annotator_id(self)
                annotation_file = resolve_annotation_file(
                    (query.get("path") or [""])[0], annotator_id
                )
                annotations = normalize_annotations_for_output(load_annotations(annotation_file))
                target_video_path = str((query.get("video_path") or [""])[0]).strip()
                if target_video_path:
                    normalized_target = normalize_path_string(target_video_path)
                    annotations = [
                        item
                        for item in annotations
                        if normalize_path_string(item.get("video_path", "")) == normalized_target
                    ]
                self.send_json({"annotations": annotations, "annotation_file": str(annotation_file)})
                return

            if parsed.path == "/api/editing-task":
                query = parse.parse_qs(parsed.query)
                task_id = (query.get("id") or [""])[0]
                self.send_json({"task": get_task(task_id)})
                return

            if parsed.path == "/media":
                query = parse.parse_qs(parsed.query)
                raw_path = (query.get("path") or [""])[0]
                media_path = Path(parse.unquote(raw_path)).expanduser().resolve()
                if not media_path.exists() or not media_path.is_file():
                    self.send_error(HTTPStatus.NOT_FOUND, "Media file not found.")
                    return
                self.serve_file(media_path, channel="media")
                return

            if parsed.path == "/preview":
                query = parse.parse_qs(parsed.query)
                raw_path = (query.get("path") or [""])[0]
                source_path = Path(parse.unquote(raw_path)).expanduser().resolve()
                preview_path = ensure_preview_available(source_path)
                self.serve_file(preview_path, channel="preview")
                return

            if parsed.path == "/stream-low":
                query = parse.parse_qs(parsed.query)
                raw_path = (query.get("path") or [""])[0]
                source_path = Path(parse.unquote(raw_path)).expanduser().resolve()
                self.serve_low_bitrate_stream(source_path)
                return

            if parsed.path.startswith("/hls/"):
                if not HLS_ENABLED:
                    self.send_error(HTTPStatus.NOT_FOUND, "HLS is disabled.")
                    return
                match = re.fullmatch(r"/hls/([0-9a-f]{40})/([A-Za-z0-9._-]+)", parsed.path)
                if not match:
                    self.send_error(HTTPStatus.NOT_FOUND, "Invalid HLS path.")
                    return
                cache_key, file_name = match.groups()
                hls_dir = hls_cache_dir_for_key(cache_key)
                target_path = (hls_dir / file_name).resolve()
                if not str(target_path).startswith(str(hls_dir.resolve())):
                    self.send_error(HTTPStatus.FORBIDDEN, "Forbidden.")
                    return
                if file_name == "index.m3u8":
                    query = parse.parse_qs(parsed.query)
                    raw_source_path = (query.get("path") or [""])[0]
                    if not raw_source_path:
                        self.send_error(HTTPStatus.BAD_REQUEST, "Missing source path.")
                        return
                    source_path = Path(parse.unquote(raw_source_path)).expanduser().resolve()
                    ensure_hls_available(source_path, cache_key)
                if not target_path.exists() or not target_path.is_file():
                    self.send_error(HTTPStatus.NOT_FOUND, "HLS file not found.")
                    return
                self.serve_file(target_path, channel="hls")
                return

            self.serve_static(parsed.path)
        except Exception as exc:
            self.handle_exception(exc)

    def do_POST(self) -> None:
        try:
            parsed = parse.urlparse(self.path)
            if request_needs_auth(parsed) and not is_request_authorized(self, parsed):
                self.send_json({"ok": False, "error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return

            if parsed.path == "/api/upload":
                self.handle_upload()
                return

            if parsed.path == "/api/settings":
                payload = read_json_body(self)
                settings = save_runtime_settings(payload)
                self.send_json(
                    {
                        "ok": True,
                        "settings": {
                            "vlm": {
                                "api_base": settings["vlm"]["api_base"],
                                "model": settings["vlm"]["model"],
                                "api_key_set": bool(settings["vlm"]["api_key"]),
                                "api_key_masked": mask_secret(settings["vlm"]["api_key"]),
                                "translate_to_zh": bool(settings["vlm"].get("translate_to_zh", True)),
                            },
                            "foley": settings["foley"],
                        },
                    }
                )
                return

            if parsed.path == "/api/vlm-understand":
                payload = read_json_body(self)
                result = run_vlm_understanding(payload)
                self.send_json(result)
                return

            if parsed.path == "/api/translate":
                payload = read_json_body(self)
                result = translate_text(str(payload.get("text", "")))
                self.send_json(result)
                return

            if parsed.path == "/api/translate-en-zh":
                payload = read_json_body(self)
                result = translate_text_en_to_zh(str(payload.get("text", "")))
                self.send_json(result)
                return

            if parsed.path == "/api/annotations":
                payload = read_json_body(self)
                annotator_id = get_required_annotator_id(self, payload)
                annotation_file = resolve_annotation_file(
                    payload.get("annotation_path"), annotator_id
                )
                validate_annotation_payload(payload)
                video_path = str(payload.get("video_path", "")).strip()
                if not video_path:
                    raise ValueError("video_path 不能为空。")
                payload["annotator_id"] = annotator_id
                payload["saved_at"] = utc_now_iso()
                payload["annotation_id"] = str(uuid.uuid4())
                payload["annotation_path"] = str(annotation_file)
                append_annotation(payload, annotation_file)
                self.send_json(
                    {
                        "ok": True,
                        "saved_at": payload["saved_at"],
                        "annotation_id": payload["annotation_id"],
                        "annotation_file": str(annotation_file),
                    }
                )
                return

            if parsed.path == "/api/annotations-delete":
                payload = read_json_body(self)
                annotator_id = get_required_annotator_id(self, payload)
                annotation_file = resolve_annotation_file(
                    payload.get("annotation_path"), annotator_id
                )
                deleted_count = delete_annotation_by_id(
                    annotation_file,
                    str(payload.get("annotation_id", "")).strip(),
                    annotator_id,
                )
                if deleted_count <= 0:
                    raise ValueError("未找到可删除的标注，或该标注不属于当前用户。")
                self.send_json(
                    {
                        "ok": True,
                        "deleted_count": deleted_count,
                        "annotation_file": str(annotation_file),
                    }
                )
                return

            if parsed.path == "/api/video-claim":
                payload = read_json_body(self)
                video_path = str(payload.get("video_path", "")).strip()
                if not video_path:
                    raise ValueError("video_path 不能为空。")
                video_relative_path = str(payload.get("video_relative_path", "")).strip()
                annotator_id = get_required_annotator_id(self, payload)
                result = claim_video(video_path, video_relative_path, annotator_id)
                self.send_json({"ok": True, "status_entry": result["status_entry"]})
                return

            if parsed.path == "/api/video-release":
                payload = read_json_body(self)
                video_path = str(payload.get("video_path", "")).strip()
                if not video_path:
                    raise ValueError("video_path 不能为空。")
                annotator_id = get_required_annotator_id(self, payload)
                status_entry = release_video_claim(video_path, annotator_id)
                self.send_json({"ok": True, "status_entry": status_entry})
                return

            if parsed.path == "/api/video-heartbeat":
                payload = read_json_body(self)
                video_path = str(payload.get("video_path", "")).strip()
                if not video_path:
                    raise ValueError("video_path 不能为空。")
                annotator_id = get_required_annotator_id(self, payload)
                status_entry = heartbeat_video_claim(video_path, annotator_id)
                self.send_json({"ok": True, "status_entry": status_entry})
                return

            if parsed.path == "/api/video-status":
                payload = read_json_body(self)
                video_path = str(payload.get("video_path", "")).strip()
                if not video_path:
                    raise ValueError("video_path 不能为空。")
                status_value = str(payload.get("status", "")).strip().lower()
                annotator_id = get_required_annotator_id(self, payload)
                status_entry = update_video_status(video_path, status_value, annotator_id)
                self.send_json({"ok": True, "status_entry": status_entry})
                return

            if parsed.path == "/api/editing":
                payload = read_json_body(self)
                task = start_editing_task(payload)
                self.send_json({"ok": True, "task": task})
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint.")
        except Exception as exc:
            self.handle_exception(exc)

    def handle_upload(self) -> None:
        form = read_multipart_form(self)
        file_item = form["files"].get("file")
        if file_item is None or not file_item.get("filename"):
            raise ValueError("Missing uploaded file.")

        root_value = form["fields"].get("root", "")
        target_root = resolve_root(root_value) if root_value else UPLOAD_DIR
        target_root.mkdir(parents=True, exist_ok=True)

        filename = sanitize_filename(file_item["filename"])
        target_path = target_root / filename
        stem = target_path.stem
        suffix = target_path.suffix
        index = 1
        while target_path.exists():
            target_path = target_root / f"{stem}_{index}{suffix}"
            index += 1

        with target_path.open("wb") as fh:
            fh.write(file_item["content"])

        self.send_json(
            {
                "ok": True,
                "saved_path": str(target_path),
                "media_url": media_url_for_path(target_path),
            }
        )

    def serve_static(self, request_path: str) -> None:
        path = request_path or "/"
        if path == "/":
            path = "/index.html"
        candidate = (STATIC_DIR / path.lstrip("/")).resolve()
        if not str(candidate).startswith(str(STATIC_DIR.resolve())):
            self.send_error(HTTPStatus.FORBIDDEN, "Forbidden.")
            return
        if not candidate.exists() or not candidate.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Static file not found.")
            return
        self.serve_file(candidate, allow_range=False, channel="static")

    def log_video_transfer(
        self,
        event: str,
        channel: str,
        file_path: Path,
        bytes_sent: int = 0,
        elapsed_ms: float = 0.0,
        detail: str = "",
    ) -> None:
        client_ip = self.client_address[0] if self.client_address else "-"
        sys.stderr.write(
            f"[{self.log_date_time_string()}] [video-transfer] {event} "
            f"channel={channel} client={client_ip} file={file_path} "
            f"bytes={bytes_sent} elapsed_ms={elapsed_ms:.1f} detail={detail}\n"
        )

    def serve_file(self, file_path: Path, allow_range: bool = True, channel: str = "media") -> None:
        file_size = file_path.stat().st_size
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        range_header = self.headers.get("Range")
        should_log_transfer = channel in {"media", "preview", "hls"}

        if allow_range and range_header:
            match = re.match(r"bytes=(\d*)-(\d*)", range_header)
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
            bytes_sent = 0
            started_at = time.perf_counter()
            outcome = "completed"
            if should_log_transfer:
                self.log_video_transfer(
                    "start",
                    channel,
                    file_path,
                    detail=f"mode=range range=bytes {start}-{end}/{file_size}",
                )
            with file_path.open("rb") as fh:
                fh.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = fh.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                        outcome = "client_disconnected"
                        break
                    bytes_sent += len(chunk)
                    remaining -= len(chunk)
            if should_log_transfer:
                self.log_video_transfer(
                    "end",
                    channel,
                    file_path,
                    bytes_sent=bytes_sent,
                    elapsed_ms=(time.perf_counter() - started_at) * 1000.0,
                    detail=f"mode=range outcome={outcome}",
                )
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(file_size))
        if allow_range:
            self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        bytes_sent = 0
        started_at = time.perf_counter()
        outcome = "completed"
        if should_log_transfer:
            self.log_video_transfer(
                "start",
                channel,
                file_path,
                detail=f"mode=full size={file_size}",
            )
        with file_path.open("rb") as fh:
            try:
                while True:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    bytes_sent += len(chunk)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                outcome = "client_disconnected"
        if should_log_transfer:
            self.log_video_transfer(
                "end",
                channel,
                file_path,
                bytes_sent=bytes_sent,
                elapsed_ms=(time.perf_counter() - started_at) * 1000.0,
                detail=f"mode=full outcome={outcome}",
            )

    def serve_low_bitrate_stream(self, source_path: Path) -> None:
        if not LOW_STREAM_ENABLED:
            self.serve_file(source_path, channel="media")
            return
        resolved = source_path.resolve()
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError(f"Media file not found: {resolved}")
        ffmpeg_bin = resolve_ffmpeg_bin()
        cmd = [
            str(ffmpeg_bin),
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(resolved),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-b:v",
            LOW_STREAM_VIDEO_BITRATE,
            "-maxrate",
            LOW_STREAM_VIDEO_BITRATE,
            "-bufsize",
            LOW_STREAM_VIDEO_BITRATE,
            "-r",
            str(LOW_STREAM_FPS),
            "-g",
            str(LOW_STREAM_FPS),
            "-keyint_min",
            str(LOW_STREAM_FPS),
            "-sc_threshold",
            "0",
        ]
        if LOW_STREAM_HEIGHT > 0:
            cmd.extend(["-vf", f"scale=-2:min({LOW_STREAM_HEIGHT}\\,ih):flags=bilinear"])
        cmd.extend(
            [
                "-c:a",
                "aac",
                "-b:a",
                LOW_STREAM_AUDIO_BITRATE,
                "-movflags",
                "frag_keyframe+empty_moov+default_base_moof",
                "-flush_packets",
                "1",
                "-f",
                "mp4",
                "pipe:1",
            ]
        )
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            bufsize=0,
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Accept-Ranges", "none")
        self.end_headers()
        bytes_sent = 0
        started_at = time.perf_counter()
        outcome = "completed"
        first_chunk_logged = False
        self.log_video_transfer(
            "start",
            "stream-low",
            resolved,
            detail="mode=transcode",
        )
        try:
            if not process.stdout:
                return
            while True:
                chunk = process.stdout.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
                bytes_sent += len(chunk)
                if not first_chunk_logged:
                    first_chunk_logged = True
                    self.log_video_transfer(
                        "first-chunk",
                        "stream-low",
                        resolved,
                        bytes_sent=bytes_sent,
                        elapsed_ms=(time.perf_counter() - started_at) * 1000.0,
                        detail="mode=transcode",
                    )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            outcome = "client_disconnected"
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=1.0)
            self.log_video_transfer(
                "end",
                "stream-low",
                resolved,
                bytes_sent=bytes_sent,
                elapsed_ms=(time.perf_counter() - started_at) * 1000.0,
                detail=f"mode=transcode outcome={outcome}",
            )

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json_dumps(payload)
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return

    def handle_exception(self, exc: Exception) -> None:
        if is_client_disconnect_error(exc):
            return
        traceback.print_exc()
        status = HTTPStatus.BAD_REQUEST
        if isinstance(exc, FileNotFoundError):
            status = HTTPStatus.NOT_FOUND
        elif isinstance(exc, RuntimeError):
            status = HTTPStatus.BAD_GATEWAY
        try:
            self.send_json(
                {
                    "ok": False,
                    "error": str(exc),
                    "type": exc.__class__.__name__,
                },
                status=status,
            )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write(
            "[%s] %s\n"
            % (self.log_date_time_string(), format % args)
        )


def run_server() -> None:
    ensure_dirs()
    port = int(os.environ.get("UMRM_UI_PORT", "8765"))
    host = os.environ.get("UMRM_UI_HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, port), UIRequestHandler)
    print(f"UMRM UI server running at http://{host}:{port}")
    print(f"Default video root: {DEFAULT_VIDEO_ROOT}")
    print(f"Annotation file: {ANNOTATION_FILE}")
    if PREVIEW_ENABLED:
        print(
            "Preview cache enabled: "
            f"height<={PREVIEW_HEIGHT or 'source'}, video={PREVIEW_VIDEO_BITRATE}, audio={PREVIEW_AUDIO_BITRATE}"
        )
    if LOW_STREAM_ENABLED:
        print(
            "Low stream enabled: "
            f"height<={LOW_STREAM_HEIGHT or 'source'}, fps={LOW_STREAM_FPS}, "
            f"video={LOW_STREAM_VIDEO_BITRATE}, audio={LOW_STREAM_AUDIO_BITRATE}"
        )
    if UI_SHARED_TOKEN:
        print("Shared access token enabled. Open from other machines with ?token=<your-token>.")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
