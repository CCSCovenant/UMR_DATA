#!/usr/bin/env python3
import json
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
SETTINGS_FILE = DATA_DIR / "runtime_settings.json"
EDITING_RUNS_DIR = BASE_DIR / "editing_runs"
PREVIEW_CACHE_DIR = BASE_DIR / "preview_cache"
DEFAULT_VIDEO_ROOT = Path(
    os.environ.get("UMRM_VIDEO_ROOT", "/data/cws/Project/UMRM/data/UMRM/videos")
).resolve()
VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v"}
MAX_FRAME_COUNT = 10
PREVIEW_ENABLED = os.environ.get("UMRM_PREVIEW_ENABLED", "1") != "0"
PREVIEW_HEIGHT = max(0, int(os.environ.get("UMRM_PREVIEW_HEIGHT", "540")))
PREVIEW_VIDEO_BITRATE = os.environ.get("UMRM_PREVIEW_VIDEO_BITRATE", "900k").strip() or "900k"
PREVIEW_AUDIO_BITRATE = os.environ.get("UMRM_PREVIEW_AUDIO_BITRATE", "96k").strip() or "96k"
UI_SHARED_TOKEN = os.environ.get("UMRM_SHARED_TOKEN", "").strip()

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

EDITING_TASKS: Dict[str, Dict[str, Any]] = {}
EDITING_TASKS_LOCK = threading.Lock()
PREVIEW_LOCKS: Dict[str, threading.Lock] = {}
PREVIEW_LOCKS_GUARD = threading.Lock()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EDITING_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    PREVIEW_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def json_dumps(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


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
                "preview_media_url": preview_url_for_path(path),
                "preview_cached": preview_cache_path(path).exists(),
            }
        )
    return videos


def media_url_for_path(path: Path) -> str:
    return f"/media?path={parse.quote(str(path.resolve()))}"


def preview_url_for_path(path: Path) -> str:
    return f"/preview?path={parse.quote(str(path.resolve()))}"


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
            cmd.extend(["-vf", f"scale=-2:min({PREVIEW_HEIGHT},ih):flags=lanczos"])
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
    return parsed.path.startswith("/api/") or parsed.path in {"/media", "/preview"}


def normalize_chat_completion_url(api_base: str) -> str:
    base = api_base.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


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
        with request.urlopen(req, timeout=timeout) as resp:
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


def translate_text(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("Text to translate cannot be empty.")

    cfg = get_translation_config()
    if cfg["api_base"] and cfg["model"]:
        prompt = (
            "Translate the following Chinese text into concise, natural English. "
            "Return translated English only. Do not add notes, quotes, or explanations.\n\n"
            f"{cleaned}"
        )
        response = call_chat_completion(
            api_base=cfg["api_base"],
            api_key=cfg["api_key"],
            model=cfg["model"],
            messages=[{"role": "user", "content": prompt}],
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
                "sl": "zh-CN",
                "tl": "en",
                "dt": "t",
                "q": cleaned,
            }
        )
    )
    with request.urlopen(google_url, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    translated = "".join(item[0] for item in payload[0] if item and item[0])
    return {"translated_text": translated, "provider": "google-gtx"}


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

    instruction = (
        "You are helping a human annotator write a current state description for a selected "
        "video clip. Base everything only on visible evidence in the frames.\n"
        "Write concise English.\n"
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


def append_annotation(payload: Dict[str, Any]) -> None:
    ensure_dirs()
    with ANNOTATION_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


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
                self.send_json(
                    {
                        "default_video_root": str(DEFAULT_VIDEO_ROOT),
                        "upload_dir": str(UPLOAD_DIR),
                        "annotation_file": str(ANNOTATION_FILE),
                        "vlm_configured": bool(vlm_cfg["api_base"] and vlm_cfg["model"]),
                        "translation_configured": bool(
                            get_translation_config()["api_base"]
                            and get_translation_config()["model"]
                        ),
                        "preview_enabled": PREVIEW_ENABLED,
                        "preview_height": PREVIEW_HEIGHT,
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

            if parsed.path == "/api/annotations":
                if not ANNOTATION_FILE.exists():
                    self.send_json({"annotations": []})
                    return
                annotations = []
                with ANNOTATION_FILE.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        annotations.append(json.loads(line))
                self.send_json({"annotations": annotations[-50:]})
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
                self.serve_file(media_path)
                return

            if parsed.path == "/preview":
                query = parse.parse_qs(parsed.query)
                raw_path = (query.get("path") or [""])[0]
                source_path = Path(parse.unquote(raw_path)).expanduser().resolve()
                preview_path = ensure_preview_available(source_path)
                self.serve_file(preview_path)
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

            if parsed.path == "/api/annotations":
                payload = read_json_body(self)
                payload["saved_at"] = utc_now_iso()
                append_annotation(payload)
                self.send_json({"ok": True, "saved_at": payload["saved_at"]})
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
        self.serve_file(candidate, allow_range=False)

    def serve_file(self, file_path: Path, allow_range: bool = True) -> None:
        file_size = file_path.stat().st_size
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        range_header = self.headers.get("Range")

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
        if allow_range:
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
    if UI_SHARED_TOKEN:
        print("Shared access token enabled. Open from other machines with ?token=<your-token>.")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
