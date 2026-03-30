import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib import request

import annotator_gateway as gateway
import inference_server as infer
import server as core


class SplitServerTests(unittest.TestCase):
    def setUp(self) -> None:
        core.EDITING_TASKS.clear()

    def test_video_id_is_consistent_between_gateway_and_inference(self) -> None:
        vid1 = gateway.compute_video_id("a/b/c.mp4", 1234, 999)
        vid2 = infer.compute_video_id("a/b/c.mp4", 1234, 999)
        self.assertEqual(vid1, vid2)
        self.assertEqual(len(vid1), 16)

    def test_gateway_list_videos_with_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "demo.mp4"
            video.write_bytes(b"abc")
            items = gateway.list_videos_with_id(root)
            self.assertEqual(len(items), 1)
            self.assertIn("video_id", items[0])
            self.assertEqual(items[0]["name"], "demo.mp4")

    def test_inference_resolve_video_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            video = root / "x.mp4"
            video.write_bytes(b"123")
            stat = video.stat()
            relative = "x.mp4"
            vid = infer.compute_video_id(relative, stat.st_size, stat.st_mtime_ns)
            with mock.patch.object(infer, "INFER_ROOT", root):
                infer.refresh_video_index()
                resolved = infer.resolve_video_path(vid)
            self.assertEqual(resolved, video.resolve())

    def test_inference_start_task_validation(self) -> None:
        with self.assertRaises(ValueError):
            infer.start_editing_task({"editing_prompt": "a", "editing_start": 0, "editing_end": 1})
        with self.assertRaises(ValueError):
            infer.start_editing_task({"video_id": "v1", "editing_start": 0, "editing_end": 1})

    def test_gateway_start_task_validation(self) -> None:
        with self.assertRaises(ValueError):
            gateway.start_editing_task({"editing_prompt": "a", "editing_start": 0, "editing_end": 1})
        with self.assertRaises(ValueError):
            gateway.start_editing_task({"video_id": "v1", "editing_start": 0, "editing_end": 1})
        with self.assertRaises(ValueError):
            gateway.start_editing_task(
                {
                    "video_id": "v1",
                    "editing_prompt": "p",
                    "editing_start": 1,
                    "editing_end": 2,
                    "clip_start": 1.5,
                    "clip_end": 2.5,
                }
            )

    def test_gateway_run_remote_editing_task_success(self) -> None:
        task_id = "t100"
        core.EDITING_TASKS[task_id] = {
            "task_id": task_id,
            "status": "queued",
            "created_at": "2026-01-01T00:00:00+00:00",
            "video_id": "v1",
            "editing_prompt": "p",
            "editing_start": 1.0,
            "editing_end": 2.0,
            "run_dir": str(Path(tempfile.gettempdir()) / "runx"),
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
        fake_local_audio = Path(tempfile.gettempdir()) / "gw_test_audio.wav"
        fake_local_audio.write_bytes(b"RIFF")
        calls = {"count": 0}

        def fake_req(method: str, path: str, payload=None, timeout=None):
            if method == "POST":
                return {"ok": True, "task": {"task_id": "remote-1", "status": "queued", "stage": "queued"}}
            calls["count"] += 1
            return {
                "task": {
                    "task_id": "remote-1",
                    "status": "completed",
                    "stage": "completed",
                    "stage_label": "已完成",
                    "audio_download_url": "/api/infer/audio?id=remote-1",
                    "audio_filename": "a.wav",
                    "progress_current": 1,
                    "progress_total": 1,
                    "progress_percent": 100,
                    "eta_hint": "",
                    "elapsed_hint": "",
                    "latest_log_line": "",
                    "log_tail": "",
                }
            }

        with mock.patch.object(gateway, "inference_request", side_effect=fake_req), mock.patch.object(
            gateway, "download_audio", return_value=fake_local_audio
        ):
            gateway.run_remote_editing_task(task_id)
        done = core.get_task(task_id)
        self.assertEqual(done["status"], "completed")
        self.assertTrue(done["audio_path"].endswith("gw_test_audio.wav"))
        self.assertGreaterEqual(calls["count"], 1)

    def test_inference_run_foley_task_success(self) -> None:
        task_id = "t200"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = root / "v.mp4"
            source.write_bytes(b"video")
            out_audio = root / "foley_output" / "editing_clip_generated.wav"
            out_audio.parent.mkdir(parents=True, exist_ok=True)
            out_audio.write_bytes(b"RIFF")
            core.EDITING_TASKS[task_id] = {
                "task_id": task_id,
                "status": "queued",
                "created_at": "2026-01-01T00:00:00+00:00",
                "video_id": "vidx",
                "editing_prompt": "p",
                "editing_start": 1.0,
                "editing_end": 2.0,
                "run_dir": str(root),
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
            fake_worker = SimpleNamespace()
            fake_worker.ensure_worker = mock.Mock()
            fake_worker.request = mock.Mock(
                side_effect=[
                    {"task": {"task_id": "rid", "status": "queued", "stage": "queued"}},
                    {
                        "task": {
                            "task_id": "rid",
                            "status": "completed",
                            "stage": "completed",
                            "stage_label": "已完成",
                            "audio_path": str(out_audio),
                            "merged_video_path": "",
                            "progress_current": 1,
                            "progress_total": 1,
                            "progress_percent": 100,
                            "eta_hint": "",
                            "elapsed_hint": "",
                            "latest_log_line": "",
                            "log_tail": "",
                        }
                    },
                ]
            )

            with mock.patch.object(infer, "resolve_video_path", return_value=source), mock.patch.object(
                core, "clip_video_segment", return_value=None
            ), mock.patch.object(
                core, "get_foley_config", return_value={"project_dir": str(root), "model_path": str(root), "guidance_scale": 4.5, "num_inference_steps": 50}
            ), mock.patch.object(
                core, "FOLEY_WORKER_MANAGER", fake_worker
            ):
                infer.run_foley_task(task_id)
            done = infer.get_task(task_id)
            self.assertEqual(done["status"], "completed")
            self.assertTrue(done["audio_download_url"].startswith("/api/infer/audio?id="))

    def test_inference_http_endpoints(self) -> None:
        class StubHandler(infer.InferenceHandler):
            pass

        with tempfile.TemporaryDirectory() as td:
            wav = Path(td) / "x.wav"
            wav.write_bytes(b"RIFF")

            with mock.patch.object(infer, "start_editing_task", return_value={"task_id": "a1", "status": "queued"}), mock.patch.object(
                infer,
                "get_task",
                return_value={
                    "task_id": "a1",
                    "status": "completed",
                    "audio_path": str(wav),
                    "stage": "completed",
                    "stage_label": "已完成",
                },
            ):
                httpd = infer.ThreadingHTTPServer(("127.0.0.1", 0), StubHandler)
                port = httpd.server_address[1]
                th = threading.Thread(target=httpd.serve_forever, daemon=True)
                th.start()
                try:
                    with request.urlopen(f"http://127.0.0.1:{port}/api/infer/editing-task?id=a1", timeout=5) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                    self.assertEqual(data["task"]["task_id"], "a1")

                    req = request.Request(
                        f"http://127.0.0.1:{port}/api/infer/editing",
                        data=json.dumps({"video_id": "x", "editing_start": 1, "editing_end": 2, "editing_prompt": "p"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with request.urlopen(req, timeout=5) as resp:
                        posted = json.loads(resp.read().decode("utf-8"))
                    self.assertTrue(posted["ok"])

                    with request.urlopen(f"http://127.0.0.1:{port}/api/infer/audio?id=a1", timeout=5) as resp:
                        content = resp.read()
                    self.assertEqual(content, b"RIFF")
                finally:
                    httpd.shutdown()
                    th.join(timeout=5)
                    httpd.server_close()
                    time.sleep(0.1)


if __name__ == "__main__":
    unittest.main()
