#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict

import server


def _load_table_with_cleanup() -> Dict[str, Dict[str, Any]]:
    table = server.load_video_status_table()
    changed = server.cleanup_expired_claims(table)
    if changed:
        server.save_video_status_table(table)
    return table


def cmd_list(args: argparse.Namespace) -> int:
    table = _load_table_with_cleanup()
    rows = []
    for video_path, entry in table.items():
        status = str(entry.get("status", "unclaimed"))
        if args.status and status != args.status:
            continue
        rows.append(
            {
                "video_path": video_path,
                "video_relative_path": entry.get("video_relative_path", ""),
                "status": status,
                "claimed_by": entry.get("claimed_by", ""),
                "claimed_at": entry.get("claimed_at", ""),
                "claim_expires_at": entry.get("claim_expires_at", ""),
                "completed_at": entry.get("completed_at", ""),
                "verified_at": entry.get("verified_at", ""),
                "updated_at": entry.get("updated_at", ""),
            }
        )
    rows.sort(key=lambda item: (item["status"], item["video_path"]))
    print(json.dumps({"count": len(rows), "items": rows}, ensure_ascii=False, indent=2))
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    del args
    table = server.load_video_status_table()
    changed = server.cleanup_expired_claims(table)
    if changed:
        server.save_video_status_table(table)
    print(json.dumps({"ok": True, "changed": bool(changed)}, ensure_ascii=False))
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    target = str(Path(args.video_path).expanduser().resolve())
    table = _load_table_with_cleanup()
    entry = server.resolve_status_entry(table, target, "")
    if entry["status"] == server.VIDEO_STATUS_CLAIMED:
        entry["status"] = server.VIDEO_STATUS_UNCLAIMED
        entry["claimed_by"] = ""
        entry["claimed_at"] = ""
        entry["claim_expires_at"] = ""
        entry["updated_at"] = server.utc_now_iso()
        table[target] = entry
        server.save_video_status_table(table)
        print(json.dumps({"ok": True, "released": True, "video_path": target}, ensure_ascii=False))
        return 0
    print(
        json.dumps(
            {"ok": True, "released": False, "video_path": target, "status": entry["status"]},
            ensure_ascii=False,
        )
    )
    return 0


def cmd_release_all_claimed(args: argparse.Namespace) -> int:
    del args
    table = _load_table_with_cleanup()
    released = 0
    for video_path, entry in table.items():
        if entry.get("status") != server.VIDEO_STATUS_CLAIMED:
            continue
        entry["status"] = server.VIDEO_STATUS_UNCLAIMED
        entry["claimed_by"] = ""
        entry["claimed_at"] = ""
        entry["claim_expires_at"] = ""
        entry["updated_at"] = server.utc_now_iso()
        table[video_path] = entry
        released += 1
    if released:
        server.save_video_status_table(table)
    print(json.dumps({"ok": True, "released_count": released}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="admintool", description="UMR 标注状态管理工具")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="查看视频状态表")
    p_list.add_argument(
        "--status",
        choices=sorted(server.VALID_VIDEO_STATUSES),
        default="",
        help="按状态过滤",
    )
    p_list.set_defaults(func=cmd_list)

    p_cleanup = sub.add_parser("cleanup-expired", help="清理过期 claimed 锁")
    p_cleanup.set_defaults(func=cmd_cleanup)

    p_release = sub.add_parser("release", help="释放单个视频锁")
    p_release.add_argument("--video-path", required=True, help="视频绝对路径或可解析路径")
    p_release.set_defaults(func=cmd_release)

    p_release_all = sub.add_parser("release-all-claimed", help="释放全部 claimed 锁")
    p_release_all.set_defaults(func=cmd_release_all_claimed)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
