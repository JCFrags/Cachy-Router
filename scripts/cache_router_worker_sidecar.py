#!/usr/bin/env python3
"""Minimal worker-local slot sidecar for the cache-router MVP.

The sidecar exposes only bounded slot-file operations inside one configured
slot directory. It lets an independent router hydrate or ingest worker-local
hot cache files without requiring router-to-worker SSH filesystem access.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(raw: str) -> str:
    name = unquote(raw)
    if not name or "/" in name or name in {".", ".."}:
        raise ValueError("slot filename must be a simple basename")
    return name


def file_info(path: Path, *, hash_file: bool = False) -> dict[str, Any]:
    row: dict[str, Any] = {"path": str(path), "exists": path.exists()}
    if path.exists():
        row["size_bytes"] = path.stat().st_size
        if hash_file:
            row["sha256"] = sha256_file(path)
    return row


class SidecarState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.slot_dir = Path(args.slot_dir).resolve()
        self.slot_dir.mkdir(parents=True, exist_ok=True)

    def slot_path(self, filename: str) -> Path:
        path = (self.slot_dir / filename).resolve()
        if path.parent != self.slot_dir:
            raise ValueError("slot path escapes configured slot directory")
        return path


class SidecarHandler(BaseHTTPRequestHandler):
    server_version = "StrixHaloCacheWorkerSidecar/0.1"

    @property
    def state(self) -> SidecarState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s %s\n" % (now_iso(), fmt % args))

    def send_json(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str) -> None:
        self.send_json(status, {"ok": False, "error": message})

    def parse_slot_path(self, suffix: str) -> tuple[str, str]:
        parts = suffix.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "slots" or parts[2] not in {"info", "content"}:
            raise ValueError("expected /slots/<filename>/info or /slots/<filename>/content")
        return safe_filename(parts[1]), parts[2]

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            if parsed.path == "/health":
                self.send_json(
                    200,
                    {
                        "ok": True,
                        "status": "ok",
                        "worker_id": self.state.args.worker_id,
                        "slot_dir": str(self.state.slot_dir),
                        "bind": self.state.args.host,
                        "port": self.state.args.port,
                    },
                )
                return
            if parsed.path == "/slots":
                rows = []
                for path in sorted(self.state.slot_dir.glob("*")):
                    if path.is_file():
                        rows.append({"filename": path.name, "size_bytes": path.stat().st_size})
                self.send_json(200, {"ok": True, "count": len(rows), "slots": rows})
                return
            filename, action = self.parse_slot_path(parsed.path)
            path = self.state.slot_path(filename)
            if action == "info":
                query = parse_qs(parsed.query)
                self.send_json(200, file_info(path, hash_file=query.get("hash", ["0"])[0] in {"1", "true", "yes"}))
                return
            if not path.exists():
                self.send_error_json(404, "slot file not found")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(path.stat().st_size))
            self.end_headers()
            with path.open("rb") as fh:
                shutil.copyfileobj(fh, self.wfile, length=1024 * 1024)
        except Exception as exc:  # noqa: BLE001
            self.send_error_json(500, str(exc))

    def do_PUT(self) -> None:  # noqa: N802
        try:
            filename, action = self.parse_slot_path(urlsplit(self.path).path)
            if action != "content":
                self.send_error_json(405, "PUT is only supported for slot content")
                return
            length = int(self.headers.get("Content-Length") or "0")
            if length <= 0:
                self.send_error_json(411, "Content-Length is required")
                return
            max_bytes = int(self.state.args.max_upload_bytes)
            if max_bytes > 0 and length > max_bytes:
                self.send_error_json(413, "upload exceeds configured max_upload_bytes")
                return
            dest = self.state.slot_path(filename)
            with tempfile.NamedTemporaryFile(prefix=filename + ".", suffix=".tmp", dir=self.state.slot_dir, delete=False) as tmp:
                tmp_path = Path(tmp.name)
                remaining = length
                while remaining:
                    chunk = self.rfile.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise RuntimeError("client closed before upload completed")
                    tmp.write(chunk)
                    remaining -= len(chunk)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_path, dest)
            self.send_json(200, file_info(dest, hash_file=True))
        except Exception as exc:  # noqa: BLE001
            self.send_error_json(500, str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18083)
    parser.add_argument("--worker-id", default="worker-main")
    parser.add_argument("--slot-dir", required=True)
    parser.add_argument("--max-upload-bytes", type=int, default=0, help="0 means no sidecar-level upload limit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state = SidecarState(args)
    server = ThreadingHTTPServer((args.host, args.port), SidecarHandler)
    server.state = state  # type: ignore[attr-defined]
    actual_host, actual_port = server.server_address[:2]
    print(json.dumps({"status": "listening", "host": actual_host, "port": actual_port, "slot_dir": str(state.slot_dir)}, sort_keys=True), flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
