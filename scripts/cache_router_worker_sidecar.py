#!/usr/bin/env python3
"""Minimal worker-local slot sidecar for the cache-router MVP.

The sidecar exposes only bounded slot-file operations inside one configured
slot directory. It lets an independent router hydrate or ingest worker-local
hot cache files without requiring router-to-worker SSH filesystem access.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import secrets
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

DEFAULT_LEASE_TTL_SECONDS = 30.0
MAX_LEASE_TTL_SECONDS = 3600.0


def is_loopback_bind(host: str) -> bool:
    return host in {"127.0.0.1", "localhost", "::1"}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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


def decode_content_base64(value: Any) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValueError("content_base64 is required")
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError) as exc:
        raise ValueError("content_base64 must be valid base64") from exc


class SidecarState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.slot_dir = Path(args.slot_dir).resolve()
        self.slot_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.publish_locks: dict[str, threading.Lock] = {}
        self.leases_lock = threading.Lock()
        self.leases: dict[str, dict[str, dict[str, Any]]] = {}

    def slot_path(self, filename: str) -> Path:
        path = (self.slot_dir / filename).resolve()
        if path.parent != self.slot_dir:
            raise ValueError("slot path escapes configured slot directory")
        return path

    def filename_lock(self, filename: str) -> threading.Lock:
        with self.lock:
            lock = self.publish_locks.get(filename)
            if lock is None:
                lock = threading.Lock()
                self.publish_locks[filename] = lock
            return lock

    def parse_lease_ttl(self, raw: Any) -> float:
        if raw is None:
            return DEFAULT_LEASE_TTL_SECONDS
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError("ttl_seconds must be a positive number when provided")
        ttl_seconds = float(raw)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if ttl_seconds > MAX_LEASE_TTL_SECONDS:
            raise ValueError(f"ttl_seconds must be <= {MAX_LEASE_TTL_SECONDS:g}")
        return ttl_seconds

    def prune_expired_leases_locked(self, filename: str, now: float) -> int:
        rows = self.leases.get(filename)
        if not rows:
            return 0
        expired = [lease_id for lease_id, row in rows.items() if float(row["expires_at_monotonic"]) <= now]
        for lease_id in expired:
            rows.pop(lease_id, None)
        if not rows:
            self.leases.pop(filename, None)
        return len(expired)

    def active_lease_count_locked(self, filename: str, now: float) -> int:
        self.prune_expired_leases_locked(filename, now)
        return len(self.leases.get(filename) or {})

    def active_lease_count(self, filename: str) -> int:
        with self.leases_lock:
            return self.active_lease_count_locked(filename, time.monotonic())

    def acquire_lease(
        self,
        *,
        filename: str,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
        ttl_seconds: float,
        holder: str | None = None,
    ) -> dict[str, Any]:
        path = self.slot_path(filename)
        with self.filename_lock(filename):
            if not path.exists():
                return {"ok": False, "filename": filename, "leased": False, "reason": "not_found"}
            info = file_info(path, hash_file=expected_sha256 is not None)
            sha256_match = expected_sha256 is None or info.get("sha256") == expected_sha256
            size_match = expected_size is None or info.get("size_bytes") == expected_size
            if not sha256_match or not size_match:
                return {
                    "ok": False,
                    "filename": filename,
                    "leased": False,
                    "reason": "precondition_failed",
                    "size_bytes": info.get("size_bytes"),
                    "sha256": info.get("sha256"),
                    "sha256_match": sha256_match,
                    "size_match": size_match,
                }
            lease_id = "lease-" + secrets.token_urlsafe(18)
            now = time.monotonic()
            with self.leases_lock:
                self.prune_expired_leases_locked(filename, now)
                rows = self.leases.setdefault(filename, {})
                rows[lease_id] = {
                    "created_at": now_iso(),
                    "expires_at_monotonic": now + ttl_seconds,
                    "holder": holder,
                    "size_bytes": info.get("size_bytes"),
                    "sha256": info.get("sha256") or expected_sha256,
                    "ttl_seconds": ttl_seconds,
                }
                active_count = len(rows)
        return {
            "ok": True,
            "filename": filename,
            "leased": True,
            "lease_id": lease_id,
            "ttl_seconds": ttl_seconds,
            "active_lease_count": active_count,
        }

    def release_lease(self, *, filename: str, lease_id: str) -> dict[str, Any]:
        with self.filename_lock(filename):
            now = time.monotonic()
            with self.leases_lock:
                self.prune_expired_leases_locked(filename, now)
                rows = self.leases.get(filename)
                released = bool(rows and rows.pop(lease_id, None) is not None)
                active_count = len(rows or {})
                if rows is not None and not rows:
                    self.leases.pop(filename, None)
        return {
            "ok": True,
            "filename": filename,
            "lease_id": lease_id,
            "released": released,
            "active_lease_count": active_count,
        }

    def inventory(self, *, hash_files: bool = False) -> dict[str, Any]:
        rows = []
        total_size = 0
        total_allocated = 0
        for path in sorted(self.slot_dir.glob("*")):
            if not path.is_file() or path.name.endswith(".tmp"):
                continue
            stat = path.stat()
            allocated = int(getattr(stat, "st_blocks", 0) or 0) * 512
            if allocated <= 0:
                allocated = stat.st_size
            info = {"filename": path.name, "size_bytes": stat.st_size, "allocated_bytes": allocated}
            total_size += int(info["size_bytes"])
            total_allocated += allocated
            if hash_files:
                info["sha256"] = sha256_file(path)
            rows.append(info)
        return {
            "ok": True,
            "worker_id": self.args.worker_id,
            "slot_dir": str(self.slot_dir),
            "count": len(rows),
            "total_size_bytes": total_size,
            "total_allocated_bytes": total_allocated,
            "slots": rows,
        }

    def publish_temp(
        self,
        *,
        filename: str,
        tmp_path: Path,
        actual_sha256: str,
        actual_size: int,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
        idempotent: bool = False,
    ) -> dict[str, Any]:
        if expected_sha256 is not None and actual_sha256 != expected_sha256:
            tmp_path.unlink(missing_ok=True)
            return {
                "ok": False,
                "filename": filename,
                "published": False,
                "performed": False,
                "error": "expected_sha256 mismatch",
                "sha256": actual_sha256,
                "sha256_match": False,
                "size_bytes": actual_size,
                "size_match": expected_size is None or actual_size == expected_size,
            }
        if expected_size is not None and actual_size != expected_size:
            tmp_path.unlink(missing_ok=True)
            return {
                "ok": False,
                "filename": filename,
                "published": False,
                "performed": False,
                "error": "size_bytes mismatch",
                "sha256": actual_sha256,
                "sha256_match": True,
                "size_bytes": actual_size,
                "size_match": False,
            }
        dest = self.slot_path(filename)
        with self.filename_lock(filename):
            if idempotent and dest.exists() and sha256_file(dest) == actual_sha256 and dest.stat().st_size == actual_size:
                tmp_path.unlink(missing_ok=True)
                return {
                    "ok": True,
                    "filename": filename,
                    "published": True,
                    "performed": False,
                    "reason": "already_present",
                    "path": str(dest),
                    "sha256": actual_sha256,
                    "sha256_match": True,
                    "size_bytes": actual_size,
                    "size_match": True,
                }
            active_lease_count = self.active_lease_count(filename)
            if active_lease_count:
                tmp_path.unlink(missing_ok=True)
                return {
                    "ok": False,
                    "filename": filename,
                    "published": False,
                    "performed": False,
                    "reason": "in_use",
                    "active_lease_count": active_lease_count,
                    "sha256": actual_sha256,
                    "sha256_match": True,
                    "size_bytes": actual_size,
                    "size_match": True,
                }
            os.replace(tmp_path, dest)
            fsync_dir(dest.parent)
            final_sha256 = sha256_file(dest)
            final_size = dest.stat().st_size
        return {
            "ok": True,
            "filename": filename,
            "published": True,
            "performed": True,
            "path": str(dest),
            "sha256": final_sha256,
            "sha256_match": final_sha256 == actual_sha256,
            "size_bytes": final_size,
            "size_match": final_size == actual_size,
        }

    def publish_bytes(
        self,
        *,
        filename: str,
        payload: bytes,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
        idempotent: bool = False,
    ) -> dict[str, Any]:
        max_bytes = int(self.args.max_upload_bytes)
        if max_bytes > 0 and len(payload) > max_bytes:
            raise ValueError("upload exceeds configured max_upload_bytes")
        with tempfile.NamedTemporaryFile(prefix=filename + ".", suffix=".tmp", dir=self.slot_dir, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(payload)
            tmp.flush()
            os.fsync(tmp.fileno())
        return self.publish_temp(
            filename=filename,
            tmp_path=tmp_path,
            actual_sha256=hashlib.sha256(payload).hexdigest(),
            actual_size=len(payload),
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            idempotent=idempotent,
        )

    def publish_stream(
        self,
        *,
        filename: str,
        stream: Any,
        length: int,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
        idempotent: bool = False,
    ) -> dict[str, Any]:
        if length <= 0:
            raise ValueError("Content-Length is required")
        max_bytes = int(self.args.max_upload_bytes)
        if max_bytes > 0 and length > max_bytes:
            raise ValueError("upload exceeds configured max_upload_bytes")
        hasher = hashlib.sha256()
        actual_size = 0
        with tempfile.NamedTemporaryFile(prefix=filename + ".", suffix=".tmp", dir=self.slot_dir, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            remaining = length
            while remaining:
                chunk = stream.read(min(1024 * 1024, remaining))
                if not chunk:
                    tmp_path.unlink(missing_ok=True)
                    raise RuntimeError("client closed before upload completed")
                tmp.write(chunk)
                hasher.update(chunk)
                actual_size += len(chunk)
                remaining -= len(chunk)
            tmp.flush()
            os.fsync(tmp.fileno())
        return self.publish_temp(
            filename=filename,
            tmp_path=tmp_path,
            actual_sha256=hasher.hexdigest(),
            actual_size=actual_size,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            idempotent=idempotent,
        )


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

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

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
            if parsed.path == "/inventory":
                query = parse_qs(parsed.query)
                self.send_json(200, self.state.inventory(hash_files=query.get("hash", ["0"])[0] in {"1", "true", "yes"}))
                return
            if parsed.path == "/slots":
                inventory = self.state.inventory(hash_files=False)
                self.send_json(200, {"ok": True, "count": inventory["count"], "slots": inventory["slots"]})
                return
            filename, action = self.parse_slot_path(parsed.path)
            path = self.state.slot_path(filename)
            if action == "info":
                query = parse_qs(parsed.query)
                self.send_json(200, file_info(path, hash_file=query.get("hash", ["0"])[0] in {"1", "true", "yes"}))
                return
            with self.state.filename_lock(filename):
                if not path.exists():
                    self.send_error_json(404, "slot file not found")
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(path.stat().st_size))
                self.end_headers()
                with path.open("rb") as fh:
                    shutil.copyfileobj(fh, self.wfile, length=1024 * 1024)
        except ValueError as exc:
            self.send_error_json(400, str(exc))
        except Exception as exc:  # noqa: BLE001
            self.send_error_json(500, str(exc))

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            content_type = self.headers.get("Content-Type", "")
            if parsed.path in {"/upload", "/hydrate"} and "application/json" not in content_type.lower():
                filename = safe_filename((query.get("filename") or [""])[0])
                expected_sha256 = (query.get("expected_sha256") or [""])[0] or None
                expected_size = None
                if query.get("size_bytes"):
                    expected_size = int(query["size_bytes"][0])
                    if expected_size < 0:
                        raise ValueError("size_bytes must be non-negative")
                if parsed.path == "/hydrate" and not expected_sha256:
                    raise ValueError("expected_sha256 is required for hydrate")
                result = self.state.publish_stream(
                    filename=filename,
                    stream=self.rfile,
                    length=int(self.headers.get("Content-Length") or "0"),
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    idempotent=parsed.path == "/hydrate",
                )
                status = 200 if result.get("ok") else 409
                result["operation"] = parsed.path.strip("/")
                result["content_encoding"] = "binary"
                self.send_json(status, result)
                return
            body = self.read_json_body()
            if parsed.path in {"/upload", "/hydrate"}:
                filename = safe_filename(str(body.get("filename") or ""))
                if parsed.path == "/upload" and body.get("content_base64") is None:
                    path = self.state.slot_path(filename)
                    info = file_info(path, hash_file=True)
                    expected_sha256 = body.get("expected_sha256")
                    expected_size = body.get("size_bytes")
                    sha256_match = expected_sha256 is None or (info.get("sha256") == expected_sha256)
                    size_match = expected_size is None or (info.get("size_bytes") == expected_size)
                    verified = bool(info["exists"]) and sha256_match and size_match
                    self.send_json(
                        200 if verified else 409,
                        {
                            "ok": verified,
                            "operation": "upload",
                            "filename": filename,
                            "exists": info["exists"],
                            "size_bytes": info.get("size_bytes"),
                            "sha256": info.get("sha256"),
                            "sha256_match": sha256_match,
                            "size_match": size_match,
                            "content_path": f"/slots/{filename}/content" if verified else None,
                        },
                    )
                    return
                payload = decode_content_base64(body.get("content_base64"))
                expected_sha256 = body.get("expected_sha256")
                if expected_sha256 is not None and not isinstance(expected_sha256, str):
                    raise ValueError("expected_sha256 must be a string when provided")
                if parsed.path == "/hydrate" and not expected_sha256:
                    raise ValueError("expected_sha256 is required for hydrate")
                expected_size_raw = body.get("size_bytes")
                expected_size = None
                if expected_size_raw is not None:
                    if not isinstance(expected_size_raw, int) or isinstance(expected_size_raw, bool) or expected_size_raw < 0:
                        raise ValueError("size_bytes must be a non-negative integer when provided")
                    expected_size = expected_size_raw
                result = self.state.publish_bytes(
                    filename=filename,
                    payload=payload,
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    idempotent=parsed.path == "/hydrate",
                )
                status = 200 if result.get("ok") else 409
                result["operation"] = parsed.path.strip("/")
                self.send_json(status, result)
                return
            if parsed.path == "/leases/acquire":
                filename = safe_filename(str(body.get("filename") or ""))
                expected_sha256 = body.get("expected_sha256")
                if expected_sha256 is not None and not isinstance(expected_sha256, str):
                    raise ValueError("expected_sha256 must be a string when provided")
                expected_size_raw = body.get("size_bytes")
                expected_size = None
                if expected_size_raw is not None:
                    if not isinstance(expected_size_raw, int) or isinstance(expected_size_raw, bool) or expected_size_raw < 0:
                        raise ValueError("size_bytes must be a non-negative integer when provided")
                    expected_size = expected_size_raw
                holder_raw = body.get("holder")
                holder = None
                if holder_raw is not None:
                    if not isinstance(holder_raw, str):
                        raise ValueError("holder must be a string when provided")
                    holder = holder_raw[:128]
                result = self.state.acquire_lease(
                    filename=filename,
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    ttl_seconds=self.state.parse_lease_ttl(body.get("ttl_seconds")),
                    holder=holder,
                )
                status = 200 if result.get("ok") else (404 if result.get("reason") == "not_found" else 409)
                self.send_json(status, result)
                return
            if parsed.path == "/leases/release":
                filename = safe_filename(str(body.get("filename") or ""))
                lease_id = body.get("lease_id")
                if not isinstance(lease_id, str) or not lease_id:
                    raise ValueError("lease_id is required")
                self.send_json(200, self.state.release_lease(filename=filename, lease_id=lease_id))
                return
            if parsed.path == "/verify":
                filename = safe_filename(str(body.get("filename") or ""))
                path = self.state.slot_path(filename)
                info = file_info(path, hash_file=True)
                expected_sha256 = body.get("sha256")
                expected_size = body.get("size_bytes")
                sha256_match = expected_sha256 is None or (info.get("sha256") == expected_sha256)
                size_match = expected_size is None or (info.get("size_bytes") == expected_size)
                verified = bool(info["exists"]) and sha256_match and size_match
                self.send_json(
                    200,
                    {
                        "ok": True,
                        "verified": verified,
                        "filename": filename,
                        "exists": info["exists"],
                        "size_bytes": info.get("size_bytes"),
                        "sha256": info.get("sha256"),
                        "sha256_match": sha256_match,
                        "size_match": size_match,
                    },
                )
                return
            if parsed.path == "/evict":
                filename = safe_filename(str(body.get("filename") or ""))
                path = self.state.slot_path(filename)
                with self.state.filename_lock(filename):
                    if not path.exists():
                        self.send_json(200, {"ok": True, "filename": filename, "evicted": False, "reason": "not_found"})
                        return
                    expected_sha256 = body.get("expected_sha256")
                    if not isinstance(expected_sha256, str) or not expected_sha256:
                        self.send_json(400, {"ok": False, "filename": filename, "evicted": False, "error": "expected_sha256 is required"})
                        return
                    active_lease_count = self.state.active_lease_count(filename)
                    if active_lease_count:
                        self.send_json(
                            423,
                            {
                                "ok": False,
                                "filename": filename,
                                "evicted": False,
                                "reason": "in_use",
                                "active_lease_count": active_lease_count,
                            },
                        )
                        return
                    current_sha256 = sha256_file(path)
                    if current_sha256 != expected_sha256:
                        self.send_json(
                            409,
                            {
                                "ok": False,
                                "filename": filename,
                                "evicted": False,
                                "error": "expected_sha256 mismatch",
                                "sha256": current_sha256,
                            },
                        )
                        return
                    size_bytes = path.stat().st_size
                    path.unlink()
                    fsync_dir(path.parent)
                    self.send_json(
                        200,
                        {
                            "ok": True,
                            "filename": filename,
                            "evicted": True,
                            "sha256": current_sha256,
                            "size_bytes": size_bytes,
                        },
                    )
                return
            self.send_error_json(404, "unknown sidecar endpoint")
        except ValueError as exc:
            self.send_error_json(400, str(exc))
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
            with self.state.filename_lock(filename):
                active_lease_count = self.state.active_lease_count(filename)
                if active_lease_count:
                    tmp_path.unlink(missing_ok=True)
                    self.send_json(
                        423,
                        {
                            "ok": False,
                            "filename": filename,
                            "published": False,
                            "performed": False,
                            "reason": "in_use",
                            "active_lease_count": active_lease_count,
                        },
                    )
                    return
                os.replace(tmp_path, dest)
                fsync_dir(dest.parent)
            self.send_json(200, file_info(dest, hash_file=True))
        except ValueError as exc:
            self.send_error_json(400, str(exc))
        except Exception as exc:  # noqa: BLE001
            self.send_error_json(500, str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Bind address. Non-loopback binds require --allow-unauthenticated-lan.")
    parser.add_argument("--port", type=int, default=18083)
    parser.add_argument("--worker-id", default="worker-main")
    parser.add_argument("--slot-dir", required=True)
    parser.add_argument("--max-upload-bytes", type=int, default=0, help="0 means no sidecar-level upload limit.")
    parser.add_argument(
        "--allow-unauthenticated-lan",
        action="store_true",
        help="Required for non-loopback binds. Use only on a trusted private LAN.",
    )
    args = parser.parse_args()
    if not is_loopback_bind(args.host) and not args.allow_unauthenticated_lan:
        raise SystemExit(
            "--host is not loopback. Use --allow-unauthenticated-lan for an explicit trusted-LAN sidecar, "
            "or bind the sidecar to loopback."
        )
    return args


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
