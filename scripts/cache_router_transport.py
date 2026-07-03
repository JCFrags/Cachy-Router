#!/usr/bin/env python3
"""Slot-file transport helpers for cache-router workers.

The cache router may run on the same host as a worker, on one of several
workers, or on an independent router PC. This module keeps slot-file movement
behind an explicit transport boundary so the daemon does not assume direct
local filesystem access to every worker-local NVMe cache.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import http.client
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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


def _prefix(binary: str, config: str = "", extra_args: str = "") -> list[str]:
    cmd = [binary]
    if config:
        cmd.extend(["-F", config])
    if extra_args:
        cmd.extend(shlex.split(extra_args))
    return cmd


def _run(cmd: list[str], *, input_text: str = "", timeout: float = 300.0) -> str:
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        sanitized = proc.stdout.replace("\r", "\n")
        raise RuntimeError(f"command failed rc={proc.returncode}: {cmd[:4]!r}\n{sanitized}")
    return proc.stdout


@dataclass(frozen=True)
class SlotFileInfo:
    path: str
    exists: bool
    size_bytes: int | None = None
    sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {"path": self.path, "exists": self.exists}
        if self.size_bytes is not None:
            row["size_bytes"] = self.size_bytes
        if self.sha256 is not None:
            row["sha256"] = self.sha256
        return row


class SlotTransport:
    def __init__(
        self,
        *,
        worker_id: str,
        kind: str,
        slot_dir: str,
        ssh_host: str = "",
        sidecar_url: str = "",
        ssh_config: str = "",
        ssh_extra_args: str = "",
        scp_extra_args: str = "",
        timeout: float = 900.0,
    ) -> None:
        if kind not in {"local", "ssh", "http"}:
            raise ValueError(f"unsupported slot transport kind: {kind}")
        if kind == "ssh" and not ssh_host:
            raise ValueError("ssh slot transport requires ssh_host")
        if kind == "http" and not sidecar_url:
            raise ValueError("http slot transport requires sidecar_url")
        self.worker_id = worker_id
        self.kind = kind
        self.slot_dir = slot_dir.rstrip("/")
        self.ssh_host = ssh_host
        self.sidecar_url = sidecar_url.rstrip("/")
        self.ssh_config = ssh_config
        self.ssh_extra_args = ssh_extra_args
        self.scp_extra_args = scp_extra_args
        self.timeout = timeout

    def describe(self) -> dict[str, Any]:
        row = {
            "worker_id": self.worker_id,
            "kind": self.kind,
            "slot_dir": self.slot_dir,
        }
        if self.kind == "ssh":
            row["ssh_host"] = self.ssh_host
        if self.kind == "http":
            row["sidecar_url"] = self.sidecar_url
        return row

    def sidecar_readiness(self, *, timeout: float | None = None) -> dict[str, Any]:
        if self.kind != "http":
            return {
                "ok": True,
                "state": "not_applicable",
                "transport": self.kind,
                "required": False,
                "checked": False,
            }
        start = time.perf_counter()
        req = urllib.request.Request(self.sidecar_url + "/health", method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout if timeout is None else timeout) as resp:
                status = resp.status
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            return {
                "ok": False,
                "state": "unhealthy",
                "transport": self.kind,
                "required": True,
                "checked": True,
                "http_status": exc.code,
                "wall_ms": (time.perf_counter() - start) * 1000.0,
                "error": raw[:200],
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "state": "unreachable",
                "transport": self.kind,
                "required": True,
                "checked": True,
                "http_status": None,
                "wall_ms": (time.perf_counter() - start) * 1000.0,
                "error": repr(exc),
            }
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {}
        reported_worker_id = body.get("worker_id") if isinstance(body, dict) else None
        worker_mismatch = isinstance(reported_worker_id, str) and reported_worker_id != self.worker_id
        ok = status == 200 and isinstance(body, dict) and body.get("ok") is True and body.get("status") == "ok" and not worker_mismatch
        row = {
            "ok": ok,
            "state": "worker_mismatch" if worker_mismatch else ("ready" if ok else "unhealthy"),
            "transport": self.kind,
            "required": True,
            "checked": True,
            "http_status": status,
            "wall_ms": (time.perf_counter() - start) * 1000.0,
        }
        if isinstance(reported_worker_id, str):
            row["worker_id"] = reported_worker_id
        return row

    def slot_path(self, filename: str) -> str:
        if not filename or "/" in filename or filename in {".", ".."}:
            raise ValueError(f"slot filename must be a simple basename: {filename!r}")
        return f"{self.slot_dir}/{filename}"

    def ensure_slot_dir(self) -> None:
        if self.kind == "local":
            Path(self.slot_dir).mkdir(parents=True, exist_ok=True)
            return
        if self.kind == "http":
            self._http_json("GET", "/health")
            return
        script = "python3 - <<'PY'\nimport os\nos.makedirs(SLOT_DIR, exist_ok=True)\nprint('{}')\nPY\n".replace("SLOT_DIR", repr(self.slot_dir))
        _run(self.ssh_cmd(["/bin/bash", "-s"]), input_text=script, timeout=30)

    def file_info(self, filename: str, *, hash_file: bool = False) -> SlotFileInfo:
        path = self.slot_path(filename)
        if self.kind == "local":
            local = Path(path)
            if not local.exists():
                return SlotFileInfo(path=path, exists=False)
            size = local.stat().st_size
            return SlotFileInfo(path=path, exists=True, size_bytes=size, sha256=sha256_file(local) if hash_file else None)
        if self.kind == "http":
            row = self._http_json("GET", f"/slots/{urllib.parse.quote(filename)}/info?hash={'1' if hash_file else '0'}")
            return SlotFileInfo(
                path=str(row["path"]),
                exists=bool(row["exists"]),
                size_bytes=row.get("size_bytes"),
                sha256=row.get("sha256"),
            )
        payload = json.dumps({"path": path, "hash_file": hash_file}, sort_keys=True)
        script = r'''
import hashlib, json, os
payload = json.loads(PAYLOAD)
path = payload["path"]
out = {"path": path, "exists": os.path.exists(path)}
if out["exists"]:
    st = os.stat(path)
    out["size_bytes"] = st.st_size
    if payload["hash_file"]:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        out["sha256"] = h.hexdigest()
print(json.dumps(out, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
        out = _run(self.ssh_cmd(["/bin/bash", "-s"]), input_text="python3 - <<'PY'\n" + script + "PY\n", timeout=self.timeout)
        row = json.loads(out)
        return SlotFileInfo(
            path=str(row["path"]),
            exists=bool(row["exists"]),
            size_bytes=row.get("size_bytes"),
            sha256=row.get("sha256"),
        )

    def upload_to_router(self, filename: str, router_blob_path: Path) -> dict[str, Any]:
        router_blob_path.parent.mkdir(parents=True, exist_ok=True)
        http_metadata: dict[str, Any] | None = None
        with tempfile.NamedTemporaryFile(prefix=router_blob_path.name + ".", suffix=".tmp", dir=router_blob_path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            if self.kind == "local":
                shutil.copyfile(self.slot_path(filename), tmp_path)
            elif self.kind == "http":
                http_metadata = self._http_json("POST", "/upload", {"filename": filename})
                if http_metadata.get("ok") is not True:
                    raise RuntimeError(f"sidecar upload preflight failed: {http_metadata}")
                self._http_download_path(str(http_metadata.get("content_path") or f"/slots/{urllib.parse.quote(filename)}/content"), tmp_path)
                downloaded_hash = sha256_file(tmp_path)
                if http_metadata.get("sha256") and downloaded_hash != http_metadata.get("sha256"):
                    raise RuntimeError(f"sidecar upload hash mismatch: sidecar={http_metadata.get('sha256')} downloaded={downloaded_hash}")
                if http_metadata.get("size_bytes") is not None and tmp_path.stat().st_size != int(http_metadata["size_bytes"]):
                    raise RuntimeError(f"sidecar upload size mismatch: sidecar={http_metadata.get('size_bytes')} downloaded={tmp_path.stat().st_size}")
            else:
                _run(
                    self.scp_cmd([f"{self.ssh_host}:{self.slot_path(filename)}", str(tmp_path)]),
                    timeout=self.timeout,
                )
            with tmp_path.open("ab") as fh:
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, router_blob_path)
            fsync_dir(router_blob_path.parent)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
        result = {
            "worker_id": self.worker_id,
            "transport": self.kind,
            "source": self.display_slot_path(filename),
            "dest": str(router_blob_path),
            "size_bytes": router_blob_path.stat().st_size,
            "sha256": sha256_file(router_blob_path),
        }
        if http_metadata is not None:
            result["sidecar_operation"] = http_metadata.get("operation")
            result["sidecar_sha256"] = http_metadata.get("sha256")
            result["sidecar_size_bytes"] = http_metadata.get("size_bytes")
        return result

    def hydrate_from_router(self, router_blob_path: Path, filename: str) -> dict[str, Any]:
        if not router_blob_path.is_file():
            raise RuntimeError(f"router blob missing: {router_blob_path}")
        expected_hash = sha256_file(router_blob_path)
        self.ensure_slot_dir()
        existing = self.file_info(filename, hash_file=True)
        if existing.exists and existing.sha256 == expected_hash:
            return {
                "worker_id": self.worker_id,
                "transport": self.kind,
                "source": str(router_blob_path),
                "dest": existing.path,
                "size_bytes": existing.size_bytes,
                "sha256": existing.sha256,
                "sha256_match": True,
                "performed": False,
            }
        hydrate_result: dict[str, Any] | None = None
        if self.kind == "local":
            dest = Path(self.slot_path(filename))
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            shutil.copyfile(router_blob_path, tmp)
            with tmp.open("ab") as fh:
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, dest)
            fsync_dir(dest.parent)
        elif self.kind == "http":
            hydrate_result = self._http_hydrate(router_blob_path, filename, expected_hash=expected_hash)
        else:
            remote_path = self.slot_path(filename)
            remote_tmp = remote_path + ".tmp"
            _run(self.scp_cmd([str(router_blob_path), f"{self.ssh_host}:{remote_tmp}"]), timeout=self.timeout)
            payload = json.dumps({"tmp": remote_tmp, "dest": remote_path}, sort_keys=True)
            script = r'''
import json, os
payload = json.loads(PAYLOAD)
with open(payload["tmp"], "ab") as fh:
    fh.flush()
    os.fsync(fh.fileno())
os.replace(payload["tmp"], payload["dest"])
dest_dir = os.path.dirname(payload["dest"]) or "."
try:
    fd = os.open(dest_dir, os.O_RDONLY)
except OSError:
    fd = None
if fd is not None:
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
print("{}")
'''.replace("PAYLOAD", repr(payload))
            _run(self.ssh_cmd(["/bin/bash", "-s"]), input_text="python3 - <<'PY'\n" + script + "PY\n", timeout=60)
        info = self.file_info(filename, hash_file=True)
        result = {
            "worker_id": self.worker_id,
            "transport": self.kind,
            "source": str(router_blob_path),
            "dest": info.path,
            "size_bytes": info.size_bytes,
            "sha256": info.sha256,
            "sha256_match": info.sha256 == expected_hash,
            "performed": bool(hydrate_result.get("performed", True)) if hydrate_result is not None else True,
        }
        if hydrate_result is not None:
            result["sidecar_operation"] = hydrate_result.get("operation")
            result["sidecar_performed"] = hydrate_result.get("performed")
        return result

    def display_slot_path(self, filename: str) -> str:
        path = self.slot_path(filename)
        if self.kind == "local":
            return path
        if self.kind == "http":
            return f"{self.sidecar_url}:{path}"
        return f"{self.ssh_host}:{path}"

    def _sidecar_request_path(self, path: str) -> str:
        parsed = urllib.parse.urlsplit(self.sidecar_url)
        base = parsed.path.rstrip("/") if parsed.path else ""
        return base + path

    def _http_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload, sort_keys=True).encode("utf-8")
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        req = urllib.request.Request(self.sidecar_url + path, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            body = resp.read().decode("utf-8")
        row = json.loads(body)
        if not isinstance(row, dict):
            raise RuntimeError(f"sidecar returned non-object JSON for {path}")
        return row

    def _http_download_path(self, path: str, dest: Path) -> None:
        if not path.startswith("/"):
            raise RuntimeError(f"sidecar content path must be absolute: {path!r}")
        url = self.sidecar_url + path
        with urllib.request.urlopen(url, timeout=self.timeout) as resp, dest.open("wb") as fh:
            shutil.copyfileobj(resp, fh, length=1024 * 1024)
            fh.flush()
            os.fsync(fh.fileno())

    def _http_hydrate(self, source: Path, filename: str, *, expected_hash: str) -> dict[str, Any]:
        parsed = urllib.parse.urlsplit(self.sidecar_url)
        if parsed.scheme not in {"http", "https"}:
            raise RuntimeError(f"unsupported sidecar URL scheme: {parsed.scheme}")
        path = self._sidecar_request_path(
            "/hydrate?"
            + urllib.parse.urlencode(
                {
                    "filename": filename,
                    "expected_sha256": expected_hash,
                    "size_bytes": str(source.stat().st_size),
                }
            )
        )
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        netloc = parsed.netloc
        conn = conn_cls(netloc, timeout=self.timeout)
        try:
            conn.putrequest("POST", path)
            conn.putheader("Content-Type", "application/octet-stream")
            conn.putheader("Content-Length", str(source.stat().st_size))
            conn.endheaders()
            with source.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                    conn.send(chunk)
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status >= 300:
                raise RuntimeError(f"sidecar hydrate failed HTTP {resp.status}: {body[:500]}")
            row = json.loads(body)
            if not isinstance(row, dict) or row.get("ok") is not True:
                raise RuntimeError(f"sidecar hydrate did not return success: {body[:500]}")
            return row
        finally:
            conn.close()

    def ssh_cmd(self, args: list[str]) -> list[str]:
        return _prefix("ssh", self.ssh_config, self.ssh_extra_args) + ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", self.ssh_host] + args

    def scp_cmd(self, args: list[str]) -> list[str]:
        return _prefix("scp", self.ssh_config, self.scp_extra_args) + ["-q", "-o", "BatchMode=yes"] + args


def run_self_test() -> int:
    with tempfile.TemporaryDirectory(prefix="cache-router-transport-") as td:
        root = Path(td)
        worker = root / "worker" / "slots"
        blobs = root / "router" / "blobs"
        worker.mkdir(parents=True)
        slot = worker / "demo.slot"
        slot.write_bytes((b"slot-payload\n" * 4096) + b"end")
        transport = SlotTransport(worker_id="worker-main", kind="local", slot_dir=str(worker), timeout=30)
        source = transport.file_info("demo.slot", hash_file=True)
        blob = blobs / "demo.blob"
        upload = transport.upload_to_router("demo.slot", blob)
        slot.unlink()
        missing = transport.file_info("demo.slot", hash_file=True)
        hydrate = transport.hydrate_from_router(blob, "demo.slot")
        final = transport.file_info("demo.slot", hash_file=True)
        hydrate_again = transport.hydrate_from_router(blob, "demo.slot")
        ok = (
            source.exists
            and not missing.exists
            and upload["sha256"] == source.sha256
            and hydrate["sha256_match"] is True
            and hydrate["performed"] is True
            and hydrate_again["sha256_match"] is True
            and hydrate_again["performed"] is False
            and final.sha256 == source.sha256
        )
        sidecar_script = Path(__file__).with_name("cache_router_worker_sidecar.py")
        sidecar = subprocess.Popen(
            [
                sys.executable,
                str(sidecar_script),
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--worker-id",
                "worker-http",
                "--slot-dir",
                str(worker),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        http_skipped = False
        http_skip_reason = ""
        try:
            assert sidecar.stdout is not None
            line = sidecar.stdout.readline()
            if not line:
                stderr = sidecar.stderr.read() if sidecar.stderr is not None else ""
                if "PermissionError" in stderr and "Operation not permitted" in stderr:
                    http_skipped = True
                    http_skip_reason = "socket creation is blocked in this execution sandbox"
                    http_ok = True
                    http_source = http_missing = http_final = SlotFileInfo(path=str(worker / "demo.slot"), exists=False)
                    http_upload = {}
                    http_hydrate = {}
                else:
                    raise RuntimeError(f"sidecar did not start: {stderr}")
            else:
                sidecar_info = json.loads(line)
                http_transport = SlotTransport(
                    worker_id="worker-http",
                    kind="http",
                    slot_dir=str(worker),
                    sidecar_url=f"http://127.0.0.1:{sidecar_info['port']}",
                    timeout=30,
                )
                slot.write_bytes((b"http-slot-payload\n" * 4096) + b"end")
                http_source = http_transport.file_info("demo.slot", hash_file=True)
                http_blob = blobs / "demo-http.blob"
                http_upload = http_transport.upload_to_router("demo.slot", http_blob)
                slot.unlink()
                http_missing = http_transport.file_info("demo.slot", hash_file=True)
                http_hydrate = http_transport.hydrate_from_router(http_blob, "demo.slot")
                http_final = http_transport.file_info("demo.slot", hash_file=True)
                http_ok = (
                    http_source.exists
                    and not http_missing.exists
                    and http_upload["sha256"] == http_source.sha256
                    and http_hydrate["sha256_match"] is True
                    and http_hydrate["performed"] is True
                    and http_final.sha256 == http_source.sha256
                )
                http_hydrate_again = http_transport.hydrate_from_router(http_blob, "demo.slot")
                http_ok = http_ok and http_hydrate_again["sha256_match"] is True and http_hydrate_again["performed"] is False
        finally:
            sidecar.terminate()
            try:
                sidecar.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sidecar.kill()
                sidecar.wait(timeout=5)
        print(
            json.dumps(
                {
                    "ok": ok and http_ok,
                    "local": {"source": source.as_dict(), "upload": upload, "hydrate": hydrate, "hydrate_again": hydrate_again, "final": final.as_dict()},
                    "http": {
                        "skipped": http_skipped,
                        "skip_reason": http_skip_reason,
                        "source": http_source.as_dict(),
                        "upload": http_upload,
                        "hydrate": http_hydrate,
                        "hydrate_again": http_hydrate_again if not http_skipped else {},
                        "final": http_final.as_dict(),
                    },
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if ok and http_ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return run_self_test()
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
