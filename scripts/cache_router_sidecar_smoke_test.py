#!/usr/bin/env python3
"""Offline smoke test for the worker slot sidecar."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

import cache_router_worker_sidecar as sidecar  # noqa: E402


def sha256_file(path: Path) -> str:
    return sidecar.sha256_file(path)


def start_sidecar(slot_dir: Path) -> ThreadingHTTPServer:
    args = argparse.Namespace(
        host="127.0.0.1",
        port=0,
        worker_id="worker-sidecar-smoke",
        slot_dir=str(slot_dir),
        max_upload_bytes=1024 * 1024,
    )
    state = sidecar.SidecarState(args)
    server = ThreadingHTTPServer(("127.0.0.1", 0), sidecar.SidecarHandler)
    server.state = state  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def request(
    method: str,
    base_url: str,
    path: str,
    *,
    body: Any | None = None,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    if body is not None and data is not None:
        raise ValueError("body and data are mutually exclusive")
    payload = data if data is not None else (None if body is None else json.dumps(body).encode("utf-8"))
    req_headers = {"Accept-Encoding": "identity"}
    if headers:
        req_headers.update(headers)
    if body is not None:
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base_url + path, data=payload, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()


def json_request(method: str, base_url: str, path: str, *, body: Any | None = None) -> tuple[int, dict[str, Any]]:
    status, _, raw = request(method, base_url, path, body=body)
    return status, json.loads(raw.decode("utf-8"))


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def publish_body(filename: str, payload: bytes, *, expected_sha256: str | None = None, size_bytes: int | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "filename": filename,
        "content_base64": base64.b64encode(payload).decode("ascii"),
    }
    if expected_sha256 is not None:
        body["expected_sha256"] = expected_sha256
    if size_bytes is not None:
        body["size_bytes"] = size_bytes
    return body


def base_url(server: ThreadingHTTPServer) -> str:
    host, port = server.server_address[:2]
    return f"http://{host}:{port}"


def main() -> int:
    first_server: ThreadingHTTPServer | None = None
    second_server: ThreadingHTTPServer | None = None
    with tempfile.TemporaryDirectory(prefix="cache-router-sidecar-") as tmp:
        slot_dir = Path(tmp) / "slots"
        slot_dir.mkdir(parents=True)
        slot_file = slot_dir / "demo.slot"
        payload = (b"sidecar-demo-payload\n" * 128) + b"end"
        slot_file.write_bytes(payload)
        slot_hash = sha256_file(slot_file)

        try:
            first_server = start_sidecar(slot_dir)
            first_base = base_url(first_server)

            status, body = json_request("GET", first_base, "/health")
            assert_true(status == 200 and body.get("ok") is True, "sidecar health should succeed")

            status, body = json_request("GET", first_base, "/inventory?hash=1")
            assert_true(status == 200, "inventory should return HTTP 200")
            assert_true(body.get("count") == 1, "inventory should list one file")
            assert_true(body.get("total_size_bytes") == len(payload), "inventory total_size_bytes should match filesystem size")
            assert_true(int(body.get("total_allocated_bytes") or 0) >= len(payload), "inventory total_allocated_bytes should cover file payload bytes")
            assert_true(body.get("slots", [{}])[0].get("sha256") == slot_hash, "inventory should include requested hash")

            status, body = json_request("GET", first_base, "/slots")
            assert_true(status == 200 and body.get("count") == 1, "legacy /slots endpoint should remain compatible")

            upload_payload = b"sidecar-upload-payload"
            upload_sha = hashlib.sha256(upload_payload).hexdigest()
            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("uploaded.slot", upload_payload, expected_sha256=upload_sha, size_bytes=len(upload_payload)),
            )
            assert_true(status == 200 and body.get("ok") is True and body.get("performed") is True, "upload endpoint should publish verified content")
            assert_true((slot_dir / "uploaded.slot").read_bytes() == upload_payload, "upload endpoint should publish exact bytes")
            assert_true(body.get("sha256") == upload_sha and body.get("sha256_match") is True and body.get("size_match") is True, "upload endpoint should report matching hash and size")

            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("uploaded.slot", b"bad", expected_sha256="0" * 64, size_bytes=3),
            )
            assert_true(status == 409 and body.get("published") is False, "upload hash mismatch should fail closed")
            assert_true((slot_dir / "uploaded.slot").read_bytes() == upload_payload, "failed upload must not replace existing content")

            hydrate_payload = b"sidecar-hydrate-payload"
            hydrate_sha = hashlib.sha256(hydrate_payload).hexdigest()
            status, body = json_request(
                "POST",
                first_base,
                "/hydrate",
                body=publish_body("hydrated.slot", hydrate_payload, expected_sha256=hydrate_sha, size_bytes=len(hydrate_payload)),
            )
            assert_true(status == 200 and body.get("ok") is True and body.get("performed") is True, "hydrate endpoint should publish verified content")
            assert_true((slot_dir / "hydrated.slot").read_bytes() == hydrate_payload, "hydrate endpoint should publish exact bytes")

            status, body = json_request(
                "POST",
                first_base,
                "/hydrate",
                body=publish_body("hydrated.slot", hydrate_payload, expected_sha256=hydrate_sha, size_bytes=len(hydrate_payload)),
            )
            assert_true(status == 200 and body.get("performed") is False and body.get("reason") == "already_present", "duplicate hydrate should safely no-op")
            assert_true((slot_dir / "hydrated.slot").read_bytes() == hydrate_payload, "duplicate hydrate must leave content unchanged")

            binary_payload = b"sidecar-binary-hydrate-payload" * 128
            binary_sha = hashlib.sha256(binary_payload).hexdigest()
            query = urllib.parse.urlencode({"filename": "binary-hydrated.slot", "expected_sha256": binary_sha, "size_bytes": len(binary_payload)})
            status, _, raw = request(
                "POST",
                first_base,
                f"/hydrate?{query}",
                data=binary_payload,
                headers={"Content-Type": "application/octet-stream"},
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200 and body.get("ok") is True and body.get("content_encoding") == "binary", "binary hydrate should publish verified content")
            assert_true((slot_dir / "binary-hydrated.slot").read_bytes() == binary_payload, "binary hydrate should publish exact bytes")

            concurrent_payload = b"sidecar-concurrent-hydrate-payload" * 128
            concurrent_sha = hashlib.sha256(concurrent_payload).hexdigest()
            concurrent_query = urllib.parse.urlencode({"filename": "concurrent-hydrated.slot", "expected_sha256": concurrent_sha, "size_bytes": len(concurrent_payload)})
            concurrent_results: list[tuple[int, dict[str, Any]]] = []
            concurrent_lock = threading.Lock()

            def hydrate_concurrently() -> None:
                status_code, _, raw_body = request(
                    "POST",
                    first_base,
                    f"/hydrate?{concurrent_query}",
                    data=concurrent_payload,
                    headers={"Content-Type": "application/octet-stream"},
                )
                with concurrent_lock:
                    concurrent_results.append((status_code, json.loads(raw_body.decode("utf-8"))))

            threads = [threading.Thread(target=hydrate_concurrently) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            assert_true(len(concurrent_results) == 2, "both concurrent hydrate requests should complete")
            assert_true(all(status_code == 200 for status_code, _ in concurrent_results), "concurrent hydrate requests should both return HTTP 200")
            performed_values = sorted(bool(row.get("performed")) for _, row in concurrent_results)
            assert_true(performed_values == [False, True], f"concurrent duplicate hydrate should publish once and no-op once: {concurrent_results}")
            assert_true((slot_dir / "concurrent-hydrated.slot").read_bytes() == concurrent_payload, "concurrent hydrate should leave exact bytes")

            lease_payload = b"sidecar-lease-payload"
            lease_sha = hashlib.sha256(lease_payload).hexdigest()
            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("leased.slot", lease_payload, expected_sha256=lease_sha, size_bytes=len(lease_payload)),
            )
            assert_true(status == 200 and body.get("ok") is True, "lease fixture upload should publish verified content")

            status, body = json_request(
                "POST",
                first_base,
                "/leases/acquire",
                body={"filename": "leased.slot", "expected_sha256": "0" * 64, "size_bytes": len(lease_payload), "ttl_seconds": 30},
            )
            assert_true(status == 409 and body.get("leased") is False and body.get("reason") == "precondition_failed", "lease acquire with wrong hash should fail closed")

            status, body = json_request(
                "POST",
                first_base,
                "/evict",
                body={"filename": "leased.slot", "expected_sha256": lease_sha},
            )
            assert_true(status == 200 and body.get("evicted") is True, "failed lease acquire should not block eviction")

            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("leased.slot", lease_payload, expected_sha256=lease_sha, size_bytes=len(lease_payload)),
            )
            assert_true(status == 200 and body.get("ok") is True, "lease fixture should be recreated after failed-acquire eviction")

            status, body = json_request(
                "POST",
                first_base,
                "/leases/acquire",
                body={
                    "filename": "leased.slot",
                    "expected_sha256": lease_sha,
                    "size_bytes": len(lease_payload),
                    "ttl_seconds": 30,
                    "holder": "sidecar-smoke",
                },
            )
            lease_id = body.get("lease_id")
            assert_true(status == 200 and body.get("leased") is True and isinstance(lease_id, str) and lease_id, "matching lease acquire should return a lease id")

            replacement_payload = b"sidecar-lease-replacement"
            replacement_sha = hashlib.sha256(replacement_payload).hexdigest()
            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("leased.slot", replacement_payload, expected_sha256=replacement_sha, size_bytes=len(replacement_payload)),
            )
            assert_true(status == 409 and body.get("reason") == "in_use", "upload replacement should be refused while lease is active")
            assert_true((slot_dir / "leased.slot").read_bytes() == lease_payload, "leased upload replacement must leave bytes unchanged")

            status, _, raw = request("PUT", first_base, "/slots/leased.slot/content", data=replacement_payload)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 423 and body.get("reason") == "in_use", "legacy PUT replacement should be refused while lease is active")
            assert_true((slot_dir / "leased.slot").read_bytes() == lease_payload, "leased legacy PUT replacement must leave bytes unchanged")

            status, body = json_request("POST", first_base, "/evict", body={"filename": "leased.slot"})
            assert_true(status == 400 and body.get("evicted") is False, "evict without expected hash should still fail closed while leased")
            assert_true((slot_dir / "leased.slot").read_bytes() == lease_payload, "leased evict without hash must leave bytes unchanged")

            status, body = json_request("POST", first_base, "/evict", body={"filename": "leased.slot", "expected_sha256": lease_sha})
            assert_true(status == 423 and body.get("reason") == "in_use" and body.get("evicted") is False, "active lease should block eviction")
            assert_true((slot_dir / "leased.slot").read_bytes() == lease_payload, "leased evict must leave bytes unchanged")

            status, body = json_request("POST", first_base, "/leases/release", body={"filename": "leased.slot", "lease_id": lease_id})
            assert_true(status == 200 and body.get("released") is True and body.get("active_lease_count") == 0, "lease release should remove the active lease")
            status, body = json_request("POST", first_base, "/leases/release", body={"filename": "leased.slot", "lease_id": lease_id})
            assert_true(status == 200 and body.get("released") is False, "lease release should be idempotent after the lease is gone")

            status, body = json_request("POST", first_base, "/evict", body={"filename": "leased.slot", "expected_sha256": lease_sha})
            assert_true(status == 200 and body.get("evicted") is True, "released lease should allow eviction")

            status, body = json_request(
                "POST",
                first_base,
                "/upload",
                body=publish_body("leased.slot", lease_payload, expected_sha256=lease_sha, size_bytes=len(lease_payload)),
            )
            assert_true(status == 200 and body.get("ok") is True, "lease expiry fixture should be recreated")
            status, body = json_request(
                "POST",
                first_base,
                "/leases/acquire",
                body={"filename": "leased.slot", "expected_sha256": lease_sha, "size_bytes": len(lease_payload), "ttl_seconds": 0.1},
            )
            assert_true(status == 200 and body.get("leased") is True, "short TTL lease acquire should succeed")
            time.sleep(0.25)
            status, body = json_request("POST", first_base, "/evict", body={"filename": "leased.slot", "expected_sha256": lease_sha})
            assert_true(status == 200 and body.get("evicted") is True, "expired lease should be pruned before eviction")

            status, _ = json_request(
                "POST",
                first_base,
                "/hydrate",
                body=publish_body("../hydrated.slot", hydrate_payload, expected_sha256=hydrate_sha, size_bytes=len(hydrate_payload)),
            )
            assert_true(status == 400, "hydrate should reject path traversal")

            status, body = json_request(
                "POST",
                first_base,
                "/hydrate",
                body={"filename": "bad.slot", "content_base64": "not-base64", "expected_sha256": hydrate_sha},
            )
            assert_true(status == 400 and body.get("ok") is False, "hydrate should reject invalid base64")

            status, body = json_request(
                "POST",
                first_base,
                "/hydrate",
                body=publish_body("too-large.slot", b"x" * (1024 * 1024 + 1), expected_sha256=hashlib.sha256(b"x" * (1024 * 1024 + 1)).hexdigest(), size_bytes=1024 * 1024 + 1),
            )
            assert_true(status == 400 and body.get("ok") is False, "hydrate should enforce max_upload_bytes")

            for filename, expected_hash in [
                ("uploaded.slot", upload_sha),
                ("hydrated.slot", hydrate_sha),
                ("binary-hydrated.slot", binary_sha),
                ("concurrent-hydrated.slot", concurrent_sha),
            ]:
                status, body = json_request("POST", first_base, "/evict", body={"filename": filename, "expected_sha256": expected_hash})
                assert_true(status == 200 and body.get("evicted") is True, f"cleanup evict should remove {filename}")

            status, body = json_request("POST", first_base, "/verify", body={"filename": "demo.slot", "sha256": slot_hash, "size_bytes": len(payload)})
            assert_true(status == 200 and body.get("verified") is True and body.get("sha256_match") is True and body.get("size_match") is True, "verify should match expected hash and size")

            status, body = json_request("POST", first_base, "/verify", body={"filename": "demo.slot", "sha256": "0" * 64})
            assert_true(status == 200 and body.get("verified") is False and body.get("sha256_match") is False, "verify should report mismatched hash without deleting")
            assert_true(slot_file.exists(), "verify mismatch must not delete the file")

            outside = slot_dir.parent / "outside.slot"
            outside.write_bytes(b"outside")
            outside_hash = sha256_file(outside)
            traversal = urllib.parse.quote("../outside.slot", safe="")
            status, _ = json_request("GET", first_base, f"/slots/{traversal}/info?hash=1")
            assert_true(status == 400, "sidecar should reject GET path traversal")
            status, _ = json_request("POST", first_base, "/verify", body={"filename": "../outside.slot", "sha256": outside_hash})
            assert_true(status == 400, "sidecar should reject verify path traversal")
            status, _ = json_request("POST", first_base, "/evict", body={"filename": "../outside.slot", "expected_sha256": outside_hash})
            assert_true(status == 400, "sidecar should reject evict path traversal")
            assert_true(outside.exists() and sha256_file(outside) == outside_hash, "path traversal attempts must not touch outside files")

            status, body = json_request("POST", first_base, "/evict", body={"filename": "demo.slot"})
            assert_true(status == 400 and body.get("evicted") is False, "evict without expected hash should fail closed")
            assert_true(slot_file.exists(), "evict without expected hash must leave the file in place")

            status, body = json_request("POST", first_base, "/evict", body={"filename": "demo.slot", "expected_sha256": "0" * 64})
            assert_true(status == 409 and body.get("evicted") is False, "evict with wrong expected hash should fail closed")
            assert_true(slot_file.exists(), "failed evict must leave the file in place")

            first_server.shutdown()
            first_server.server_close()
            first_server = None

            second_server = start_sidecar(slot_dir)
            second_base = base_url(second_server)
            status, body = json_request("GET", second_base, "/inventory?hash=1")
            assert_true(status == 200 and body.get("count") == 1, "restarted sidecar should recover inventory from disk")
            assert_true(body.get("slots", [{}])[0].get("sha256") == slot_hash, "restarted inventory should preserve file hash")

            status, body = json_request("POST", second_base, "/evict", body={"filename": "demo.slot", "expected_sha256": slot_hash})
            assert_true(status == 200 and body.get("evicted") is True, "evict with matching expected hash should delete file")
            assert_true(not slot_file.exists(), "successful evict should remove the file")

            status, body = json_request("POST", second_base, "/evict", body={"filename": "demo.slot", "expected_sha256": slot_hash})
            assert_true(status == 200 and body.get("evicted") is False and body.get("reason") == "not_found", "second evict should be a no-op")

            status, body = json_request("GET", second_base, "/inventory?hash=1")
            assert_true(status == 200 and body.get("count") == 0 and body.get("total_size_bytes") == 0, "inventory should be empty after evict")

            print(json.dumps({"ok": True, "slot_dir": str(slot_dir), "slot_sha256": slot_hash}, sort_keys=True))
            return 0
        finally:
            for server in (first_server, second_server):
                if server is not None:
                    server.shutdown()
                    server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
