#!/usr/bin/env python3
"""Offline matrix test for setup-doctor inventory and live-check behavior."""

from __future__ import annotations

import json
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

import cache_router_setup_doctor as doctor  # noqa: E402


class HealthHandler(BaseHTTPRequestHandler):
    server_version = "CachyRouterSetupDoctorMatrix/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps({"ok": True, "status": "ok"}, sort_keys=True).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def base_url(server: ThreadingHTTPServer) -> str:
    host, port = server.server_address[:2]
    return f"http://{host}:{port}"


def closed_loopback_url() -> str:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    host, port = sock.getsockname()
    sock.close()
    return f"http://{host}:{port}"


def worker_row(index: int, *, kind: str = "http", sidecar: bool = True, worker_url: str | None = None, slot_path: str | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "worker_id": f"worker-{index}",
        "ssh_host": f"worker-{index}-ssh",
        "worker_url": worker_url or f"http://127.0.0.1:{18100 + index}",
        "slot_save_path": f"/var/lib/cache-router/worker-{index}/slots" if slot_path is None else slot_path,
        "slot_id": 0,
        "transport": {"kind": kind},
    }
    if sidecar:
        row["transport"]["sidecar_url"] = f"http://127.0.0.1:{18200 + index}"
    return row


def valid_cache_storage(*, volume_id_hash: str | None = None) -> dict[str, Any]:
    return {
        "cache_root": "/home/<user>/.cache/strix-halo-cache-router",
        "durable_blob_encryption_at_rest": {
            "required": True,
            "mode": "operator_managed_encrypted_filesystem",
            "evidence_basis": "operator_attestation",
            "volume_id_hash": volume_id_hash or ("a" * 64),
            "key_owner": "operator",
        },
    }


def issue_codes(issues: list[doctor.Issue], *, level: str | None = None) -> set[str]:
    return {issue.code for issue in issues if level is None or issue.level == level}


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[doctor.Issue]]:
    return doctor.validate_inventory({"workers": rows})


def validate_inventory(raw: dict[str, Any]) -> tuple[list[dict[str, Any]], list[doctor.Issue]]:
    return doctor.validate_inventory(raw)


def assert_no_failures(rows: list[dict[str, Any]], message: str) -> list[dict[str, Any]]:
    workers, issues = validate_rows(rows)
    failures = issue_codes(issues, level="fail")
    assert_true(not failures, f"{message}: unexpected failures {sorted(failures)}")
    return workers


def assert_failure_codes(rows: list[dict[str, Any]], expected_codes: set[str], message: str) -> None:
    _, issues = validate_rows(rows)
    failures = issue_codes(issues, level="fail")
    missing = expected_codes - failures
    assert_true(not missing, f"{message}: missing expected failures {sorted(missing)} from {sorted(failures)}")


def test_valid_inventory_counts() -> list[int]:
    counts = [1, 2, 3, 8]
    for count in counts:
        workers = assert_no_failures([worker_row(index) for index in range(1, count + 1)], f"valid {count}-worker inventory")
        assert_true(len(workers) == count, f"valid {count}-worker inventory should preserve worker count")
    return counts


def test_sidecar_requirement_scope() -> None:
    assert_failure_codes([worker_row(1, kind="http", sidecar=False)], {"missing_url"}, "HTTP transport should require sidecar_url")
    assert_no_failures([worker_row(1, kind="local", sidecar=False)], "local transport should not require sidecar_url")
    assert_no_failures([worker_row(1, kind="ssh", sidecar=False)], "ssh transport should not require sidecar_url")


def test_broken_inventories() -> dict[str, list[str]]:
    cases: dict[str, tuple[list[dict[str, Any]], set[str]]] = {}
    duplicate = [worker_row(1), worker_row(2)]
    duplicate[1]["worker_id"] = duplicate[0]["worker_id"]
    cases["duplicate_worker_id"] = (duplicate, {"duplicate_worker_id"})
    invalid_url = [worker_row(1, worker_url="not-a-url")]
    cases["invalid_worker_url"] = (invalid_url, {"invalid_url"})
    invalid_sidecar = [worker_row(1)]
    invalid_sidecar[0]["transport"]["sidecar_url"] = "not-a-url"
    cases["invalid_http_sidecar_url"] = (invalid_sidecar, {"invalid_url"})
    missing_slot = [worker_row(1, slot_path="")]
    cases["missing_slot_path"] = (missing_slot, {"missing_path"})
    newline_slot = [worker_row(1, slot_path="/var/lib/cache-router\nslots")]
    cases["newline_slot_path"] = (newline_slot, {"unsafe_path"})
    unsafe_path = [worker_row(1, slot_path="relative/slots")]
    cases["relative_slot_path"] = (unsafe_path, {"relative_path"})
    traversal_path = [worker_row(1, slot_path="/var/lib/cache-router/../outside")]
    cases["path_traversal"] = (traversal_path, {"path_traversal"})
    bad_transport = [worker_row(1, kind="ftp")]
    cases["bad_transport"] = (bad_transport, {"invalid_transport"})
    missing_ssh_host = [worker_row(1, kind="ssh", sidecar=False)]
    missing_ssh_host[0]["ssh_host"] = ""
    cases["missing_ssh_host"] = (missing_ssh_host, {"missing_ssh_host"})

    observed: dict[str, list[str]] = {}
    for name, (rows, expected) in cases.items():
        _, issues = validate_rows(rows)
        failures = issue_codes(issues, level="fail")
        missing = expected - failures
        assert_true(not missing, f"{name}: missing expected failure codes {sorted(missing)} from {sorted(failures)}")
        observed[name] = sorted(failures)
    return observed


def test_cache_storage_encryption() -> dict[str, Any]:
    valid_raw = {"cache_storage": valid_cache_storage(), "workers": [worker_row(1)]}
    _, valid_issues = validate_inventory(valid_raw)
    assert_true(not issue_codes(valid_issues, level="fail"), f"valid cache_storage should not fail: {sorted(issue_codes(valid_issues, level='fail'))}")

    placeholder_raw = {"cache_storage": valid_cache_storage(volume_id_hash="<encrypted-volume-id-sha256>"), "workers": [worker_row(1)]}
    _, placeholder_issues = validate_inventory(placeholder_raw)
    assert_true("encryption_volume_hash_placeholder" in issue_codes(placeholder_issues, level="warn"), "placeholder volume_id_hash should warn")
    assert_true(not issue_codes(placeholder_issues, level="fail"), "placeholder public cache_storage should not fail")

    cases: dict[str, tuple[dict[str, Any], set[str]]] = {}
    missing_metadata = valid_cache_storage()
    missing_metadata.pop("durable_blob_encryption_at_rest", None)
    cases["missing_encryption_metadata"] = (missing_metadata, {"encryption_at_rest_missing"})

    plaintext_mode = valid_cache_storage()
    plaintext_mode["durable_blob_encryption_at_rest"]["mode"] = "plaintext-dev"
    cases["plaintext_mode"] = (plaintext_mode, {"invalid_encryption_at_rest_mode"})

    required_false = valid_cache_storage()
    required_false["durable_blob_encryption_at_rest"]["required"] = False
    cases["required_false"] = (required_false, {"encryption_at_rest_not_required"})

    bad_evidence_basis = valid_cache_storage()
    bad_evidence_basis["durable_blob_encryption_at_rest"]["evidence_basis"] = "trust_me"
    cases["bad_evidence_basis"] = (bad_evidence_basis, {"invalid_encryption_evidence_basis"})

    bad_volume_hash = valid_cache_storage()
    bad_volume_hash["durable_blob_encryption_at_rest"]["volume_id_hash"] = "raw-volume-name"
    cases["bad_volume_hash"] = (bad_volume_hash, {"invalid_encryption_volume_hash"})

    blank_key_owner = valid_cache_storage()
    blank_key_owner["durable_blob_encryption_at_rest"]["key_owner"] = ""
    cases["blank_key_owner"] = (blank_key_owner, {"invalid_encryption_key_owner"})

    observed: dict[str, list[str]] = {}
    for name, (cache_storage, expected) in cases.items():
        _, issues = validate_inventory({"cache_storage": cache_storage, "workers": [worker_row(1)]})
        failures = issue_codes(issues, level="fail")
        missing = expected - failures
        assert_true(not missing, f"{name}: missing expected failure codes {sorted(missing)} from {sorted(failures)}")
        observed[name] = sorted(failures)
    return {
        "valid_ok": True,
        "placeholder_warns": "encryption_volume_hash_placeholder" in issue_codes(placeholder_issues, level="warn"),
        "broken_cases": observed,
    }


def assert_live_failure(*, router_base_url: str, worker_url: str, sidecar_url: str, expected_code: str, message: str) -> list[str]:
    rows = [worker_row(1, worker_url=worker_url)]
    rows[0]["transport"]["sidecar_url"] = sidecar_url
    live_issues, _ = doctor.run_live_checks(rows, router_base_url=router_base_url, timeout=0.25)
    failures = issue_codes(live_issues, level="fail")
    assert_true(expected_code in failures, f"{message}: expected {expected_code}, got {sorted(failures)}")
    return sorted(failures)


def test_live_checks() -> dict[str, Any]:
    router = start_health_server()
    worker = start_health_server()
    sidecar = start_health_server()
    try:
        router_url = base_url(router)
        worker_url = base_url(worker)
        sidecar_url = base_url(sidecar)

        rows = [worker_row(1, worker_url=worker_url)]
        rows[0]["transport"]["sidecar_url"] = base_url(sidecar)
        live_issues, live_checks = doctor.run_live_checks(rows, router_base_url=router_url, timeout=2.0)
        assert_true(not issue_codes(live_issues, level="fail"), "healthy loopback live checks should not fail")
        assert_true(live_checks.get("router", {}).get("ok") is True, "router health live check should be ok")
        first_worker = live_checks.get("workers", [{}])[0]
        assert_true(first_worker.get("worker_health", {}).get("ok") is True, "worker health live check should be ok")
        assert_true(first_worker.get("sidecar_health", {}).get("ok") is True, "sidecar health live check should be ok")

        failures = {
            "live_unreachable_router": assert_live_failure(
                router_base_url=closed_loopback_url(),
                worker_url=worker_url,
                sidecar_url=sidecar_url,
                expected_code="router_health_failed",
                message="closed router endpoint should fail",
            ),
            "live_unreachable_worker": assert_live_failure(
                router_base_url=router_url,
                worker_url=closed_loopback_url(),
                sidecar_url=sidecar_url,
                expected_code="worker_health_failed",
                message="closed worker endpoint should fail",
            ),
            "live_unreachable_http_sidecar": assert_live_failure(
                router_base_url=router_url,
                worker_url=worker_url,
                sidecar_url=closed_loopback_url(),
                expected_code="sidecar_health_failed",
                message="closed sidecar endpoint should fail",
            ),
        }

        original_live_get_json = doctor.live_get_json
        remote_router = "http://203.0.113.10:18080"

        def fake_live_get_json(url: str, *, timeout: float) -> tuple[bool, dict[str, Any], float]:
            if url == remote_router + "/health":
                return True, {"http_status": 200, "body": {"ok": True}}, 0.0
            return original_live_get_json(url, timeout=timeout)

        doctor.live_get_json = fake_live_get_json
        try:
            loopback_rows = [worker_row(1, worker_url="http://127.0.0.1:18101")]
            loopback_rows[0]["transport"]["sidecar_url"] = "http://127.0.0.1:18102"
            loopback_issues, loopback_checks = doctor.run_live_checks(loopback_rows, router_base_url=remote_router, timeout=0.25)
        finally:
            doctor.live_get_json = original_live_get_json
        warnings = issue_codes(loopback_issues, level="warn")
        assert_true("loopback_worker_live_check_skipped" in warnings, "remote router with loopback worker URL should warn and skip worker check")
        assert_true("loopback_sidecar_live_check_skipped" in warnings, "remote router with loopback sidecar URL should warn and skip sidecar check")
        assert_true(not issue_codes(loopback_issues, level="fail"), "remote router loopback skip case should not fail")

        return {
            "healthy_workers": len(live_checks.get("workers", [])),
            "failure_cases": failures,
            "loopback_skip_warnings": sorted(warnings),
            "loopback_skip_workers": len(loopback_checks.get("workers", [])),
        }
    finally:
        for server in (router, worker, sidecar):
            server.shutdown()
            server.server_close()


def main() -> int:
    valid_counts = test_valid_inventory_counts()
    test_sidecar_requirement_scope()
    broken = test_broken_inventories()
    cache_storage = test_cache_storage_encryption()
    live = test_live_checks()
    print(
        json.dumps(
            {
                "ok": True,
                "valid_inventory_counts": valid_counts,
                "broken_cases": broken,
                "cache_storage_encryption": cache_storage,
                "live_checks": live,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
