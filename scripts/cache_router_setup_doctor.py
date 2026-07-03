#!/usr/bin/env python3
"""Validate cache-router worker inventory and print setup guidance.

This is the offline-first setup checker for GitHub users. It validates the
router worker inventory shape without starting services or contacting workers by
default. Add ``--live`` only after workers and the router are already running.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


WORKER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UNKNOWN_STRICT_VALUES = {"unknown", "not_captured", "not_interpreted"}
ENCRYPTION_AT_REST_MODES = {"operator_managed_encrypted_filesystem", "platform_encrypted_volume"}
ENCRYPTION_EVIDENCE_BASES = {"operator_attestation", "setup_doctor_metadata"}
STRICT_CACHE_FIELDS = [
    "model_architecture",
    "model_hash",
    "gguf_tensor_manifest_hash",
    "tokenizer_hash",
    "chat_template_effective_hash",
    "tools_schema_hash",
    "system_prompt_hash",
    "special_token_policy",
    "llama_cpp_source_commit",
    "llama_cpp_cache_abi_version",
    "patchset_id",
    "build_backend",
    "gpu_backend_driver",
    "kv_unified_mode",
    "ctx_size",
    "ctx_checkpoints_config",
    "cache_type_k",
    "cache_type_v",
    "flash_attention_mode",
    "rope_freq_base",
    "rope_freq_scale",
    "yarn_or_rope_scaling_metadata",
    "reasoning_format",
    "jinja_template_mode",
    "mtp_enabled",
    "spec_draft_model_hash",
    "spec_draft_config",
    "n_parallel",
    "n_seq_max",
]
STRICT_HASH_FIELDS = {
    "model_hash",
    "gguf_tensor_manifest_hash",
    "tokenizer_hash",
    "chat_template_effective_hash",
    "tools_schema_hash",
    "system_prompt_hash",
    "spec_draft_model_hash",
}


@dataclass(frozen=True)
class Issue:
    level: str
    code: str
    message: str
    path: str

    def as_dict(self) -> dict[str, str]:
        return {"level": self.level, "code": self.code, "message": self.message, "path": self.path}


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc


def is_placeholder(value: Any) -> bool:
    return isinstance(value, str) and ("<" in value or ">" in value)


def bool_value(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def strict_metadata_value_issue(row: dict[str, Any], field: str, *, mtp_enabled: bool) -> str | None:
    value = row.get(field)
    if value in (None, ""):
        return "absent"
    if is_placeholder(value):
        return "placeholder"
    if isinstance(value, str) and value in UNKNOWN_STRICT_VALUES:
        return "uncaptured"
    if field in {"spec_draft_model_hash", "spec_draft_config"} and not mtp_enabled and value == "none":
        return None
    if field in STRICT_HASH_FIELDS and not (isinstance(value, str) and SHA256_RE.fullmatch(value)):
        return "invalid"
    if field in {"kv_unified_mode", "mtp_enabled"} and not isinstance(value, bool):
        return "invalid"
    if field in {"ctx_size", "n_parallel", "n_seq_max"} and (not isinstance(value, int) or isinstance(value, bool) or value <= 0):
        return "invalid"
    return None


def strict_cache_metadata_issues(row: dict[str, Any], *, prefix: str) -> list[Issue]:
    issues: list[Issue] = []
    mtp_enabled = bool(row.get("mtp_enabled", True))
    auto_derive = bool_value(row.get("strict_metadata_auto"), True)
    force_runtime = bool_value(row.get("strict_metadata_force_runtime") or row.get("strict_metadata_runtime_override"), False)
    if auto_derive:
        derivable = [
            field
            for field in STRICT_CACHE_FIELDS
            if strict_metadata_value_issue(row, field, mtp_enabled=mtp_enabled) is not None
        ]
        if force_runtime:
            issues.append(
                Issue(
                    "info",
                    "strict_metadata_force_runtime",
                    "runtime strict metadata derivation will replace hand-entered strict compatibility fields",
                    f"{prefix}.strict_metadata_force_runtime",
                )
            )
        if derivable:
            issues.append(
                Issue(
                    "info",
                    "strict_metadata_auto",
                    f"runtime strict metadata derivation is enabled; daemon will derive or fail closed for {len(derivable)} unset or placeholder fields",
                    f"{prefix}.strict_metadata_auto",
                )
            )
        return issues
    for field in STRICT_CACHE_FIELDS:
        value = row.get(field)
        field_path = f"{prefix}.{field}"
        if value in (None, ""):
            issues.append(Issue("warn", "strict_metadata_absent", f"{field} is required for cache build/use strict compatibility", field_path))
            continue
        if is_placeholder(value):
            issues.append(Issue("warn", "strict_metadata_placeholder", f"{field} still contains a placeholder; replace it before cache build/use", field_path))
            continue
        if isinstance(value, str) and value in UNKNOWN_STRICT_VALUES:
            issues.append(Issue("warn", "strict_metadata_uncaptured", f"{field} is {value!r}; cache build/use will fail closed", field_path))
            continue
        if field in {"spec_draft_model_hash", "spec_draft_config"} and not mtp_enabled and value == "none":
            continue
        if field in STRICT_HASH_FIELDS and not (isinstance(value, str) and SHA256_RE.fullmatch(value)):
            issues.append(Issue("warn", "strict_metadata_invalid", f"{field} should be a lowercase SHA-256 hex digest for cache build/use", field_path))
        elif field in {"kv_unified_mode", "mtp_enabled"} and not isinstance(value, bool):
            issues.append(Issue("warn", "strict_metadata_invalid", f"{field} should be a boolean for cache build/use", field_path))
        elif field in {"ctx_size", "n_parallel", "n_seq_max"} and (not isinstance(value, int) or isinstance(value, bool) or value <= 0):
            issues.append(Issue("warn", "strict_metadata_invalid", f"{field} should be a positive integer for cache build/use", field_path))
    return issues


def url_issue(value: str, *, path: str, field: str) -> Issue | None:
    if not value:
        return Issue("fail", "missing_url", f"{field} is required", path)
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return Issue("fail", "invalid_url", f"{field} must be an http(s) URL", path)
    if is_placeholder(value):
        return Issue("warn", "placeholder_url", f"{field} still contains a placeholder", path)
    return None


def safe_path_issue(value: str, *, path: str, field: str) -> Issue | None:
    if not value:
        return Issue("fail", "missing_path", f"{field} is required", path)
    if "\n" in value or "\r" in value:
        return Issue("fail", "unsafe_path", f"{field} must not contain newlines", path)
    if not value.startswith("/"):
        return Issue("fail", "relative_path", f"{field} must be an absolute worker-local path", path)
    if "/../" in value or value.endswith("/.."):
        return Issue("fail", "path_traversal", f"{field} must not contain '..' path traversal", path)
    if is_placeholder(value):
        return Issue("warn", "placeholder_path", f"{field} still contains a placeholder", path)
    return None


def get_workers(raw: Any) -> tuple[list[dict[str, Any]], list[Issue]]:
    issues: list[Issue] = []
    rows = raw.get("workers") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return [], [Issue("fail", "missing_workers", "inventory must be an object with a workers list or a workers list", "workers")]
    if not rows:
        issues.append(Issue("fail", "empty_workers", "workers list must not be empty", "workers"))
    workers: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            issues.append(Issue("fail", "worker_not_object", "worker entry must be an object", f"workers[{index}]"))
            continue
        workers.append(row)
    return workers, issues


def cache_storage(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    value = raw.get("cache_storage")
    return value if isinstance(value, dict) else None


def validate_cache_storage(raw: Any) -> list[Issue]:
    if not isinstance(raw, dict):
        return []
    if "cache_storage" not in raw:
        return [
            Issue(
                "warn",
                "cache_storage_missing",
                "cache_storage is recommended before cache build/use so durable blob encryption-at-rest can be audited",
                "cache_storage",
            )
        ]
    storage = raw.get("cache_storage")
    if not isinstance(storage, dict):
        return [Issue("fail", "cache_storage_not_object", "cache_storage must be an object", "cache_storage")]
    issues: list[Issue] = []
    cache_root = storage.get("cache_root")
    if not isinstance(cache_root, str) or not cache_root:
        issues.append(Issue("fail", "cache_root_missing", "cache_storage.cache_root is required", "cache_storage.cache_root"))
    elif not cache_root.startswith("/"):
        issues.append(Issue("fail", "cache_root_relative", "cache_storage.cache_root must be an absolute local path outside Git", "cache_storage.cache_root"))
    elif is_placeholder(cache_root):
        issues.append(Issue("warn", "cache_root_placeholder", "cache_storage.cache_root still contains a placeholder", "cache_storage.cache_root"))

    metadata = storage.get("durable_blob_encryption_at_rest")
    if not isinstance(metadata, dict):
        return issues + [
            Issue(
                "fail",
                "encryption_at_rest_missing",
                "cache_storage.durable_blob_encryption_at_rest is required when cache_storage is declared",
                "cache_storage.durable_blob_encryption_at_rest",
            )
        ]
    if metadata.get("required") is not True:
        issues.append(Issue("fail", "encryption_at_rest_not_required", "durable blob encryption-at-rest must be explicitly required", "cache_storage.durable_blob_encryption_at_rest.required"))
    mode = metadata.get("mode")
    if mode not in ENCRYPTION_AT_REST_MODES:
        issues.append(Issue("fail", "invalid_encryption_at_rest_mode", "encryption mode must be operator_managed_encrypted_filesystem or platform_encrypted_volume", "cache_storage.durable_blob_encryption_at_rest.mode"))
    evidence_basis = metadata.get("evidence_basis")
    if evidence_basis not in ENCRYPTION_EVIDENCE_BASES:
        issues.append(Issue("fail", "invalid_encryption_evidence_basis", "encryption evidence basis must be operator_attestation or setup_doctor_metadata", "cache_storage.durable_blob_encryption_at_rest.evidence_basis"))
    volume_id_hash = metadata.get("volume_id_hash")
    if is_placeholder(volume_id_hash):
        issues.append(Issue("warn", "encryption_volume_hash_placeholder", "replace the encrypted volume identifier hash before using cache build/use", "cache_storage.durable_blob_encryption_at_rest.volume_id_hash"))
    elif not isinstance(volume_id_hash, str) or not SHA256_RE.fullmatch(volume_id_hash):
        issues.append(Issue("fail", "invalid_encryption_volume_hash", "volume_id_hash must be a lowercase SHA-256 digest, not a raw volume name or key", "cache_storage.durable_blob_encryption_at_rest.volume_id_hash"))
    key_owner = metadata.get("key_owner")
    if is_placeholder(key_owner):
        issues.append(Issue("warn", "encryption_key_owner_placeholder", "replace key_owner with a short non-secret operator label", "cache_storage.durable_blob_encryption_at_rest.key_owner"))
    elif not isinstance(key_owner, str) or not key_owner.strip():
        issues.append(Issue("fail", "invalid_encryption_key_owner", "key_owner must be a non-empty non-secret label", "cache_storage.durable_blob_encryption_at_rest.key_owner"))
    return issues


def worker_id(row: dict[str, Any]) -> str:
    return str(row.get("worker_id") or "").strip()


def worker_url(row: dict[str, Any]) -> str:
    return str(row.get("worker_url") or row.get("url") or "").strip().rstrip("/")


def slot_save_path(row: dict[str, Any]) -> str:
    return str(row.get("slot_save_path") or row.get("worker_slot_dir") or "").strip().rstrip("/")


def transport(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("transport")
    if isinstance(raw, dict):
        return raw
    return {}


def transport_kind(row: dict[str, Any]) -> str:
    return str(transport(row).get("kind") or row.get("worker_transport") or "local").strip()


def sidecar_url(row: dict[str, Any]) -> str:
    return str(transport(row).get("sidecar_url") or row.get("worker_sidecar_url") or "").strip().rstrip("/")


def ssh_host(row: dict[str, Any]) -> str:
    return str(row.get("ssh_host") or transport(row).get("ssh_host") or row.get("worker_ssh_host") or "").strip()


def is_loopback_url(value: str) -> bool:
    parsed = urllib.parse.urlparse(value)
    return parsed.hostname in {"127.0.0.1", "localhost", "::1"}


def validate_worker(row: dict[str, Any], index: int) -> list[Issue]:
    issues: list[Issue] = []
    prefix = f"workers[{index}]"
    wid = worker_id(row)
    if not wid:
        issues.append(Issue("fail", "missing_worker_id", "worker_id is required", f"{prefix}.worker_id"))
    elif not WORKER_ID_RE.match(wid):
        issues.append(Issue("fail", "invalid_worker_id", "worker_id must be a short slug: letters, numbers, dot, underscore, dash", f"{prefix}.worker_id"))
    elif is_placeholder(wid):
        issues.append(Issue("warn", "placeholder_worker_id", "worker_id still contains a placeholder", f"{prefix}.worker_id"))

    url = worker_url(row)
    issue = url_issue(url, path=f"{prefix}.worker_url", field="worker_url")
    if issue:
        issues.append(issue)

    slot_path = slot_save_path(row)
    issue = safe_path_issue(slot_path, path=f"{prefix}.slot_save_path", field="slot_save_path")
    if issue:
        issues.append(issue)

    slot_id = row.get("slot_id", 0)
    if not isinstance(slot_id, int) or slot_id < 0:
        issues.append(Issue("fail", "invalid_slot_id", "slot_id must be a non-negative integer", f"{prefix}.slot_id"))

    kind = transport_kind(row)
    if kind not in {"local", "ssh", "http"}:
        issues.append(Issue("fail", "invalid_transport", "transport.kind must be local, ssh, or http", f"{prefix}.transport.kind"))
    if kind == "http":
        issue = url_issue(sidecar_url(row), path=f"{prefix}.transport.sidecar_url", field="transport.sidecar_url")
        if issue:
            issues.append(issue)
    if kind == "ssh" and not ssh_host(row):
        issues.append(Issue("fail", "missing_ssh_host", "ssh transport requires transport.ssh_host or top-level ssh_host", f"{prefix}.transport.ssh_host"))
    if kind == "local" and len(str(row.get("worker_url", ""))) > 0 and not worker_url(row).startswith("http://127.0.0.1"):
        issues.append(Issue("warn", "local_transport_remote_url", "local transport assumes router and worker share a filesystem; use http sidecars for independent router placement", f"{prefix}.transport.kind"))

    if not ssh_host(row):
        issues.append(Issue("warn", "missing_setup_ssh_host", "optional ssh_host is missing; doctor cannot print a complete start-worker command for this worker", f"{prefix}.ssh_host"))
    elif is_placeholder(ssh_host(row)):
        issues.append(Issue("warn", "placeholder_ssh_host", "ssh_host still contains a placeholder", f"{prefix}.ssh_host"))
    issues.extend(strict_cache_metadata_issues(row, prefix=prefix))
    return issues


def validate_inventory(raw: Any) -> tuple[list[dict[str, Any]], list[Issue]]:
    workers, issues = get_workers(raw)
    issues.extend(validate_cache_storage(raw))
    seen_ids: dict[str, int] = {}
    seen_urls: dict[str, int] = {}
    seen_slots: dict[str, int] = {}
    for index, row in enumerate(workers):
        issues.extend(validate_worker(row, index))
        wid = worker_id(row)
        url = worker_url(row)
        slot_path = slot_save_path(row)
        if wid:
            if wid in seen_ids:
                issues.append(Issue("fail", "duplicate_worker_id", f"worker_id duplicates workers[{seen_ids[wid]}]", f"workers[{index}].worker_id"))
            seen_ids[wid] = index
        if url:
            if url in seen_urls:
                issues.append(Issue("fail", "duplicate_worker_url", f"worker_url duplicates workers[{seen_urls[url]}]", f"workers[{index}].worker_url"))
            seen_urls[url] = index
        if slot_path:
            if slot_path in seen_slots:
                issues.append(Issue("fail", "duplicate_slot_save_path", f"slot_save_path duplicates workers[{seen_slots[slot_path]}]", f"workers[{index}].slot_save_path"))
            seen_slots[slot_path] = index
    return workers, issues


def live_get_json(url: str, *, timeout: float) -> tuple[bool, dict[str, Any], float]:
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            body = json.loads(raw) if raw else {}
            return resp.status == 200, {"http_status": resp.status, "body": body}, (time.perf_counter() - start) * 1000.0
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return False, {"error": repr(exc)}, (time.perf_counter() - start) * 1000.0


def run_live_checks(workers: list[dict[str, Any]], *, router_base_url: str, timeout: float) -> tuple[list[Issue], dict[str, Any]]:
    issues: list[Issue] = []
    checks: dict[str, Any] = {"workers": []}
    router_is_remote = bool(router_base_url and not is_loopback_url(router_base_url))
    if router_base_url:
        ok, body, wall_ms = live_get_json(router_base_url.rstrip("/") + "/health", timeout=timeout)
        checks["router"] = {"ok": ok, "wall_ms": wall_ms, **body}
        if not ok:
            issues.append(Issue("fail", "router_health_failed", "router /health did not return HTTP 200 JSON", "router_base_url"))
    for index, row in enumerate(workers):
        worker_check: dict[str, Any] = {"worker_id": worker_id(row)}
        url = worker_url(row)
        if url and not is_placeholder(url):
            if router_is_remote and is_loopback_url(url):
                worker_check["worker_health"] = {"ok": None, "skipped": True, "reason": "loopback URL is router-relative; use router /health or run doctor on the router host"}
                issues.append(Issue("warn", "loopback_worker_live_check_skipped", "worker_url is loopback relative to the router host and was not checked from this controller", f"workers[{index}].worker_url"))
            else:
                ok, body, wall_ms = live_get_json(url + "/health", timeout=timeout)
                worker_check["worker_health"] = {"ok": ok, "wall_ms": wall_ms, **body}
                if not ok:
                    issues.append(Issue("fail", "worker_health_failed", "worker /health did not return HTTP 200 JSON", f"workers[{index}].worker_url"))
        if transport_kind(row) == "http":
            surl = sidecar_url(row)
            if surl and not is_placeholder(surl):
                if router_is_remote and is_loopback_url(surl):
                    worker_check["sidecar_health"] = {"ok": None, "skipped": True, "reason": "loopback URL is router-relative; use router /health or run doctor on the router host"}
                    issues.append(Issue("warn", "loopback_sidecar_live_check_skipped", "sidecar_url is loopback relative to the router host and was not checked from this controller", f"workers[{index}].transport.sidecar_url"))
                else:
                    ok, body, wall_ms = live_get_json(surl + "/health", timeout=timeout)
                    worker_check["sidecar_health"] = {"ok": ok, "wall_ms": wall_ms, **body}
                    if not ok:
                        issues.append(Issue("fail", "sidecar_health_failed", "sidecar /health did not return HTTP 200 JSON", f"workers[{index}].transport.sidecar_url"))
        checks["workers"].append(worker_check)
    return issues, checks


def start_worker_command(row: dict[str, Any]) -> str | None:
    host = ssh_host(row)
    if not host:
        return None
    parts = [
        "python3 scripts/cache_router_remote_stack.py start-worker",
        f"--remote-host {host}",
        f"--worker-id {worker_id(row)}",
        "--worker-bind-host 0.0.0.0",
        f"--worker-transport {transport_kind(row)}",
        "--sidecar-bind-host 0.0.0.0",
        "--allow-unauthenticated-lan",
    ]
    if transport_kind(row) == "http":
        parts.append(f"--worker-sidecar-url {sidecar_url(row)}")
    llama_server = row.get("llama_server_path") or row.get("llama_server")
    model_path = row.get("model_path") or row.get("model_file") or row.get("model")
    mtp_model = row.get("spec_draft_model_path") or row.get("mtp_model") or row.get("draft_model_path")
    if llama_server:
        parts.append(f"--llama-server {llama_server}")
    if model_path:
        parts.append(f"--model {model_path}")
    if mtp_model:
        parts.append(f"--mtp-model {mtp_model}")
    if row.get("ctx_size"):
        parts.append(f"--ctx-size {row['ctx_size']}")
    return " \\\n  ".join(parts)


def build_summary(
    *,
    workers_file: Path,
    raw_inventory: Any,
    workers: list[dict[str, Any]],
    issues: list[Issue],
    live_checks: dict[str, Any] | None,
    router_host_alias: str,
    router_base_url: str,
) -> dict[str, Any]:
    failures = [issue for issue in issues if issue.level == "fail"]
    warnings = [issue for issue in issues if issue.level == "warn"]
    worker_rows = []
    for row in workers:
        worker_rows.append(
            {
                "worker_id": worker_id(row),
                "worker_url": worker_url(row),
                "slot_save_path": slot_save_path(row),
                "slot_id": row.get("slot_id", 0),
                "transport": {"kind": transport_kind(row), "sidecar_url": sidecar_url(row) or None, "ssh_host": ssh_host(row) or None},
            }
        )
    storage = cache_storage(raw_inventory)
    encryption = storage.get("durable_blob_encryption_at_rest") if isinstance(storage, dict) else None
    router_command_parts = [
        "python3 scripts/cache_router_remote_stack.py restart-router",
        f"  --remote-host {router_host_alias}",
        "  --router-host 0.0.0.0",
        "  --no-router-auth",
        "  --allow-unauthenticated-lan",
    ]
    if isinstance(encryption, dict):
        router_command_parts.extend(
            [
                f"  --durable-blob-encryption-mode {encryption.get('mode')}",
                f"  --durable-blob-encryption-evidence-basis {encryption.get('evidence_basis')}",
                f"  --durable-blob-encryption-volume-id-hash {encryption.get('volume_id_hash')}",
                f"  --durable-blob-encryption-key-owner {encryption.get('key_owner')}",
            ]
        )
    router_command_parts.append(f"  --workers-file {workers_file.as_posix()}")
    commands = {
        "start_workers": [cmd for cmd in (start_worker_command(row) for row in workers) if cmd],
        "start_router": " \\\n".join(router_command_parts),
        "check_router": f"curl -fsS {router_base_url.rstrip('/') if router_base_url else 'http://<router-lan-ip>:18080'}/health",
    }
    runtime_notice = (
        "Generated start/check commands are live operations. Run them only with an operator-supplied deployment "
        "inventory, reachable private hosts, and a trusted private LAN boundary."
    )
    storage_summary: dict[str, Any] = {
        "declared": storage is not None,
        "cache_root": "<cache-root>" if storage and storage.get("cache_root") else None,
        "encryption_at_rest": None,
    }
    if storage:
        metadata = storage.get("durable_blob_encryption_at_rest")
        if isinstance(metadata, dict):
            storage_summary["encryption_at_rest"] = {
                "required": metadata.get("required"),
                "mode": metadata.get("mode"),
                "evidence_basis": metadata.get("evidence_basis"),
                "volume_id_hash": metadata.get("volume_id_hash") if isinstance(metadata.get("volume_id_hash"), str) else None,
                "key_owner": str(metadata.get("key_owner"))[:80] if metadata.get("key_owner") is not None else None,
            }
    return {
        "ok": not failures,
        "workers_file": workers_file.as_posix(),
        "worker_count": len(workers),
        "cache_storage": storage_summary,
        "failures": len(failures),
        "warnings": len(warnings),
        "issues": [issue.as_dict() for issue in issues],
        "workers": worker_rows,
        "commands": commands,
        "runtime_notice": runtime_notice,
        "client": {
            "base_url": (router_base_url.rstrip("/") + "/v1") if router_base_url else "http://<router-lan-ip>:18080/v1",
            "model": "Step-3.7",
            "auth": "authentication not required for trusted LAN mode",
        },
        "live_checks": live_checks or {},
    }


def print_text(summary: dict[str, Any]) -> None:
    status = "ok" if summary["ok"] else "fail"
    print(f"cache-router setup doctor: {status}")
    print(f"workers file: {summary['workers_file']}")
    print(f"workers: {summary['worker_count']}")
    print(f"failures: {summary['failures']}")
    print(f"warnings: {summary['warnings']}")
    storage = summary.get("cache_storage") if isinstance(summary.get("cache_storage"), dict) else {}
    if storage:
        encryption = storage.get("encryption_at_rest") if isinstance(storage.get("encryption_at_rest"), dict) else {}
        print(f"cache storage declared: {bool(storage.get('declared'))}")
        if encryption:
            print(f"durable blob encryption required: {encryption.get('required')}")
            print(f"durable blob encryption mode: {encryption.get('mode')}")
    if summary["issues"]:
        print("")
        print("Issues:")
        for issue in summary["issues"]:
            print(f"- {issue['level']} {issue['code']} at {issue['path']}: {issue['message']}")
    print("")
    print("Client:")
    for key, value in summary["client"].items():
        print(f"- {key}: {value}")
    if summary["commands"]["start_workers"]:
        print("")
        print("Runtime notice:")
        print(summary["runtime_notice"])
        print("")
        print("Start workers:")
        for command in summary["commands"]["start_workers"]:
            print(command)
    elif summary.get("runtime_notice"):
        print("")
        print("Runtime notice:")
        print(summary["runtime_notice"])
    print("")
    print("Start router:")
    print(summary["commands"]["start_router"])
    print("")
    print("Check router:")
    print(summary["commands"]["check_router"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers-file", default="configs/cache-router/workers.example.json", help="Worker inventory JSON file.")
    parser.add_argument("--router-host-alias", default="<router-ssh-alias>", help="SSH alias for the router host used in printed commands.")
    parser.add_argument("--router-base-url", default="", help="Existing router base URL for printed client settings and optional live check, e.g. http://<router-lan-ip>:18080.")
    parser.add_argument("--live", action="store_true", help="Also check /health on the router, workers, and HTTP sidecars. Does not start or stop services.")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout for --live checks.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workers_file = Path(args.workers_file)
    raw = load_json(workers_file)
    workers, issues = validate_inventory(raw)
    live_checks: dict[str, Any] | None = None
    if args.live:
        live_issues, live_checks = run_live_checks(workers, router_base_url=args.router_base_url, timeout=args.timeout)
        issues.extend(live_issues)
    summary = build_summary(
        workers_file=workers_file,
        raw_inventory=raw,
        workers=workers,
        issues=issues,
        live_checks=live_checks,
        router_host_alias=args.router_host_alias,
        router_base_url=args.router_base_url,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print_text(summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
