#!/usr/bin/env python3
"""Audit or rebuild a Cachy Router durable store from manifests.

This is an offline tool. It reads only a local cache root and never contacts
workers, sidecars, or router endpoints.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

SCHEMA_VERSION = "2026-07-01.1"
DELETED_MANIFEST_STATUSES = {"tenant_deleted", "deleted"}
REGISTRY_AUDIT_ACTIONS = ["lookup", "hit", "miss", "restore", "commit", "fallback", "denial"]
ENCRYPTION_AT_REST_MODES = {"operator_managed_encrypted_filesystem", "platform_encrypted_volume"}
ENCRYPTION_EVIDENCE_BASES = {"operator_attestation", "setup_doctor_metadata"}
STRICT_COMPATIBILITY_FIELDS = [
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
    "spec_draft_model_hash",
    "spec_draft_config",
    "n_parallel",
    "n_seq_max",
]
MANIFEST_REQUIRED_FIELDS = [
    "schema_version",
    "cache_id",
    "cache_key_hash",
    "manifest_id",
    "source_worker_id",
    "slot_file_sha256",
    "slot_file_size_bytes",
    "slot_filename",
    "worker_residency",
]


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", prefix=path.name + ".", suffix=".tmp", dir=path.parent, delete=False) as tmp:
        tmp.write(json_dumps(value))
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    try:
        os.replace(tmp_path, path)
        fsync_dir(path.parent)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def store_paths(cache_root: Path) -> dict[str, Path]:
    router_store = cache_root / "router-store"
    return {
        "cache_root": cache_root,
        "router_store": router_store,
        "manifests": router_store / "manifests",
        "blobs": router_store / "blobs",
        "registry": router_store / "registry.json",
        "registry_leases": router_store / "registry-leases.json",
        "registry_lock": router_store / "registry.lock",
        "registry_audit": router_store / "registry-audit.jsonl",
    }


def relative_to_root(path: Path, cache_root: Path) -> str:
    try:
        return path.resolve().relative_to(cache_root.resolve()).as_posix()
    except ValueError:
        return "<outside-cache-root>"


def content_addressed_blob_path(cache_root: Path, digest: str) -> Path:
    paths = store_paths(cache_root)
    return paths["blobs"] / digest[:2] / f"{digest}.slot"


@contextmanager
def registry_file_lock(cache_root: Path) -> Any:
    lock_path = store_paths(cache_root)["registry_lock"]
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_registry_leases_unlocked(cache_root: Path) -> dict[str, Any]:
    path = store_paths(cache_root)["registry_leases"]
    if path.is_file():
        try:
            leases = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            leases = {}
    else:
        leases = {}
    if not isinstance(leases, dict):
        leases = {}
    rows = [row for row in leases.get("leases", []) if isinstance(row, dict)]
    return {"schema_version": str(leases.get("schema_version") or SCHEMA_VERSION), "leases": rows}


def save_registry_leases_unlocked(cache_root: Path, leases: dict[str, Any]) -> None:
    leases["schema_version"] = SCHEMA_VERSION
    leases["updated_at"] = now_iso()
    write_json(store_paths(cache_root)["registry_leases"], leases)


def prune_registry_leases_unlocked(leases: dict[str, Any], *, now_unix: float) -> int:
    active: list[dict[str, Any]] = []
    expired = 0
    for row in leases.get("leases", []):
        if not isinstance(row, dict):
            expired += 1
            continue
        try:
            expires_at_unix = float(row.get("expires_at_unix"))
        except (TypeError, ValueError):
            expired += 1
            continue
        if expires_at_unix <= now_unix:
            expired += 1
            continue
        active.append(row)
    leases["leases"] = active
    return expired


def load_registry_leases(cache_root: Path) -> dict[str, Any]:
    with registry_file_lock(cache_root):
        leases = load_registry_leases_unlocked(cache_root)
        pruned = prune_registry_leases_unlocked(leases, now_unix=time.time())
        if pruned:
            save_registry_leases_unlocked(cache_root, leases)
        return leases


def prune_registry_leases(cache_root: Path) -> int:
    with registry_file_lock(cache_root):
        leases = load_registry_leases_unlocked(cache_root)
        pruned = prune_registry_leases_unlocked(leases, now_unix=time.time())
        if pruned:
            save_registry_leases_unlocked(cache_root, leases)
        return pruned


def acquire_registry_lease(
    cache_root: Path,
    *,
    operation: str,
    cache_id: str,
    cache_key_hash: str,
    owner_id: str,
    manifest_id: str | None = None,
    ttl_seconds: float = 300.0,
) -> dict[str, Any]:
    if not is_sha256_hex(cache_key_hash):
        raise RuntimeError("registry lease cache_key_hash must be a sha256 hex string")
    now_unix = time.time()
    ttl = max(0.001, float(ttl_seconds))
    expires_at_unix = now_unix + ttl
    with registry_file_lock(cache_root):
        leases = load_registry_leases_unlocked(cache_root)
        prune_registry_leases_unlocked(leases, now_unix=now_unix)
        for row in leases.get("leases", []):
            if row.get("cache_key_hash") == cache_key_hash and row.get("owner_id") != owner_id:
                raise RuntimeError(
                    "registry lease conflict: "
                    f"operation={operation} cache_key_hash={cache_key_hash} holder_owner={row.get('owner_id')}"
                )
        lease = {
            "schema_version": SCHEMA_VERSION,
            "lease_id": "lease-" + hashlib.sha256(f"{time.time_ns()}:{owner_id}:{cache_key_hash}".encode()).hexdigest()[:16],
            "owner_id": owner_id,
            "operation": str(operation)[:80],
            "cache_id": str(cache_id)[:120],
            "cache_key_hash": cache_key_hash,
            "manifest_id": str(manifest_id)[:120] if manifest_id is not None else None,
            "created_at": now_iso(),
            "created_at_unix": now_unix,
            "expires_at": datetime.fromtimestamp(expires_at_unix, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expires_at_unix": expires_at_unix,
            "ttl_seconds": ttl,
        }
        leases.setdefault("leases", []).append(lease)
        save_registry_leases_unlocked(cache_root, leases)
        return lease


def release_registry_lease(cache_root: Path, lease: dict[str, Any]) -> bool:
    lease_id = lease.get("lease_id") if isinstance(lease, dict) else None
    owner_id = lease.get("owner_id") if isinstance(lease, dict) else None
    if not lease_id:
        return False
    with registry_file_lock(cache_root):
        leases = load_registry_leases_unlocked(cache_root)
        before = len(leases.get("leases", []))
        leases["leases"] = [
            row
            for row in leases.get("leases", [])
            if not (isinstance(row, dict) and row.get("lease_id") == lease_id and (owner_id is None or row.get("owner_id") == owner_id))
        ]
        removed = len(leases["leases"]) != before
        if removed:
            save_registry_leases_unlocked(cache_root, leases)
        return removed


def append_registry_audit_event(
    cache_root: Path,
    *,
    operation: str,
    outcome: str,
    audit_actions: list[str],
    cache_id: Any = None,
    cache_key_hash: Any = None,
    manifest_id: Any = None,
    tenant_hash: Any = None,
    conversation_hash: Any = None,
    scope: Any = None,
    artifact_sha256: Any = None,
    reason: str | None = None,
    request_id: str | None = None,
    trace_id: str | None = None,
) -> None:
    bounded_actions = [action for action in REGISTRY_AUDIT_ACTIONS if action in set(audit_actions)]
    if not bounded_actions:
        return
    operation_key = f"{operation}:{request_id or ''}:{trace_id or ''}:{cache_id or ''}:{cache_key_hash or ''}:{manifest_id or ''}:{artifact_sha256 or ''}"
    operation_id = "op-" + hashlib.sha256(operation_key.encode()).hexdigest()[:16]
    event_key = f"{now_iso()}:{operation_id}"
    row = {
        "schema_version": SCHEMA_VERSION,
        "event_id": "registry-audit-" + hashlib.sha256(event_key.encode()).hexdigest()[:16],
        "operation_id": operation_id,
        "timestamp": now_iso(),
        "actor": "cache-router-store-audit",
        "source": "offline_store_tool",
        "operation": str(operation)[:80],
        "outcome": str(outcome)[:80],
        "audit_actions": bounded_actions,
        "request_id": request_id,
        "trace_id": trace_id or request_id,
        "cache_id": str(cache_id)[:120] if cache_id is not None else None,
        "cache_key_hash": cache_key_hash if is_sha256_hex(cache_key_hash) else None,
        "manifest_id": str(manifest_id)[:120] if manifest_id is not None else None,
        "tenant_hash": tenant_hash if is_sha256_hex(tenant_hash) else None,
        "conversation_hash": conversation_hash if is_sha256_hex(conversation_hash) else None,
        "scope": str(scope)[:80] if scope is not None else None,
        "artifact_sha256": artifact_sha256 if is_sha256_hex(artifact_sha256) else None,
        "reason": str(reason)[:200] if reason else None,
        "privacy": {
            "raw_prompt_logged": False,
            "raw_tenant_id_logged": False,
            "raw_conversation_id_logged": False,
            "raw_cache_blob_path_logged": False,
            "raw_worker_slot_path_logged": False,
            "contains_secret_material": False,
        },
    }
    path = store_paths(cache_root)["registry_audit"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def resolve_store_path(cache_root: Path, value: Any) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = cache_root / path
    return path


def load_manifest(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON: {exc}"
    if not isinstance(value, dict):
        return None, "manifest is not a JSON object"
    return value, None


def iter_manifest_paths(cache_root: Path) -> list[Path]:
    manifests_dir = store_paths(cache_root)["manifests"]
    if not manifests_dir.exists():
        return []
    return sorted(path for path in manifests_dir.glob("*.json") if path.is_file())


def encryption_at_rest_errors(manifest: dict[str, Any], *, require_encryption_at_rest: bool, active: bool) -> list[str]:
    if not require_encryption_at_rest or not active:
        return []
    errors: list[str] = []
    metadata = manifest.get("encryption_at_rest")
    if not isinstance(metadata, dict):
        return ["encryption_at_rest metadata must be an object when required"]
    if metadata.get("required") is not True:
        errors.append("encryption_at_rest.required must be true")
    mode = metadata.get("mode")
    if mode not in ENCRYPTION_AT_REST_MODES:
        allowed = ", ".join(sorted(ENCRYPTION_AT_REST_MODES))
        errors.append(f"encryption_at_rest.mode must be one of: {allowed}")
    evidence_basis = metadata.get("evidence_basis")
    if evidence_basis not in ENCRYPTION_EVIDENCE_BASES:
        allowed = ", ".join(sorted(ENCRYPTION_EVIDENCE_BASES))
        errors.append(f"encryption_at_rest.evidence_basis must be one of: {allowed}")
    if not is_sha256_hex(metadata.get("volume_id_hash")):
        errors.append("encryption_at_rest.volume_id_hash must be a lowercase SHA-256 hex digest")
    key_owner = metadata.get("key_owner")
    if not isinstance(key_owner, str) or not key_owner.strip():
        errors.append("encryption_at_rest.key_owner must be a non-empty operator label")
    return errors


def manifest_errors(manifest: dict[str, Any], *, require_encryption_at_rest: bool = False, active: bool = True) -> list[str]:
    errors = [f"missing {field}" for field in MANIFEST_REQUIRED_FIELDS if manifest.get(field) in (None, "")]
    errors.extend(f"missing strict field {field}" for field in STRICT_COMPATIBILITY_FIELDS if manifest.get(field) in (None, ""))
    digest = str(manifest.get("slot_file_sha256", ""))
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        errors.append("slot_file_sha256 must be a lowercase SHA-256 hex digest")
    try:
        size = int(manifest.get("slot_file_size_bytes"))
    except (TypeError, ValueError):
        errors.append("slot_file_size_bytes must be a positive integer")
    else:
        if size <= 0:
            errors.append("slot_file_size_bytes must be positive")
    if not isinstance(manifest.get("worker_residency"), dict):
        errors.append("worker_residency must be an object")
    errors.extend(encryption_at_rest_errors(manifest, require_encryption_at_rest=require_encryption_at_rest, active=active))
    return errors


def public_encryption_metadata(manifest: dict[str, Any]) -> dict[str, Any] | None:
    metadata = manifest.get("encryption_at_rest")
    if not isinstance(metadata, dict):
        return None
    return {
        "required": metadata.get("required"),
        "mode": metadata.get("mode"),
        "evidence_basis": metadata.get("evidence_basis"),
        "volume_id_hash": metadata.get("volume_id_hash") if is_sha256_hex(metadata.get("volume_id_hash")) else None,
        "key_owner": str(metadata.get("key_owner"))[:80] if metadata.get("key_owner") is not None else None,
    }


def audit_manifest(cache_root: Path, manifest_path: Path, *, require_encryption_at_rest: bool = False) -> dict[str, Any]:
    manifest, load_error = load_manifest(manifest_path)
    relative_manifest = relative_to_root(manifest_path, cache_root)
    if load_error:
        return {
            "ok": False,
            "manifest_path": relative_manifest,
            "errors": [load_error],
            "warnings": [],
    }
    assert manifest is not None
    warnings: list[str] = []
    digest = str(manifest.get("slot_file_sha256", ""))
    validation_status = str(manifest.get("validation_status") or "validated")
    deleted_manifest = validation_status in DELETED_MANIFEST_STATUSES
    errors = manifest_errors(manifest, require_encryption_at_rest=require_encryption_at_rest, active=not deleted_manifest)
    blob_path = content_addressed_blob_path(cache_root, digest) if len(digest) >= 2 else store_paths(cache_root)["blobs"] / "<invalid>"
    manifest_blob_path = manifest.get("router_blob_path")
    if manifest_blob_path:
        try:
            recorded = Path(str(manifest_blob_path))
            if not recorded.is_absolute():
                recorded = (cache_root / recorded).resolve()
            if recorded.resolve() != blob_path.resolve():
                warnings.append("router_blob_path differs from content-addressed path; audit used content address")
        except OSError:
            warnings.append("router_blob_path could not be resolved; audit used content address")
    if not blob_path.is_file():
        if deleted_manifest:
            warnings.append("deleted manifest blob already absent after GC")
        else:
            errors.append("content-addressed blob missing")
    else:
        actual_size = blob_path.stat().st_size
        actual_hash = sha256_file(blob_path)
        if str(actual_hash) != digest:
            errors.append("content-addressed blob hash mismatch")
        try:
            expected_size = int(manifest.get("slot_file_size_bytes"))
        except (TypeError, ValueError):
            expected_size = None
        if expected_size is not None and actual_size != expected_size:
            errors.append("content-addressed blob size mismatch")
    return {
        "ok": not errors,
        "manifest_path": relative_manifest,
        "cache_id": manifest.get("cache_id"),
        "cache_key_hash": manifest.get("cache_key_hash"),
        "manifest_id": manifest.get("manifest_id"),
        "blob_path": relative_to_root(blob_path, cache_root),
        "slot_file_sha256": digest,
        "slot_file_size_bytes": manifest.get("slot_file_size_bytes"),
        "validation_status": validation_status,
        "active": not deleted_manifest,
        "encryption_at_rest": public_encryption_metadata(manifest),
        "errors": errors,
        "warnings": warnings,
    }


def registry_entry_from_manifest(cache_root: Path, manifest: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    digest = str(manifest["slot_file_sha256"])
    entry = {
        "cache_id": manifest["cache_id"],
        "cache_key_hash": manifest["cache_key_hash"],
        "manifest_id": manifest["manifest_id"],
        "manifest_path": str(manifest_path),
        "router_blob_path": str(content_addressed_blob_path(cache_root, digest)),
        "slot_filename": manifest["slot_filename"],
        "slot_file_sha256": digest,
        "slot_file_size_bytes": int(manifest["slot_file_size_bytes"]),
        "strict_key_fields": {field: manifest[field] for field in STRICT_COMPATIBILITY_FIELDS},
        "created_at": manifest.get("created_at"),
        "last_used_at": manifest.get("last_used_at"),
        "source_worker_id": manifest.get("source_worker_id"),
        "validation_status": manifest.get("validation_status", "validated"),
        "encryption_at_rest": public_encryption_metadata(manifest),
        "worker_residency": manifest.get("worker_residency", {}),
    }
    for field in ["scope", "tenant_hash", "conversation_hash", "policy_id_hash"]:
        if field in manifest:
            entry[field] = manifest.get(field)
    if manifest.get("quarantine_reason") is not None:
        entry["quarantine_reason"] = manifest.get("quarantine_reason")
    if manifest.get("quarantined_at") is not None:
        entry["quarantined_at"] = manifest.get("quarantined_at")
    if manifest.get("deleted_at") is not None:
        entry["deleted_at"] = manifest.get("deleted_at")
    if manifest.get("deletion_reason") is not None:
        entry["deletion_reason"] = manifest.get("deletion_reason")
    return entry


def rebuild_registry(cache_root: Path, manifest_paths: list[Path] | None = None, *, require_encryption_at_rest: bool = False) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    entries: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    paths = manifest_paths if manifest_paths is not None else iter_manifest_paths(cache_root)
    for manifest_path in paths:
        manifest, load_error = load_manifest(manifest_path)
        if load_error or manifest is None:
            rejected.append({"manifest_path": relative_to_root(manifest_path, cache_root), "errors": [load_error or "invalid manifest"]})
            continue
        audit = audit_manifest(cache_root, manifest_path, require_encryption_at_rest=require_encryption_at_rest)
        if not audit["ok"]:
            rejected.append({"manifest_path": audit["manifest_path"], "errors": audit["errors"]})
            continue
        entries.append(registry_entry_from_manifest(cache_root, manifest, manifest_path))
    entries.sort(key=lambda row: (str(row.get("cache_id", "")), str(row.get("cache_key_hash", ""))))
    return {"schema_version": SCHEMA_VERSION, "entries": entries}, rejected


def load_registry_or_rebuild(cache_root: Path) -> dict[str, Any]:
    registry_path = store_paths(cache_root)["registry"]
    if registry_path.is_file():
        try:
            registry = json.loads(registry_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            registry = {}
        if isinstance(registry, dict) and isinstance(registry.get("entries"), list):
            return registry
    registry, _ = rebuild_registry(cache_root)
    return registry


def mark_manifest_tenant_deleted(manifest: dict[str, Any], *, deleted_at: str) -> dict[str, Any]:
    updated = json.loads(json.dumps(manifest))
    worker_residency = updated.get("worker_residency") if isinstance(updated.get("worker_residency"), dict) else {}
    updated["previous_validation_status"] = updated.get("validation_status", "validated")
    updated["validation_status"] = "tenant_deleted"
    updated["deleted_at"] = deleted_at
    updated["deletion_reason"] = "tenant_delete"
    updated["worker_residency"] = {str(worker_id): False for worker_id in sorted(worker_residency)}
    return updated


def tenant_delete(cache_root: Path, tenant_hash: str, *, apply: bool = False) -> dict[str, Any]:
    if not is_sha256_hex(tenant_hash):
        return {
            "ok": False,
            "applied": False,
            "errors": ["tenant_hash must be a lowercase SHA-256 hex digest"],
        }
    paths = store_paths(cache_root)
    registry = load_registry_or_rebuild(cache_root)
    entries = [row for row in registry.get("entries", []) if isinstance(row, dict)]
    removed = [row for row in entries if row.get("tenant_hash") == tenant_hash]
    kept = [row for row in entries if row.get("tenant_hash") != tenant_hash]
    deleted_at = now_iso()
    manifest_updates: list[tuple[Path, dict[str, Any]]] = []
    gc_records: list[dict[str, Any]] = []
    errors: list[str] = []

    for entry in removed:
        manifest_path = resolve_store_path(cache_root, entry.get("manifest_path"))
        manifest: dict[str, Any] | None = None
        if manifest_path is not None and manifest_path.is_file():
            manifest, load_error = load_manifest(manifest_path)
            if load_error or manifest is None:
                errors.append(f"{relative_to_root(manifest_path, cache_root)}: {load_error or 'invalid manifest'}")
            else:
                manifest_updates.append((manifest_path, mark_manifest_tenant_deleted(manifest, deleted_at=deleted_at)))

        digest = str((manifest or entry).get("slot_file_sha256") or "")
        blob_path = content_addressed_blob_path(cache_root, digest) if is_sha256_hex(digest) else resolve_store_path(cache_root, entry.get("router_blob_path"))
        gc_records.append(
            {
                "schema_version": SCHEMA_VERSION,
                "action": "delete_unreferenced_blob_after_tenant_delete",
                "status": "scheduled",
                "scheduled_at": deleted_at,
                "tenant_hash": tenant_hash,
                "cache_id": entry.get("cache_id"),
                "cache_key_hash": entry.get("cache_key_hash"),
                "manifest_id": entry.get("manifest_id"),
                "manifest_path": relative_to_root(manifest_path, cache_root) if manifest_path is not None else None,
                "slot_file_sha256": digest if is_sha256_hex(digest) else None,
                "router_blob_path": relative_to_root(blob_path, cache_root) if blob_path is not None else None,
            }
        )

    if apply and not errors:
        for manifest_path, manifest in manifest_updates:
            write_json(manifest_path, manifest)
        registry["entries"] = kept
        write_json(paths["registry"], registry)
        for row in gc_records:
            append_jsonl(paths["router_store"] / "gc-queue.jsonl", row)
            append_registry_audit_event(
                cache_root,
                operation="tenant_delete_gc_schedule",
                outcome="scheduled",
                audit_actions=["commit"],
                cache_id=row.get("cache_id"),
                cache_key_hash=row.get("cache_key_hash"),
                manifest_id=row.get("manifest_id"),
                tenant_hash=tenant_hash,
                artifact_sha256=row.get("slot_file_sha256"),
                reason="tenant_delete",
            )
        for entry in removed:
            append_registry_audit_event(
                cache_root,
                operation="tenant_delete_registry_remove",
                outcome="applied",
                audit_actions=["commit"],
                cache_id=entry.get("cache_id"),
                cache_key_hash=entry.get("cache_key_hash"),
                manifest_id=entry.get("manifest_id"),
                tenant_hash=tenant_hash,
                conversation_hash=entry.get("conversation_hash"),
                scope=entry.get("scope"),
                artifact_sha256=entry.get("slot_file_sha256"),
                reason="tenant_delete",
            )

    return {
        "ok": not errors,
        "applied": bool(apply and not errors),
        "tenant_hash": tenant_hash,
        "removed_registry_entries": len(removed),
        "remaining_registry_entries": len(kept),
        "manifests_marked_tenant_deleted": len(manifest_updates),
        "gc_records_scheduled": len(gc_records),
        "gc_queue": "router-store/gc-queue.jsonl",
        "errors": errors,
    }


def active_blob_references(cache_root: Path, *, require_encryption_at_rest: bool = False) -> tuple[set[Path], list[str]]:
    references: set[Path] = set()
    errors: list[str] = []
    for manifest_path in iter_manifest_paths(cache_root):
        manifest, load_error = load_manifest(manifest_path)
        if load_error or manifest is None:
            errors.append(f"{relative_to_root(manifest_path, cache_root)}: {load_error or 'invalid manifest'}")
            continue
        validation_status = str(manifest.get("validation_status") or "validated")
        deleted_manifest = validation_status in DELETED_MANIFEST_STATUSES
        manifest_validation_errors = manifest_errors(manifest, require_encryption_at_rest=require_encryption_at_rest, active=not deleted_manifest)
        if manifest_validation_errors:
            errors.append(f"{relative_to_root(manifest_path, cache_root)}: {', '.join(manifest_validation_errors)}")
            continue
        if str(manifest.get("validation_status") or "validated") in DELETED_MANIFEST_STATUSES:
            continue
        digest = str(manifest.get("slot_file_sha256") or "")
        if is_sha256_hex(digest):
            references.add(content_addressed_blob_path(cache_root, digest).resolve())
        else:
            errors.append(f"{relative_to_root(manifest_path, cache_root)}: slot_file_sha256 must be a lowercase SHA-256 hex digest")
    return references, errors


def iter_blob_paths(cache_root: Path) -> list[Path]:
    blobs = store_paths(cache_root)["blobs"]
    if not blobs.exists():
        return []
    return sorted(path for path in blobs.glob("*/*.slot") if path.is_file())


def gc_unreferenced_blobs(cache_root: Path, *, apply: bool = False, require_encryption_at_rest: bool = False) -> dict[str, Any]:
    references, reference_errors = active_blob_references(cache_root, require_encryption_at_rest=require_encryption_at_rest)
    if reference_errors:
        return {
            "ok": False,
            "applied": False,
            "active_blob_references": len(references),
            "unreferenced_blob_candidates": 0,
            "deleted_blobs": [],
            "candidate_blobs": [],
            "errors": ["destructive GC refused because active manifest references could not be proven"] + reference_errors,
        }
    candidates = [path for path in iter_blob_paths(cache_root) if path.resolve() not in references]
    deleted: list[str] = []
    errors: list[str] = []
    if apply:
        for path in candidates:
            try:
                path.unlink()
                deleted.append(relative_to_root(path, cache_root))
                append_registry_audit_event(
                    cache_root,
                    operation="blob_gc_delete",
                    outcome="deleted",
                    audit_actions=["commit"],
                    artifact_sha256=path.stem,
                    reason="unreferenced_blob",
                )
                try:
                    path.parent.rmdir()
                except OSError:
                    pass
            except OSError as exc:
                errors.append(f"{relative_to_root(path, cache_root)}: {exc}")
    return {
        "ok": not errors,
        "applied": bool(apply and not errors),
        "active_blob_references": len(references),
        "unreferenced_blob_candidates": len(candidates),
        "deleted_blobs": deleted,
        "candidate_blobs": [relative_to_root(path, cache_root) for path in candidates],
        "errors": errors,
    }


def public_registry_preview(cache_root: Path, registry: dict[str, Any]) -> dict[str, Any]:
    preview = json.loads(json.dumps(registry))
    for entry in preview.get("entries", []):
        if isinstance(entry, dict):
            if entry.get("manifest_path"):
                entry["manifest_path"] = relative_to_root(Path(str(entry["manifest_path"])), cache_root)
            if entry.get("router_blob_path"):
                entry["router_blob_path"] = relative_to_root(Path(str(entry["router_blob_path"])), cache_root)
    return preview


def audit_store(cache_root: Path, *, require_encryption_at_rest: bool = False) -> dict[str, Any]:
    paths = iter_manifest_paths(cache_root)
    manifest_reports = [audit_manifest(cache_root, path, require_encryption_at_rest=require_encryption_at_rest) for path in paths]
    registry, rejected = rebuild_registry(cache_root, paths, require_encryption_at_rest=require_encryption_at_rest)
    errors = [
        {"manifest_path": report["manifest_path"], "errors": report["errors"]}
        for report in manifest_reports
        if not report["ok"]
    ]
    warnings = [
        {"manifest_path": report["manifest_path"], "warnings": report["warnings"]}
        for report in manifest_reports
        if report["warnings"]
    ]
    return {
        "ok": not errors and not rejected,
        "cache_root": "<cache-root>",
        "require_encryption_at_rest": require_encryption_at_rest,
        "manifest_count": len(manifest_reports),
        "valid_manifest_count": sum(1 for report in manifest_reports if report["ok"]),
        "rebuilt_registry_entries": len(registry["entries"]),
        "errors": errors,
        "warnings": warnings,
        "manifests": manifest_reports,
    }


def make_manifest(
    cache_root: Path,
    *,
    cache_id: str,
    payload: bytes,
    worker_id: str = "worker-a",
    scope: str = "tenant",
    tenant_hash: str | None = None,
    conversation_hash: str | None = None,
    policy_id_hash: str | None = None,
) -> dict[str, Any]:
    digest = hashlib.sha256(payload).hexdigest()
    blob = content_addressed_blob_path(cache_root, digest)
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(payload)
    tenant_hash = tenant_hash or hashlib.sha256(b"store-audit-default-tenant").hexdigest()
    conversation_hash = conversation_hash or hashlib.sha256((cache_id + "-conversation").encode("utf-8")).hexdigest()
    policy_id_hash = policy_id_hash or hashlib.sha256(b"store-audit-default-policy").hexdigest()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "cache_id": cache_id,
        "cache_key_hash": hashlib.sha256(cache_id.encode("utf-8")).hexdigest(),
        "manifest_id": "manifest-" + cache_id,
        "source_worker_id": worker_id,
        "scope": scope,
        "tenant_hash": tenant_hash,
        "conversation_hash": conversation_hash,
        "policy_id_hash": policy_id_hash,
        "prefix_sha256": hashlib.sha256((cache_id + "-prefix").encode("utf-8")).hexdigest(),
        "prefix_token_count": 16,
        "model": "synthetic-model",
        "model_identity": "synthetic-model-identity",
        "model_path": "/models/<main-model>.gguf",
        "model_file_size": 1,
        "model_architecture": "synthetic-architecture",
        "model_hash": "a" * 64,
        "gguf_tensor_manifest_hash": "b" * 64,
        "tokenizer_hash": "c" * 64,
        "chat_template_effective_hash": "d" * 64,
        "tools_schema_hash": "e" * 64,
        "system_prompt_hash": "f" * 64,
        "special_token_policy": "synthetic-special-tokens-v1",
        "llama_server_path": "/home/<user>/llama.cpp/build/bin/llama-server",
        "llama_server_version": "synthetic-llama-server",
        "llama_cpp_source_commit": "synthetic9828",
        "llama_cpp_cache_abi_version": "cache-abi-synthetic",
        "patchset_id": "none",
        "build_backend": "vulkan_radv",
        "gpu_backend_driver": "synthetic-radv",
        "kv_unified_mode": True,
        "ctx_size": 65536,
        "ctx_checkpoints_config": "ctx-checkpoints-0",
        "cache_type_k": "q8_0",
        "cache_type_v": "q8_0",
        "flash_attention_mode": "on",
        "rope_freq_base": "default",
        "rope_freq_scale": "default",
        "yarn_or_rope_scaling_metadata": "none",
        "reasoning_format": "synthetic",
        "jinja_template_mode": "enabled",
        "mtp_enabled": False,
        "spec_draft_model_identity": "none",
        "spec_draft_model_path": "none",
        "spec_draft_model_size": 0,
        "spec_draft_model_hash": "none",
        "spec_draft_config": "none",
        "n_parallel": 1,
        "n_seq_max": 1,
        "slot_file_sha256": digest,
        "slot_file_size_bytes": len(payload),
        "slot_filename": f"{cache_id}.slot",
        "router_blob_path": str(blob),
        "worker_slot_path": "/home/<user>/.cache/cachy-router/workers/worker-a/slots/<slot>",
        "worker_transport": {"kind": "local"},
        "created_at": "2026-07-01T00:00:00Z",
        "last_used_at": None,
        "validation_status": "validated",
        "encryption_at_rest": {
            "required": True,
            "mode": "operator_managed_encrypted_filesystem",
            "evidence_basis": "operator_attestation",
            "volume_id_hash": hashlib.sha256(b"store-audit-encrypted-volume").hexdigest(),
            "key_owner": "operator",
        },
        "worker_residency": {worker_id: True},
    }
    manifest_path = store_paths(cache_root)["manifests"] / f"{manifest['cache_key_hash']}.json"
    write_json(manifest_path, manifest)
    return manifest


def self_test() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-store-audit-") as tmp:
        root = Path(tmp)
        cache_root = root / "valid-cache"
        first = make_manifest(cache_root, cache_id="alpha", payload=b"alpha-slot-payload")
        second = make_manifest(cache_root, cache_id="beta", payload=b"beta-slot-payload")
        audit_ok = audit_store(cache_root)
        encryption_required_audit = audit_store(cache_root, require_encryption_at_rest=True)
        registry, rejected = rebuild_registry(cache_root)
        encrypted_registry, encrypted_rejected = rebuild_registry(cache_root, require_encryption_at_rest=True)
        registry_preview = public_registry_preview(cache_root, registry)
        preview_text = json.dumps(registry_preview)
        preview_redacted = str(cache_root) not in preview_text and all(
            not str(entry.get("manifest_path", "")).startswith("/") and not str(entry.get("router_blob_path", "")).startswith("/")
            for entry in registry_preview.get("entries", [])
            if isinstance(entry, dict)
        )
        write_json(store_paths(cache_root)["registry"], registry)
        registry_written = store_paths(cache_root)["registry"].is_file()
        registry_entries_before_leases = len(json.loads(store_paths(cache_root)["registry"].read_text(encoding="utf-8")).get("entries", []))
        lease_first = acquire_registry_lease(
            cache_root,
            operation="restore_hydrate",
            cache_id=str(first["cache_id"]),
            cache_key_hash=str(first["cache_key_hash"]),
            manifest_id=str(first["manifest_id"]),
            owner_id="store-audit-owner-a",
            ttl_seconds=60.0,
        )
        lease_conflict_blocked = False
        try:
            acquire_registry_lease(
                cache_root,
                operation="restore_hydrate",
                cache_id=str(first["cache_id"]),
                cache_key_hash=str(first["cache_key_hash"]),
                manifest_id=str(first["manifest_id"]),
                owner_id="store-audit-owner-b",
                ttl_seconds=60.0,
            )
        except RuntimeError as exc:
            lease_conflict_blocked = "registry lease conflict" in str(exc)
        lease_release_removed = release_registry_lease(cache_root, lease_first)
        lease_second = acquire_registry_lease(
            cache_root,
            operation="build_upload",
            cache_id=str(first["cache_id"]),
            cache_key_hash=str(first["cache_key_hash"]),
            manifest_id=str(first["manifest_id"]),
            owner_id="store-audit-owner-b",
            ttl_seconds=60.0,
        )
        lease_after_release_acquired = bool(lease_second.get("lease_id"))
        release_registry_lease(cache_root, lease_second)
        expired_lease = acquire_registry_lease(
            cache_root,
            operation="restore_hydrate",
            cache_id=str(second["cache_id"]),
            cache_key_hash=str(second["cache_key_hash"]),
            manifest_id=str(second["manifest_id"]),
            owner_id="store-audit-expired-owner",
            ttl_seconds=0.001,
        )
        active_lease = acquire_registry_lease(
            cache_root,
            operation="build_upload",
            cache_id=str(first["cache_id"]),
            cache_key_hash=str(first["cache_key_hash"]),
            manifest_id=str(first["manifest_id"]),
            owner_id="store-audit-active-owner",
            ttl_seconds=60.0,
        )
        time.sleep(0.01)
        expired_pruned = prune_registry_leases(cache_root)
        lease_rows_after_prune = load_registry_leases(cache_root).get("leases", [])
        lease_ids_after_prune = {row.get("lease_id") for row in lease_rows_after_prune if isinstance(row, dict)}
        registry_entries_after_leases = len(json.loads(store_paths(cache_root)["registry"].read_text(encoding="utf-8")).get("entries", []))
        active_lease_survived_prune = active_lease.get("lease_id") in lease_ids_after_prune
        expired_lease_removed = expired_lease.get("lease_id") not in lease_ids_after_prune
        release_registry_lease(cache_root, active_lease)
        lease_cleanup_empty = len(load_registry_leases(cache_root).get("leases", [])) == 0

        tenant_a = hashlib.sha256(b"tenant-a").hexdigest()
        tenant_b = hashlib.sha256(b"tenant-b").hexdigest()
        tenant_root = root / "tenant-delete-cache"
        tenant_a_only = make_manifest(tenant_root, cache_id="tenant-a-only", payload=b"tenant-a-only-slot", tenant_hash=tenant_a)
        tenant_a_shared = make_manifest(tenant_root, cache_id="tenant-a-shared", payload=b"shared-slot", tenant_hash=tenant_a)
        tenant_b_shared = make_manifest(tenant_root, cache_id="tenant-b-shared", payload=b"shared-slot", tenant_hash=tenant_b)
        tenant_registry, tenant_rejected = rebuild_registry(tenant_root)
        write_json(store_paths(tenant_root)["registry"], tenant_registry)
        tenant_delete_preview = tenant_delete(tenant_root, tenant_a, apply=False)
        tenant_audit_before_apply = not store_paths(tenant_root)["registry_audit"].exists()
        tenant_delete_result = tenant_delete(tenant_root, tenant_a, apply=True)
        tenant_registry_after = json.loads(store_paths(tenant_root)["registry"].read_text(encoding="utf-8"))
        tenant_queue_path = store_paths(tenant_root)["router_store"] / "gc-queue.jsonl"
        tenant_queue_rows = [
            json.loads(line)
            for line in tenant_queue_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        tenant_audit_path = store_paths(tenant_root)["registry_audit"]
        tenant_audit_text_after_delete = tenant_audit_path.read_text(encoding="utf-8")
        tenant_a_only_blob = content_addressed_blob_path(tenant_root, tenant_a_only["slot_file_sha256"])
        shared_blob = content_addressed_blob_path(tenant_root, tenant_a_shared["slot_file_sha256"])
        tenant_delete_manifest_path = store_paths(tenant_root)["manifests"] / f"{tenant_a_only['cache_key_hash']}.json"
        tenant_deleted_manifest = json.loads(tenant_delete_manifest_path.read_text(encoding="utf-8"))
        gc_preview = gc_unreferenced_blobs(tenant_root, apply=False)
        gc_result = gc_unreferenced_blobs(tenant_root, apply=True)
        tenant_gc_audit = audit_store(tenant_root)
        tenant_audit_text_after_gc = tenant_audit_path.read_text(encoding="utf-8")
        tenant_audit_rows = [
            json.loads(line)
            for line in tenant_audit_text_after_gc.splitlines()
            if line.strip()
        ]
        tenant_audit_operations = {row.get("operation") for row in tenant_audit_rows}
        tenant_audit_redacted = str(tenant_root) not in json.dumps(tenant_audit_rows)
        tenant_audit_has_ids = all(
            str(row.get("event_id", "")).startswith("registry-audit-") and str(row.get("operation_id", "")).startswith("op-")
            for row in tenant_audit_rows
        )

        bad_gc_root = root / "bad-gc-cache"
        make_manifest(bad_gc_root, cache_id="bad-gc-active", payload=b"bad-gc-active-slot")
        bad_gc_orphan = content_addressed_blob_path(bad_gc_root, hashlib.sha256(b"bad-gc-orphan-slot").hexdigest())
        bad_gc_orphan.parent.mkdir(parents=True, exist_ok=True)
        bad_gc_orphan.write_bytes(b"bad-gc-orphan-slot")
        bad_gc_manifests = store_paths(bad_gc_root)["manifests"]
        (bad_gc_manifests / "bad-json.json").write_text("{bad json", encoding="utf-8")
        bad_gc_result = gc_unreferenced_blobs(bad_gc_root, apply=True)

        empty_audit = audit_store(root / "empty-cache")

        path_warning_root = root / "path-warning-cache"
        path_manifest = make_manifest(path_warning_root, cache_id="path-warning", payload=b"path-warning-slot-payload")
        path_manifest_path = store_paths(path_warning_root)["manifests"] / f"{path_manifest['cache_key_hash']}.json"
        path_manifest["router_blob_path"] = "/outside-cache-root/not-read.slot"
        write_json(path_manifest_path, path_manifest)
        path_warning_audit = audit_store(path_warning_root)

        quarantine_root = root / "quarantine-cache"
        quarantine_manifest = make_manifest(quarantine_root, cache_id="quarantined", payload=b"quarantined-slot-payload")
        quarantine_manifest_path = store_paths(quarantine_root)["manifests"] / f"{quarantine_manifest['cache_key_hash']}.json"
        quarantine_manifest["validation_status"] = "quarantined"
        quarantine_manifest["quarantine_reason"] = "corrupt_blob"
        quarantine_manifest["quarantined_at"] = "2026-07-01T00:00:01Z"
        quarantine_manifest["worker_residency"] = {"worker-a": False}
        write_json(quarantine_manifest_path, quarantine_manifest)
        audit_quarantine = audit_store(quarantine_root)
        quarantine_registry, quarantine_rejected = rebuild_registry(quarantine_root)
        quarantine_entry = quarantine_registry["entries"][0] if quarantine_registry["entries"] else {}

        corrupt_blob = content_addressed_blob_path(cache_root, first["slot_file_sha256"])
        corrupt_blob.write_bytes(b"corrupt")
        audit_corrupt = audit_store(cache_root)
        corrupt_blob.write_bytes(b"alpha-slot-payload")
        missing_blob = content_addressed_blob_path(cache_root, second["slot_file_sha256"])
        missing_blob.unlink()
        audit_missing = audit_store(cache_root)

        size_root = root / "size-mismatch-cache"
        size_manifest = make_manifest(size_root, cache_id="size-mismatch", payload=b"size-mismatch-slot-payload")
        size_manifest_path = store_paths(size_root)["manifests"] / f"{size_manifest['cache_key_hash']}.json"
        size_manifest["slot_file_size_bytes"] = int(size_manifest["slot_file_size_bytes"]) + 1
        write_json(size_manifest_path, size_manifest)
        audit_size_mismatch = audit_store(size_root)

        invalid_root = root / "invalid-cache"
        invalid_manifests = store_paths(invalid_root)["manifests"]
        invalid_manifests.mkdir(parents=True, exist_ok=True)
        (invalid_manifests / "bad-json.json").write_text("{bad json", encoding="utf-8")
        audit_bad_json = audit_store(invalid_root)

        field_root = root / "field-cache"
        field_manifest = make_manifest(field_root, cache_id="field-missing", payload=b"field-missing-slot-payload")
        field_manifest_path = store_paths(field_root)["manifests"] / f"{field_manifest['cache_key_hash']}.json"
        field_manifest.pop("tokenizer_hash", None)
        write_json(field_manifest_path, field_manifest)
        audit_field_missing = audit_store(field_root)

        missing_encryption_root = root / "missing-encryption-cache"
        missing_encryption_manifest = make_manifest(missing_encryption_root, cache_id="missing-encryption", payload=b"missing-encryption-slot")
        missing_encryption_manifest_path = store_paths(missing_encryption_root)["manifests"] / f"{missing_encryption_manifest['cache_key_hash']}.json"
        missing_encryption_manifest.pop("encryption_at_rest", None)
        write_json(missing_encryption_manifest_path, missing_encryption_manifest)
        audit_missing_encryption_required = audit_store(missing_encryption_root, require_encryption_at_rest=True)

        plaintext_encryption_root = root / "plaintext-encryption-cache"
        plaintext_encryption_manifest = make_manifest(plaintext_encryption_root, cache_id="plaintext-encryption", payload=b"plaintext-encryption-slot")
        plaintext_encryption_manifest_path = store_paths(plaintext_encryption_root)["manifests"] / f"{plaintext_encryption_manifest['cache_key_hash']}.json"
        plaintext_encryption_manifest["encryption_at_rest"] = {
            "required": True,
            "mode": "plaintext-dev",
            "evidence_basis": "operator_attestation",
            "volume_id_hash": "not-a-sha",
            "key_owner": "operator",
        }
        write_json(plaintext_encryption_manifest_path, plaintext_encryption_manifest)
        audit_plaintext_encryption_required = audit_store(plaintext_encryption_root, require_encryption_at_rest=True)

        return {
            "ok": (
                audit_ok["ok"]
                and encryption_required_audit["ok"]
                and len(registry["entries"]) == 2
                and len(encrypted_registry["entries"]) == 2
                and not rejected
                and not encrypted_rejected
                and registry_preview["entries"]
                and preview_redacted
                and registry_written
                and lease_conflict_blocked
                and lease_release_removed
                and lease_after_release_acquired
                and expired_pruned == 1
                and expired_lease_removed
                and active_lease_survived_prune
                and lease_cleanup_empty
                and registry_entries_before_leases == registry_entries_after_leases == 2
                and not tenant_rejected
                and tenant_delete_preview["ok"]
                and not tenant_delete_preview["applied"]
                and tenant_audit_before_apply
                and tenant_delete_result["ok"]
                and tenant_delete_result["applied"]
                and tenant_delete_result["removed_registry_entries"] == 2
                and len(tenant_registry_after.get("entries", [])) == 1
                and tenant_registry_after["entries"][0].get("tenant_hash") == tenant_b
                and len(tenant_queue_rows) == 2
                and tenant_deleted_manifest.get("validation_status") == "tenant_deleted"
                and tenant_deleted_manifest.get("worker_residency", {}).get("worker-a") is False
                and gc_preview["unreferenced_blob_candidates"] == 1
                and gc_result["ok"]
                and not tenant_a_only_blob.exists()
                and shared_blob.exists()
                and tenant_gc_audit["ok"]
                and len(tenant_audit_rows) == 5
                and tenant_audit_text_after_gc.startswith(tenant_audit_text_after_delete)
                and tenant_audit_operations == {"tenant_delete_gc_schedule", "tenant_delete_registry_remove", "blob_gc_delete"}
                and all("commit" in row.get("audit_actions", []) for row in tenant_audit_rows)
                and tenant_audit_has_ids
                and tenant_audit_redacted
                and not bad_gc_result["ok"]
                and bad_gc_orphan.exists()
                and empty_audit["ok"]
                and path_warning_audit["ok"]
                and bool(path_warning_audit["warnings"])
                and audit_quarantine["ok"]
                and not quarantine_rejected
                and quarantine_entry.get("validation_status") == "quarantined"
                and quarantine_entry.get("quarantine_reason") == "corrupt_blob"
                and quarantine_entry.get("worker_residency", {}).get("worker-a") is False
                and not audit_corrupt["ok"]
                and not audit_missing["ok"]
                and not audit_size_mismatch["ok"]
                and not audit_bad_json["ok"]
                and not audit_field_missing["ok"]
                and not audit_missing_encryption_required["ok"]
                and not audit_plaintext_encryption_required["ok"]
            ),
            "valid_audit": {
                "ok": audit_ok["ok"],
                "manifest_count": audit_ok["manifest_count"],
                "rebuilt_registry_entries": audit_ok["rebuilt_registry_entries"],
            },
            "encryption_at_rest": {
                "required_audit_ok": encryption_required_audit["ok"],
                "rebuilt_registry_entries": len(encrypted_registry["entries"]),
                "missing_metadata_rejected": not audit_missing_encryption_required["ok"],
                "plaintext_mode_rejected": not audit_plaintext_encryption_required["ok"],
                "mode": "operator_managed_encrypted_filesystem",
            },
            "rebuild": {
                "entries": len(registry["entries"]),
                "rejected": len(rejected),
            },
            "registry_preview_redacted": preview_redacted,
            "registry_written": registry_written,
            "registry_leases": {
                "conflict_blocked": lease_conflict_blocked,
                "release_removed": lease_release_removed,
                "after_release_acquired": lease_after_release_acquired,
                "expired_pruned": expired_pruned,
                "expired_removed": expired_lease_removed,
                "active_survived_prune": active_lease_survived_prune,
                "cleanup_empty": lease_cleanup_empty,
                "registry_entries_unchanged": registry_entries_before_leases == registry_entries_after_leases,
            },
            "tenant_delete": {
                "dry_run_ok": tenant_delete_preview["ok"] and not tenant_delete_preview["applied"],
                "removed_registry_entries": tenant_delete_result["removed_registry_entries"],
                "remaining_registry_entries": len(tenant_registry_after.get("entries", [])),
                "gc_records_scheduled": len(tenant_queue_rows),
                "deleted_manifest_marked": tenant_deleted_manifest.get("validation_status") == "tenant_deleted",
            },
            "registry_audit": {
                "dry_run_wrote_no_rows": tenant_audit_before_apply,
                "rows": len(tenant_audit_rows),
                "append_only": tenant_audit_text_after_gc.startswith(tenant_audit_text_after_delete),
                "has_ids": tenant_audit_has_ids,
                "operations": sorted(str(operation) for operation in tenant_audit_operations),
                "redacted": tenant_audit_redacted,
            },
            "blob_gc": {
                "unreferenced_candidates": gc_preview["unreferenced_blob_candidates"],
                "deleted_unreferenced_blob": not tenant_a_only_blob.exists(),
                "preserved_active_shared_blob": shared_blob.exists(),
                "post_gc_audit_ok": tenant_gc_audit["ok"],
                "bad_manifest_blocks_destructive_gc": not bad_gc_result["ok"] and bad_gc_orphan.exists(),
            },
            "empty_store_ok": empty_audit["ok"],
            "path_warning_ok": path_warning_audit["ok"] and bool(path_warning_audit["warnings"]),
            "quarantine_metadata_preserved": quarantine_entry.get("validation_status") == "quarantined"
            and quarantine_entry.get("quarantine_reason") == "corrupt_blob",
            "corrupt_blob_rejected": not audit_corrupt["ok"],
            "missing_blob_rejected": not audit_missing["ok"],
            "size_mismatch_rejected": not audit_size_mismatch["ok"],
            "bad_json_rejected": not audit_bad_json["ok"],
            "missing_strict_field_rejected": not audit_field_missing["ok"],
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", default=".cache/cachy-router", help="Local Cachy Router cache root to audit.")
    parser.add_argument("--rebuild-registry", action="store_true", help="Emit a registry rebuilt from valid manifests.")
    parser.add_argument("--write-registry", action="store_true", help="Write the rebuilt registry to router-store/registry.json.")
    parser.add_argument("--delete-tenant", metavar="TENANT_HASH", help="Remove one tenant hash from the registry and schedule its blobs for GC.")
    parser.add_argument("--gc-unreferenced-blobs", action="store_true", help="Find or delete content-addressed blobs not referenced by active manifests.")
    parser.add_argument("--require-encryption-at-rest", action="store_true", help="Reject active manifests unless they carry valid operator-managed encryption-at-rest metadata.")
    parser.add_argument("--apply", action="store_true", help="Apply --delete-tenant or --gc-unreferenced-blobs mutations. Without this flag they are dry-runs.")
    parser.add_argument("--self-test", action="store_true", help="Run an offline temp-dir self-test.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        result = self_test()
        print(json_dumps(result) if args.json else f"cache-router store audit self-test: {'ok' if result['ok'] else 'fail'}")
        return 0 if result["ok"] else 1

    cache_root = Path(args.cache_root)
    if args.delete_tenant or args.gc_unreferenced_blobs:
        operations: dict[str, Any] = {}
        ok = True
        if args.delete_tenant:
            operations["tenant_delete"] = tenant_delete(cache_root, str(args.delete_tenant), apply=bool(args.apply))
            ok = ok and bool(operations["tenant_delete"].get("ok"))
        if args.gc_unreferenced_blobs:
            operations["blob_gc"] = gc_unreferenced_blobs(
                cache_root,
                apply=bool(args.apply),
                require_encryption_at_rest=bool(args.require_encryption_at_rest),
            )
            ok = ok and bool(operations["blob_gc"].get("ok"))
        result = {
            "ok": ok,
            "cache_root": "<cache-root>",
            "applied": bool(args.apply),
            "require_encryption_at_rest": bool(args.require_encryption_at_rest),
            "operations": operations,
        }
    elif args.rebuild_registry or args.write_registry:
        registry, rejected = rebuild_registry(cache_root, require_encryption_at_rest=bool(args.require_encryption_at_rest))
        result = {
            "ok": not rejected,
            "cache_root": "<cache-root>",
            "require_encryption_at_rest": bool(args.require_encryption_at_rest),
            "rebuilt_registry_entries": len(registry["entries"]),
            "rejected_manifests": rejected,
            "registry_preview": public_registry_preview(cache_root, registry),
        }
        if args.write_registry and not rejected:
            write_json(store_paths(cache_root)["registry"], registry)
            result["wrote_registry"] = "router-store/registry.json"
            append_registry_audit_event(
                cache_root,
                operation="registry_rebuild_write",
                outcome="applied",
                audit_actions=["commit"],
                reason="operator_write_registry",
            )
            result["registry_audit"] = "router-store/registry-audit.jsonl"
    else:
        result = audit_store(cache_root, require_encryption_at_rest=bool(args.require_encryption_at_rest))

    if args.json:
        print(json_dumps(result))
    else:
        print(f"cache-router store audit: {'ok' if result['ok'] else 'fail'}")
        print(f"manifests: {result.get('manifest_count', 0)}")
        print(f"rebuilt registry entries: {result.get('rebuilt_registry_entries', 0)}")
        for row in result.get("errors", []):
            print(f"- error {row['manifest_path']}: {', '.join(row['errors'])}")
        for row in result.get("rejected_manifests", []):
            print(f"- rejected {row['manifest_path']}: {', '.join(row['errors'])}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
