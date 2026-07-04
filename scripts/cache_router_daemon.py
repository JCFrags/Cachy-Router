#!/usr/bin/env python3
"""Long-running OpenAI-compatible cache-router daemon.

This is the MVP router endpoint for trusted home-LAN cache-router experiments.
It exposes an OpenAI-compatible surface and routes cached completion requests
to configured router-managed llama.cpp workers.

Normal OpenAI requests pass through to the worker. Cache-accelerated requests
use a nonstandard `cache_router` object that is stripped before any backend
request is sent.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import hmac
import json
import math
import os
import re
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cache_router_transport import SlotTransport


SCHEMA_VERSION = "2026-06-30.1"
TENANT_HASH = hashlib.sha256(b"openai-cache-router-localhost-tenant").hexdigest()
CONVERSATION_HASH = hashlib.sha256(b"openai-cache-router-localhost-conversation").hexdigest()
POLICY_HASH = hashlib.sha256(b"openai-cache-router-localhost-policy").hexdigest()
NATIVE_COMPLETION_OPTION_FIELDS = {
    "temperature",
    "top_k",
    "top_p",
    "min_p",
    "seed",
    "repeat_penalty",
    "repeat_last_n",
    "presence_penalty",
    "frequency_penalty",
    "penalize_nl",
    "mirostat",
    "mirostat_tau",
    "mirostat_eta",
    "typical_p",
    "tfs_z",
    "dynatemp_range",
    "dynatemp_exponent",
    "min_keep",
    "stop",
    "ignore_eos",
    "n_probs",
    "grammar",
    "json_schema",
    "samplers",
    "spec_draft_n_max",
    "spec_draft_n_min",
    "n_draft",
    "draft_n",
    "draft_n_min",
    "speculative_decoding",
    "spec_draft",
    "speculative.n_max",
    "speculative.n_min",
    "speculative.p_min",
    "speculative.type",
}
UNKNOWN_STRICT_VALUES = {"unknown", "not_captured", "not_interpreted"}
QUARANTINED_CACHE_STATUSES = {"quarantined", "corrupt"}
INACTIVE_MANIFEST_STATUSES = QUARANTINED_CACHE_STATUSES | {"tenant_deleted"}
ENCRYPTION_AT_REST_MODES = {"operator_managed_encrypted_filesystem", "platform_encrypted_volume"}
ENCRYPTION_EVIDENCE_BASES = {"operator_attestation", "setup_doctor_metadata"}
STRICT_HASH_FIELDS = {
    "model_hash",
    "gguf_tensor_manifest_hash",
    "tokenizer_hash",
    "chat_template_effective_hash",
    "tools_schema_hash",
    "system_prompt_hash",
    "spec_draft_model_hash",
}
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
    "ctx_checkpoints_config",
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
BASE_CACHE_KEY_FIELDS = [
    "cache_id",
    "scope",
    "tenant_hash",
    "conversation_hash",
    "policy_id_hash",
    "prefix_sha256",
    "prefix_token_count",
    "model_identity",
    "model_file_size",
    "llama_server_version",
    "ctx_size",
    "cache_type_k",
    "cache_type_v",
    "mtp_enabled",
    "spec_draft_model_identity",
    "spec_draft_model_size",
]
CACHE_KEY_FIELDS = BASE_CACHE_KEY_FIELDS + STRICT_COMPATIBILITY_FIELDS
AUDIT_ACTIONS = {"lookup", "hit", "miss", "restore", "commit", "fallback", "denial"}
CACHE_SCOPES = {"global_system", "tenant", "conversation", "private_disabled"}
TOKENIZER_FINGERPRINT_PROBES = [
    "",
    "Cachy Router strict tokenizer probe.",
    "<|im_start|>system\nCache probe.<|im_end|>\n<|im_start|>user\nping<|im_end|>",
    "Unicode probe: cafe naive facade -> ASCII fallback stable.",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def native_completion_options(body: dict[str, Any]) -> dict[str, Any]:
    """Keep deterministic sampling knobs aligned between OpenAI and native paths."""
    options: dict[str, Any] = {}
    for field in sorted(NATIVE_COMPLETION_OPTION_FIELDS):
        if field not in body:
            continue
        value = body[field]
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool, list, dict)) and not (isinstance(value, float) and math.isnan(value)):
            options[field] = value
    return options


def new_opaque_id(prefix: str) -> str:
    payload = f"{time.time_ns()}:{threading.get_ident()}".encode("utf-8")
    return prefix + "-" + hashlib.sha256(payload).hexdigest()[:16]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def is_sha256_hex(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def is_placeholder_value(value: Any) -> bool:
    return isinstance(value, str) and ("<" in value or ">" in value)


def durable_blob_encryption_metadata(args: argparse.Namespace) -> dict[str, Any] | None:
    mode = str(getattr(args, "durable_blob_encryption_mode", "") or "").strip()
    evidence_basis = str(getattr(args, "durable_blob_encryption_evidence_basis", "") or "").strip()
    volume_id_hash = str(getattr(args, "durable_blob_encryption_volume_id_hash", "") or "").strip()
    key_owner = str(getattr(args, "durable_blob_encryption_key_owner", "") or "").strip()
    if not any([mode, evidence_basis, volume_id_hash, key_owner]):
        return None
    if mode not in ENCRYPTION_AT_REST_MODES:
        raise RuntimeError("durable blob encryption mode must be operator_managed_encrypted_filesystem or platform_encrypted_volume")
    if evidence_basis not in ENCRYPTION_EVIDENCE_BASES:
        raise RuntimeError("durable blob encryption evidence basis must be operator_attestation or setup_doctor_metadata")
    if not is_sha256_hex(volume_id_hash):
        raise RuntimeError("durable blob encryption volume id hash must be a lowercase SHA-256 hex digest")
    if not key_owner:
        raise RuntimeError("durable blob encryption key owner must be a non-empty operator label")
    return {
        "required": True,
        "mode": mode,
        "evidence_basis": evidence_basis,
        "volume_id_hash": volume_id_hash,
        "key_owner": key_owner[:80],
    }


def strict_value_error(field: str, value: Any, *, mtp_enabled: bool = True) -> str | None:
    if field == "kv_unified_mode":
        if isinstance(value, bool):
            return None
        return f"{field} must be a boolean"
    if field in {"ctx_size", "n_parallel", "n_seq_max"}:
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return None
        return f"{field} must be a positive integer"
    if field in {"spec_draft_model_hash", "spec_draft_config"} and not mtp_enabled:
        if value == "none":
            return None
        return f"{field} must be 'none' when mtp_enabled is false"
    if value in (None, ""):
        return f"manifest missing {field}"
    text = str(value)
    if is_placeholder_value(text):
        return f"{field} cannot be a placeholder for strict cache restore"
    if text in UNKNOWN_STRICT_VALUES:
        return f"{field} cannot be {text!r} for strict cache restore"
    if field in STRICT_HASH_FIELDS and not is_sha256_hex(text):
        return f"{field} must be a sha256 hex string"
    return None


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


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
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)


def is_quarantined_cache_status(value: Any) -> bool:
    return str(value or "") in QUARANTINED_CACHE_STATUSES


class DurableBlobCorruptError(RuntimeError):
    """Deterministic router-owned blob validation failure."""


class CachePolicyDeniedError(RuntimeError):
    """Fail-closed cache policy denial before restore or generation."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"cache policy denied: {reason}")


class RegistryLeaseConflictError(RuntimeError):
    def __init__(self, operation: str, cache_key_hash: str, holder: dict[str, Any]) -> None:
        self.operation = operation
        self.cache_key_hash = cache_key_hash
        self.holder = holder
        holder_owner = str(holder.get("owner_id") or "unknown")
        expires_at = str(holder.get("expires_at") or "unknown")
        super().__init__(
            f"registry lease conflict: operation={operation} cache_key_hash={cache_key_hash} "
            f"holder_owner={holder_owner} expires_at={expires_at}"
        )


class CacheKeyMismatchError(RuntimeError):
    """Strict cache key material did not match a scoped registry/manifest row."""

    def __init__(
        self,
        cache_id: str,
        expected_cache_key_hash: str,
        actual_cache_key_hash: Any = None,
        manifest_id: Any = None,
    ) -> None:
        self.cache_id = cache_id
        self.expected_cache_key_hash = expected_cache_key_hash
        self.actual_cache_key_hash = str(actual_cache_key_hash) if actual_cache_key_hash not in (None, "") else None
        self.manifest_id = str(manifest_id) if manifest_id not in (None, "") else None
        actual = self.actual_cache_key_hash or "missing"
        super().__init__(f"cache_id/cache_key_hash mismatch: cache_id={cache_id!r} expected={expected_cache_key_hash} actual={actual}")


class QueueBackpressureError(RuntimeError):
    """Bounded router queue refusal before contacting a worker."""

    def __init__(self, reason: str, worker_id: str | None = None) -> None:
        self.reason = reason
        self.worker_id = worker_id
        target = f" for worker {worker_id}" if worker_id else ""
        super().__init__(f"queue backpressure{target}: {reason}")


def durable_blob_requires_quarantine(exc: Exception) -> bool:
    return isinstance(exc, DurableBlobCorruptError)


def default_cache_policy() -> dict[str, Any]:
    return {
        "scope": "conversation",
        "tenant_hash": TENANT_HASH,
        "conversation_hash": CONVERSATION_HASH,
        "policy_id_hash": POLICY_HASH,
        "policy_basis": "default_local_policy",
    }


def cache_policy_from_extension(extension: dict[str, Any]) -> dict[str, Any]:
    scope = extension.get("scope", "conversation")
    if not isinstance(scope, str) or scope not in CACHE_SCOPES:
        allowed = ", ".join(sorted(CACHE_SCOPES))
        raise ValueError(f"cache_router.scope must be one of: {allowed}")

    tenant_hash = extension.get("tenant_hash", TENANT_HASH)
    if not is_sha256_hex(tenant_hash):
        raise ValueError("cache_router.tenant_hash must be a sha256 hex string")

    if "conversation_hash" in extension:
        conversation_hash: Any = extension.get("conversation_hash")
    else:
        conversation_hash = CONVERSATION_HASH if scope == "conversation" else None
    if conversation_hash in ("", "none"):
        conversation_hash = None
    if scope == "conversation" and not is_sha256_hex(conversation_hash):
        raise ValueError("cache_router.conversation_hash must be a sha256 hex string for conversation scope")
    if conversation_hash is not None and not is_sha256_hex(conversation_hash):
        raise ValueError("cache_router.conversation_hash must be a sha256 hex string or omitted")

    policy_id_hash = extension.get("policy_id_hash", POLICY_HASH)
    if not is_sha256_hex(policy_id_hash):
        raise ValueError("cache_router.policy_id_hash must be a sha256 hex string")

    return {
        "scope": scope,
        "tenant_hash": str(tenant_hash),
        "conversation_hash": str(conversation_hash) if conversation_hash is not None else None,
        "policy_id_hash": str(policy_id_hash),
        "policy_basis": "request_cache_router",
    }


def restore_validation_mode_from_extension(extension: dict[str, Any]) -> str:
    value = extension.get("restore_validation", extension.get("restore_validation_mode", "off"))
    mode = str(value or "off").strip().lower().replace("-", "_")
    if mode in {"off", "none", "disabled"}:
        return "off"
    if mode in {"deterministic_recompute", "deterministic_text", "cold_recompute"}:
        return "deterministic_recompute"
    raise ValueError("cache_router.restore_validation must be 'off' or 'deterministic_recompute'")


def restore_validation_recompute_from_extension(extension: dict[str, Any], mode: str) -> bool:
    value = extension.get("restore_validation_recompute_on_mismatch")
    if value is None:
        return mode == "deterministic_recompute"
    if not isinstance(value, bool):
        raise ValueError("cache_router.restore_validation_recompute_on_mismatch must be a boolean")
    return value


def policy_metadata(cache_policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "scope": cache_policy["scope"],
        "tenant_hash": cache_policy["tenant_hash"],
        "conversation_hash": cache_policy.get("conversation_hash"),
        "policy_id_hash": cache_policy["policy_id_hash"],
    }


def cache_record_policy(record: dict[str, Any]) -> dict[str, Any]:
    defaults = default_cache_policy()
    return {
        "scope": record.get("scope") or defaults["scope"],
        "tenant_hash": record.get("tenant_hash") or defaults["tenant_hash"],
        "conversation_hash": record.get("conversation_hash", defaults["conversation_hash"]),
        "policy_id_hash": record.get("policy_id_hash") or defaults["policy_id_hash"],
        "policy_basis": "manifest_policy",
    }


def manifest_cache_policy(manifest: dict[str, Any]) -> dict[str, Any]:
    return cache_record_policy(manifest)


def request_policy_denial_reason(cache_policy: dict[str, Any]) -> str | None:
    scope = cache_policy.get("scope")
    if scope not in {"tenant", "conversation"}:
        if scope == "private_disabled":
            return "private_disabled"
        if scope == "global_system":
            return "global_system_not_allowlisted"
        return "scope_not_allowed"
    if not is_sha256_hex(cache_policy.get("tenant_hash")):
        return "tenant_hash_missing"
    if scope == "conversation" and not is_sha256_hex(cache_policy.get("conversation_hash")):
        return "conversation_hash_missing"
    if not is_sha256_hex(cache_policy.get("policy_id_hash")):
        return "policy_id_hash_missing"
    return None


def cache_policy_denial_reason(cache_policy: dict[str, Any], manifest: dict[str, Any]) -> str | None:
    request_denial = request_policy_denial_reason(cache_policy)
    if request_denial:
        return request_denial
    manifest_policy = manifest_cache_policy(manifest)
    if cache_policy.get("policy_id_hash") != manifest_policy.get("policy_id_hash"):
        return "policy_id_mismatch"
    if cache_policy.get("scope") != manifest_policy.get("scope"):
        return "scope_mismatch"
    if cache_policy.get("tenant_hash") != manifest_policy.get("tenant_hash"):
        return "tenant_scope_mismatch"
    if cache_policy.get("scope") == "conversation" and cache_policy.get("conversation_hash") != manifest_policy.get("conversation_hash"):
        return "conversation_scope_mismatch"
    return None


def cache_policy_event_summary(cache_policy: dict[str, Any], *, denial_reason: str | None = None) -> dict[str, Any]:
    scope = cache_policy.get("scope")
    basis = str(cache_policy.get("policy_basis") or "policy_rule")
    if basis in {"default_local_policy", "request_cache_router", "manifest_policy"}:
        basis = "policy_rule"
    return {
        "scope_allowed": denial_reason not in {"scope_not_allowed", "global_system_not_allowlisted", "private_disabled"},
        "persistence_allowed": scope != "private_disabled" and denial_reason != "private_disabled",
        "cross_tenant_reuse_allowed": False,
        "global_system_allowlisted": False,
        "policy_id_hash": cache_policy.get("policy_id_hash"),
        "policy_basis": basis,
    }


def cache_key_material_from_record(record: dict[str, Any]) -> dict[str, Any]:
    return {field: record.get(field) for field in CACHE_KEY_FIELDS}


def validated_cache_key_material(record: dict[str, Any], *, label: str = "cache key") -> dict[str, Any]:
    fields = cache_key_material_from_record(record)
    errors: list[str] = []
    if fields.get("mtp_enabled") is False:
        if fields.get("spec_draft_model_identity") in (None, ""):
            fields["spec_draft_model_identity"] = "none"
        if fields.get("spec_draft_model_size") in (None, ""):
            fields["spec_draft_model_size"] = 0
    for field in BASE_CACHE_KEY_FIELDS:
        value = fields.get(field)
        if field == "conversation_hash" and fields.get("scope") != "conversation":
            fields[field] = None
            continue
        if value in (None, ""):
            errors.append(f"{field} missing")
    if fields.get("scope") not in CACHE_SCOPES:
        errors.append("scope invalid")
    if not is_sha256_hex(fields.get("tenant_hash")):
        errors.append("tenant_hash must be a sha256 hex string")
    if fields.get("scope") == "conversation" and not is_sha256_hex(fields.get("conversation_hash")):
        errors.append("conversation_hash must be a sha256 hex string for conversation scope")
    if not is_sha256_hex(fields.get("policy_id_hash")):
        errors.append("policy_id_hash must be a sha256 hex string")
    if not is_sha256_hex(fields.get("prefix_sha256")):
        errors.append("prefix_sha256 must be a sha256 hex string")
    for field in ["prefix_token_count", "model_file_size", "ctx_size"]:
        value = fields.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            errors.append(f"{field} must be a positive integer")
    if not isinstance(fields.get("mtp_enabled"), bool):
        errors.append("mtp_enabled must be a boolean")
    spec_size = fields.get("spec_draft_model_size")
    if not isinstance(spec_size, int) or isinstance(spec_size, bool) or spec_size < 0:
        errors.append("spec_draft_model_size must be a non-negative integer")
    mtp_enabled = bool(fields.get("mtp_enabled"))
    for field in STRICT_COMPATIBILITY_FIELDS:
        error = strict_value_error(field, fields.get(field), mtp_enabled=mtp_enabled)
        if error:
            errors.append(error)
    if mtp_enabled and fields.get("spec_draft_model_identity") in (None, "", "none"):
        errors.append("spec_draft_model_identity missing when mtp_enabled is true")
    if errors:
        raise RuntimeError(f"{label} material invalid: " + "; ".join(errors))
    return fields


def cache_key_hash_from_record(record: dict[str, Any], *, label: str = "cache key") -> str:
    return sha256_json(validated_cache_key_material(record, label=label))


def classify_full_reprocess(
    *,
    decision: str,
    cache_hit_level: str,
    cached_tokens: Any,
    processed_prompt_tokens: Any,
    prompt_basis: str | None = None,
) -> str:
    if decision != "restore_then_generate" or cache_hit_level not in {"local_nvme", "durable_blob"}:
        return "not_interpreted"
    try:
        processed = int(processed_prompt_tokens)
    except (TypeError, ValueError):
        return "not_captured"
    if prompt_basis == "suffix_only_after_slot_restore":
        return "no" if processed >= 0 else "not_captured"
    try:
        cached = int(cached_tokens)
    except (TypeError, ValueError):
        return "not_captured"
    if cached <= 0:
        return "yes"
    if processed >= cached:
        return "yes"
    return "no"


def audit_actions_for_event(
    *,
    phase: str,
    decision: str,
    cache_hit_level: str,
    compatibility_result: str,
    fallback_required: bool,
    fallback_reason: str | None,
) -> list[str]:
    actions: set[str] = set()
    if phase == "registry_lookup" or cache_hit_level in {"registry_only", "local_nvme", "durable_blob"}:
        actions.add("lookup")
    if phase == "worker_selected":
        actions.add("lookup")
    if phase == "cache_commit_published":
        actions.add("commit")
    if decision in {"hot_local_hit", "durable_hit_hydrate", "restore_then_generate"} and cache_hit_level in {"local_nvme", "durable_blob"}:
        actions.add("hit")
    if compatibility_result == "miss" or fallback_reason == "no_compatible_manifest":
        actions.add("miss")
    if (
        decision in {"restore_then_generate", "fallback_after_restore_failure"}
        or phase in {"restore_requested", "restore_validated"}
        or fallback_reason == "restore_validation_failed"
    ):
        actions.add("restore")
    if fallback_required or decision in {"fallback_after_restore_failure", "cold_prefill"}:
        actions.add("fallback")
    if decision == "reject_policy" or compatibility_result == "policy_denied" or fallback_reason == "policy_denied":
        actions.add("denial")
    return [action for action in ["lookup", "hit", "miss", "restore", "commit", "fallback", "denial"] if action in actions]


def registry_audit_operation(row: dict[str, Any]) -> str:
    actions = row.get("audit_actions") if isinstance(row.get("audit_actions"), list) else []
    phase = str(row.get("phase") or "unknown")
    if "denial" in actions:
        return "denial"
    if "commit" in actions:
        return "commit"
    if "restore" in actions:
        return "restore"
    if "fallback" in actions:
        return "fallback"
    if "hit" in actions:
        return "hit"
    if "miss" in actions:
        return "miss"
    if "lookup" in actions:
        return "lookup"
    return phase


def registry_audit_outcome(row: dict[str, Any]) -> str:
    if row.get("policy_denial_reason"):
        return "denied"
    validation_status = str(row.get("validation_status") or "")
    if validation_status == "quarantined":
        return "quarantined"
    if row.get("fallback_required") is True:
        return "fallback"
    compatibility_result = str(row.get("compatibility_result") or "")
    if compatibility_result == "match":
        return "success"
    if compatibility_result == "miss":
        return "miss"
    if compatibility_result == "mismatch":
        return "mismatch"
    if compatibility_result == "policy_denied":
        return "denied"
    return str(row.get("decision") or "recorded")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    fsync_dir(path.parent)


def read_jsonl_tail(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def filter_decision_rows(rows: list[dict[str, Any]], *, request_id: str | None = None) -> list[dict[str, Any]]:
    if not request_id:
        return rows
    return [row for row in rows if row.get("request_id") == request_id]


def admin_route_path(path: str) -> str | None:
    parsed_path = urllib.parse.urlparse(path).path
    if parsed_path in {"/metrics", "/router/status", "/router/workers", "/router/cache", "/router/decisions"}:
        return parsed_path
    return None


def metric_escape(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def metric_labels(labels: dict[str, Any] | None) -> str:
    if not labels:
        return ""
    parts = [f'{key}="{metric_escape(value)}"' for key, value in sorted(labels.items())]
    return "{" + ",".join(parts) + "}"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def load_auth_token(args: argparse.Namespace) -> str:
    token = (args.auth_token or "").strip()
    if token:
        return token
    if args.auth_token_file:
        path = Path(args.auth_token_file)
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()
    return ""


def http_request(
    method: str,
    url: str,
    *,
    payload: Any | None = None,
    timeout: float = 900.0,
    stream: bool = False,
) -> tuple[int, dict[str, str], bytes, float]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept-Encoding": "identity"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            elapsed = (time.perf_counter() - start) * 1000.0
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, hdrs, raw, elapsed
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        elapsed = (time.perf_counter() - start) * 1000.0
        hdrs = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
        return exc.code, hdrs, raw, elapsed


def json_request(method: str, url: str, *, payload: Any | None = None, timeout: float = 900.0) -> tuple[int, Any, float]:
    status, _, raw, elapsed = http_request(method, url, payload=payload, timeout=timeout)
    if not raw:
        return status, {}, elapsed
    try:
        return status, json.loads(raw.decode("utf-8", errors="replace")), elapsed
    except json.JSONDecodeError:
        return status, {"raw": raw.decode("utf-8", errors="replace")}, elapsed


def looks_like_loading_response(status: int, raw: bytes) -> bool:
    if status != 503:
        return False
    text = raw.decode("utf-8", errors="replace").lower()
    return "loading" in text or "load model" in text


def extract_ttft_ms_from_json(body: Any) -> float | None:
    if not isinstance(body, dict):
        return None
    candidates: list[Any] = [
        body.get("ttft_ms"),
        body.get("time_to_first_token_ms"),
        body.get("time_to_first_token"),
    ]
    timings = body.get("timings")
    if isinstance(timings, dict):
        candidates.extend([timings.get("ttft_ms"), timings.get("time_to_first_token_ms"), timings.get("time_to_first_token")])
    for candidate in candidates:
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value >= 0.0:
            return value
    return None


def extract_backend_ttft_ms(raw: bytes) -> float | None:
    if not raw:
        return None
    try:
        body = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None
    return extract_ttft_ms_from_json(body)


def is_loopback_bind(host: str) -> bool:
    value = (host or "").strip().lower()
    return value in {"localhost", "::1"} or value.startswith("127.")


def token_count(worker_url: str, text: str, timeout: float) -> int:
    for payload in ({"content": text, "add_special": False}, {"content": text}, {"prompt": text}):
        status, body, _ = json_request("POST", worker_url + "/tokenize", payload=payload, timeout=timeout)
        if status >= 400:
            continue
        if isinstance(body, dict):
            tokens = body.get("tokens")
            if isinstance(tokens, list):
                return len(tokens)
            if isinstance(tokens, int):
                return tokens
        if isinstance(body, list):
                return len(body)
    raise RuntimeError("/tokenize did not return a token list")


def first_model_row(models_body: Any, model: str) -> dict[str, Any]:
    rows = models_body.get("data") if isinstance(models_body, dict) else None
    if not isinstance(rows, list):
        return {}
    fallback: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not fallback:
            fallback = row
        if str(row.get("id") or "") == model:
            return row
    return fallback


def metadata_needs_derivation(field: str, value: Any, *, mtp_enabled: bool) -> bool:
    if field in {"model_file_size", "spec_draft_model_size"}:
        return not isinstance(value, int) or isinstance(value, bool) or value <= 0
    if field in {"ctx_size", "n_parallel", "n_seq_max"}:
        return strict_value_error(field, value, mtp_enabled=mtp_enabled) is not None
    if field in STRICT_COMPATIBILITY_FIELDS:
        return strict_value_error(field, value, mtp_enabled=mtp_enabled) is not None
    if value in (None, ""):
        return True
    return str(value) in UNKNOWN_STRICT_VALUES


def tokenizer_fingerprint(worker_url: str, timeout: float) -> str | None:
    probe_rows: list[dict[str, Any]] = []
    for probe in TOKENIZER_FINGERPRINT_PROBES:
        tokens: list[Any] | None = None
        for payload in ({"content": probe, "add_special": False}, {"content": probe}, {"prompt": probe}):
            status, body, _ = json_request("POST", worker_url + "/tokenize", payload=payload, timeout=timeout)
            if status >= 400:
                continue
            if isinstance(body, dict) and isinstance(body.get("tokens"), list):
                tokens = body["tokens"]
                break
            if isinstance(body, list):
                tokens = body
                break
        if tokens is None:
            return None
        probe_rows.append({"probe_sha256": sha256_text(probe), "token_ids": tokens})
    return sha256_json({"basis": "runtime-tokenize-v1", "probes": probe_rows})


def derive_commit_from_version(version: str) -> str | None:
    match = re.search(r"\(([0-9a-fA-F]{7,40})\)", version or "")
    if match:
        return match.group(1).lower()
    match = re.search(r"\b([0-9a-fA-F]{12,40})\b", version or "")
    if match:
        return match.group(1).lower()
    return None


def infer_build_backend(path: str, props: dict[str, Any]) -> str:
    text = " ".join(
        [
            str(path or ""),
            str(props.get("build_info") or ""),
            str(props.get("llama_server_path") or ""),
            str(props.get("server_path") or ""),
        ]
    ).lower()
    if "vulkan" in text or "radv" in text:
        return "vulkan_radv_runtime"
    if "rocm" in text or "hip" in text:
        return "rocm_runtime"
    if "cuda" in text:
        return "cuda_runtime"
    if "metal" in text:
        return "metal_runtime"
    return "runtime_backend_unreported"


def runtime_json(worker_url: str, path: str, timeout: float) -> dict[str, Any]:
    status, body, _ = json_request("GET", worker_url + path, timeout=timeout)
    if status >= 400 or not isinstance(body, dict):
        return {}
    return body


def generation_params_from_props(props: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    settings = props.get("default_generation_settings") if isinstance(props.get("default_generation_settings"), dict) else {}
    params = settings.get("params") if isinstance(settings.get("params"), dict) else settings
    return settings, params if isinstance(params, dict) else {}


def first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def derive_worker_metadata_from_runtime(
    metadata: dict[str, Any],
    worker_url: str,
    timeout: float,
    *,
    force_runtime: bool = False,
) -> dict[str, Any]:
    mtp_enabled = bool(metadata.get("mtp_enabled"))
    derivation_fields = {
        "model_identity",
        "model_path",
        "model_file_size",
        "llama_server_version",
        "ctx_size",
        "spec_draft_model_identity",
        "spec_draft_model_path",
        "spec_draft_model_size",
        *STRICT_COMPATIBILITY_FIELDS,
    }
    if not force_runtime and not any(metadata_needs_derivation(field, metadata.get(field), mtp_enabled=mtp_enabled) for field in derivation_fields):
        if metadata.get("model_file_size", 0) and metadata.get("n_parallel", 0) and metadata.get("n_seq_max", 0):
            return metadata

    derived = dict(metadata)
    models_body = runtime_json(worker_url, "/v1/models", timeout)
    props = runtime_json(worker_url, "/props", timeout)
    model_row = first_model_row(models_body, str(derived.get("model") or ""))
    model_meta = model_row.get("meta") if isinstance(model_row.get("meta"), dict) else {}
    default_generation_settings, default_generation = generation_params_from_props(props)
    runtime_model_path_reported = first_present(props.get("model_path"), model_meta.get("file"), model_meta.get("path"))
    runtime_model_path = str(
        runtime_model_path_reported
        if force_runtime
        else first_present(runtime_model_path_reported, derived.get("model_path")) or ""
    )
    runtime_model_id = str(model_row.get("id") or derived.get("model") or "")
    runtime_version_reported = first_present(props.get("llama_server_version"), props.get("server_version"), props.get("build_info"))
    runtime_version = str(
        runtime_version_reported
        if force_runtime
        else first_present(runtime_version_reported, derived.get("llama_server_version")) or ""
    )
    total_slots = props.get("total_slots")
    path_basename = Path(runtime_model_path).name if runtime_model_path else ""
    runtime_cache_type_k = str(
        first_present(props.get("cache_type_k"), default_generation.get("cache_type_k"), default_generation_settings.get("cache_type_k"))
        or "runtime-cache-type-k-unreported"
    )
    runtime_cache_type_v = str(
        first_present(props.get("cache_type_v"), default_generation.get("cache_type_v"), default_generation_settings.get("cache_type_v"))
        or "runtime-cache-type-v-unreported"
    )
    runtime_kv_unified_raw = first_present(props.get("kv_unified"), props.get("kv_unified_mode"), default_generation.get("kv_unified"), default_generation_settings.get("kv_unified"))
    runtime_kv_unified_mode = bool(runtime_kv_unified_raw) if isinstance(runtime_kv_unified_raw, bool) else True
    runtime_material = {
        "basis": "runtime-v1-models-props-v2",
        "model_id": runtime_model_id,
        "model_path_basename": path_basename,
        "model_meta": model_meta,
        "props": {
            "build_info": props.get("build_info"),
            "chat_template_sha256": sha256_text(str(props.get("chat_template") or "")),
            "default_generation_settings": default_generation_settings,
            "llama_server_version": runtime_version,
            "modalities": props.get("modalities"),
            "model_alias": props.get("model_alias"),
            "total_slots": total_slots,
        },
        "runtime_cache": {
            "cache_type_k": runtime_cache_type_k,
            "cache_type_v": runtime_cache_type_v,
            "kv_unified_mode": runtime_kv_unified_mode,
        },
    }
    runtime_hash = sha256_json(runtime_material)

    def should_derive(field: str) -> bool:
        return force_runtime or metadata_needs_derivation(field, derived.get(field), mtp_enabled=mtp_enabled)

    if runtime_model_path and should_derive("model_path"):
        derived["model_path"] = runtime_model_path
    if runtime_version and should_derive("llama_server_version"):
        derived["llama_server_version"] = runtime_version
    if should_derive("model_identity"):
        derived["model_identity"] = f"{runtime_model_id or 'runtime-model'}:{runtime_hash[:16]}"

    size = model_meta.get("size") or model_meta.get("file_size") or model_meta.get("model_size")
    if should_derive("model_file_size"):
        try:
            parsed_size = int(size)
        except (TypeError, ValueError):
            parsed_size = 0
        derived["model_file_size"] = parsed_size if parsed_size > 0 else max(1, len(json.dumps(runtime_material, sort_keys=True)))

    if should_derive("ctx_size"):
        n_ctx = first_present(default_generation_settings.get("n_ctx"), model_meta.get("n_ctx"), model_meta.get("context_length"))
        try:
            parsed_ctx = int(n_ctx)
        except (TypeError, ValueError):
            parsed_ctx = 0
        if parsed_ctx > 0:
            derived["ctx_size"] = parsed_ctx
    if force_runtime:
        if should_derive("cache_type_k"):
            derived["cache_type_k"] = runtime_cache_type_k
        if should_derive("cache_type_v"):
            derived["cache_type_v"] = runtime_cache_type_v
        if should_derive("kv_unified_mode"):
            derived["kv_unified_mode"] = runtime_kv_unified_mode

    if should_derive("model_architecture"):
        architecture = model_meta.get("general.architecture") or model_meta.get("architecture") or model_meta.get("arch")
        n_params = model_meta.get("n_params") or model_meta.get("general.parameter_count")
        n_embd = model_meta.get("n_embd") or model_meta.get("embedding_length")
        if architecture:
            derived["model_architecture"] = f"runtime-{architecture}"
        elif n_params or n_embd:
            derived["model_architecture"] = f"runtime-nparams-{n_params or 'na'}-embd-{n_embd or 'na'}"
        else:
            derived["model_architecture"] = "runtime-model-meta-" + runtime_hash[:16]
    if should_derive("model_hash"):
        derived["model_hash"] = sha256_json({"basis": "runtime-model-fingerprint-v1", "material": runtime_material})
    if should_derive("gguf_tensor_manifest_hash"):
        derived["gguf_tensor_manifest_hash"] = sha256_json(
            {
                "basis": "runtime-gguf-tensor-manifest-surrogate-v1",
                "model_path_basename": path_basename,
                "model_file_size": derived["model_file_size"],
                "model_meta": model_meta,
            }
        )
    if should_derive("tokenizer_hash"):
        derived["tokenizer_hash"] = tokenizer_fingerprint(worker_url, timeout) or sha256_json({"basis": "runtime-tokenizer-unavailable-v1", "model": runtime_model_id})
    if should_derive("chat_template_effective_hash"):
        derived["chat_template_effective_hash"] = sha256_text(str(props.get("chat_template") or ""))
    if should_derive("tools_schema_hash"):
        derived["tools_schema_hash"] = sha256_json({"basis": "no-request-tools-schema", "tools": []})
    if should_derive("system_prompt_hash"):
        derived["system_prompt_hash"] = sha256_text("")
    if should_derive("special_token_policy"):
        derived["special_token_policy"] = "runtime-special-token-policy-" + sha256_json(
            {"chat_template": props.get("chat_template"), "tokenizer": derived.get("tokenizer_hash")}
        )[:16]
    if should_derive("llama_cpp_source_commit"):
        derived["llama_cpp_source_commit"] = derive_commit_from_version(runtime_version) or "runtime-commit-unreported-" + runtime_hash[:16]
    if should_derive("llama_cpp_cache_abi_version"):
        derived["llama_cpp_cache_abi_version"] = "runtime-cache-abi-" + sha256_json(
            {
                "version": runtime_version,
                "ctx_size": derived.get("ctx_size"),
                "cache_type_k": derived.get("cache_type_k"),
                "cache_type_v": derived.get("cache_type_v"),
                "kv_unified_mode": derived.get("kv_unified_mode"),
                "mtp_enabled": mtp_enabled,
                "total_slots": total_slots,
            }
        )[:16]
    if should_derive("patchset_id"):
        derived["patchset_id"] = "runtime-patchset-" + sha256_json({"version": runtime_version, "build_info": props.get("build_info")})[:16]
    if should_derive("build_backend"):
        derived["build_backend"] = infer_build_backend(str(derived.get("llama_server_path") or ""), props)
    if should_derive("gpu_backend_driver"):
        derived["gpu_backend_driver"] = "runtime-driver-" + sha256_text(str(derived.get("build_backend") or ""))[:16]
    if should_derive("ctx_checkpoints_config"):
        derived["ctx_checkpoints_config"] = "runtime-ctx-checkpoints-" + sha256_json(
            {"ctx_size": derived.get("ctx_size"), "cache_type_k": derived.get("cache_type_k"), "cache_type_v": derived.get("cache_type_v")}
        )[:16]
    for field, default_value in [
        ("flash_attention_mode", default_generation.get("flash_attn") if "flash_attn" in default_generation else "runtime-unreported"),
        ("rope_freq_base", default_generation.get("rope_freq_base") or "runtime-default"),
        ("rope_freq_scale", default_generation.get("rope_freq_scale") or "runtime-default"),
        ("yarn_or_rope_scaling_metadata", default_generation.get("yarn_ext_factor") or "runtime-default"),
        ("reasoning_format", default_generation.get("reasoning_format") or "runtime-unreported"),
        ("jinja_template_mode", "enabled" if props.get("chat_template") else "runtime-unreported"),
    ]:
        if should_derive(field):
            derived[field] = str(default_value)
    if mtp_enabled:
        draft_reported = props.get("draft_model") if isinstance(props.get("draft_model"), dict) else {}
        draft_runtime_material = {
            "basis": "runtime-spec-draft-material-v1",
            "model": runtime_model_id,
            "draft_model": draft_reported,
            "default_generation_settings": {
                key: default_generation.get(key)
                for key in sorted(default_generation)
                if "draft" in key or "spec" in key
            },
        }
        if should_derive("spec_draft_model_identity"):
            derived["spec_draft_model_identity"] = "runtime-spec-draft-" + sha256_json(draft_runtime_material)[:16]
        try:
            draft_size = int(
                first_present(
                    draft_reported.get("size"),
                    draft_reported.get("file_size"),
                    None if force_runtime else derived.get("spec_draft_model_size"),
                )
            )
        except (TypeError, ValueError):
            draft_size = 0
        if should_derive("spec_draft_model_size") or draft_size <= 0:
            derived["spec_draft_model_size"] = draft_size if draft_size > 0 else max(1, len(str(derived["spec_draft_model_identity"])))
        spec_material = {
            "basis": "runtime-spec-draft-fingerprint-v1",
            "identity": derived.get("spec_draft_model_identity"),
            "path": None if force_runtime else derived.get("spec_draft_model_path"),
            "size": derived.get("spec_draft_model_size"),
            "runtime_material": draft_runtime_material,
        }
        if should_derive("spec_draft_model_hash"):
            derived["spec_draft_model_hash"] = sha256_json(spec_material)
        if should_derive("spec_draft_config"):
            derived["spec_draft_config"] = "runtime-spec-draft-" + sha256_json(spec_material)[:16]
    try:
        parsed_slots = int(total_slots)
    except (TypeError, ValueError):
        parsed_slots = 0
    for field in ("n_parallel", "n_seq_max"):
        if should_derive(field):
            derived[field] = max(1, parsed_slots)
    return derived


def native_completion_payload(
    prompt: str,
    *,
    n_predict: int,
    slot_id: int,
    cache_prompt: bool = True,
    generation_options: dict[str, Any] | None = None,
    stream: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "prompt": prompt,
        "n_predict": n_predict,
        "cache_prompt": cache_prompt,
        "id_slot": slot_id,
        "stream": stream,
    }
    if generation_options:
        for key, value in generation_options.items():
            if key in {"prompt", "n_predict", "id_slot", "cache_prompt", "stream"}:
                continue
            payload[key] = value
    return payload


def completion(
    worker_url: str,
    prompt: str,
    *,
    n_predict: int,
    slot_id: int,
    timeout: float,
    cache_prompt: bool = True,
    generation_options: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], float]:
    payload = native_completion_payload(
        prompt,
        n_predict=n_predict,
        slot_id=slot_id,
        cache_prompt=cache_prompt,
        generation_options=generation_options,
        stream=False,
    )
    status, body, wall_ms = json_request("POST", worker_url + "/completion", payload=payload, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"worker /completion failed HTTP {status}: {body}")
    if not isinstance(body, dict):
        raise RuntimeError("worker /completion returned non-object JSON")
    body["_router_wall_ms"] = wall_ms
    return body, wall_ms


def completion_stream_ttft_probe(
    worker_url: str,
    prompt: str,
    *,
    slot_id: int,
    timeout: float,
    cache_prompt: bool = False,
    generation_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = native_completion_payload(
        prompt,
        n_predict=1,
        slot_id=slot_id,
        cache_prompt=cache_prompt,
        generation_options=generation_options,
        stream=True,
    )
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        worker_url + "/completion",
        data=data,
        headers={"Accept-Encoding": "identity", "Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    content_parts: list[str] = []
    parsed_chunks = 0
    first_token_ms: float | None = None
    final_payload: dict[str, Any] = {}
    raw_preview = b""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            while True:
                line = resp.readline()
                if not line:
                    break
                if len(raw_preview) < 256:
                    raw_preview += line[: max(0, 256 - len(raw_preview))]
                stripped = line.strip()
                if not stripped or stripped.startswith(b":"):
                    continue
                if stripped.startswith(b"data:"):
                    stripped = stripped[5:].strip()
                if stripped == b"[DONE]":
                    break
                try:
                    chunk = json.loads(stripped.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    continue
                if not isinstance(chunk, dict):
                    continue
                parsed_chunks += 1
                final_payload.update(chunk)
                token_text = chunk.get("content")
                if isinstance(token_text, str) and token_text:
                    if first_token_ms is None:
                        first_token_ms = (time.perf_counter() - started) * 1000.0
                    content_parts.append(token_text)
            wall_ms = (time.perf_counter() - started) * 1000.0
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        raise RuntimeError(f"worker /completion stream probe failed HTTP {exc.code}: {raw.decode('utf-8', errors='replace')[:500]}") from exc
    if first_token_ms is None and parsed_chunks > 0:
        first_token_ms = wall_ms
    result: dict[str, Any] = {
        "measurement_basis": "router_observed_native_completion_stream_first_token",
        "time_to_first_token_ms": first_token_ms,
        "wall_ms": wall_ms,
        "parsed_chunks": parsed_chunks,
        "content_chars": sum(len(part) for part in content_parts),
        "raw_preview_sha256": hashlib.sha256(raw_preview).hexdigest() if raw_preview else None,
    }
    for key in ("tokens_evaluated", "tokens_cached", "tokens_predicted", "timings"):
        if key in final_payload:
            result[key] = final_payload[key]
    return result


def slot_action(worker_url: str, slot_id: int, action: str, filename: str | None, timeout: float) -> tuple[dict[str, Any], float]:
    payload = {} if filename is None else {"filename": filename}
    status, body, wall_ms = json_request("POST", f"{worker_url}/slots/{slot_id}?action={action}", payload=payload, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"slot action {action} failed HTTP {status}: {body}")
    if not isinstance(body, dict):
        body = {"body": body}
    return body, wall_ms


def openai_completion_response(
    *,
    model: str,
    text: str,
    prompt_tokens: int,
    completion_tokens: int,
    cache_router: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "id": "cmpl-cache-router-" + hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16],
        "object": "text_completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"text": text, "index": 0, "finish_reason": "stop", "logprobs": None}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
    if cache_router is not None:
        response["cache_router"] = cache_router
    return response


def openai_chat_response(*, model: str, content: str, cache_router: dict[str, Any] | None = None) -> dict[str, Any]:
    response = {
        "id": "chatcmpl-cache-router-" + hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16],
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    if cache_router is not None:
        response["cache_router"] = cache_router
    return response


def slot_rows(slots: Any) -> list[dict[str, Any]]:
    if isinstance(slots, list):
        return [row for row in slots if isinstance(row, dict)]
    if isinstance(slots, dict):
        rows = slots.get("slots")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        if isinstance(slots.get("id"), int):
            return [slots]
    return []


def slot_availability(slots: Any, slot_id: int) -> dict[str, Any]:
    rows = slot_rows(slots)
    selected = next((row for row in rows if row.get("id") == slot_id), rows[0] if rows else None)
    if selected is None:
        return {"available": None, "busy_score": 1, "reason": "slot_state_unknown"}
    is_processing = selected.get("is_processing")
    next_tokens = selected.get("next_token") if isinstance(selected.get("next_token"), list) else []
    has_next = any(isinstance(row, dict) and row.get("has_next_token") is True for row in next_tokens)
    stalled_next_token = is_processing is False and has_next
    busy = is_processing is True or has_next
    return {
        "available": not busy,
        "busy_score": 100 if stalled_next_token else 2 if busy else 0,
        "reason": "stalled_next_token" if stalled_next_token else "busy" if busy else "idle",
        "is_processing": is_processing,
        "has_next_token": has_next,
        "poisoned": stalled_next_token,
        "n_prompt_tokens": selected.get("n_prompt_tokens"),
        "n_prompt_tokens_processed": selected.get("n_prompt_tokens_processed"),
    }


def routable_availability(availability: dict[str, Any]) -> bool:
    return availability.get("poisoned") is not True


def row_value(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default


def path_like(value: Any) -> bool:
    text = str(value or "")
    return text.startswith("/") or text.endswith(".gguf")


def bool_value(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def int_value(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    return int(value)


@dataclass
class WorkerRuntime:
    worker_id: str
    url: str
    slot_id: int
    slot_save_path: str
    transport: SlotTransport
    model: str
    model_identity: str
    model_path: str
    model_file_size: int
    model_architecture: str
    model_hash: str
    gguf_tensor_manifest_hash: str
    tokenizer_hash: str
    chat_template_effective_hash: str
    tools_schema_hash: str
    system_prompt_hash: str
    special_token_policy: str
    llama_server_path: str
    llama_server_version: str
    llama_cpp_source_commit: str
    llama_cpp_cache_abi_version: str
    patchset_id: str
    build_backend: str
    gpu_backend_driver: str
    kv_unified_mode: bool
    ctx_size: int
    ctx_checkpoints_config: str
    cache_type_k: str
    cache_type_v: str
    flash_attention_mode: str
    rope_freq_base: str
    rope_freq_scale: str
    yarn_or_rope_scaling_metadata: str
    reasoning_format: str
    jinja_template_mode: str
    mtp_enabled: bool
    spec_draft_model_identity: str
    spec_draft_model_path: str
    spec_draft_model_size: int
    spec_draft_model_hash: str
    spec_draft_config: str
    n_parallel: int
    n_seq_max: int
    strict_metadata_auto: bool = True
    strict_metadata_force_runtime: bool = False

    def model_readiness(self, *, timeout: float = 5.0) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            status, body, wall_ms = json_request("GET", self.url + "/v1/models", timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            return {
                "http_status": None,
                "error": repr(exc),
                "wall_ms": (time.perf_counter() - start) * 1000.0,
                "ok": False,
                "state": "unreachable",
                "expected_model": self.model,
                "model_ids": [],
            }
        model_ids: list[str] = []
        if isinstance(body, dict) and isinstance(body.get("data"), list):
            for row in body["data"]:
                if isinstance(row, dict) and isinstance(row.get("id"), str):
                    model_ids.append(row["id"])
        model_present = self.model in model_ids
        if status == 503:
            state = "warming"
        elif status == 200 and model_present:
            state = "ready"
        elif status == 200:
            state = "model_mismatch"
        else:
            state = "not_ready"
        return {
            "http_status": status,
            "body": body,
            "wall_ms": wall_ms,
            "ok": status == 200 and model_present,
            "state": state,
            "expected_model": self.model,
            "model_ids": model_ids,
        }

    def health(self, *, timeout: float = 5.0) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            status, body, wall_ms = json_request("GET", self.url + "/health", timeout=timeout)
            return {"http_status": status, "body": body, "wall_ms": wall_ms, "ok": status == 200 and isinstance(body, dict) and body.get("status") == "ok"}
        except Exception as exc:  # noqa: BLE001
            return {"http_status": None, "error": repr(exc), "wall_ms": (time.perf_counter() - start) * 1000.0, "ok": False}

    def slots(self, *, timeout: float = 5.0) -> Any:
        try:
            status, body, _ = json_request("GET", self.url + "/slots", timeout=timeout)
            return body if status == 200 else {"http_status": status, "body": body}
        except Exception as exc:  # noqa: BLE001
            return {"http_status": None, "error": repr(exc)}

    def availability(self) -> dict[str, Any]:
        return slot_availability(self.slots(), self.slot_id)

    def sidecar_readiness(self, *, timeout: float = 5.0) -> dict[str, Any]:
        return self.transport.sidecar_readiness(timeout=timeout)

    def summary(self, state: "CacheRouterState", *, include_slots: bool = True) -> dict[str, Any]:
        readiness = state.worker_readiness(self, include_sidecar=True)
        if self.strict_metadata_force_runtime:
            strict_metadata_source = "runtime_forced"
        elif self.strict_metadata_auto:
            strict_metadata_source = "runtime_fill_missing"
        else:
            strict_metadata_source = "inventory"
        row = {
            "worker_id": self.worker_id,
            "url": self.url,
            "health": readiness.get("health", {}),
            "model_readiness": readiness.get("model_readiness", {}),
            "sidecar_readiness": readiness.get("sidecar_readiness", {}),
            "readiness": {
                "ok": readiness.get("ok", False),
                "state": readiness.get("state", "unknown"),
                "checked_at": readiness.get("checked_at"),
                "poll_interval_seconds": readiness.get("poll_interval_seconds"),
            },
            "availability": self.availability(),
            "slot_save_path": self.slot_save_path,
            "slot_id": self.slot_id,
            "transport": self.transport.describe(),
            "model": self.model,
            "model_identity": self.model_identity,
            "model_path": self.model_path,
            "model_file_size": self.model_file_size,
            "model_architecture": self.model_architecture,
            "model_hash": self.model_hash,
            "gguf_tensor_manifest_hash": self.gguf_tensor_manifest_hash,
            "tokenizer_hash": self.tokenizer_hash,
            "chat_template_effective_hash": self.chat_template_effective_hash,
            "tools_schema_hash": self.tools_schema_hash,
            "system_prompt_hash": self.system_prompt_hash,
            "special_token_policy": self.special_token_policy,
            "llama_server_path": self.llama_server_path,
            "llama_server_version": self.llama_server_version,
            "llama_cpp_source_commit": self.llama_cpp_source_commit,
            "llama_cpp_cache_abi_version": self.llama_cpp_cache_abi_version,
            "patchset_id": self.patchset_id,
            "build_backend": self.build_backend,
            "gpu_backend_driver": self.gpu_backend_driver,
            "kv_unified_mode": self.kv_unified_mode,
            "ctx_size": self.ctx_size,
            "ctx_checkpoints_config": self.ctx_checkpoints_config,
            "cache_type_k": self.cache_type_k,
            "cache_type_v": self.cache_type_v,
            "flash_attention_mode": self.flash_attention_mode,
            "rope_freq_base": self.rope_freq_base,
            "rope_freq_scale": self.rope_freq_scale,
            "yarn_or_rope_scaling_metadata": self.yarn_or_rope_scaling_metadata,
            "reasoning_format": self.reasoning_format,
            "jinja_template_mode": self.jinja_template_mode,
            "mtp_enabled": self.mtp_enabled,
            "spec_draft_model_identity": self.spec_draft_model_identity,
            "spec_draft_model_path": self.spec_draft_model_path,
            "spec_draft_model_size": self.spec_draft_model_size,
            "spec_draft_model_hash": self.spec_draft_model_hash,
            "spec_draft_config": self.spec_draft_config,
            "n_parallel": self.n_parallel,
            "n_seq_max": self.n_seq_max,
            "strict_metadata_auto": self.strict_metadata_auto,
            "strict_metadata_force_runtime": self.strict_metadata_force_runtime,
            "strict_metadata_source": strict_metadata_source,
        }
        if include_slots:
            row["slots"] = self.slots()
        return row


def worker_metadata_from_row(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    raw_model = row_value(row, "model_name", "alias", default="")
    legacy_model = row.get("model")
    if not raw_model and legacy_model and not path_like(legacy_model):
        raw_model = str(legacy_model)
    model = str(raw_model or args.model)

    model_path = str(row_value(row, "model_path", "model_file", default=""))
    if not model_path and legacy_model and path_like(legacy_model):
        model_path = str(legacy_model)
    model_path = model_path or args.model_path

    mtp_enabled = bool_value(row_value(row, "mtp_enabled", default=args.mtp_enabled), args.mtp_enabled)
    spec_path = str(row_value(row, "spec_draft_model_path", "mtp_model", "draft_model_path", default=args.spec_draft_model_path))
    if not mtp_enabled:
        spec_path = "none"
    return {
        "model": model,
        "model_identity": str(row_value(row, "model_identity", "model_hash", default=model_path)),
        "model_path": model_path,
        "model_file_size": int_value(row_value(row, "model_file_size", "model_size_bytes", default=args.model_file_size), args.model_file_size),
        "model_architecture": str(row_value(row, "model_architecture", default=args.model_architecture)),
        "model_hash": str(row_value(row, "model_hash", default=args.model_hash)),
        "gguf_tensor_manifest_hash": str(row_value(row, "gguf_tensor_manifest_hash", default=args.gguf_tensor_manifest_hash)),
        "tokenizer_hash": str(row_value(row, "tokenizer_hash", default=args.tokenizer_hash)),
        "chat_template_effective_hash": str(row_value(row, "chat_template_effective_hash", "chat_template_hash", default=args.chat_template_effective_hash)),
        "tools_schema_hash": str(row_value(row, "tools_schema_hash", "tools_hash", default=args.tools_schema_hash)),
        "system_prompt_hash": str(row_value(row, "system_prompt_hash", default=args.system_prompt_hash)),
        "special_token_policy": str(row_value(row, "special_token_policy", default=args.special_token_policy)),
        "llama_server_path": str(row_value(row, "llama_server_path", "llama_server", default=args.llama_server_path)),
        "llama_server_version": str(row_value(row, "llama_server_version", "runtime_version", default=args.llama_server_version)),
        "llama_cpp_source_commit": str(row_value(row, "llama_cpp_source_commit", "llama_server_commit", default=args.llama_cpp_source_commit)),
        "llama_cpp_cache_abi_version": str(row_value(row, "llama_cpp_cache_abi_version", "cache_abi_version", default=args.llama_cpp_cache_abi_version)),
        "patchset_id": str(row_value(row, "patchset_id", default=args.patchset_id)),
        "build_backend": str(row_value(row, "build_backend", default=args.build_backend)),
        "gpu_backend_driver": str(row_value(row, "gpu_backend_driver", default=args.gpu_backend_driver)),
        "kv_unified_mode": bool_value(row_value(row, "kv_unified_mode", default=args.kv_unified_mode), args.kv_unified_mode),
        "ctx_size": int_value(row_value(row, "ctx_size", default=args.ctx_size), args.ctx_size),
        "ctx_checkpoints_config": str(row_value(row, "ctx_checkpoints_config", default=args.ctx_checkpoints_config)),
        "cache_type_k": str(row_value(row, "cache_type_k", default=args.cache_type_k)),
        "cache_type_v": str(row_value(row, "cache_type_v", default=args.cache_type_v)),
        "flash_attention_mode": str(row_value(row, "flash_attention_mode", default=args.flash_attention_mode)),
        "rope_freq_base": str(row_value(row, "rope_freq_base", default=args.rope_freq_base)),
        "rope_freq_scale": str(row_value(row, "rope_freq_scale", default=args.rope_freq_scale)),
        "yarn_or_rope_scaling_metadata": str(row_value(row, "yarn_or_rope_scaling_metadata", default=args.yarn_or_rope_scaling_metadata)),
        "reasoning_format": str(row_value(row, "reasoning_format", default=args.reasoning_format)),
        "jinja_template_mode": str(row_value(row, "jinja_template_mode", default=args.jinja_template_mode)),
        "mtp_enabled": mtp_enabled,
        "spec_draft_model_identity": str(row_value(row, "spec_draft_model_identity", "mtp_model_identity", "spec_draft_model_hash", default=spec_path) if mtp_enabled else "none"),
        "spec_draft_model_path": spec_path,
        "spec_draft_model_size": int_value(
            row_value(row, "spec_draft_model_size", "mtp_model_size", "mtp_model_size_bytes", default=args.spec_draft_model_size),
            args.spec_draft_model_size,
        )
        if mtp_enabled
        else 0,
        "spec_draft_model_hash": str(row_value(row, "spec_draft_model_hash", "mtp_model_hash", default=args.spec_draft_model_hash if mtp_enabled else "none")),
        "spec_draft_config": str(row_value(row, "spec_draft_config", default=args.spec_draft_config if mtp_enabled else "none")),
        "n_parallel": int_value(row_value(row, "n_parallel", default=args.n_parallel), args.n_parallel),
        "n_seq_max": int_value(row_value(row, "n_seq_max", default=args.n_seq_max), args.n_seq_max),
    }


def load_workers(args: argparse.Namespace) -> list[WorkerRuntime]:
    if args.workers_file:
        raw = read_json(Path(args.workers_file), {})
        rows = raw.get("workers") if isinstance(raw, dict) else raw
        if not isinstance(rows, list) or not rows:
            raise ValueError("--workers-file must contain a non-empty workers list")
        workers: list[WorkerRuntime] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("each worker entry must be an object")
            worker_id = str(row.get("worker_id") or "").strip()
            url = str(row.get("url") or row.get("worker_url") or "").rstrip("/")
            slot_dir = str(row.get("slot_save_path") or row.get("worker_slot_dir") or "").rstrip("/")
            if not worker_id or not url or not slot_dir:
                raise ValueError("worker entries require worker_id, worker_url/url, and slot_save_path/worker_slot_dir")
            if worker_id in seen:
                raise ValueError(f"duplicate worker_id in workers file: {worker_id}")
            seen.add(worker_id)
            transport_row = row.get("transport") if isinstance(row.get("transport"), dict) else {}
            transport_kind = str(transport_row.get("kind") or row.get("worker_transport") or "local")
            ssh_host = str(transport_row.get("ssh_host") or row.get("worker_ssh_host") or "")
            sidecar_url = str(transport_row.get("sidecar_url") or row.get("worker_sidecar_url") or "")
            metadata = worker_metadata_from_row(row, args)
            derive_metadata = bool_value(row_value(row, "strict_metadata_auto", default=args.derive_strict_metadata), args.derive_strict_metadata)
            force_runtime_metadata = bool_value(
                row_value(row, "strict_metadata_force_runtime", "strict_metadata_runtime_override", default=args.strict_metadata_force_runtime),
                args.strict_metadata_force_runtime,
            )
            if derive_metadata:
                metadata = derive_worker_metadata_from_runtime(
                    metadata,
                    url,
                    min(args.timeout, args.strict_metadata_timeout),
                    force_runtime=force_runtime_metadata,
                )
            metadata["strict_metadata_auto"] = derive_metadata
            metadata["strict_metadata_force_runtime"] = force_runtime_metadata
            workers.append(
                WorkerRuntime(
                    worker_id=worker_id,
                    url=url,
                    slot_id=int(row.get("slot_id", args.slot_id)),
                    slot_save_path=slot_dir,
                    transport=SlotTransport(
                        worker_id=worker_id,
                        kind=transport_kind,
                        slot_dir=slot_dir,
                        ssh_host=ssh_host,
                        sidecar_url=sidecar_url,
                        ssh_config=str(transport_row.get("ssh_config") or args.ssh_config),
                        ssh_extra_args=str(transport_row.get("ssh_extra_args") or args.ssh_extra_args),
                        scp_extra_args=str(transport_row.get("scp_extra_args") or args.scp_extra_args),
                        timeout=args.timeout,
                    ),
                    **metadata,
                )
            )
        return workers

    metadata = worker_metadata_from_row({}, args)
    if args.derive_strict_metadata:
        metadata = derive_worker_metadata_from_runtime(
            metadata,
            args.worker_url.rstrip("/"),
            min(args.timeout, args.strict_metadata_timeout),
            force_runtime=args.strict_metadata_force_runtime,
        )
    metadata["strict_metadata_auto"] = args.derive_strict_metadata
    metadata["strict_metadata_force_runtime"] = args.strict_metadata_force_runtime
    return [
        WorkerRuntime(
            worker_id=args.worker_id,
            url=args.worker_url.rstrip("/"),
            slot_id=args.slot_id,
            slot_save_path=args.worker_slot_dir.rstrip("/"),
            transport=SlotTransport(
                worker_id=args.worker_id,
                kind=args.worker_transport,
                slot_dir=args.worker_slot_dir,
                ssh_host=args.worker_ssh_host,
                sidecar_url=args.worker_sidecar_url,
                ssh_config=args.ssh_config,
                ssh_extra_args=args.ssh_extra_args,
                scp_extra_args=args.scp_extra_args,
                timeout=args.timeout,
            ),
            **metadata,
        )
    ]


class CacheRouterState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cache_root = Path(args.cache_root)
        self.router_store = self.cache_root / "router-store"
        self.blobs = self.router_store / "blobs"
        self.manifests = self.router_store / "manifests"
        self.registry_path = self.router_store / "registry.json"
        self.registry_leases_path = self.router_store / "registry-leases.json"
        self.registry_lock_path = self.router_store / "registry.lock"
        self.registry_audit_path = self.router_store / "registry-audit.jsonl"
        self.registry_wal_path = self.router_store / "registry-wal.jsonl"
        self.registry_lease_ttl_seconds = max(1.0, float(getattr(args, "registry_lease_ttl_seconds", 300.0) or 300.0))
        self.registry_lease_owner_id = new_opaque_id("router")
        self.workers_lock = threading.RLock()
        self.workers = load_workers(args)
        self.worker_by_id = {worker.worker_id: worker for worker in self.workers}
        self.default_worker_id = self.workers[0].worker_id
        self.events_path = self.cache_root / "router" / "logs" / "cache-router-events.jsonl"
        self.auth_token = load_auth_token(args)
        self.encryption_at_rest = durable_blob_encryption_metadata(args)
        self.lock = threading.Lock()
        self.registry_audit_lock = threading.Lock()
        self.readiness_lock = threading.Lock()
        self.scheduler_lock = threading.Lock()
        self.readiness_poll_interval = max(0.0, float(getattr(args, "readiness_poll_interval", 1.0)))
        self.readiness_timeout = max(0.1, float(getattr(args, "readiness_timeout", 5.0)))
        self.readiness_cache: dict[str, dict[str, Any]] = {}
        self.worker_active_requests: dict[str, int] = {worker.worker_id: 0 for worker in self.workers}
        self.worker_queue_depths: dict[str, int] = {worker.worker_id: 0 for worker in self.workers}
        self.worker_queue_condition = threading.Condition(self.scheduler_lock)
        self.queue_limit_per_worker = max(0, int(getattr(args, "queue_limit_per_worker", 0) or 0))
        self.queue_wait_timeout = max(0.0, float(getattr(args, "queue_wait_timeout", 0.0) or 0.0))
        self.cold_route_cursor = 0
        self.inventory_reload_interval = max(0.0, float(getattr(args, "inventory_reload_interval", 0.0)))
        self.inventory_next_reload_monotonic = 0.0
        self.inventory_fingerprint = self.compute_inventory_fingerprint()
        self.inventory_last_loaded_at = now_iso()
        self.inventory_last_reload_at: str | None = None
        self.inventory_last_reload_error: str | None = None
        self.metrics_lock = threading.Lock()
        self.metrics: dict[str, Any] = {
            "active_requests": 0,
            "max_active_requests": 0,
            "requests_total": {},
            "errors_total": {},
            "worker_selected_total": {},
            "cache_events_total": {},
            "cache_outcomes_total": {},
            "queue_rejected_total": {},
            "request_latency_ms": [],
            "routing_decision_latency_ms": [],
            "queue_wait_ms": [],
            "ttft_ms": [],
        }
        for path in [self.blobs, self.manifests, self.events_path.parent]:
            path.mkdir(parents=True, exist_ok=True)
        if getattr(args, "rebuild_registry", False):
            self.rebuild_registry_from_manifests(reason="operator_startup_rebuild")
        elif not self.registry_path.exists():
            self.rebuild_registry_from_manifests(reason="missing_registry_startup")
        elif getattr(args, "replay_registry_wal", True):
            self.replay_registry_wal(reason="startup")
        for worker in self.workers:
            if worker.transport.kind == "local":
                worker.transport.ensure_slot_dir()
        self.readiness_stop = threading.Event()
        self.readiness_thread: threading.Thread | None = None
        if self.readiness_poll_interval > 0:
            self.readiness_thread = threading.Thread(target=self.readiness_loop, name="cache-router-readiness", daemon=True)
            self.readiness_thread.start()

    def compute_inventory_fingerprint(self) -> str | None:
        workers_file = str(getattr(self.args, "workers_file", "") or "")
        if not workers_file:
            return "single-worker-cli"
        path = Path(workers_file)
        try:
            data = path.read_bytes()
        except OSError:
            return None
        return hashlib.sha256(data).hexdigest()

    def maybe_reload_inventory(self, *, force: bool = False) -> None:
        workers_file = str(getattr(self.args, "workers_file", "") or "")
        if not workers_file or self.inventory_reload_interval <= 0:
            return
        now = time.monotonic()
        if not force and now < self.inventory_next_reload_monotonic:
            return
        self.inventory_next_reload_monotonic = now + self.inventory_reload_interval
        fingerprint = self.compute_inventory_fingerprint()
        if fingerprint is None:
            self.inventory_last_reload_error = "workers file could not be read"
            return
        if fingerprint == self.inventory_fingerprint:
            return
        try:
            workers = load_workers(self.args)
        except Exception as exc:  # noqa: BLE001
            self.inventory_last_reload_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            return
        for worker in workers:
            if worker.transport.kind == "local":
                worker.transport.ensure_slot_dir()
        with self.scheduler_lock:
            old_active = dict(self.worker_active_requests)
            old_queued = dict(self.worker_queue_depths)
        with self.workers_lock:
            self.workers = workers
            self.worker_by_id = {worker.worker_id: worker for worker in workers}
            self.default_worker_id = workers[0].worker_id
        with self.worker_queue_condition:
            self.worker_active_requests = {worker.worker_id: int(old_active.get(worker.worker_id, 0)) for worker in workers}
            self.worker_queue_depths = {worker.worker_id: int(old_queued.get(worker.worker_id, 0)) for worker in workers}
            self.worker_queue_condition.notify_all()
        with self.readiness_lock:
            self.readiness_cache.clear()
        self.inventory_fingerprint = fingerprint
        self.inventory_last_loaded_at = now_iso()
        self.inventory_last_reload_at = self.inventory_last_loaded_at
        self.inventory_last_reload_error = None

    def workers_snapshot(self) -> list[WorkerRuntime]:
        self.maybe_reload_inventory()
        with self.workers_lock:
            return list(self.workers)

    def worker_lookup(self, worker_id: str) -> WorkerRuntime | None:
        self.maybe_reload_inventory()
        with self.workers_lock:
            return self.worker_by_id.get(worker_id)

    def default_worker(self) -> WorkerRuntime:
        self.maybe_reload_inventory()
        with self.workers_lock:
            return self.workers[0]

    def default_worker_id_snapshot(self) -> str:
        with self.workers_lock:
            return self.default_worker_id

    def inventory_status(self) -> dict[str, Any]:
        self.maybe_reload_inventory()
        return {
            "workers_file": str(getattr(self.args, "workers_file", "") or ""),
            "reload_interval_seconds": self.inventory_reload_interval,
            "last_loaded_at": self.inventory_last_loaded_at,
            "last_reload_at": self.inventory_last_reload_at,
            "last_reload_error": self.inventory_last_reload_error,
        }

    def metric_key(self, labels: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((str(key), str(value)) for key, value in labels.items()))

    def inc_metric(self, bucket: str, labels: dict[str, Any], amount: int = 1) -> None:
        key = self.metric_key(labels)
        with self.metrics_lock:
            values = self.metrics.setdefault(bucket, {})
            values[key] = int(values.get(key, 0)) + amount

    def observe_metric(self, bucket: str, value: float) -> None:
        with self.metrics_lock:
            values = self.metrics.setdefault(bucket, [])
            values.append(float(value))
            if len(values) > 2000:
                del values[: len(values) - 2000]

    def begin_request(self) -> float:
        with self.metrics_lock:
            self.metrics["active_requests"] = int(self.metrics.get("active_requests", 0)) + 1
            self.metrics["max_active_requests"] = max(
                int(self.metrics.get("max_active_requests", 0)),
                int(self.metrics["active_requests"]),
            )
        return time.perf_counter()

    def finish_request(self, *, method: str, path: str, status: int, started_at: float, worker_id: str | None = None) -> None:
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        labels = {"method": method, "path": path, "status": status}
        if worker_id:
            labels["worker_id"] = worker_id
        with self.metrics_lock:
            self.metrics["active_requests"] = max(0, int(self.metrics.get("active_requests", 0)) - 1)
        self.inc_metric("requests_total", labels)
        self.observe_metric("request_latency_ms", elapsed_ms)
        if status >= 400:
            self.inc_metric("errors_total", labels)

    def record_worker_selected(self, worker_id: str, *, reason: str) -> None:
        self.inc_metric("worker_selected_total", {"worker_id": worker_id, "reason": reason})

    def worker_active_count(self, worker_id: str) -> int:
        with self.scheduler_lock:
            return int(self.worker_active_requests.get(worker_id, 0))

    def worker_queue_depth(self, worker_id: str) -> int:
        with self.scheduler_lock:
            return int(self.worker_queue_depths.get(worker_id, 0))

    def worker_capacity(self, worker: WorkerRuntime) -> int:
        try:
            n_parallel = max(1, int(worker.n_parallel))
        except (TypeError, ValueError):
            n_parallel = 1
        try:
            n_seq_max = max(1, int(worker.n_seq_max))
        except (TypeError, ValueError):
            n_seq_max = 1
        return max(1, min(n_parallel, n_seq_max))

    def begin_worker_attempt(self, worker_id: str) -> None:
        with self.scheduler_lock:
            self.worker_active_requests[worker_id] = int(self.worker_active_requests.get(worker_id, 0)) + 1

    def finish_worker_attempt(self, worker_id: str) -> None:
        with self.worker_queue_condition:
            self.worker_active_requests[worker_id] = max(0, int(self.worker_active_requests.get(worker_id, 0)) - 1)
            self.worker_queue_condition.notify_all()

    def record_queue_rejected(self, worker_id: str, *, reason: str) -> None:
        self.inc_metric("queue_rejected_total", {"worker_id": worker_id, "reason": reason})

    def acquire_worker_slot(self, worker: WorkerRuntime) -> dict[str, Any]:
        worker_id = worker.worker_id
        capacity = self.worker_capacity(worker)
        if self.queue_limit_per_worker <= 0:
            self.begin_worker_attempt(worker_id)
            return {
                "acquired": True,
                "queued": False,
                "queue_wait_ms": 0.0,
                "queue_depth_at_admit": self.worker_queue_depth(worker_id),
                "capacity": capacity,
                "reason": "queue_disabled",
            }

        started = time.perf_counter()
        with self.worker_queue_condition:
            if int(self.worker_active_requests.get(worker_id, 0)) < capacity:
                self.worker_active_requests[worker_id] = int(self.worker_active_requests.get(worker_id, 0)) + 1
                return {
                    "acquired": True,
                    "queued": False,
                    "queue_wait_ms": 0.0,
                    "queue_depth_at_admit": int(self.worker_queue_depths.get(worker_id, 0)),
                    "capacity": capacity,
                    "reason": "admitted",
                }
            if int(self.worker_queue_depths.get(worker_id, 0)) >= self.queue_limit_per_worker:
                return {
                    "acquired": False,
                    "queued": False,
                    "queue_wait_ms": 0.0,
                    "queue_depth_at_admit": int(self.worker_queue_depths.get(worker_id, 0)),
                    "capacity": capacity,
                    "reason": "queue_full",
                }
            self.worker_queue_depths[worker_id] = int(self.worker_queue_depths.get(worker_id, 0)) + 1
            queued = True
            try:
                while True:
                    active = int(self.worker_active_requests.get(worker_id, 0))
                    if active < capacity:
                        self.worker_queue_depths[worker_id] = max(0, int(self.worker_queue_depths.get(worker_id, 0)) - 1)
                        queued = False
                        self.worker_active_requests[worker_id] = active + 1
                        wait_ms = (time.perf_counter() - started) * 1000.0
                        self.observe_metric("queue_wait_ms", wait_ms)
                        return {
                            "acquired": True,
                            "queued": True,
                            "queue_wait_ms": wait_ms,
                            "queue_depth_at_admit": int(self.worker_queue_depths.get(worker_id, 0)),
                            "capacity": capacity,
                            "reason": "admitted_after_wait",
                        }
                    elapsed = time.perf_counter() - started
                    remaining = self.queue_wait_timeout - elapsed
                    if remaining <= 0:
                        wait_ms = elapsed * 1000.0
                        self.observe_metric("queue_wait_ms", wait_ms)
                        return {
                            "acquired": False,
                            "queued": True,
                            "queue_wait_ms": wait_ms,
                            "queue_depth_at_admit": int(self.worker_queue_depths.get(worker_id, 0)),
                            "capacity": capacity,
                            "reason": "queue_timeout",
                        }
                    self.worker_queue_condition.wait(timeout=remaining)
            finally:
                if queued:
                    self.worker_queue_depths[worker_id] = max(0, int(self.worker_queue_depths.get(worker_id, 0)) - 1)
                    self.worker_queue_condition.notify_all()

    def rotate_cold_candidates(self, candidates: list[WorkerRuntime]) -> tuple[list[WorkerRuntime], int]:
        if len(candidates) < 2:
            return candidates, 0
        with self.scheduler_lock:
            offset = self.cold_route_cursor % len(candidates)
            self.cold_route_cursor += 1
        return candidates[offset:] + candidates[:offset], offset

    def record_cache_event(
        self,
        *,
        phase: str,
        decision: str,
        cache_hit_level: str,
        compatibility_result: str,
        validation_status: str | None,
        fallback_required: bool,
        fallback_reason: str | None,
        hydration_latency_ms: float | None,
        restore_latency_ms: float | None,
        full_reprocess_suspected: str,
    ) -> None:
        labels = {
            "phase": phase,
            "decision": decision,
            "cache_hit_level": cache_hit_level,
            "compatibility_result": compatibility_result,
            "validation_status": validation_status or "none",
            "fallback_required": str(bool(fallback_required)).lower(),
            "fallback_reason": fallback_reason or "none",
        }
        self.inc_metric("cache_events_total", labels)
        outcomes: set[str] = set()
        if compatibility_result == "miss" or fallback_reason == "no_compatible_manifest":
            outcomes.add("miss")
        if decision == "restore_then_generate" and cache_hit_level in {"local_nvme", "durable_blob"} and full_reprocess_suspected == "no":
            outcomes.add("hit")
        if cache_hit_level == "durable_blob" and (hydration_latency_ms is not None or decision == "restore_then_generate" or fallback_reason == "hydration_failed"):
            outcomes.add("hydrate")
        if decision in {"restore_then_generate", "fallback_after_restore_failure"} or restore_latency_ms is not None:
            outcomes.add("restore")
        if fallback_required:
            outcomes.add("fallback")
        if validation_status in {"quarantined", "corrupt"}:
            outcomes.add("validation_failure")
        for outcome in sorted(outcomes):
            self.inc_metric(
                "cache_outcomes_total",
                {
                    "outcome": outcome,
                    "decision": decision,
                    "cache_hit_level": cache_hit_level,
                    "fallback_reason": fallback_reason or "none",
                    "validation_status": validation_status or "none",
                },
            )

    def render_metric_counter(self, lines: list[str], name: str, help_text: str, values: dict[Any, Any]) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} counter")
        for key, value in sorted(values.items()):
            labels = dict(key)
            lines.append(f"{name}{metric_labels(labels)} {int(value)}")

    def render_latency_summary(self, lines: list[str], name: str, help_text: str, values: list[float]) -> None:
        snapshot = list(values)
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f'{name}{metric_labels({"quantile": "p50"})} {percentile(snapshot, 50):.3f}')
        lines.append(f'{name}{metric_labels({"quantile": "p95"})} {percentile(snapshot, 95):.3f}')
        lines.append(f'{name}{metric_labels({"quantile": "p99"})} {percentile(snapshot, 99):.3f}')
        lines.append(f"{name}_count {len(snapshot)}")
        lines.append(f"{name}_sum {sum(snapshot):.3f}")

    def metrics_text(self) -> str:
        worker_rows = self.worker_summaries(include_slots=False)
        with self.metrics_lock:
            metrics = {
                "active_requests": int(self.metrics.get("active_requests", 0)),
                "max_active_requests": int(self.metrics.get("max_active_requests", 0)),
                "requests_total": dict(self.metrics.get("requests_total", {})),
                "errors_total": dict(self.metrics.get("errors_total", {})),
                "worker_selected_total": dict(self.metrics.get("worker_selected_total", {})),
                "cache_events_total": dict(self.metrics.get("cache_events_total", {})),
                "cache_outcomes_total": dict(self.metrics.get("cache_outcomes_total", {})),
                "queue_rejected_total": dict(self.metrics.get("queue_rejected_total", {})),
                "request_latency_ms": list(self.metrics.get("request_latency_ms", [])),
                "routing_decision_latency_ms": list(self.metrics.get("routing_decision_latency_ms", [])),
                "queue_wait_ms": list(self.metrics.get("queue_wait_ms", [])),
                "ttft_ms": list(self.metrics.get("ttft_ms", [])),
            }
        lines = [
            "# HELP cachy_router_active_requests In-flight requests currently handled by the router.",
            "# TYPE cachy_router_active_requests gauge",
            f"cachy_router_active_requests {metrics['active_requests']}",
            "# HELP cachy_router_max_active_requests Highest observed in-flight request count since process start.",
            "# TYPE cachy_router_max_active_requests gauge",
            f"cachy_router_max_active_requests {metrics['max_active_requests']}",
            "# HELP cachy_router_workers_configured Configured worker count.",
            "# TYPE cachy_router_workers_configured gauge",
            f"cachy_router_workers_configured {len(worker_rows)}",
            "# HELP cachy_router_workers_healthy Healthy worker count from the latest metrics scrape.",
            "# TYPE cachy_router_workers_healthy gauge",
            f"cachy_router_workers_healthy {sum(1 for row in worker_rows if row.get('health', {}).get('ok'))}",
            "# HELP cachy_router_workers_ready Worker count eligible for routing after process health and /v1/models readiness.",
            "# TYPE cachy_router_workers_ready gauge",
            f"cachy_router_workers_ready {sum(1 for row in worker_rows if row.get('readiness', {}).get('ok'))}",
            "# HELP cachy_router_worker_ready Worker readiness from the latest metrics scrape, 1 when /health and /v1/models both pass.",
            "# TYPE cachy_router_worker_ready gauge",
        ]
        for row in worker_rows:
            worker_id = row.get("worker_id", "")
            ready = 1 if row.get("readiness", {}).get("ok") else 0
            reason = row.get("availability", {}).get("reason", "unknown")
            state = row.get("readiness", {}).get("state", "unknown")
            lines.append(f"cachy_router_worker_ready{metric_labels({'worker_id': worker_id, 'state': state, 'availability': reason})} {ready}")
        lines.append("# HELP cachy_router_worker_sidecar_ready Worker sidecar readiness, 1 when the configured HTTP sidecar is ready or no sidecar is required.")
        lines.append("# TYPE cachy_router_worker_sidecar_ready gauge")
        for row in worker_rows:
            worker_id = row.get("worker_id", "")
            sidecar = row.get("sidecar_readiness") if isinstance(row.get("sidecar_readiness"), dict) else {}
            state = sidecar.get("state", "unknown")
            transport = sidecar.get("transport", "unknown")
            required = str(bool(sidecar.get("required"))).lower()
            ready = 1 if sidecar.get("ok") else 0
            lines.append(f"cachy_router_worker_sidecar_ready{metric_labels({'worker_id': worker_id, 'state': state, 'transport': transport, 'required': required})} {ready}")
        lines.append("# HELP cachy_router_worker_queue_depth Requests waiting in the router-side queue for each worker.")
        lines.append("# TYPE cachy_router_worker_queue_depth gauge")
        lines.append("# HELP cachy_router_worker_queue_capacity Configured active request capacity for each worker.")
        lines.append("# TYPE cachy_router_worker_queue_capacity gauge")
        for worker in self.workers_snapshot():
            labels = {"worker_id": worker.worker_id, "enabled": str(self.queue_limit_per_worker > 0).lower()}
            lines.append(f"cachy_router_worker_queue_depth{metric_labels(labels)} {self.worker_queue_depth(worker.worker_id)}")
            lines.append(f"cachy_router_worker_queue_capacity{metric_labels(labels)} {self.worker_capacity(worker)}")
        lines.append("# HELP cachy_router_worker_queue_limit Configured maximum router-side queue depth per worker. Zero means queueing is disabled.")
        lines.append("# TYPE cachy_router_worker_queue_limit gauge")
        lines.append(f"cachy_router_worker_queue_limit {self.queue_limit_per_worker}")
        self.render_metric_counter(lines, "cachy_router_requests_total", "Requests handled by the router.", metrics["requests_total"])
        self.render_metric_counter(lines, "cachy_router_errors_total", "Router responses with HTTP status >= 400.", metrics["errors_total"])
        self.render_metric_counter(lines, "cachy_router_worker_selected_total", "Worker selections by worker and reason.", metrics["worker_selected_total"])
        self.render_metric_counter(lines, "cachy_router_cache_events_total", "Cache decision events emitted by the router.", metrics["cache_events_total"])
        self.render_metric_counter(lines, "cachy_router_cache_outcomes_total", "Cache outcome counts for hits, misses, hydration, restore, fallback, and validation failures.", metrics["cache_outcomes_total"])
        self.render_metric_counter(lines, "cachy_router_queue_rejected_total", "Router-side queue admission rejections by worker and reason.", metrics["queue_rejected_total"])
        self.render_latency_summary(lines, "cachy_router_request_latency_ms", "Router request latency in milliseconds.", metrics["request_latency_ms"])
        self.render_latency_summary(lines, "cachy_router_routing_decision_latency_ms", "Router routing or cache decision latency in milliseconds.", metrics["routing_decision_latency_ms"])
        self.render_latency_summary(lines, "cachy_router_queue_wait_ms", "Router-side queue wait in milliseconds for queued requests.", metrics["queue_wait_ms"])
        self.render_latency_summary(lines, "cachy_router_ttft_ms", "Router-observed or backend-reported time to first token in milliseconds.", metrics["ttft_ms"])
        return "\n".join(lines) + "\n"

    def worker_readiness(self, worker: WorkerRuntime, *, refresh: bool = False, include_sidecar: bool = True) -> dict[str, Any]:
        now = time.monotonic()
        with self.readiness_lock:
            cached = self.readiness_cache.get(worker.worker_id)
            if (
                cached is not None
                and not refresh
                and self.readiness_poll_interval > 0
                and now - float(cached.get("_checked_monotonic", 0.0)) < self.readiness_poll_interval
            ):
                sidecar = cached.get("sidecar_readiness") if isinstance(cached.get("sidecar_readiness"), dict) else {}
                if not include_sidecar or sidecar.get("checked") is True or sidecar.get("state") == "not_applicable":
                    return {key: value for key, value in cached.items() if not key.startswith("_")}
        health = worker.health(timeout=self.readiness_timeout)
        model_readiness = worker.model_readiness(timeout=self.readiness_timeout) if health.get("ok") else {
            "ok": False,
            "state": "process_unhealthy",
            "expected_model": worker.model,
            "model_ids": [],
            "http_status": None,
            "skipped": True,
        }
        if include_sidecar:
            sidecar_readiness = worker.sidecar_readiness(timeout=self.readiness_timeout)
        else:
            sidecar_readiness = {
                "ok": None,
                "state": "not_checked",
                "transport": worker.transport.kind,
                "required": worker.transport.kind == "http",
                "checked": False,
            }
        if not health.get("ok"):
            state = "process_unhealthy"
        else:
            state = str(model_readiness.get("state") or ("ready" if model_readiness.get("ok") else "not_ready"))
        row = {
            "_checked_monotonic": now,
            "checked_at": now_iso(),
            "poll_interval_seconds": self.readiness_poll_interval,
            "worker_id": worker.worker_id,
            "health": health,
            "model_readiness": model_readiness,
            "sidecar_readiness": sidecar_readiness,
            "ok": bool(health.get("ok") and model_readiness.get("ok")),
            "state": state,
        }
        with self.readiness_lock:
            self.readiness_cache[worker.worker_id] = row
        return {key: value for key, value in row.items() if not key.startswith("_")}

    def poll_readiness_once(self) -> None:
        for worker in self.workers_snapshot():
            self.worker_readiness(worker, refresh=True, include_sidecar=True)

    def readiness_loop(self) -> None:
        while not self.readiness_stop.is_set():
            self.poll_readiness_once()
            self.readiness_stop.wait(self.readiness_poll_interval)

    def close(self) -> None:
        self.readiness_stop.set()
        if self.readiness_thread is not None:
            self.readiness_thread.join(timeout=1.0)

    def select_worker(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = False,
        model: str | None = None,
        scheduler_trace: dict[str, Any] | None = None,
    ) -> WorkerRuntime:
        candidates = self.candidate_workers(
            preferred_worker_id,
            prefer_residency=prefer_residency,
            allow_fallback=allow_fallback,
            model=model,
            scheduler_trace=scheduler_trace,
        )
        if not candidates:
            target = f" for model {model}" if model else ""
            raise RuntimeError(f"no ready cache-router worker available{target}")
        return candidates[0]

    def ordered_workers(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = True,
        model: str | None = None,
    ) -> list[WorkerRuntime]:
        workers = self.workers_snapshot()
        if preferred_worker_id:
            worker = next((row for row in workers if row.worker_id == preferred_worker_id), None)
            if worker is None:
                raise KeyError(f"unknown worker_id: {preferred_worker_id}")
            candidates = [worker]
            if allow_fallback:
                candidates.extend(w for w in workers if w.worker_id != preferred_worker_id)
        elif prefer_residency:
            hot = [worker for worker in workers if prefer_residency.get(worker.worker_id) is True]
            cold = [worker for worker in workers if worker.worker_id not in {w.worker_id for w in hot}]
            candidates = hot + cold
        else:
            candidates = list(workers)
        if model is not None:
            candidates = [worker for worker in candidates if worker.model == model]
        return candidates

    def rank_worker(
        self,
        worker: WorkerRuntime,
        availability: dict[str, Any],
        ordered_index: dict[str, int],
        preferred_worker_id: str | None,
        prefer_residency: dict[str, Any] | None,
        active_requests: int,
        queue_depth: int,
    ) -> tuple[int, int, int, int, int, int]:
        busy_score = int(availability.get("busy_score", 1))
        active_score = max(0, int(active_requests))
        queue_score = max(0, int(queue_depth))
        preferred_score = 0 if preferred_worker_id and worker.worker_id == preferred_worker_id else 1
        hot_score = 0 if prefer_residency and prefer_residency.get(worker.worker_id) is True else 1
        index_score = ordered_index.get(worker.worker_id, len(ordered_index))
        if preferred_worker_id:
            return (busy_score, active_score, queue_score, preferred_score, hot_score, index_score)
        return (busy_score, active_score, queue_score, hot_score, preferred_score, index_score)

    def candidate_workers(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = True,
        model: str | None = None,
        rotate_cold: bool = False,
        scheduler_trace: dict[str, Any] | None = None,
    ) -> list[WorkerRuntime]:
        candidates = self.ordered_workers(preferred_worker_id, prefer_residency=prefer_residency, allow_fallback=allow_fallback, model=model)
        rotation_offset = 0
        if rotate_cold and preferred_worker_id is None and not prefer_residency:
            candidates, rotation_offset = self.rotate_cold_candidates(candidates)
        ordered_index = {worker.worker_id: index for index, worker in enumerate(candidates)}
        healthy: list[tuple[WorkerRuntime, dict[str, Any], tuple[int, int, int, int, int, int]]] = []
        trace_rows: list[dict[str, Any]] = []
        last_readiness: dict[str, Any] | None = None
        for worker in candidates:
            readiness = self.worker_readiness(worker, include_sidecar=False)
            last_readiness = readiness
            if readiness.get("ok"):
                availability = worker.availability()
                active_requests = self.worker_active_count(worker.worker_id)
                queue_depth = self.worker_queue_depth(worker.worker_id)
                if not routable_availability(availability):
                    last_readiness = {**readiness, "availability": availability}
                    trace_rows.append(
                        {
                            "worker_id": worker.worker_id,
                            "eligible": False,
                            "readiness_state": readiness.get("state"),
                            "availability_reason": availability.get("reason"),
                            "busy_score": int(availability.get("busy_score", 1)),
                            "active_requests": active_requests,
                            "queue_depth": queue_depth,
                            "queue_capacity": self.worker_capacity(worker),
                            "preferred": bool(preferred_worker_id and worker.worker_id == preferred_worker_id),
                            "hot_residency": bool(prefer_residency and prefer_residency.get(worker.worker_id) is True),
                            "order_index": ordered_index.get(worker.worker_id),
                            "rank": None,
                        }
                    )
                    continue
                rank = self.rank_worker(worker, availability, ordered_index, preferred_worker_id, prefer_residency, active_requests, queue_depth)
                healthy.append((worker, availability, rank))
                trace_rows.append(
                    {
                        "worker_id": worker.worker_id,
                        "eligible": True,
                        "readiness_state": readiness.get("state"),
                        "availability_reason": availability.get("reason"),
                        "busy_score": int(availability.get("busy_score", 1)),
                        "active_requests": active_requests,
                        "queue_depth": queue_depth,
                        "queue_capacity": self.worker_capacity(worker),
                        "preferred": bool(preferred_worker_id and worker.worker_id == preferred_worker_id),
                        "hot_residency": bool(prefer_residency and prefer_residency.get(worker.worker_id) is True),
                        "order_index": ordered_index.get(worker.worker_id),
                        "rank": list(rank),
                    }
                )
            else:
                trace_rows.append(
                    {
                        "worker_id": worker.worker_id,
                        "eligible": False,
                        "readiness_state": readiness.get("state"),
                        "availability_reason": "not_ready",
                        "busy_score": None,
                        "active_requests": self.worker_active_count(worker.worker_id),
                        "queue_depth": self.worker_queue_depth(worker.worker_id),
                        "queue_capacity": self.worker_capacity(worker),
                        "preferred": bool(preferred_worker_id and worker.worker_id == preferred_worker_id),
                        "hot_residency": bool(prefer_residency and prefer_residency.get(worker.worker_id) is True),
                        "order_index": ordered_index.get(worker.worker_id),
                        "rank": None,
                    }
                )
        if scheduler_trace is not None:
            scheduler_trace.update(
                {
                    "policy": "availability_active_round_robin_v1",
                    "model": model,
                    "preferred_worker_id": preferred_worker_id,
                    "allow_fallback": allow_fallback,
                    "prefer_residency": sorted(
                        worker_id for worker_id, present in (prefer_residency or {}).items() if present is True
                    ),
                    "rotation_offset": rotation_offset,
                    "rank_fields": ["busy_score", "active_requests", "queue_depth", "hot_residency", "preferred_worker", "order_index"]
                    if not preferred_worker_id
                    else ["busy_score", "active_requests", "queue_depth", "preferred_worker", "hot_residency", "order_index"],
                    "candidates": trace_rows,
                }
            )
        if not healthy:
            target = f" for model {model}" if model else ""
            raise RuntimeError(f"no ready cache-router worker available{target}; last_readiness={last_readiness}")
        healthy.sort(key=lambda item: item[2])
        if scheduler_trace is not None:
            winner = healthy[0]
            scheduler_trace["winner_worker_id"] = winner[0].worker_id
            scheduler_trace["winner_rank"] = list(winner[2])
            scheduler_trace["winner_reason"] = (
                f"lowest rank by busy_score={winner[2][0]}, active_requests={winner[2][1]}, queue_depth={winner[2][2]}, "
                f"availability={winner[1].get('reason', 'unknown')}"
            )
        return [worker for worker, _, _ in healthy]

    def worker_health(self, worker: WorkerRuntime | None = None) -> dict[str, Any]:
        return (worker or self.default_worker()).health()

    def worker_summaries(self, *, include_slots: bool = True) -> list[dict[str, Any]]:
        return [worker.summary(self, include_slots=include_slots) for worker in self.workers_snapshot()]

    def worker_summary(self) -> dict[str, Any]:
        return self.default_worker().summary(self)

    def configured_model_ids(self) -> list[str]:
        models = {self.args.model}
        models.update(worker.model for worker in self.workers_snapshot() if worker.model)
        return sorted(model for model in models if model)

    def ready_model_ids(self) -> list[str]:
        models = {
            worker.model
            for worker in self.workers_snapshot()
            if worker.model and self.worker_readiness(worker, include_sidecar=False).get("ok") and routable_availability(worker.availability())
        }
        return sorted(models)

    def has_model(self, model: str) -> bool:
        return model in set(self.configured_model_ids())

    def append_registry_wal(self, *, operation: str, outcome: str, reason: str | None = None, **fields: Any) -> None:
        row = {
            "schema_version": SCHEMA_VERSION,
            "event_id": "registry-wal-" + hashlib.sha256(f"{time.time_ns()}:{operation}".encode()).hexdigest()[:16],
            "timestamp": now_iso(),
            "operation": operation,
            "outcome": outcome,
            "reason": str(reason)[:200] if reason else None,
        }
        for key in [
            "cache_id",
            "cache_key_hash",
            "manifest_id",
            "worker_id",
            "entry_count",
            "manifest_count",
            "skipped_count",
            "pending_count",
            "recovered_count",
        ]:
            if key in fields:
                row[key] = fields[key]
        append_jsonl(self.registry_wal_path, row)

    def read_registry_wal_rows(self) -> list[dict[str, Any]]:
        if not self.registry_wal_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with self.registry_wal_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
        return rows

    def replay_registry_wal(self, *, reason: str) -> dict[str, Any]:
        rows = self.read_registry_wal_rows()
        if not rows:
            return {"action": "none", "reason": "wal_empty"}
        pending_prepared: dict[str, dict[str, Any]] = {}
        for row in rows:
            operation = str(row.get("operation") or "")
            outcome = str(row.get("outcome") or "")
            cache_key_hash = row.get("cache_key_hash")
            key = str(cache_key_hash) if is_sha256_hex(cache_key_hash) else ""
            if operation in {"manifest_prepare", "manifest_prepared"} and outcome in {"start", "success"} and key:
                pending_prepared[key] = row
            elif operation in {"registry_commit", "registry_committed"} and outcome == "success" and key:
                pending_prepared.pop(key, None)
            elif operation == "rebuild_registry" and outcome == "success":
                pending_prepared.clear()

        registry_valid = False
        registry_keys: set[str] = set()
        if self.registry_path.exists():
            try:
                registry = read_json(self.registry_path, {"entries": []})
                entries = registry.get("entries") if isinstance(registry, dict) else None
                if isinstance(entries, list):
                    registry_valid = True
                    registry_keys = {str(row.get("cache_key_hash")) for row in entries if isinstance(row, dict) and is_sha256_hex(row.get("cache_key_hash"))}
            except (OSError, json.JSONDecodeError):
                registry_valid = False

        pending_keys = {
            key
            for key in pending_prepared
            if (self.manifests / f"{key}.json").is_file() and key not in registry_keys
        }
        if registry_valid and not pending_keys:
            self.append_registry_wal(
                operation="wal_replay",
                outcome="noop",
                reason=reason,
                pending_count=len(pending_prepared),
                recovered_count=0,
            )
            return {"action": "noop", "pending_count": len(pending_prepared), "recovered_count": 0}

        rebuilt = self.rebuild_registry_from_manifests(reason=f"wal_replay_{reason}")
        recovered = len(
            {
                str(row.get("cache_key_hash"))
                for row in rebuilt.get("entries", [])
                if isinstance(row, dict) and str(row.get("cache_key_hash")) in set(pending_prepared)
            }
        )
        self.append_registry_wal(
            operation="wal_replay",
            outcome="rebuilt",
            reason=reason,
            pending_count=len(pending_prepared),
            recovered_count=recovered,
        )
        return {"action": "rebuilt", "pending_count": len(pending_prepared), "recovered_count": recovered}

    def manifest_blob_path(self, manifest: dict[str, Any], expected_hash: str) -> Path:
        content_addressed_blob_path = self.blobs / expected_hash[:2] / f"{expected_hash}.slot"
        if content_addressed_blob_path.is_file():
            return content_addressed_blob_path
        manifest_blob_path = Path(str(manifest.get("router_blob_path") or ""))
        if manifest_blob_path and not manifest_blob_path.is_absolute():
            manifest_blob_path = self.cache_root / manifest_blob_path
        try:
            resolved_blob_path = manifest_blob_path.resolve()
            resolved_store = self.router_store.resolve()
            if resolved_store in resolved_blob_path.parents:
                return manifest_blob_path
        except Exception:  # noqa: BLE001
            pass
        return content_addressed_blob_path

    def manifest_blob_valid(self, manifest: dict[str, Any], *, expected_hash: str, expected_size: int) -> bool:
        blob_path = self.manifest_blob_path(manifest, expected_hash)
        if not blob_path.is_file():
            return False
        try:
            if blob_path.stat().st_size != expected_size:
                return False
            return sha256_file(blob_path) == expected_hash
        except OSError:
            return False

    def registry_entry_from_manifest(self, manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(manifest, dict):
            return None
        if str(manifest.get("validation_status") or "") in INACTIVE_MANIFEST_STATUSES:
            return None
        required = [
            "cache_id",
            "cache_key_hash",
            "manifest_id",
            "scope",
            "tenant_hash",
            "policy_id_hash",
            "slot_file_sha256",
            "slot_file_size_bytes",
            "slot_filename",
        ]
        if any(manifest.get(field) in (None, "") for field in required):
            return None
        try:
            canonical_key = cache_key_hash_from_record(manifest, label=f"registry rebuild manifest {manifest_path.name}")
        except RuntimeError:
            return None
        if manifest.get("cache_key_hash") != canonical_key:
            return None
        try:
            slot_size = int(manifest.get("slot_file_size_bytes"))
        except (TypeError, ValueError):
            return None
        if slot_size <= 0:
            return None
        slot_hash = str(manifest.get("slot_file_sha256") or "")
        if not is_sha256_hex(slot_hash):
            return None
        if not self.manifest_blob_valid(manifest, expected_hash=slot_hash, expected_size=slot_size):
            return None
        return {
            "cache_id": manifest["cache_id"],
            "cache_key_hash": manifest["cache_key_hash"],
            "manifest_id": manifest["manifest_id"],
            "manifest_path": str(manifest_path),
            "scope": manifest.get("scope"),
            "tenant_hash": manifest.get("tenant_hash"),
            "conversation_hash": manifest.get("conversation_hash"),
            "policy_id_hash": manifest.get("policy_id_hash"),
            "router_blob_path": manifest.get("router_blob_path"),
            "slot_filename": manifest.get("slot_filename"),
            "slot_file_sha256": manifest.get("slot_file_sha256"),
            "slot_file_size_bytes": slot_size,
            "strict_key_fields": {field: manifest[field] for field in STRICT_COMPATIBILITY_FIELDS if field in manifest},
            "created_at": manifest.get("created_at"),
            "last_used_at": manifest.get("last_used_at"),
            "source_worker_id": manifest.get("source_worker_id"),
            "validation_status": manifest.get("validation_status") or "validated",
            "encryption_at_rest": manifest.get("encryption_at_rest"),
            "worker_residency": manifest.get("worker_residency") if isinstance(manifest.get("worker_residency"), dict) else {},
        }

    def rebuild_registry_from_manifests(self, *, reason: str) -> dict[str, Any]:
        with self.registry_file_lock():
            entries: list[dict[str, Any]] = []
            skipped = 0
            for manifest_path in sorted(self.manifests.glob("*.json")):
                try:
                    manifest = read_json(manifest_path, {})
                except (OSError, json.JSONDecodeError):
                    skipped += 1
                    continue
                entry = self.registry_entry_from_manifest(manifest_path, manifest)
                if entry is None:
                    skipped += 1
                    continue
                entries.append(entry)
            registry = {
                "schema_version": SCHEMA_VERSION,
                "rebuilt_at": now_iso(),
                "rebuild_reason": reason,
                "entries": entries,
            }
            self.save_registry(registry)
            self.append_registry_wal(
                operation="rebuild_registry",
                outcome="success",
                reason=reason,
                entry_count=len(entries),
                manifest_count=len(list(self.manifests.glob("*.json"))),
                skipped_count=skipped,
            )
            return registry

    def load_registry(self) -> dict[str, Any]:
        if not self.registry_path.exists() and getattr(self.args, "auto_rebuild_registry", True):
            return self.rebuild_registry_from_manifests(reason="missing_registry")
        try:
            registry = read_json(self.registry_path, {"schema_version": SCHEMA_VERSION, "entries": []})
        except json.JSONDecodeError:
            if getattr(self.args, "auto_rebuild_registry", True):
                self.append_registry_wal(operation="load_registry", outcome="invalid_json", reason="auto_rebuild")
                return self.rebuild_registry_from_manifests(reason="invalid_registry_json")
            raise
        if not isinstance(registry, dict) or not isinstance(registry.get("entries"), list):
            if getattr(self.args, "auto_rebuild_registry", True):
                self.append_registry_wal(operation="load_registry", outcome="invalid_shape", reason="auto_rebuild")
                return self.rebuild_registry_from_manifests(reason="invalid_registry_shape")
            raise RuntimeError("registry.json must be an object with an entries list")
        return registry

    def save_registry(self, registry: dict[str, Any]) -> None:
        registry["schema_version"] = SCHEMA_VERSION
        registry["updated_at"] = now_iso()
        write_json(self.registry_path, registry)

    @contextmanager
    def registry_file_lock(self) -> Any:
        self.registry_lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.registry_lock_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def load_registry_leases_unlocked(self) -> dict[str, Any]:
        data = read_json(self.registry_leases_path, {"schema_version": SCHEMA_VERSION, "leases": []})
        if not isinstance(data, dict):
            data = {"schema_version": SCHEMA_VERSION, "leases": []}
        leases = [lease for lease in data.get("leases", []) if isinstance(lease, dict)]
        data["schema_version"] = str(data.get("schema_version") or SCHEMA_VERSION)
        data["leases"] = leases
        return data

    def save_registry_leases_unlocked(self, leases: dict[str, Any]) -> None:
        leases["schema_version"] = SCHEMA_VERSION
        leases["updated_at"] = now_iso()
        write_json(self.registry_leases_path, leases)

    def load_registry_leases(self) -> dict[str, Any]:
        with self.registry_file_lock():
            leases = self.load_registry_leases_unlocked()
            pruned = self.prune_expired_registry_leases_unlocked(leases, now_unix=time.time())
            if pruned:
                self.save_registry_leases_unlocked(leases)
            return leases

    def prune_expired_registry_leases_unlocked(self, leases: dict[str, Any], *, now_unix: float) -> int:
        active: list[dict[str, Any]] = []
        expired = 0
        for lease in leases.get("leases", []):
            if not isinstance(lease, dict):
                expired += 1
                continue
            try:
                expires_at_unix = float(lease.get("expires_at_unix"))
            except (TypeError, ValueError):
                expired += 1
                continue
            if expires_at_unix <= now_unix:
                expired += 1
                continue
            active.append(lease)
        leases["leases"] = active
        return expired

    def prune_expired_registry_leases(self) -> int:
        with self.registry_file_lock():
            leases = self.load_registry_leases_unlocked()
            pruned = self.prune_expired_registry_leases_unlocked(leases, now_unix=time.time())
            if pruned:
                self.save_registry_leases_unlocked(leases)
            return pruned

    def acquire_registry_lease(
        self,
        *,
        operation: str,
        cache_id: str,
        cache_key_hash: str,
        manifest_id: str | None = None,
        worker_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        owner_id: str | None = None,
        ttl_seconds: float | None = None,
    ) -> dict[str, Any]:
        if not is_sha256_hex(cache_key_hash):
            raise RuntimeError("registry lease cache_key_hash must be a sha256 hex string")
        owner = owner_id or self.registry_lease_owner_id
        ttl = max(0.001, float(ttl_seconds if ttl_seconds is not None else self.registry_lease_ttl_seconds))
        now_unix = time.time()
        created_at = now_iso()
        expires_at_unix = now_unix + ttl
        with self.registry_file_lock():
            leases = self.load_registry_leases_unlocked()
            pruned = self.prune_expired_registry_leases_unlocked(leases, now_unix=now_unix)
            for lease in leases.get("leases", []):
                if lease.get("cache_key_hash") == cache_key_hash and lease.get("owner_id") != owner:
                    if pruned:
                        self.save_registry_leases_unlocked(leases)
                    raise RegistryLeaseConflictError(operation, cache_key_hash, lease)
            lease = {
                "schema_version": SCHEMA_VERSION,
                "lease_id": new_opaque_id("lease"),
                "owner_id": owner,
                "operation": str(operation)[:80],
                "cache_id": str(cache_id)[:120],
                "cache_key_hash": cache_key_hash,
                "manifest_id": str(manifest_id)[:120] if manifest_id is not None else None,
                "worker_id": str(worker_id)[:120] if worker_id is not None else None,
                "request_id": request_id,
                "trace_id": trace_id or request_id,
                "created_at": created_at,
                "created_at_unix": now_unix,
                "expires_at": datetime.fromtimestamp(expires_at_unix, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "expires_at_unix": expires_at_unix,
                "ttl_seconds": ttl,
            }
            leases.setdefault("leases", []).append(lease)
            self.save_registry_leases_unlocked(leases)
            return lease

    def release_registry_lease(self, lease: dict[str, Any] | None) -> bool:
        if not isinstance(lease, dict):
            return False
        lease_id = lease.get("lease_id")
        owner_id = lease.get("owner_id")
        if not lease_id:
            return False
        with self.registry_file_lock():
            leases = self.load_registry_leases_unlocked()
            before = len(leases.get("leases", []))
            leases["leases"] = [
                row
                for row in leases.get("leases", [])
                if not (isinstance(row, dict) and row.get("lease_id") == lease_id and (owner_id is None or row.get("owner_id") == owner_id))
            ]
            removed = len(leases.get("leases", [])) != before
            if removed:
                self.save_registry_leases_unlocked(leases)
            return removed

    def quarantine_cache_entry(
        self,
        entry: dict[str, Any],
        manifest: dict[str, Any],
        *,
        reason: str,
        request_id: str | None = None,
        trace_id: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        manifest_path = Path(entry["manifest_path"])
        now = now_iso()
        worker_ids = set()
        with self.workers_lock:
            worker_ids.update(worker.worker_id for worker in self.workers)
        for source in (manifest.get("worker_residency"), entry.get("worker_residency")):
            if isinstance(source, dict):
                worker_ids.update(str(worker_id) for worker_id in source)
        cleared_residency = {worker_id: False for worker_id in sorted(worker_ids)}
        manifest["validation_status"] = "quarantined"
        manifest["quarantine_reason"] = str(reason)[:80]
        manifest["quarantined_at"] = now
        manifest["worker_residency"] = cleared_residency
        self.append_registry_wal(
            operation="quarantine_prepare",
            outcome="start",
            reason=str(reason)[:80],
            cache_id=entry.get("cache_id") or manifest.get("cache_id"),
            cache_key_hash=entry.get("cache_key_hash") or manifest.get("cache_key_hash"),
            manifest_id=entry.get("manifest_id") or manifest.get("manifest_id"),
            worker_id=worker_id,
        )
        write_json(manifest_path, manifest)
        registry = self.load_registry()
        for row in registry.get("entries", []):
            if row.get("cache_key_hash") == entry.get("cache_key_hash") or row.get("manifest_path") == entry.get("manifest_path"):
                row["validation_status"] = "quarantined"
                row["quarantine_reason"] = str(reason)[:80]
                row["quarantined_at"] = now
                row["worker_residency"] = dict(cleared_residency)
        self.save_registry(registry)
        self.append_registry_wal(
            operation="quarantine_commit",
            outcome="quarantined",
            reason=str(reason)[:80],
            cache_id=entry.get("cache_id") or manifest.get("cache_id"),
            cache_key_hash=entry.get("cache_key_hash") or manifest.get("cache_key_hash"),
            manifest_id=entry.get("manifest_id") or manifest.get("manifest_id"),
            worker_id=worker_id,
        )
        self.emit_registry_audit_event(
            operation="quarantine",
            outcome="quarantined",
            audit_actions=["commit"],
            request_id=request_id,
            trace_id=trace_id,
            cache_id=entry.get("cache_id") or manifest.get("cache_id"),
            cache_key_hash=entry.get("cache_key_hash") or manifest.get("cache_key_hash"),
            manifest_id=entry.get("manifest_id") or manifest.get("manifest_id"),
            worker_id=worker_id,
            tenant_hash=entry.get("tenant_hash") or manifest.get("tenant_hash"),
            conversation_hash=entry.get("conversation_hash") or manifest.get("conversation_hash"),
            scope=entry.get("scope") or manifest.get("scope"),
            reason=str(reason)[:80],
            source="registry_mutation",
        )

    def find_entry(
        self,
        cache_id: str,
        *,
        cache_policy: dict[str, Any] | None = None,
        expected_cache_key_hash: str | None = None,
    ) -> dict[str, Any] | None:
        registry = self.load_registry()
        matches: list[tuple[int, dict[str, Any]]] = []
        for index, entry in enumerate(registry.get("entries", [])):
            if not isinstance(entry, dict):
                continue
            if entry.get("cache_id") != cache_id:
                continue
            if expected_cache_key_hash is not None and entry.get("cache_key_hash") != expected_cache_key_hash:
                continue
            if cache_policy is not None and cache_policy_denial_reason(cache_policy, entry):
                continue
            matches.append((index, entry))
        if not matches:
            return None

        def rank(row: tuple[int, dict[str, Any]]) -> tuple[int, str, str, int]:
            index, entry = row
            validation_rank = 0 if is_quarantined_cache_status(entry.get("validation_status")) else 1
            last_used = str(entry.get("last_used_at") or "")
            created = str(entry.get("created_at") or "")
            return validation_rank, last_used, created, index

        return max(matches, key=rank)[1]

    def cache_key_fields(self, cache_id: str, prefix_text: str, worker: WorkerRuntime, *, cache_policy: dict[str, Any] | None = None) -> dict[str, Any]:
        active_policy = cache_policy or default_cache_policy()
        prefix_tokens = token_count(worker.url, prefix_text, self.args.timeout)
        fields = {
            "cache_id": cache_id,
            "scope": active_policy["scope"],
            "tenant_hash": active_policy["tenant_hash"],
            "conversation_hash": active_policy.get("conversation_hash"),
            "policy_id_hash": active_policy["policy_id_hash"],
            "prefix_sha256": sha256_text(prefix_text),
            "prefix_token_count": prefix_tokens,
            "model_identity": worker.model_identity,
            "model_file_size": worker.model_file_size,
            "llama_server_version": worker.llama_server_version,
            "ctx_size": worker.ctx_size,
            "cache_type_k": worker.cache_type_k,
            "cache_type_v": worker.cache_type_v,
            "mtp_enabled": worker.mtp_enabled,
            "spec_draft_model_identity": worker.spec_draft_model_identity,
            "spec_draft_model_size": worker.spec_draft_model_size,
        }
        fields.update(self.strict_cache_key_fields(worker))
        return fields

    def request_cache_key_candidates(
        self,
        *,
        cache_id: str,
        prefix_text: str,
        cache_policy: dict[str, Any],
        worker_id: str | None = None,
        model: str | None = None,
        allow_fallback: bool = True,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        errors: list[str] = []
        seen: set[str] = set()
        for worker in self.ordered_workers(worker_id, allow_fallback=allow_fallback, model=model):
            try:
                key_fields = self.cache_key_fields(cache_id, prefix_text, worker, cache_policy=cache_policy)
                cache_key_hash = cache_key_hash_from_record(key_fields, label="request-derived cache key")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{worker.worker_id}: {type(exc).__name__}: {exc}")
                continue
            if cache_key_hash in seen:
                continue
            seen.add(cache_key_hash)
            candidates.append({"worker_id": worker.worker_id, "cache_key_hash": cache_key_hash})
        if not candidates:
            detail = "; ".join(errors[:3]) if errors else "no ready compatible worker candidates"
            raise RuntimeError(f"request strict cache key derivation failed: {detail}")
        return candidates

    def strict_cache_key_fields(self, worker: WorkerRuntime) -> dict[str, Any]:
        fields = {
            "model_architecture": worker.model_architecture,
            "model_hash": worker.model_hash,
            "gguf_tensor_manifest_hash": worker.gguf_tensor_manifest_hash,
            "tokenizer_hash": worker.tokenizer_hash,
            "chat_template_effective_hash": worker.chat_template_effective_hash,
            "tools_schema_hash": worker.tools_schema_hash,
            "system_prompt_hash": worker.system_prompt_hash,
            "special_token_policy": worker.special_token_policy,
            "llama_cpp_source_commit": worker.llama_cpp_source_commit,
            "llama_cpp_cache_abi_version": worker.llama_cpp_cache_abi_version,
            "patchset_id": worker.patchset_id,
            "build_backend": worker.build_backend,
            "gpu_backend_driver": worker.gpu_backend_driver,
            "kv_unified_mode": worker.kv_unified_mode,
            "ctx_checkpoints_config": worker.ctx_checkpoints_config,
            "flash_attention_mode": worker.flash_attention_mode,
            "rope_freq_base": worker.rope_freq_base,
            "rope_freq_scale": worker.rope_freq_scale,
            "yarn_or_rope_scaling_metadata": worker.yarn_or_rope_scaling_metadata,
            "reasoning_format": worker.reasoning_format,
            "jinja_template_mode": worker.jinja_template_mode,
            "spec_draft_model_hash": worker.spec_draft_model_hash if worker.mtp_enabled else "none",
            "spec_draft_config": worker.spec_draft_config if worker.mtp_enabled else "none",
            "n_parallel": worker.n_parallel,
            "n_seq_max": worker.n_seq_max,
        }
        errors = [error for field, value in fields.items() if (error := strict_value_error(field, value, mtp_enabled=worker.mtp_enabled))]
        if errors:
            raise RuntimeError("strict cache key incomplete: " + "; ".join(errors))
        return fields

    def worker_cache_compatibility_mismatch(self, manifest: dict[str, Any], worker: WorkerRuntime) -> str | None:
        expected = {
            "model_identity": worker.model_identity,
            "model_file_size": worker.model_file_size,
            "llama_server_version": worker.llama_server_version,
            "ctx_size": worker.ctx_size,
            "cache_type_k": worker.cache_type_k,
            "cache_type_v": worker.cache_type_v,
            "mtp_enabled": worker.mtp_enabled,
            "spec_draft_model_identity": worker.spec_draft_model_identity,
            "spec_draft_model_size": worker.spec_draft_model_size,
        }
        try:
            expected.update(self.strict_cache_key_fields(worker))
        except RuntimeError as exc:
            return str(exc)
        for field, worker_value in expected.items():
            manifest_value = manifest.get(field)
            if not worker.mtp_enabled and field == "spec_draft_model_identity":
                if manifest_value not in (None, "", worker_value):
                    return f"{field} mismatch: manifest={manifest_value!r} worker={worker_value!r}"
                continue
            if not worker.mtp_enabled and field == "spec_draft_model_size":
                try:
                    manifest_size = int(manifest_value or 0)
                except (TypeError, ValueError):
                    return f"{field} mismatch: manifest={manifest_value!r} worker={worker_value!r}"
                if manifest_size not in {0, worker_value}:
                    return f"{field} mismatch: manifest={manifest_value!r} worker={worker_value!r}"
                continue
            strict_error = strict_value_error(field, manifest_value, mtp_enabled=worker.mtp_enabled) if field in STRICT_COMPATIBILITY_FIELDS else None
            if strict_error:
                return strict_error
            if manifest_value in (None, ""):
                return f"manifest missing {field}"
            if manifest_value != worker_value:
                return f"{field} mismatch: manifest={manifest_value!r} worker={worker_value!r}"
        return None

    def emit_registry_audit_event(
        self,
        *,
        operation: str,
        outcome: str,
        audit_actions: list[str],
        request_id: str | None,
        trace_id: str | None,
        cache_id: Any,
        cache_key_hash: Any,
        manifest_id: Any,
        worker_id: Any = None,
        tenant_hash: Any = None,
        conversation_hash: Any = None,
        scope: Any = None,
        reason: str | None = None,
        source: str = "daemon",
        source_event_id: str | None = None,
    ) -> None:
        bounded_actions = [action for action in ["lookup", "hit", "miss", "restore", "commit", "fallback", "denial"] if action in set(audit_actions)]
        if not bounded_actions:
            return
        operation_key = f"{operation}:{request_id or ''}:{trace_id or ''}:{cache_id or ''}:{cache_key_hash or ''}:{manifest_id or ''}"
        operation_id = "op-" + hashlib.sha256(operation_key.encode()).hexdigest()[:16]
        event_key = f"{time.time_ns()}:{operation_id}"
        row = {
            "schema_version": SCHEMA_VERSION,
            "event_id": "registry-audit-" + hashlib.sha256(event_key.encode()).hexdigest()[:16],
            "operation_id": operation_id,
            "timestamp": now_iso(),
            "actor": "cache-router-daemon",
            "source": source,
            "source_event_id": source_event_id,
            "operation": str(operation)[:80],
            "outcome": str(outcome)[:80],
            "audit_actions": bounded_actions,
            "request_id": request_id,
            "trace_id": trace_id or request_id,
            "cache_id": str(cache_id)[:120] if cache_id is not None else None,
            "cache_key_hash": cache_key_hash if is_sha256_hex(cache_key_hash) else None,
            "manifest_id": str(manifest_id)[:120] if manifest_id is not None else None,
            "worker_id": str(worker_id)[:120] if worker_id is not None else None,
            "tenant_hash": tenant_hash if is_sha256_hex(tenant_hash) else None,
            "conversation_hash": conversation_hash if is_sha256_hex(conversation_hash) else None,
            "scope": str(scope)[:80] if scope is not None else None,
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
        self.registry_audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.registry_audit_lock:
            with self.registry_audit_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            fsync_dir(self.registry_audit_path.parent)

    def emit_registry_audit_from_decision_event(self, row: dict[str, Any], *, cache_id: str | None = None) -> None:
        actions = row.get("audit_actions")
        if not isinstance(actions, list) or not actions:
            return
        if row.get("phase") == "worker_selected" and row.get("decision") == "no_op" and row.get("cache_hit_level") == "none":
            return
        self.emit_registry_audit_event(
            operation=registry_audit_operation(row),
            outcome=registry_audit_outcome(row),
            audit_actions=[str(action) for action in actions],
            request_id=str(row.get("request_id")) if row.get("request_id") else None,
            trace_id=str(row.get("trace_id")) if row.get("trace_id") else None,
            cache_id=cache_id,
            cache_key_hash=row.get("cache_key_hash"),
            manifest_id=row.get("manifest_id"),
            worker_id=row.get("worker_id"),
            tenant_hash=row.get("tenant_hash"),
            conversation_hash=row.get("conversation_hash"),
            scope=row.get("scope"),
            reason=row.get("policy_denial_reason") or row.get("fallback_reason") or row.get("validation_status"),
            source="decision_event",
            source_event_id=str(row.get("event_id")) if row.get("event_id") else None,
        )

    def emit_event(
        self,
        *,
        phase: str,
        decision: str,
        cache_id: str,
        cache_key_hash: str | None,
        manifest_id: str | None,
        cache_hit_level: str,
        compatibility_result: str,
        latency_ms: float | None,
        prompt_tokens: int | None,
        processed_prompt_tokens: int | None,
        cached_tokens: int | None,
        generated_tokens: int | None,
        prompt_tps: float | None,
        eval_tps: float | None,
        ttft_ms: float | None = None,
        restore_latency_ms: float | None = None,
        hydration_latency_ms: float | None = None,
        fallback_required: bool = False,
        fallback_reason: str | None = None,
        worker_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        model_id: str | None = None,
        cache_event_basis: str = "slot_state",
        restore_observed_basis: str | None = None,
        prompt_basis: str | None = None,
        validation_status: str | None = None,
        scheduler: dict[str, Any] | None = None,
        cache_policy: dict[str, Any] | None = None,
        policy_denial_reason: str | None = None,
        notes: str = "",
    ) -> None:
        event_policy = cache_policy or default_cache_policy()
        selected_worker_id = worker_id
        reuse_ratio = cached_tokens / prompt_tokens if prompt_tokens and cached_tokens is not None else None
        if isinstance(reuse_ratio, (int, float)):
            reuse_ratio = max(0.0, min(1.0, float(reuse_ratio)))
        full_reprocess_suspected = classify_full_reprocess(
            decision=decision,
            cache_hit_level=cache_hit_level,
            cached_tokens=cached_tokens,
            processed_prompt_tokens=processed_prompt_tokens,
            prompt_basis=prompt_basis,
        )
        event_request_id = request_id or cache_id
        row = {
            "schema_version": SCHEMA_VERSION,
            "event_id": "evt-" + hashlib.sha256(f"{time.time_ns()}:{phase}:{cache_id}".encode()).hexdigest()[:16],
            "trace_id": trace_id or event_request_id,
            "request_id": event_request_id,
            "request_hash": hashlib.sha256(event_request_id.encode()).hexdigest(),
            "timestamp": now_iso(),
            "phase": phase,
            "decision": decision,
            "tenant_hash": event_policy["tenant_hash"],
            "conversation_hash": event_policy.get("conversation_hash"),
            "scope": event_policy["scope"],
            "model_id": model_id or self.args.model,
            "worker_id": selected_worker_id,
            "cache_key_hash": cache_key_hash,
            "manifest_id": manifest_id,
            "cache_hit_level": cache_hit_level,
            "compatibility_result": compatibility_result,
            "validation_status": validation_status or ("validated" if compatibility_result == "match" else "not_applicable"),
            "fallback_required": fallback_required,
            "fallback_reason": fallback_reason,
            "policy_denial_reason": policy_denial_reason,
            "audit_actions": audit_actions_for_event(
                phase=phase,
                decision=decision,
                cache_hit_level=cache_hit_level,
                compatibility_result=compatibility_result,
                fallback_required=fallback_required,
                fallback_reason=fallback_reason,
            ),
            "latency_ms": latency_ms,
            "metrics": {
                "decision_latency_ms": latency_ms,
                "registry_lookup_latency_ms": None,
                "hydration_latency_ms": hydration_latency_ms,
                "restore_latency_ms": restore_latency_ms,
                "ttft_ms": ttft_ms,
                "prompt_tokens": prompt_tokens,
                "cached_tokens": cached_tokens,
                "processed_prompt_tokens": processed_prompt_tokens,
                "generated_tokens": generated_tokens,
                "prompt_tps": prompt_tps,
                "eval_tps": eval_tps,
                "reuse_ratio": reuse_ratio,
                "full_reprocess_suspected": full_reprocess_suspected,
                "cache_event_basis": cache_event_basis,
                "restore_observed_basis": restore_observed_basis or ("slot_state" if restore_latency_ms is not None else "not_checked"),
            },
            "scheduler": scheduler or {
                "policy": "not_recorded",
                "candidates": [],
                "winner_worker_id": selected_worker_id,
                "winner_reason": "scheduler trace not recorded for this event",
            },
            "policy": cache_policy_event_summary(event_policy, denial_reason=policy_denial_reason),
            "privacy": {
                "raw_prompt_logged": False,
                "raw_tenant_id_logged": False,
                "raw_conversation_id_logged": False,
                "raw_cache_blob_path_logged": False,
                "raw_environment_logged": False,
                "contains_secret_material": False,
                "redaction_status": "synthetic_example",
            },
            "notes": notes[:200] if notes else None,
        }
        append_jsonl(self.events_path, row)
        self.emit_registry_audit_from_decision_event(row, cache_id=cache_id)
        self.record_cache_event(
            phase=phase,
            decision=decision,
            cache_hit_level=cache_hit_level,
            compatibility_result=compatibility_result,
            validation_status=row.get("validation_status"),
            fallback_required=fallback_required,
            fallback_reason=fallback_reason,
            hydration_latency_ms=hydration_latency_ms,
            restore_latency_ms=restore_latency_ms,
            full_reprocess_suspected=full_reprocess_suspected,
        )
        if latency_ms is not None:
            self.observe_metric("routing_decision_latency_ms", latency_ms)
        if ttft_ms is not None:
            self.observe_metric("ttft_ms", ttft_ms)

    def emit_cache_key_mismatch_event(
        self,
        *,
        cache_id: str,
        expected_cache_key_hash: str,
        manifest_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        model_id: str | None = None,
        cache_policy: dict[str, Any] | None = None,
        notes: str = "Request-supplied cache_key_hash did not match the scoped registry/manifest row.",
    ) -> None:
        self.emit_event(
            phase="registry_lookup",
            decision="no_op",
            cache_id=cache_id,
            cache_key_hash=expected_cache_key_hash,
            manifest_id=manifest_id,
            cache_hit_level="registry_only",
            compatibility_result="mismatch",
            validation_status="not_checked",
            latency_ms=None,
            prompt_tokens=None,
            processed_prompt_tokens=None,
            cached_tokens=None,
            generated_tokens=None,
            prompt_tps=None,
            eval_tps=None,
            fallback_required=True,
            fallback_reason="cache_key_mismatch",
            request_id=request_id,
            trace_id=trace_id,
            model_id=model_id,
            cache_event_basis="registry_lookup",
            restore_observed_basis="not_checked",
            cache_policy=cache_policy,
            notes=notes,
        )

    def build_cache(
        self,
        *,
        cache_id: str,
        prefix_text: str,
        refresh: bool = False,
        worker_id: str | None = None,
        model: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        cache_policy: dict[str, Any] | None = None,
        expected_cache_key_hash: str | None = None,
    ) -> dict[str, Any]:
        with self.lock:
            active_policy = cache_policy or default_cache_policy()
            if denial_reason := request_policy_denial_reason(active_policy):
                self.emit_event(
                    phase="registry_lookup",
                    decision="reject_policy",
                    cache_id=cache_id,
                    cache_key_hash=None,
                    manifest_id=None,
                    cache_hit_level="registry_only",
                    compatibility_result="policy_denied",
                    validation_status="not_checked",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason="policy_denied",
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model,
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=active_policy,
                    policy_denial_reason=denial_reason,
                    notes=f"Cache build denied by tenant/scope policy: {denial_reason}.",
                )
                raise CachePolicyDeniedError(denial_reason)
            if refresh and expected_cache_key_hash is not None:
                try:
                    self.ensure_entry(cache_id, cache_policy=active_policy, expected_cache_key_hash=expected_cache_key_hash)
                except CacheKeyMismatchError as exc:
                    self.emit_cache_key_mismatch_event(
                        cache_id=cache_id,
                        expected_cache_key_hash=exc.expected_cache_key_hash,
                        manifest_id=exc.manifest_id,
                        request_id=request_id,
                        trace_id=trace_id,
                        model_id=model,
                        cache_policy=active_policy,
                        notes="Refresh rejected before worker slot mutation because the supplied cache_key_hash did not match the current scoped entry.",
                    )
                    raise
            scheduler_trace: dict[str, Any] = {}
            worker = self.select_worker(worker_id, model=model, scheduler_trace=scheduler_trace)
            key_fields = self.cache_key_fields(cache_id, prefix_text, worker, cache_policy=active_policy)
            cache_key_hash = cache_key_hash_from_record(key_fields, label="computed cache key")
            existing = self.find_entry(cache_id, cache_policy=active_policy, expected_cache_key_hash=cache_key_hash)
            existing_active = bool(existing) and not is_quarantined_cache_status(existing.get("validation_status"))
            if existing_active and isinstance(existing.get("manifest_path"), str):
                try:
                    existing_manifest = read_json(Path(existing["manifest_path"]), {})
                except Exception:  # noqa: BLE001
                    existing_active = False
                else:
                    if isinstance(existing_manifest, dict) and is_quarantined_cache_status(existing_manifest.get("validation_status")):
                        existing_active = False
            if existing_active and not refresh and existing.get("cache_key_hash") == cache_key_hash:
                return {"cache_id": cache_id, "cache_key_hash": cache_key_hash, "cache_exists": True, "entry": existing}

            manifest_id = "manifest-" + cache_key_hash[:16]
            lease = self.acquire_registry_lease(
                operation="build_upload",
                cache_id=cache_id,
                cache_key_hash=cache_key_hash,
                manifest_id=manifest_id,
                worker_id=worker.worker_id,
                request_id=request_id,
                trace_id=trace_id,
            )
            try:
                return self.build_cache_under_registry_lease(
                    cache_id=cache_id,
                    cache_key_hash=cache_key_hash,
                    manifest_id=manifest_id,
                    prefix_text=prefix_text,
                    worker=worker,
                    key_fields=key_fields,
                    active_policy=active_policy,
                    scheduler_trace=scheduler_trace,
                    replace_cache_key_hash=expected_cache_key_hash if refresh else None,
                    request_id=request_id,
                    trace_id=trace_id,
                )
            finally:
                self.release_registry_lease(lease)

    def build_cache_under_registry_lease(
        self,
        *,
        cache_id: str,
        cache_key_hash: str,
        manifest_id: str,
        prefix_text: str,
        worker: WorkerRuntime,
        key_fields: dict[str, Any],
        active_policy: dict[str, Any],
        scheduler_trace: dict[str, Any],
        replace_cache_key_hash: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        self.record_worker_selected(worker.worker_id, reason="cache_build")
        slot_filename = f"cache-router-openai-{cache_key_hash[:16]}.slot"
        slot_action(worker.url, worker.slot_id, "erase", None, self.args.timeout)
        comp, build_wall_ms = completion(
            worker.url,
            prefix_text,
            n_predict=0,
            slot_id=worker.slot_id,
            timeout=self.args.timeout,
        )
        save_body, save_wall_ms = slot_action(worker.url, worker.slot_id, "save", slot_filename, self.args.timeout)
        slot_info = worker.transport.file_info(slot_filename, hash_file=True)
        if not slot_info.exists or not slot_info.sha256:
            raise RuntimeError(f"slot save did not create expected file: {worker.transport.display_slot_path(slot_filename)}")
        slot_hash = slot_info.sha256
        blob_path = self.blobs / slot_hash[:2] / f"{slot_hash}.slot"
        ingest_start = time.perf_counter()
        ingest = worker.transport.upload_to_router(slot_filename, blob_path)
        blob_hash = ingest["sha256"]
        ingest_wall_ms = (time.perf_counter() - ingest_start) * 1000.0
        if blob_hash != slot_hash:
            raise RuntimeError("router blob hash mismatch after ingest")

        manifest_path = self.manifests / f"{cache_key_hash}.json"
        manifest = {
            "schema_version": "2026-07-01.1",
            "cache_id": cache_id,
            "cache_key_hash": cache_key_hash,
            "manifest_id": manifest_id,
            "source_worker_id": worker.worker_id,
            "scope": active_policy["scope"],
            "tenant_hash": active_policy["tenant_hash"],
            "conversation_hash": active_policy.get("conversation_hash"),
            "policy_id_hash": active_policy["policy_id_hash"],
            "prefix_sha256": key_fields["prefix_sha256"],
            "prefix_token_count": key_fields["prefix_token_count"],
            "model": worker.model,
            "model_identity": worker.model_identity,
            "model_path": worker.model_path,
            "model_file_size": worker.model_file_size,
            "model_architecture": key_fields["model_architecture"],
            "model_hash": key_fields["model_hash"],
            "gguf_tensor_manifest_hash": key_fields["gguf_tensor_manifest_hash"],
            "tokenizer_hash": key_fields["tokenizer_hash"],
            "chat_template_effective_hash": key_fields["chat_template_effective_hash"],
            "tools_schema_hash": key_fields["tools_schema_hash"],
            "system_prompt_hash": key_fields["system_prompt_hash"],
            "special_token_policy": key_fields["special_token_policy"],
            "llama_server_path": worker.llama_server_path,
            "llama_server_version": worker.llama_server_version,
            "llama_cpp_source_commit": key_fields["llama_cpp_source_commit"],
            "llama_cpp_cache_abi_version": key_fields["llama_cpp_cache_abi_version"],
            "patchset_id": key_fields["patchset_id"],
            "build_backend": key_fields["build_backend"],
            "gpu_backend_driver": key_fields["gpu_backend_driver"],
            "kv_unified_mode": key_fields["kv_unified_mode"],
            "ctx_size": worker.ctx_size,
            "ctx_checkpoints_config": key_fields["ctx_checkpoints_config"],
            "cache_type_k": worker.cache_type_k,
            "cache_type_v": worker.cache_type_v,
            "flash_attention_mode": key_fields["flash_attention_mode"],
            "rope_freq_base": key_fields["rope_freq_base"],
            "rope_freq_scale": key_fields["rope_freq_scale"],
            "yarn_or_rope_scaling_metadata": key_fields["yarn_or_rope_scaling_metadata"],
            "reasoning_format": key_fields["reasoning_format"],
            "jinja_template_mode": key_fields["jinja_template_mode"],
            "mtp_enabled": worker.mtp_enabled,
            "spec_draft_model_identity": worker.spec_draft_model_identity,
            "spec_draft_model_path": worker.spec_draft_model_path,
            "spec_draft_model_size": worker.spec_draft_model_size,
            "spec_draft_model_hash": key_fields["spec_draft_model_hash"],
            "spec_draft_config": key_fields["spec_draft_config"],
            "n_parallel": key_fields["n_parallel"],
            "n_seq_max": key_fields["n_seq_max"],
            "slot_file_sha256": slot_hash,
            "slot_file_size_bytes": ingest["size_bytes"],
            "slot_filename": slot_filename,
            "router_blob_path": str(blob_path),
            "worker_slot_path": worker.transport.display_slot_path(slot_filename),
            "worker_transport": worker.transport.describe(),
            "created_at": now_iso(),
            "last_used_at": None,
            "validation_status": "validated",
            "worker_residency": {worker.worker_id: True},
        }
        if self.encryption_at_rest is not None:
            manifest["encryption_at_rest"] = dict(self.encryption_at_rest)
        self.append_registry_wal(
            operation="manifest_prepare",
            outcome="start",
            cache_id=cache_id,
            cache_key_hash=cache_key_hash,
            manifest_id=manifest_id,
            worker_id=worker.worker_id,
        )
        write_json(manifest_path, manifest)
        self.append_registry_wal(
            operation="manifest_prepared",
            outcome="success",
            cache_id=cache_id,
            cache_key_hash=cache_key_hash,
            manifest_id=manifest_id,
            worker_id=worker.worker_id,
        )
        registry = self.load_registry()
        replacement_keys = {cache_key_hash}
        if replace_cache_key_hash:
            replacement_keys.add(replace_cache_key_hash)
        registry["entries"] = [
            row
            for row in registry.get("entries", [])
            if not (
                row.get("cache_id") == cache_id
                and cache_policy_denial_reason(active_policy, row) is None
                and row.get("cache_key_hash") in replacement_keys
            )
        ]
        registry["entries"].append(
            {
                "cache_id": cache_id,
                "cache_key_hash": cache_key_hash,
                "manifest_id": manifest_id,
                "manifest_path": str(manifest_path),
                "scope": active_policy["scope"],
                "tenant_hash": active_policy["tenant_hash"],
                "conversation_hash": active_policy.get("conversation_hash"),
                "policy_id_hash": active_policy["policy_id_hash"],
                "router_blob_path": str(blob_path),
                "slot_filename": slot_filename,
                "slot_file_sha256": slot_hash,
                "slot_file_size_bytes": ingest["size_bytes"],
                "strict_key_fields": {field: key_fields[field] for field in STRICT_COMPATIBILITY_FIELDS},
                "created_at": manifest["created_at"],
                "last_used_at": None,
                "source_worker_id": worker.worker_id,
                "validation_status": "validated",
                "encryption_at_rest": dict(self.encryption_at_rest) if self.encryption_at_rest is not None else None,
                "worker_residency": {worker.worker_id: True},
            }
        )
        self.save_registry(registry)
        self.append_registry_wal(
            operation="registry_committed",
            outcome="success",
            cache_id=cache_id,
            cache_key_hash=cache_key_hash,
            manifest_id=manifest_id,
            worker_id=worker.worker_id,
            entry_count=len(registry.get("entries", [])),
        )
        timings = comp.get("timings") if isinstance(comp.get("timings"), dict) else {}
        self.emit_event(
            phase="cache_commit_published",
            decision="no_op",
            cache_id=cache_id,
            cache_key_hash=cache_key_hash,
            manifest_id=manifest_id,
            cache_hit_level="durable_blob",
            compatibility_result="match",
            latency_ms=ingest_wall_ms,
            prompt_tokens=comp.get("tokens_evaluated"),
            processed_prompt_tokens=comp.get("tokens_evaluated"),
            cached_tokens=comp.get("tokens_cached") or 0,
            generated_tokens=comp.get("tokens_predicted"),
            prompt_tps=timings.get("prompt_per_second"),
            eval_tps=timings.get("predicted_per_second"),
            ttft_ms=extract_ttft_ms_from_json(comp),
            worker_id=worker.worker_id,
            request_id=request_id,
            trace_id=trace_id,
            model_id=worker.model,
            scheduler=scheduler_trace,
            cache_policy=active_policy,
            notes="Router daemon built prefix cache and published durable blob.",
        )
        return {
            "cache_id": cache_id,
            "worker_id": worker.worker_id,
            "policy": policy_metadata(active_policy),
            "cache_key_hash": cache_key_hash,
            "manifest_id": manifest_id,
            "prefix_tokens": key_fields["prefix_token_count"],
            "slot_filename": slot_filename,
            "slot_file_sha256": slot_hash,
            "slot_file_size_bytes": ingest["size_bytes"],
            "build_prompt_ms": timings.get("prompt_ms"),
            "build_wall_ms": build_wall_ms,
            "save_ms": (save_body.get("timings") or {}).get("save_ms"),
            "save_wall_ms": save_wall_ms,
            "ingest_ms": ingest_wall_ms,
            "router_blob_path": str(blob_path),
            "manifest_path": str(manifest_path),
            "n_saved": save_body.get("n_saved"),
            "completion": {
                "tokens_evaluated": comp.get("tokens_evaluated"),
                "tokens_predicted": comp.get("tokens_predicted"),
                "tokens_cached": comp.get("tokens_cached"),
                "timings": timings,
            },
        }

    def ensure_entry(
        self,
        cache_id: str,
        *,
        cache_policy: dict[str, Any] | None = None,
        expected_cache_key_hash: str | None = None,
    ) -> dict[str, Any]:
        entry = self.find_entry(cache_id, cache_policy=cache_policy, expected_cache_key_hash=expected_cache_key_hash)
        if not entry:
            if expected_cache_key_hash is not None:
                if cache_policy is not None:
                    unscoped_exact = self.find_entry(cache_id, expected_cache_key_hash=expected_cache_key_hash)
                    if unscoped_exact is not None:
                        raise CachePolicyDeniedError(cache_policy_denial_reason(cache_policy, unscoped_exact) or "tenant_scope_mismatch")
                    scoped_any = self.find_entry(cache_id, cache_policy=cache_policy)
                    if scoped_any is not None:
                        raise CacheKeyMismatchError(
                            cache_id,
                            expected_cache_key_hash,
                            scoped_any.get("cache_key_hash"),
                            scoped_any.get("manifest_id"),
                        )
                else:
                    unscoped_any = self.find_entry(cache_id)
                    if unscoped_any is not None:
                        raise CacheKeyMismatchError(
                            cache_id,
                            expected_cache_key_hash,
                            unscoped_any.get("cache_key_hash"),
                            unscoped_any.get("manifest_id"),
                        )
                raise KeyError(f"cache_id/cache_key_hash not found: {cache_id}")
            if cache_policy is not None:
                unscoped_entry = self.find_entry(cache_id)
                if unscoped_entry is not None:
                    raise CachePolicyDeniedError(cache_policy_denial_reason(cache_policy, unscoped_entry) or "tenant_scope_mismatch")
            raise KeyError(f"cache_id not found: {cache_id}")
        if not isinstance(entry, dict):
            raise RuntimeError(f"registry entry is not an object for cache_id={cache_id!r}")
        entry_required = ["cache_id", "cache_key_hash", "manifest_id", "manifest_path"]
        missing_entry = [field for field in entry_required if entry.get(field) in (None, "")]
        if missing_entry:
            raise RuntimeError(f"registry entry missing required fields for cache_id={cache_id!r}: {', '.join(missing_entry)}")
        if is_quarantined_cache_status(entry.get("validation_status")):
            reason = str(entry.get("quarantine_reason") or "unspecified")
            raise RuntimeError(f"cache registry entry quarantined: reason={reason[:80]}")
        manifest_path = Path(entry["manifest_path"])
        try:
            manifest = read_json(manifest_path, {})
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"manifest invalid JSON: {manifest_path}: {exc}") from exc
        if not manifest:
            raise RuntimeError(f"manifest missing or empty: {manifest_path}")
        if not isinstance(manifest, dict):
            raise RuntimeError(f"manifest must be a JSON object: {manifest_path}")
        if is_quarantined_cache_status(manifest.get("validation_status")):
            reason = str(manifest.get("quarantine_reason") or "unspecified")
            raise RuntimeError(f"cache manifest quarantined: reason={reason[:80]}")
        manifest_required = [
            "cache_id",
            "cache_key_hash",
            "manifest_id",
            "model",
            "model_identity",
            "model_file_size",
            "llama_server_version",
            "ctx_size",
            "cache_type_k",
            "cache_type_v",
            "mtp_enabled",
            "slot_file_sha256",
            "slot_file_size_bytes",
            "slot_filename",
            "router_blob_path",
        ]
        manifest_required.extend(STRICT_COMPATIBILITY_FIELDS)
        missing_manifest = [field for field in manifest_required if manifest.get(field) in (None, "")]
        if missing_manifest:
            raise RuntimeError(f"manifest missing required fields: {', '.join(missing_manifest)}")
        strict_errors = [
            error
            for field in STRICT_COMPATIBILITY_FIELDS
            if (error := strict_value_error(field, manifest.get(field), mtp_enabled=bool(manifest.get("mtp_enabled"))))
        ]
        if strict_errors:
            raise RuntimeError("manifest strict key invalid: " + "; ".join(strict_errors))
        if manifest.get("mtp_enabled") is True and manifest.get("spec_draft_model_identity") in (None, ""):
            raise RuntimeError("manifest missing required fields: spec_draft_model_identity")
        if manifest.get("cache_id") != cache_id:
            raise RuntimeError(f"manifest cache_id mismatch: manifest={manifest.get('cache_id')!r} request={cache_id!r}")
        for field in ["cache_key_hash", "manifest_id"]:
            if entry.get(field) != manifest.get(field):
                raise RuntimeError(f"registry/manifest {field} mismatch: registry={entry.get(field)!r} manifest={manifest.get(field)!r}")
        canonical_cache_key_hash = cache_key_hash_from_record(manifest, label="manifest cache key")
        if manifest.get("cache_key_hash") != canonical_cache_key_hash:
            raise CacheKeyMismatchError(cache_id, canonical_cache_key_hash, manifest.get("cache_key_hash"), manifest.get("manifest_id"))
        if expected_cache_key_hash is not None:
            if entry.get("cache_key_hash") != expected_cache_key_hash:
                raise CacheKeyMismatchError(cache_id, expected_cache_key_hash, entry.get("cache_key_hash"), entry.get("manifest_id"))
            if manifest.get("cache_key_hash") != expected_cache_key_hash:
                raise CacheKeyMismatchError(cache_id, expected_cache_key_hash, manifest.get("cache_key_hash"), manifest.get("manifest_id"))
        try:
            slot_size = int(manifest["slot_file_size_bytes"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("manifest slot_file_size_bytes must be a positive integer") from exc
        if slot_size <= 0:
            raise RuntimeError("manifest slot_file_size_bytes must be positive")
        return {"entry": entry, "manifest": manifest}

    def hydrate_if_needed(self, manifest: dict[str, Any], worker: WorkerRuntime) -> dict[str, Any]:
        slot_filename = manifest["slot_filename"]
        slot_info = worker.transport.file_info(slot_filename, hash_file=True)
        before_exists = slot_info.exists
        expected_hash = manifest["slot_file_sha256"]
        if before_exists and slot_info.sha256 == expected_hash:
            return {
                "performed": False,
                "dest_existed_before": True,
                "sha256_match": True,
                "wall_ms": 0.0,
                "worker_slot_path": slot_info.path,
                "transport": worker.transport.describe(),
                "worker_id": worker.worker_id,
            }
        start = time.perf_counter()
        blob_path = self.manifest_blob_path(manifest, expected_hash)
        if not blob_path.is_file():
            raise RuntimeError(f"router blob missing: {blob_path}")
        expected_size = int(manifest["slot_file_size_bytes"])
        actual_size = blob_path.stat().st_size
        if actual_size != expected_size:
            raise DurableBlobCorruptError(f"router blob size mismatch: manifest={expected_size} blob={actual_size}")
        blob_hash = sha256_file(blob_path)
        if blob_hash != expected_hash:
            raise DurableBlobCorruptError(f"router blob hash mismatch: manifest={expected_hash} blob={blob_hash}")
        hydrated = worker.transport.hydrate_from_router(blob_path, slot_filename)
        dest_hash = hydrated.get("sha256")
        dest_matches_manifest = dest_hash == expected_hash
        return {
            "performed": True,
            "dest_existed_before": before_exists,
            "worker_slot_path": hydrated["dest"],
            "source_blob_path": str(blob_path),
            "dest_sha256": dest_hash,
            "blob_sha256": blob_hash,
            "source_sha256": expected_hash,
            "transport_sha256_match": hydrated["sha256_match"],
            "sha256_match": dest_matches_manifest,
            "size_bytes": hydrated["size_bytes"],
            "transport": worker.transport.describe(),
            "worker_id": worker.worker_id,
            "wall_ms": (time.perf_counter() - start) * 1000.0,
        }

    def use_cache(
        self,
        *,
        cache_id: str,
        prefix_text: str,
        suffix_text: str,
        max_tokens: int,
        generation_options: dict[str, Any] | None = None,
        allow_fallback: bool = True,
        worker_id: str | None = None,
        model: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        cache_policy: dict[str, Any] | None = None,
        expected_cache_key_hash: str | None = None,
        restore_validation_mode: str = "off",
        restore_validation_recompute_on_mismatch: bool = False,
        measure_true_ttft: bool = False,
        restore_generation_strategy: str = "full_prompt_after_slot_restore",
    ) -> dict[str, Any]:
        with self.lock:
            active_policy = cache_policy or default_cache_policy()
            if denial_reason := request_policy_denial_reason(active_policy):
                self.emit_event(
                    phase="registry_lookup",
                    decision="reject_policy",
                    cache_id=cache_id,
                    cache_key_hash=None,
                    manifest_id=None,
                    cache_hit_level="registry_only",
                    compatibility_result="policy_denied",
                    validation_status="not_checked",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason="policy_denied",
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model,
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=active_policy,
                    policy_denial_reason=denial_reason,
                    notes=f"Cache use denied by tenant/scope policy and masked as a scoped miss to the client: {denial_reason}.",
                )
                raise KeyError(f"cache_id not found: {cache_id}") from CachePolicyDeniedError(denial_reason)
            if expected_cache_key_hash is None:
                raise ValueError("expected_cache_key_hash is required for strict cache use")
            try:
                loaded = self.ensure_entry(cache_id, cache_policy=active_policy, expected_cache_key_hash=expected_cache_key_hash)
                entry = loaded["entry"]
                manifest = loaded["manifest"]
                if model and manifest.get("model") != model:
                    raise RuntimeError(f"cache model mismatch: manifest={manifest.get('model')!r} request={model!r}")
            except CacheKeyMismatchError as exc:
                self.emit_cache_key_mismatch_event(
                    cache_id=cache_id,
                    expected_cache_key_hash=exc.expected_cache_key_hash,
                    manifest_id=exc.manifest_id,
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model,
                    cache_policy=active_policy,
                )
                raise
            except KeyError:
                raise
            except CachePolicyDeniedError as exc:
                registry_entry = self.find_entry(cache_id, expected_cache_key_hash=expected_cache_key_hash)
                if isinstance(registry_entry, dict):
                    cache_key_hash = registry_entry.get("cache_key_hash")
                    manifest_id = registry_entry.get("manifest_id")
                else:
                    cache_key_hash = None
                    manifest_id = None
                self.emit_event(
                    phase="registry_lookup",
                    decision="reject_policy",
                    cache_id=cache_id,
                    cache_key_hash=cache_key_hash if isinstance(cache_key_hash, str) else None,
                    manifest_id=manifest_id if isinstance(manifest_id, str) else None,
                    cache_hit_level="registry_only",
                    compatibility_result="policy_denied",
                    validation_status="not_checked",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason="policy_denied",
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model,
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=active_policy,
                    policy_denial_reason=exc.reason,
                    notes=f"Cache use denied by tenant/scope policy and masked as a scoped miss to the client: {exc.reason}.",
                )
                raise KeyError(f"cache_id not found: {cache_id}") from exc
            except Exception as exc:  # noqa: BLE001
                registry_entry = self.find_entry(cache_id, cache_policy=active_policy, expected_cache_key_hash=expected_cache_key_hash) or self.find_entry(
                    cache_id,
                    expected_cache_key_hash=expected_cache_key_hash,
                )
                if isinstance(registry_entry, dict):
                    cache_key_hash = registry_entry.get("cache_key_hash")
                    manifest_id = registry_entry.get("manifest_id")
                else:
                    cache_key_hash = None
                    manifest_id = None
                error_text = str(exc)
                if "model mismatch" in error_text:
                    reason = "cache_key_mismatch"
                elif "manifest" in error_text or "registry entry" in error_text:
                    reason = "manifest_quarantined"
                elif "cache_key" in error_text:
                    reason = "cache_key_mismatch"
                else:
                    reason = "manifest_quarantined"
                self.emit_event(
                    phase="registry_lookup",
                    decision="reject_capacity",
                    cache_id=cache_id,
                    cache_key_hash=cache_key_hash if isinstance(cache_key_hash, str) else None,
                    manifest_id=manifest_id if isinstance(manifest_id, str) else None,
                    cache_hit_level="registry_only",
                    compatibility_result="mismatch",
                    validation_status="quarantined",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason=reason,
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model,
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=active_policy,
                    notes="Cache registry or manifest validation failed before restore.",
                )
                raise
            if denial_reason := cache_policy_denial_reason(active_policy, manifest):
                self.emit_event(
                    phase="registry_lookup",
                    decision="reject_policy",
                    cache_id=cache_id,
                    cache_key_hash=manifest.get("cache_key_hash"),
                    manifest_id=manifest.get("manifest_id"),
                    cache_hit_level="registry_only",
                    compatibility_result="policy_denied",
                    validation_status="not_checked",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason="policy_denied",
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=model or manifest.get("model"),
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=active_policy,
                    policy_denial_reason=denial_reason,
                    notes=f"Cache use denied by tenant/scope policy and masked as a scoped miss to the client: {denial_reason}.",
                )
                raise KeyError(f"cache_id not found: {cache_id}") from CachePolicyDeniedError(denial_reason)
            residency = manifest.get("worker_residency") if isinstance(manifest.get("worker_residency"), dict) else {}
            target_model = model or manifest.get("model")
            ordered = self.ordered_workers(worker_id, prefer_residency=residency, allow_fallback=allow_fallback, model=target_model)
            first_choice_worker_id = ordered[0].worker_id if ordered else None
            scheduler_trace: dict[str, Any] = {}
            candidates = self.candidate_workers(
                worker_id,
                prefer_residency=residency,
                allow_fallback=allow_fallback,
                model=target_model,
                scheduler_trace=scheduler_trace,
            )
            attempts: list[dict[str, Any]] = []
            for worker in candidates:
                lease = self.acquire_registry_lease(
                    operation="restore_hydrate",
                    cache_id=cache_id,
                    cache_key_hash=manifest["cache_key_hash"],
                    manifest_id=manifest.get("manifest_id"),
                    worker_id=worker.worker_id,
                    request_id=request_id,
                    trace_id=trace_id,
                )
                try:
                    mismatch = self.worker_cache_compatibility_mismatch(manifest, worker)
                    if mismatch:
                        raise RuntimeError(f"incompatible cache for worker {worker.worker_id}: {mismatch}")
                    if not isinstance(prefix_text, str) or not prefix_text:
                        raise RuntimeError("cache restore generation requires cache_router.prefix_text")
                    if manifest.get("prefix_sha256") != sha256_text(prefix_text):
                        raise RuntimeError("cache_key_mismatch: cache_router.prefix_text does not match manifest prefix_sha256")
                    hydrate = self.hydrate_if_needed(manifest, worker)
                    if not hydrate.get("sha256_match"):
                        raise RuntimeError("hydrated slot hash mismatch")
                    restore_body, restore_wall_ms = slot_action(worker.url, worker.slot_id, "restore", manifest["slot_filename"], self.args.timeout)
                    if int(restore_body.get("n_restored") or 0) <= 0:
                        raise RuntimeError("restore_validation_failed: slot restore reported zero restored tokens")
                    if restore_generation_strategy == "suffix_only_after_slot_restore":
                        restore_prompt = suffix_text
                        restore_cache_prompt = True
                        restore_prompt_basis = "suffix_only_after_slot_restore"
                    else:
                        restore_prompt = prefix_text + suffix_text
                        restore_cache_prompt = False
                        restore_prompt_basis = "full_prompt_after_slot_restore"
                    cold_validation_prompt = prefix_text + suffix_text
                    comp, wall_ms = completion(
                        worker.url,
                        restore_prompt,
                        n_predict=max_tokens,
                        slot_id=worker.slot_id,
                        timeout=self.args.timeout,
                        cache_prompt=restore_cache_prompt,
                        generation_options=generation_options,
                    )
                    timings = comp.get("timings") if isinstance(comp.get("timings"), dict) else {}
                    ttft_probe: dict[str, Any] | None = None
                    if measure_true_ttft:
                        try:
                            ttft_restore_body, ttft_restore_wall_ms = slot_action(worker.url, worker.slot_id, "restore", manifest["slot_filename"], self.args.timeout)
                            if int(ttft_restore_body.get("n_restored") or 0) <= 0:
                                raise RuntimeError("ttft probe restore reported zero restored tokens")
                            ttft_probe = completion_stream_ttft_probe(
                                worker.url,
                                restore_prompt,
                                slot_id=worker.slot_id,
                                timeout=self.args.timeout,
                                cache_prompt=restore_cache_prompt,
                                generation_options=generation_options,
                            )
                            ttft_probe["restore_wall_ms"] = ttft_restore_wall_ms
                            ttft_probe["restore_n_restored"] = ttft_restore_body.get("n_restored")
                            true_ttft_ms = ttft_probe.get("time_to_first_token_ms")
                            if isinstance(true_ttft_ms, (int, float)) and not isinstance(true_ttft_ms, bool):
                                comp["time_to_first_token_ms"] = float(true_ttft_ms)
                                comp["ttft_measurement_basis"] = ttft_probe.get("measurement_basis")
                                timings["time_to_first_token_ms"] = float(true_ttft_ms)
                                timings["time_to_first_token_basis"] = ttft_probe.get("measurement_basis")
                                timings.setdefault("ttft_ms", float(true_ttft_ms))
                        except Exception as exc:  # noqa: BLE001
                            ttft_probe = {
                                "measurement_basis": "router_observed_native_completion_stream_first_token",
                                "error": f"{type(exc).__name__}: {exc}"[:500],
                            }
                    restored_content = str(comp.get("content", ""))
                    restore_validation: dict[str, Any] = {"mode": restore_validation_mode, "status": "not_checked"}
                    if restore_validation_mode == "deterministic_recompute":
                        validation_started = time.perf_counter()
                        erase_body, erase_wall_ms = slot_action(worker.url, worker.slot_id, "erase", None, self.args.timeout)
                        cold_comp, cold_wall_ms = completion(
                            worker.url,
                            cold_validation_prompt,
                            n_predict=max_tokens,
                            slot_id=worker.slot_id,
                            timeout=self.args.timeout,
                            cache_prompt=False,
                            generation_options=generation_options,
                        )
                        cold_timings = cold_comp.get("timings") if isinstance(cold_comp.get("timings"), dict) else {}
                        cold_content = str(cold_comp.get("content", ""))
                        text_match = cold_content == restored_content
                        restore_validation = {
                            "mode": "deterministic_recompute",
                            "status": "pass" if text_match else "fail",
                            "basis": "same_worker_cold_recompute_after_slot_erase",
                            "text_match": text_match,
                            "restored_text_sha256": sha256_text(restored_content),
                            "cold_text_sha256": sha256_text(cold_content),
                            "validation_latency_ms": (time.perf_counter() - validation_started) * 1000.0,
                            "erase_wall_ms": erase_wall_ms,
                            "cold_wall_ms": cold_wall_ms,
                            "erase_n_erased": erase_body.get("n_erased"),
                        }
                        if not text_match:
                            self.quarantine_cache_entry(
                                entry,
                                manifest,
                                reason="restore_validation_failed",
                                request_id=request_id,
                                trace_id=trace_id,
                                worker_id=worker.worker_id,
                            )
                            failure_hit_level = "durable_blob" if hydrate.get("performed") else "local_nvme"
                            attempts.append(
                                {
                                    "worker_id": worker.worker_id,
                                    "status": "restore_validation_failed",
                                    "cache_hit_level": failure_hit_level,
                                    "hydration_performed": hydrate.get("performed"),
                                    "prompt_basis": restore_prompt_basis,
                                    "cache_prompt": restore_cache_prompt,
                                }
                            )
                            self.emit_event(
                                phase="restore_validated",
                                decision="fallback_after_restore_failure",
                                cache_id=cache_id,
                                cache_key_hash=manifest["cache_key_hash"],
                                manifest_id=manifest.get("manifest_id"),
                                cache_hit_level=failure_hit_level,
                                compatibility_result="match",
                                validation_status="quarantined",
                                latency_ms=restore_wall_ms,
                                prompt_tokens=comp.get("tokens_evaluated"),
                                processed_prompt_tokens=comp.get("tokens_evaluated"),
                                cached_tokens=comp.get("tokens_cached"),
                                generated_tokens=comp.get("tokens_predicted"),
                                prompt_tps=timings.get("prompt_per_second"),
                                eval_tps=timings.get("predicted_per_second"),
                                ttft_ms=extract_ttft_ms_from_json(comp),
                                restore_latency_ms=restore_wall_ms,
                                hydration_latency_ms=hydrate.get("wall_ms"),
                                fallback_required=True,
                                fallback_reason="restore_validation_failed",
                                worker_id=worker.worker_id,
                                request_id=request_id,
                                trace_id=trace_id,
                                model_id=worker.model,
                                scheduler=scheduler_trace,
                                cache_policy=active_policy,
                                prompt_basis=restore_prompt_basis,
                                notes="Router deterministic restore validation mismatched a same-worker cold recompute; cache was quarantined and cold output is returned only when explicitly requested.",
                            )
                            if not restore_validation_recompute_on_mismatch or not allow_fallback:
                                raise RuntimeError("restore_validation_failed: deterministic restored output mismatched cold recompute")
                            recompute_attempt = {
                                "worker_id": worker.worker_id,
                                "status": "recomputed_cold",
                                "cache_hit_level": "none",
                                "hydration_performed": False,
                                "prompt_basis": "full_prompt_after_validation_mismatch",
                                "cache_prompt": False,
                            }
                            attempts.append(recompute_attempt)
                            self.record_worker_selected(worker.worker_id, reason="validation_recompute")
                            return {
                                "cache_id": cache_id,
                                "worker_id": worker.worker_id,
                                "first_choice_worker_id": first_choice_worker_id,
                                "policy": policy_metadata(active_policy),
                                "cache_key_hash": manifest["cache_key_hash"],
                                "manifest_id": manifest.get("manifest_id"),
                                "attempts": attempts,
                                "fallback_used": True,
                                "recomputed_cold": True,
                                "hydrate": hydrate,
                                "restore": {
                                    "body": restore_body,
                                    "wall_ms": restore_wall_ms,
                                    "n_restored": restore_body.get("n_restored"),
                                },
                                "restore_validation": restore_validation,
                                "completion": {
                                    "content": cold_content,
                                    "tokens_evaluated": cold_comp.get("tokens_evaluated"),
                                    "tokens_cached": cold_comp.get("tokens_cached"),
                                    "tokens_predicted": cold_comp.get("tokens_predicted"),
                                    "timings": cold_timings,
                                    "wall_ms": cold_wall_ms,
                                    "prompt_basis": "full_prompt_after_validation_mismatch",
                                    "cache_prompt": False,
                                    "generation_settings_hash": sha256_json(generation_options or {}),
                                },
                            }
                    now = now_iso()
                    manifest["last_used_at"] = now
                    manifest.setdefault("worker_residency", {})[worker.worker_id] = True
                    self.append_registry_wal(
                        operation="restore_residency_prepare",
                        outcome="start",
                        reason="restore_success",
                        cache_id=cache_id,
                        cache_key_hash=manifest["cache_key_hash"],
                        manifest_id=manifest.get("manifest_id"),
                        worker_id=worker.worker_id,
                    )
                    write_json(Path(entry["manifest_path"]), manifest)
                    registry = self.load_registry()
                    for row in registry.get("entries", []):
                        if row.get("cache_id") == cache_id and row.get("cache_key_hash") == manifest["cache_key_hash"]:
                            row["last_used_at"] = now
                            row.setdefault("worker_residency", {})[worker.worker_id] = True
                    self.save_registry(registry)
                    self.append_registry_wal(
                        operation="restore_residency_commit",
                        outcome="success",
                        reason="restore_success",
                        cache_id=cache_id,
                        cache_key_hash=manifest["cache_key_hash"],
                        manifest_id=manifest.get("manifest_id"),
                        worker_id=worker.worker_id,
                    )
                    success_attempt = {
                        "worker_id": worker.worker_id,
                        "status": "success",
                        "cache_hit_level": "durable_blob" if hydrate.get("performed") else "local_nvme",
                        "hydration_performed": hydrate.get("performed"),
                        "prompt_basis": restore_prompt_basis,
                        "cache_prompt": restore_cache_prompt,
                    }
                    attempts.append(success_attempt)
                    self.record_worker_selected(worker.worker_id, reason=str(success_attempt["cache_hit_level"]))
                    self.emit_event(
                        phase="restore_validated",
                        decision="restore_then_generate",
                        cache_id=cache_id,
                        cache_key_hash=manifest["cache_key_hash"],
                        manifest_id=manifest.get("manifest_id"),
                        cache_hit_level="durable_blob" if hydrate.get("performed") else "local_nvme",
                        compatibility_result="match",
                        latency_ms=restore_wall_ms,
                        prompt_tokens=comp.get("tokens_evaluated"),
                        processed_prompt_tokens=comp.get("tokens_evaluated"),
                        cached_tokens=comp.get("tokens_cached"),
                        generated_tokens=comp.get("tokens_predicted"),
                        prompt_tps=timings.get("prompt_per_second"),
                        eval_tps=timings.get("predicted_per_second"),
                        ttft_ms=extract_ttft_ms_from_json(comp),
                        restore_latency_ms=restore_wall_ms,
                        hydration_latency_ms=hydrate.get("wall_ms"),
                        worker_id=worker.worker_id,
                        request_id=request_id,
                        trace_id=trace_id,
                        model_id=worker.model,
                        scheduler=scheduler_trace,
                        cache_policy=active_policy,
                        prompt_basis=restore_prompt_basis,
                        notes=(
                            "Router daemon restored the saved slot, then routed only the suffix with llama.cpp cache_prompt enabled so the worker can append to the restored KV state."
                            if restore_prompt_basis == "suffix_only_after_slot_restore"
                            else "Router daemon restored the saved slot, then routed the full prompt with llama.cpp cache_prompt disabled; suffix-only append remains experimental until live restore validation passes."
                        ),
                    )
                    return {
                        "cache_id": cache_id,
                        "worker_id": worker.worker_id,
                        "first_choice_worker_id": first_choice_worker_id,
                        "policy": policy_metadata(active_policy),
                        "cache_key_hash": manifest["cache_key_hash"],
                        "manifest_id": manifest.get("manifest_id"),
                        "attempts": attempts,
                        "fallback_used": any(row.get("status") == "failed" for row in attempts)
                        or (first_choice_worker_id is not None and worker.worker_id != first_choice_worker_id),
                        "hydrate": hydrate,
                        "restore": {
                            "body": restore_body,
                            "wall_ms": restore_wall_ms,
                            "n_restored": restore_body.get("n_restored"),
                        },
                        "restore_validation": restore_validation,
                        "completion": {
                            "content": restored_content,
                            "tokens_evaluated": comp.get("tokens_evaluated"),
                            "tokens_cached": comp.get("tokens_cached"),
                            "tokens_predicted": comp.get("tokens_predicted"),
                            "timings": timings,
                            "time_to_first_token_ms": comp.get("time_to_first_token_ms"),
                            "ttft_measurement_basis": comp.get("ttft_measurement_basis"),
                            "ttft_probe": ttft_probe,
                            "wall_ms": wall_ms,
                            "prompt_basis": restore_prompt_basis,
                            "cache_prompt": restore_cache_prompt,
                            "generation_settings_hash": sha256_json(generation_options or {}),
                        },
                    }
                except Exception as exc:  # noqa: BLE001
                    error = f"{type(exc).__name__}: {exc}"
                    failure_reason = "restore_validation_failed"
                    if "incompatible cache" in str(exc):
                        failure_reason = "cache_key_mismatch"
                    elif "hydrated" in str(exc) or "hydrate" in str(exc) or "router blob" in str(exc):
                        failure_reason = "hydration_failed"
                    quarantine_entry = durable_blob_requires_quarantine(exc)
                    fallback_decision = "fallback_after_restore_failure" if allow_fallback else "reject_capacity"
                    attempts.append({"worker_id": worker.worker_id, "status": "failed", "error": error[:500]})
                    if quarantine_entry:
                        self.quarantine_cache_entry(
                            entry,
                            manifest,
                            reason="corrupt_blob",
                            request_id=request_id,
                            trace_id=trace_id,
                            worker_id=worker.worker_id,
                        )
                    else:
                        manifest.setdefault("worker_residency", {})[worker.worker_id] = False
                        self.append_registry_wal(
                            operation="restore_residency_prepare",
                            outcome="start",
                            reason=failure_reason,
                            cache_id=cache_id,
                            cache_key_hash=manifest.get("cache_key_hash"),
                            manifest_id=manifest.get("manifest_id"),
                            worker_id=worker.worker_id,
                        )
                        write_json(Path(entry["manifest_path"]), manifest)
                        registry = self.load_registry()
                        for row in registry.get("entries", []):
                            if row.get("cache_id") == cache_id and row.get("cache_key_hash") == manifest.get("cache_key_hash"):
                                row.setdefault("worker_residency", {})[worker.worker_id] = False
                        self.save_registry(registry)
                        self.append_registry_wal(
                            operation="restore_residency_commit",
                            outcome="failed_worker_marked",
                            reason=failure_reason,
                            cache_id=cache_id,
                            cache_key_hash=manifest.get("cache_key_hash"),
                            manifest_id=manifest.get("manifest_id"),
                            worker_id=worker.worker_id,
                        )
                    self.emit_event(
                        phase="request_failed",
                        decision=fallback_decision,
                        cache_id=cache_id,
                        cache_key_hash=manifest.get("cache_key_hash"),
                        manifest_id=manifest.get("manifest_id"),
                        cache_hit_level="durable_blob" if allow_fallback else "none",
                        compatibility_result="mismatch" if failure_reason == "cache_key_mismatch" else "match",
                        validation_status="quarantined",
                        latency_ms=None,
                        prompt_tokens=None,
                        processed_prompt_tokens=None,
                        cached_tokens=None,
                        generated_tokens=None,
                        prompt_tps=None,
                        eval_tps=None,
                        fallback_required=True,
                        fallback_reason=failure_reason,
                        worker_id=worker.worker_id,
                        request_id=request_id,
                        trace_id=trace_id,
                        model_id=worker.model,
                        scheduler=scheduler_trace,
                        cache_policy=active_policy,
                        notes="Durable blob checksum failed; cache entry was quarantined."
                        if quarantine_entry
                        else "Cache restore/use attempt failed; router will try next eligible worker if available.",
                    )
                    if quarantine_entry or not allow_fallback:
                        break
                finally:
                    self.release_registry_lease(lease)
            raise RuntimeError(f"cache restore/use failed on all candidate workers: {attempts}")


class RouterHandler(BaseHTTPRequestHandler):
    server_version = "CachyRouter/0.1"

    @property
    def state(self) -> CacheRouterState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s %s\n" % (now_iso(), fmt % args))

    def read_body(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc

    def router_debug_headers(self, extra_headers: dict[str, str] | None = None) -> dict[str, str]:
        headers = dict(extra_headers or {})
        present = {key.lower() for key in headers}
        if "x-cache-router-request-id" not in present:
            headers["X-Cache-Router-Request-ID"] = new_opaque_id("req")
        if "x-cache-router-trace-id" not in present:
            headers["X-Cache-Router-Trace-ID"] = new_opaque_id("trace")
        if "x-cache-router-worker" not in present:
            headers["X-Cache-Router-Worker"] = "none"
        return headers

    def send_json(self, status: int, body: Any, *, extra_headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body, indent=None, sort_keys=True).encode("utf-8")
        headers = self.router_debug_headers(extra_headers)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def send_text(
        self,
        status: int,
        body: str,
        *,
        content_type: str = "text/plain; version=0.0.4; charset=utf-8",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        data = body.encode("utf-8")
        headers = self.router_debug_headers(extra_headers)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str, *, code: str = "cache_router_error", extra_headers: dict[str, str] | None = None) -> None:
        self.send_json(status, {"error": {"message": message, "type": code, "code": status}}, extra_headers=extra_headers)

    def send_tracked_error_json(self, status: int, message: str, *, code: str) -> None:
        started_at = self.state.begin_request()
        request_id = new_opaque_id("req")
        trace_id = new_opaque_id("trace")
        try:
            self.send_error_json(
                status,
                message,
                code=code,
                extra_headers={
                    "X-Cache-Router-Request-ID": request_id,
                    "X-Cache-Router-Trace-ID": trace_id,
                    "X-Cache-Router-Worker": "none",
                },
            )
        finally:
            self.state.finish_request(method=self.command, path=self.path, status=status, started_at=started_at)

    def route_error(self, exc: Exception) -> tuple[int, str, str]:
        message = str(exc)
        if isinstance(exc, QueueBackpressureError) or "queue backpressure" in message:
            return 503, "service_unavailable", message
        if isinstance(exc, RegistryLeaseConflictError) or "registry lease conflict" in message:
            return 503, "service_unavailable", message
        if isinstance(exc, CachePolicyDeniedError) or "cache policy denied" in message:
            return 403, "cache_policy_denied", message
        if "no healthy cache-router worker available" in message or "no ready cache-router worker available" in message:
            return 503, "service_unavailable", message
        if "unknown worker_id" in message:
            return 404, "worker_not_found", message
        return 500, "cache_router_error", message

    def require_configured_model(self, body: dict[str, Any]) -> bool:
        model = body.get("model")
        if not isinstance(model, str) or not model:
            self.send_tracked_error_json(400, "request model is required", code="invalid_request_error")
            return False
        if not self.state.has_model(model):
            self.send_tracked_error_json(404, f"model is not configured: {model}", code="model_not_found")
            return False
        return True

    def authorized(self) -> bool:
        token = self.state.auth_token
        if not token:
            return True
        auth = self.headers.get("Authorization", "")
        provided = ""
        if auth.lower().startswith("bearer "):
            provided = auth.split(" ", 1)[1].strip()
        if not provided:
            provided = (self.headers.get("X-API-Key") or self.headers.get("api-key") or "").strip()
        return hmac.compare_digest(provided, token)

    def require_auth(self) -> bool:
        if self.authorized():
            return True
        self.send_json(
            401,
            {
                "error": {
                    "message": "missing or invalid cache-router bearer token",
                    "type": "authentication_error",
                    "code": 401,
                }
            },
        )
        return False

    def proxy(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        preferred_worker_id: str | None = None,
        allow_fallback: bool = True,
        request_id: str | None = None,
        trace_id: str | None = None,
        started_at: float | None = None,
        event_phase: str = "worker_selected",
        event_decision: str = "no_op",
        event_fallback_required: bool | None = None,
        event_fallback_reason: str | None = None,
    ) -> None:
        started_at = started_at if started_at is not None else self.state.begin_request()
        request_id = request_id or new_opaque_id("req")
        trace_id = trace_id or new_opaque_id("trace")
        worker_id: str | None = None
        status = 500
        try:
            decision_start = time.perf_counter()
            request_model = body.get("model") if isinstance(body, dict) and isinstance(body.get("model"), str) else None
            scheduler_trace: dict[str, Any] = {}
            candidates = self.state.candidate_workers(
                preferred_worker_id,
                allow_fallback=allow_fallback,
                model=request_model,
                rotate_cold=preferred_worker_id is None,
                scheduler_trace=scheduler_trace,
            )
            decision_latency_ms = (time.perf_counter() - decision_start) * 1000.0
            last_error = ""
            fallback_reason_code: str | None = None
            streaming_request = isinstance(body, dict) and body.get("stream") is True
            selected: tuple[WorkerRuntime, dict[str, Any], int, dict[str, str], bytes] | None = None
            selected_stream: tuple[WorkerRuntime, dict[str, Any], int, dict[str, str], Any, float] | None = None
            for index, worker in enumerate(candidates):
                availability = worker.availability()
                url = worker.url + path
                admission = self.state.acquire_worker_slot(worker)
                if not admission.get("acquired"):
                    reason = str(admission.get("reason") or "queue_rejected")
                    self.state.record_queue_rejected(worker.worker_id, reason=reason)
                    last_error = f"{worker.worker_id}: queue admission rejected: {reason}"
                    fallback_reason_code = "worker_capacity"
                    if allow_fallback and index < len(candidates) - 1:
                        continue
                    raise QueueBackpressureError(reason, worker.worker_id)
                stream_opened = False
                try:
                    if streaming_request:
                        data = None if body is None else json.dumps(body).encode("utf-8")
                        req = urllib.request.Request(
                            url,
                            data=data,
                            headers={"Accept-Encoding": "identity", "Content-Type": "application/json"},
                            method=method,
                        )
                        stream_started_at = time.perf_counter()
                        resp = urllib.request.urlopen(req, timeout=self.state.args.timeout)
                        stream_opened = True
                        headers = {k.lower(): v for k, v in resp.headers.items()}
                        selected_stream = (worker, availability, int(resp.status), headers, resp, stream_started_at)
                        break
                    candidate_status, headers, raw, _ = http_request(method, url, payload=body, timeout=self.state.args.timeout)
                except urllib.error.HTTPError as exc:
                    raw = exc.read()
                    candidate_status = int(exc.code)
                    headers = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
                    if looks_like_loading_response(candidate_status, raw) and allow_fallback and index < len(candidates) - 1:
                        last_error = f"{worker.worker_id}: backend returned loading 503"
                        fallback_reason_code = "worker_capacity"
                        self.state.worker_readiness(worker, refresh=True, include_sidecar=False)
                        continue
                    selected = (worker, availability, candidate_status, headers, raw)
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = f"{worker.worker_id}: {type(exc).__name__}: {exc}"
                    if allow_fallback and index < len(candidates) - 1:
                        fallback_reason_code = "worker_unavailable"
                        self.state.worker_readiness(worker, refresh=True, include_sidecar=False)
                        continue
                    raise RuntimeError(f"no ready cache-router worker available; last_backend_error={last_error}") from exc
                finally:
                    if not stream_opened:
                        self.state.finish_worker_attempt(worker.worker_id)
                if looks_like_loading_response(candidate_status, raw) and allow_fallback and index < len(candidates) - 1:
                    last_error = f"{worker.worker_id}: backend returned loading 503"
                    fallback_reason_code = "worker_capacity"
                    self.state.worker_readiness(worker, refresh=True, include_sidecar=False)
                    continue
                selected = (worker, availability, candidate_status, headers, raw)
                break
            if selected is None and selected_stream is None:
                raise RuntimeError(f"no ready cache-router worker available; last_backend_error={last_error}")
            if selected_stream is not None:
                worker, availability, status, headers, resp, stream_started_at = selected_stream
                ttft_ms: float | None = None
            else:
                worker, availability, status, headers, raw = selected  # type: ignore[misc]
                ttft_ms = extract_backend_ttft_ms(raw)
            worker_id = worker.worker_id
            self.state.record_worker_selected(worker.worker_id, reason=str(availability.get("reason", "unknown")))
            event_emitted = False

            def emit_proxy_event(observed_ttft_ms: float | None) -> None:
                nonlocal event_emitted
                if event_emitted:
                    return
                event_emitted = True
                self.state.emit_event(
                    phase=event_phase,
                    decision=event_decision,
                    cache_id=request_id,
                    cache_key_hash=None,
                    manifest_id=None,
                    cache_hit_level="none",
                    compatibility_result="not_checked",
                    latency_ms=decision_latency_ms,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    ttft_ms=observed_ttft_ms,
                    fallback_required=event_fallback_required if event_fallback_required is not None else bool(last_error),
                    fallback_reason=event_fallback_reason if event_fallback_reason is not None else fallback_reason_code,
                    worker_id=worker.worker_id,
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=request_model,
                    cache_event_basis="request_metadata",
                    restore_observed_basis="not_checked",
                    scheduler=scheduler_trace,
                    notes=f"OpenAI proxy selected worker by availability={availability.get('reason', 'unknown')} status={status} candidate_count={len(candidates)}.",
                )
            self.send_response(status)
            content_type = headers.get("content-type", "application/json")
            self.send_header("Content-Type", content_type)
            if selected_stream is None:
                self.send_header("Content-Length", str(len(raw)))
            elif headers.get("cache-control"):
                self.send_header("Cache-Control", headers["cache-control"])
            self.send_header("X-Cache-Router-Request-ID", request_id)
            self.send_header("X-Cache-Router-Trace-ID", trace_id)
            self.send_header("X-Cache-Router-Worker", worker.worker_id)
            self.send_header("X-Cache-Router-Worker-Availability", str(availability.get("reason", "unknown")))
            self.send_header("X-Cache-Router-Worker-Busy-Score", str(availability.get("busy_score", "unknown")))
            self.end_headers()
            if selected_stream is None:
                emit_proxy_event(ttft_ms)
                self.wfile.write(raw)
            else:
                try:
                    read_chunk = resp.read1 if hasattr(resp, "read1") else resp.read
                    while True:
                        chunk = read_chunk(8192)
                        if not chunk:
                            break
                        if ttft_ms is None:
                            ttft_ms = (time.perf_counter() - stream_started_at) * 1000.0
                        self.wfile.write(chunk)
                        self.wfile.flush()
                finally:
                    resp.close()
                    self.state.finish_worker_attempt(worker.worker_id)
                    emit_proxy_event(ttft_ms)
        except (KeyError, RuntimeError) as exc:
            status, code, message = self.route_error(exc)
            self.send_error_json(
                status,
                message,
                code=code,
                extra_headers={
                    "X-Cache-Router-Request-ID": request_id,
                    "X-Cache-Router-Trace-ID": trace_id,
                    "X-Cache-Router-Worker": worker_id or "none",
                },
            )
        finally:
            self.state.finish_request(method=method, path=path, status=status, started_at=started_at, worker_id=worker_id)

    def do_GET(self) -> None:  # noqa: N802
        try:
            if self.path == "/health":
                security = {
                    "auth_required": bool(self.state.auth_token),
                    "production_mode": bool(getattr(self.state.args, "production_mode", False)),
                    "admin_endpoints_enabled": not bool(getattr(self.state.args, "disable_admin_endpoints", False)),
                    "trusted_lan_unauthenticated": not bool(self.state.auth_token),
                }
                authorized_health = not self.state.auth_token or self.authorized()
                body: dict[str, Any] = {"status": "ok", "security": security}
                if authorized_health:
                    summaries = self.state.worker_summaries(include_slots=False)
                    healthy = sum(1 for row in summaries if row.get("health", {}).get("ok"))
                    ready = sum(1 for row in summaries if row.get("readiness", {}).get("ok"))
                    route_ready = sum(
                        1
                        for row in summaries
                        if row.get("readiness", {}).get("ok") and routable_availability(row.get("availability", {}))
                    )
                    body["status"] = "ok" if route_ready else "degraded"
                    body.update(
                        {
                            "router": {"pid": os.getpid(), "bind": self.state.args.host, "port": self.state.args.port, "security": security},
                            "workers": summaries,
                            "worker_count": len(summaries),
                            "healthy_workers": healthy,
                            "ready_workers": ready,
                            "route_ready_workers": route_ready,
                            "cache_root": str(self.state.cache_root),
                        }
                    )
                self.send_json(200 if body["status"] == "ok" else 503, body)
                return
            if not self.require_auth():
                return
            disabled_admin_path = admin_route_path(self.path)
            if disabled_admin_path and getattr(self.state.args, "disable_admin_endpoints", False):
                self.send_error_json(404, f"admin endpoint is disabled: {disabled_admin_path}", code="not_found")
                return
            if self.path in {"/v1", "/v1/"}:
                endpoints = [
                    "/v1/models",
                    "/v1/completions",
                    "/v1/chat/completions",
                    "/tokenize",
                ]
                if not getattr(self.state.args, "disable_admin_endpoints", False):
                    endpoints.extend(["/router/status", "/router/workers", "/router/cache", "/router/decisions", "/metrics"])
                self.send_json(
                    200,
                    {
                        "object": "cache_router.endpoint",
                        "status": "ok",
                        "base_url": "/v1",
                        "model": self.state.args.model,
                        "endpoints": endpoints,
                        "cache_extension": "optional cache_router object on completions/chat completions",
                    },
                )
            elif self.path == "/v1/models":
                models = self.state.ready_model_ids()
                if not models:
                    self.send_error_json(503, "no ready cache-router worker available", code="service_unavailable")
                    return
                self.send_json(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {
                                "id": model,
                                "object": "model",
                                "created": int(time.time()),
                                "owned_by": "llamacpp-cache-router",
                            }
                            for model in models
                        ],
                    },
                )
            elif self.path == "/router/status":
                registry = self.state.load_registry()
                workers = self.state.worker_summaries(include_slots=False)
                healthy = sum(1 for row in workers if row.get("health", {}).get("ok"))
                ready = sum(1 for row in workers if row.get("readiness", {}).get("ok"))
                route_ready = sum(
                    1
                    for row in workers
                    if row.get("readiness", {}).get("ok") and routable_availability(row.get("availability", {}))
                )
                self.send_json(
                    200,
                    {
                        "status": "ok" if route_ready else "degraded",
                        "router": {
                            "pid": os.getpid(),
                            "bind": self.state.args.host,
                            "port": self.state.args.port,
                            "security": {
                                "auth_required": bool(self.state.auth_token),
                                "production_mode": bool(getattr(self.state.args, "production_mode", False)),
                                "admin_endpoints_enabled": not bool(getattr(self.state.args, "disable_admin_endpoints", False)),
                                "trusted_lan_unauthenticated": not bool(self.state.auth_token),
                            },
                        },
                        "workers": workers,
                        "worker_count": len(workers),
                        "healthy_workers": healthy,
                        "ready_workers": ready,
                        "route_ready_workers": route_ready,
                        "cache_root": str(self.state.cache_root),
                        "registry_entries": len(registry.get("entries", [])),
                        "inventory": self.state.inventory_status(),
                    },
                )
            elif self.path == "/router/workers":
                workers = self.state.worker_summaries()
                route_ready = sum(
                    1
                    for row in workers
                    if row.get("readiness", {}).get("ok") and routable_availability(row.get("availability", {}))
                )
                self.send_json(
                    200,
                    {
                        "workers": workers,
                        "count": len(workers),
                        "healthy": sum(1 for row in workers if row.get("health", {}).get("ok")),
                        "ready": sum(1 for row in workers if row.get("readiness", {}).get("ok")),
                        "route_ready": route_ready,
                    },
                )
            elif self.path == "/router/cache":
                registry = self.state.load_registry()
                entries = []
                for row in registry.get("entries", []):
                    entries.append(
                        {
                            "cache_id": row.get("cache_id"),
                            "cache_key_hash": row.get("cache_key_hash"),
                            "manifest_id": row.get("manifest_id"),
                            "slot_file_sha256": row.get("slot_file_sha256"),
                            "slot_file_size_bytes": row.get("slot_file_size_bytes"),
                            "created_at": row.get("created_at"),
                            "last_used_at": row.get("last_used_at"),
                            "validation_status": row.get("validation_status"),
                            "quarantine_reason": row.get("quarantine_reason"),
                            "quarantined_at": row.get("quarantined_at"),
                            "worker_residency": row.get("worker_residency"),
                        }
                    )
                self.send_json(200, {"entries": entries, "count": len(entries)})
            elif urllib.parse.urlparse(self.path).path == "/router/decisions":
                parsed = urllib.parse.urlparse(self.path)
                params = urllib.parse.parse_qs(parsed.query)
                request_id = (params.get("request_id") or [""])[0].strip() or None
                try:
                    limit = int((params.get("limit") or ["100"])[0])
                except ValueError:
                    limit = 100
                limit = min(1000, max(1, limit))
                rows = filter_decision_rows(read_jsonl_tail(self.state.events_path, limit), request_id=request_id)
                self.send_json(200, {"events": rows, "count": len(rows), "limit": limit, "request_id": request_id})
            elif self.path == "/metrics":
                self.send_text(200, self.state.metrics_text())
            else:
                self.send_error_json(404, f"unknown path: {self.path}", code="not_found")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc))

    def handle_cached_completion(self, body: dict[str, Any], *, chat: bool) -> None:
        request_id = new_opaque_id("req")
        trace_id = new_opaque_id("trace")
        response_headers = {"X-Cache-Router-Request-ID": request_id, "X-Cache-Router-Trace-ID": trace_id, "X-Cache-Router-Worker": "none"}
        extension = body.get("cache_router") or {}
        if not isinstance(extension, dict):
            self.send_error_json(400, "cache_router must be an object", extra_headers=response_headers)
            return
        mode = str(extension.get("mode", "auto"))
        if body.get("stream") and mode != "bypass":
            self.send_error_json(400, "cached mode is currently non-streaming; use stream=false", extra_headers=response_headers)
            return
        cache_id = str(extension.get("cache_id") or "default")
        expected_cache_key_hash = extension.get("cache_key_hash")
        request_supplied_cache_key_hash = expected_cache_key_hash is not None
        derived_cache_key_hash = False
        if expected_cache_key_hash is not None:
            expected_cache_key_hash = str(expected_cache_key_hash).strip()
            if not is_sha256_hex(expected_cache_key_hash):
                self.send_error_json(400, "cache_router.cache_key_hash must be a sha256 hex string", code="invalid_request_error", extra_headers=response_headers)
                return
        requested_worker_id = extension.get("worker_id")
        if requested_worker_id is not None:
            requested_worker_id = str(requested_worker_id)
        allow_fallback = extension.get("allow_fallback", True)
        if not isinstance(allow_fallback, bool):
            self.send_error_json(400, "cache_router.allow_fallback must be a boolean", extra_headers=response_headers)
            return
        measure_true_ttft = extension.get("measure_true_ttft", False)
        if not isinstance(measure_true_ttft, bool):
            self.send_error_json(400, "cache_router.measure_true_ttft must be a boolean", code="invalid_request_error", extra_headers=response_headers)
            return
        restore_generation_strategy = str(extension.get("restore_generation_strategy") or "full_prompt_after_slot_restore")
        if restore_generation_strategy not in {"full_prompt_after_slot_restore", "suffix_only_after_slot_restore"}:
            self.send_error_json(
                400,
                "cache_router.restore_generation_strategy must be full_prompt_after_slot_restore or suffix_only_after_slot_restore",
                code="invalid_request_error",
                extra_headers=response_headers,
            )
            return
        prefix_text = extension.get("prefix_text")
        suffix_text = extension.get("suffix_text")
        if suffix_text is None:
            suffix_text = body.get("prompt", "")
        if not isinstance(suffix_text, str):
            self.send_error_json(400, "suffix_text or prompt must be a string", extra_headers=response_headers)
            return
        request_model = body.get("model") if isinstance(body.get("model"), str) else self.state.args.model
        max_tokens = int(body.get("max_tokens") or body.get("max_completion_tokens") or 16)
        generation_options = native_completion_options(body)
        if mode == "bypass":
            clean = dict(body)
            clean.pop("cache_router", None)
            self.proxy(
                "POST",
                "/v1/chat/completions" if chat else "/v1/completions",
                clean,
                preferred_worker_id=requested_worker_id,
                allow_fallback=allow_fallback,
                request_id=request_id,
                trace_id=trace_id,
            )
            return
        try:
            cache_policy = cache_policy_from_extension(extension)
            restore_validation_mode = restore_validation_mode_from_extension(extension)
            restore_validation_recompute = restore_validation_recompute_from_extension(extension, restore_validation_mode)
        except ValueError as exc:
            self.send_error_json(400, str(exc), code="invalid_request_error", extra_headers=response_headers)
            return
        started_at = self.state.begin_request()
        status_for_metrics = 500
        worker_for_metrics: str | None = None
        try:
            metadata: dict[str, Any] = {"mode": mode, "cache_id": cache_id}
            if expected_cache_key_hash is not None:
                metadata["cache_key_hash"] = expected_cache_key_hash
            if requested_worker_id:
                metadata["requested_worker_id"] = requested_worker_id
            metadata["allow_fallback"] = allow_fallback
            metadata["restore_generation_strategy"] = restore_generation_strategy
            if measure_true_ttft:
                metadata["measure_true_ttft"] = True
            metadata["policy"] = policy_metadata(cache_policy)
            metadata["generation_settings_hash"] = sha256_json(generation_options)
            if restore_validation_mode != "off":
                metadata["restore_validation"] = {
                    "mode": restore_validation_mode,
                    "recompute_on_mismatch": restore_validation_recompute,
                }
            if denial_reason := request_policy_denial_reason(cache_policy):
                self.state.emit_event(
                    phase="registry_lookup",
                    decision="reject_policy",
                    cache_id=cache_id,
                    cache_key_hash=None,
                    manifest_id=None,
                    cache_hit_level="registry_only",
                    compatibility_result="policy_denied",
                    validation_status="not_checked",
                    latency_ms=None,
                    prompt_tokens=None,
                    processed_prompt_tokens=None,
                    cached_tokens=None,
                    generated_tokens=None,
                    prompt_tps=None,
                    eval_tps=None,
                    fallback_required=True,
                    fallback_reason="policy_denied",
                    request_id=request_id,
                    trace_id=trace_id,
                    model_id=request_model,
                    cache_event_basis="registry_lookup",
                    restore_observed_basis="not_checked",
                    cache_policy=cache_policy,
                    policy_denial_reason=denial_reason,
                    notes=f"Cached request denied by tenant/scope policy before lookup: {denial_reason}.",
                )
                raise CachePolicyDeniedError(denial_reason)
            if mode in {"use", "auto"} and expected_cache_key_hash is None:
                if not isinstance(prefix_text, str) or not prefix_text:
                    status_for_metrics = 400
                    self.send_error_json(
                        400,
                        "cache_router.cache_key_hash or cache_router.prefix_text is required for strict cache lookup",
                        code="invalid_request_error",
                        extra_headers=response_headers,
                    )
                    return
                strict_candidates = self.state.request_cache_key_candidates(
                    cache_id=cache_id,
                    prefix_text=prefix_text,
                    cache_policy=cache_policy,
                    worker_id=requested_worker_id,
                    model=request_model,
                    allow_fallback=allow_fallback,
                )
                exact_candidate = next(
                    (
                        row
                        for row in strict_candidates
                        if self.state.find_entry(cache_id, cache_policy=cache_policy, expected_cache_key_hash=str(row["cache_key_hash"])) is not None
                    ),
                    None,
                )
                expected_cache_key_hash = str((exact_candidate or strict_candidates[0])["cache_key_hash"])
                derived_cache_key_hash = True
                metadata["cache_key_hash"] = expected_cache_key_hash
                metadata["cache_key_hash_basis"] = "request_prefix_runtime"
            if mode in {"build", "refresh"}:
                if not isinstance(prefix_text, str) or not prefix_text:
                    status_for_metrics = 400
                    self.send_error_json(400, "cache_router.prefix_text is required for build/refresh", extra_headers=response_headers)
                    return
                metadata["build"] = self.state.build_cache(
                    cache_id=cache_id,
                    prefix_text=prefix_text,
                    refresh=(mode == "refresh"),
                    worker_id=requested_worker_id,
                    model=request_model,
                    request_id=request_id,
                    trace_id=trace_id,
                    cache_policy=cache_policy,
                    expected_cache_key_hash=expected_cache_key_hash if mode == "refresh" else None,
                )
                worker_for_metrics = metadata["build"].get("worker_id")
                if worker_for_metrics:
                    response_headers["X-Cache-Router-Worker"] = str(worker_for_metrics)
                text = json.dumps({"cache_built": True, "cache_id": cache_id})
                status_for_metrics = 200
                self.send_json(
                    200,
                    openai_chat_response(model=request_model, content=text, cache_router=metadata)
                    if chat
                    else openai_completion_response(model=request_model, text=text, prompt_tokens=0, completion_tokens=0, cache_router=metadata),
                    extra_headers=response_headers,
                )
                return
            if mode == "auto":
                exact_entry = self.state.find_entry(cache_id, cache_policy=cache_policy, expected_cache_key_hash=expected_cache_key_hash)
                if exact_entry is None and not request_supplied_cache_key_hash:
                    if not isinstance(prefix_text, str) or not prefix_text:
                        self.state.emit_event(
                            phase="registry_lookup",
                            decision="no_op",
                            cache_id=cache_id,
                            cache_key_hash=expected_cache_key_hash,
                            manifest_id=None,
                            cache_hit_level="none",
                            compatibility_result="miss",
                            latency_ms=None,
                            prompt_tokens=None,
                            processed_prompt_tokens=None,
                            cached_tokens=None,
                            generated_tokens=None,
                            prompt_tps=None,
                            eval_tps=None,
                            fallback_required=False,
                            request_id=request_id,
                            trace_id=trace_id,
                            model_id=request_model,
                            cache_event_basis="registry_lookup",
                            restore_observed_basis="not_checked",
                            cache_policy=cache_policy,
                            notes="Auto cache lookup missed but prefix_text was unavailable for build.",
                        )
                        status_for_metrics = 400
                        self.send_error_json(400, "cache miss in auto mode requires cache_router.prefix_text", extra_headers=response_headers)
                        return
                    self.state.emit_event(
                        phase="registry_lookup",
                        decision="cold_prefill",
                        cache_id=cache_id,
                        cache_key_hash=None,
                        manifest_id=None,
                        cache_hit_level="none",
                        compatibility_result="miss",
                        latency_ms=None,
                        prompt_tokens=None,
                        processed_prompt_tokens=None,
                        cached_tokens=None,
                        generated_tokens=None,
                        prompt_tps=None,
                        eval_tps=None,
                        fallback_required=True,
                        fallback_reason="no_compatible_manifest",
                        request_id=request_id,
                        trace_id=trace_id,
                        model_id=request_model,
                        cache_event_basis="registry_lookup",
                        restore_observed_basis="not_checked",
                        cache_policy=cache_policy,
                        notes="Auto cache lookup missed; router will build a cache from the provided prefix.",
                    )
                    metadata["build"] = self.state.build_cache(
                        cache_id=cache_id,
                        prefix_text=prefix_text,
                        refresh=False,
                        worker_id=requested_worker_id,
                        model=request_model,
                        request_id=request_id,
                        trace_id=trace_id,
                        cache_policy=cache_policy,
                    )
                    expected_cache_key_hash = str(metadata["build"].get("cache_key_hash") or "")
                    if expected_cache_key_hash:
                        metadata["cache_key_hash"] = expected_cache_key_hash
                        if derived_cache_key_hash:
                            metadata["cache_key_hash_basis"] = "built_from_request_prefix"
                use = self.state.use_cache(
                    cache_id=cache_id,
                    prefix_text=prefix_text,
                    suffix_text=suffix_text,
                    max_tokens=max_tokens,
                    generation_options=generation_options,
                    worker_id=requested_worker_id,
                    allow_fallback=allow_fallback,
                    model=request_model,
                    request_id=request_id,
                    trace_id=trace_id,
                    cache_policy=cache_policy,
                    expected_cache_key_hash=expected_cache_key_hash,
                    restore_validation_mode=restore_validation_mode,
                    restore_validation_recompute_on_mismatch=restore_validation_recompute,
                    measure_true_ttft=measure_true_ttft,
                    restore_generation_strategy=restore_generation_strategy,
                )
            elif mode == "use":
                use = self.state.use_cache(
                    cache_id=cache_id,
                    prefix_text=prefix_text,
                    suffix_text=suffix_text,
                    max_tokens=max_tokens,
                    generation_options=generation_options,
                    worker_id=requested_worker_id,
                    allow_fallback=allow_fallback,
                    model=request_model,
                    request_id=request_id,
                    trace_id=trace_id,
                    cache_policy=cache_policy,
                    expected_cache_key_hash=expected_cache_key_hash,
                    restore_validation_mode=restore_validation_mode,
                    restore_validation_recompute_on_mismatch=restore_validation_recompute,
                    measure_true_ttft=measure_true_ttft,
                    restore_generation_strategy=restore_generation_strategy,
                )
            else:
                status_for_metrics = 400
                self.send_error_json(400, f"unsupported cache_router mode: {mode}", extra_headers=response_headers)
                return
            metadata["use"] = use
            worker_for_metrics = use.get("worker_id")
            if worker_for_metrics:
                response_headers["X-Cache-Router-Worker"] = str(worker_for_metrics)
            attempts = use.get("attempts") if isinstance(use.get("attempts"), list) else []
            if attempts and isinstance(attempts[-1], dict) and attempts[-1].get("cache_hit_level"):
                response_headers["X-Cache-Router-Cache-Hit-Level"] = str(attempts[-1].get("cache_hit_level"))
            content = use["completion"]["content"]
            prompt_tokens = int(use["completion"].get("tokens_evaluated") or 0)
            completion_tokens = int(use["completion"].get("tokens_predicted") or 0)
            if chat:
                response = openai_chat_response(model=request_model, content=content, cache_router=metadata)
                response["usage"] = {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                }
            else:
                response = openai_completion_response(
                    model=request_model,
                    text=content,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    cache_router=metadata,
                )
            status_for_metrics = 200
            self.send_json(200, response, extra_headers=response_headers)
        except CacheKeyMismatchError as exc:
            can_reconstruct_cold_prompt = not chat and isinstance(prefix_text, str) and bool(prefix_text)
            if allow_fallback and mode in {"auto", "use"} and can_reconstruct_cold_prompt:
                clean = dict(body)
                clean.pop("cache_router", None)
                clean["prompt"] = prefix_text + suffix_text
                status_for_metrics = None
                self.proxy(
                    "POST",
                    "/v1/chat/completions" if chat else "/v1/completions",
                    clean,
                    preferred_worker_id=requested_worker_id,
                    allow_fallback=True,
                    request_id=request_id,
                    trace_id=trace_id,
                    started_at=started_at,
                    event_phase="cold_prefill_selected",
                    event_decision="cold_prefill",
                    event_fallback_required=True,
                    event_fallback_reason="cache_key_mismatch",
                )
                return
            status_for_metrics = 404
            self.send_error_json(404, str(exc), code="cache_not_found", extra_headers=response_headers)
        except KeyError as exc:
            self.state.emit_event(
                phase="registry_lookup",
                decision="no_op",
                cache_id=cache_id,
                cache_key_hash=expected_cache_key_hash,
                manifest_id=None,
                cache_hit_level="none",
                compatibility_result="miss",
                latency_ms=None,
                prompt_tokens=None,
                processed_prompt_tokens=None,
                cached_tokens=None,
                generated_tokens=None,
                prompt_tps=None,
                eval_tps=None,
                fallback_required=False,
                request_id=request_id,
                trace_id=trace_id,
                model_id=request_model,
                cache_event_basis="registry_lookup",
                restore_observed_basis="not_checked",
                cache_policy=cache_policy,
                notes="Cache lookup missed for a use request.",
            )
            status_for_metrics = 404
            self.send_error_json(404, str(exc), code="cache_not_found", extra_headers=response_headers)
        except CachePolicyDeniedError as exc:
            status_for_metrics, code, message = self.route_error(exc)
            self.send_error_json(status_for_metrics, message, code=code, extra_headers=response_headers)
        except RegistryLeaseConflictError as exc:
            status_for_metrics, code, message = self.route_error(exc)
            self.send_error_json(status_for_metrics, message, code=code, extra_headers=response_headers)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            cache_failure_reason = "restore_validation_failed"
            if "hydrated" in str(exc) or "hydrate" in str(exc) or "router blob" in str(exc):
                cache_failure_reason = "hydration_failed"
            elif "quarantined" in str(exc) or "manifest" in str(exc) or "registry entry" in str(exc):
                cache_failure_reason = "manifest_quarantined"
            elif "incompatible cache" in str(exc) or "cache_key" in str(exc):
                cache_failure_reason = "cache_key_mismatch"
            can_reconstruct_cold_prompt = not chat and isinstance(prefix_text, str) and bool(prefix_text)
            if allow_fallback and mode in {"auto", "use"} and can_reconstruct_cold_prompt:
                clean = dict(body)
                clean.pop("cache_router", None)
                clean["prompt"] = prefix_text + suffix_text
                status_for_metrics = None
                self.proxy(
                    "POST",
                    "/v1/chat/completions" if chat else "/v1/completions",
                    clean,
                    preferred_worker_id=requested_worker_id,
                    allow_fallback=True,
                    request_id=request_id,
                    trace_id=trace_id,
                    started_at=started_at,
                    event_phase="cold_prefill_selected",
                    event_decision="cold_prefill",
                    event_fallback_required=True,
                    event_fallback_reason=cache_failure_reason,
                )
                return
            if allow_fallback and mode in {"auto", "use"} and not can_reconstruct_cold_prompt:
                exc = RuntimeError(f"{exc}; cold fallback requires cache_router.prefix_text to reconstruct the full prompt")
            status_for_metrics, code, message = self.route_error(exc)
            self.send_error_json(status_for_metrics, message, code=code, extra_headers=response_headers)
        finally:
            if status_for_metrics is not None:
                self.state.finish_request(method="POST", path=self.path, status=status_for_metrics, started_at=started_at, worker_id=worker_for_metrics)

    def do_POST(self) -> None:  # noqa: N802
        try:
            if not self.require_auth():
                return
            body = self.read_body()
            if not isinstance(body, dict):
                self.send_error_json(400, "request body must be a JSON object")
                return
            if self.path == "/v1/completions":
                if not self.require_configured_model(body):
                    return
                if "cache_router" in body:
                    self.handle_cached_completion(body, chat=False)
                else:
                    self.proxy("POST", self.path, body)
            elif self.path == "/v1/chat/completions":
                if not self.require_configured_model(body):
                    return
                if "cache_router" in body:
                    self.handle_cached_completion(body, chat=True)
                else:
                    self.proxy("POST", self.path, body)
            elif self.path == "/tokenize":
                self.proxy("POST", self.path, body)
            else:
                self.send_error_json(404, f"unknown path: {self.path}", code="not_found")
        except ValueError as exc:
            self.send_error_json(400, str(exc), code="invalid_json")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--worker-url", default="http://127.0.0.1:18082")
    parser.add_argument("--worker-id", default="worker-main")
    parser.add_argument("--workers-file", default="", help="Optional JSON inventory with a workers list. If omitted, single-worker CLI flags are used.")
    parser.add_argument("--auth-token", default="", help="Optional bearer token for routers that should require client auth.")
    parser.add_argument("--auth-token-file", default="", help="Optional file containing a bearer token for client auth.")
    parser.add_argument("--allow-unauthenticated-lan", action="store_true", help="Required to bind an unauthenticated router to a non-loopback address. Use only on a trusted private LAN.")
    parser.add_argument("--disable-admin-endpoints", action="store_true", help="Disable /router/* inspection endpoints and /metrics while leaving OpenAI-compatible routes available.")
    parser.add_argument("--production-mode", action="store_true", help="Require authenticated production posture: auth token present, no unauthenticated LAN flag, and admin endpoints disabled unless explicitly allowed.")
    parser.add_argument("--allow-production-admin-endpoints", action="store_true", help="Keep authenticated admin endpoints enabled when --production-mode is set.")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--auto-rebuild-registry", action=argparse.BooleanOptionalAction, default=True, help="Automatically rebuild registry.json from validated manifests when the registry is missing or invalid.")
    parser.add_argument("--rebuild-registry", action="store_true", help="Rebuild registry.json from validated manifests on startup before serving requests.")
    parser.add_argument("--replay-registry-wal", action=argparse.BooleanOptionalAction, default=True, help="Replay registry WAL intent records on startup and rebuild registry.json from manifests when committed manifests are missing from the registry.")
    parser.add_argument("--durable-blob-encryption-mode", default="", choices=["", "operator_managed_encrypted_filesystem", "platform_encrypted_volume"], help="Optional operator-attested encryption-at-rest mode for router-owned durable blobs.")
    parser.add_argument("--durable-blob-encryption-evidence-basis", default="", choices=["", "operator_attestation", "setup_doctor_metadata"], help="Basis for durable blob encryption-at-rest metadata when the operator attests the cache root is encrypted.")
    parser.add_argument("--durable-blob-encryption-volume-id-hash", default="", help="Lowercase SHA-256 digest of the operator-managed encrypted volume identifier; do not pass raw volume names or keys.")
    parser.add_argument("--durable-blob-encryption-key-owner", default="", help="Short non-secret label for the operator or platform that owns the at-rest encryption key.")
    parser.add_argument("--worker-slot-dir", default="")
    parser.add_argument("--worker-transport", choices=["local", "ssh", "http"], default="local")
    parser.add_argument("--worker-ssh-host", default="")
    parser.add_argument("--worker-sidecar-url", default="")
    parser.add_argument("--ssh-config", default="")
    parser.add_argument("--ssh-extra-args", default="")
    parser.add_argument("--scp-extra-args", default="")
    parser.add_argument("--model", default="model")
    parser.add_argument("--model-path", default="")
    parser.add_argument("--model-file-size", type=int, default=0)
    parser.add_argument("--model-architecture", default="not_captured")
    parser.add_argument("--derive-strict-metadata", action=argparse.BooleanOptionalAction, default=True, help="Fill missing strict cache compatibility fields from worker runtime APIs such as /v1/models, /props, and /tokenize.")
    parser.add_argument("--strict-metadata-force-runtime", action=argparse.BooleanOptionalAction, default=False, help="When deriving strict metadata, prefer runtime-computed strict fields over hand-entered inventory values.")
    parser.add_argument("--strict-metadata-timeout", type=float, default=5.0, help="HTTP timeout in seconds for strict metadata runtime probes.")
    parser.add_argument("--model-hash", default="not_captured")
    parser.add_argument("--gguf-tensor-manifest-hash", default="not_captured")
    parser.add_argument("--tokenizer-hash", default="not_captured")
    parser.add_argument("--chat-template-effective-hash", default="not_captured")
    parser.add_argument("--tools-schema-hash", default="not_captured")
    parser.add_argument("--system-prompt-hash", default="not_captured")
    parser.add_argument("--special-token-policy", default="not_captured")
    parser.add_argument("--llama-server-path", default="")
    parser.add_argument("--llama-server-version", default="unknown")
    parser.add_argument("--llama-cpp-source-commit", default="not_captured")
    parser.add_argument("--llama-cpp-cache-abi-version", default="not_captured")
    parser.add_argument("--patchset-id", default="not_captured")
    parser.add_argument("--build-backend", default="not_captured")
    parser.add_argument("--gpu-backend-driver", default="not_captured")
    parser.add_argument("--kv-unified-mode", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ctx-size", type=int, default=65536)
    parser.add_argument("--ctx-checkpoints-config", default="not_captured")
    parser.add_argument("--cache-type-k", default="q8_0")
    parser.add_argument("--cache-type-v", default="q8_0")
    parser.add_argument("--flash-attention-mode", default="not_captured")
    parser.add_argument("--rope-freq-base", default="not_captured")
    parser.add_argument("--rope-freq-scale", default="not_captured")
    parser.add_argument("--yarn-or-rope-scaling-metadata", default="not_captured")
    parser.add_argument("--reasoning-format", default="not_captured")
    parser.add_argument("--jinja-template-mode", default="not_captured")
    parser.add_argument("--mtp-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--spec-draft-model-path", default="")
    parser.add_argument("--spec-draft-model-size", type=int, default=0)
    parser.add_argument("--spec-draft-model-hash", default="not_captured")
    parser.add_argument("--spec-draft-config", default="not_captured")
    parser.add_argument("--n-parallel", type=int, default=1)
    parser.add_argument("--n-seq-max", type=int, default=1)
    parser.add_argument("--slot-id", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--readiness-poll-interval", type=float, default=1.0, help="Seconds between worker /health and /v1/models readiness polls. Use 0 for per-request probing only.")
    parser.add_argument("--readiness-timeout", type=float, default=5.0, help="HTTP timeout in seconds for worker readiness probes.")
    parser.add_argument("--inventory-reload-interval", type=float, default=5.0, help="Seconds between workers-file hot-reload checks. Use 0 to disable inventory reload.")
    parser.add_argument("--queue-limit-per-worker", type=int, default=0, help="Maximum queued normal proxy requests per worker. Zero disables router-side queueing/backpressure.")
    parser.add_argument("--queue-wait-timeout", type=float, default=0.0, help="Maximum seconds a queued normal proxy request may wait for a worker slot before a bounded 503 response.")
    args = parser.parse_args()
    if not args.workers_file:
        if not args.worker_slot_dir:
            parser.error("--worker-slot-dir is required unless --workers-file is provided")
        if not args.model_path:
            parser.error("--model-path is required unless --workers-file is provided")
        if not args.llama_server_path:
            parser.error("--llama-server-path is required unless --workers-file is provided")
    if args.production_mode:
        if args.allow_unauthenticated_lan:
            parser.error("--production-mode cannot be combined with --allow-unauthenticated-lan")
        if not load_auth_token(args):
            parser.error("--production-mode requires --auth-token or --auth-token-file")
        if not args.allow_production_admin_endpoints:
            args.disable_admin_endpoints = True
    if not is_loopback_bind(args.host) and not load_auth_token(args) and not args.allow_unauthenticated_lan:
        parser.error("--host is not loopback and router auth is disabled; pass --allow-unauthenticated-lan only for a trusted private LAN")
    return args


def main() -> int:
    args = parse_args()
    state = CacheRouterState(args)
    server = ThreadingHTTPServer((args.host, args.port), RouterHandler)
    server.state = state  # type: ignore[attr-defined]
    print(json.dumps({"status": "starting", "pid": os.getpid(), "bind": args.host, "port": args.port, "cache_root": args.cache_root}), flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
