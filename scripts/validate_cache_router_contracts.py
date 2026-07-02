#!/usr/bin/env python3
"""Offline validation for cache-router decision and validation contracts."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

DECISION_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-decision-event.schema.json"
VALIDATION_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-validation-result.schema.json"
MANIFEST_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-manifest.schema.json"
WORKER_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/worker-capabilities.schema.json"
POLICY_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-policy.schema.json"
DECISION_TRACE_PATH = PACKAGE_ROOT / "docs/architecture/examples/cache-router-decision-trace.jsonl"
VALIDATION_RESULTS_PATH = PACKAGE_ROOT / "docs/architecture/examples/cache-router-validation-results.jsonl"
NEGATIVE_FIXTURES_PATH = PACKAGE_ROOT / "docs/architecture/examples/negative/cache-router-negative-fixtures.jsonl"
STRICT_KEY_NEGATIVE_FIXTURES_PATH = PACKAGE_ROOT / "docs/architecture/examples/negative/cache-router-strict-key-negative-fixtures.jsonl"
REPLAY_ROOT = PACKAGE_ROOT / "docs/architecture/examples/replay"

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
ABSOLUTE_PATH_RE = re.compile(r"^/(?:home|tmp|var|mnt|models|cache|srv|opt|root|etc|run|proc)/")
WINDOWS_PATH_RE = re.compile(r"^[A-Za-z]:\\")
URL_RE = re.compile(r"https?://[^\s]+", re.I)
HOST_PORT_RE = re.compile(r"\b(?:localhost|127\.0\.0\.1|0\.0\.0\.0|[A-Za-z][A-Za-z0-9.-]*\.[A-Za-z]{2,}):\d{2,5}\b", re.I)
SECRET_VALUE_PATTERNS = [
    re.compile(r"\bOPENAI(?:_API)?_KEY(?:\b|_)", re.I),
    re.compile(r"\bANTHROPIC(?:_API)?_KEY(?:\b|_)", re.I),
    re.compile(r"\bOPENROUTER(?:_API)?_KEY(?:\b|_)", re.I),
    re.compile(r"\bauthorization\s*[:=]\s*bearer\b", re.I),
    re.compile(r"\bbearer\b", re.I),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"BEGIN\s+(?:OPENSSH|RSA)\s+PRIVATE\s+KEY", re.I),
]
FORBIDDEN_KEYS = {
    "api_key",
    "authorization",
    "cache_blob_path",
    "cache_path",
    "content",
    "conversation_id",
    "credential",
    "env",
    "environment",
    "local_path",
    "messages",
    "password",
    "prompt",
    "raw_cache_blob_path",
    "raw_conversation_id",
    "raw_prompt",
    "raw_tenant_id",
    "secret",
    "session_id",
    "slot_path",
    "tenant_id",
    "token",
}
RISKY_KEY_PARTS = {
    "api_key",
    "authorization",
    "credential",
    "env",
    "environment",
    "host",
    "hostname",
    "message",
    "password",
    "path",
    "prompt",
    "secret",
    "tenant_id",
    "conversation_id",
    "username",
}
SAFE_PRIVACY_KEYS = {
    "base_url",
    "cache_event_basis",
    "cache_key_hash",
    "cache_type_k",
    "cache_type_v",
    "chat_template_effective_hash",
    "contains_secret_material",
    "content_address",
    "conversation_hash",
    "generated_tokens",
    "global_system_allowlisted",
    "operator_global_allowlist_hashes",
    "prefix_token_ids_hash",
    "processed_prompt_tokens",
    "prompt_tps",
    "prompt_tokens",
    "raw_cache_blob_path_logged",
    "raw_conversation_id_logged",
    "raw_environment_logged",
    "raw_prompt_logged",
    "raw_tenant_id_logged",
    "request_hash",
    "special_token_policy",
    "tenant_hash",
    "token_prefix_hash",
    "tokenizer_hash",
}
FORBIDDEN_PATH_PARTS = {
    "cache_blobs",
    "full_transcripts",
    "raw_cache",
    "raw_logs",
    "raw_slots",
    "slot_files",
    "ssd_cache",
    "transcripts",
}
CACHE_DEPENDENT_DECISIONS = {
    "durable_hit_hydrate",
    "fallback_after_restore_failure",
    "hot_local_hit",
    "restore_then_generate",
}
CACHE_DEPENDENT_PHASES = {
    "hydrate_requested",
    "restore_requested",
    "restore_validated",
}
CACHE_POSITIVE_DECISIONS = {
    "durable_hit_hydrate",
    "hot_local_hit",
    "restore_then_generate",
}
QUARANTINE_FAILURE_REASONS = {
    "checksum_mismatch",
    "deterministic_text_mismatch",
    "logits_mismatch",
    "mtp_restore_mismatch",
    "poisoning_negative_test_failed",
    "restore_api_failed",
    "size_mismatch",
    "top_k_mismatch",
}
CORRECTNESS_CHECKS = {
    "deterministic_text_match",
    "logits_match",
    "top_k_match",
}
UNKNOWN_VALUES = {"unknown", "not_captured", "not_interpreted"}
APPROVED_SCOPES = {"global_system", "tenant", "conversation", "private_disabled"}
STRICT_COMPATIBILITY_FIELDS = [
    "model_hash",
    "gguf_tensor_manifest_hash",
    "tokenizer_hash",
    "chat_template_effective_hash",
    "llama_cpp_source_commit",
    "llama_cpp_cache_abi_version",
    "build_backend",
    "gpu_backend_driver",
    "kv_unified_mode",
    "ctx_size",
    "ctx_checkpoints_config",
    "cache_type_k",
    "cache_type_v",
    "flash_attention_mode",
    "reasoning_format",
    "jinja_template_mode",
    "mtp_enabled",
    "spec_draft_model_hash",
    "spec_draft_config",
]
OPTIONAL_STRICT_COMPATIBILITY_FIELDS = [
    "model_architecture",
    "special_token_policy",
    "patchset_id",
    "rope_freq_base",
    "rope_freq_scale",
    "yarn_or_rope_scaling_metadata",
    "n_parallel",
    "n_seq_max",
]
ALL_STRICT_COMPATIBILITY_FIELDS = STRICT_COMPATIBILITY_FIELDS + OPTIONAL_STRICT_COMPATIBILITY_FIELDS
SHA_COMPATIBILITY_FIELDS = {
    "model_hash",
    "gguf_tensor_manifest_hash",
    "tokenizer_hash",
    "chat_template_effective_hash",
    "spec_draft_model_hash",
}
BOOLEAN_COMPATIBILITY_FIELDS = {"kv_unified_mode", "mtp_enabled"}
INTEGER_COMPATIBILITY_FIELDS = {"ctx_size", "n_parallel", "n_seq_max"}


def rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(PACKAGE_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{rel(path)}:{lineno}: invalid JSONL: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"{rel(path)}:{lineno}: JSONL row must be an object")
        rows.append(row)
    return rows


def resolve_ref(ref: str, root_schema: dict[str, Any]) -> dict[str, Any]:
    if not ref.startswith("#/$defs/"):
        raise ValueError(f"unsupported schema ref: {ref}")
    name = ref.rsplit("/", 1)[-1]
    try:
        value = root_schema["$defs"][name]
    except KeyError as exc:
        raise ValueError(f"unknown schema ref: {ref}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"schema ref is not an object: {ref}")
    return value


def json_type_matches(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "string":
        return isinstance(value, str)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    if expected == "array":
        return isinstance(value, list)
    return False


def schema_errors(value: Any, schema: dict[str, Any], root_schema: dict[str, Any], path: str = "$") -> list[str]:
    errors: list[str] = []
    if "$ref" in schema:
        return schema_errors(value, resolve_ref(str(schema["$ref"]), root_schema), root_schema, path)

    if "oneOf" in schema:
        matches = []
        messages = []
        for option in schema["oneOf"]:
            option_errors = schema_errors(value, option, root_schema, path)
            if option_errors:
                messages.extend(option_errors[:1])
            else:
                matches.append(option)
        if len(matches) != 1:
            errors.append(f"{path}: expected exactly one matching schema, got {len(matches)}")
            if messages:
                errors.append(messages[0])
        return errors

    if "anyOf" in schema:
        branch_errors = [schema_errors(value, option, root_schema, path) for option in schema["anyOf"]]
        if not any(not option_errors for option_errors in branch_errors):
            errors.append(f"{path}: expected at least one matching schema")
            if branch_errors and branch_errors[0]:
                errors.append(branch_errors[0][0])
        return errors

    for option in schema.get("allOf", []):
        errors.extend(schema_errors(value, option, root_schema, path))

    if "if" in schema:
        if not schema_errors(value, schema["if"], root_schema, path):
            errors.extend(schema_errors(value, schema.get("then", {}), root_schema, path))

    if "not" in schema and not schema_errors(value, schema["not"], root_schema, path):
        errors.append(f"{path}: value matches prohibited schema")

    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected constant {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: expected one of {schema['enum']!r}")

    expected_type = schema.get("type")
    if isinstance(expected_type, str) and not json_type_matches(value, expected_type):
        errors.append(f"{path}: expected type {expected_type}")
        return errors

    if isinstance(value, dict):
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                errors.append(f"{path}: missing required key {key}")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extra = sorted(set(value) - set(properties))
            for key in extra:
                errors.append(f"{path}.{key}: unexpected key")
        for key, child_schema in properties.items():
            if key in value:
                errors.extend(schema_errors(value[key], child_schema, root_schema, f"{path}.{key}"))

    if isinstance(value, list):
        if "minItems" in schema and len(value) < int(schema["minItems"]):
            errors.append(f"{path}: fewer than minItems {schema['minItems']}")
        if "maxItems" in schema and len(value) > int(schema["maxItems"]):
            errors.append(f"{path}: more than maxItems {schema['maxItems']}")
        if schema.get("uniqueItems") is True:
            rendered = [json.dumps(item, sort_keys=True) for item in value]
            if len(rendered) != len(set(rendered)):
                errors.append(f"{path}: duplicate array items")
        if "items" in schema:
            for index, item in enumerate(value):
                errors.extend(schema_errors(item, schema["items"], root_schema, f"{path}[{index}]"))

    if isinstance(value, str):
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            errors.append(f"{path}: shorter than minLength {schema['minLength']}")
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            errors.append(f"{path}: longer than maxLength {schema['maxLength']}")
        if "pattern" in schema and not re.search(str(schema["pattern"]), value):
            errors.append(f"{path}: does not match pattern {schema['pattern']!r}")
        if schema.get("format") == "date-time":
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                errors.append(f"{path}: invalid date-time: {exc}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: less than minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: greater than maximum {schema['maximum']}")

    return errors


def walk(value: Any, path: str = "$"):
    yield path, value
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk(child, f"{path}[{index}]")


def privacy_errors(value: dict[str, Any], source: str) -> list[str]:
    errors: list[str] = []
    for path, item in walk(value):
        key = path.rsplit(".", 1)[-1].lower()
        if key == "path" and ".mutations[" in path:
            continue
        if key in FORBIDDEN_KEYS:
            errors.append(f"{source}:{path}: forbidden raw/sensitive key {key}")
        if key not in SAFE_PRIVACY_KEYS and any(part in key for part in RISKY_KEY_PARTS):
            errors.append(f"{source}:{path}: risky public fixture key {key}")
        if isinstance(item, str):
            lower = item.lower()
            for pattern in SECRET_VALUE_PATTERNS:
                if pattern.search(item):
                    errors.append(f"{source}:{path}: suspicious secret-like value")
                    break
            if ABSOLUTE_PATH_RE.search(item) or WINDOWS_PATH_RE.search(item) or item.startswith("../") or "/../" in item or item.startswith("~/") or lower.startswith("file://"):
                errors.append(f"{source}:{path}: raw or traversing filesystem path")
            if ".gguf" in lower or "/models" in lower:
                errors.append(f"{source}:{path}: raw model path or artifact name")
            if "/slots" in lower or "/metrics" in lower or "/v1" in lower or "/cache" in lower:
                errors.append(f"{source}:{path}: raw runtime endpoint text")
            if URL_RE.search(item) and (not lower.startswith("https://") or not lower.endswith(".invalid")):
                errors.append(f"{source}:{path}: fixture URL must use a reserved .invalid endpoint")
            if HOST_PORT_RE.search(item):
                errors.append(f"{source}:{path}: raw host:port endpoint text")
            if any(f"/{part}/" in lower or lower.endswith(f"/{part}") for part in FORBIDDEN_PATH_PARTS):
                errors.append(f"{source}:{path}: raw cache/log path fragment")
    return errors


def require_sha(value: Any, source: str, field: str, *, nullable: bool = False) -> list[str]:
    if value is None and nullable:
        return []
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        return [f"{source}: {field} must be a lowercase SHA-256 hex digest"]
    return []


def require_non_unknown_string(value: Any, source: str, field: str) -> list[str]:
    if not isinstance(value, str) or not value or value in UNKNOWN_VALUES:
        return [f"{source}: {field} must be a non-empty captured string"]
    return []


def require_positive_int(value: Any, source: str, field: str) -> list[str]:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        return [f"{source}: {field} must be a positive integer"]
    return []


def require_bool(value: Any, source: str, field: str) -> list[str]:
    if not isinstance(value, bool):
        return [f"{source}: {field} must be a boolean"]
    return []


def require_keys(row: dict[str, Any], keys: list[str], source: str) -> list[str]:
    return [f"{source}: missing required key {key}" for key in keys if key not in row]


def validate_compatibility_block(row: Any, source: str, *, include_model_id: bool = False) -> list[str]:
    errors: list[str] = []
    if not isinstance(row, dict):
        return [f"{source}: compatibility must be an object"]
    required = list(ALL_STRICT_COMPATIBILITY_FIELDS)
    if include_model_id:
        required.insert(0, "model_id")
    errors.extend(require_keys(row, required, source))
    if include_model_id and "model_id" in row:
        errors.extend(require_non_unknown_string(row.get("model_id"), source, "model_id"))
    for field in ALL_STRICT_COMPATIBILITY_FIELDS:
        if field not in row:
            continue
        value = row.get(field)
        if field in SHA_COMPATIBILITY_FIELDS:
            if field == "spec_draft_model_hash" and value == "none":
                continue
            errors.extend(require_sha(value, source, field))
        elif field in BOOLEAN_COMPATIBILITY_FIELDS:
            errors.extend(require_bool(value, source, field))
        elif field in INTEGER_COMPATIBILITY_FIELDS:
            errors.extend(require_positive_int(value, source, field))
        else:
            errors.extend(require_non_unknown_string(value, source, field))
    if row.get("build_backend") not in {"vulkan_radv", "vulkan_amdvlk", "rocm_hip", "cpu", "other"}:
        errors.append(f"{source}: build_backend has unsupported value")
    if row.get("flash_attention_mode") not in {"on", "off", "auto"}:
        errors.append(f"{source}: flash_attention_mode has unsupported value")
    if row.get("mtp_enabled") is True:
        if row.get("spec_draft_model_hash") == "none":
            errors.append(f"{source}: mtp_enabled true requires spec_draft_model_hash")
        if row.get("spec_draft_config") == "none":
            errors.append(f"{source}: mtp_enabled true requires spec_draft_config")
    if row.get("mtp_enabled") is False:
        if row.get("spec_draft_model_hash") != "none":
            errors.append(f"{source}: mtp_enabled false requires spec_draft_model_hash none")
        if row.get("spec_draft_config") != "none":
            errors.append(f"{source}: mtp_enabled false requires spec_draft_config none")
    errors.extend(privacy_errors(row, source))
    return errors


def validate_decision_event(row: dict[str, Any], schema: dict[str, Any], source: str) -> list[str]:
    errors = schema_errors(row, schema, schema, source)
    errors.extend(privacy_errors(row, source))
    for field in ["request_hash", "tenant_hash", "cache_key_hash"]:
        errors.extend(require_sha(row.get(field), source, field, nullable=field == "cache_key_hash"))
    errors.extend(require_sha(row.get("conversation_hash"), source, "conversation_hash", nullable=True))
    policy = row.get("policy", {})
    if isinstance(policy, dict):
        errors.extend(require_sha(policy.get("policy_id_hash"), source, "policy.policy_id_hash", nullable=True))
        if policy.get("cross_tenant_reuse_allowed") is not False:
            errors.append(f"{source}: policy.cross_tenant_reuse_allowed must be false")
    privacy = row.get("privacy", {})
    if isinstance(privacy, dict):
        for key in [
            "raw_prompt_logged",
            "raw_tenant_id_logged",
            "raw_conversation_id_logged",
            "raw_cache_blob_path_logged",
            "raw_environment_logged",
            "contains_secret_material",
        ]:
            if privacy.get(key) is not False:
                errors.append(f"{source}: privacy.{key} must be false")

    decision = row.get("decision")
    phase = row.get("phase")
    compatibility = row.get("compatibility_result")
    fallback_required = row.get("fallback_required")
    fallback_reason = row.get("fallback_reason")
    cache_key_hash = row.get("cache_key_hash")
    manifest_id = row.get("manifest_id")
    validation_status = row.get("validation_status")
    cache_hit_level = row.get("cache_hit_level")

    if decision in CACHE_DEPENDENT_DECISIONS or phase in CACHE_DEPENDENT_PHASES:
        if not cache_key_hash:
            errors.append(f"{source}: cache-dependent decision requires cache_key_hash")
        if not manifest_id:
            errors.append(f"{source}: cache-dependent decision requires manifest_id")
    if decision in CACHE_POSITIVE_DECISIONS:
        if compatibility != "match":
            errors.append(f"{source}: cache-positive decision requires compatibility_result match")
        if fallback_required is not False:
            errors.append(f"{source}: cache-positive decision must not require fallback")
    if cache_hit_level in {"local_nvme", "durable_blob"} and decision != "fallback_after_restore_failure":
        if not cache_key_hash or not manifest_id:
            errors.append(f"{source}: cache hit level {cache_hit_level} requires cache identity")
        if compatibility != "match":
            errors.append(f"{source}: cache hit level {cache_hit_level} requires compatibility_result match")
    if decision == "restore_then_generate":
        if validation_status != "validated":
            errors.append(f"{source}: restore_then_generate requires validation_status validated")
        if fallback_reason not in {None, "none"}:
            errors.append(f"{source}: restore_then_generate must not carry a fallback reason")
    if decision == "fallback_after_restore_failure":
        if fallback_required is not True:
            errors.append(f"{source}: restore failure must require fallback")
        if fallback_reason in {None, "none"}:
            errors.append(f"{source}: restore failure must explain fallback_reason")
        if validation_status not in {"quarantined", "corrupt"}:
            errors.append(f"{source}: restore failure must mark validation_status quarantined or corrupt")
    if fallback_required is True and fallback_reason in {None, "none"}:
        errors.append(f"{source}: fallback_required true needs a concrete fallback_reason")
    if fallback_required is False and fallback_reason not in {None, "none"}:
        errors.append(f"{source}: fallback_required false should not carry fallback_reason {fallback_reason!r}")
    if validation_status in {"quarantined", "corrupt"} and fallback_required is not True:
        errors.append(f"{source}: quarantined/corrupt validation_status must require fallback")

    metrics = row.get("metrics", {})
    if isinstance(metrics, dict):
        for key in ["prompt_tokens", "cached_tokens", "processed_prompt_tokens"]:
            value = metrics.get(key)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
                errors.append(f"{source}: metrics.{key} must be a non-negative integer or null")
        reuse_ratio = metrics.get("reuse_ratio")
        if reuse_ratio is not None and not (isinstance(reuse_ratio, (int, float)) and not isinstance(reuse_ratio, bool) and 0 <= reuse_ratio <= 1):
            errors.append(f"{source}: metrics.reuse_ratio must be between 0 and 1 or null")
    return errors


def validate_validation_result(row: dict[str, Any], schema: dict[str, Any], source: str) -> list[str]:
    errors = schema_errors(row, schema, schema, source)
    errors.extend(privacy_errors(row, source))
    for field in ["cache_key_hash", "blob_hash"]:
        errors.extend(require_sha(row.get(field), source, field, nullable=field == "blob_hash"))
    metrics = row.get("metrics", {})
    if isinstance(metrics, dict):
        errors.extend(require_sha(metrics.get("deterministic_text_digest"), source, "metrics.deterministic_text_digest", nullable=True))

    status = row.get("status")
    failure_reason = row.get("failure_reason")
    fallback_required = row.get("fallback_required")
    quarantine_recommended = row.get("quarantine_recommended")
    security_signal = row.get("security_signal")
    checks = row.get("checks", {})

    if status == "pass":
        if row.get("compatibility_result") != "match":
            errors.append(f"{source}: passing validation requires compatibility_result match")
        if fallback_required is not False:
            errors.append(f"{source}: passing validation must not require fallback")
        if quarantine_recommended is not False:
            errors.append(f"{source}: passing validation must not recommend quarantine")
        if failure_reason is not None:
            errors.append(f"{source}: passing validation must not carry failure_reason")
        if security_signal not in {None, "none"}:
            errors.append(f"{source}: passing validation must not carry security_signal {security_signal!r}")
        if row.get("validation_type") in {"next_token_logits", "top_k_distribution", "deterministic_text", "mtp_restore"}:
            if not any(checks.get(key) == "pass" for key in CORRECTNESS_CHECKS):
                errors.append(f"{source}: passing correctness validation needs at least one correctness check")
    if status in {"fail", "error"}:
        if failure_reason in {None, "not_applicable"}:
            errors.append(f"{source}: failed validation needs a concrete failure_reason")
        if fallback_required is not True:
            errors.append(f"{source}: failed validation must require fallback")
        if failure_reason in QUARANTINE_FAILURE_REASONS and quarantine_recommended is not True:
            errors.append(f"{source}: corrupt/restore failure must recommend quarantine")
        if failure_reason in QUARANTINE_FAILURE_REASONS and security_signal in {None, "none"}:
            errors.append(f"{source}: corrupt/restore failure needs a security_signal")
        if failure_reason == "tenant_policy_mismatch" and security_signal not in {"tenant_mismatch", "policy_violation"}:
            errors.append(f"{source}: tenant policy mismatch needs tenant/policy security_signal")
    if isinstance(checks, dict):
        failed_blob_checks = {
            "checksum_match",
            "blob_size_match",
            "restore_api_success",
            "logits_match",
            "top_k_match",
            "deterministic_text_match",
        }
        if any(checks.get(key) == "fail" for key in failed_blob_checks):
            if fallback_required is not True:
                errors.append(f"{source}: failed restore/blob/correctness check must require fallback")
            if any(checks.get(key) == "fail" for key in failed_blob_checks - {"checksum_match", "blob_size_match"}):
                if quarantine_recommended is not True:
                    errors.append(f"{source}: failed restore/correctness check must recommend quarantine")
    return errors


def validate_replay_policy(row: dict[str, Any], schema: dict[str, Any], source: str) -> list[str]:
    errors = schema_errors(row, schema, schema, source)
    errors.extend(privacy_errors(row, source))
    errors.extend(require_sha(row.get("policy_id_hash"), source, "policy_id_hash"))
    if row.get("allow_cross_tenant_reuse") is not False:
        errors.append(f"{source}: allow_cross_tenant_reuse must be false")
    if row.get("audit_required") is not True:
        errors.append(f"{source}: audit_required must be true")
    if row.get("require_tenant_hash") is not True:
        errors.append(f"{source}: require_tenant_hash must be true")
    if row.get("user_content_global_cache_allowed") is not False:
        errors.append(f"{source}: user_content_global_cache_allowed must be false")
    if row.get("encryption_at_rest_required") is not True:
        errors.append(f"{source}: encryption_at_rest_required must be true")
    return errors


def validate_replay_request(row: dict[str, Any], source: str) -> list[str]:
    required = [
        "scenario_id",
        "trace_id",
        "request_id",
        "request_hash",
        "tenant_hash",
        "conversation_hash",
        "scope",
        "policy_id",
        "model_id",
        "cache_key_hash",
        "token_prefix_hash",
        "prefix_token_ids_hash",
        "n_tokens",
        "candidate_worker_ids",
        "compatibility",
    ]
    errors = require_keys(row, required, source)
    errors.extend(privacy_errors(row, source))
    for field in ["request_hash", "tenant_hash", "conversation_hash", "cache_key_hash", "token_prefix_hash", "prefix_token_ids_hash"]:
        errors.extend(require_sha(row.get(field), source, field))
    if row.get("scope") not in APPROVED_SCOPES:
        errors.append(f"{source}: scope must be an approved cache scope")
    errors.extend(require_non_unknown_string(row.get("model_id"), source, "model_id"))
    errors.extend(require_positive_int(row.get("n_tokens"), source, "n_tokens"))
    candidate_worker_ids = row.get("candidate_worker_ids")
    if not isinstance(candidate_worker_ids, list) or not candidate_worker_ids or not all(isinstance(item, str) and item for item in candidate_worker_ids):
        errors.append(f"{source}: candidate_worker_ids must be a non-empty string array")
    errors.extend(validate_compatibility_block(row.get("compatibility"), f"{source}.compatibility"))
    return errors


def validate_replay_manifest(row: dict[str, Any], source: str) -> list[str]:
    required = [
        "manifest_id",
        "cache_key_hash",
        "scope",
        "tenant_hash",
        "conversation_hash",
        "model_id",
        "token_prefix_hash",
        "prefix_token_ids_hash",
        "n_tokens",
        "validation_status",
        "durable_available",
        "encrypted_at_rest",
        "artifact_sha256",
        "artifact_size_bytes",
        "residency",
        "compatibility",
    ]
    errors = require_keys(row, required, source)
    errors.extend(privacy_errors(row, source))
    for field in ["cache_key_hash", "tenant_hash", "conversation_hash", "token_prefix_hash", "prefix_token_ids_hash", "artifact_sha256"]:
        errors.extend(require_sha(row.get(field), source, field))
    if row.get("scope") not in APPROVED_SCOPES:
        errors.append(f"{source}: scope must be an approved cache scope")
    errors.extend(require_non_unknown_string(row.get("manifest_id"), source, "manifest_id"))
    errors.extend(require_non_unknown_string(row.get("model_id"), source, "model_id"))
    errors.extend(require_positive_int(row.get("n_tokens"), source, "n_tokens"))
    errors.extend(require_positive_int(row.get("artifact_size_bytes"), source, "artifact_size_bytes"))
    for field in ["durable_available", "encrypted_at_rest"]:
        errors.extend(require_bool(row.get(field), source, field))
    if row.get("validation_status") not in {"validated", "quarantined", "corrupt", "expired"}:
        errors.append(f"{source}: validation_status must be valid")
    if row.get("restore_validation_status") not in {None, "pass", "fail"}:
        errors.append(f"{source}: restore_validation_status must be pass, fail, or omitted")
    residency = row.get("residency")
    if not isinstance(residency, list):
        errors.append(f"{source}: residency must be an array")
    else:
        for index, item in enumerate(residency):
            item_source = f"{source}.residency[{index}]"
            if not isinstance(item, dict):
                errors.append(f"{item_source}: residency entry must be an object")
                continue
            errors.extend(require_non_unknown_string(item.get("worker_id"), item_source, "worker_id"))
            if item.get("level") != "local_nvme":
                errors.append(f"{item_source}: level must be local_nvme")
            if item.get("status") not in {"available", "stale", "missing", "corrupt"}:
                errors.append(f"{item_source}: status must be available, stale, missing, or corrupt")
    errors.extend(validate_compatibility_block(row.get("compatibility"), f"{source}.compatibility"))
    return errors


def validate_replay_worker(row: dict[str, Any], source: str) -> list[str]:
    required = [
        "worker_id",
        "health_status",
        "capacity_available",
        "loaded_model_ids",
        "supports_hydration",
        "supports_restore",
        "supports_commit",
        "max_ctx_size",
        "compatibility",
    ]
    errors = require_keys(row, required, source)
    errors.extend(privacy_errors(row, source))
    errors.extend(require_non_unknown_string(row.get("worker_id"), source, "worker_id"))
    if row.get("health_status") not in {"healthy", "degraded", "draining", "offline"}:
        errors.append(f"{source}: health_status must be valid")
    for field in ["capacity_available", "supports_hydration", "supports_restore", "supports_commit"]:
        errors.extend(require_bool(row.get(field), source, field))
    errors.extend(require_positive_int(row.get("max_ctx_size"), source, "max_ctx_size"))
    loaded_model_ids = row.get("loaded_model_ids")
    if not isinstance(loaded_model_ids, list) or not loaded_model_ids or not all(isinstance(item, str) and item for item in loaded_model_ids):
        errors.append(f"{source}: loaded_model_ids must be a non-empty string array")
    errors.extend(validate_compatibility_block(row.get("compatibility"), f"{source}.compatibility", include_model_id=True))
    return errors


def validate_cross_records(
    decisions: list[dict[str, Any]],
    validations: list[dict[str, Any]],
    *,
    decision_source: Path = DECISION_TRACE_PATH,
) -> list[str]:
    errors: list[str] = []
    by_key: dict[tuple[Any, Any, Any, Any], list[dict[str, Any]]] = {}
    for row in validations:
        key = (row.get("trace_id"), row.get("manifest_id"), row.get("cache_key_hash"), row.get("worker_id"))
        by_key.setdefault(key, []).append(row)
    for index, row in enumerate(decisions, start=1):
        key = (row.get("trace_id"), row.get("manifest_id"), row.get("cache_key_hash"), row.get("worker_id"))
        if row.get("phase") == "restore_validated" or row.get("decision") in {"restore_then_generate", "fallback_after_restore_failure"}:
            if row.get("manifest_id") is None or row.get("cache_key_hash") is None or row.get("worker_id") is None:
                continue
            matches = by_key.get(key, [])
            source = f"{rel(decision_source)}:{index}"
            if row.get("validation_status") == "validated" and not any(match.get("status") == "pass" for match in matches):
                errors.append(f"{source}: validated decision lacks matching passing validation result")
            if row.get("validation_status") in {"quarantined", "corrupt"} and not any(match.get("status") in {"fail", "error"} and match.get("fallback_required") is True for match in matches):
                errors.append(f"{source}: quarantined/corrupt decision lacks matching failing validation result")
    return errors


def manifest_match_reason(manifest: dict[str, Any], request: dict[str, Any]) -> str:
    for field in ["cache_key_hash", "scope", "model_id", "token_prefix_hash", "prefix_token_ids_hash"]:
        if manifest.get(field) != request.get(field):
            return f"manifest_mismatch:{field}"
    if manifest.get("n_tokens") != request.get("n_tokens"):
        return "manifest_mismatch:n_tokens"
    if manifest.get("validation_status") != "validated":
        return "manifest_mismatch:validation_status"
    if manifest.get("encrypted_at_rest") is not True:
        return "manifest_mismatch:encrypted_at_rest"
    manifest_compatibility = manifest.get("compatibility", {})
    request_compatibility = request.get("compatibility", {})
    for field in ALL_STRICT_COMPATIBILITY_FIELDS:
        if manifest_compatibility.get(field) != request_compatibility.get(field):
            return f"strict_key_mismatch:{field}"
    return "match"


def policy_match_reason(policy: dict[str, Any], request: dict[str, Any], manifest: dict[str, Any]) -> str:
    if request.get("scope") not in policy.get("allowed_scopes", []):
        return "policy_denied:scope_not_allowed"
    if policy.get("allow_cross_tenant_reuse") is not False:
        return "policy_denied:cross_tenant_policy_unsafe"
    if request.get("tenant_hash") != manifest.get("tenant_hash"):
        return "policy_denied:tenant_scope_mismatch"
    if (
        request.get("scope") == "conversation"
        and policy.get("require_conversation_hash_for_conversation_scope") is True
        and request.get("conversation_hash") != manifest.get("conversation_hash")
    ):
        return "policy_denied:conversation_scope_mismatch"
    if request.get("scope") == "global_system":
        if not (
            policy.get("allow_global_system_cache") is True
            and request.get("token_prefix_hash") in policy.get("operator_global_allowlist_hashes", [])
        ):
            return "policy_denied:global_system_not_allowlisted"
    if request.get("scope") == "private_disabled":
        return "policy_denied:private_disabled"
    return "match"


def worker_match_reason(worker: dict[str, Any], request: dict[str, Any], *, operation: str) -> str:
    if worker.get("health_status") != "healthy":
        return "worker_unavailable:health_status"
    if worker.get("capacity_available") is not True:
        return "worker_unavailable:worker_capacity"
    if request.get("model_id") not in worker.get("loaded_model_ids", []):
        return "worker_unavailable:model_not_loaded"
    if worker.get("max_ctx_size", 0) < request.get("compatibility", {}).get("ctx_size", 0):
        return "worker_unavailable:ctx_size"
    worker_compatibility = worker.get("compatibility", {})
    request_compatibility = request.get("compatibility", {})
    for field in ALL_STRICT_COMPATIBILITY_FIELDS:
        if worker_compatibility.get(field) != request_compatibility.get(field):
            return f"worker_strict_key_mismatch:{field}"
    if operation in {"local_restore", "durable_hydrate", "restore_validation"} and worker.get("supports_restore") is not True:
        return "worker_unavailable:worker_lacks_restore"
    if operation == "durable_hydrate" and worker.get("supports_hydration") is not True:
        return "worker_unavailable:worker_lacks_hydration"
    return "match"


def evaluate_strict_key_fixture(
    fixture: dict[str, Any],
    *,
    requests: dict[str, dict[str, Any]],
    manifests: dict[str, dict[str, Any]],
    workers: dict[str, dict[str, Any]],
    policies: dict[str, dict[str, Any]],
) -> str:
    request = copy.deepcopy(requests[str(fixture["request_id"])])
    manifest = copy.deepcopy(manifests[str(fixture["manifest_id"])]) if fixture.get("manifest_id") else None
    worker = copy.deepcopy(workers[str(fixture["worker_id"])]) if fixture.get("worker_id") else None
    policy = copy.deepcopy(policies[request["policy_id"]])
    bundle: dict[str, Any] = {
        "request": request,
        "policy": policy,
    }
    if manifest is not None:
        bundle["manifest"] = manifest
    if worker is not None:
        bundle["worker"] = worker
    bundle = apply_mutations(bundle, fixture.get("mutations", []))
    request = bundle["request"]
    policy = bundle["policy"]
    manifest = bundle.get("manifest")
    worker = bundle.get("worker")
    operation = str(fixture.get("operation", "local_restore"))

    if manifest is not None:
        reason = manifest_match_reason(manifest, request)
        if reason != "match":
            return reason
        reason = policy_match_reason(policy, request, manifest)
        if reason != "match":
            return reason
    if worker is not None:
        reason = worker_match_reason(worker, request, operation=operation)
        if reason != "match":
            return reason
    return "restore_allowed"


def get_path(value: Any, dotted_path: str) -> tuple[Any, str]:
    parts = dotted_path.split(".")
    current = value
    for part in parts[:-1]:
        if not isinstance(current, dict):
            raise ValueError(f"cannot descend through non-object path {dotted_path}")
        current = current.setdefault(part, {})
    return current, parts[-1]


def apply_mutations(base: dict[str, Any], mutations: list[dict[str, Any]]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for mutation in mutations:
        parent, key = get_path(result, str(mutation["path"]))
        if mutation["op"] == "set":
            parent[key] = mutation.get("value")
        elif mutation["op"] == "delete":
            parent.pop(key, None)
        else:
            raise ValueError(f"unsupported mutation op: {mutation['op']}")
    return result


def source_row(contract_type: str, line: int) -> dict[str, Any]:
    path = DECISION_TRACE_PATH if contract_type == "decision_event" else VALIDATION_RESULTS_PATH
    rows = load_jsonl(path)
    if line < 1 or line > len(rows):
        raise ValueError(f"{rel(path)} has no line {line}")
    return rows[line - 1]


def validate_external_files(events_path: Path | None, validations_path: Path | None) -> dict[str, Any]:
    decision_schema = load_json(DECISION_SCHEMA_PATH)
    validation_schema = load_json(VALIDATION_SCHEMA_PATH)
    errors: list[str] = []
    event_count = 0
    validation_count = 0
    events: list[dict[str, Any]] = []
    validations: list[dict[str, Any]] = []

    if events_path is not None:
        events = load_jsonl(events_path)
        event_count = len(events)
        for index, row in enumerate(events, start=1):
            errors.extend(validate_decision_event(row, decision_schema, f"{events_path}:{index}"))
    if validations_path is not None:
        validations = load_jsonl(validations_path)
        validation_count = len(validations)
        for index, row in enumerate(validations, start=1):
            errors.extend(validate_validation_result(row, validation_schema, f"{validations_path}:{index}"))
    if events and validations:
        errors.extend(validate_cross_records(events, validations, decision_source=events_path or DECISION_TRACE_PATH))

    return {
        "ok": not errors,
        "errors": errors,
        "external_event_rows": event_count,
        "external_validation_rows": validation_count,
        "uses_jsonschema": False,
    }


def validate_replay_fixture_dir(
    replay_root: Path = REPLAY_ROOT,
    *,
    strict_key_negatives_path: Path = STRICT_KEY_NEGATIVE_FIXTURES_PATH,
) -> dict[str, Any]:
    manifest_schema = load_json(MANIFEST_SCHEMA_PATH)
    worker_schema = load_json(WORKER_SCHEMA_PATH)
    policy_schema = load_json(POLICY_SCHEMA_PATH)
    decision_schema = load_json(DECISION_SCHEMA_PATH)

    requests_path = replay_root / "requests.jsonl"
    workers_path = replay_root / "workers.jsonl"
    registry_path = replay_root / "registry.jsonl"
    policies_path = replay_root / "policies.jsonl"
    expected_path = replay_root / "expected-decisions.jsonl"
    manifests_positive_path = replay_root / "manifests-positive.jsonl"
    workers_positive_path = replay_root / "workers-positive.jsonl"
    mock_output_path = replay_root / "mock-router-output.jsonl"

    errors: list[str] = []
    requests = load_jsonl(requests_path)
    workers = load_jsonl(workers_path)
    registry = load_jsonl(registry_path)
    policies = load_jsonl(policies_path)
    expected = load_jsonl(expected_path)
    strict_rows = load_jsonl(strict_key_negatives_path) if strict_key_negatives_path.exists() else []

    for index, row in enumerate(requests, start=1):
        errors.extend(validate_replay_request(row, f"{rel(requests_path)}:{index}"))
    for index, row in enumerate(workers, start=1):
        errors.extend(validate_replay_worker(row, f"{rel(workers_path)}:{index}"))
    for index, row in enumerate(registry, start=1):
        errors.extend(validate_replay_manifest(row, f"{rel(registry_path)}:{index}"))
    for index, row in enumerate(policies, start=1):
        errors.extend(validate_replay_policy(row, policy_schema, f"{rel(policies_path)}:{index}"))
    for index, row in enumerate(expected, start=1):
        errors.extend(privacy_errors(row, f"{rel(expected_path)}:{index}"))

    if manifests_positive_path.exists():
        for index, row in enumerate(load_jsonl(manifests_positive_path), start=1):
            source = f"{rel(manifests_positive_path)}:{index}"
            errors.extend(schema_errors(row, manifest_schema, manifest_schema, source))
            errors.extend(privacy_errors(row, source))
    if workers_positive_path.exists():
        for index, row in enumerate(load_jsonl(workers_positive_path), start=1):
            source = f"{rel(workers_positive_path)}:{index}"
            errors.extend(schema_errors(row, worker_schema, worker_schema, source))
            errors.extend(privacy_errors(row, source))
    if mock_output_path.exists():
        for index, row in enumerate(load_jsonl(mock_output_path), start=1):
            errors.extend(validate_decision_event(row, decision_schema, f"{rel(mock_output_path)}:{index}"))

    request_by_id = {row["request_id"]: row for row in requests if "request_id" in row}
    manifest_by_id = {row["manifest_id"]: row for row in registry if "manifest_id" in row}
    worker_by_id = {row["worker_id"]: row for row in workers if "worker_id" in row}
    policy_by_id = {row["policy_id"]: row for row in policies if "policy_id" in row}
    rejected_strict_rows: list[str] = []
    for index, fixture in enumerate(strict_rows, start=1):
        source = f"{rel(strict_key_negatives_path)}:{index}:{fixture.get('fixture_id')}"
        errors.extend(privacy_errors(fixture, source))
        for key in ["fixture_id", "request_id", "expected_rejection_substring", "mismatch_field", "mutations"]:
            if key not in fixture:
                errors.append(f"{source}: missing required key {key}")
        if errors and any(error.startswith(source) for error in errors):
            continue
        try:
            reason = evaluate_strict_key_fixture(
                fixture,
                requests=request_by_id,
                manifests=manifest_by_id,
                workers=worker_by_id,
                policies=policy_by_id,
            )
        except KeyError as exc:
            errors.append(f"{source}: references unknown fixture object {exc}")
            continue
        expected = str(fixture.get("expected_rejection_substring", ""))
        if reason == "restore_allowed":
            errors.append(f"{source}: strict-key negative unexpectedly allowed restore")
            continue
        if expected and expected not in reason:
            errors.append(f"{source}: expected rejection containing {expected!r}, got {reason!r}")
            continue
        rejected_strict_rows.append(str(fixture.get("fixture_id")))

    return {
        "ok": not errors,
        "errors": errors,
        "replay_requests": len(requests),
        "replay_workers": len(workers),
        "replay_registry_manifests": len(registry),
        "replay_policies": len(policies),
        "strict_key_negative_fixtures": len(strict_rows),
        "strict_key_negative_fixtures_rejected": len(rejected_strict_rows),
        "uses_jsonschema": False,
    }


def validate_all() -> dict[str, Any]:
    decision_schema = load_json(DECISION_SCHEMA_PATH)
    validation_schema = load_json(VALIDATION_SCHEMA_PATH)
    decisions = load_jsonl(DECISION_TRACE_PATH)
    validations = load_jsonl(VALIDATION_RESULTS_PATH)
    fixture_rows = load_jsonl(NEGATIVE_FIXTURES_PATH)
    replay_summary = validate_replay_fixture_dir(REPLAY_ROOT)
    errors: list[str] = []

    for index, row in enumerate(decisions, start=1):
        errors.extend(validate_decision_event(row, decision_schema, f"{rel(DECISION_TRACE_PATH)}:{index}"))
    for index, row in enumerate(validations, start=1):
        errors.extend(validate_validation_result(row, validation_schema, f"{rel(VALIDATION_RESULTS_PATH)}:{index}"))
    errors.extend(validate_cross_records(decisions, validations))

    rejected: list[str] = []
    for index, fixture in enumerate(fixture_rows, start=1):
        contract_type = fixture.get("contract_type")
        expected = str(fixture.get("expected_rejection_substring", ""))
        payload = apply_mutations(
            source_row(str(contract_type), int(fixture["source_line"])),
            fixture.get("mutations", []),
        )
        source = f"{rel(NEGATIVE_FIXTURES_PATH)}:{index}:{fixture.get('fixture_id')}"
        if contract_type == "decision_event":
            fixture_errors = validate_decision_event(payload, decision_schema, source)
        elif contract_type == "validation_result":
            fixture_errors = validate_validation_result(payload, validation_schema, source)
        else:
            fixture_errors = [f"{source}: unknown contract_type {contract_type!r}"]
        if not fixture_errors:
            errors.append(f"{source}: negative fixture unexpectedly passed")
            continue
        if expected and not any(expected in error for error in fixture_errors):
            errors.append(f"{source}: expected rejection containing {expected!r}, got {fixture_errors[:3]!r}")
            continue
        rejected.append(str(fixture.get("fixture_id")))
    errors.extend(replay_summary["errors"])

    return {
        "ok": not errors,
        "errors": errors,
        "positive_decision_events": len(decisions),
        "positive_validation_results": len(validations),
        "negative_fixtures": len(fixture_rows),
        "negative_fixtures_rejected": len(rejected),
        "replay_requests": replay_summary["replay_requests"],
        "replay_workers": replay_summary["replay_workers"],
        "replay_registry_manifests": replay_summary["replay_registry_manifests"],
        "replay_policies": replay_summary["replay_policies"],
        "strict_key_negative_fixtures": replay_summary["strict_key_negative_fixtures"],
        "strict_key_negative_fixtures_rejected": replay_summary["strict_key_negative_fixtures_rejected"],
        "rejected_fixture_ids": rejected,
        "uses_jsonschema": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit machine-readable summary")
    parser.add_argument("--events", type=Path, help="validate an external decision-event JSONL file")
    parser.add_argument("--validations", type=Path, help="validate an external validation-result JSONL file")
    parser.add_argument("--replay-fixtures", type=Path, help="validate replay input fixtures and strict-key negative fixtures")
    args = parser.parse_args()

    if args.replay_fixtures:
        summary = validate_replay_fixture_dir(args.replay_fixtures)
    elif args.events or args.validations:
        summary = validate_external_files(args.events, args.validations)
    else:
        summary = validate_all()
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        if summary["ok"]:
            if args.replay_fixtures:
                print(
                    "cache-router replay fixtures ok: "
                    f"{summary['replay_requests']} requests, "
                    f"{summary['replay_registry_manifests']} manifests, "
                    f"{summary['replay_workers']} workers, "
                    f"{summary['strict_key_negative_fixtures_rejected']}/{summary['strict_key_negative_fixtures']} strict-key negatives rejected"
                )
            elif args.events or args.validations:
                print(
                    "cache-router external contracts ok: "
                    f"{summary['external_event_rows']} decision events, "
                    f"{summary['external_validation_rows']} validation results"
                )
            else:
                print(
                    "cache-router contracts ok: "
                    f"{summary['positive_decision_events']} decision events, "
                    f"{summary['positive_validation_results']} validation results, "
                    f"{summary['negative_fixtures_rejected']}/{summary['negative_fixtures']} negative fixtures rejected, "
                    f"{summary['strict_key_negative_fixtures_rejected']}/{summary['strict_key_negative_fixtures']} strict-key negatives rejected"
                )
        else:
            print("cache-router contract validation failed:", file=sys.stderr)
            for error in summary["errors"]:
                print(f"- {error}", file=sys.stderr)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
