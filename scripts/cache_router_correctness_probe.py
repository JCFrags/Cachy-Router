#!/usr/bin/env python3
"""Operator-run restore correctness probe for Cachy Router.

This script is a live gate runner. It contacts only the router URL supplied by
the operator and writes no raw prompts or raw responses to repo files. The
checks compare deterministic cold completions against cached restore/use
completions and inspect router decision events to distinguish successful
restore from fail-closed cold recompute.

It does not prove logits/top-k equivalence; those require worker/runtime support
outside the current router-only API surface.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

import validate_cache_router_contracts as contracts  # noqa: E402

PASS = "pass"
FAIL = "fail"
SKIP = "skip"
SCHEMA_PASS = "pass"
SCHEMA_FAIL = "fail"
SCHEMA_ERROR = "error"
SCHEMA_SKIPPED = "skipped"
RESTORE_HIT_LEVELS = {"local_nvme", "durable_blob"}
RESTORE_DECISIONS = {"restore_then_generate", "hot_local_hit", "durable_hit_hydrate"}
COLD_RECOMPUTE_DECISIONS = {"cold_prefill", "fallback_after_restore_failure"}
COLD_RECOMPUTE_REASONS = {
    "restore_validation_failed",
    "restore_failed",
    "no_compatible_manifest",
    "cache_key_mismatch",
    "worker_unavailable",
    "worker_capacity",
}


@dataclass(frozen=True)
class CorrectnessCase:
    name: str
    description: str
    prefix_text: str
    suffix_text: str
    requires_mtp_enabled: bool = False


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def safe_id_component(text: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in ".:-" else "-" for char in text)
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] or "id"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def redacted_text_fingerprint(text: str) -> dict[str, Any]:
    return {
        "sha256": sha256_hex(text),
        "bytes": len(text.encode("utf-8")),
        "chars": len(text),
    }


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    data = None
    req_headers = dict(headers)
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = int(response.status)
            response_headers = {key.lower(): value for key, value in response.headers.items()}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = int(exc.code)
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
    except urllib.error.URLError as exc:
        return 0, {}, {"error": {"type": "transport_error", "message": str(exc)}}

    try:
        decoded = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception as exc:  # noqa: BLE001
        decoded = {"error": {"type": "invalid_json", "message": f"{type(exc).__name__}: {exc}"}}
    return status, response_headers, decoded if isinstance(decoded, dict) else {"value": decoded}


def extract_completion_text(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    choice = choices[0]
    if not isinstance(choice, dict):
        return ""
    if isinstance(choice.get("text"), str):
        return choice["text"]
    message = choice.get("message")
    if isinstance(message, dict) and isinstance(message.get("content"), str):
        return message["content"]
    return ""


def extract_native_text(body: dict[str, Any]) -> str:
    value = body.get("content")
    return value if isinstance(value, str) else ""


def extract_tokens(body: dict[str, Any]) -> list[Any] | int | None:
    tokens = body.get("tokens")
    if isinstance(tokens, list):
        return tokens
    if isinstance(tokens, int) and not isinstance(tokens, bool):
        return tokens
    if isinstance(body, list):
        return body
    return None


def tokenize_text(router_url: str, headers: dict[str, str], model: str, text: str, timeout: float) -> dict[str, Any]:
    payloads = [
        {"model": model, "content": text, "add_special": False},
        {"content": text, "add_special": False},
        {"prompt": text},
    ]
    for payload in payloads:
        status, _, body = request_json("POST", f"{router_url}/tokenize", headers=headers, body=payload, timeout=timeout)
        if status >= 400:
            continue
        tokens = extract_tokens(body)
        if isinstance(tokens, list):
            return {"available": True, "count": len(tokens), "hash": sha256_json(tokens), "tokens": tokens}
        if isinstance(tokens, int):
            return {"available": True, "count": tokens, "hash": None, "tokens": None}
    return {"available": False, "count": None, "hash": None, "tokens": None}


def events_for_request(router_url: str, headers: dict[str, str], request_id: str, timeout: float) -> list[dict[str, Any]]:
    if not request_id:
        return []
    query = urllib.parse.urlencode({"request_id": request_id})
    status, _, body = request_json("GET", f"{router_url}/router/decisions?{query}", headers=headers, timeout=timeout)
    if status != 200:
        return []
    events = body.get("events")
    return [event for event in events if isinstance(event, dict)] if isinstance(events, list) else []


def worker_rows(router_url: str, headers: dict[str, str], timeout: float) -> list[dict[str, Any]]:
    status, _, body = request_json("GET", f"{router_url}/router/workers", headers=headers, timeout=timeout)
    if status != 200:
        return []
    rows = body.get("workers")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def rows_have_mtp_enabled(rows: list[dict[str, Any]], worker_id: str | None = None) -> bool:
    if worker_id:
        return any(str(row.get("worker_id") or "") == worker_id and row.get("mtp_enabled") is True for row in rows)
    return any(row.get("mtp_enabled") is True for row in rows)


def deployment_has_mtp_enabled(router_url: str, headers: dict[str, str], timeout: float, worker_id: str | None = None) -> bool:
    return rows_have_mtp_enabled(worker_rows(router_url, headers, timeout), worker_id=worker_id)


def classify_trial(cold_text: str, restored_text: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = {str(event.get("decision") or "") for event in events}
    hit_levels = {str(event.get("cache_hit_level") or "") for event in events}
    fallback_reasons = {str(event.get("fallback_reason") or "") for event in events if event.get("fallback_reason")}
    validation_statuses = {str(event.get("validation_status") or "") for event in events if event.get("validation_status")}
    used_restore = bool(decisions & RESTORE_DECISIONS or hit_levels & RESTORE_HIT_LEVELS)
    recomputed_cold = bool(decisions & COLD_RECOMPUTE_DECISIONS or fallback_reasons & COLD_RECOMPUTE_REASONS)
    output_matches = cold_text == restored_text

    if not events:
        return {
            "status": FAIL,
            "reason": "missing_decision_events",
            "used_restore": False,
            "recomputed_cold": False,
            "output_matches": output_matches,
        }
    if output_matches and recomputed_cold:
        return {
            "status": PASS,
            "reason": "fail_closed_recomputed_cold_output_matches",
            "used_restore": False,
            "recomputed_cold": True,
            "output_matches": True,
            "decisions": sorted(decision for decision in decisions if decision),
            "fallback_reasons": sorted(reason for reason in fallback_reasons if reason),
            "validation_statuses": sorted(status for status in validation_statuses if status),
        }
    if output_matches and used_restore:
        return {
            "status": PASS,
            "reason": "restored_output_matches_cold",
            "used_restore": True,
            "recomputed_cold": False,
            "output_matches": True,
            "decisions": sorted(decision for decision in decisions if decision),
            "fallback_reasons": sorted(reason for reason in fallback_reasons if reason),
            "validation_statuses": sorted(status for status in validation_statuses if status),
        }
    return {
        "status": FAIL,
        "reason": "restored_output_mismatch" if used_restore else "cold_recompute_output_mismatch",
        "used_restore": used_restore,
        "recomputed_cold": recomputed_cold,
        "output_matches": output_matches,
        "decisions": sorted(decision for decision in decisions if decision),
        "fallback_reasons": sorted(reason for reason in fallback_reasons if reason),
        "validation_statuses": sorted(status for status in validation_statuses if status),
    }


def representative_restore_event(events: list[dict[str, Any]]) -> dict[str, Any]:
    for event in reversed(events):
        if event.get("decision") in RESTORE_DECISIONS or event.get("phase") in {"restore_requested", "restore_validated"}:
            return event
    return events[-1] if events else {}


def decision_metric(event: dict[str, Any], name: str) -> Any:
    if name in event:
        return event.get(name)
    metrics = event.get("metrics")
    if isinstance(metrics, dict):
        return metrics.get(name)
    return None


def token_compare(cold_tokens: dict[str, Any], restored_tokens: dict[str, Any], max_generated_token_ids: int) -> dict[str, Any]:
    if not cold_tokens.get("available") or not restored_tokens.get("available"):
        return {"available": False, "match": None, "checked_token_count": None, "cold_token_count": None, "restored_token_count": None}
    cold_list = cold_tokens.get("tokens")
    restored_list = restored_tokens.get("tokens")
    if isinstance(cold_list, list) and isinstance(restored_list, list):
        checked = min(max_generated_token_ids, len(cold_list), len(restored_list))
        return {
            "available": True,
            "match": cold_list[:checked] == restored_list[:checked],
            "checked_token_count": checked,
            "cold_token_count": len(cold_list),
            "restored_token_count": len(restored_list),
            "cold_token_hash": cold_tokens.get("hash"),
            "restored_token_hash": restored_tokens.get("hash"),
        }
    return {
        "available": True,
        "match": cold_tokens.get("count") == restored_tokens.get("count"),
        "checked_token_count": None,
        "cold_token_count": cold_tokens.get("count"),
        "restored_token_count": restored_tokens.get("count"),
        "cold_token_hash": cold_tokens.get("hash"),
        "restored_token_hash": restored_tokens.get("hash"),
    }


def non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def extract_restored_draft_activity(restored_body: dict[str, Any]) -> dict[str, Any]:
    cache_router = restored_body.get("cache_router") if isinstance(restored_body.get("cache_router"), dict) else {}
    use_meta = cache_router.get("use") if isinstance(cache_router.get("use"), dict) else {}
    completion_meta = use_meta.get("completion") if isinstance(use_meta.get("completion"), dict) else {}
    timings = completion_meta.get("timings") if isinstance(completion_meta.get("timings"), dict) else {}
    draft_n = non_negative_int(timings.get("draft_n"))
    draft_n_accepted = non_negative_int(timings.get("draft_n_accepted"))
    return {
        "available": bool(timings),
        "draft_n": draft_n,
        "draft_n_accepted": draft_n_accepted,
        "observed": any(value is not None and value > 0 for value in (draft_n, draft_n_accepted)),
    }


def extract_router_restore_validation(restored_body: dict[str, Any]) -> dict[str, Any]:
    cache_router = restored_body.get("cache_router") if isinstance(restored_body.get("cache_router"), dict) else {}
    use_meta = cache_router.get("use") if isinstance(cache_router.get("use"), dict) else {}
    validation = use_meta.get("restore_validation") if isinstance(use_meta.get("restore_validation"), dict) else {}
    return dict(validation)


def validation_status_set(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value if item}


def internal_restore_validation_passed(
    *,
    cold_baseline_nondeterministic: bool,
    router_restore_validation: dict[str, Any],
    validation_statuses: list[Any],
) -> bool:
    return bool(
        cold_baseline_nondeterministic
        and router_restore_validation.get("status") == "pass"
        and router_restore_validation.get("text_match") is not False
        and "validated" in validation_status_set(validation_statuses)
    )


def trial_internal_restore_validation_passed(trial: dict[str, Any]) -> bool:
    router_restore_validation = trial.get("router_restore_validation") if isinstance(trial.get("router_restore_validation"), dict) else {}
    validation_statuses = trial.get("validation_statuses") if isinstance(trial.get("validation_statuses"), list) else []
    return internal_restore_validation_passed(
        cold_baseline_nondeterministic=trial.get("cold_baseline_nondeterministic") is True,
        router_restore_validation=router_restore_validation,
        validation_statuses=validation_statuses,
    )


def default_checks() -> dict[str, str]:
    return {
        "manifest_schema_valid": "not_checked",
        "cache_key_match": "not_checked",
        "checksum_match": "not_checked",
        "blob_size_match": "not_checked",
        "restore_api_success": "not_checked",
        "logits_match": "not_checked",
        "top_k_match": "not_checked",
        "deterministic_text_match": "not_checked",
        "tenant_policy_match": "not_checked",
        "poisoning_negative_test_passed": "not_checked",
    }


def validation_result_from_trial(trial: dict[str, Any], index: int) -> dict[str, Any]:
    case_id = safe_id_component(str(trial.get("case") or "case"))
    run_id = int(trial.get("run") or 0)
    status = str(trial.get("status") or FAIL)
    missing_identity = [
        name
        for name in ["trace_id", "manifest_id", "worker_id", "cache_key_hash"]
        if not trial.get(name)
    ]
    validation_statuses = validation_status_set(trial.get("validation_statuses"))
    fallback_reasons = set(trial.get("fallback_reasons") if isinstance(trial.get("fallback_reasons"), list) else [])
    fail_closed_restore = bool(
        trial.get("recomputed_cold") is True
        and ("quarantined" in validation_statuses or "restore_validation_failed" in fallback_reasons)
    )
    token_match = trial.get("token_match")
    draft_disable_runtime_block = draft_disable_contradicted_by_timings(trial)
    runtime_blocked_valid_cache = bool(
        draft_disable_runtime_block
        and not fail_closed_restore
        and trial.get("output_matches") is True
        and token_match is not False
    )
    internal_restore_validated = trial_internal_restore_validation_passed(trial)
    schema_status = SCHEMA_PASS if ((status == PASS and not fail_closed_restore) or internal_restore_validated or runtime_blocked_valid_cache) and not missing_identity else SCHEMA_FAIL
    if status == SKIP:
        schema_status = SCHEMA_SKIPPED
    elif trial.get("phase") in {"build", "cold", "restore"}:
        schema_status = SCHEMA_ERROR
    elif missing_identity:
        schema_status = SCHEMA_ERROR

    failure_reason: str | None = None
    if schema_status in {SCHEMA_FAIL, SCHEMA_ERROR}:
        if trial.get("phase") in {"build", "cold", "restore"} or missing_identity:
            failure_reason = "restore_api_failed"
        elif trial.get("case") == "mtp_enabled":
            failure_reason = "mtp_restore_mismatch"
        else:
            failure_reason = "deterministic_text_mismatch"

    checks = default_checks()
    checks["cache_key_match"] = "pass" if trial.get("cache_key_hash") and not missing_identity else "fail"
    checks["restore_api_success"] = "pass" if trial.get("used_restore") else "skipped" if trial.get("recomputed_cold") else "fail"
    checks["deterministic_text_match"] = "pass" if trial.get("output_matches") else "fail"
    if fail_closed_restore:
        checks["restore_api_success"] = "fail"
        checks["deterministic_text_match"] = "fail"
    if internal_restore_validated:
        checks["restore_api_success"] = "pass"
        checks["deterministic_text_match"] = "pass"
    if schema_status == SCHEMA_SKIPPED:
        checks = {key: "skipped" for key in checks}

    if token_match is False and not internal_restore_validated and not runtime_blocked_valid_cache:
        checks["deterministic_text_match"] = "fail"
        schema_status = SCHEMA_FAIL
        failure_reason = "deterministic_text_mismatch"

    fallback_required = schema_status in {SCHEMA_FAIL, SCHEMA_ERROR}
    validation_type = "mtp_restore" if trial.get("case") == "mtp_enabled" else "deterministic_text"
    digest = trial.get("restored_output", {}).get("sha256") if isinstance(trial.get("restored_output"), dict) else None
    n_tokens = trial.get("restored_token_count") or trial.get("cold_token_count")
    cache_key_hash = str(trial.get("cache_key_hash") or sha256_hex(f"missing-cache-key:{case_id}:{run_id}"))
    worker_id = safe_id_component(str(trial.get("worker_id") or trial.get("selected_worker") or "worker-none"))
    trace_id = safe_id_component(str(trial.get("trace_id") or trial.get("request_id") or f"trace-{index}"))
    manifest_id = safe_id_component(str(trial.get("manifest_id") or f"manifest-{cache_key_hash[:16]}"))

    return {
        "schema_version": "2026-06-30.1",
        "validation_id": f"val-{case_id}-{run_id:03d}-{index:03d}",
        "trace_id": trace_id,
        "timestamp": now_iso(),
        "manifest_id": manifest_id,
        "worker_id": worker_id,
        "validation_type": validation_type,
        "status": schema_status,
        "cache_key_hash": cache_key_hash,
        "compatibility_result": "match" if schema_status == SCHEMA_PASS else "not_checked" if schema_status == SCHEMA_SKIPPED else "mismatch",
        "blob_hash": None,
        "n_tokens": n_tokens if isinstance(n_tokens, int) and n_tokens > 0 else None,
        "checks": checks,
        "tolerance": {
            "logit_max_abs_delta": None,
            "top_k_jaccard_min": None,
            "deterministic_text_digest_match_required": True,
        },
        "metrics": {
            "validation_latency_ms": trial.get("validation_latency_ms"),
            "restore_latency_ms": trial.get("restore_latency_ms"),
            "blob_size_bytes": None,
            "top_k_jaccard": None,
            "logit_max_abs_delta": None,
            "deterministic_text_digest": digest,
            "prompt_tokens": trial.get("cold_token_count"),
            "cached_tokens": trial.get("cached_tokens"),
            "processed_prompt_tokens": trial.get("processed_prompt_tokens"),
        },
        "failure_reason": failure_reason,
        "fallback_not_required_reason": "case skipped by deployment capability" if schema_status == SCHEMA_SKIPPED else None,
        "quarantine_recommended": schema_status in {SCHEMA_FAIL, SCHEMA_ERROR},
        "fallback_required": fallback_required,
        "security_signal": "stale_residency" if schema_status in {SCHEMA_FAIL, SCHEMA_ERROR} else "none",
        "notes": str(trial.get("reason") or "deterministic text comparison")[:300],
    }


def validate_validation_rows(rows: list[dict[str, Any]]) -> list[str]:
    schema = contracts.load_json(contracts.VALIDATION_SCHEMA_PATH)
    errors: list[str] = []
    for index, row in enumerate(rows, start=1):
        errors.extend(contracts.validate_validation_result(row, schema, f"validation-results.jsonl:{index}"))
    return errors


def validate_decision_rows(rows: list[dict[str, Any]]) -> list[str]:
    schema = contracts.load_json(contracts.DECISION_SCHEMA_PATH)
    errors: list[str] = []
    for index, row in enumerate(rows, start=1):
        errors.extend(contracts.validate_decision_event(row, schema, f"decision-events.jsonl:{index}"))
    return errors


def draft_disable_contradicted_by_timings(row: dict[str, Any]) -> bool:
    return bool(
        row.get("draft_disable_requested_for_correctness") is True
        and row.get("draft_activity_observed_when_disabled") is True
    )


def runtime_blocked_by_upstream(row: dict[str, Any]) -> bool:
    draft_disable_contradicted = draft_disable_contradicted_by_timings(row)
    return bool(
        row.get("status") == FAIL
        and draft_disable_contradicted
        and (
            row.get("cold_baseline_nondeterministic") is True
            or row.get("output_matches") is True
        )
    )


def case_status_counts(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for row in rows:
        case = str(row.get("case") or "unknown")
        status = str(row.get("status") or "unknown")
        counts.setdefault(case, {})
        counts[case][status] = counts[case].get(status, 0) + 1
    return counts


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def default_output_dir(cache_id_prefix: str) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return Path("runtime") / "cache-router-correctness" / f"{safe_id_component(cache_id_prefix)}-{stamp}"


def redacted_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": result["ok"],
        "status": result["status"],
        "scope": result["scope"],
        "model_hash": sha256_hex(str(result.get("model") or "")) if result.get("model") else None,
        "runs_per_case": result["runs_per_case"],
        "comparison_basis": result["comparison_basis"],
        "cold_baseline": result["cold_baseline"],
        "elapsed_seconds": result["elapsed_seconds"],
        "cases_requested": result["cases_requested"],
        "case_count": result["case_count"],
        "case_status_counts": result["case_status_counts"],
        "passed": result["passed"],
        "failed": result["failed"],
        "skipped": result["skipped"],
        "runtime_blocked_failures": result["runtime_blocked_failures"],
        "cache_correctness_failures": result["cache_correctness_failures"],
        "allow_skips": result["allow_skips"],
        "draft_disable_requested_for_correctness": result["draft_disable_requested_for_correctness"],
        "draft_disable_contradicted_by_timings": result["draft_disable_contradicted_by_timings"],
        "cold_baseline_nondeterministic": result["cold_baseline_nondeterministic"],
        "router_restore_validation": result["router_restore_validation"],
        "validation_rows": result["validation_rows"],
        "decision_event_rows": result["decision_event_rows"],
        "artifact_validation_errors": result["artifact_validation_errors"],
        "notes": result["notes"],
    }


def correctness_cases(near_context_repeat: int) -> list[CorrectnessCase]:
    near_context_prefix = (
        "The following reusable context line is intentionally repeated for a "
        "near-context restore probe.\n"
        * max(1, near_context_repeat)
    )
    return [
        CorrectnessCase(
            name="deterministic_generation",
            description="fixed-seed deterministic output after cache restore",
            prefix_text="System: answer with concise deterministic text.\nUser facts: alpha=1, beta=2.\n",
            suffix_text="Return the sum of alpha and beta in one sentence.",
        ),
        CorrectnessCase(
            name="system_prompt_boundary",
            description="restore at the system-prompt boundary",
            prefix_text="System: You are a precise router correctness probe. Use only the provided facts.\n",
            suffix_text="User: Say exactly one short sentence about cache restore.",
        ),
        CorrectnessCase(
            name="mid_conversation",
            description="restore in the middle of a multi-turn conversation",
            prefix_text=(
                "System: Stay deterministic.\n"
                "User: Define Cachy Router.\n"
                "Assistant: Cachy Router is a trusted-LAN OpenAI-compatible router.\n"
            ),
            suffix_text="User: Summarize that definition in five words.",
        ),
        CorrectnessCase(
            name="after_tool_output",
            description="restore after a tool-output shaped block",
            prefix_text=(
                "System: Treat tool output as authoritative.\n"
                "Tool result JSON: {\"worker\":\"worker-a\",\"ready\":true,\"cache\":\"hot\"}\n"
            ),
            suffix_text="User: Which worker was ready?",
        ),
        CorrectnessCase(
            name="near_context_limit",
            description="restore with a long reusable prefix",
            prefix_text=near_context_prefix,
            suffix_text="Return exactly: COMPLETED",
        ),
        CorrectnessCase(
            name="mtp_enabled",
            description="restore on a deployment whose selected workers report mtp_enabled=true",
            prefix_text="System: MTP-enabled correctness probe prefix.\n",
            suffix_text="User: Produce a deterministic short answer for the MTP case.",
            requires_mtp_enabled=True,
        ),
        CorrectnessCase(
            name="branch_retry",
            description="branch/retry cache reuse with one shared prefix and repeated suffix use",
            prefix_text="System: Branch and retry probe. Shared reusable prefix.\nFacts: branch=A, retry=true.\n",
            suffix_text="User: Report the branch letter.",
        ),
    ]


def selected_cases(all_cases: list[CorrectnessCase], requested: list[str]) -> list[CorrectnessCase]:
    if not requested or requested == ["all"]:
        return all_cases
    by_name = {case.name: case for case in all_cases}
    missing = [name for name in requested if name not in by_name]
    if missing:
        raise SystemExit(f"unknown case(s): {', '.join(missing)}")
    return [by_name[name] for name in requested]


def draft_disable_options(disable_draft: bool) -> dict[str, Any]:
    if not disable_draft:
        return {}
    return {
        "spec_draft_n_max": 0,
        "spec_draft_n_min": 0,
        "n_draft": 0,
        "draft_n": 0,
        "draft_n_min": 0,
        "speculative_decoding": False,
        "spec_draft": False,
        "speculative.n_max": 0,
        "speculative.n_min": 0,
        "speculative.p_min": 1.0,
        "speculative.type": "none",
    }


def completion_body(
    *,
    model: str,
    prompt: str,
    max_tokens: int,
    temperature: float,
    seed: int,
    top_k: int,
    top_p: float,
    min_p: float,
    disable_draft: bool,
    cache_router: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_k": top_k,
        "top_p": top_p,
        "min_p": min_p,
        "seed": seed,
        "stream": False,
    }
    body.update(draft_disable_options(disable_draft))
    if cache_router is not None:
        body["cache_router"] = cache_router
    return body


def native_completion_body(
    *,
    prompt: str,
    max_tokens: int,
    temperature: float,
    seed: int,
    top_k: int,
    top_p: float,
    min_p: float,
    disable_draft: bool,
    slot_id: int,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "prompt": prompt,
        "n_predict": max_tokens,
        "temperature": temperature,
        "top_k": top_k,
        "top_p": top_p,
        "min_p": min_p,
        "seed": seed,
        "cache_prompt": False,
        "id_slot": slot_id,
        "stream": False,
    }
    body.update(draft_disable_options(disable_draft))
    return body


def native_cold_completion(
    *,
    worker_url: str,
    prompt: str,
    max_tokens: int,
    temperature: float,
    seed: int,
    top_k: int,
    top_p: float,
    min_p: float,
    disable_draft: bool,
    slot_id: int,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    erase_status, erase_headers, erase_body = request_json(
        "POST",
        f"{worker_url.rstrip('/')}/slots/{slot_id}?action=erase",
        headers={},
        timeout=timeout,
        body={},
    )
    if erase_status >= 400:
        return erase_status, erase_headers, erase_body
    return request_json(
        "POST",
        f"{worker_url.rstrip('/')}/completion",
        headers={},
        timeout=timeout,
        body=native_completion_body(
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            min_p=min_p,
            disable_draft=disable_draft,
            seed=seed,
            slot_id=slot_id,
        ),
    )


def run_trial(
    *,
    router_url: str,
    headers: dict[str, str],
    model: str,
    case: CorrectnessCase,
    run_index: int,
    cache_id_prefix: str,
    max_tokens: int,
    temperature: float,
    top_k: int,
    top_p: float,
    min_p: float,
    disable_draft: bool,
    seed: int,
    timeout: float,
    worker_id: str | None,
    allow_fallback: bool,
    max_generated_token_ids: int,
    cold_baseline: str,
    cold_worker_url: str,
    cold_slot_id: int,
    router_restore_validation: str,
) -> dict[str, Any]:
    trial_started = time.perf_counter()
    conversation_hash = sha256_hex(f"{cache_id_prefix}:{case.name}:{run_index}:conversation")
    cache_id = f"{safe_id_component(cache_id_prefix)}-{safe_id_component(case.name)}-{run_index:03d}"
    base_router: dict[str, Any] = {
        "target": "suffix_route",
        "scope": "conversation",
        "tenant_hash": sha256_hex(f"{cache_id_prefix}:tenant"),
        "conversation_hash": conversation_hash,
        "policy_id_hash": sha256_hex("cache-router-correctness-probe-policy"),
        "allow_fallback": allow_fallback,
    }
    if worker_id:
        base_router["worker_id"] = worker_id
    cold_router: dict[str, Any] | None = None
    if worker_id:
        cold_router = {"mode": "bypass", "worker_id": worker_id, "allow_fallback": allow_fallback}

    build_router = {
        **base_router,
        "mode": "refresh",
        "cache_id": cache_id,
        "prefix_text": case.prefix_text,
        "suffix_text": "",
    }
    build_status, build_headers, build_body = request_json(
        "POST",
        f"{router_url}/v1/completions",
        headers=headers,
        timeout=timeout,
        body=completion_body(
            model=model,
            prompt="",
            max_tokens=1,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            min_p=min_p,
            disable_draft=disable_draft,
            seed=seed + run_index,
            cache_router=build_router,
        ),
    )
    if build_status != 200:
        return {
            "case": case.name,
            "run": run_index,
            "status": FAIL,
            "phase": "build",
            "http_status": build_status,
            "request_id": build_headers.get("x-cache-router-request-id", ""),
            "error_type": build_body.get("error", {}).get("type") if isinstance(build_body.get("error"), dict) else None,
            "draft_disable_requested_for_correctness": bool(disable_draft),
            "draft_activity_observed_when_disabled": False,
            "validation_latency_ms": (time.perf_counter() - trial_started) * 1000.0,
        }
    build_meta = build_body.get("cache_router", {}).get("build", {}) if isinstance(build_body.get("cache_router"), dict) else {}
    build_cache_key_hash = build_meta.get("cache_key_hash") if isinstance(build_meta, dict) else None

    if cold_baseline == "worker-native":
        if not cold_worker_url:
            return {
                "case": case.name,
                "run": run_index,
                "status": FAIL,
                "phase": "cold",
                "http_status": 0,
                "error_type": "missing_cold_worker_url",
                "cache_key_hash": build_cache_key_hash,
                "draft_disable_requested_for_correctness": bool(disable_draft),
                "draft_activity_observed_when_disabled": False,
                "validation_latency_ms": (time.perf_counter() - trial_started) * 1000.0,
            }
        cold_status, cold_headers, cold_body = native_cold_completion(
            worker_url=cold_worker_url,
            prompt=case.prefix_text + case.suffix_text,
            max_tokens=max_tokens,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            min_p=min_p,
            disable_draft=disable_draft,
            seed=seed + run_index,
            slot_id=cold_slot_id,
            timeout=timeout,
        )
    else:
        cold_status, cold_headers, cold_body = request_json(
            "POST",
            f"{router_url}/v1/completions",
            headers=headers,
            timeout=timeout,
            body=completion_body(
                model=model,
                prompt=case.prefix_text + case.suffix_text,
                max_tokens=max_tokens,
                temperature=temperature,
                top_k=top_k,
                top_p=top_p,
                min_p=min_p,
                disable_draft=disable_draft,
                seed=seed + run_index,
                cache_router=cold_router,
            ),
        )
    if cold_status != 200:
        return {
            "case": case.name,
            "run": run_index,
            "status": FAIL,
            "phase": "cold",
            "http_status": cold_status,
            "request_id": cold_headers.get("x-cache-router-request-id", ""),
            "error_type": cold_body.get("error", {}).get("type") if isinstance(cold_body.get("error"), dict) else None,
            "cache_key_hash": build_cache_key_hash,
            "draft_disable_requested_for_correctness": bool(disable_draft),
            "draft_activity_observed_when_disabled": False,
            "validation_latency_ms": (time.perf_counter() - trial_started) * 1000.0,
        }

    use_router = {
        **base_router,
        "mode": "use",
        "cache_id": cache_id,
        "prefix_text": case.prefix_text,
        "suffix_text": case.suffix_text,
    }
    if router_restore_validation != "off":
        use_router["restore_validation"] = router_restore_validation
        use_router["restore_validation_recompute_on_mismatch"] = True
    restored_status, restored_headers, restored_body = request_json(
        "POST",
        f"{router_url}/v1/completions",
        headers=headers,
        timeout=timeout,
        body=completion_body(
            model=model,
            prompt=case.suffix_text,
            max_tokens=max_tokens,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p,
            min_p=min_p,
            disable_draft=disable_draft,
            seed=seed + run_index,
            cache_router=use_router,
        ),
    )
    if restored_status != 200:
        return {
            "case": case.name,
            "run": run_index,
            "status": FAIL,
            "phase": "restore",
            "http_status": restored_status,
            "request_id": restored_headers.get("x-cache-router-request-id", ""),
            "error_type": restored_body.get("error", {}).get("type") if isinstance(restored_body.get("error"), dict) else None,
            "cache_key_hash": build_cache_key_hash,
            "draft_disable_requested_for_correctness": bool(disable_draft),
            "draft_activity_observed_when_disabled": False,
            "validation_latency_ms": (time.perf_counter() - trial_started) * 1000.0,
        }

    cold_text = extract_native_text(cold_body) if cold_baseline == "worker-native" else extract_completion_text(cold_body)
    restored_text = extract_completion_text(restored_body)
    cold_tokens = tokenize_text(router_url, headers, model, cold_text, timeout)
    restored_tokens = tokenize_text(router_url, headers, model, restored_text, timeout)
    token_result = token_compare(cold_tokens, restored_tokens, max_generated_token_ids)
    draft_activity = extract_restored_draft_activity(restored_body)
    router_restore_validation = extract_router_restore_validation(restored_body)
    request_id = restored_headers.get("x-cache-router-request-id", "")
    events = events_for_request(router_url, headers, request_id, timeout)
    restore_event = representative_restore_event(events)
    classification = classify_trial(cold_text, restored_text, events)
    internal_cold_hash = router_restore_validation.get("cold_text_sha256")
    cold_baseline_nondeterministic = bool(
        isinstance(internal_cold_hash, str)
        and internal_cold_hash
        and internal_cold_hash != redacted_text_fingerprint(cold_text)["sha256"]
    )
    validation_statuses = classification.get("validation_statuses", [])
    if cold_baseline_nondeterministic:
        if internal_restore_validation_passed(
            cold_baseline_nondeterministic=True,
            router_restore_validation=router_restore_validation,
            validation_statuses=validation_statuses if isinstance(validation_statuses, list) else [],
        ):
            classification = {
                **classification,
                "status": PASS,
                "reason": "router_restore_validation_pass_cold_baseline_nondeterministic",
            }
        else:
            classification = {**classification, "status": FAIL, "reason": "cold_baseline_nondeterministic", "output_matches": False}
    elif token_result.get("match") is False:
        classification = {**classification, "status": FAIL, "reason": "restored_token_ids_mismatch", "output_matches": False}
    draft_disable_contradicted = bool(disable_draft and draft_activity.get("observed"))
    if draft_disable_contradicted:
        if classification["status"] == PASS:
            classification = {**classification, "status": FAIL}
        classification = {
            **classification,
            "reason": f"{classification['reason']}; draft_activity_observed_when_disabled",
        }
    return {
        "case": case.name,
        "run": run_index,
        "status": classification["status"],
        "phase": "compare",
        "reason": classification["reason"],
        "request_id": request_id,
        "trace_id": restore_event.get("trace_id"),
        "manifest_id": restore_event.get("manifest_id"),
        "cache_key_hash": restore_event.get("cache_key_hash") or build_cache_key_hash,
        "worker_id": restore_event.get("worker_id") or restored_headers.get("x-cache-router-worker", ""),
        "selected_worker": restored_headers.get("x-cache-router-worker", ""),
        "cache_hit_level": restored_headers.get("x-cache-router-cache-hit-level", ""),
        "used_restore": classification.get("used_restore", False),
        "recomputed_cold": classification.get("recomputed_cold", False),
        "output_matches": classification.get("output_matches", False),
        "cold_output": redacted_text_fingerprint(cold_text),
        "restored_output": redacted_text_fingerprint(restored_text),
        "tokenization_available": token_result.get("available", False),
        "token_match": token_result.get("match"),
        "checked_token_count": token_result.get("checked_token_count"),
        "cold_token_count": token_result.get("cold_token_count"),
        "restored_token_count": token_result.get("restored_token_count"),
        "cold_token_hash": token_result.get("cold_token_hash"),
        "restored_token_hash": token_result.get("restored_token_hash"),
        "event_count": len(events),
        "restore_latency_ms": decision_metric(restore_event, "restore_latency_ms"),
        "cached_tokens": decision_metric(restore_event, "cached_tokens"),
        "processed_prompt_tokens": decision_metric(restore_event, "processed_prompt_tokens"),
        "draft_activity": draft_activity,
        "draft_disable_requested_for_correctness": bool(disable_draft),
        "draft_activity_observed_when_disabled": draft_disable_contradicted,
        "router_restore_validation": router_restore_validation,
        "cold_baseline_nondeterministic": cold_baseline_nondeterministic,
        "validation_latency_ms": (time.perf_counter() - trial_started) * 1000.0,
        "decisions": classification.get("decisions", []),
        "fallback_reasons": classification.get("fallback_reasons", []),
        "validation_statuses": classification.get("validation_statuses", []),
        "decision_events": events,
    }


def run_live(args: argparse.Namespace) -> dict[str, Any]:
    router_url = args.router_url.rstrip("/")
    headers: dict[str, str] = {}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    if args.x_api_key:
        headers["X-API-Key"] = args.x_api_key

    all_cases = selected_cases(correctness_cases(args.near_context_repeat), args.case)
    has_mtp = deployment_has_mtp_enabled(router_url, headers, args.timeout, worker_id=args.worker_id)
    results: list[dict[str, Any]] = []
    decision_events: list[dict[str, Any]] = []
    started = time.perf_counter()
    for case in all_cases:
        if case.requires_mtp_enabled and not has_mtp:
            results.append(
                {
                    "case": case.name,
                    "status": SKIP,
                    "reason": "selected_worker_not_mtp_enabled" if args.worker_id else "no_worker_reports_mtp_enabled",
                    "description": case.description,
                }
            )
            continue
        for run_index in range(1, args.runs + 1):
            results.append(
                run_trial(
                    router_url=router_url,
                    headers=headers,
                    model=args.model,
                    case=case,
                    run_index=run_index,
                    cache_id_prefix=args.cache_id_prefix,
                    max_tokens=args.max_tokens,
                    temperature=args.temperature,
                    top_k=args.top_k,
                    top_p=args.top_p,
                    min_p=args.min_p,
                    disable_draft=not args.allow_draft,
                    seed=args.seed,
                    timeout=args.timeout,
                    worker_id=args.worker_id,
                    allow_fallback=not args.no_fallback,
                    max_generated_token_ids=args.max_generated_token_ids,
                    cold_baseline=args.cold_baseline,
                    cold_worker_url=args.cold_worker_url,
                    cold_slot_id=args.cold_slot_id,
                    router_restore_validation=args.router_restore_validation,
                )
            )

    for row in results:
        row_events = row.get("decision_events")
        if isinstance(row_events, list):
            decision_events.extend(event for event in row_events if isinstance(event, dict))
    validation_rows = [validation_result_from_trial(row, index) for index, row in enumerate(results, start=1)]
    artifact_errors = []
    artifact_errors.extend(validate_decision_rows(decision_events))
    artifact_errors.extend(validate_validation_rows(validation_rows))
    if decision_events:
        artifact_errors.extend(contracts.validate_cross_records(decision_events, validation_rows, decision_source=Path("decision-events.jsonl")))

    visible_results = [{key: value for key, value in row.items() if key != "decision_events"} for row in results]
    failed = [row for row in results if row.get("status") == FAIL]
    skipped = [row for row in results if row.get("status") == SKIP]
    passed = [row for row in results if row.get("status") == PASS]
    draft_observed_when_disabled = [
        row
        for row in results
        if row.get("draft_activity_observed_when_disabled") is True
    ]
    nondeterministic_cold = [
        row
        for row in results
        if row.get("cold_baseline_nondeterministic") is True
    ]
    runtime_blocked_failures = [row for row in failed if runtime_blocked_by_upstream(row)]
    cache_correctness_failures = [row for row in failed if not runtime_blocked_by_upstream(row)]
    status = "pass"
    if failed or artifact_errors or (skipped and not args.allow_skips):
        status = "runtime_blocked" if failed and len(runtime_blocked_failures) == len(failed) and not artifact_errors else "fail"
    output_paths: dict[str, str] = {}
    result = {
        "ok": not failed and not artifact_errors and (not skipped or args.allow_skips),
        "status": status,
        "scope": "operator-supplied live router correctness gate; compares deterministic response text and decision events, not logits/top-k",
        "router_url": router_url,
        "model": args.model,
        "runs_per_case": args.runs,
        "comparison_basis": "response_text_sha256_token_hashes_and_router_decision_events",
        "cold_baseline": args.cold_baseline,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "cases_requested": [case.name for case in all_cases],
        "case_count": len(all_cases),
        "case_status_counts": case_status_counts(results),
        "passed": len(passed),
        "failed": len(failed),
        "skipped": len(skipped),
        "runtime_blocked_failures": len(runtime_blocked_failures),
        "cache_correctness_failures": len(cache_correctness_failures),
        "allow_skips": args.allow_skips,
        "draft_disable_requested_for_correctness": not args.allow_draft,
        "router_restore_validation": args.router_restore_validation,
        "draft_disable_contradicted_by_timings": len(draft_observed_when_disabled),
        "draft_activity_observed_when_disabled": len(draft_observed_when_disabled),
        "cold_baseline_nondeterministic": len(nondeterministic_cold),
        "validation_rows": len(validation_rows),
        "decision_event_rows": len(decision_events),
        "artifact_validation_errors": artifact_errors,
        "results": visible_results,
        "notes": [
            "does not contact private hosts unless the supplied router URL points at them",
            "worker-native cold baseline contacts --cold-worker-url and erases the selected validation slot before cold generation",
            "does not write raw prompts or raw outputs to artifacts",
            "does not prove logits/top-k equivalence",
            "requests per-request draft/speculative-disable fields by default to isolate cache restore from MTP draft nondeterminism",
            "does not prove draft-disable was honored unless restored timings show no draft activity",
            "router-side deterministic restore validation is opt-in with --router-restore-validation=deterministic_recompute",
            "schema-valid validation rows are still deployment evidence, not a public correctness claim",
            "cache-id prefixes should be unique per run when testing shared deployments",
            "status=runtime_blocked is still a failing gate; it means deterministic cold-baseline proof is blocked by runtime draft/MTP nondeterminism",
        ],
    }
    if not args.no_output_files:
        out_dir = Path(args.out_dir) if args.out_dir else default_output_dir(args.cache_id_prefix)
        summary_path = out_dir / "summary.json"
        validations_path = out_dir / "validation-results.jsonl"
        decisions_path = out_dir / "decision-events.jsonl"
        write_json(summary_path, redacted_summary(result))
        write_jsonl(validations_path, validation_rows)
        write_jsonl(decisions_path, decision_events)
        output_paths = {
            "summary": str(summary_path),
            "validation_results": str(validations_path),
            "decision_events": str(decisions_path),
        }
    result["output_paths"] = output_paths
    return result


def run_self_test() -> dict[str, Any]:
    restored = [{"decision": "restore_then_generate", "cache_hit_level": "local_nvme"}]
    recompute = [{"decision": "fallback_after_restore_failure", "fallback_reason": "restore_validation_failed"}]
    no_events: list[dict[str, Any]] = []

    case_names = [case.name for case in correctness_cases(near_context_repeat=2)]
    expected_case_names = {
        "deterministic_generation",
        "system_prompt_boundary",
        "mid_conversation",
        "after_tool_output",
        "near_context_limit",
        "mtp_enabled",
        "branch_retry",
    }
    expected_case_ids = {safe_id_component(name) for name in expected_case_names}

    def passing_trial(case_name: str, run_id: int) -> dict[str, Any]:
        return {
            "case": case_name,
            "run": run_id,
            "status": PASS,
            "reason": "restored_output_matches_cold",
            "trace_id": f"trace-{case_name}",
            "manifest_id": f"manifest-{case_name}",
            "worker_id": "worker-main",
            "cache_key_hash": sha256_hex(f"self-test-cache-key:{case_name}"),
            "used_restore": True,
            "recomputed_cold": False,
            "output_matches": True,
            "restored_output": {"sha256": sha256_hex(f"same:{case_name}"), "bytes": 4, "chars": 4},
            "token_match": True,
            "restored_token_count": 1,
            "cold_token_count": 1,
            "validation_latency_ms": 1.0,
            "restore_latency_ms": 1.0,
        }

    def recompute_trial(case_name: str, run_id: int) -> dict[str, Any]:
        return {
            "case": case_name,
            "run": run_id,
            "status": PASS,
            "reason": "fail_closed_recomputed_cold_output_matches",
            "trace_id": f"trace-recompute-{case_name}",
            "manifest_id": f"manifest-recompute-{case_name}",
            "worker_id": "worker-main",
            "cache_key_hash": sha256_hex(f"self-test-cache-key:recompute:{case_name}"),
            "used_restore": False,
            "recomputed_cold": True,
            "output_matches": True,
            "fallback_reasons": ["restore_validation_failed"],
            "validation_statuses": ["quarantined"],
            "restored_output": {"sha256": sha256_hex(f"same-recompute:{case_name}"), "bytes": 4, "chars": 4},
            "token_match": True,
            "restored_token_count": 1,
            "cold_token_count": 1,
            "validation_latency_ms": 1.0,
            "restore_latency_ms": None,
        }

    failing_trial = {
        "case": "deterministic_generation",
        "run": 99,
        "status": FAIL,
        "reason": "restored_token_ids_mismatch",
        "trace_id": "trace-fail",
        "manifest_id": "manifest-fail",
        "worker_id": "worker-main",
        "cache_key_hash": sha256_hex("self-test-cache-key:fail"),
        "used_restore": True,
        "recomputed_cold": False,
        "output_matches": False,
        "restored_output": {"sha256": sha256_hex("hot"), "bytes": 3, "chars": 3},
        "token_match": False,
        "restored_token_count": 1,
        "cold_token_count": 1,
        "validation_latency_ms": 1.0,
        "restore_latency_ms": 1.0,
    }
    draft_disable_ignored_trial = {
        "case": "deterministic_generation",
        "run": 98,
        "status": FAIL,
        "reason": "matched; draft_activity_observed_when_disabled",
        "trace_id": "trace-draft-disable",
        "manifest_id": "manifest-draft-disable",
        "worker_id": "worker-main",
        "cache_key_hash": sha256_hex("self-test-cache-key:draft-disable"),
        "used_restore": True,
        "recomputed_cold": False,
        "output_matches": True,
        "draft_disable_requested_for_correctness": True,
        "draft_activity_observed_when_disabled": True,
        "restored_output": {"sha256": sha256_hex("same"), "bytes": 4, "chars": 4},
        "token_match": True,
        "restored_token_count": 1,
        "cold_token_count": 1,
        "validation_latency_ms": 1.0,
        "restore_latency_ms": 1.0,
    }
    nondeterministic_internal_pass_trial = {
        "case": "mtp_enabled",
        "run": 100,
        "status": FAIL,
        "reason": "cold_baseline_nondeterministic; draft_activity_observed_when_disabled",
        "trace_id": "trace-internal-pass",
        "manifest_id": "manifest-internal-pass",
        "worker_id": "worker-main",
        "cache_key_hash": sha256_hex("self-test-cache-key:internal-pass"),
        "used_restore": True,
        "recomputed_cold": False,
        "output_matches": False,
        "draft_disable_requested_for_correctness": True,
        "cold_baseline_nondeterministic": True,
        "draft_activity_observed_when_disabled": True,
        "validation_statuses": ["validated"],
        "router_restore_validation": {
            "mode": "deterministic_recompute",
            "status": "pass",
            "text_match": True,
            "restored_text_sha256": sha256_hex("same-internal"),
            "cold_text_sha256": sha256_hex("same-internal"),
        },
        "restored_output": {"sha256": sha256_hex("same-internal"), "bytes": 4, "chars": 4},
        "token_match": False,
        "restored_token_count": 1,
        "cold_token_count": 1,
        "validation_latency_ms": 1.0,
        "restore_latency_ms": 1.0,
    }
    fail_closed_draft_blocked_trial = {
        "case": "near_context_limit",
        "run": 101,
        "status": FAIL,
        "reason": "fail_closed_recomputed_cold_output_matches; draft_activity_observed_when_disabled",
        "trace_id": "trace-fail-closed-draft-blocked",
        "manifest_id": "manifest-fail-closed-draft-blocked",
        "worker_id": "worker-main",
        "cache_key_hash": sha256_hex("self-test-cache-key:fail-closed-draft-blocked"),
        "used_restore": False,
        "recomputed_cold": True,
        "output_matches": True,
        "fallback_reasons": ["restore_validation_failed"],
        "validation_statuses": ["quarantined"],
        "draft_disable_requested_for_correctness": True,
        "draft_activity_observed_when_disabled": True,
        "restored_output": {"sha256": sha256_hex("same-fail-closed"), "bytes": 4, "chars": 4},
        "token_match": True,
        "restored_token_count": 1,
        "cold_token_count": 1,
        "validation_latency_ms": 1.0,
        "restore_latency_ms": None,
    }
    skipped_trial = {
        "case": "mtp_enabled",
        "run": 0,
        "status": SKIP,
        "reason": "no_worker_reports_mtp_enabled",
    }
    validation_trials = []
    for index, case_name in enumerate(case_names, start=1):
        validation_trials.append(passing_trial(case_name, index))
        validation_trials.append(recompute_trial(case_name, index + 100))
    validation_trials.extend(
        [
            skipped_trial,
            failing_trial,
            draft_disable_ignored_trial,
            nondeterministic_internal_pass_trial,
            fail_closed_draft_blocked_trial,
        ]
    )
    validation_rows = [validation_result_from_trial(trial, index) for index, trial in enumerate(validation_trials, start=1)]
    validation_errors = validate_validation_rows(validation_rows)
    draft_options = draft_disable_options(True)
    draft_activity = extract_restored_draft_activity(
        {"cache_router": {"use": {"completion": {"timings": {"draft_n": 4, "draft_n_accepted": 3}}}}}
    )
    restore_validation = extract_router_restore_validation(
        {"cache_router": {"use": {"restore_validation": {"status": "fail", "cold_text_sha256": sha256_hex("cold")}}}}
    )
    mtp_worker_rows = [
        {"worker_id": "worker-mtp", "mtp_enabled": True},
        {"worker_id": "worker-no-mtp", "mtp_enabled": False},
    ]
    row_case_names = {
        row["validation_id"].removeprefix("val-").rsplit("-", 2)[0]
        for row in validation_rows
        if row.get("status") == SCHEMA_PASS
    }
    failed_rows = [row for row in validation_rows if row.get("status") == SCHEMA_FAIL]
    recompute_failed_rows = [row for row in failed_rows if "trace-recompute" in row.get("trace_id", "")]
    internal_pass_rows = [row for row in validation_rows if row.get("trace_id") == "trace-internal-pass"]
    draft_disable_rows = [row for row in validation_rows if row.get("trace_id") == "trace-draft-disable"]
    fail_closed_draft_blocked_rows = [row for row in validation_rows if row.get("trace_id") == "trace-fail-closed-draft-blocked"]
    skipped_rows = [row for row in validation_rows if row.get("status") == SCHEMA_SKIPPED]
    mtp_rows = [row for row in validation_rows if "mtp-enabled" in row.get("validation_id", "")]
    recompute_rows = [trial for trial in validation_trials if trial.get("recomputed_cold") is True]
    checks = [
        classify_trial("same", "same", restored)["status"] == PASS,
        classify_trial("same", "same", recompute)["status"] == PASS,
        classify_trial("cold", "hot", restored)["status"] == FAIL,
        classify_trial("same", "same", no_events)["status"] == FAIL,
        set(case_names) == expected_case_names,
        row_case_names == expected_case_ids,
        len(recompute_rows) == len(case_names) + 1,
        mtp_rows and all(row.get("validation_type") == "mtp_restore" for row in mtp_rows),
        rows_have_mtp_enabled(mtp_worker_rows) is True,
        rows_have_mtp_enabled(mtp_worker_rows, worker_id="worker-mtp") is True,
        rows_have_mtp_enabled(mtp_worker_rows, worker_id="worker-no-mtp") is False,
        rows_have_mtp_enabled(mtp_worker_rows, worker_id="worker-missing") is False,
        draft_options.get("speculative.n_max") == 0 and draft_options.get("speculative.type") == "none",
        draft_activity.get("observed") is True and draft_activity.get("draft_n_accepted") == 3,
        restore_validation.get("status") == "fail" and restore_validation.get("cold_text_sha256") == sha256_hex("cold"),
        len(skipped_rows) == 1,
        len(recompute_failed_rows) == len(case_names),
        len(internal_pass_rows) == 1 and internal_pass_rows[0].get("status") == SCHEMA_PASS,
        len(internal_pass_rows) == 1 and internal_pass_rows[0].get("checks", {}).get("deterministic_text_match") == "pass",
        len(draft_disable_rows) == 1 and draft_disable_rows[0].get("status") == SCHEMA_PASS,
        len(draft_disable_rows) == 1 and draft_disable_rows[0].get("failure_reason") is None,
        len(fail_closed_draft_blocked_rows) == 1 and fail_closed_draft_blocked_rows[0].get("status") == SCHEMA_FAIL,
        len(fail_closed_draft_blocked_rows) == 1 and fail_closed_draft_blocked_rows[0].get("fallback_required") is True,
        len(fail_closed_draft_blocked_rows) == 1 and fail_closed_draft_blocked_rows[0].get("quarantine_recommended") is True,
        trial_internal_restore_validation_passed(nondeterministic_internal_pass_trial) is True,
        runtime_blocked_by_upstream(nondeterministic_internal_pass_trial) is True,
        runtime_blocked_by_upstream(draft_disable_ignored_trial) is True,
        runtime_blocked_by_upstream(failing_trial) is False,
        case_status_counts(validation_trials).get("mtp_enabled", {}).get(FAIL) == 1,
        len(failed_rows) == len(case_names) + 2 and all(row.get("fallback_required") is True for row in failed_rows),
        not validation_errors,
    ]
    return {
        "ok": all(checks),
        "scope": "self-test only; no router contacted",
        "checks": len(checks),
        "passed": sum(1 for item in checks if item),
        "case_names_checked": case_names,
        "schema_valid_pass_rows": len(row_case_names),
        "schema_valid_pass_trial_rows": sum(1 for row in validation_rows if row.get("status") == SCHEMA_PASS),
        "schema_valid_recompute_rows": len(recompute_rows),
        "schema_valid_skipped_rows": len(skipped_rows),
        "schema_valid_fail_rows": len(failed_rows),
        "validation_rows_checked": len(validation_rows),
        "validation_errors": validation_errors,
        "live_only_rows": [129, 130],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="Run local probe self-tests without contacting a router.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--router-url", help="Operator-supplied Cachy Router URL, for example http://<router-lan-ip>:18080.")
    parser.add_argument("--model", help="Configured model ID served through the router.")
    parser.add_argument("--api-key", help="Bearer token for router auth, if configured.")
    parser.add_argument("--x-api-key", help="X-API-Key value for router auth, if configured.")
    parser.add_argument("--worker-id", help="Optional explicit worker target.")
    parser.add_argument(
        "--cold-baseline",
        choices=["router-openai", "worker-native"],
        default="router-openai",
        help="Cold comparison path. router-openai uses the router OpenAI bypass path; worker-native uses --cold-worker-url /completion with a slot erase.",
    )
    parser.add_argument("--cold-worker-url", default="", help="Worker URL used only when --cold-baseline=worker-native.")
    parser.add_argument("--cold-slot-id", type=int, default=0, help="Worker slot ID erased and used for --cold-baseline=worker-native.")
    parser.add_argument("--no-fallback", action="store_true", help="Set cache_router.allow_fallback=false.")
    parser.add_argument("--allow-skips", action="store_true", help="Allow skipped cases, for subset deployment checks.")
    parser.add_argument("--case", action="append", default=[], help="Case to run; repeatable. Default: all.")
    parser.add_argument("--runs", type=int, default=10, help="Runs per selected case. Final gate expects at least 10.")
    parser.add_argument("--max-tokens", type=int, default=32, help="Generated tokens per cold/restored comparison.")
    parser.add_argument("--max-generated-token-ids", type=int, default=32, help="Generated token IDs to compare when /tokenize exposes token lists.")
    parser.add_argument("--temperature", type=float, default=0.0, help="Sampling temperature.")
    parser.add_argument("--top-k", type=int, default=40, help="Explicit top-k sent to cold and restored requests for generation-parameter parity.")
    parser.add_argument("--top-p", type=float, default=0.95, help="Explicit top-p sent to cold and restored requests for generation-parameter parity.")
    parser.add_argument("--min-p", type=float, default=0.05, help="Explicit min-p sent to cold and restored requests for generation-parameter parity.")
    parser.add_argument(
        "--allow-draft",
        action="store_true",
        help="Do not send per-request draft/speculative-disable fields. By default the correctness gate isolates cache restore from MTP draft nondeterminism.",
    )
    parser.add_argument(
        "--router-restore-validation",
        choices=["off", "deterministic_recompute"],
        default="off",
        help="Ask the router to validate restored output. deterministic_recompute erases the selected slot, recomputes cold on the same worker, and allows fail-closed cold output on mismatch.",
    )
    parser.add_argument("--seed", type=int, default=12345, help="Base deterministic seed.")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    parser.add_argument("--near-context-repeat", type=int, default=512, help="Repeated lines in the near-context case prefix.")
    parser.add_argument("--cache-id-prefix", default="correctness-probe", help="Public-safe cache ID prefix for this run.")
    parser.add_argument("--out-dir", help="Directory for redacted summary and JSONL artifacts. Default: runtime/cache-router-correctness/<run-id>.")
    parser.add_argument("--no-output-files", action="store_true", help="Do not write redacted runtime artifacts for a live run.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        result = run_self_test()
    else:
        if not args.router_url or not args.model:
            raise SystemExit("--router-url and --model are required unless --self-test is used")
        if args.cold_baseline == "worker-native" and not args.cold_worker_url:
            raise SystemExit("--cold-baseline=worker-native requires --cold-worker-url")
        if (
            args.runs < 1
            or args.max_tokens < 1
            or args.max_generated_token_ids < 1
            or args.cold_slot_id < 0
            or args.top_k < 1
            or args.top_p <= 0
            or args.min_p < 0
            or args.timeout <= 0
            or args.near_context_repeat < 1
        ):
            raise SystemExit("runs, max tokens, max generated token IDs, top-k, top-p, min-p, timeout, and near-context repeat must be positive")
        result = run_live(args)

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
