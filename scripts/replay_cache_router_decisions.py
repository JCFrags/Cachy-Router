#!/usr/bin/env python3
"""Replay synthetic cache-router decisions without running a router."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPLAY_ROOT = PACKAGE_ROOT / "docs/architecture/examples/replay"
REQUESTS_PATH = REPLAY_ROOT / "requests.jsonl"
WORKERS_PATH = REPLAY_ROOT / "workers.jsonl"
REGISTRY_PATH = REPLAY_ROOT / "registry.jsonl"
POLICIES_PATH = REPLAY_ROOT / "policies.jsonl"
EXPECTED_PATH = REPLAY_ROOT / "expected-decisions.jsonl"
POLICY_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-policy.schema.json"
DECISION_SCHEMA_PATH = PACKAGE_ROOT / "schemas/cache-router/cache-decision-event.schema.json"

sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))
import validate_cache_router_contracts as contracts  # noqa: E402


STRICT_COMPATIBILITY_FIELDS = contracts.ALL_STRICT_COMPATIBILITY_FIELDS


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError(f"{path}:{line_no}: row must be an object")
        rows.append(row)
    return rows


def validate_policy(policy: dict[str, Any], policy_schema: dict[str, Any], source: str) -> list[str]:
    errors = contracts.schema_errors(policy, policy_schema, policy_schema, source)
    errors.extend(contracts.privacy_errors(policy, source))
    errors.extend(contracts.require_sha(policy.get("policy_id_hash"), source, "policy_id_hash"))
    if policy.get("allow_cross_tenant_reuse") is not False:
        errors.append(f"{source}: allow_cross_tenant_reuse must be false")
    if policy.get("audit_required") is not True:
        errors.append(f"{source}: audit_required must be true")
    if policy.get("require_tenant_hash") is not True:
        errors.append(f"{source}: require_tenant_hash must be true")
    if policy.get("user_content_global_cache_allowed") is not False:
        errors.append(f"{source}: user_content_global_cache_allowed must be false")
    return errors


def eligible_workers(request: dict[str, Any], workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {worker["worker_id"]: worker for worker in workers}
    selected: list[dict[str, Any]] = []
    for worker_id in request.get("candidate_worker_ids", []):
        worker = by_id.get(worker_id)
        if not worker:
            continue
        if worker.get("health_status") != "healthy":
            continue
        if worker.get("capacity_available") is not True:
            continue
        if request["model_id"] not in worker.get("loaded_model_ids", []):
            continue
        if not worker_matches_request(worker, request):
            continue
        selected.append(worker)
    return selected


def worker_matches_request(worker: dict[str, Any], request: dict[str, Any]) -> bool:
    compatibility = worker.get("compatibility", {})
    request_compatibility = request.get("compatibility", {})
    for field in STRICT_COMPATIBILITY_FIELDS:
        if compatibility.get(field) != request_compatibility.get(field):
            return False
    return True


def candidate_manifests(request: dict[str, Any], registry: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        manifest
        for manifest in registry
        if manifest.get("cache_key_hash") == request.get("cache_key_hash")
        or manifest.get("token_prefix_hash") == request.get("token_prefix_hash")
    ]


def manifest_matches_request(manifest: dict[str, Any], request: dict[str, Any]) -> tuple[bool, str | None]:
    for field in ["cache_key_hash", "scope", "model_id", "token_prefix_hash", "prefix_token_ids_hash"]:
        if manifest.get(field) != request.get(field):
            return False, field
    if manifest.get("n_tokens") != request.get("n_tokens"):
        return False, "n_tokens"
    if manifest.get("validation_status") != "validated":
        return False, "validation_status"
    if manifest.get("encrypted_at_rest") is not True:
        return False, "encrypted_at_rest"
    manifest_compatibility = manifest.get("compatibility", {})
    request_compatibility = request.get("compatibility", {})
    for field in STRICT_COMPATIBILITY_FIELDS:
        if manifest_compatibility.get(field) != request_compatibility.get(field):
            return False, field
    return True, None


def policy_allows(policy: dict[str, Any], request: dict[str, Any], manifest: dict[str, Any]) -> tuple[bool, str | None]:
    if request.get("scope") not in policy.get("allowed_scopes", []):
        return False, "scope_not_allowed"
    if policy.get("allow_cross_tenant_reuse") is not False:
        return False, "cross_tenant_policy_unsafe"
    if request.get("tenant_hash") != manifest.get("tenant_hash"):
        return False, "tenant_scope_mismatch"
    if (
        request.get("scope") == "conversation"
        and policy.get("require_conversation_hash_for_conversation_scope") is True
        and request.get("conversation_hash") != manifest.get("conversation_hash")
    ):
        return False, "conversation_scope_mismatch"
    if request.get("scope") == "global_system":
        allowed = (
            policy.get("allow_global_system_cache") is True
            and request.get("token_prefix_hash") in policy.get("operator_global_allowlist_hashes", [])
        )
        if not allowed:
            return False, "global_system_not_allowlisted"
    if request.get("scope") == "private_disabled":
        return False, "private_disabled"
    return True, None


def has_local_residency(manifest: dict[str, Any], worker_id: str) -> bool:
    return any(
        item.get("worker_id") == worker_id and item.get("level") == "local_nvme"
        and item.get("status") == "available"
        for item in manifest.get("residency", [])
    )


def first_eligible_worker(workers: list[dict[str, Any]]) -> dict[str, Any] | None:
    return workers[0] if workers else None


def event_metrics(request: dict[str, Any], *, cached_tokens: int, processed_tokens: int, hydration_ms: float | None = None, restore_ms: float | None = None) -> dict[str, Any]:
    prompt_tokens = int(request["n_tokens"])
    reuse_ratio = cached_tokens / prompt_tokens if prompt_tokens else 0
    return {
        "decision_latency_ms": 1.0,
        "registry_lookup_latency_ms": 0.5,
        "hydration_latency_ms": hydration_ms,
        "restore_latency_ms": restore_ms,
        "ttft_ms": None,
        "prompt_tokens": prompt_tokens,
        "cached_tokens": cached_tokens,
        "processed_prompt_tokens": processed_tokens,
        "generated_tokens": None,
        "prompt_tps": None,
        "eval_tps": None,
        "reuse_ratio": reuse_ratio,
        "full_reprocess_suspected": "not_interpreted",
        "cache_event_basis": "synthetic_example",
        "restore_observed_basis": "synthetic_example",
    }


def make_event(
    index: int,
    request: dict[str, Any],
    policy: dict[str, Any],
    *,
    phase: str,
    decision: str,
    worker_id: str | None,
    manifest: dict[str, Any] | None,
    cache_hit_level: str,
    compatibility_result: str,
    validation_status: str,
    fallback_required: bool,
    fallback_reason: str | None,
    metrics: dict[str, Any],
    note: str,
) -> dict[str, Any]:
    return {
        "schema_version": "2026-06-30.1",
        "event_id": f"evt-replay-{index:03d}",
        "trace_id": request["trace_id"],
        "request_id": request["request_id"],
        "request_hash": request["request_hash"],
        "timestamp": f"2026-06-30T06:00:{index:02d}Z",
        "phase": phase,
        "decision": decision,
        "tenant_hash": request["tenant_hash"],
        "conversation_hash": request["conversation_hash"],
        "scope": request["scope"],
        "model_id": request["model_id"],
        "worker_id": worker_id,
        "cache_key_hash": manifest["cache_key_hash"] if manifest else request.get("cache_key_hash"),
        "manifest_id": manifest["manifest_id"] if manifest else None,
        "cache_hit_level": cache_hit_level,
        "compatibility_result": compatibility_result,
        "validation_status": validation_status,
        "fallback_required": fallback_required,
        "fallback_reason": fallback_reason,
        "latency_ms": metrics["decision_latency_ms"],
        "metrics": metrics,
        "policy": {
            "scope_allowed": request["scope"] in policy.get("allowed_scopes", []),
            "persistence_allowed": request["scope"] != "private_disabled" and policy.get("allow_private_disabled_persistence") is False,
            "cross_tenant_reuse_allowed": False,
            "global_system_allowlisted": request.get("token_prefix_hash") in policy.get("operator_global_allowlist_hashes", []),
            "policy_id_hash": policy["policy_id_hash"],
            "policy_basis": "policy_rule",
        },
        "privacy": {
            "raw_prompt_logged": False,
            "raw_tenant_id_logged": False,
            "raw_conversation_id_logged": False,
            "raw_cache_blob_path_logged": False,
            "raw_environment_logged": False,
            "contains_secret_material": False,
            "redaction_status": "synthetic_example",
        },
        "notes": note,
    }


def decide_one(index: int, request: dict[str, Any], workers: list[dict[str, Any]], registry: list[dict[str, Any]], policies: dict[str, dict[str, Any]]) -> dict[str, Any]:
    policy = policies[request["policy_id"]]
    worker_candidates = eligible_workers(request, workers)
    manifests = candidate_manifests(request, registry)
    fallback_worker = first_eligible_worker(worker_candidates)

    if not worker_candidates:
        return make_event(
            index,
            request,
            policy,
            phase="request_failed",
            decision="reject_capacity",
            worker_id=None,
            manifest=None,
            cache_hit_level="none",
            compatibility_result="not_checked",
            validation_status="not_applicable",
            fallback_required=True,
            fallback_reason="worker_capacity",
            metrics=event_metrics(request, cached_tokens=0, processed_tokens=0),
            note="Scenario F: no eligible synthetic worker capacity.",
        )

    if not manifests:
        return make_event(
            index,
            request,
            policy,
            phase="cold_prefill_selected",
            decision="cold_prefill",
            worker_id=fallback_worker["worker_id"] if fallback_worker else None,
            manifest=None,
            cache_hit_level="none",
            compatibility_result="miss",
            validation_status="not_applicable",
            fallback_required=True,
            fallback_reason="no_compatible_manifest",
            metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
            note="Scenario A: no compatible synthetic manifest.",
        )

    exact_matches: list[dict[str, Any]] = []
    mismatch_manifest = manifests[0]
    for manifest in manifests:
        matched, _reason = manifest_matches_request(manifest, request)
        if matched:
            exact_matches.append(manifest)
    if not exact_matches:
        return make_event(
            index,
            request,
            policy,
            phase="cold_prefill_selected",
            decision="cold_prefill",
            worker_id=fallback_worker["worker_id"] if fallback_worker else None,
            manifest=mismatch_manifest,
            cache_hit_level="registry_only",
            compatibility_result="mismatch",
            validation_status="not_checked",
            fallback_required=True,
            fallback_reason="cache_key_mismatch",
            metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
            note="Scenario E: strict compatibility mismatch, cold prefill.",
        )

    manifest = exact_matches[0]
    allowed, _policy_reason = policy_allows(policy, request, manifest)
    if not allowed:
        if policy.get("fallback_on_policy_denial") == "reject_policy":
            decision = "reject_policy"
            phase = "request_failed"
            worker_id = None
        else:
            decision = "cold_prefill"
            phase = "cold_prefill_selected"
            worker_id = fallback_worker["worker_id"] if fallback_worker else None
        return make_event(
            index,
            request,
            policy,
            phase=phase,
            decision=decision,
            worker_id=worker_id,
            manifest=manifest,
            cache_hit_level="registry_only",
            compatibility_result="policy_denied",
            validation_status="not_checked",
            fallback_required=True,
            fallback_reason="policy_denied",
            metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
            note="Scenario D: tenant/scope policy denies cache reuse.",
        )

    if manifest.get("restore_validation_status") == "fail":
        worker = first_eligible_worker([worker for worker in worker_candidates if worker.get("supports_restore") is True])
        if not worker:
            return make_event(
                index,
                request,
                policy,
                phase="cold_prefill_selected",
                decision="cold_prefill",
                worker_id=fallback_worker["worker_id"] if fallback_worker else None,
                manifest=manifest,
                cache_hit_level="registry_only",
                compatibility_result="match",
                validation_status="not_checked",
                fallback_required=True,
                fallback_reason="no_compatible_manifest",
                metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
                note="Synthetic restore candidate lacked restore capability, cold prefill.",
            )
        return make_event(
            index,
            request,
            policy,
            phase="restore_validated",
            decision="fallback_after_restore_failure",
            worker_id=worker["worker_id"],
            manifest=manifest,
            cache_hit_level="durable_blob",
            compatibility_result="match",
            validation_status="quarantined",
            fallback_required=True,
            fallback_reason="restore_validation_failed",
            metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"]), hydration_ms=12.0, restore_ms=20.0),
            note="Scenario G: synthetic restore validation failed, fallback required.",
        )

    for worker in worker_candidates:
        if worker.get("supports_restore") is True and has_local_residency(manifest, worker["worker_id"]):
            return make_event(
                index,
                request,
                policy,
                phase="worker_selected",
                decision="hot_local_hit",
                worker_id=worker["worker_id"],
                manifest=manifest,
                cache_hit_level="local_nvme",
                compatibility_result="match",
                validation_status="validated",
                fallback_required=False,
                fallback_reason=None,
                metrics=event_metrics(request, cached_tokens=int(request["n_tokens"]), processed_tokens=0),
                note="Scenario B: compatible local synthetic cache hit.",
            )

    if manifest.get("durable_available") is True:
        worker = first_eligible_worker([
            worker
            for worker in worker_candidates
            if worker.get("supports_hydration") is True and worker.get("supports_restore") is True
        ])
        if worker:
            return make_event(
                index,
                request,
                policy,
                phase="hydrate_requested",
                decision="durable_hit_hydrate",
                worker_id=worker["worker_id"],
                manifest=manifest,
                cache_hit_level="durable_blob",
                compatibility_result="match",
                validation_status="validated",
                fallback_required=False,
                fallback_reason=None,
                metrics=event_metrics(request, cached_tokens=int(request["n_tokens"]), processed_tokens=0, hydration_ms=12.0),
                note="Scenario C: compatible durable synthetic cache needs hydration.",
            )

    return make_event(
        index,
        request,
        policy,
        phase="cold_prefill_selected",
        decision="cold_prefill",
        worker_id=fallback_worker["worker_id"] if fallback_worker else None,
        manifest=manifest,
        cache_hit_level="registry_only",
        compatibility_result="match",
        validation_status="not_checked",
        fallback_required=True,
        fallback_reason="no_compatible_manifest",
        metrics=event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
        note="Synthetic compatible manifest could not be restored, cold prefill.",
    )


def compare_expected(events: list[dict[str, Any]], expected: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    by_request = {event["request_id"]: event for event in events}
    for row in expected:
        request_id = row["request_id"]
        event = by_request.get(request_id)
        if event is None:
            errors.append(f"missing event for {request_id}")
            continue
        for key, expected_value in row.items():
            if key == "request_id":
                continue
            actual = event.get(key)
            if actual != expected_value:
                errors.append(f"{request_id}: expected {key}={expected_value!r}, got {actual!r}")
    return errors


def replay() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fixture_summary = contracts.validate_replay_fixture_dir(REPLAY_ROOT)
    policy_schema = load_json(POLICY_SCHEMA_PATH)
    decision_schema = load_json(DECISION_SCHEMA_PATH)
    requests = load_jsonl(REQUESTS_PATH)
    workers = load_jsonl(WORKERS_PATH)
    registry = load_jsonl(REGISTRY_PATH)
    policies_list = load_jsonl(POLICIES_PATH)
    expected = load_jsonl(EXPECTED_PATH)
    policies = {policy["policy_id"]: policy for policy in policies_list}
    errors: list[str] = list(fixture_summary["errors"])

    for index, policy in enumerate(policies_list, start=1):
        errors.extend(validate_policy(policy, policy_schema, f"{POLICIES_PATH}:{index}"))

    events = [decide_one(index, request, workers, registry, policies) for index, request in enumerate(requests, start=1)]
    for index, event in enumerate(events, start=1):
        errors.extend(contracts.validate_decision_event(event, decision_schema, f"replay-event:{index}"))
    errors.extend(compare_expected(events, expected))

    summary = {
        "ok": not errors,
        "errors": errors,
        "requests": len(requests),
        "workers": len(workers),
        "registry_manifests": len(registry),
        "policies": len(policies_list),
        "events": len(events),
        "expected_decisions": len(expected),
        "decisions_by_type": {},
        "fixture_preflight_ok": fixture_summary["ok"],
    }
    counts: dict[str, int] = {}
    for event in events:
        counts[event["decision"]] = counts.get(event["decision"], 0) + 1
    summary["decisions_by_type"] = dict(sorted(counts.items()))
    return events, summary


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write emitted decision events to this JSONL path")
    parser.add_argument("--json", action="store_true", help="print a JSON summary instead of event JSONL")
    args = parser.parse_args()

    events, summary = replay()
    if not summary["ok"]:
        print("cache-router replay failed:", file=sys.stderr)
        for error in summary["errors"]:
            print(f"- {error}", file=sys.stderr)
        return 1

    if args.output:
        write_jsonl(args.output, events)
        summary["output"] = str(args.output)

    if args.json or args.output:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        for event in events:
            print(json.dumps(event, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
