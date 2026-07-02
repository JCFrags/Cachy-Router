#!/usr/bin/env python3
"""Offline mocked cache-router ranking prototype.

This script consumes synthetic ranking fixtures only. It does not open sockets,
read model files, hydrate cache blobs, restore slots, or run a production router.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
PROTOTYPE_ROOT = PACKAGE_ROOT / "docs/architecture/examples/router-prototype"
CASES_PATH = PROTOTYPE_ROOT / "ranking-cases.jsonl"
GOLDEN_PATH = PROTOTYPE_ROOT / "golden-decisions.jsonl"

sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))
import replay_cache_router_decisions as replay  # noqa: E402
import validate_cache_router_contracts as contracts  # noqa: E402


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


def deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def by_id(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    return {row[key]: row for row in rows}


def apply_worker_overrides(workers: list[dict[str, Any]], overrides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    worker_map = by_id(copy.deepcopy(workers), "worker_id")
    for row in overrides:
        worker_id = row["worker_id"]
        if worker_id not in worker_map:
            raise ValueError(f"worker override references unknown worker_id {worker_id}")
        worker_map[worker_id] = deep_merge(worker_map[worker_id], row.get("overrides", {}))
    return list(worker_map.values())


def materialize_case(
    case: dict[str, Any],
    requests_by_id: dict[str, dict[str, Any]],
    workers: list[dict[str, Any]],
    manifests_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    request = deep_merge(requests_by_id[case["base_request_id"]], case.get("request_overrides", {}))
    case_workers = apply_worker_overrides(workers, case.get("worker_overrides", []))
    registry: list[dict[str, Any]] = []
    for row in case.get("registry", []):
        base_manifest = manifests_by_id[row["base_manifest_id"]]
        registry.append(deep_merge(base_manifest, row.get("overrides", {})))
    return request, case_workers, registry


def local_residency_available(manifest: dict[str, Any], worker_id: str) -> bool:
    for item in manifest.get("residency", []):
        if item.get("worker_id") == worker_id and item.get("level") == "local_nvme":
            return item.get("status", "available") == "available"
    return False


def candidate_manifests_with_reasons(
    request: dict[str, Any],
    registry: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    exact: list[dict[str, Any]] = []
    rejected: list[tuple[dict[str, Any], str]] = []
    for manifest in replay.candidate_manifests(request, registry):
        matched, reason = replay.manifest_matches_request(manifest, request)
        if matched:
            exact.append(manifest)
        else:
            rejected.append((manifest, reason or "unknown"))
    return exact, rejected


def ranked_options(
    request: dict[str, Any],
    workers: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    policy: dict[str, Any],
) -> list[tuple[tuple[int, int, int, str, str], str, dict[str, Any], dict[str, Any]]]:
    worker_order = {worker_id: index for index, worker_id in enumerate(request.get("candidate_worker_ids", []))}
    options: list[tuple[tuple[int, int, int, str, str], str, dict[str, Any], dict[str, Any]]] = []
    for manifest in manifests:
        allowed, _reason = replay.policy_allows(policy, request, manifest)
        if not allowed:
            continue
        priority = int(manifest.get("rank_priority", 100))
        for worker in workers:
            order = worker_order.get(worker["worker_id"], 10_000)
            if worker.get("supports_restore") is True and local_residency_available(manifest, worker["worker_id"]):
                options.append(((0, priority, order, manifest["manifest_id"], worker["worker_id"]), "hot_local", manifest, worker))
            elif (
                manifest.get("durable_available") is True
                and worker.get("supports_restore") is True
                and worker.get("supports_hydration") is True
            ):
                options.append(((1, priority, order, manifest["manifest_id"], worker["worker_id"]), "durable", manifest, worker))
    return sorted(options, key=lambda row: row[0])


def event_for_case(
    index: int,
    case: dict[str, Any],
    request: dict[str, Any],
    workers: list[dict[str, Any]],
    registry: list[dict[str, Any]],
    policies: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    policy = policies[request["policy_id"]]
    worker_candidates = replay.eligible_workers(request, workers)
    fallback_worker = replay.first_eligible_worker(worker_candidates)
    exact_manifests, rejected = candidate_manifests_with_reasons(request, registry)

    def finish(event: dict[str, Any]) -> dict[str, Any]:
        event["event_id"] = f"evt-prototype-{index:03d}"
        event["timestamp"] = f"2026-06-30T06:30:{index:02d}Z"
        event["notes"] = f"{case['case_id']}: {event.get('notes', '')}"[:200]
        return event

    if not worker_candidates:
        return finish(
            replay.make_event(
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
                metrics=replay.event_metrics(request, cached_tokens=0, processed_tokens=0),
                note="no eligible synthetic worker capacity",
            )
        )

    if not exact_manifests:
        manifest = sorted((row for row, _reason in rejected), key=lambda row: row["manifest_id"])[0] if rejected else None
        note = "strict compatibility mismatch" if rejected else "no compatible synthetic manifest"
        return finish(
            replay.make_event(
                index,
                request,
                policy,
                phase="cold_prefill_selected",
                decision="cold_prefill",
                worker_id=fallback_worker["worker_id"] if fallback_worker else None,
                manifest=manifest,
                cache_hit_level="registry_only" if manifest else "none",
                compatibility_result="mismatch" if manifest else "miss",
                validation_status="not_checked" if manifest else "not_applicable",
                fallback_required=True,
                fallback_reason="cache_key_mismatch" if manifest else "no_compatible_manifest",
                metrics=replay.event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
                note=note,
            )
        )

    policy_denied = [manifest for manifest in exact_manifests if not replay.policy_allows(policy, request, manifest)[0]]
    policy_allowed = [manifest for manifest in exact_manifests if replay.policy_allows(policy, request, manifest)[0]]
    restore_failed = [manifest for manifest in policy_allowed if manifest.get("restore_validation_status") == "fail"]
    if restore_failed and len(restore_failed) == len(policy_allowed):
        manifest = sorted(restore_failed, key=lambda row: (int(row.get("rank_priority", 100)), row["manifest_id"]))[0]
        worker = replay.first_eligible_worker([worker for worker in worker_candidates if worker.get("supports_restore") is True])
        if worker:
            return finish(
                replay.make_event(
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
                    metrics=replay.event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"]), hydration_ms=12.0, restore_ms=20.0),
                    note="synthetic restore validation failed, fallback required",
                )
            )
        return finish(
            replay.make_event(
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
                metrics=replay.event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
                note="restore-failed manifest lacked eligible restore worker",
            )
        )

    usable_manifests = [manifest for manifest in exact_manifests if manifest.get("restore_validation_status") != "fail"]
    options = ranked_options(request, worker_candidates, usable_manifests, policy)
    if not options:
        manifest = sorted(exact_manifests, key=lambda row: (int(row.get("rank_priority", 100)), row["manifest_id"]))[0]
        if policy_denied and len(policy_denied) == len(exact_manifests):
            compatibility_result = "policy_denied"
            fallback_reason = "policy_denied"
            note = "tenant or scope policy denied cache reuse"
        else:
            compatibility_result = "match"
            fallback_reason = "no_compatible_manifest"
            note = "compatible manifest exists but no worker can restore or hydrate"
        return finish(
            replay.make_event(
                index,
                request,
                policy,
                phase="cold_prefill_selected",
                decision="cold_prefill",
                worker_id=fallback_worker["worker_id"] if fallback_worker else None,
                manifest=manifest,
                cache_hit_level="registry_only",
                compatibility_result=compatibility_result,
                validation_status="not_checked",
                fallback_required=True,
                fallback_reason=fallback_reason,
                metrics=replay.event_metrics(request, cached_tokens=0, processed_tokens=int(request["n_tokens"])),
                note=note,
            )
        )

    _rank, option_kind, manifest, worker = options[0]
    if option_kind == "hot_local":
        return finish(
            replay.make_event(
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
                metrics=replay.event_metrics(request, cached_tokens=int(request["n_tokens"]), processed_tokens=0),
                note="ranked hot local synthetic cache hit",
            )
        )
    return finish(
        replay.make_event(
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
            metrics=replay.event_metrics(request, cached_tokens=int(request["n_tokens"]), processed_tokens=0, hydration_ms=12.0),
            note="ranked durable synthetic cache hit requiring hydration",
        )
    )


def compare_golden(events: list[dict[str, Any]], golden: list[dict[str, Any]]) -> list[str]:
    by_case = {event["case_id"]: event["event"] for event in events}
    errors: list[str] = []
    for row in golden:
        event = by_case.get(row["case_id"])
        if event is None:
            errors.append(f"missing prototype event for {row['case_id']}")
            continue
        for key, expected_value in row.items():
            if key == "case_id":
                continue
            if event.get(key) != expected_value:
                errors.append(f"{row['case_id']}: expected {key}={expected_value!r}, got {event.get(key)!r}")
    return errors


def run_prototype() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cases = load_jsonl(CASES_PATH)
    golden = load_jsonl(GOLDEN_PATH)
    requests = load_jsonl(replay.REQUESTS_PATH)
    workers = load_jsonl(replay.WORKERS_PATH)
    registry = load_jsonl(replay.REGISTRY_PATH)
    policies_list = load_jsonl(replay.POLICIES_PATH)
    policies = by_id(policies_list, "policy_id")
    requests_by_id = by_id(requests, "request_id")
    manifests_by_id = by_id(registry, "manifest_id")
    decision_schema = replay.load_json(replay.DECISION_SCHEMA_PATH)

    errors: list[str] = []
    wrapped_events: list[dict[str, Any]] = []
    for index, case in enumerate(cases, start=1):
        source = f"{CASES_PATH}:{index}"
        errors.extend(contracts.privacy_errors(case, source))
        request, case_workers, case_registry = materialize_case(case, requests_by_id, workers, manifests_by_id)
        event = event_for_case(index, case, request, case_workers, case_registry, policies)
        errors.extend(contracts.validate_decision_event(event, decision_schema, f"prototype-event:{index}"))
        wrapped_events.append({"case_id": case["case_id"], "event": event})
    errors.extend(compare_golden(wrapped_events, golden))

    decisions: dict[str, int] = {}
    for row in wrapped_events:
        decision = row["event"]["decision"]
        decisions[decision] = decisions.get(decision, 0) + 1

    summary = {
        "ok": not errors,
        "errors": errors,
        "cases": len(cases),
        "golden_decisions": len(golden),
        "events": len(wrapped_events),
        "decisions_by_type": dict(sorted(decisions.items())),
        "fixtures": {
            "cases": CASES_PATH.relative_to(PACKAGE_ROOT).as_posix(),
            "golden": GOLDEN_PATH.relative_to(PACKAGE_ROOT).as_posix(),
        },
    }
    return [row["event"] for row in wrapped_events], summary


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write emitted decision events to this JSONL path")
    parser.add_argument("--json", action="store_true", help="print a JSON summary instead of event JSONL")
    args = parser.parse_args()

    events, summary = run_prototype()
    if not summary["ok"]:
        print("cache-router offline prototype failed:", file=sys.stderr)
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
