#!/usr/bin/env python3
"""Report the remaining Cachy Router release-proof gaps.

The acceptance matrix is allowed to contain unfinished rows while the project
is still pre-release. This helper makes those rows auditable: each unfinished
metric must map to a bounded proof bucket so live-only or upstream-gated work
does not disappear into prose.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PACKAGE_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from validate_acceptance_metrics import DEFAULT_DOC, HEADING_RE, METRIC_RE, REQUIRED_GROUPS, VALID_STATUSES, rel  # noqa: E402


UNFINISHED_STATUSES = {"live-gated", "partial", "planned", "blocked-by-upstream"}
SCRIPT_RE = re.compile(r"`?(scripts/[A-Za-z0-9_./-]+\.py)`?")

PROOF_BUCKETS: dict[str, dict[str, Any]] = {
    "live_endpoint_matrix": {
        "description": "operator-supplied deployment-wide OpenAI endpoint matrix",
        "command": "python3 scripts/cache_router_live_endpoint_matrix.py --router-url http://<router-lan-ip>:18080 --workers-file configs/cache-router/<deployment>.workers.json --json",
    },
    "suffix_benchmark_true_timing": {
        "description": "operator-run suffix benchmark plus true first-token timing support",
        "command": "python3 scripts/cache_router_suffix_benchmark_gate.py --router-url http://<router-lan-ip>:18080 --model <model-name> --runs 10 --max-tokens 1 --json",
    },
    "upstream_runtime_validation": {
        "description": "runtime must expose logits/top-k or equivalent validation before this can be proven",
        "command": None,
    },
    "restore_correctness_live": {
        "description": "operator-run restore correctness probe with redacted validation results",
        "command": "python3 scripts/cache_router_correctness_probe.py --router-url http://<router-lan-ip>:18080 --model <model-name> --runs 10 --json",
    },
    "restart_restore_live": {
        "description": "operator-run restart restore proof on a live worker",
        "command": "python3 scripts/cache_router_one_node_poc.py --base-url http://<worker-lan-ip>:8080 --out-dir runtime/cache-router-one-node/<public-safe-run-id>",
    },
    "one_node_live_poc": {
        "description": "operator-run one-worker live cache build/use proof",
        "command": "python3 scripts/cache_router_one_node_poc.py --base-url http://<worker-lan-ip>:8080 --out-dir runtime/cache-router-one-node/<public-safe-run-id>",
    },
    "two_node_restore_live": {
        "description": "operator-run two-worker restore/hydration proof",
        "command": "python3 scripts/cache_router_two_node_live_test.py --base-url http://<router-lan-ip>:18080 --source-worker-id worker-a --target-worker-id worker-b",
    },
    "busy_worker_live": {
        "description": "operator-run busy-worker routing proof",
        "command": "python3 scripts/cache_router_busy_worker_probe.py --base-url http://<router-lan-ip>:18080 --busy-worker-id worker-a --expected-fallback-worker-id worker-b",
    },
    "store_hydration_live": {
        "description": "operator-run durable store hydration proof",
        "command": "python3 scripts/cache_router_store_hydration_poc.py --remote-host <worker-ssh-host> --llama-server <path-to-llama-server> --model <path-to-model.gguf> --out-dir runtime/cache-router-store-hydration/<public-safe-run-id>",
    },
    "long_soak_duration": {
        "description": "full-duration 8-24 hour soak result, not the short self-test",
        "command": "python3 scripts/cache_router_long_soak_probe.py --duration-seconds 28800 --baseline-after-seconds 3600 --json",
    },
    "clean_checkout_release": {
        "description": "committed clean checkout must run make check from a detached worktree",
        "command": "make check-clean-checkout",
    },
}


def parse_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_group: str | None = None
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        heading = HEADING_RE.match(line)
        if heading:
            name = heading.group("name")
            current_group = name if name in REQUIRED_GROUPS else None
            continue
        metric = METRIC_RE.match(line)
        if not metric:
            continue
        rows.append(
            {
                "line": lineno,
                "group": current_group,
                "status": metric.group("status"),
                "metric": metric.group("metric").strip(),
                "evidence": metric.group("evidence").strip(),
            }
        )
    return rows


def classify_gap(row: dict[str, Any]) -> str | None:
    status = row["status"]
    group = row["group"]
    metric = row["metric"].lower()
    evidence = row["evidence"].lower()
    combined = f"{metric} {evidence}"

    if status not in UNFINISHED_STATUSES:
        return None
    if status == "blocked-by-upstream":
        return "upstream_runtime_validation"
    if "cache_router_live_endpoint_matrix.py" in combined:
        return "live_endpoint_matrix"
    if "cache_router_suffix_benchmark_gate.py" in combined or "true-ttft" in combined or "true ttft" in combined:
        return "suffix_benchmark_true_timing"
    if "cache_router_correctness_probe.py" in combined:
        return "restore_correctness_live"
    if "restart restore" in combined:
        return "restart_restore_live"
    if "cache_router_one_node_poc.py" in combined:
        return "one_node_live_poc"
    if "cache_router_two_node_live_test.py" in combined or "two-node restore" in combined:
        return "two_node_restore_live"
    if "cache_router_busy_worker_probe.py" in combined:
        return "busy_worker_live"
    if "cache_router_store_hydration_poc.py" in combined:
        return "store_hydration_live"
    if "cache_router_long_soak_probe.py" in combined or "8-24 hour soak" in combined:
        return "long_soak_duration"
    if "make check" in metric and "clean checkout" in metric:
        return "clean_checkout_release"
    return None


def validate_gap(row: dict[str, Any], bucket: str | None) -> list[str]:
    errors: list[str] = []
    status = row["status"]
    evidence = row["evidence"].lower()
    location = f"{row['path']}:{row['line']}"

    if status not in VALID_STATUSES:
        errors.append(f"{location}: invalid status {status!r}")
    if status in UNFINISHED_STATUSES and bucket is None:
        errors.append(f"{location}: unfinished row is not mapped to a release proof bucket")
    if status == "planned":
        errors.append(f"{location}: planned rows are not allowed in the release gap report")
    if status == "blocked-by-upstream" and not any(term in evidence for term in ["requires", "not present", "support"]):
        errors.append(f"{location}: blocked-by-upstream row must state the missing runtime support")
    if status == "live-gated" and not any(term in evidence for term in ["operator", "deployment", "live", "runtime", "worker"]):
        errors.append(f"{location}: live-gated row must state the live/deployment evidence requirement")
    if status == "partial" and not any(term in evidence for term in ["required", "until", "remains"]):
        errors.append(f"{location}: partial row must state the missing final proof")
    return errors


def build_report(path: Path) -> tuple[dict[str, Any], list[str]]:
    rows = parse_rows(path)
    errors: list[str] = []
    unfinished: list[dict[str, Any]] = []
    by_status = {status: 0 for status in sorted(VALID_STATUSES)}
    by_group: dict[str, dict[str, int]] = {}
    by_bucket: dict[str, dict[str, Any]] = {}

    for row in rows:
        row["path"] = rel(path)
        status = row["status"]
        by_status[status] = by_status.get(status, 0) + 1
        group_counts = by_group.setdefault(str(row["group"]), {})
        group_counts[status] = group_counts.get(status, 0) + 1
        if status not in UNFINISHED_STATUSES:
            continue

        bucket = classify_gap(row)
        row_errors = validate_gap(row, bucket)
        errors.extend(row_errors)
        scripts = sorted(set(SCRIPT_RE.findall(row["evidence"])))
        public_row = {
            "line": row["line"],
            "group": row["group"],
            "status": status,
            "metric": row["metric"],
            "bucket": bucket,
            "scripts": scripts,
        }
        unfinished.append(public_row)
        if bucket:
            bucket_row = by_bucket.setdefault(
                bucket,
                {
                    "description": PROOF_BUCKETS[bucket]["description"],
                    "command": PROOF_BUCKETS[bucket]["command"],
                    "count": 0,
                    "rows": [],
                },
            )
            bucket_row["count"] += 1
            bucket_row["rows"].append(
                {
                    "line": row["line"],
                    "group": row["group"],
                    "status": status,
                    "metric": row["metric"],
                    "scripts": scripts,
                }
            )

    report = {
        "ok": not errors,
        "path": rel(path),
        "total_metrics": len(rows),
        "done_metrics": by_status.get("done", 0),
        "unfinished_metrics": len(unfinished),
        "status_counts": by_status,
        "group_status_counts": by_group,
        "proof_buckets": by_bucket,
        "unfinished_rows": unfinished,
    }
    return report, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", default=str(DEFAULT_DOC), help="Acceptance metrics markdown file.")
    parser.add_argument("--json", action="store_true", help="Print the full report as JSON.")
    parser.add_argument("--summary", action="store_true", help="Print only status and proof-bucket counts.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report, errors = build_report(Path(args.doc))
    if args.json:
        print(json.dumps({**report, "errors": errors}, indent=2, sort_keys=True))
    elif args.summary:
        summary = {
            "ok": report["ok"],
            "total_metrics": report["total_metrics"],
            "done_metrics": report["done_metrics"],
            "unfinished_metrics": report["unfinished_metrics"],
            "status_counts": report["status_counts"],
            "proof_bucket_counts": {
                bucket: details["count"] for bucket, details in sorted(report["proof_buckets"].items())
            },
            "errors": errors,
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"release gaps: {'ok' if not errors else 'fail'}")
        print(f"path: {report['path']}")
        print(f"done_metrics: {report['done_metrics']}")
        print(f"unfinished_metrics: {report['unfinished_metrics']}")
        for bucket, details in sorted(report["proof_buckets"].items()):
            print(f"- {bucket}: {details['count']} rows")
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
