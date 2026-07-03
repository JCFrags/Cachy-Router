#!/usr/bin/env python3
"""Validate the final Cachy Router acceptance-metrics document.

The acceptance document is intentionally a project contract, not a runtime
test suite. This checker keeps the contract structured enough that future work
can promote metrics from planned/partial/live-gated to done without losing
evidence links or silently dropping whole sections.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOC = PACKAGE_ROOT / "docs/architecture/final-acceptance-metrics.md"
README_PATH = PACKAGE_ROOT / "README.md"

REQUIRED_GROUPS = [
    "Core API",
    "Inventory",
    "Readiness",
    "Scheduling",
    "Cache Build/Use",
    "Strict Compatibility",
    "Correctness",
    "Sidecar",
    "Registry/Store",
    "Security",
    "Observability",
    "Performance",
    "Packaging",
    "Docs And Evidence",
]
EXPECTED_GROUP_COUNTS = {
    "Core API": 10,
    "Inventory": 10,
    "Readiness": 10,
    "Scheduling": 12,
    "Cache Build/Use": 14,
    "Strict Compatibility": 18,
    "Correctness": 14,
    "Sidecar": 13,
    "Registry/Store": 11,
    "Security": 12,
    "Observability": 11,
    "Performance": 10,
    "Packaging": 13,
    "Docs And Evidence": 10,
}
EXPECTED_TOTAL_METRICS = sum(EXPECTED_GROUP_COUNTS.values())

VALID_STATUSES = {
    "done",
    "live-gated",
    "partial",
    "planned",
    "blocked-by-upstream",
}

HEADING_RE = re.compile(r"^## (?P<name>.+?)\s*$")
METRIC_RE = re.compile(r"^- \[(?P<status>[a-z-]+)\] (?P<metric>.+?) -- evidence: (?P<evidence>.+?)\s*$")
README_SNAPSHOT_RE = re.compile(
    r"`(?P<total>\d+)`\s+total metrics:\s+"
    r"`(?P<done>\d+) done`,\s+"
    r"`(?P<live_gated>\d+) live-gated`,\s+"
    r"`(?P<partial>\d+) partial`,\s+"
    r"`(?P<planned>\d+) planned`,\s+"
    r"`(?P<blocked>\d+) blocked-by-upstream`"
)


def rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(PACKAGE_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def validate_readme_status(summary: dict[str, Any], errors: list[str]) -> None:
    if not README_PATH.is_file():
        errors.append(f"{rel(README_PATH)}: file does not exist")
        return
    text = README_PATH.read_text(encoding="utf-8")
    normalized_text = re.sub(r"\s+", " ", text)
    match = README_SNAPSHOT_RE.search(normalized_text)
    if not match:
        errors.append(f"{rel(README_PATH)}: missing acceptance snapshot status counts")
        return

    expected = {
        "total": int(summary["total_metrics"]),
        "done": int(summary["status_counts"]["done"]),
        "live_gated": int(summary["status_counts"]["live-gated"]),
        "partial": int(summary["status_counts"]["partial"]),
        "planned": int(summary["status_counts"]["planned"]),
        "blocked": int(summary["status_counts"]["blocked-by-upstream"]),
    }
    actual = {key: int(value) for key, value in match.groupdict().items()}
    if actual != expected:
        errors.append(f"{rel(README_PATH)}: acceptance snapshot mismatch: expected {expected}, found {actual}")

    required_snippets = [
        "deployment-wide `/v1/models`, `/v1/completions`, and `/v1/chat/completions` success remains `live-gated`",
        "`cache_router.mode=bypass` may stream after the extension is stripped, while cached build/use modes are rejected clearly when `stream=true`",
        "One no-MTP trusted-LAN suffix benchmark gate is now `done` for prompt-token reduction and true TTFT",
    ]
    for snippet in required_snippets:
        if snippet not in normalized_text:
            errors.append(f"{rel(README_PATH)}: missing status caveat: {snippet}")


def validate(path: Path, *, strict: bool) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    current_group: str | None = None
    groups: dict[str, list[dict[str, str]]] = {group: [] for group in REQUIRED_GROUPS}
    status_counts = {status: 0 for status in sorted(VALID_STATUSES)}

    if not path.is_file():
        return {}, [f"{rel(path)}: file does not exist"]

    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        heading = HEADING_RE.match(line)
        if heading:
            name = heading.group("name")
            current_group = name if name in groups else None
            continue

        metric = METRIC_RE.match(line)
        if not metric:
            continue
        if current_group is None:
            errors.append(f"{rel(path)}:{lineno}: metric row is outside a required group")
            continue

        status = metric.group("status")
        evidence = metric.group("evidence").strip()
        if status not in VALID_STATUSES:
            errors.append(f"{rel(path)}:{lineno}: invalid status {status!r}")
        else:
            status_counts[status] += 1
        if not evidence:
            errors.append(f"{rel(path)}:{lineno}: evidence must not be empty")
        if status == "done" and any(marker in evidence.lower() for marker in ["tbd", "missing", "planned", "future"]):
            errors.append(f"{rel(path)}:{lineno}: done metric has non-current evidence: {evidence}")
        groups[current_group].append(
            {
                "status": status,
                "metric": metric.group("metric").strip(),
                "evidence": evidence,
            }
        )

    for group, rows in groups.items():
        if not rows:
            errors.append(f"{rel(path)}: required group has no metric rows: {group}")
        expected_count = EXPECTED_GROUP_COUNTS[group]
        if len(rows) != expected_count:
            errors.append(f"{rel(path)}: group {group!r} expected {expected_count} metric rows, found {len(rows)}")

    total = sum(len(rows) for rows in groups.values())
    if total != EXPECTED_TOTAL_METRICS:
        errors.append(f"{rel(path)}: expected exactly {EXPECTED_TOTAL_METRICS} acceptance metrics, found {total}")

    if strict:
        unfinished = total - status_counts.get("done", 0)
        if unfinished:
            errors.append(f"{rel(path)}: strict mode requires all metrics done; unfinished={unfinished}")

    summary = {
        "ok": not errors,
        "path": rel(path),
        "groups": {group: len(rows) for group, rows in groups.items()},
        "status_counts": status_counts,
        "total_metrics": total,
        "strict": strict,
    }
    validate_readme_status(summary, errors)
    summary["ok"] = not errors
    return summary, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--doc", default=str(DEFAULT_DOC), help="Acceptance metrics markdown file.")
    parser.add_argument("--strict", action="store_true", help="Require every metric to be marked done.")
    parser.add_argument("--json", action="store_true", help="Print a machine-readable summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary, errors = validate(Path(args.doc), strict=args.strict)
    if args.json:
        print(json.dumps({**summary, "errors": errors}, indent=2, sort_keys=True))
    else:
        print(f"acceptance metrics: {'ok' if not errors else 'fail'}")
        if summary:
            print(f"path: {summary['path']}")
            print(f"total_metrics: {summary['total_metrics']}")
            print("statuses:")
            for status, count in summary["status_counts"].items():
                print(f"- {status}: {count}")
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
