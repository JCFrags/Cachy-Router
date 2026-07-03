#!/usr/bin/env python3
"""Validate public Cachy Router claim wording against acceptance status.

This is a documentation guardrail, not a benchmark runner. It keeps the public
claim map explicit about claim tier, acceptance status, acceptable wording, and
file-backed evidence so README/release wording cannot silently drift into
production or distributed-cache correctness claims.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
CLAIM_MAP_PATH = PACKAGE_ROOT / "docs/benchmark-claim-map.md"

ALLOWED_TIERS = {
    "Portable behavior",
    "Exploratory live result",
    "Scoped live result",
    "Architecture plan",
    "Future work",
    "Not claimed",
}
ALLOWED_STATUS_MARKERS = {
    "done",
    "live-gated",
    "partial",
    "planned",
    "blocked-by-upstream",
    "not-claimed",
}
REQUIRED_CLAIMS = [
    "OpenAI-compatible surface",
    "Worker inventories",
    "normal non-cached streaming",
    "explicit cache extension",
    "Router-owned durable blobs",
    "sidecar can reject eviction",
    "untrusted networks",
    "Distributed cache correctness",
    "production load-balancing",
]
REQUIRED_CAVEATS = [
    "Cache restore is only safe for compatible model",
    "accelerated path is explicit",
    "No-key mode is for trusted private LANs only",
    "Raw proof artifacts should stay private",
]
FORBIDDEN_CLAIM_WORDING = [
    "production-ready",
    "internet-safe",
    "distributed cache correctness is proven",
    "proven for arbitrary models",
    "guaranteed correctness",
]
PATH_RE = re.compile(r"`(?P<path>(?:docs|scripts|schemas|configs|evidence|README|Makefile)[^` ]*)`")


def rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(PACKAGE_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def split_markdown_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def table_rows(text: str) -> tuple[list[str], list[dict[str, str]]]:
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == "## Current Public Claims":
            start = index + 1
            break
    if start is None:
        return [], []

    table_lines: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            if table_lines:
                break
            continue
        if line.lstrip().startswith("|"):
            table_lines.append(line)
            continue
        if table_lines:
            break

    if len(table_lines) < 3:
        return [], []
    headers = split_markdown_row(table_lines[0])
    rows: list[dict[str, str]] = []
    for line in table_lines[2:]:
        cells = split_markdown_row(line)
        if len(cells) != len(headers):
            rows.append({"__parse_error__": f"expected {len(headers)} cells, found {len(cells)}", "__line__": line})
            continue
        rows.append(dict(zip(headers, cells, strict=True)))
    return headers, rows


def existing_evidence_paths(evidence: str) -> list[str]:
    paths: list[str] = []
    for match in PATH_RE.finditer(evidence):
        path = match.group("path")
        if (PACKAGE_ROOT / path).exists():
            paths.append(path)
    return paths


def status_markers(status: str) -> set[str]:
    return {marker for marker in ALLOWED_STATUS_MARKERS if f"`{marker}`" in status or marker in status.split()}


def is_cache_or_performance_claim(row: dict[str, str]) -> bool:
    haystack = " ".join([row.get("claim", ""), row.get("acceptable wording", ""), row.get("tier", "")]).lower()
    return any(
        marker in haystack
        for marker in [
            "cache",
            "prefix",
            "restore",
            "hydrate",
            "durable blob",
            "prompt-processing",
            "performance",
            "ttft",
            "benchmark",
        ]
    )


def validate_claim_rows(headers: list[str], rows: list[dict[str, str]]) -> list[str]:
    errors: list[str] = []
    expected_headers = ["claim", "tier", "acceptance status", "acceptable wording", "evidence"]
    if headers != expected_headers:
        errors.append(f"{rel(CLAIM_MAP_PATH)}: Current Public Claims headers must be {expected_headers}, found {headers}")
        return errors
    if not rows:
        errors.append(f"{rel(CLAIM_MAP_PATH)}: Current Public Claims table has no rows")
        return errors

    claim_text = "\n".join(row.get("claim", "") for row in rows)
    for required in REQUIRED_CLAIMS:
        if required.lower() not in claim_text.lower():
            errors.append(f"{rel(CLAIM_MAP_PATH)}: missing required claim containing {required!r}")

    for index, row in enumerate(rows, start=1):
        if "__parse_error__" in row:
            errors.append(f"{rel(CLAIM_MAP_PATH)}: claim row {index}: {row['__parse_error__']}: {row.get('__line__', '')}")
            continue

        claim = row["claim"]
        tier = row["tier"]
        status = row["acceptance status"]
        wording = row["acceptable wording"]
        evidence = row["evidence"]
        markers = status_markers(status)
        paths = existing_evidence_paths(evidence)

        if tier not in ALLOWED_TIERS:
            errors.append(f"{rel(CLAIM_MAP_PATH)}: claim row {index} has invalid tier {tier!r}")
        if not markers:
            errors.append(f"{rel(CLAIM_MAP_PATH)}: claim row {index} lacks an allowed acceptance status marker")
        if tier == "Not claimed":
            if "not-claimed" not in markers:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: not-claimed row {index} must carry `not-claimed` status")
            if wording != "Do not claim this.":
                errors.append(f"{rel(CLAIM_MAP_PATH)}: not-claimed row {index} must use exact wording 'Do not claim this.'")
        else:
            if "not-claimed" in markers:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: claimed row {index} must not use `not-claimed` status")
            if not paths:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: claimed row {index} must cite at least one existing evidence path")
            wording_lower = wording.lower()
            for forbidden in FORBIDDEN_CLAIM_WORDING:
                if forbidden in wording_lower:
                    errors.append(f"{rel(CLAIM_MAP_PATH)}: claim row {index} uses forbidden wording {forbidden!r}")

        if is_cache_or_performance_claim(row):
            if "done" not in markers and "live-gated" not in markers and "not-claimed" not in markers:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: cache/performance row {index} needs a concrete acceptance status marker")
            if tier != "Not claimed" and len(paths) < 2:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: cache/performance row {index} must cite at least two file-backed evidence sources")
            if tier == "Exploratory live result" and "live-gated" not in markers:
                errors.append(f"{rel(CLAIM_MAP_PATH)}: exploratory live cache/performance row {index} must include `live-gated`")

        if "arbitrary models" in claim.lower() and tier != "Not claimed":
            errors.append(f"{rel(CLAIM_MAP_PATH)}: arbitrary-model correctness must remain not claimed")
        if "untrusted networks" in claim.lower() and tier != "Not claimed":
            errors.append(f"{rel(CLAIM_MAP_PATH)}: untrusted-network readiness must remain not claimed")
        if "production load-balancing" in claim.lower() and tier != "Not claimed":
            errors.append(f"{rel(CLAIM_MAP_PATH)}: production load-balancing must remain not claimed")

    return errors


def validate_claim_map(path: Path) -> tuple[dict[str, Any], list[str]]:
    if not path.is_file():
        return {}, [f"{rel(path)}: file does not exist"]
    text = path.read_text(encoding="utf-8")
    headers, rows = table_rows(text)
    errors = validate_claim_rows(headers, rows)
    for caveat in REQUIRED_CAVEATS:
        if caveat not in text:
            errors.append(f"{rel(path)}: missing required caveat containing {caveat!r}")
    summary = {
        "ok": not errors,
        "path": rel(path),
        "claim_rows": len(rows),
        "cache_or_performance_rows": sum(1 for row in rows if "__parse_error__" not in row and is_cache_or_performance_claim(row)),
        "not_claimed_rows": sum(1 for row in rows if row.get("tier") == "Not claimed"),
    }
    return summary, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claim-map", default=str(CLAIM_MAP_PATH), help="Claim map markdown file.")
    parser.add_argument("--json", action="store_true", help="Print a machine-readable summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary, errors = validate_claim_map(Path(args.claim_map))
    if args.json:
        print(json.dumps({**summary, "errors": errors}, indent=2, sort_keys=True))
    else:
        print(f"claim map: {'ok' if not errors else 'fail'}")
        if summary:
            print(f"path: {summary['path']}")
            print(f"claim_rows: {summary['claim_rows']}")
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
