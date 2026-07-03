#!/usr/bin/env python3
"""Validate the architecture document against implemented MVP guardrails.

This linter proves a narrow documentation claim: the architecture doc names the
implemented route/cache/security/compatibility surfaces, points to the offline
probes that cover them, and keeps planned-only or production-only behavior
explicitly scoped. It does not prove live worker reachability or cache
correctness.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARCH_DOC = ROOT / "docs" / "architecture" / "cache-router.md"
ENDPOINT_DOC = ROOT / "docs" / "architecture" / "cache-router-openai-endpoint.md"
ACCEPTANCE_DOC = ROOT / "docs" / "architecture" / "final-acceptance-metrics.md"
CLAIM_MAP = ROOT / "docs" / "benchmark-claim-map.md"
DAEMON = ROOT / "scripts" / "cache_router_daemon.py"
WORKER_SIDECAR = ROOT / "scripts" / "cache_router_worker_sidecar.py"
STORE_AUDIT = ROOT / "scripts" / "cache_router_store_audit.py"
MAKEFILE = ROOT / "Makefile"

IMPLEMENTED_ROUTES = [
    "`GET /health`",
    "`GET /v1`",
    "`GET /v1/models`",
    "`POST /v1/chat/completions`",
    "`POST /v1/completions`",
    "`POST /tokenize`",
    "`GET /router/cache`",
    "`GET /router/workers`",
    "`GET /router/status`",
    "`GET /router/decisions`",
    "`GET /metrics`",
]

DOC_SNIPPETS = {
    "status": [
        "trusted-LAN OpenAI-compatible router MVP now exists",
        "N-worker inventory parsing",
        "normal non-cached streaming",
        "redacted registry/store audit JSONL",
        "Operator-run endpoint-matrix, restore-correctness, suffix-benchmark, and long-soak harnesses now exist",
        "One no-MTP trusted-LAN suffix benchmark and one no-MTP trusted-LAN deterministic restore-correctness gate have scoped live passes",
        "Deployment-wide endpoint success, MTP-enabled restore correctness, restart/two-node restore, distributed-cache correctness, full 8-24 hour long-soak behavior, live production load behavior, and production security/ops behavior remain gated",
    ],
    "route": [
        "Implemented router public/admin APIs:",
        "scripts/validate_cache_router_endpoint_docs.py",
        "scripts/cache_router_daemon_smoke_test.py",
        "OpenAI-shaped error types",
        "router-owned request/trace/worker headers",
    ],
    "cache": [
        "The daemon MVP enforces hashed tenant/scope policy for cache-extension",
        "client sees the same scoped cache-miss shape used for an absent cache",
        "router-store/registry-audit.jsonl",
        "not tamper-proof/WORM audit storage or distributed log ordering",
        "content-addressed blob placement, blob SHA-256, and blob size",
        "operator-managed encrypted cache root",
        "`--require-encryption-at-rest`",
    ],
    "compatibility": [
        "Strict Cache Key",
        "fail-closed equality guard",
        "Cache-id-only restore is rejected before lookup",
        "unknown`, `not_captured`, and `not_interpreted` are invalid",
        "MTP-enabled, restart, two-node, and logit/top-k validation gates",
    ],
    "planned_only": [
        "not live worker generation, sidecar hydrate/restore, distributed-store, or deployment-network evidence",
        "Minimum tests before broad semantic or distributed restore correctness can support a public claim",
        "MVP Phases",
        "Phase 6: production hardening",
        "Stop Conditions",
    ],
}

IMPLEMENTATION_SNIPPETS = {
    DAEMON: [
        'self.path == "/health"',
        'self.path in {"/v1", "/v1/"}',
        'self.path == "/v1/models"',
        'self.path == "/v1/completions"',
        'self.path == "/v1/chat/completions"',
        'self.path == "/tokenize"',
        'self.path == "/router/status"',
        'self.path == "/router/workers"',
        'self.path == "/router/cache"',
        'urllib.parse.urlparse(self.path).path == "/router/decisions"',
        'self.path == "/metrics"',
        "def strict_cache_key_fields(",
        "def cache_key_hash_from_record(",
        "def cache_policy_denial_reason(",
        "registry_audit_path",
        "router_debug_headers",
        "def durable_blob_encryption_metadata(",
        "--durable-blob-encryption-mode",
    ],
    WORKER_SIDECAR: [
        'parsed.path == "/health"',
        'parsed.path == "/inventory"',
        '"/upload", "/hydrate"',
        'parsed.path == "/verify"',
        'parsed.path == "/evict"',
    ],
    STORE_AUDIT: [
        "def audit_store(",
        "def rebuild_registry(",
        "def tenant_delete(",
        "def gc_unreferenced_blobs(",
        "registry-audit.jsonl",
        "--require-encryption-at-rest",
    ],
}

MAKE_CHECK_SNIPPETS = [
    "scripts/validate_cache_router_contracts.py --json",
    "scripts/replay_cache_router_decisions.py --json",
    "scripts/cache_router_offline_prototype.py --json",
    "scripts/cache_router_daemon_smoke_test.py",
    "scripts/cache_router_sidecar_smoke_test.py",
    "scripts/cache_router_transport.py --self-test",
    "scripts/cache_router_store_audit.py --self-test --json",
    "scripts/cache_router_performance_probe.py --json",
    "scripts/cache_router_correctness_probe.py --self-test --json",
    "scripts/cache_router_long_soak_probe.py --self-test --json",
    "scripts/cache_router_live_endpoint_matrix.py --self-test --json",
    "scripts/cache_router_suffix_benchmark_gate.py --self-test --json",
    "scripts/validate_cache_router_setup_docs.py",
    "scripts/validate_cache_router_endpoint_docs.py",
    "scripts/validate_cache_router_claim_map.py --json",
    "scripts/validate_cache_router_architecture_doc.py",
    "scripts/validate_acceptance_metrics.py --json",
    "scripts/cache_router_release_gap_report.py --summary",
]


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def require_snippets(text: str, snippets: list[str], source: str, errors: list[str]) -> None:
    normalized_text = normalized(text)
    for snippet in snippets:
        if snippet not in text and snippet not in normalized_text:
            errors.append(f"{source}: missing snippet {snippet!r}")


def validate() -> dict[str, Any]:
    errors: list[str] = []
    arch = read(ARCH_DOC)
    makefile = read(MAKEFILE)

    for route in IMPLEMENTED_ROUTES:
        if route not in arch:
            errors.append(f"{rel(ARCH_DOC)}: missing implemented route {route}")

    for group, snippets in DOC_SNIPPETS.items():
        require_snippets(arch, snippets, f"{rel(ARCH_DOC)}[{group}]", errors)

    for path in [ENDPOINT_DOC, ACCEPTANCE_DOC, CLAIM_MAP]:
        if rel(path) not in arch:
            errors.append(f"{rel(ARCH_DOC)}: should reference {rel(path)}")

    for path, snippets in IMPLEMENTATION_SNIPPETS.items():
        text = read(path)
        for snippet in snippets:
            if snippet not in text:
                errors.append(f"{rel(path)}: implementation snippet missing {snippet!r}")

    for snippet in MAKE_CHECK_SNIPPETS:
        if snippet not in makefile:
            errors.append(f"{rel(MAKEFILE)}: make check missing {snippet}")

    forbidden_stronger_claims = [
        "distributed cache correctness is proven",
        "internet-safe",
        "tenant isolation is proven",
    ]
    arch_lower = arch.lower()
    for claim in forbidden_stronger_claims:
        if claim in arch_lower:
            errors.append(f"{rel(ARCH_DOC)}: forbidden over-claim wording {claim!r}")

    return {
        "ok": not errors,
        "errors": errors,
        "architecture_doc": rel(ARCH_DOC),
        "implemented_routes": len(IMPLEMENTED_ROUTES),
        "doc_snippet_groups": {group: len(snippets) for group, snippets in DOC_SNIPPETS.items()},
        "make_check_guards": len(MAKE_CHECK_SNIPPETS),
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
