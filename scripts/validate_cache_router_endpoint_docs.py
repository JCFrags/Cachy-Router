#!/usr/bin/env python3
"""Validate the public endpoint document against daemon route contracts.

This is intentionally a small mechanical linter. It proves that the endpoint
doc names the route surface and key operator-visible behaviors that the daemon
and smoke tests exercise offline.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_DOC = ROOT / "docs" / "architecture" / "cache-router-openai-endpoint.md"
DAEMON = ROOT / "scripts" / "cache_router_daemon.py"

REQUIRED_ENDPOINT_LINES = {
    "- `GET /health`",
    "- `GET /v1`",
    "- `GET /v1/models`",
    "- `POST /v1/completions`",
    "- `POST /v1/chat/completions`",
    "- `POST /tokenize`",
    "- `GET /router/status`",
    "- `GET /router/workers`",
    "- `GET /router/cache`",
    "- `GET /router/decisions`",
    "- `GET /metrics`",
}

REQUIRED_DAEMON_ROUTE_SNIPPETS = {
    'self.path == "/health"',
    'self.path in {"/v1", "/v1/"}',
    'self.path == "/v1/models"',
    'self.path == "/router/status"',
    'self.path == "/router/workers"',
    'self.path == "/router/cache"',
    'urllib.parse.urlparse(self.path).path == "/router/decisions"',
    'self.path == "/metrics"',
    'self.path == "/v1/completions"',
    'self.path == "/v1/chat/completions"',
    'self.path == "/tokenize"',
}

REQUIRED_ERROR_TYPES = {
    "authentication_error",
    "invalid_request_error",
    "model_not_found",
    "worker_not_found",
    "service_unavailable",
    "cache_not_found",
    "invalid_json",
    "not_found",
    "cache_router_error",
}

REQUIRED_BEHAVIOR_SNIPPETS = {
    "--disable-admin-endpoints",
    "bearer token",
    "`X-API-Key`",
    "X-Cache-Router-Request-ID",
    "X-Cache-Router-Trace-ID",
    "X-Cache-Router-Worker",
    "Router-generated responses that do not select a worker",
    "Client-supplied `X-Cache-Router-*` headers are ignored",
    "`X-Cache-Router-Worker: none`",
    "`?request_id=<id>`",
    "OpenAI-shaped error",
}


def documented_endpoint_lines(text: str) -> set[str]:
    return {line.strip() for line in text.splitlines() if re.match(r"^- `(GET|POST) /", line.strip())}


def validate() -> dict[str, Any]:
    doc = ENDPOINT_DOC.read_text(encoding="utf-8")
    daemon = DAEMON.read_text(encoding="utf-8")
    errors: list[str] = []

    documented = documented_endpoint_lines(doc)
    missing_doc_routes = sorted(REQUIRED_ENDPOINT_LINES - documented)
    if missing_doc_routes:
        errors.append("endpoint doc missing route bullets: " + ", ".join(missing_doc_routes))

    missing_daemon_routes = sorted(snippet for snippet in REQUIRED_DAEMON_ROUTE_SNIPPETS if snippet not in daemon)
    if missing_daemon_routes:
        errors.append("daemon route snippets missing: " + ", ".join(missing_daemon_routes))

    missing_error_types = sorted(error_type for error_type in REQUIRED_ERROR_TYPES if f"`{error_type}`" not in doc)
    if missing_error_types:
        errors.append("endpoint doc missing error types: " + ", ".join(missing_error_types))

    missing_behaviors = sorted(snippet for snippet in REQUIRED_BEHAVIOR_SNIPPETS if snippet not in doc)
    if missing_behaviors:
        errors.append("endpoint doc missing behavior snippets: " + ", ".join(missing_behaviors))

    if "endpoints.extend([\"/router/status\", \"/router/workers\", \"/router/cache\", \"/router/decisions\", \"/metrics\"])" not in daemon:
        errors.append("daemon /v1 route advertisement no longer contains the expected admin endpoint list")
    if "not getattr(self.state.args, \"disable_admin_endpoints\", False)" not in daemon:
        errors.append("daemon /v1 route advertisement is not gated by disable_admin_endpoints")

    return {
        "ok": not errors,
        "errors": errors,
        "endpoint_doc": str(ENDPOINT_DOC.relative_to(ROOT)),
        "daemon": str(DAEMON.relative_to(ROOT)),
        "documented_endpoint_count": len(documented),
        "required_endpoint_count": len(REQUIRED_ENDPOINT_LINES),
        "required_error_types": sorted(REQUIRED_ERROR_TYPES),
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
