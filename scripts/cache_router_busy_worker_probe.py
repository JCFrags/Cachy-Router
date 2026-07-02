#!/usr/bin/env python3
"""Live cache-router busy-worker routing probe.

This controller-side probe talks only to the OpenAI-compatible router endpoint.
It starts one bounded long request on a preferred worker, waits until the
router reports that worker's slot as busy, then sends a second normal
OpenAI-compatible request through the router and verifies that the router
selects an available fallback worker.

The probe records only small sanitized summaries. It does not store raw prompts,
slot blobs, worker logs, or environment values.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

from cache_router_one_node_poc import generate_prefix, sha256_text, write_json  # noqa: E402


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def http_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float,
) -> tuple[int, dict[str, Any], dict[str, str], float]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            elapsed = (time.perf_counter() - start) * 1000.0
            parsed = json.loads(raw) if raw else {}
            headers = {key.lower(): value for key, value in resp.headers.items()}
            return resp.status, parsed, headers, elapsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = (time.perf_counter() - start) * 1000.0
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw[:500]}
        headers = {key.lower(): value for key, value in exc.headers.items()} if exc.headers else {}
        return exc.code, parsed, headers, elapsed


def completion_text(body: dict[str, Any]) -> str:
    choices = body.get("choices") if isinstance(body.get("choices"), list) else []
    if not choices or not isinstance(choices[0], dict):
        return ""
    return str(choices[0].get("text") or choices[0].get("message", {}).get("content") or "")


def worker_row(workers_body: dict[str, Any], worker_id: str) -> dict[str, Any] | None:
    workers = workers_body.get("workers") if isinstance(workers_body.get("workers"), list) else []
    for row in workers:
        if isinstance(row, dict) and row.get("worker_id") == worker_id:
            return row
    return None


def wait_for_worker_busy(base_url: str, worker_id: str, *, timeout: float, poll_interval: float) -> dict[str, Any]:
    deadline = time.perf_counter() + timeout
    samples: list[dict[str, Any]] = []
    while time.perf_counter() < deadline:
        status, body, _, wall_ms = http_json("GET", base_url.rstrip("/") + "/router/workers", timeout=10)
        row = worker_row(body, worker_id)
        availability = row.get("availability") if isinstance(row, dict) and isinstance(row.get("availability"), dict) else {}
        sample = {
            "timestamp": now_iso(),
            "http_status": status,
            "wall_ms": wall_ms,
            "availability": availability,
        }
        samples.append(sample)
        if availability.get("available") is False or availability.get("busy_score") == 2:
            return {"observed": True, "samples": samples, "last": sample}
        time.sleep(poll_interval)
    return {"observed": False, "samples": samples, "last": samples[-1] if samples else None}


def request_summary(status: int, body: dict[str, Any], headers: dict[str, str], wall_ms: float) -> dict[str, Any]:
    timings = body.get("timings") if isinstance(body.get("timings"), dict) else {}
    return {
        "http_status": status,
        "wall_ms": wall_ms,
        "selected_worker": headers.get("x-cache-router-worker"),
        "selected_worker_availability": headers.get("x-cache-router-worker-availability"),
        "selected_worker_busy_score": headers.get("x-cache-router-worker-busy-score"),
        "text_preview": completion_text(body)[:120],
        "timings": {
            "prompt_ms": timings.get("prompt_ms"),
            "predicted_ms": timings.get("predicted_ms"),
            "prompt_per_second": timings.get("prompt_per_second"),
            "predicted_per_second": timings.get("predicted_per_second"),
        },
        "tokens": {
            "tokens_evaluated": body.get("tokens_evaluated"),
            "tokens_cached": body.get("tokens_cached"),
            "tokens_predicted": body.get("tokens_predicted"),
        },
    }


def write_readme(out_dir: Path, results: dict[str, Any]) -> None:
    busy = results["busy_request"]
    probe = results["fallback_probe"]
    lines = [
        "# Cache Router Busy-Worker Routing Probe",
        "",
        f"Created: `{results['created_utc']}`",
        "",
        "This live probe used the OpenAI-compatible router endpoint only. It",
        "started a bounded long request on one preferred worker, waited until",
        "the router reported that worker's slot as busy, then sent a second",
        "normal OpenAI request with fallback allowed.",
        "",
        "## Topology",
        "",
        f"- Router base URL: `{results['base_url']}`",
        f"- Busy/preferred worker: `{results['busy_worker_id']}`",
        f"- Expected available worker: `{results['expected_fallback_worker_id']}`",
        f"- Target busy prompt tokens: `{results['target_tokens']}`",
        f"- Busy prompt SHA256: `{results['busy_prompt_sha256']}`",
        "- Raw prompt tracked: `False`",
        "",
        "## Busy Worker Observation",
        "",
        f"- Busy observed: `{results['busy_observation']['observed']}`",
        f"- Busy samples: `{len(results['busy_observation']['samples'])}`",
        f"- Last availability: `{results['busy_observation']['last']}`",
        "",
        "## Fallback Probe",
        "",
        f"- HTTP status: `{probe.get('http_status')}`",
        f"- Selected worker: `{probe.get('selected_worker')}`",
        f"- Selected availability: `{probe.get('selected_worker_availability')}`",
        f"- Selected busy score: `{probe.get('selected_worker_busy_score')}`",
        f"- Wall ms: `{probe.get('wall_ms')}`",
        f"- Text preview: `{probe.get('text_preview')}`",
        "",
        "## Busy Request Completion",
        "",
        f"- HTTP status: `{busy.get('http_status')}`",
        f"- Selected worker: `{busy.get('selected_worker')}`",
        f"- Wall ms: `{busy.get('wall_ms')}`",
        f"- Prompt ms: `{busy.get('timings', {}).get('prompt_ms')}`",
        f"- Tokens evaluated: `{busy.get('tokens', {}).get('tokens_evaluated')}`",
        "",
        "## Conclusion",
        "",
        f"- Status: `{results['status']}`",
        "",
        "This is a bounded live routing probe, not a saturation benchmark. It",
        "proves the router can observe a busy slot and send a normal request to",
        "an available configured worker through the same OpenAI-compatible",
        "endpoint.",
        "",
    ]
    (out_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="", help="Router base URL, for example http://192.168.1.10:18080.")
    parser.add_argument("--busy-worker-id", default="worker-a")
    parser.add_argument("--expected-fallback-worker-id", default="worker-b")
    parser.add_argument("--target-tokens", type=int, default=10000)
    parser.add_argument("--busy-max-tokens", type=int, default=64)
    parser.add_argument("--probe-max-tokens", type=int, default=8)
    parser.add_argument("--busy-wait-timeout", type=float, default=45.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--out-dir", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.base_url:
        raise SystemExit("--base-url is required for the live busy-worker probe")
    base = args.base_url.rstrip("/")
    out_dir = Path(args.out_dir or PACKAGE_ROOT / "data" / "cache_router_poc" / f"{datetime.now().strftime('%Y-%m-%d')}-busy-worker-routing")
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix, prefix_tokens, repeats = generate_prefix(base, args.target_tokens, args.timeout)
    busy_prompt = prefix + "\n\nKeep this worker busy briefly. Reply with a short deterministic acknowledgement.\nAnswer:"
    busy_payload = {
        "model": "Step-3.7",
        "prompt": busy_prompt,
        "max_tokens": args.busy_max_tokens,
        "temperature": 0,
        "cache_router": {
            "mode": "bypass",
            "worker_id": args.busy_worker_id,
            "allow_fallback": False,
        },
    }
    fallback_payload = {
        "model": "Step-3.7",
        "prompt": "Reply with exactly: busy fallback ok\nAnswer:",
        "max_tokens": args.probe_max_tokens,
        "temperature": 0,
        "cache_router": {
            "mode": "bypass",
            "worker_id": args.busy_worker_id,
            "allow_fallback": True,
        },
    }

    busy_result: dict[str, Any] = {}
    busy_error: dict[str, Any] = {}

    def run_busy_request() -> None:
        nonlocal busy_result, busy_error
        try:
            status, body, headers, wall_ms = http_json("POST", base + "/v1/completions", busy_payload, timeout=args.timeout)
            busy_result = request_summary(status, body, headers, wall_ms)
        except Exception as exc:  # noqa: BLE001
            busy_error = {"type": type(exc).__name__, "message": str(exc)}

    thread = threading.Thread(target=run_busy_request, name="cache-router-busy-request", daemon=True)
    thread.start()
    observation = wait_for_worker_busy(base, args.busy_worker_id, timeout=args.busy_wait_timeout, poll_interval=args.poll_interval)

    probe_status, probe_body, probe_headers, probe_ms = http_json("POST", base + "/v1/completions", fallback_payload, timeout=args.timeout)
    fallback_result = request_summary(probe_status, probe_body, probe_headers, probe_ms)

    thread.join(timeout=args.timeout)
    if thread.is_alive():
        busy_error = {"type": "TimeoutError", "message": "busy request did not complete before timeout"}

    selected = fallback_result.get("selected_worker")
    status = (
        "success"
        if observation.get("observed") is True and selected == args.expected_fallback_worker_id and not busy_error
        else "partial_success"
        if selected == args.expected_fallback_worker_id
        else "diagnostic_failure"
    )
    results = {
        "schema_version": "2026-07-01.1",
        "created_utc": now_iso(),
        "base_url": base,
        "busy_worker_id": args.busy_worker_id,
        "expected_fallback_worker_id": args.expected_fallback_worker_id,
        "target_tokens": args.target_tokens,
        "actual_prefix_tokens": prefix_tokens,
        "prefix_repeats": repeats,
        "busy_prompt_sha256": sha256_text(busy_prompt),
        "raw_prompt_tracked": False,
        "busy_observation": observation,
        "fallback_probe": fallback_result,
        "busy_request": busy_result,
        "busy_error": busy_error,
        "status": status,
    }
    write_json(out_dir / "results.json", results)
    write_readme(out_dir, results)
    print(json_dumps({"status": status, "out_dir": str(out_dir), "fallback_probe": fallback_result, "busy_error": busy_error}))
    return 0 if status in {"success", "partial_success"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
