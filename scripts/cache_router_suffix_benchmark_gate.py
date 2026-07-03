#!/usr/bin/env python3
"""Operator-run suffix-route prompt-token and first-token-proxy benchmark gate.

Live mode contacts only an operator-supplied Cachy Router URL. It builds one
explicit prefix cache, then repeats cold full-prompt and restored suffix-route
requests. The gate enforces prompt-eval token reduction and a non-streaming
one-token first-token proxy. It reports true TTFT only when a deployment exposes
real first-token timing; it does not relabel total response latency as TTFT.

The self-test is fully offline and validates the benchmark math and failure
classification using synthetic rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

ACCEPTANCE_MIN_RUNS = 10
RESTORED_CACHE_HIT_LEVELS = {"local_nvme", "durable_blob"}
REDACTED_ROW_FIELDS = [
    "run",
    "cold_http_status",
    "restored_http_status",
    "cold_prompt_tokens",
    "restored_prompt_tokens",
    "restored_cached_tokens",
    "restored_prompt_basis",
    "cold_first_token_proxy_ms",
    "restored_first_token_proxy_ms",
    "restored_generation_wall_ms",
    "cold_true_ttft_ms",
    "restored_true_ttft_ms",
    "measurement_basis",
    "cold_stream_http_status",
    "cache_hit_level",
    "fallback_used",
    "full_reprocess_suspected",
    "restore_error",
]


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_id_component(text: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in ".:-" else "-" for char in text)
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] or "id"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def percentile(samples: list[float], pct: float) -> float | None:
    values = sorted(float(sample) for sample in samples if isinstance(sample, (int, float)) and not isinstance(sample, bool))
    if not values:
        return None
    index = max(0, min(len(values) - 1, math.ceil((pct / 100.0) * len(values)) - 1))
    return values[index]


def stats(samples: list[float]) -> dict[str, float | None]:
    values = [float(sample) for sample in samples if isinstance(sample, (int, float)) and not isinstance(sample, bool)]
    return {
        "count": len(values),
        "median_ms": percentile(values, 50),
        "p95_ms": percentile(values, 95),
        "min_ms": min(values) if values else None,
        "max_ms": max(values) if values else None,
    }


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any], float]:
    data = None
    req_headers = {"Accept-Encoding": "identity", **headers}
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
        status = int(exc.code)
    except urllib.error.URLError as exc:
        return 0, {}, {"error": {"type": "transport_error", "message": str(exc)}}, (time.perf_counter() - started) * 1000.0
    try:
        decoded = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception as exc:  # noqa: BLE001
        decoded = {"error": {"type": "invalid_json", "message": f"{type(exc).__name__}: {exc}"}}
    return status, response_headers, decoded if isinstance(decoded, dict) else {"value": decoded}, (time.perf_counter() - started) * 1000.0


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    if args.x_api_key:
        headers["X-API-Key"] = args.x_api_key
    return headers


def generate_prefix(repeat: int) -> str:
    line = "Cachy Router public synthetic benchmark prefix line with stable reusable context.\n"
    return line * max(1, repeat)


def cold_body(
    model: str,
    prefix_text: str,
    suffix_text: str,
    *,
    max_tokens: int,
    stream: bool = False,
    worker_id: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "prompt": prefix_text + suffix_text,
        "max_tokens": max_tokens,
        "temperature": 0,
        "stream": stream,
    }
    if worker_id:
        body["cache_router"] = {
            "mode": "bypass",
            "worker_id": worker_id,
            "allow_fallback": False,
        }
    return body


def build_body(model: str, prefix_text: str, cache_id: str, worker_id: str | None) -> dict[str, Any]:
    extension: dict[str, Any] = {
        "mode": "refresh",
        "cache_id": cache_id,
        "prefix_text": prefix_text,
        "suffix_text": "",
        "target": "suffix_route",
        "allow_fallback": False,
    }
    if worker_id:
        extension["worker_id"] = worker_id
    return {
        "model": model,
        "prompt": "",
        "max_tokens": 1,
        "temperature": 0,
        "stream": False,
        "cache_router": extension,
    }


def restored_body(
    model: str,
    prefix_text: str,
    suffix_text: str,
    cache_id: str,
    cache_key_hash: str | None,
    worker_id: str | None,
    *,
    max_tokens: int,
    measure_true_ttft: bool = False,
    restore_generation_strategy: str = "suffix_only_after_slot_restore",
) -> dict[str, Any]:
    extension: dict[str, Any] = {
        "mode": "use",
        "cache_id": cache_id,
        "prefix_text": prefix_text,
        "suffix_text": suffix_text,
        "target": "suffix_route",
        "allow_fallback": False,
        "restore_generation_strategy": restore_generation_strategy,
    }
    if measure_true_ttft:
        extension["measure_true_ttft"] = True
    if cache_key_hash:
        extension["cache_key_hash"] = cache_key_hash
    if worker_id:
        extension["worker_id"] = worker_id
    return {
        "model": model,
        "prompt": suffix_text,
        "max_tokens": max_tokens,
        "temperature": 0,
        "stream": False,
        "cache_router": extension,
    }


def usage_prompt_tokens(body: dict[str, Any]) -> int | None:
    usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
    value = usage.get("prompt_tokens")
    return int(value) if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def nested_use(body: dict[str, Any]) -> dict[str, Any]:
    cache_router = body.get("cache_router") if isinstance(body.get("cache_router"), dict) else {}
    use = cache_router.get("use") if isinstance(cache_router.get("use"), dict) else {}
    return use


def restored_prompt_tokens(body: dict[str, Any]) -> int | None:
    use = nested_use(body)
    completion = use.get("completion") if isinstance(use.get("completion"), dict) else {}
    value = completion.get("tokens_evaluated")
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return usage_prompt_tokens(body)


def restored_cached_tokens(body: dict[str, Any]) -> int | None:
    completion = nested_use(body).get("completion")
    if isinstance(completion, dict):
        value = completion.get("tokens_cached")
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return None


def restored_wall_ms(body: dict[str, Any]) -> float | None:
    completion = nested_use(body).get("completion")
    if isinstance(completion, dict):
        value = completion.get("wall_ms")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    return None


def classify_full_reprocess(cached_tokens: Any, processed_prompt_tokens: Any, *, cold_prompt_tokens: Any = None) -> str:
    try:
        processed = int(processed_prompt_tokens)
    except (TypeError, ValueError):
        return "not_captured"
    if cold_prompt_tokens is not None:
        reduction = reduction_percent(cold_prompt_tokens, processed)
        if isinstance(reduction, (int, float)) and reduction >= 90.0:
            return "no"
    try:
        cached = int(cached_tokens)
    except (TypeError, ValueError):
        return "not_captured"
    if cached <= 0:
        return "yes"
    if processed >= cached:
        return "yes"
    return "no"


def row_full_reprocess_status(row: dict[str, Any]) -> str:
    value = row.get("full_reprocess_suspected")
    if value in {"yes", "no", "not_captured", "not_interpreted"}:
        return str(value)
    return classify_full_reprocess(
        row.get("restored_cached_tokens"),
        row.get("restored_prompt_tokens"),
        cold_prompt_tokens=row.get("cold_prompt_tokens"),
    )


def extract_true_ttft_ms(body: dict[str, Any]) -> float | None:
    use = nested_use(body)
    completion = use.get("completion") if isinstance(use.get("completion"), dict) else {}
    for key in ["true_ttft_ms", "time_to_first_token_ms"]:
        value = completion.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    timings = completion.get("timings") if isinstance(completion.get("timings"), dict) else {}
    for key in ["true_ttft_ms", "time_to_first_token_ms", "ttft_ms", "first_token_ms"]:
        value = timings.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    cache_router = body.get("cache_router") if isinstance(body.get("cache_router"), dict) else {}
    for key in ["ttft_ms", "time_to_first_token_ms"]:
        value = cache_router.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    return None


def request_stream_ttft(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any],
    timeout: float,
) -> tuple[int, float | None, float, dict[str, str]]:
    data = json.dumps(body, sort_keys=True).encode("utf-8")
    req_headers = {"Accept-Encoding": "identity", "Content-Type": "application/json", **headers}
    request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    started = time.perf_counter()
    ttft_ms: float | None = None
    response_headers: dict[str, str] = {}
    status = 0
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            status = int(response.status)
            while True:
                line = response.readline()
                if not line:
                    break
                stripped = line.strip()
                if not stripped or stripped.startswith(b":"):
                    continue
                if ttft_ms is None:
                    ttft_ms = (time.perf_counter() - started) * 1000.0
                break
            response.read()
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
        exc.read()
    except urllib.error.URLError:
        status = 0
    return status, ttft_ms, (time.perf_counter() - started) * 1000.0, response_headers


def run_one_live(args: argparse.Namespace, router_url: str, headers: dict[str, str], prefix: str, suffix: str, cache_id: str, cache_key_hash: str | None, run_index: int) -> dict[str, Any]:
    cold_status, cold_headers, cold_response, cold_elapsed = request_json(
        "POST",
        f"{router_url}/v1/completions",
        headers=headers,
        body=cold_body(args.model, prefix, suffix, max_tokens=args.max_tokens, worker_id=args.worker_id),
        timeout=args.timeout,
    )
    restored_status, restored_headers, restored_response, restored_elapsed = request_json(
        "POST",
        f"{router_url}/v1/completions",
        headers=headers,
        body=restored_body(
            args.model,
            prefix,
            suffix,
            cache_id,
            cache_key_hash,
            args.worker_id,
            max_tokens=args.max_tokens,
            measure_true_ttft=args.true_ttft_probe,
            restore_generation_strategy=args.restore_generation_strategy,
        ),
        timeout=args.timeout,
    )
    cold_stream_status = None
    cold_stream_ttft_ms = None
    if args.true_ttft_probe:
        cold_stream_status, cold_stream_ttft_ms, _, _ = request_stream_ttft(
            "POST",
            f"{router_url}/v1/completions",
            headers=headers,
            body=cold_body(args.model, prefix, suffix, max_tokens=1, stream=True, worker_id=args.worker_id),
            timeout=args.timeout,
        )
    hit_level = restored_headers.get("x-cache-router-cache-hit-level", "")
    use = nested_use(restored_response)
    attempts = use.get("attempts") if isinstance(use.get("attempts"), list) else []
    fallback_used = use.get("fallback_used") is True
    restored_tokens = restored_prompt_tokens(restored_response)
    cached_tokens = restored_cached_tokens(restored_response)
    cold_tokens = usage_prompt_tokens(cold_response)
    completion = use.get("completion") if isinstance(use.get("completion"), dict) else {}
    return {
        "run": run_index,
        "cold_http_status": cold_status,
        "restored_http_status": restored_status,
        "cold_prompt_tokens": cold_tokens,
        "restored_prompt_tokens": restored_tokens,
        "restored_cached_tokens": cached_tokens,
        "restored_prompt_basis": completion.get("prompt_basis"),
        "cold_first_token_proxy_ms": cold_elapsed,
        "restored_first_token_proxy_ms": restored_elapsed,
        "restored_generation_wall_ms": restored_wall_ms(restored_response),
        "cold_true_ttft_ms": cold_stream_ttft_ms if cold_stream_ttft_ms is not None else extract_true_ttft_ms(cold_response),
        "restored_true_ttft_ms": extract_true_ttft_ms(restored_response),
        "measurement_basis": "router_stream_ttft_probe" if args.true_ttft_probe else ("non_stream_single_token_wall" if args.max_tokens == 1 else "non_stream_response_wall"),
        "cold_stream_http_status": cold_stream_status,
        "cache_hit_level": hit_level,
        "fallback_used": fallback_used,
        "full_reprocess_suspected": classify_full_reprocess(cached_tokens, restored_tokens, cold_prompt_tokens=cold_tokens),
        "restore_error": restored_status != 200
        or hit_level not in RESTORED_CACHE_HIT_LEVELS
        or fallback_used
        or any(isinstance(row, dict) and row.get("status") == "failed" for row in attempts),
        "request_ids": {
            "cold": cold_headers.get("x-cache-router-request-id", ""),
            "restored": restored_headers.get("x-cache-router-request-id", ""),
        },
    }


def reduction_percent(cold_tokens: int | None, restored_tokens: int | None) -> float | None:
    if not isinstance(cold_tokens, int) or cold_tokens <= 0 or not isinstance(restored_tokens, int):
        return None
    return 100.0 * (1.0 - (restored_tokens / cold_tokens))


def summarize_runs(
    rows: list[dict[str, Any]],
    *,
    min_runs: int,
    prompt_reduction_threshold_percent: float,
    median_ttft_improvement_threshold_percent: float,
    require_restored_p95_lower: bool,
) -> dict[str, Any]:
    prompt_reductions = [reduction_percent(row.get("cold_prompt_tokens"), row.get("restored_prompt_tokens")) for row in rows]
    prompt_reductions = [float(value) for value in prompt_reductions if isinstance(value, (int, float))]
    cold_proxy = [float(row["cold_first_token_proxy_ms"]) for row in rows if isinstance(row.get("cold_first_token_proxy_ms"), (int, float))]
    restored_proxy = [
        float(row["restored_first_token_proxy_ms"]) for row in rows if isinstance(row.get("restored_first_token_proxy_ms"), (int, float))
    ]
    cold_true_ttft = [float(row["cold_true_ttft_ms"]) for row in rows if isinstance(row.get("cold_true_ttft_ms"), (int, float))]
    restored_true_ttft = [float(row["restored_true_ttft_ms"]) for row in rows if isinstance(row.get("restored_true_ttft_ms"), (int, float))]
    restore_errors = sum(1 for row in rows if row.get("restore_error"))
    fallback_count = sum(1 for row in rows if row.get("fallback_used"))
    missing_cache_hit_levels = sum(1 for row in rows if not row.get("cache_hit_level"))
    invalid_cache_hit_levels = sum(
        1 for row in rows if row.get("cache_hit_level") and row.get("cache_hit_level") not in RESTORED_CACHE_HIT_LEVELS
    )
    full_reprocess_suspected_count = sum(1 for row in rows if row_full_reprocess_status(row) == "yes")
    prompt_reduction_min = min(prompt_reductions) if prompt_reductions else None
    prompt_reduction_median = percentile(prompt_reductions, 50)
    cold_proxy_stats = stats(cold_proxy)
    restored_proxy_stats = stats(restored_proxy)
    cold_true_ttft_stats = stats(cold_true_ttft)
    restored_true_ttft_stats = stats(restored_true_ttft)
    proxy_available = len(cold_proxy) >= min_runs and len(restored_proxy) >= min_runs
    true_ttft_available = len(cold_true_ttft) >= min_runs and len(restored_true_ttft) >= min_runs
    proxy_median_improvement = None
    if cold_proxy_stats["median_ms"] and restored_proxy_stats["median_ms"] is not None:
        proxy_median_improvement = 100.0 * (1.0 - (float(restored_proxy_stats["median_ms"]) / float(cold_proxy_stats["median_ms"])))
    true_ttft_median_improvement = None
    if cold_true_ttft_stats["median_ms"] and restored_true_ttft_stats["median_ms"] is not None:
        true_ttft_median_improvement = 100.0 * (
            1.0 - (float(restored_true_ttft_stats["median_ms"]) / float(cold_true_ttft_stats["median_ms"]))
        )
    proxy_p95_ok = False
    if cold_proxy_stats["p95_ms"] is not None and restored_proxy_stats["p95_ms"] is not None:
        proxy_p95_ok = (
            float(restored_proxy_stats["p95_ms"]) < float(cold_proxy_stats["p95_ms"])
            if require_restored_p95_lower
            else float(restored_proxy_stats["p95_ms"]) <= float(cold_proxy_stats["p95_ms"])
        )
    true_ttft_p95_ok = False
    if cold_true_ttft_stats["p95_ms"] is not None and restored_true_ttft_stats["p95_ms"] is not None:
        true_ttft_p95_ok = (
            float(restored_true_ttft_stats["p95_ms"]) < float(cold_true_ttft_stats["p95_ms"])
            if require_restored_p95_lower
            else float(restored_true_ttft_stats["p95_ms"]) <= float(cold_true_ttft_stats["p95_ms"])
        )
    prompt_ok = len(rows) >= min_runs and prompt_reduction_min is not None and prompt_reduction_min >= prompt_reduction_threshold_percent
    proxy_ok = (
        proxy_available
        and proxy_median_improvement is not None
        and proxy_median_improvement >= median_ttft_improvement_threshold_percent
        and proxy_p95_ok
    )
    true_ttft_ok = (
        true_ttft_available
        and true_ttft_median_improvement is not None
        and true_ttft_median_improvement >= median_ttft_improvement_threshold_percent
        and true_ttft_p95_ok
    )
    if len(rows) < min_runs:
        status = "insufficient_runs"
    elif restore_errors or fallback_count or missing_cache_hit_levels or invalid_cache_hit_levels or full_reprocess_suspected_count:
        status = "fail"
    elif prompt_ok and true_ttft_ok:
        status = "pass"
    elif prompt_ok and proxy_ok:
        status = "pass_proxy"
    elif prompt_ok and not true_ttft_available:
        status = "insufficient_true_ttft"
    else:
        status = "fail"
    return {
        "status": status,
        "ok": status == "pass",
        "proxy_benchmark_ok": status in {"pass", "pass_proxy"},
        "runs": len(rows),
        "min_runs": min_runs,
        "restore_errors": restore_errors,
        "fallback_count": fallback_count,
        "cache_hit_levels_allowed": sorted(RESTORED_CACHE_HIT_LEVELS),
        "missing_cache_hit_levels": missing_cache_hit_levels,
        "invalid_cache_hit_levels": invalid_cache_hit_levels,
        "cache_hit_level_ok": len(rows) >= min_runs and missing_cache_hit_levels == 0 and invalid_cache_hit_levels == 0,
        "full_reprocess_suspected_count": full_reprocess_suspected_count,
        "full_reprocess_ok": full_reprocess_suspected_count == 0,
        "prompt_eval_token_reduction_percent_min": prompt_reduction_min,
        "prompt_eval_token_reduction_percent_median": prompt_reduction_median,
        "prompt_eval_token_reduction_threshold_percent": prompt_reduction_threshold_percent,
        "prompt_reduction_ok": prompt_ok,
        "first_token_proxy_available": proxy_available,
        "cold_first_token_proxy": cold_proxy_stats,
        "restored_first_token_proxy": restored_proxy_stats,
        "first_token_proxy_median_improvement_percent": proxy_median_improvement,
        "first_token_proxy_p95_ok": proxy_p95_ok,
        "first_token_proxy_ok": proxy_ok,
        "true_ttft_available": true_ttft_available,
        "cold_true_ttft": cold_true_ttft_stats,
        "restored_true_ttft": restored_true_ttft_stats,
        "true_ttft_median_improvement_percent": true_ttft_median_improvement,
        "true_ttft_p95_ok": true_ttft_p95_ok,
        "true_ttft_ok": true_ttft_ok,
        "median_timing_improvement_threshold_percent": median_ttft_improvement_threshold_percent,
        "restored_p95_lower_than_cold_p95": proxy_p95_ok,
        "acceptance_true_ttft_done": status == "pass",
        "measurement_caveat": "first_token_proxy_ms is non-streaming one-token response wall time, not direct TTFT",
    }


def summarize_self_test_case(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize_runs(
        rows,
        min_runs=10,
        prompt_reduction_threshold_percent=90.0,
        median_ttft_improvement_threshold_percent=30.0,
        require_restored_p95_lower=True,
    )
    keys = [
        "status",
        "ok",
        "proxy_benchmark_ok",
        "runs",
        "restore_errors",
        "fallback_count",
        "cache_hit_level_ok",
        "missing_cache_hit_levels",
        "invalid_cache_hit_levels",
        "full_reprocess_suspected_count",
        "prompt_reduction_ok",
        "prompt_eval_token_reduction_percent_min",
        "first_token_proxy_ok",
        "first_token_proxy_p95_ok",
        "true_ttft_available",
        "true_ttft_ok",
        "true_ttft_p95_ok",
        "acceptance_true_ttft_done",
    ]
    return {key: summary.get(key) for key in keys}


def redacted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe: list[dict[str, Any]] = []
    for row in rows:
        safe.append({key: row.get(key) for key in REDACTED_ROW_FIELDS if key in row})
    return safe


def run_live(args: argparse.Namespace) -> dict[str, Any]:
    router_url = args.router_url.rstrip("/")
    headers = {}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    if args.x_api_key:
        headers["X-API-Key"] = args.x_api_key
    prefix = generate_prefix(args.prefix_repeat)
    suffix = args.suffix_text
    cache_id = f"{safe_id_component(args.cache_id_prefix)}-{int(time.time())}"
    build_status, _, build_response, build_elapsed = request_json(
        "POST",
        f"{router_url}/v1/completions",
        headers=headers,
        body=build_body(args.model, prefix, cache_id, args.worker_id),
        timeout=args.timeout,
    )
    build_meta = build_response.get("cache_router", {}).get("build") if isinstance(build_response.get("cache_router"), dict) else {}
    cache_key_hash = build_meta.get("cache_key_hash") if isinstance(build_meta, dict) else None
    rows = []
    if build_status == 200:
        for run_index in range(1, args.runs + 1):
            rows.append(run_one_live(args, router_url, headers, prefix, suffix, cache_id, cache_key_hash, run_index))
            if args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
    summary = summarize_runs(
        rows,
        min_runs=ACCEPTANCE_MIN_RUNS,
        prompt_reduction_threshold_percent=args.prompt_reduction_threshold_percent,
        median_ttft_improvement_threshold_percent=args.median_ttft_improvement_threshold_percent,
        require_restored_p95_lower=not args.allow_equal_p95,
    )
    if build_status != 200:
        summary["status"] = "build_failed"
        summary["ok"] = False
    result = {
        "schema_version": "2026-07-02.1",
        "status": summary["status"],
        "ok": summary["status"] == "pass",
        "proxy_benchmark_ok": summary["status"] in {"pass", "pass_proxy"},
        "scope": "operator-supplied suffix-route benchmark gate; live deployment evidence, not distributed-cache correctness proof",
        "created_utc": now_iso(),
        "model_hash": sha256_text(args.model),
        "cache_id": cache_id,
        "cache_key_hash": cache_key_hash,
        "prefix_hash": sha256_text(prefix),
        "prefix_chars": len(prefix),
        "suffix_hash": sha256_text(suffix),
        "suffix_chars": len(suffix),
        "build_http_status": build_status,
        "build_elapsed_ms": build_elapsed,
        "summary": summary,
        "runs": redacted_rows(rows),
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
        "notes": [
            "does not write raw prompts or raw responses to artifacts",
            "first_token_proxy_ms is a non-streaming one-token wall-time proxy, not direct TTFT",
            "pass_proxy indicates proxy improvement only; top-level ok requires true first-token timing",
            "true TTFT uses a normal streaming cold probe and cache_router.measure_true_ttft for restored cached probes",
        ],
    }
    if not args.no_output_files:
        out_dir = Path(args.out_dir) if args.out_dir else Path("runtime") / "cache-router-suffix-benchmark" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_path = out_dir / "summary.json"
        summary_path.write_text(json.dumps({key: value for key, value in result.items() if key != "cache_id"}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result["output_paths"] = {"summary": str(summary_path)}
    return result


def run_self_test() -> dict[str, Any]:
    passing_rows = [
        {
            "cold_prompt_tokens": 1000,
            "restored_prompt_tokens": 40,
            "cold_first_token_proxy_ms": 100.0 + index,
            "restored_first_token_proxy_ms": 50.0,
            "cold_true_ttft_ms": 100.0 + index,
            "restored_true_ttft_ms": 50.0,
            "restore_error": False,
            "fallback_used": False,
            "cache_hit_level": "local_nvme",
            "restored_cached_tokens": 960,
            "full_reprocess_suspected": "no",
            "request_ids": {"cold": f"cold-{index}", "restored": f"restored-{index}"},
            "raw_prompt": "do not emit this field",
            "response_content": "do not emit this field",
        }
        for index in range(10)
    ]
    proxy_only_rows = [{**row, "cold_true_ttft_ms": None, "restored_true_ttft_ms": None} for row in passing_rows]
    cases = {
        "pass_true_ttft": passing_rows,
        "pass_proxy_only": proxy_only_rows,
        "fail_low_prompt_reduction": [{**row, "restored_prompt_tokens": 300} for row in passing_rows],
        "fail_restore_error": [{**row, "restore_error": True} for row in passing_rows],
        "fail_fallback": [{**row, "fallback_used": True} for row in passing_rows],
        "fail_missing_cache_hit_level": [{**row, "cache_hit_level": ""} for row in passing_rows],
        "fail_invalid_cache_hit_level": [{**row, "cache_hit_level": "registry_only"} for row in passing_rows],
        "fail_full_reprocess_zero_cached": [
            {**row, "restored_cached_tokens": 0, "full_reprocess_suspected": "yes"} for row in passing_rows
        ],
        "fail_full_reprocess_processed_ge_cached": [
            {**row, "restored_cached_tokens": 40, "full_reprocess_suspected": "yes"} for row in passing_rows
        ],
        "fail_worse_true_ttft_p95": [
            {**row, "restored_true_ttft_ms": 140.0, "restored_first_token_proxy_ms": 140.0} for row in passing_rows
        ],
        "insufficient_runs": passing_rows[:9],
        "insufficient_true_ttft": [
            {**row, "cold_first_token_proxy_ms": None, "restored_first_token_proxy_ms": None, "cold_true_ttft_ms": None, "restored_true_ttft_ms": None}
            for row in passing_rows
        ],
    }
    summaries = {name: summarize_self_test_case(rows) for name, rows in cases.items()}
    redacted = redacted_rows(passing_rows[:1])
    check_results = {
        "pass_true_ttft_status": summaries["pass_true_ttft"]["status"] == "pass",
        "pass_true_ttft_top_level_ok": summaries["pass_true_ttft"]["ok"] is True,
        "pass_true_ttft_marks_acceptance_done": summaries["pass_true_ttft"]["acceptance_true_ttft_done"] is True,
        "pass_proxy_only_status": summaries["pass_proxy_only"]["status"] == "pass_proxy",
        "pass_proxy_only_not_top_level_ok": summaries["pass_proxy_only"]["ok"] is False,
        "pass_proxy_only_sets_proxy_ok": summaries["pass_proxy_only"]["proxy_benchmark_ok"] is True,
        "pass_proxy_only_does_not_mark_true_ttft_done": summaries["pass_proxy_only"]["acceptance_true_ttft_done"] is False,
        "low_prompt_reduction_fails": summaries["fail_low_prompt_reduction"]["status"] == "fail",
        "restore_error_fails": summaries["fail_restore_error"]["status"] == "fail",
        "fallback_fails": summaries["fail_fallback"]["status"] == "fail",
        "missing_cache_hit_level_fails": summaries["fail_missing_cache_hit_level"]["status"] == "fail",
        "invalid_cache_hit_level_fails": summaries["fail_invalid_cache_hit_level"]["status"] == "fail",
        "zero_cached_full_reprocess_fails": summaries["fail_full_reprocess_zero_cached"]["status"] == "fail",
        "processed_ge_cached_full_reprocess_fails": summaries["fail_full_reprocess_processed_ge_cached"]["status"] == "fail",
        "large_prompt_reduction_overrides_equal_cached_processed": classify_full_reprocess(10, 10, cold_prompt_tokens=6665) == "no",
        "worse_true_ttft_p95_fails": summaries["fail_worse_true_ttft_p95"]["status"] == "fail",
        "insufficient_runs_classified": summaries["insufficient_runs"]["status"] == "insufficient_runs",
        "insufficient_true_ttft_classified": summaries["insufficient_true_ttft"]["status"] == "insufficient_true_ttft",
        "prompt_reduction_math": summaries["pass_true_ttft"]["prompt_eval_token_reduction_percent_min"] == 96.0,
        "redacts_request_ids_and_raw_payloads": bool(redacted)
        and "request_ids" not in redacted[0]
        and "raw_prompt" not in redacted[0]
        and "response_content" not in redacted[0],
    }
    return {
        "ok": all(check_results.values()),
        "scope": "self-test only; no router contacted",
        "checks": len(check_results),
        "passed": sum(1 for item in check_results.values() if item),
        "failed_checks": [name for name, passed in check_results.items() if not passed],
        "case_statuses": summaries,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="Run offline math/redaction self-test.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--router-url", help="Operator-supplied Cachy Router URL, for example http://<router-lan-ip>:18080.")
    parser.add_argument("--model", help="Configured model served through the router.")
    parser.add_argument("--api-key", help="Bearer token for router auth, if configured.")
    parser.add_argument("--x-api-key", help="X-API-Key value for router auth, if configured.")
    parser.add_argument("--worker-id", help="Optional explicit worker target.")
    parser.add_argument("--runs", type=int, default=10, help="Repeated cold/restored runs. Final gate requires at least 10.")
    parser.add_argument("--max-tokens", type=int, default=1, help="Generated tokens per probe request. Proxy timing gate expects 1.")
    parser.add_argument("--prefix-repeat", type=int, default=512, help="Synthetic public prefix line repetitions.")
    parser.add_argument("--suffix-text", default="\n\nAnswer with exactly: cache router benchmark ok", help="Public-safe suffix text.")
    parser.add_argument("--cache-id-prefix", default="suffix-benchmark", help="Public-safe cache ID prefix.")
    parser.add_argument("--prompt-reduction-threshold-percent", type=float, default=90.0, help="Required minimum prompt-eval token reduction.")
    parser.add_argument("--median-ttft-improvement-threshold-percent", type=float, default=30.0, help="Required restored median timing improvement.")
    parser.add_argument("--allow-equal-p95", action="store_true", help="Allow restored p95 TTFT equal to cold p95 instead of strictly lower.")
    parser.add_argument("--no-true-ttft-probe", dest="true_ttft_probe", action="store_false", help="Disable streaming true-TTFT probes and report proxy timing only.")
    parser.set_defaults(true_ttft_probe=True)
    parser.add_argument(
        "--restore-generation-strategy",
        choices=["suffix_only_after_slot_restore", "full_prompt_after_slot_restore"],
        default="suffix_only_after_slot_restore",
        help="Cached restore generation strategy to benchmark.",
    )
    parser.add_argument("--pause-seconds", type=float, default=0.0, help="Pause between repeated probe runs.")
    parser.add_argument("--timeout", type=float, default=180.0, help="HTTP timeout in seconds.")
    parser.add_argument("--out-dir", help="Ignored runtime directory for redacted summary.json.")
    parser.add_argument("--no-output-files", action="store_true", help="Do not write runtime summary.json in live mode.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        result = run_self_test()
    else:
        if not args.router_url or not args.model:
            raise SystemExit("--router-url and --model are required unless --self-test is used")
        if args.runs < ACCEPTANCE_MIN_RUNS:
            raise SystemExit(f"--runs must be at least {ACCEPTANCE_MIN_RUNS} for the acceptance benchmark gate")
        if args.max_tokens < 1 or args.prefix_repeat < 1 or args.timeout <= 0 or args.pause_seconds < 0:
            raise SystemExit("max tokens, prefix repeat, timeout, and pause must be valid")
        result = run_live(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
