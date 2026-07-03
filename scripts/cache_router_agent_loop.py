#!/usr/bin/env python3
"""Minimal Cachy Router agent loop for live cache capability checks.

The script is intentionally small: it runs named subagents, each of which
performs one bounded router check and reports sanitized metrics. Live mode
contacts only the operator-supplied router URL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]

REDACTED_KEYS = {
    "api_key",
    "authorization",
    "body",
    "choices",
    "content",
    "messages",
    "prompt",
    "raw",
    "response",
    "text",
    "text_preview",
}
PATH_KEYS = {
    "artifact",
    "artifact_path",
    "blob_path",
    "cache_root",
    "manifest_path",
    "out_dir",
    "path",
    "router_blob_path",
    "slot_path",
    "source_blob_path",
    "worker_slot_path",
}


@dataclass(frozen=True)
class AgentContext:
    router_url: str
    model: str
    headers: dict[str, str]
    api_key: str | None
    x_api_key: str | None
    workers_file: Path | None
    run_id: str
    timeout: float
    runs: int
    benchmark_runs: int
    max_tokens: int
    worker_id: str | None
    second_worker_id: str | None
    router_restore_validation: str
    no_output_files: bool
    prefix_text: str | None


@dataclass(frozen=True)
class AgentResult:
    name: str
    ok: bool
    status: str
    elapsed_ms: float
    metrics: dict[str, Any]
    capabilities: list[str]
    caveats: list[str]


@dataclass(frozen=True)
class Subagent:
    name: str
    claim_class: str
    run: Callable[[AgentContext], AgentResult]


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def now_compact() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def safe_id_component(text: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in ".:-" else "-" for char in text)
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] or "id"


def sanitize(value: Any, *, key: str = "") -> Any:
    key_lc = key.lower()
    if key_lc in REDACTED_KEYS:
        return {"redacted": True}
    if key_lc in PATH_KEYS:
        return {"redacted_path": True}
    if isinstance(value, dict):
        return {str(child_key): sanitize(child_value, key=str(child_key)) for child_key, child_value in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        if value.startswith("/") or "/home/" in value:
            return {"redacted_path": True}
        if len(value) > 240:
            return {"sha256": sha256_text(value), "chars": len(value), "bytes": len(value.encode("utf-8"))}
    return value


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


def body_cache_router(body: dict[str, Any]) -> dict[str, Any]:
    value = body.get("cache_router")
    return value if isinstance(value, dict) else {}


def response_error_type(body: dict[str, Any]) -> str | None:
    error = body.get("error")
    if isinstance(error, dict) and isinstance(error.get("type"), str):
        return error["type"]
    return None


def cache_key_from_body(body: dict[str, Any]) -> str | None:
    cache_router = body_cache_router(body)
    for path in (("cache_key_hash",), ("build", "cache_key_hash"), ("use", "cache_key_hash")):
        cursor: Any = cache_router
        for key in path:
            cursor = cursor.get(key) if isinstance(cursor, dict) else None
        if isinstance(cursor, str) and cursor:
            return cursor
    return None


def auth_cli_args(ctx: AgentContext) -> list[str]:
    args: list[str] = []
    if ctx.api_key:
        args.extend(["--api-key", ctx.api_key])
    if ctx.x_api_key:
        args.extend(["--x-api-key", ctx.x_api_key])
    return args


def sanitized_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    redact_next = False
    secret_flags = {"--api-key", "--x-api-key", "--auth-token", "--auth-token-file"}
    for arg in command:
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue
        redacted.append(arg)
        if arg in secret_flags:
            redact_next = True
    return redacted


def cache_hit_level(headers: dict[str, str], body: dict[str, Any]) -> str:
    header_value = headers.get("x-cache-router-cache-hit-level")
    if header_value:
        return header_value
    use = body_cache_router(body).get("use")
    attempts = use.get("attempts") if isinstance(use, dict) else None
    if isinstance(attempts, list) and attempts and isinstance(attempts[-1], dict):
        value = attempts[-1].get("cache_hit_level")
        if isinstance(value, str):
            return value
    return "none"


def selected_worker(headers: dict[str, str]) -> str:
    value = headers.get("x-cache-router-worker") or "none"
    return value if value else "none"


def response_summary(label: str, status: int, headers: dict[str, str], body: dict[str, Any], elapsed_ms: float) -> dict[str, Any]:
    cache_router = body_cache_router(body)
    return {
        "label": label,
        "http_status": status,
        "ok": status == 200,
        "elapsed_ms": elapsed_ms,
        "request_id": headers.get("x-cache-router-request-id"),
        "trace_id": headers.get("x-cache-router-trace-id"),
        "selected_worker": selected_worker(headers),
        "cache_hit_level": cache_hit_level(headers, body),
        "cache_key_hash": cache_key_from_body(body),
        "has_build": isinstance(cache_router.get("build"), dict),
        "has_use": isinstance(cache_router.get("use"), dict),
        "error_type": response_error_type(body),
        "body_sha256": sha256_json(sanitize(body)),
    }


def ready_worker_ids(ctx: AgentContext) -> tuple[list[str], dict[str, Any]]:
    status, _, body, _ = request_json("GET", f"{ctx.router_url}/router/workers", headers=ctx.headers, timeout=ctx.timeout)
    if status != 200:
        return [], {"status": status, "error_type": response_error_type(body)}
    rows = body.get("workers")
    if not isinstance(rows, list):
        return [], {"status": status, "error": "workers response missing workers array"}
    worker_ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        readiness = row.get("readiness") if isinstance(row.get("readiness"), dict) else {}
        if readiness.get("ok") is not True:
            continue
        worker_id = row.get("worker_id")
        if isinstance(worker_id, str) and worker_id:
            worker_ids.append(worker_id)
    return worker_ids, {"status": status, "workers_seen": len(rows), "ready_workers": len(worker_ids)}


def count_cache_entries(ctx: AgentContext, cache_id: str) -> dict[str, Any]:
    status, _, body, _ = request_json("GET", f"{ctx.router_url}/router/cache", headers=ctx.headers, timeout=ctx.timeout)
    entries = body.get("entries") if isinstance(body.get("entries"), list) else []
    matching = [row for row in entries if isinstance(row, dict) and row.get("cache_id") == cache_id]
    keys = sorted(str(row.get("cache_key_hash")) for row in matching if isinstance(row.get("cache_key_hash"), str))
    return {"status": status, "count": len(matching), "cache_key_hashes": keys}


def run_health(ctx: AgentContext) -> AgentResult:
    started = time.perf_counter()
    checks: dict[str, Any] = {}
    caveats: list[str] = []
    for path in ["/health", "/v1", "/v1/models", "/router/workers", "/metrics"]:
        status, _, body, elapsed_ms = request_json("GET", f"{ctx.router_url}{path}", headers=ctx.headers, timeout=ctx.timeout)
        checks[path] = {
            "status": status,
            "elapsed_ms": elapsed_ms,
            "error_type": response_error_type(body),
            "body": sanitize(body),
        }
        if path == "/metrics" and status in {404, 405}:
            caveats.append("/metrics unavailable or disabled")
    workers = checks.get("/router/workers", {}).get("body", {}).get("workers", [])
    models = checks.get("/v1/models", {}).get("body", {}).get("data", [])
    ok = all(checks.get(path, {}).get("status") == 200 for path in ["/health", "/v1", "/v1/models", "/router/workers"])
    metrics = {
        "checks": checks,
        "worker_count": len(workers) if isinstance(workers, list) else None,
        "ready_workers": checks.get("/router/workers", {}).get("body", {}).get("ready"),
        "model_count": len(models) if isinstance(models, list) else None,
    }
    return AgentResult(
        name="health",
        ok=ok,
        status="ok" if ok else "fail",
        elapsed_ms=(time.perf_counter() - started) * 1000.0,
        metrics=metrics,
        capabilities=["normal OpenAI-compatible pass-through", "router admin health"] if ok else [],
        caveats=caveats,
    )


def cache_body(
    ctx: AgentContext,
    *,
    mode: str,
    cache_id: str,
    suffix_text: str,
    prefix_text: str | None = None,
    cache_key_hash: str | None = None,
    worker_id: str | None = None,
    allow_fallback: bool = False,
) -> dict[str, Any]:
    extension: dict[str, Any] = {
        "mode": mode,
        "cache_id": cache_id,
        "suffix_text": suffix_text,
        "target": "suffix_route",
        "allow_fallback": allow_fallback,
    }
    if prefix_text is not None:
        extension["prefix_text"] = prefix_text
    if cache_key_hash is not None:
        extension["cache_key_hash"] = cache_key_hash
    if worker_id:
        extension["worker_id"] = worker_id
    return {
        "model": ctx.model,
        "prompt": suffix_text,
        "max_tokens": ctx.max_tokens,
        "temperature": 0,
        "stream": False,
        "cache_router": extension,
    }


def run_cache_cycle(ctx: AgentContext) -> AgentResult:
    started = time.perf_counter()
    worker_ids, worker_meta = ready_worker_ids(ctx)
    target_worker = ctx.worker_id or (worker_ids[0] if worker_ids else None)
    second_worker = ctx.second_worker_id
    if second_worker is None:
        second_worker = next((worker_id for worker_id in worker_ids if worker_id != target_worker), None)
    if not target_worker:
        return AgentResult(
            name="cache-cycle",
            ok=False,
            status="no_ready_worker",
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
            metrics={"worker_discovery": worker_meta},
            capabilities=[],
            caveats=["no ready worker available for cache-cycle"],
        )

    base_prefix = ctx.prefix_text or (
        "Cachy Router synthetic static docs.\n"
        "This public-safe document is used only to verify durable branch cache restore.\n"
        "The output intentionally stores only hashes, counts, and router-owned metadata.\n"
    )
    prefix_a = base_prefix + "\nAgent branch: parent read static docs branch A.\n"
    prefix_b = base_prefix + "\nAgent branch: subagent read static docs branch B.\n"
    suffix_a = "\nQuestion: answer using branch A context."
    suffix_b = "\nQuestion: answer using branch B context."
    cache_id = f"agent-loop-{safe_id_component(ctx.run_id)}"
    branch_inputs = {
        "cache_id": cache_id,
        "prefix_a": {"sha256": sha256_text(prefix_a), "bytes": len(prefix_a.encode("utf-8")), "chars": len(prefix_a)},
        "prefix_b": {"sha256": sha256_text(prefix_b), "bytes": len(prefix_b.encode("utf-8")), "chars": len(prefix_b)},
        "target_worker": target_worker,
        "second_worker": second_worker,
    }
    steps: list[dict[str, Any]] = []

    def post_step(label: str, body: dict[str, Any]) -> dict[str, Any]:
        status, headers, response_body, elapsed_ms = request_json(
            "POST",
            f"{ctx.router_url}/v1/completions",
            headers=ctx.headers,
            body=body,
            timeout=ctx.timeout,
        )
        summary = response_summary(label, status, headers, response_body, elapsed_ms)
        steps.append(summary)
        return summary

    before_entries = count_cache_entries(ctx, cache_id)
    build_a = post_step(
        "auto-build-branch-a",
        cache_body(ctx, mode="auto", cache_id=cache_id, prefix_text=prefix_a, suffix_text=suffix_a, worker_id=target_worker),
    )
    key_a = build_a.get("cache_key_hash")
    auto_hit_a = post_step(
        "auto-hit-branch-a",
        cache_body(ctx, mode="auto", cache_id=cache_id, prefix_text=prefix_a, suffix_text=suffix_a, worker_id=target_worker),
    )
    build_b = post_step(
        "auto-build-branch-b",
        cache_body(ctx, mode="auto", cache_id=cache_id, prefix_text=prefix_b, suffix_text=suffix_b, worker_id=target_worker),
    )
    key_b = build_b.get("cache_key_hash")
    old_branch_restore: dict[str, Any] | None = None
    if isinstance(key_a, str) and key_a:
        old_branch_restore = post_step(
            "use-old-branch-a-by-handle",
            cache_body(ctx, mode="use", cache_id=cache_id, cache_key_hash=key_a, suffix_text=suffix_a, worker_id=target_worker),
        )
    cross_worker_restore: dict[str, Any] | None = None
    if isinstance(key_a, str) and key_a and second_worker:
        cross_worker_restore = post_step(
            "use-branch-a-second-worker",
            cache_body(ctx, mode="use", cache_id=cache_id, cache_key_hash=key_a, suffix_text=suffix_a, worker_id=second_worker),
        )
    negative = post_step(
        "negative-absent-cache-key",
        cache_body(ctx, mode="use", cache_id=cache_id, cache_key_hash="0" * 64, suffix_text=suffix_a, worker_id=target_worker),
    )
    after_entries = count_cache_entries(ctx, cache_id)

    cache_hit_counts: dict[str, int] = {}
    selected_worker_counts: dict[str, int] = {}
    for step in steps:
        cache_hit_counts[str(step.get("cache_hit_level") or "none")] = cache_hit_counts.get(str(step.get("cache_hit_level") or "none"), 0) + 1
        selected = str(step.get("selected_worker") or "none")
        selected_worker_counts[selected] = selected_worker_counts.get(selected, 0) + 1

    expected_keys = {key for key in [key_a, key_b] if isinstance(key, str) and key}
    after_keys = set(after_entries.get("cache_key_hashes", []))
    ok = (
        build_a.get("ok") is True
        and auto_hit_a.get("ok") is True
        and build_b.get("ok") is True
        and isinstance(old_branch_restore, dict)
        and old_branch_restore.get("ok") is True
        and negative.get("http_status") == 404
        and expected_keys.issubset(after_keys)
        and len(expected_keys) >= 2
    )
    if cross_worker_restore is not None and cross_worker_restore.get("ok") is not True:
        ok = False
    caveats: list[str] = []
    if second_worker is None:
        caveats.append("durable_blob hydration to a second worker skipped because only one ready worker was selected")
    metrics = {
        "worker_discovery": worker_meta,
        "branch_inputs": branch_inputs,
        "before_entries": before_entries,
        "after_entries": after_entries,
        "steps": steps,
        "cache_hit_level_counts": cache_hit_counts,
        "selected_worker_counts": selected_worker_counts,
        "branch_keys_distinct": len(expected_keys) >= 2,
        "old_branch_restore_ok": bool(old_branch_restore and old_branch_restore.get("ok")),
        "cross_worker_restore_ok": None if cross_worker_restore is None else bool(cross_worker_restore.get("ok")),
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
    }
    return AgentResult(
        name="cache-cycle",
        ok=ok,
        status="ok" if ok else "fail",
        elapsed_ms=(time.perf_counter() - started) * 1000.0,
        metrics=metrics,
        capabilities=[
            "explicit suffix-route cache hits",
            "worker-local hot cache",
            "router-owned durable blob hydration" if cross_worker_restore else "router-owned durable branch handle",
        ],
        caveats=caveats,
    )


def script_result(name: str, ctx: AgentContext, command: list[str], *, claim_class: str) -> AgentResult:
    started = time.perf_counter()
    proc = subprocess.run(command, capture_output=True, text=True, timeout=ctx.timeout + 30.0, check=False)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    parsed: Any
    try:
        parsed = json.loads(proc.stdout)
    except Exception:  # noqa: BLE001
        parsed = {
            "stdout_sha256": sha256_text(proc.stdout),
            "stdout_bytes": len(proc.stdout.encode("utf-8")),
            "stderr_sha256": sha256_text(proc.stderr),
            "stderr_bytes": len(proc.stderr.encode("utf-8")),
        }
    metrics = {
        "returncode": proc.returncode,
        "command": [
            str(PACKAGE_ROOT / "scripts" / Path(arg).name) if index == 1 else arg
            for index, arg in enumerate(sanitized_command(command))
        ],
        "result": sanitize(parsed),
        "stderr": {"sha256": sha256_text(proc.stderr), "bytes": len(proc.stderr.encode("utf-8"))} if proc.stderr else None,
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
    }
    ok = proc.returncode == 0 and (not isinstance(parsed, dict) or parsed.get("ok", True) is not False)
    return AgentResult(
        name=name,
        ok=ok,
        status="ok" if ok else "fail",
        elapsed_ms=elapsed_ms,
        metrics=metrics,
        capabilities=[claim_class] if ok else [],
        caveats=[],
    )


def endpoint_matrix_command(ctx: AgentContext) -> list[str]:
    command = [
        sys.executable,
        str(PACKAGE_ROOT / "scripts" / "cache_router_live_endpoint_matrix.py"),
        "--json",
        "--router-url",
        ctx.router_url,
        "--timeout",
        str(ctx.timeout),
        "--max-tokens",
        str(ctx.max_tokens),
    ]
    if ctx.workers_file:
        command.extend(["--workers-file", str(ctx.workers_file)])
    command.extend(auth_cli_args(ctx))
    if ctx.no_output_files:
        command.append("--no-output-files")
    return command


def run_endpoint_matrix(ctx: AgentContext) -> AgentResult:
    command = endpoint_matrix_command(ctx)
    return script_result("endpoint-matrix", ctx, command, claim_class="normal OpenAI-compatible pass-through")


def suffix_benchmark_command(ctx: AgentContext) -> list[str]:
    command = [
        sys.executable,
        str(PACKAGE_ROOT / "scripts" / "cache_router_suffix_benchmark_gate.py"),
        "--json",
        "--router-url",
        ctx.router_url,
        "--model",
        ctx.model,
        "--timeout",
        str(ctx.timeout),
        "--runs",
        str(max(10, ctx.benchmark_runs)),
        "--max-tokens",
        "1",
        "--cache-id-prefix",
        f"agent-loop-benchmark-{safe_id_component(ctx.run_id)}",
    ]
    if ctx.worker_id:
        command.extend(["--worker-id", ctx.worker_id])
    command.extend(auth_cli_args(ctx))
    if ctx.no_output_files:
        command.append("--no-output-files")
    return command


def run_suffix_benchmark(ctx: AgentContext) -> AgentResult:
    command = suffix_benchmark_command(ctx)
    return script_result("suffix-benchmark", ctx, command, claim_class="scheduling or suffix-route performance probe")


def correctness_command(ctx: AgentContext) -> list[str]:
    command = [
        sys.executable,
        str(PACKAGE_ROOT / "scripts" / "cache_router_correctness_probe.py"),
        "--json",
        "--router-url",
        ctx.router_url,
        "--model",
        ctx.model,
        "--timeout",
        str(ctx.timeout),
        "--runs",
        str(ctx.runs),
        "--max-tokens",
        str(max(1, ctx.max_tokens)),
        "--cache-id-prefix",
        f"agent-loop-correctness-{safe_id_component(ctx.run_id)}",
        "--no-fallback",
    ]
    if ctx.router_restore_validation != "off":
        command.extend(["--router-restore-validation", ctx.router_restore_validation])
    if ctx.worker_id:
        command.extend(["--worker-id", ctx.worker_id])
    command.extend(auth_cli_args(ctx))
    if ctx.no_output_files:
        command.append("--no-output-files")
    return command


def run_correctness(ctx: AgentContext) -> AgentResult:
    command = correctness_command(ctx)
    return script_result("correctness", ctx, command, claim_class="correctness tests that are still missing")


def run_offline_performance(ctx: AgentContext) -> AgentResult:
    command = [
        sys.executable,
        str(PACKAGE_ROOT / "scripts" / "cache_router_performance_probe.py"),
        "--json",
    ]
    return script_result("offline-performance", ctx, command, claim_class="offline performance regression probe")


def build_subagents() -> dict[str, Subagent]:
    return {
        "health": Subagent("health", "normal OpenAI-compatible pass-through", run_health),
        "cache-cycle": Subagent("cache-cycle", "explicit suffix-route cache hits", run_cache_cycle),
        "endpoint-matrix": Subagent("endpoint-matrix", "normal OpenAI-compatible pass-through", run_endpoint_matrix),
        "suffix-benchmark": Subagent("suffix-benchmark", "scheduling or suffix-route performance probe", run_suffix_benchmark),
        "correctness": Subagent("correctness", "correctness tests that are still missing", run_correctness),
        "offline-performance": Subagent("offline-performance", "offline performance regression probe", run_offline_performance),
    }


def selected_subagents(args: argparse.Namespace) -> list[str]:
    if args.subagent:
        return args.subagent
    suites = {
        "smoke": ["health", "cache-cycle"],
        "cache": ["cache-cycle"],
        "benchmark": ["suffix-benchmark"],
        "correctness": ["correctness"],
        "offline": ["offline-performance"],
        "all": ["health", "endpoint-matrix", "cache-cycle", "suffix-benchmark", "correctness", "offline-performance"],
    }
    return suites[args.suite]


def read_prefix_files(paths: list[str]) -> str | None:
    if not paths:
        return None
    chunks = []
    for raw_path in paths:
        path = Path(raw_path)
        chunks.append(path.read_text(encoding="utf-8"))
    return "\n\n".join(chunks)


def command_has_pair(command: list[str], flag: str, value: str) -> bool:
    for index, arg in enumerate(command[:-1]):
        if arg == flag and command[index + 1] == value:
            return True
    return False


def run_self_test() -> dict[str, Any]:
    bearer_token = "secret-token"
    x_api_key = "<api-key>"
    ctx = AgentContext(
        router_url="http://127.0.0.1:18080",
        model="fake-model",
        headers={"Authorization": f"Bearer {bearer_token}", "X-API-Key": x_api_key},
        api_key=bearer_token,
        x_api_key=x_api_key,
        workers_file=Path("configs/cache-router/workers.example.json"),
        run_id="self-test",
        timeout=1.0,
        runs=10,
        benchmark_runs=10,
        max_tokens=1,
        worker_id="worker-a",
        second_worker_id=None,
        router_restore_validation="deterministic_recompute",
        no_output_files=True,
        prefix_text=None,
    )
    wrapped_commands = {
        "endpoint_matrix": endpoint_matrix_command(ctx),
        "suffix_benchmark": suffix_benchmark_command(ctx),
        "correctness": correctness_command(ctx),
    }
    redacted_commands = {name: sanitized_command(command) for name, command in wrapped_commands.items()}
    redacted_text = json.dumps(redacted_commands, sort_keys=True)
    checks = {
        "auth_cli_args_forwards_bearer": auth_cli_args(ctx)[:2] == ["--api-key", bearer_token],
        "auth_cli_args_forwards_x_api_key": auth_cli_args(ctx)[2:] == ["--x-api-key", x_api_key],
        "all_wrapped_commands_include_bearer": all(command_has_pair(command, "--api-key", bearer_token) for command in wrapped_commands.values()),
        "all_wrapped_commands_include_x_api_key": all(command_has_pair(command, "--x-api-key", x_api_key) for command in wrapped_commands.values()),
        "correctness_uses_restore_validation": command_has_pair(wrapped_commands["correctness"], "--router-restore-validation", "deterministic_recompute"),
        "redacted_commands_hide_bearer": bearer_token not in redacted_text,
        "redacted_commands_hide_x_api_key": x_api_key not in redacted_text,
        "redacted_commands_keep_flags": all("--api-key" in command and "--x-api-key" in command for command in redacted_commands.values()),
        "redacted_commands_mark_secret_values": redacted_text.count("<redacted>") == 6,
    }
    return {
        "ok": all(checks.values()),
        "scope": "self-test only; no router contacted",
        "checks": len(checks),
        "passed": sum(1 for passed in checks.values() if passed),
        "failed_checks": [name for name, passed in checks.items() if not passed],
        "wrapped_commands": sanitize(redacted_commands),
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="Run local command-construction checks without contacting a router.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--router-url", help="Operator-supplied Cachy Router URL, for example http://<router-lan-ip>:18080.")
    parser.add_argument("--model", help="Configured model ID served through the router.")
    parser.add_argument("--suite", choices=["smoke", "cache", "benchmark", "correctness", "offline", "all"], default="smoke")
    parser.add_argument("--subagent", action="append", choices=sorted(build_subagents()), help="Subagent to run. Repeatable. Overrides --suite.")
    parser.add_argument("--run-id", default=f"agent-loop-{now_compact()}", help="Public-safe run id used for cache ids.")
    parser.add_argument("--workers-file", help="Operator deployment inventory for endpoint-matrix.")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout seconds.")
    parser.add_argument("--runs", type=int, default=3, help="Runs for correctness subagent.")
    parser.add_argument("--benchmark-runs", type=int, default=10, help="Runs for benchmark subagent.")
    parser.add_argument("--max-tokens", type=int, default=8, help="Generated tokens for inline probes.")
    parser.add_argument("--worker-id", help="Preferred worker id for cache-cycle and wrapped probes.")
    parser.add_argument("--second-worker-id", help="Optional second worker id for durable-blob hydration check.")
    parser.add_argument(
        "--router-restore-validation",
        choices=["off", "deterministic_recompute"],
        default="deterministic_recompute",
        help="Restore validation mode passed to the correctness subagent.",
    )
    parser.add_argument("--api-key-env", default="CACHE_ROUTER_API_KEY", help="Environment variable containing router bearer token.")
    parser.add_argument("--x-api-key-env", default="CACHE_ROUTER_X_API_KEY", help="Environment variable containing router X-API-Key token.")
    parser.add_argument("--prefix-file", action="append", default=[], help="Optional public-safe static document prefix file. Repeatable.")
    parser.add_argument("--no-output-files", dest="no_output_files", action="store_true", default=True, help="Do not write runtime artifacts where wrapped probes support it.")
    parser.add_argument("--write-output-files", dest="no_output_files", action="store_false", help="Allow wrapped probes to write their redacted runtime artifacts.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        result = run_self_test()
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
        else:
            print(json.dumps(result, sort_keys=True))
        return 0 if result["ok"] else 1
    if not args.router_url or not args.model:
        raise SystemExit("--router-url and --model are required unless --self-test is set")
    if args.timeout <= 0 or args.runs <= 0 or args.benchmark_runs <= 0 or args.max_tokens <= 0:
        raise SystemExit("timeout, runs, benchmark-runs, and max-tokens must be positive")
    headers: dict[str, str] = {}
    api_key = (os.environ.get(args.api_key_env) or "").strip()
    x_api_key = (os.environ.get(args.x_api_key_env) or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if x_api_key:
        headers["X-API-Key"] = x_api_key
    ctx = AgentContext(
        router_url=args.router_url.rstrip("/"),
        model=args.model,
        headers=headers,
        api_key=api_key or None,
        x_api_key=x_api_key or None,
        workers_file=Path(args.workers_file) if args.workers_file else None,
        run_id=args.run_id,
        timeout=args.timeout,
        runs=args.runs,
        benchmark_runs=args.benchmark_runs,
        max_tokens=args.max_tokens,
        worker_id=args.worker_id,
        second_worker_id=args.second_worker_id,
        router_restore_validation=args.router_restore_validation,
        no_output_files=args.no_output_files,
        prefix_text=read_prefix_files(args.prefix_file),
    )
    registry = build_subagents()
    requested = selected_subagents(args)
    results: list[AgentResult] = []
    for name in requested:
        subagent = registry[name]
        results.append(subagent.run(ctx))
    output = {
        "ok": all(result.ok for result in results),
        "status": "ok" if all(result.ok for result in results) else "fail",
        "run_id": args.run_id,
        "router_url": args.router_url,
        "model": args.model,
        "subagents": [
            {
                "name": result.name,
                "ok": result.ok,
                "status": result.status,
                "elapsed_ms": result.elapsed_ms,
                "metrics": sanitize(result.metrics),
                "capabilities": result.capabilities,
                "caveats": result.caveats,
            }
            for result in results
        ],
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
    }
    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        print(json.dumps(output, sort_keys=True))
    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
