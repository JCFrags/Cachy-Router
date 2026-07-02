#!/usr/bin/env python3
"""One-node cache-router proof of concept over llama.cpp slot save/restore.

This is not a production router. It is a small controller-side harness that
generates a deterministic synthetic public prefix, measures a cold full prompt,
saves a prefix slot cache, restores it after an externally controlled server
restart, and measures restored suffix/full-prompt routes.

The script records hashes, token counts, timings, slot API metadata, and
redacted cache-router decision events. It does not write the full prompt text to
the output directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "2026-06-30.1"
TENANT_HASH = hashlib.sha256(b"cache-router-poc-tenant").hexdigest()
CONVERSATION_HASH = hashlib.sha256(b"cache-router-poc-conversation").hexdigest()
POLICY_HASH = hashlib.sha256(b"cache-router-poc-policy").hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def http_request(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: float,
    text: bool = False,
) -> tuple[int, Any, float]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept-Encoding": "identity"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if text:
                return resp.status, raw, elapsed_ms
            return resp.status, json.loads(raw) if raw else {}, elapsed_ms
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        try:
            body: Any = json.loads(raw)
        except json.JSONDecodeError:
            body = {"error_text": raw}
        return exc.code, body, elapsed_ms


def get_json(base_url: str, path: str, timeout: float) -> Any:
    status, body, _ = http_request("GET", base_url + path, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"GET {path} failed with HTTP {status}: {body}")
    return body


def post_json(base_url: str, path: str, payload: dict[str, Any], timeout: float) -> tuple[int, Any, float]:
    return http_request("POST", base_url + path, payload=payload, timeout=timeout)


def request_text(base_url: str, path: str, timeout: float) -> str:
    status, body, _ = http_request("GET", base_url + path, timeout=timeout, text=True)
    if status >= 400:
        raise RuntimeError(f"GET {path} failed with HTTP {status}: {body}")
    return str(body)


def parse_metrics(text: str) -> dict[str, float]:
    wanted = {
        "llamacpp:prompt_tokens_total": "prompt_tokens_total",
        "llamacpp:prompt_seconds_total": "prompt_seconds_total",
        "llamacpp:tokens_predicted_total": "tokens_predicted_total",
        "llamacpp:tokens_predicted_seconds_total": "tokens_predicted_seconds_total",
        "llamacpp:requests_processing": "requests_processing",
        "llamacpp:requests_deferred": "requests_deferred",
    }
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 2 or parts[0] not in wanted:
            continue
        try:
            out[wanted[parts[0]]] = float(parts[1])
        except ValueError:
            continue
    return out


def metrics_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in sorted(set(before) | set(after)):
        if key in before and key in after:
            out[key] = after[key] - before[key]
    return out


def first_slot(slots: Any, slot_id: int) -> dict[str, Any]:
    if isinstance(slots, list):
        for row in slots:
            if isinstance(row, dict) and row.get("id") == slot_id:
                return row
    return {}


def sanitize_slot(slot: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "id",
        "n_ctx",
        "speculative",
        "is_processing",
        "n_prompt_tokens",
        "n_prompt_tokens_processed",
        "n_prompt_tokens_cache",
    ]
    return {key: slot.get(key) for key in keys if key in slot}


def token_count(base_url: str, content: str, timeout: float) -> int:
    attempts = [
        {"content": content, "add_special": False},
        {"content": content},
        {"prompt": content},
    ]
    last_body: Any = None
    for payload in attempts:
        status, body, _ = post_json(base_url, "/tokenize", payload, timeout)
        last_body = body
        if status >= 400:
            continue
        if isinstance(body, dict):
            tokens = body.get("tokens")
            if isinstance(tokens, list):
                return len(tokens)
            if isinstance(tokens, int):
                return tokens
        if isinstance(body, list):
            return len(body)
    raise RuntimeError(f"/tokenize did not return tokens; last response={last_body!r}")


def make_prefix(repeats: int) -> str:
    segment = (
        "Public synthetic cache-router prefix. "
        "This sentence contains no private source code, credentials, user data, "
        "or repository content. It exists only to fill llama.cpp context for a "
        "one-node slot save and restore measurement on AMD Strix Halo. "
    )
    return (segment * repeats).strip() + "\n\n"


def generate_prefix(base_url: str, target_tokens: int, timeout: float) -> tuple[str, int, int]:
    unit_tokens = max(1, token_count(base_url, make_prefix(1), timeout))
    repeats = max(1, math.ceil(target_tokens / unit_tokens))
    best_text = make_prefix(repeats)
    best_tokens = token_count(base_url, best_text, timeout)
    for _ in range(8):
        if abs(best_tokens - target_tokens) <= max(256, int(target_tokens * 0.015)):
            break
        next_repeats = max(1, round(repeats * (target_tokens / max(1, best_tokens))))
        if next_repeats == repeats:
            next_repeats += 1 if best_tokens < target_tokens else -1
            next_repeats = max(1, next_repeats)
        repeats = next_repeats
        candidate = make_prefix(repeats)
        candidate_tokens = token_count(base_url, candidate, timeout)
        if abs(candidate_tokens - target_tokens) <= abs(best_tokens - target_tokens):
            best_text, best_tokens = candidate, candidate_tokens
    return best_text, best_tokens, repeats


def ensure_idle(base_url: str, slot_id: int, timeout: float) -> None:
    health = get_json(base_url, "/health", timeout)
    if health.get("status") != "ok":
        raise RuntimeError(f"server health is not ok: {health}")
    slot = first_slot(get_json(base_url, "/slots", timeout), slot_id)
    if slot.get("is_processing"):
        raise RuntimeError(f"slot {slot_id} is busy")
    metrics = parse_metrics(request_text(base_url, "/metrics", timeout))
    if metrics.get("requests_processing", 0) or metrics.get("requests_deferred", 0):
        raise RuntimeError(f"server has active/deferred requests: {metrics}")


def completion(
    base_url: str,
    prompt: str,
    *,
    n_predict: int,
    slot_id: int,
    cache_prompt: bool,
    timeout: float,
) -> dict[str, Any]:
    payload = {
        "prompt": prompt,
        "n_predict": n_predict,
        "temperature": 0.0,
        "top_k": 1,
        "cache_prompt": cache_prompt,
        "id_slot": slot_id,
        "stream": False,
    }
    before_metrics = parse_metrics(request_text(base_url, "/metrics", timeout))
    before_slots = get_json(base_url, "/slots", timeout)
    status, body, wall_ms = post_json(base_url, "/completion", payload, timeout)
    after_metrics = parse_metrics(request_text(base_url, "/metrics", timeout))
    after_slots = get_json(base_url, "/slots", timeout)
    if status >= 400:
        raise RuntimeError(f"/completion failed with HTTP {status}: {body}")
    timings = body.get("timings") if isinstance(body, dict) else {}
    timings = timings if isinstance(timings, dict) else {}
    return {
        "http_status": status,
        "wall_ms": wall_ms,
        "tokens_evaluated": body.get("tokens_evaluated") if isinstance(body, dict) else None,
        "tokens_predicted": body.get("tokens_predicted") if isinstance(body, dict) else None,
        "tokens_cached": body.get("tokens_cached") if isinstance(body, dict) else None,
        "timings": {
            "prompt_ms": timings.get("prompt_ms"),
            "prompt_per_second": timings.get("prompt_per_second"),
            "predicted_ms": timings.get("predicted_ms"),
            "predicted_per_second": timings.get("predicted_per_second"),
            "draft_n": timings.get("draft_n"),
            "draft_n_accepted": timings.get("draft_n_accepted"),
        },
        "metrics_delta": metrics_delta(before_metrics, after_metrics),
        "slot_before": sanitize_slot(first_slot(before_slots, slot_id)),
        "slot_after": sanitize_slot(first_slot(after_slots, slot_id)),
        "content_preview": str(body.get("content", ""))[:120] if isinstance(body, dict) else "",
    }


def slot_action(base_url: str, slot_id: int, action: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    status, body, wall_ms = post_json(base_url, f"/slots/{slot_id}?action={action}", payload, timeout)
    if status >= 400:
        raise RuntimeError(f"slot action {action} failed with HTTP {status}: {body}")
    return {"http_status": status, "wall_ms": wall_ms, "body": body}


def remote_file_stat(ssh_host: str, remote_path: str, timeout: float) -> dict[str, Any]:
    if not ssh_host or not remote_path:
        return {}
    script = (
        "set -euo pipefail\n"
        "p=$1\n"
        "if [ -f \"$p\" ]; then "
        "printf 'exists=yes\\nsize=%s\\nmtime=%s\\n' \"$(stat -c %s \"$p\")\" \"$(stat -c %y \"$p\")\"; "
        "else printf 'exists=no\\n'; fi\n"
    )
    proc = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", ssh_host, "/bin/bash", "-s", "--", remote_path],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    out: dict[str, Any] = {"ssh_rc": proc.returncode}
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key] = int(value) if value.isdigit() else value
    return out


def cache_event(
    *,
    phase: str,
    decision: str,
    trace_id: str,
    request_id: str,
    request_hash: str,
    model_id: str,
    worker_id: str | None,
    cache_key_hash: str | None,
    manifest_id: str | None,
    cache_hit_level: str,
    compatibility_result: str,
    validation_status: str | None,
    fallback_required: bool,
    fallback_reason: str | None,
    latency_ms: float | None,
    prompt_tokens: int | None,
    processed_prompt_tokens: int | None,
    cached_tokens: int | None,
    generated_tokens: int | None,
    prompt_tps: float | None,
    eval_tps: float | None,
    restore_latency_ms: float | None = None,
    notes: str = "",
) -> dict[str, Any]:
    reuse_ratio = cached_tokens / prompt_tokens if prompt_tokens and cached_tokens is not None else None
    if isinstance(reuse_ratio, (int, float)):
        reuse_ratio = max(0.0, min(1.0, float(reuse_ratio)))
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": "evt-" + hashlib.sha256(f"{trace_id}:{phase}:{request_id}:{time.time_ns()}".encode()).hexdigest()[:16],
        "trace_id": trace_id,
        "request_id": request_id,
        "request_hash": request_hash,
        "timestamp": now_iso(),
        "phase": phase,
        "decision": decision,
        "tenant_hash": TENANT_HASH,
        "conversation_hash": CONVERSATION_HASH,
        "scope": "conversation",
        "model_id": model_id,
        "worker_id": worker_id,
        "cache_key_hash": cache_key_hash,
        "manifest_id": manifest_id,
        "cache_hit_level": cache_hit_level,
        "compatibility_result": compatibility_result,
        "validation_status": validation_status,
        "fallback_required": fallback_required,
        "fallback_reason": fallback_reason,
        "latency_ms": latency_ms,
        "metrics": {
            "decision_latency_ms": latency_ms,
            "registry_lookup_latency_ms": None,
            "hydration_latency_ms": None,
            "restore_latency_ms": restore_latency_ms,
            "ttft_ms": None,
            "prompt_tokens": prompt_tokens,
            "cached_tokens": cached_tokens,
            "processed_prompt_tokens": processed_prompt_tokens,
            "generated_tokens": generated_tokens,
            "prompt_tps": prompt_tps,
            "eval_tps": eval_tps,
            "reuse_ratio": reuse_ratio,
            "full_reprocess_suspected": "not_interpreted",
            "cache_event_basis": "slot_state",
            "restore_observed_basis": "slot_state" if restore_latency_ms is not None else "not_checked",
        },
        "policy": {
            "scope_allowed": True,
            "persistence_allowed": True,
            "cross_tenant_reuse_allowed": False,
            "global_system_allowlisted": False,
            "policy_id_hash": POLICY_HASH,
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
        "notes": notes[:200] if notes else None,
    }


def phase_inputs(args: argparse.Namespace) -> dict[str, Any]:
    base_url = args.base_url.rstrip("/")
    props = get_json(base_url, "/props", args.timeout)
    prefix, prefix_tokens, repeats = generate_prefix(base_url, args.target_tokens, args.timeout)
    suffix = (
        "\n\nRouter query: Using only the cached public synthetic prefix above, "
        "reply with exactly: cache router restore ok\nAnswer:"
    )
    full_prompt = prefix + suffix
    suffix_tokens = token_count(base_url, suffix, args.timeout)
    full_tokens = token_count(base_url, full_prompt, args.timeout)
    model_id = str(props.get("model_alias") or props.get("model_path") or "unknown-model")
    key_fields = {
        "prefix_hash": sha256_text(prefix),
        "prefix_tokens": prefix_tokens,
        "model_id": model_id,
        "model_path": props.get("model_path"),
        "n_ctx": (props.get("default_generation_settings") or {}).get("n_ctx"),
        "runtime_id": args.runtime_id,
        "cache_settings": args.cache_settings,
        "slot_id": args.slot_id,
    }
    cache_key_hash = sha256_json(key_fields)
    cache_filename = args.cache_filename or f"cache-router-poc-{cache_key_hash[:16]}.slot"
    return {
        "base_url": base_url,
        "props": props,
        "prefix": prefix,
        "suffix": suffix,
        "full_prompt": full_prompt,
        "prefix_tokens": prefix_tokens,
        "suffix_tokens": suffix_tokens,
        "full_tokens": full_tokens,
        "repeats": repeats,
        "prefix_hash": key_fields["prefix_hash"],
        "suffix_hash": sha256_text(suffix),
        "full_prompt_hash": sha256_text(full_prompt),
        "model_id": model_id,
        "key_fields": key_fields,
        "cache_key_hash": cache_key_hash,
        "cache_filename": cache_filename,
        "manifest_id": "manifest-" + cache_key_hash[:16],
    }


def save_registry(out_dir: Path, inputs: dict[str, Any], args: argparse.Namespace, save_result: dict[str, Any]) -> None:
    registry = {
        "schema_version": "2026-07-01.1",
        "created_utc": now_iso(),
        "cache_key_hash": inputs["cache_key_hash"],
        "manifest_id": inputs["manifest_id"],
        "cache_filename": inputs["cache_filename"],
        "remote_slot_dir": args.remote_slot_dir,
        "remote_slot_path_redacted": f"{args.remote_slot_dir.rstrip('/')}/<cache_filename>" if args.remote_slot_dir else None,
        "prefix_hash": inputs["prefix_hash"],
        "prefix_tokens": inputs["prefix_tokens"],
        "prefix_chars": len(inputs["prefix"]),
        "suffix_hash": inputs["suffix_hash"],
        "suffix_tokens": inputs["suffix_tokens"],
        "full_prompt_hash": inputs["full_prompt_hash"],
        "full_prompt_tokens": inputs["full_tokens"],
        "key_fields": inputs["key_fields"],
        "slot_save": save_result,
    }
    write_json(out_dir / "registry-entry.json", registry)


def summarize_completion(row: dict[str, Any]) -> dict[str, Any]:
    timings = row.get("timings") or {}
    return {
        "tokens_evaluated": row.get("tokens_evaluated"),
        "tokens_cached": row.get("tokens_cached"),
        "tokens_predicted": row.get("tokens_predicted"),
        "prompt_ms": timings.get("prompt_ms"),
        "prompt_per_second": timings.get("prompt_per_second"),
        "predicted_ms": timings.get("predicted_ms"),
        "predicted_per_second": timings.get("predicted_per_second"),
        "wall_ms": row.get("wall_ms"),
    }


def update_reductions(results: dict[str, Any]) -> None:
    phases = results.get("phases") or {}
    cold = ((phases.get("cold") or {}).get("completion") or {}).get("timings") or {}
    suffix = ((phases.get("restore") or {}).get("completion") or {}).get("timings") or {}
    replay = ((phases.get("full_replay") or {}).get("completion") or {}).get("timings") or {}
    suffix_restore_ms = ((phases.get("restore") or {}).get("restore") or {}).get("wall_ms")
    replay_restore_ms = ((phases.get("full_replay") or {}).get("restore") or {}).get("wall_ms")
    cold_ms = cold.get("prompt_ms")
    suffix_ms = suffix.get("prompt_ms")
    replay_ms = replay.get("prompt_ms")
    reductions: dict[str, Any] = {
        "target_reduction_percent": 90.0,
        "cold_prompt_ms": cold_ms,
        "suffix_route_prompt_ms": suffix_ms,
        "full_replay_prompt_ms": replay_ms,
        "suffix_route_restore_ms": suffix_restore_ms,
        "full_replay_restore_ms": replay_restore_ms,
    }
    if isinstance(cold_ms, (int, float)) and cold_ms > 0:
        if isinstance(suffix_ms, (int, float)):
            reductions["suffix_route_prompt_only_reduction_percent"] = 100.0 * (1.0 - suffix_ms / cold_ms)
            if isinstance(suffix_restore_ms, (int, float)):
                reductions["suffix_route_restore_inclusive_reduction_percent"] = 100.0 * (
                    1.0 - ((suffix_restore_ms + suffix_ms) / cold_ms)
                )
        if isinstance(replay_ms, (int, float)):
            reductions["full_replay_prompt_only_reduction_percent"] = 100.0 * (1.0 - replay_ms / cold_ms)
            if isinstance(replay_restore_ms, (int, float)):
                reductions["full_replay_restore_inclusive_reduction_percent"] = 100.0 * (
                    1.0 - ((replay_restore_ms + replay_ms) / cold_ms)
                )
    results["reductions"] = reductions


def update_status(results: dict[str, Any]) -> None:
    phases = results.get("phases") or {}
    completed = [name for name in ("cold", "build_cache", "restore", "full_replay") if name in phases]
    reductions = results.get("reductions") or {}
    suffix_reduction = reductions.get("suffix_route_restore_inclusive_reduction_percent")
    full_replay_reduction = reductions.get("full_replay_restore_inclusive_reduction_percent")
    restore = phases.get("restore") or {}
    restore_body = ((restore.get("restore") or {}).get("body") or {}) if isinstance(restore, dict) else {}
    n_restored = restore_body.get("n_restored")
    if isinstance(n_restored, int) and n_restored > 0 and isinstance(suffix_reduction, (int, float)):
        if suffix_reduction >= 90.0 and isinstance(full_replay_reduction, (int, float)) and full_replay_reduction >= 90.0:
            status = "success"
        elif suffix_reduction >= 90.0:
            status = "partial_success"
        else:
            status = "diagnostic_failure"
    else:
        status = "blocked"
    results["completed_phases"] = completed
    results["status"] = status


def update_common_results(args: argparse.Namespace, inputs: dict[str, Any], results: dict[str, Any]) -> None:
    results.setdefault("schema_version", "2026-07-01.1")
    results.setdefault("run_id", args.run_id)
    results.setdefault("created_utc", now_iso())
    results["updated_utc"] = now_iso()
    results["base_url"] = args.base_url.rstrip("/")
    results["target_tokens"] = args.target_tokens
    results["prompt"] = {
        "prefix_hash": inputs["prefix_hash"],
        "prefix_tokens": inputs["prefix_tokens"],
        "prefix_chars": len(inputs["prefix"]),
        "prefix_repeats": inputs["repeats"],
        "suffix_hash": inputs["suffix_hash"],
        "suffix_tokens": inputs["suffix_tokens"],
        "full_prompt_hash": inputs["full_prompt_hash"],
        "full_prompt_tokens": inputs["full_tokens"],
        "raw_prompt_tracked": False,
    }
    results["cache"] = {
        "cache_key_hash": inputs["cache_key_hash"],
        "cache_filename": inputs["cache_filename"],
        "manifest_id": inputs["manifest_id"],
        "remote_slot_dir_redacted": args.remote_slot_dir,
    }
    results["service"] = {
        "model_id": inputs["model_id"],
        "model_path": inputs["props"].get("model_path"),
        "n_ctx": (inputs["props"].get("default_generation_settings") or {}).get("n_ctx"),
        "runtime_id": args.runtime_id,
        "slot_id": args.slot_id,
    }


def run_cold(args: argparse.Namespace, inputs: dict[str, Any], results: dict[str, Any]) -> None:
    ensure_idle(inputs["base_url"], args.slot_id, args.timeout)
    comp = completion(
        inputs["base_url"],
        inputs["full_prompt"],
        n_predict=args.n_predict,
        slot_id=args.slot_id,
        cache_prompt=True,
        timeout=args.timeout,
    )
    results.setdefault("phases", {})["cold"] = {
        "completed_utc": now_iso(),
        "input_tokens": inputs["full_tokens"],
        "completion": comp,
        "summary": summarize_completion(comp),
    }
    append_jsonl(
        Path(args.out_dir) / "cache-router-events.jsonl",
        cache_event(
            phase="cold_prefill_selected",
            decision="cold_prefill",
            trace_id=args.run_id,
            request_id="cold-full-prompt",
            request_hash=inputs["full_prompt_hash"],
            model_id=inputs["model_id"],
            worker_id=args.worker_id,
            cache_key_hash=inputs["cache_key_hash"],
            manifest_id=None,
            cache_hit_level="none",
            compatibility_result="miss",
            validation_status="not_applicable",
            fallback_required=True,
            fallback_reason="no_compatible_manifest",
            latency_ms=comp["timings"].get("prompt_ms"),
            prompt_tokens=comp.get("tokens_evaluated"),
            processed_prompt_tokens=comp.get("tokens_evaluated"),
            cached_tokens=comp.get("tokens_cached") or 0,
            generated_tokens=comp.get("tokens_predicted"),
            prompt_tps=comp["timings"].get("prompt_per_second"),
            eval_tps=comp["timings"].get("predicted_per_second"),
            notes="Cold full P+Q prompt before registry hit.",
        ),
    )


def run_build_cache(args: argparse.Namespace, inputs: dict[str, Any], results: dict[str, Any]) -> None:
    ensure_idle(inputs["base_url"], args.slot_id, args.timeout)
    erase_result = slot_action(inputs["base_url"], args.slot_id, "erase", {}, args.timeout)
    comp = completion(
        inputs["base_url"],
        inputs["prefix"],
        n_predict=args.build_n_predict,
        slot_id=args.slot_id,
        cache_prompt=True,
        timeout=args.timeout,
    )
    save_result = slot_action(
        inputs["base_url"],
        args.slot_id,
        "save",
        {"filename": inputs["cache_filename"]},
        args.timeout,
    )
    remote_slot_path = f"{args.remote_slot_dir.rstrip('/')}/{inputs['cache_filename']}" if args.remote_slot_dir else ""
    save_result["remote_file_stat"] = remote_file_stat(args.ssh_host, remote_slot_path, args.timeout) if remote_slot_path else {}
    save_registry(Path(args.out_dir), inputs, args, save_result)
    results.setdefault("phases", {})["build_cache"] = {
        "completed_utc": now_iso(),
        "target_tokens": args.target_tokens,
        "actual_prefix_tokens": inputs["prefix_tokens"],
        "erase": erase_result,
        "completion": comp,
        "save": save_result,
        "summary": summarize_completion(comp),
    }
    append_jsonl(
        Path(args.out_dir) / "cache-router-events.jsonl",
        cache_event(
            phase="cache_commit_published",
            decision="no_op",
            trace_id=args.run_id,
            request_id="build-prefix-cache",
            request_hash=inputs["prefix_hash"],
            model_id=inputs["model_id"],
            worker_id=args.worker_id,
            cache_key_hash=inputs["cache_key_hash"],
            manifest_id=inputs["manifest_id"],
            cache_hit_level="none",
            compatibility_result="not_checked",
            validation_status="validated",
            fallback_required=False,
            fallback_reason=None,
            latency_ms=save_result.get("wall_ms"),
            prompt_tokens=comp.get("tokens_evaluated"),
            processed_prompt_tokens=comp.get("tokens_evaluated"),
            cached_tokens=comp.get("tokens_cached") or 0,
            generated_tokens=comp.get("tokens_predicted"),
            prompt_tps=comp["timings"].get("prompt_per_second"),
            eval_tps=comp["timings"].get("predicted_per_second"),
            notes="Router registry entry published after slot save.",
        ),
    )


def run_restore(args: argparse.Namespace, inputs: dict[str, Any], results: dict[str, Any], *, full_replay: bool) -> None:
    ensure_idle(inputs["base_url"], args.slot_id, args.timeout)
    restore_result = slot_action(
        inputs["base_url"],
        args.slot_id,
        "restore",
        {"filename": inputs["cache_filename"]},
        args.timeout,
    )
    prompt = inputs["full_prompt"] if full_replay else inputs["suffix"]
    request_id = "full-prompt-replay" if full_replay else "suffix-only-route"
    request_hash = inputs["full_prompt_hash"] if full_replay else inputs["suffix_hash"]
    comp = completion(
        inputs["base_url"],
        prompt,
        n_predict=args.n_predict,
        slot_id=args.slot_id,
        cache_prompt=True,
        timeout=args.timeout,
    )
    phase_name = "full_replay" if full_replay else "restore"
    results.setdefault("phases", {})[phase_name] = {
        "completed_utc": now_iso(),
        "route": "full_prompt_replay" if full_replay else "suffix_only",
        "input_tokens": inputs["full_tokens"] if full_replay else inputs["suffix_tokens"],
        "restore": restore_result,
        "completion": comp,
        "summary": summarize_completion(comp),
    }
    append_jsonl(
        Path(args.out_dir) / "cache-router-events.jsonl",
        cache_event(
            phase="restore_validated",
            decision="restore_then_generate",
            trace_id=args.run_id,
            request_id=request_id,
            request_hash=request_hash,
            model_id=inputs["model_id"],
            worker_id=args.worker_id,
            cache_key_hash=inputs["cache_key_hash"],
            manifest_id=inputs["manifest_id"],
            cache_hit_level="local_nvme",
            compatibility_result="match",
            validation_status="validated",
            fallback_required=False,
            fallback_reason=None,
            latency_ms=restore_result.get("wall_ms"),
            prompt_tokens=comp.get("tokens_evaluated"),
            processed_prompt_tokens=comp.get("tokens_evaluated"),
            cached_tokens=comp.get("tokens_cached"),
            generated_tokens=comp.get("tokens_predicted"),
            prompt_tps=comp["timings"].get("prompt_per_second"),
            eval_tps=comp["timings"].get("predicted_per_second"),
            restore_latency_ms=restore_result.get("wall_ms"),
            notes=("Full P+Q replay after restore." if full_replay else "Suffix-only route after restore."),
        ),
    )


def print_summary(results: dict[str, Any]) -> None:
    phases = results.get("phases") or {}
    print("phase,tokens_evaluated,tokens_cached,prompt_ms,total_wall_ms")
    for name in ("cold", "build_cache", "restore", "full_replay"):
        row = phases.get(name) or {}
        summary = row.get("summary") or {}
        print(
            ",".join(
                str(x)
                for x in [
                    name,
                    summary.get("tokens_evaluated"),
                    summary.get("tokens_cached"),
                    summary.get("prompt_ms"),
                    summary.get("wall_ms"),
                ]
            )
        )
    reductions = results.get("reductions") or {}
    for key in sorted(reductions):
        print(f"{key}={reductions[key]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18082")
    parser.add_argument("--slot-id", type=int, default=0)
    parser.add_argument("--target-tokens", type=int, default=30000)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--cache-filename", default="")
    parser.add_argument("--phase", choices=["cold", "build-cache", "restore", "full-replay", "all"], required=True)
    parser.add_argument("--run-id", default="one-node-cache-router-poc")
    parser.add_argument("--worker-id", default="worker-temp-slot")
    parser.add_argument("--n-predict", type=int, default=16)
    parser.add_argument("--build-n-predict", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--ssh-host", default="")
    parser.add_argument("--remote-slot-dir", default="")
    parser.add_argument("--runtime-id", default="unknown-runtime")
    parser.add_argument("--cache-settings", default="slot-save-restore")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    inputs = phase_inputs(args)
    results_path = out_dir / "results.json"
    results = read_json(results_path, {})
    update_common_results(args, inputs, results)

    if args.phase in ("cold", "all"):
        run_cold(args, inputs, results)
    if args.phase in ("build-cache", "all"):
        run_build_cache(args, inputs, results)
    if args.phase in ("restore", "all"):
        run_restore(args, inputs, results, full_replay=False)
    if args.phase in ("full-replay", "all"):
        run_restore(args, inputs, results, full_replay=True)

    update_reductions(results)
    update_status(results)
    write_json(results_path, results)
    print_summary(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
