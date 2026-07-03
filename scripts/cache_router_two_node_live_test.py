#!/usr/bin/env python3
"""Run a live two-worker cache-router continuation test through the router.

This is primarily a controller-side endpoint test: cache build/use requests go
through only the OpenAI-compatible router endpoint. When requested, the harness
uses SSH only to move the target worker's synthetic POC slot file aside so the
next router request must hydrate from the durable store.
"""

from __future__ import annotations

import argparse
import json
import math
import posixpath
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

from cache_router_one_node_poc import make_prefix, sha256_text, write_json  # noqa: E402
from cache_router_remote_stack import remote_file_info, remote_move_if_exists  # noqa: E402


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def http_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 900.0,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any], float]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            elapsed = (time.perf_counter() - start) * 1000.0
            parsed = json.loads(raw) if raw else {}
            return resp.status, parsed, elapsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        elapsed = (time.perf_counter() - start) * 1000.0
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        return exc.code, parsed, elapsed


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    token = args.api_key.strip()
    if not token and args.api_key_file:
        token = Path(args.api_key_file).read_text(encoding="utf-8").strip()
    if not token:
        return {}
    if args.auth_header == "x-api-key":
        return {"X-API-Key": token}
    return {"Authorization": f"Bearer {token}"}


def token_count(base_url: str, content: str, timeout: float, headers: dict[str, str]) -> int:
    attempts = [
        {"content": content, "add_special": False},
        {"content": content},
        {"prompt": content},
    ]
    last_body: Any = None
    for payload in attempts:
        status, body, _ = http_json("POST", base_url.rstrip("/") + "/tokenize", payload, timeout=timeout, headers=headers)
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


def generate_prefix(base_url: str, target_tokens: int, timeout: float, headers: dict[str, str]) -> tuple[str, int, int]:
    unit_tokens = max(1, token_count(base_url, make_prefix(1), timeout, headers))
    repeats = max(1, math.ceil(target_tokens / unit_tokens))
    best_text = make_prefix(repeats)
    best_tokens = token_count(base_url, best_text, timeout, headers)
    for _ in range(8):
        if abs(best_tokens - target_tokens) <= max(256, int(target_tokens * 0.015)):
            break
        next_repeats = max(1, round(repeats * (target_tokens / max(1, best_tokens))))
        if next_repeats == repeats:
            next_repeats += 1 if best_tokens < target_tokens else -1
            next_repeats = max(1, next_repeats)
        repeats = next_repeats
        candidate = make_prefix(repeats)
        candidate_tokens = token_count(base_url, candidate, timeout, headers)
        if abs(candidate_tokens - target_tokens) <= abs(best_tokens - target_tokens):
            best_text, best_tokens = candidate, candidate_tokens
    return best_text, best_tokens, repeats


def cache_meta(response: dict[str, Any], key: str) -> dict[str, Any]:
    meta = response.get("cache_router") if isinstance(response.get("cache_router"), dict) else {}
    row = meta.get(key)
    if not isinstance(row, dict):
        raise RuntimeError(f"router response missing cache_router.{key}: {response}")
    return row


def route_times(use: dict[str, Any]) -> dict[str, float | int | str | bool | None]:
    hydrate = use.get("hydrate") if isinstance(use.get("hydrate"), dict) else {}
    restore = use.get("restore") if isinstance(use.get("restore"), dict) else {}
    completion = use.get("completion") if isinstance(use.get("completion"), dict) else {}
    timings = completion.get("timings") if isinstance(completion.get("timings"), dict) else {}
    return {
        "worker_id": use.get("worker_id"),
        "hydration_performed": hydrate.get("performed"),
        "hydrate_dest_existed_before": hydrate.get("dest_existed_before"),
        "hydrate_sha256_match": hydrate.get("sha256_match"),
        "hydrate_size_bytes": hydrate.get("size_bytes"),
        "hydrate_ms": hydrate.get("wall_ms"),
        "restore_ms": restore.get("wall_ms"),
        "n_restored": restore.get("n_restored"),
        "suffix_prompt_tokens": completion.get("tokens_evaluated"),
        "suffix_prompt_ms": timings.get("prompt_ms"),
        "generated_tokens": completion.get("tokens_predicted"),
        "eval_ms": timings.get("predicted_ms"),
        "completion_wall_ms": completion.get("wall_ms"),
        "content": completion.get("content"),
    }


def route_reductions(cold_prompt_ms: float, route: dict[str, Any]) -> dict[str, float | None]:
    prompt_ms = route.get("suffix_prompt_ms")
    restore_ms = route.get("restore_ms")
    hydrate_ms = route.get("hydrate_ms")
    out: dict[str, float | None] = {
        "prompt_only_reduction_percent": None,
        "restore_inclusive_reduction_percent": None,
        "hydrate_restore_inclusive_reduction_percent": None,
    }
    if isinstance(prompt_ms, (int, float)) and cold_prompt_ms:
        out["prompt_only_reduction_percent"] = 100.0 * (1.0 - float(prompt_ms) / cold_prompt_ms)
    if isinstance(prompt_ms, (int, float)) and isinstance(restore_ms, (int, float)) and cold_prompt_ms:
        out["restore_inclusive_reduction_percent"] = 100.0 * (1.0 - (float(prompt_ms) + float(restore_ms)) / cold_prompt_ms)
    if (
        isinstance(prompt_ms, (int, float))
        and isinstance(restore_ms, (int, float))
        and isinstance(hydrate_ms, (int, float))
        and cold_prompt_ms
    ):
        out["hydrate_restore_inclusive_reduction_percent"] = 100.0 * (
            1.0 - (float(prompt_ms) + float(restore_ms) + float(hydrate_ms)) / cold_prompt_ms
        )
    return out


def make_completion_body(*, cache_id: str, worker_id: str, mode: str, suffix_text: str, prefix_text: str = "", max_tokens: int = 8) -> dict[str, Any]:
    extension: dict[str, Any] = {
        "mode": mode,
        "cache_id": cache_id,
        "worker_id": worker_id,
        "suffix_text": suffix_text,
        "target": "suffix_route",
    }
    if prefix_text:
        extension["prefix_text"] = prefix_text
    return {
        "model": "Step-3.7",
        "prompt": suffix_text,
        "max_tokens": max_tokens,
        "temperature": 0,
        "cache_router": extension,
    }


def load_workers_file(path: str) -> list[dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = raw.get("workers") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError(f"workers file does not contain a workers list: {path}")
    return [row for row in rows if isinstance(row, dict)]


def find_worker_row(path: str, worker_id: str) -> dict[str, Any]:
    for row in load_workers_file(path):
        if row.get("worker_id") == worker_id:
            return row
    raise KeyError(f"worker_id {worker_id!r} not found in {path}")


def isolate_worker_slot(*, workers_file: str, worker_id: str, slot_filename: str, timeout: float) -> dict[str, Any]:
    if not slot_filename or "/" in slot_filename or slot_filename in {".", ".."}:
        raise ValueError(f"slot_filename must be a simple basename: {slot_filename!r}")
    row = find_worker_row(workers_file, worker_id)
    transport = row.get("transport") if isinstance(row.get("transport"), dict) else {}
    ssh_host = str(row.get("ssh_host") or transport.get("ssh_host") or row.get("worker_ssh_host") or "").strip()
    slot_dir = str(row.get("slot_save_path") or row.get("worker_slot_dir") or "").rstrip("/")
    if not ssh_host:
        raise ValueError(f"worker {worker_id} has no ssh_host in {workers_file}")
    if not slot_dir:
        raise ValueError(f"worker {worker_id} has no slot_save_path in {workers_file}")
    source = posixpath.join(slot_dir, slot_filename)
    isolated = posixpath.join(posixpath.dirname(slot_dir), "isolated-for-hydration", f"{int(time.time())}-{slot_filename}")
    before = remote_file_info(ssh_host, source, hash_file=True, timeout=timeout)
    move = remote_move_if_exists(ssh_host, source, isolated, timeout=timeout)
    after = remote_file_info(ssh_host, source, hash_file=False, timeout=timeout)
    return {
        "worker_id": worker_id,
        "ssh_host": ssh_host,
        "slot_filename": slot_filename,
        "worker_slot_path": source,
        "isolated_path": isolated,
        "before": before,
        "move": move,
        "after": after,
        "local_slot_absent_after_isolation": not bool(after.get("exists")),
    }


def write_readme(out_dir: Path, results: dict[str, Any]) -> None:
    build = results["cache_build"]
    native = results["routes"]["native_hot_source_worker"]
    hydrated = results["routes"]["hydrated_target_worker"]
    target_hot = results["routes"]["target_worker_hot_second_use"]
    forced = results["routes"].get("target_worker_forced_rehydrate_after_isolation")
    isolation = results.get("forced_target_local_miss")
    lines = [
        "# Two-Worker Cache Router Live Test",
        "",
        f"Created: `{results['created_utc']}`",
        "",
        "Cache build/use requests in this run went through the OpenAI-compatible",
        "router endpoint. When the forced local-miss option is enabled, the",
        "harness uses SSH only to move the synthetic POC target-worker slot file",
        "aside before the final cache use.",
        "",
        "## Topology",
        "",
        f"- Router base URL: `{results['router_base_url']}`",
        f"- Source worker: `{results['source_worker_id']}`",
        f"- Target worker: `{results['target_worker_id']}`",
        f"- Cache ID: `{results['cache_id']}`",
        f"- Prefix SHA256: `{results['prefix_sha256']}`",
        "",
        "## Cache Build",
        "",
        f"- Prefix tokens: `{build.get('prefix_tokens')}`",
        f"- Build prompt ms: `{build.get('build_prompt_ms')}`",
        f"- Slot SHA256: `{build.get('slot_file_sha256')}`",
        f"- Slot size bytes: `{build.get('slot_file_size_bytes')}`",
        f"- Source worker: `{build.get('worker_id')}`",
        "",
        "## Routes",
        "",
        "| route | worker | hydration | restore ms | suffix prompt tokens | suffix prompt ms | content |",
        "|---|---:|---:|---:|---:|---:|---|",
        f"| native hot source | `{native.get('worker_id')}` | `{native.get('hydration_performed')}` | `{native.get('restore_ms')}` | `{native.get('suffix_prompt_tokens')}` | `{native.get('suffix_prompt_ms')}` | `{native.get('content')}` |",
        f"| hydrated target | `{hydrated.get('worker_id')}` | `{hydrated.get('hydration_performed')}` | `{hydrated.get('restore_ms')}` | `{hydrated.get('suffix_prompt_tokens')}` | `{hydrated.get('suffix_prompt_ms')}` | `{hydrated.get('content')}` |",
        f"| target hot second use | `{target_hot.get('worker_id')}` | `{target_hot.get('hydration_performed')}` | `{target_hot.get('restore_ms')}` | `{target_hot.get('suffix_prompt_tokens')}` | `{target_hot.get('suffix_prompt_ms')}` | `{target_hot.get('content')}` |",
    ]
    if forced:
        lines.append(
            f"| target forced rehydrate | `{forced.get('worker_id')}` | `{forced.get('hydration_performed')}` | `{forced.get('restore_ms')}` | `{forced.get('suffix_prompt_tokens')}` | `{forced.get('suffix_prompt_ms')}` | `{forced.get('content')}` |"
        )
    lines.extend(
        [
            "",
            "## Forced Target Local-Miss Check",
            "",
        ]
    )
    if isolation:
        lines.extend(
            [
                f"- Target worker slot existed before isolation: `{isolation.get('before', {}).get('exists')}`",
                f"- Target worker slot absent after isolation: `{isolation.get('local_slot_absent_after_isolation')}`",
                f"- Isolated copy path recorded on worker: `{isolation.get('isolated_path')}`",
                f"- Forced route hydration performed: `{forced.get('hydration_performed') if forced else None}`",
                f"- Forced route hydration SHA256 matched: `{forced.get('hydrate_sha256_match') if forced else None}`",
            ]
        )
    else:
        lines.append("- Not requested for this run.")
    lines.extend(
        [
            "",
            "## Reductions",
            "",
            f"- Native hot source reductions: `{results['reductions']['native_hot_source_worker']}`",
            f"- Hydrated target reductions: `{results['reductions']['hydrated_target_worker']}`",
            f"- Target hot second-use reductions: `{results['reductions']['target_worker_hot_second_use']}`",
            f"- Target forced-rehydrate reductions: `{results['reductions'].get('target_worker_forced_rehydrate_after_isolation')}`",
            f"- Hydrated target reached 90% prompt-only target: `{results['target_reached']['hydrated_prompt_only_90']}`",
            f"- Hydrated target reached 90% hydrate+restore-inclusive target: `{results['target_reached']['hydrated_hri_90']}`",
            f"- Target hot route within margin of native hot route: `{results['target_reached']['target_hot_within_native_margin']}`",
            f"- Forced target local-miss rehydrated from durable store: `{results['target_reached'].get('forced_rehydrate_after_isolation')}`",
            "",
            "## Caveats",
            "",
            "- This proves one router with two live workers and HTTP sidecar hydration on the current LAN.",
            "- It is still a trusted-LAN MVP, not untrusted-network hardening, eviction, tenant isolation, or concurrent scheduling.",
            "- The current strict key uses observed runtime metadata and slot hashes; full model tensor hash and logits/top-k restore validation remain future hardening.",
            "",
        ]
    )
    (out_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="", help="Router base URL, for example http://<router-lan-ip>:18080.")
    parser.add_argument("--api-key", default="", help="Optional router API key/bearer token for authenticated routers.")
    parser.add_argument("--api-key-file", default="", help="Optional file containing the router API key/bearer token.")
    parser.add_argument("--auth-header", choices=["bearer", "x-api-key"], default="bearer", help="Header style for --api-key.")
    parser.add_argument("--source-worker-id", default="worker-a")
    parser.add_argument("--target-worker-id", default="worker-b")
    parser.add_argument("--target-tokens", type=int, default=30000)
    parser.add_argument("--cache-id", default="")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--native-margin-ratio", type=float, default=2.0)
    parser.add_argument("--workers-file", default=str(PACKAGE_ROOT / "configs" / "cache-router" / "workers.example.json"))
    parser.add_argument(
        "--force-target-local-miss",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Move the target worker's local slot aside after hot use, then prove durable-store rehydration through the router.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.base_url:
        raise SystemExit("--base-url is required for the live two-worker test")
    created = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cache_id = args.cache_id or f"two-worker-{created}"
    out_dir = Path(args.out_dir or PACKAGE_ROOT / "data" / "cache_router_poc" / f"{datetime.now().strftime('%Y-%m-%d')}-two-worker-router")
    out_dir.mkdir(parents=True, exist_ok=True)
    base = args.base_url.rstrip("/")
    headers = auth_headers(args)
    prefix, prefix_tokens, repeats = generate_prefix(base, args.target_tokens, args.timeout, headers)
    prefix_hash = sha256_text(prefix)

    health_status, health, health_ms = http_json("GET", base + "/health", timeout=args.timeout, headers=headers)
    workers_status, workers, workers_ms = http_json("GET", base + "/router/workers", timeout=args.timeout, headers=headers)
    if health_status != 200 or workers_status != 200:
        raise RuntimeError(f"router not healthy: health={health_status} workers={workers_status}")

    build_status, build_response, build_wall_ms = http_json(
        "POST",
        base + "/v1/completions",
        make_completion_body(
            cache_id=cache_id,
            worker_id=args.source_worker_id,
            mode="refresh",
            prefix_text=prefix,
            suffix_text="",
            max_tokens=1,
        ),
        timeout=args.timeout,
        headers=headers,
    )
    if build_status != 200:
        raise RuntimeError(f"cache build failed HTTP {build_status}: {build_response}")
    build = cache_meta(build_response, "build")
    slot_filename = str(build.get("slot_filename") or "")

    native_status, native_response, native_wall_ms = http_json(
        "POST",
        base + "/v1/completions",
        make_completion_body(
            cache_id=cache_id,
            worker_id=args.source_worker_id,
            mode="use",
            suffix_text="Reply with exactly: native cache ok",
            max_tokens=8,
        ),
        timeout=args.timeout,
        headers=headers,
    )
    if native_status != 200:
        raise RuntimeError(f"native source cache use failed HTTP {native_status}: {native_response}")
    native = route_times(cache_meta(native_response, "use"))

    hydrated_status, hydrated_response, hydrated_wall_ms = http_json(
        "POST",
        base + "/v1/completions",
        make_completion_body(
            cache_id=cache_id,
            worker_id=args.target_worker_id,
            mode="use",
            suffix_text="Reply with exactly: remote cache ok",
            max_tokens=8,
        ),
        timeout=args.timeout,
        headers=headers,
    )
    if hydrated_status != 200:
        raise RuntimeError(f"target worker cache use failed HTTP {hydrated_status}: {hydrated_response}")
    hydrated = route_times(cache_meta(hydrated_response, "use"))

    hot_status, hot_response, hot_wall_ms = http_json(
        "POST",
        base + "/v1/completions",
        make_completion_body(
            cache_id=cache_id,
            worker_id=args.target_worker_id,
            mode="use",
            suffix_text="Reply with exactly: remote hot cache ok",
            max_tokens=8,
        ),
        timeout=args.timeout,
        headers=headers,
    )
    if hot_status != 200:
        raise RuntimeError(f"target worker hot cache use failed HTTP {hot_status}: {hot_response}")
    target_hot = route_times(cache_meta(hot_response, "use"))

    forced_isolation: dict[str, Any] | None = None
    forced_rehydrate: dict[str, Any] | None = None
    forced_wall_ms: float | None = None
    if args.force_target_local_miss:
        forced_isolation = isolate_worker_slot(
            workers_file=args.workers_file,
            worker_id=args.target_worker_id,
            slot_filename=slot_filename,
            timeout=args.timeout,
        )
        forced_status, forced_response, forced_wall_ms = http_json(
            "POST",
            base + "/v1/completions",
            make_completion_body(
                cache_id=cache_id,
                worker_id=args.target_worker_id,
                mode="use",
                suffix_text="Reply with exactly: forced rehydrate ok",
                max_tokens=8,
            ),
            timeout=args.timeout,
            headers=headers,
        )
        if forced_status != 200:
            raise RuntimeError(f"target worker forced rehydrate failed HTTP {forced_status}: {forced_response}")
        forced_rehydrate = route_times(cache_meta(forced_response, "use"))

    decisions_status, decisions, decisions_ms = http_json("GET", base + "/router/decisions", timeout=args.timeout, headers=headers)
    cold_prompt_ms = float(build.get("build_prompt_ms") or 0.0)
    reductions = {
        "native_hot_source_worker": route_reductions(cold_prompt_ms, native),
        "hydrated_target_worker": route_reductions(cold_prompt_ms, hydrated),
        "target_worker_hot_second_use": route_reductions(cold_prompt_ms, target_hot),
    }
    if forced_rehydrate:
        reductions["target_worker_forced_rehydrate_after_isolation"] = route_reductions(cold_prompt_ms, forced_rehydrate)
    native_route_ms = (native.get("restore_ms") or 0) + (native.get("suffix_prompt_ms") or 0)
    target_hot_route_ms = (target_hot.get("restore_ms") or 0) + (target_hot.get("suffix_prompt_ms") or 0)
    target_hot_ratio = float(target_hot_route_ms) / float(native_route_ms) if native_route_ms else None
    results = {
        "schema_version": "2026-07-01.1",
        "created_utc": now_iso(),
        "router_base_url": base,
        "router_auth": {"configured": bool(headers), "header": args.auth_header if headers else "none"},
        "source_worker_id": args.source_worker_id,
        "target_worker_id": args.target_worker_id,
        "cache_id": cache_id,
        "target_tokens": args.target_tokens,
        "prefix_tokens": prefix_tokens,
        "prefix_repeats": repeats,
        "prefix_sha256": prefix_hash,
        "router_health": {"status": health_status, "wall_ms": health_ms, "body": health},
        "router_workers": {"status": workers_status, "wall_ms": workers_ms, "body": workers},
        "cache_build": build,
        "request_walls_ms": {
            "build": build_wall_ms,
            "native_hot_source_worker": native_wall_ms,
            "hydrated_target_worker": hydrated_wall_ms,
            "target_worker_hot_second_use": hot_wall_ms,
            "decisions": decisions_ms,
        },
        "routes": {
            "native_hot_source_worker": native,
            "hydrated_target_worker": hydrated,
            "target_worker_hot_second_use": target_hot,
        },
        "reductions": reductions,
        "target_hot_vs_native_hot_ratio": target_hot_ratio,
        "target_reached": {
            "hydrated_prompt_only_90": (reductions["hydrated_target_worker"].get("prompt_only_reduction_percent") or 0) >= 90.0,
            "hydrated_hri_90": (reductions["hydrated_target_worker"].get("hydrate_restore_inclusive_reduction_percent") or 0) >= 90.0,
            "target_hot_within_native_margin": target_hot_ratio is not None and target_hot_ratio <= args.native_margin_ratio,
            "native_margin_ratio": args.native_margin_ratio,
        },
        "decisions_tail": decisions if decisions_status == 200 else {"status": decisions_status, "body": decisions},
    }
    if forced_isolation:
        results["forced_target_local_miss"] = forced_isolation
    if forced_rehydrate:
        results["request_walls_ms"]["target_worker_forced_rehydrate_after_isolation"] = forced_wall_ms
        results["routes"]["target_worker_forced_rehydrate_after_isolation"] = forced_rehydrate
        forced_reductions = reductions["target_worker_forced_rehydrate_after_isolation"]
        results["target_reached"]["forced_rehydrate_after_isolation"] = (
            bool(forced_isolation and forced_isolation.get("local_slot_absent_after_isolation"))
            and forced_rehydrate.get("hydration_performed") is True
            and forced_rehydrate.get("hydrate_sha256_match") is True
            and (forced_reductions.get("prompt_only_reduction_percent") or 0) >= 90.0
        )
        results["target_reached"]["forced_rehydrate_hri_90"] = (
            (forced_reductions.get("hydrate_restore_inclusive_reduction_percent") or 0) >= 90.0
        )
    write_json(out_dir / "results.json", results)
    write_readme(out_dir, results)
    print(json_dumps(results))
    ok = all(
        [
            results["target_reached"]["hydrated_prompt_only_90"],
            results["target_reached"]["hydrated_hri_90"],
            results["target_reached"]["target_hot_within_native_margin"],
            results["target_reached"].get("forced_rehydrate_after_isolation", True),
        ]
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
