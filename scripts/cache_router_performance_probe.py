#!/usr/bin/env python3
"""Offline performance probes for Cachy Router.

The probes use loopback fake workers and a temporary local cache root. They do
not contact private hosts and do not prove live model-generation performance.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import sys
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

import cache_router_daemon  # noqa: E402
import cache_router_daemon_smoke_test as smoke  # noqa: E402


class QuietRouterHandler(cache_router_daemon.RouterHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return


def percentile(samples: list[float], pct: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]


def stats(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {"min_ms": 0.0, "median_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
    return {
        "min_ms": min(samples),
        "median_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "max_ms": max(samples),
    }


def timed_request(url: str, *, headers: dict[str, str], body: dict[str, Any]) -> tuple[float, int, dict[str, str], bytes]:
    started = time.perf_counter()
    status, response_headers, raw = smoke.request("POST", url, headers=headers, body=body)
    return (time.perf_counter() - started) * 1000.0, status, response_headers, raw


def assert_completion_ok(status: int, raw: bytes, label: str) -> None:
    if status != 200:
        raise AssertionError(f"{label} returned HTTP {status}: {raw[:200]!r}")
    body = json.loads(raw.decode("utf-8"))
    text = body.get("choices", [{}])[0].get("text")
    if text != "ok":
        raise AssertionError(f"{label} returned unexpected completion text: {text!r}")


def run_target_concurrency_probe(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-perf-concurrency-") as tmp:
        root = Path(tmp)
        worker_states = {
            "worker-main": smoke.FakeWorkerState(),
            "worker-backup": smoke.FakeWorkerState(),
        }
        workers = {
            worker_id: smoke.start_server(smoke.FakeWorkerHandler, state_attr="fake_state", state=worker_state)
            for worker_id, worker_state in worker_states.items()
        }
        router: ThreadingHTTPServer | None = None
        router_state: cache_router_daemon.CacheRouterState | None = None
        try:
            for worker_state in worker_states.values():
                worker_state.completion_delay_seconds = args.concurrent_worker_delay_seconds
            worker_urls = {
                worker_id: f"http://{server.server_address[0]}:{server.server_address[1]}" for worker_id, server in workers.items()
            }
            router_args = smoke.router_args(worker_urls["worker-main"], root / "cache", root / "worker-main-slots")
            router_args.readiness_poll_interval = 0.0
            router_args.readiness_timeout = 2.0
            router_args.n_parallel = args.concurrent_active_slots
            router_args.n_seq_max = args.concurrent_active_slots
            router_args.queue_limit_per_worker = args.concurrent_queue_limit_per_worker
            router_args.queue_wait_timeout = args.concurrent_queue_wait_timeout
            workers_file = root / "workers.json"
            worker_rows = []
            for worker_id, worker_url in worker_urls.items():
                row = smoke.inventory_worker_entry(worker_id, worker_url, root / f"{worker_id}-slots")
                row["n_parallel"] = args.concurrent_active_slots
                row["n_seq_max"] = args.concurrent_active_slots
                worker_rows.append(row)
            workers_file.write_text(json.dumps({"workers": worker_rows}, sort_keys=True), encoding="utf-8")
            router_args.workers_file = str(workers_file)
            router_state = cache_router_daemon.CacheRouterState(router_args)
            router_state.poll_readiness_once()
            router = ThreadingHTTPServer(("127.0.0.1", 0), QuietRouterHandler)
            router.state = router_state  # type: ignore[attr-defined]
            router_thread = threading.Thread(target=router.serve_forever, daemon=True)
            router_thread.start()
            router_url = f"http://{router.server_address[0]}:{router.server_address[1]}"
            headers = {"Authorization": "Bearer secret-token"}
            worker_ids = sorted(worker_urls)
            target_requests = len(worker_ids) * args.concurrent_active_slots
            barrier = threading.Barrier(target_requests)

            def concurrent_request(index: int) -> dict[str, Any]:
                worker_id = worker_ids[index % len(worker_ids)]
                body = {
                    "model": "fake-model",
                    "prompt": f"offline target concurrency {index}",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "bypass",
                        "cache_id": f"target-concurrency-{index}",
                        "worker_id": worker_id,
                        "allow_fallback": False,
                    },
                }
                started = time.perf_counter()
                try:
                    barrier.wait(timeout=5.0)
                    status, response_headers, raw = smoke.request("POST", router_url + "/v1/completions", headers=headers, body=body)
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    decoded: dict[str, Any]
                    try:
                        decoded = json.loads(raw.decode("utf-8"))
                    except Exception:  # noqa: BLE001
                        decoded = {}
                    error_type = decoded.get("error", {}).get("type") if isinstance(decoded.get("error"), dict) else None
                    success = (
                        status == 200
                        and decoded.get("choices", [{}])[0].get("text") == "ok"
                        and response_headers.get("x-cache-router-worker") == worker_id
                    )
                    bounded_error = status in {429, 503} and error_type in {"rate_limit_error", "service_unavailable"}
                    return {
                        "index": index,
                        "target_worker_id": worker_id,
                        "status": status,
                        "success": success,
                        "bounded_error": bounded_error,
                        "elapsed_ms": elapsed_ms,
                        "request_id": response_headers.get("x-cache-router-request-id", ""),
                        "selected_worker_id": response_headers.get("x-cache-router-worker", ""),
                        "error_type": error_type,
                    }
                except Exception as exc:  # noqa: BLE001
                    return {
                        "index": index,
                        "target_worker_id": worker_id,
                        "status": None,
                        "success": False,
                        "bounded_error": False,
                        "elapsed_ms": (time.perf_counter() - started) * 1000.0,
                        "exception": f"{type(exc).__name__}: {exc}",
                    }

            with concurrent.futures.ThreadPoolExecutor(max_workers=target_requests) as executor:
                results = list(executor.map(concurrent_request, range(target_requests)))

            exceptions = [row for row in results if row.get("exception")]
            successes = [row for row in results if row.get("success")]
            bounded_errors = [row for row in results if row.get("bounded_error")]
            for row in successes:
                request_id = str(row.get("request_id") or "")
                status, _, raw = smoke.request("GET", router_url + f"/router/decisions?request_id={request_id}", headers=headers)
                events = json.loads(raw.decode("utf-8")).get("events", []) if status == 200 else []
                if status != 200 or len(events) != 1 or events[0].get("worker_id") != row.get("target_worker_id"):
                    raise AssertionError(f"concurrent request {row.get('index')} has no matching decision event")

            with router_state.metrics_lock:
                max_active_requests = int(router_state.metrics.get("max_active_requests", 0))
            active_after = {worker_id: router_state.worker_active_count(worker_id) for worker_id in worker_ids}
            queued_after = {worker_id: router_state.worker_queue_depth(worker_id) for worker_id in worker_ids}
            worker_request_counts = {worker_id: len(worker_states[worker_id].requests) for worker_id in worker_ids}
            all_responses_bounded = len(successes) + len(bounded_errors) == target_requests
            all_target_capacity_succeeded = len(successes) == target_requests
            return {
                "ok": bool(not exceptions and all_responses_bounded and all_target_capacity_succeeded and max_active_requests >= target_requests),
                "workers": len(worker_ids),
                "active_slots_per_worker": args.concurrent_active_slots,
                "target_requests": target_requests,
                "successful_completions": len(successes),
                "bounded_errors": len(bounded_errors),
                "client_exceptions": len(exceptions),
                "max_active_requests": max_active_requests,
                "worker_request_counts": worker_request_counts,
                "active_after": active_after,
                "queued_after": queued_after,
                "results": results,
                "scope": "offline loopback explicit-worker normal completions at N workers * configured active slots; not a production load or soak test",
            }
        finally:
            if router is not None:
                router.shutdown()
                router.server_close()
            if router_state is not None:
                router_state.close()
            for worker in workers.values():
                worker.shutdown()
                worker.server_close()


def run_pass_through_probe(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-perf-proxy-") as tmp:
        root = Path(tmp)
        worker_state = smoke.FakeWorkerState()
        worker = smoke.start_server(smoke.FakeWorkerHandler, state_attr="fake_state", state=worker_state)
        router: ThreadingHTTPServer | None = None
        router_state: cache_router_daemon.CacheRouterState | None = None
        try:
            worker_host, worker_port = worker.server_address[:2]
            worker_url = f"http://{worker_host}:{worker_port}"
            router_args = smoke.router_args(worker_url, root / "cache", root / "worker-slots")
            router_args.readiness_poll_interval = 60.0
            router_args.readiness_timeout = 2.0
            router_state = cache_router_daemon.CacheRouterState(router_args)
            router_state.poll_readiness_once()
            router = ThreadingHTTPServer(("127.0.0.1", 0), QuietRouterHandler)
            router.state = router_state  # type: ignore[attr-defined]
            router_thread = threading.Thread(target=router.serve_forever, daemon=True)
            router_thread.start()
            router_host, router_port = router.server_address[:2]
            router_url = f"http://{router_host}:{router_port}"
            headers = {"Authorization": "Bearer secret-token"}
            body = {"model": "fake-model", "prompt": "offline performance probe", "max_tokens": 1}

            for _ in range(args.warmup_requests):
                direct_ms, direct_status, _, direct_raw = timed_request(worker_url + "/v1/completions", headers={}, body=body)
                assert_completion_ok(direct_status, direct_raw, "warmup direct worker")
                routed_ms, routed_status, _, routed_raw = timed_request(router_url + "/v1/completions", headers=headers, body=body)
                assert_completion_ok(routed_status, routed_raw, "warmup router")
                _ = direct_ms + routed_ms

            direct_samples: list[float] = []
            routed_samples: list[float] = []
            overhead_samples: list[float] = []
            for _ in range(args.requests):
                direct_ms, direct_status, _, direct_raw = timed_request(worker_url + "/v1/completions", headers={}, body=body)
                assert_completion_ok(direct_status, direct_raw, "direct worker")
                routed_ms, routed_status, routed_headers, routed_raw = timed_request(router_url + "/v1/completions", headers=headers, body=body)
                assert_completion_ok(routed_status, routed_raw, "router")
                if routed_headers.get("x-cache-router-worker") != "worker-main":
                    raise AssertionError(f"router selected unexpected worker: {routed_headers.get('x-cache-router-worker')!r}")
                direct_samples.append(direct_ms)
                routed_samples.append(routed_ms)
                overhead_samples.append(max(0.0, routed_ms - direct_ms))

            return {
                "ok": percentile(overhead_samples, 95) <= args.router_overhead_threshold_ms,
                "threshold_ms": args.router_overhead_threshold_ms,
                "requests": args.requests,
                "warmup_requests": args.warmup_requests,
                "direct_worker": stats(direct_samples),
                "router": stats(routed_samples),
                "router_overhead": stats(overhead_samples),
                "scope": "loopback fake-worker normal /v1/completions; excludes model generation but includes router selection/proxy cost",
            }
        finally:
            if router is not None:
                router.shutdown()
                router.server_close()
            if router_state is not None:
                router_state.close()
            worker.shutdown()
            worker.server_close()


def run_lookup_probe(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-perf-lookup-") as tmp:
        root = Path(tmp)
        worker_state = smoke.FakeWorkerState()
        worker = smoke.start_server(smoke.FakeWorkerHandler, state_attr="fake_state", state=worker_state)
        router_state: cache_router_daemon.CacheRouterState | None = None
        try:
            worker_host, worker_port = worker.server_address[:2]
            worker_url = f"http://{worker_host}:{worker_port}"
            router_args = smoke.router_args(worker_url, root / "cache", root / "worker-slots")
            router_args.readiness_poll_interval = 0.0
            router_state = cache_router_daemon.CacheRouterState(router_args)
            lookup_ids = [f"lookup-{index:04d}" for index in range(args.lookup_entries)]
            for cache_id in lookup_ids:
                smoke.seed_cache_entry(router_state, cache_id, hot_local=True, durable_blob=True)
            target_cache_id = lookup_ids[-1]
            policy = cache_router_daemon.default_cache_policy()

            for _ in range(args.warmup_requests):
                loaded = router_state.ensure_entry(target_cache_id, cache_policy=policy)
                if loaded["manifest"].get("cache_id") != target_cache_id:
                    raise AssertionError("warmup cache lookup returned the wrong manifest")

            samples: list[float] = []
            for _ in range(args.lookup_iterations):
                started = time.perf_counter()
                loaded = router_state.ensure_entry(target_cache_id, cache_policy=policy)
                samples.append((time.perf_counter() - started) * 1000.0)
                if loaded["entry"].get("cache_id") != target_cache_id or loaded["manifest"].get("cache_id") != target_cache_id:
                    raise AssertionError("cache lookup returned the wrong registry entry or manifest")

            lookup_stats = stats(samples)
            return {
                "ok": lookup_stats["p95_ms"] <= args.cache_lookup_threshold_ms,
                "threshold_ms": args.cache_lookup_threshold_ms,
                "iterations": args.lookup_iterations,
                "warmup_requests": args.warmup_requests,
                "registry_entries": args.lookup_entries,
                "cache_lookup": lookup_stats,
                "scope": "local registry.json plus manifest JSON validation through CacheRouterState.ensure_entry",
            }
        finally:
            if router_state is not None:
                router_state.close()
            worker.shutdown()
            worker.server_close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--requests", type=int, default=80, help="Measured direct/router request pairs for pass-through overhead.")
    parser.add_argument("--lookup-iterations", type=int, default=200, help="Measured local registry lookup iterations.")
    parser.add_argument("--lookup-entries", type=int, default=32, help="Seeded registry entries before measuring lookup.")
    parser.add_argument("--warmup-requests", type=int, default=10, help="Warmup iterations before each measured probe.")
    parser.add_argument("--router-overhead-threshold-ms", type=float, default=50.0, help="Maximum allowed p95 router overhead in milliseconds.")
    parser.add_argument("--cache-lookup-threshold-ms", type=float, default=25.0, help="Maximum allowed p95 local cache lookup latency in milliseconds.")
    parser.add_argument("--concurrent-active-slots", type=int, default=2, help="Configured active slots per loopback worker for the concurrency probe.")
    parser.add_argument("--concurrent-queue-limit-per-worker", type=int, default=2, help="Router queue depth per worker for the concurrency probe.")
    parser.add_argument("--concurrent-queue-wait-timeout", type=float, default=3.0, help="Queue wait timeout in seconds for the concurrency probe.")
    parser.add_argument("--concurrent-worker-delay-seconds", type=float, default=0.2, help="Fake worker completion delay used to force overlapping requests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (
        args.requests <= 0
        or args.lookup_iterations <= 0
        or args.lookup_entries <= 0
        or args.warmup_requests < 0
        or args.concurrent_active_slots <= 0
        or args.concurrent_queue_limit_per_worker < 0
        or args.concurrent_queue_wait_timeout <= 0
        or args.concurrent_worker_delay_seconds <= 0
    ):
        raise SystemExit("probe counts, timeouts, and concurrency settings must be positive")
    pass_through = run_pass_through_probe(args)
    lookup = run_lookup_probe(args)
    target_concurrency = run_target_concurrency_probe(args)
    result = {
        "ok": bool(pass_through["ok"] and lookup["ok"] and target_concurrency["ok"]),
        "pass_through_overhead": pass_through,
        "cache_lookup_latency": lookup,
        "target_concurrency": target_concurrency,
        "notes": [
            "offline loopback benchmark only",
            "does not contact private hosts",
            "does not prove live model-generation, distributed-cache performance, production load balancing, or long-soak behavior",
        ],
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
