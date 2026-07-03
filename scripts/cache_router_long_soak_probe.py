#!/usr/bin/env python3
"""Offline long-soak probe for Cachy Router.

The default duration is an 8-hour acceptance run using only loopback fake
workers. A short --self-test mode exists for make check; it proves the harness
starts, samples RSS, routes requests, and computes thresholds, but it is not
long-soak evidence.
"""

from __future__ import annotations

import argparse
import json
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


ACCEPTANCE_DURATION_SECONDS = 8 * 3600.0
ACCEPTANCE_BASELINE_AFTER_SECONDS = 3600.0


class QuietRouterHandler(cache_router_daemon.RouterHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return


def rss_bytes() -> int | None:
    status_path = Path("/proc/self/status")
    if not status_path.is_file():
        return None
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return int(parts[1]) * 1024
                except ValueError:
                    return None
    return None


def summarize_growth(samples: list[dict[str, Any]], *, baseline_after_seconds: float) -> dict[str, Any]:
    rss_samples = [row for row in samples if isinstance(row.get("rss_bytes"), int)]
    if not rss_samples:
        return {
            "baseline_sample_found": False,
            "post_baseline_window_ok": False,
            "baseline_rss_bytes": None,
            "final_rss_bytes": None,
            "final_rss_multiplier": None,
            "growth_per_hour": None,
        }
    baseline_match = next((row for row in rss_samples if float(row["elapsed_seconds"]) >= baseline_after_seconds), None)
    baseline = baseline_match or rss_samples[0]
    final = rss_samples[-1]
    baseline_rss = int(baseline["rss_bytes"])
    final_rss = int(final["rss_bytes"])
    elapsed_hours = max((float(final["elapsed_seconds"]) - float(baseline["elapsed_seconds"])) / 3600.0, 0.0)
    growth_ratio = (final_rss - baseline_rss) / baseline_rss if baseline_rss > 0 else 0.0
    growth_per_hour = growth_ratio / elapsed_hours if elapsed_hours > 0 else 0.0
    return {
        "baseline_sample_found": baseline_match is not None,
        "post_baseline_window_ok": float(final["elapsed_seconds"]) > float(baseline["elapsed_seconds"]),
        "baseline_elapsed_seconds": baseline["elapsed_seconds"],
        "baseline_rss_bytes": baseline_rss,
        "final_elapsed_seconds": final["elapsed_seconds"],
        "final_rss_bytes": final_rss,
        "final_rss_multiplier": final_rss / baseline_rss if baseline_rss > 0 else None,
        "growth_per_hour": growth_per_hour,
    }


def evaluate_soak_result(
    *,
    samples: list[dict[str, Any]],
    success_count: int,
    bounded_error_count: int,
    failure_count: int,
    duration_seconds: float,
    baseline_after_seconds: float,
    max_rss_growth_per_hour: float,
    final_rss_multiplier: float,
) -> dict[str, Any]:
    growth = summarize_growth(samples, baseline_after_seconds=baseline_after_seconds)
    final_multiplier = growth.get("final_rss_multiplier")
    growth_per_hour = growth.get("growth_per_hour")
    actual_duration_seconds = max((float(row.get("elapsed_seconds", 0.0)) for row in samples), default=0.0)
    duration_ok = actual_duration_seconds >= ACCEPTANCE_DURATION_SECONDS
    baseline_delay_ok = baseline_after_seconds >= ACCEPTANCE_BASELINE_AFTER_SECONDS
    acceptance_run_requested = (
        duration_seconds >= ACCEPTANCE_DURATION_SECONDS
        or baseline_after_seconds >= ACCEPTANCE_BASELINE_AFTER_SECONDS
    )
    baseline_sample_found = growth.get("baseline_sample_found") is True
    post_baseline_window_ok = growth.get("post_baseline_window_ok") is True
    final_rss_ok = final_multiplier is not None and final_multiplier <= final_rss_multiplier
    rss_growth_ok = growth_per_hour is not None and growth_per_hour <= max_rss_growth_per_hour
    zero_failures_ok = failure_count == 0 and bounded_error_count == 0
    threshold_ok = final_rss_ok and rss_growth_ok
    full_acceptance_duration = (
        duration_ok
        and baseline_delay_ok
        and baseline_sample_found
        and post_baseline_window_ok
    )
    acceptance_ok = (
        full_acceptance_duration
        and success_count > 0
        and zero_failures_ok
        and threshold_ok
    )
    harness_smoke_ok = (
        failure_count == 0
        and success_count > 0
        and bool(samples)
    )
    return {
        "ok": acceptance_ok if acceptance_run_requested else harness_smoke_ok,
        "acceptance_ok": acceptance_ok,
        "acceptance_run_requested": acceptance_run_requested,
        "harness_smoke_ok": harness_smoke_ok,
        "full_acceptance_duration": full_acceptance_duration,
        "actual_duration_seconds": actual_duration_seconds,
        "duration_ok": duration_ok,
        "baseline_delay_ok": baseline_delay_ok,
        "baseline_sample_found": baseline_sample_found,
        "post_baseline_window_ok": post_baseline_window_ok,
        "zero_failures_ok": zero_failures_ok,
        "success_count": success_count,
        "bounded_error_count": bounded_error_count,
        "failure_count": failure_count,
        "threshold_ok": threshold_ok,
        "rss_growth_ok": rss_growth_ok,
        "final_rss_ok": final_rss_ok,
        "rss": growth,
    }


def run_acceptance_math_self_test() -> dict[str, Any]:
    baseline = {"elapsed_seconds": 3600.0, "rss_bytes": 100_000_000}
    good_final = {"elapsed_seconds": 28800.0, "rss_bytes": 107_000_000}
    high_growth_final = {"elapsed_seconds": 28800.0, "rss_bytes": 108_000_000}
    high_multiplier_final = {"elapsed_seconds": 28800.0, "rss_bytes": 111_000_000}
    no_rss_samples = [{"elapsed_seconds": 3600.0, "rss_bytes": None}, {"elapsed_seconds": 28800.0, "rss_bytes": None}]

    cases = [
        {
            "name": "full_acceptance_pass",
            "expected_acceptance_ok": True,
            "kwargs": {
                "samples": [baseline, good_final],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "short_smoke_is_not_acceptance",
            "expected_acceptance_ok": False,
            "expected_ok": True,
            "kwargs": {
                "samples": [{"elapsed_seconds": 0.0, "rss_bytes": 100_000_000}, {"elapsed_seconds": 1.0, "rss_bytes": 100_000_000}],
                "success_count": 1,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": 1.0,
                "baseline_after_seconds": 0.0,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "failure_count_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [baseline, good_final],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 1,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "rss_growth_per_hour_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [baseline, high_growth_final],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "final_rss_multiplier_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [baseline, high_multiplier_final],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "missing_rss_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": no_rss_samples,
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "baseline_delay_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [baseline, good_final],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": 0.0,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "missing_baseline_sample_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [{"elapsed_seconds": 0.0, "rss_bytes": 100_000_000}, {"elapsed_seconds": 28800.0, "rss_bytes": 107_000_000}],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "bounded_error_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [baseline, good_final],
                "success_count": 100,
                "bounded_error_count": 1,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
        {
            "name": "requested_duration_without_actual_elapsed_blocks_acceptance",
            "expected_acceptance_ok": False,
            "kwargs": {
                "samples": [{"elapsed_seconds": 3600.0, "rss_bytes": 100_000_000}, {"elapsed_seconds": 7200.0, "rss_bytes": 100_500_000}],
                "success_count": 100,
                "bounded_error_count": 0,
                "failure_count": 0,
                "duration_seconds": ACCEPTANCE_DURATION_SECONDS,
                "baseline_after_seconds": ACCEPTANCE_BASELINE_AFTER_SECONDS,
                "max_rss_growth_per_hour": 0.01,
                "final_rss_multiplier": 1.10,
            },
        },
    ]
    results = []
    for case in cases:
        observed = evaluate_soak_result(**case["kwargs"])
        expected_ok = case.get("expected_ok", case["expected_acceptance_ok"])
        passed = (
            observed["acceptance_ok"] is case["expected_acceptance_ok"]
            and observed["ok"] is expected_ok
        )
        results.append(
            {
                "name": case["name"],
                "passed": passed,
                "ok": observed["ok"],
                "acceptance_ok": observed["acceptance_ok"],
                "acceptance_run_requested": observed["acceptance_run_requested"],
                "full_acceptance_duration": observed["full_acceptance_duration"],
                "duration_ok": observed["duration_ok"],
                "baseline_sample_found": observed["baseline_sample_found"],
                "zero_failures_ok": observed["zero_failures_ok"],
                "threshold_ok": observed["threshold_ok"],
            }
        )
    return {
        "ok": all(row["passed"] for row in results),
        "checks": len(results),
        "passed": sum(1 for row in results if row["passed"]),
        "cases": results,
    }


def request_once(router_url: str, headers: dict[str, str], index: int) -> dict[str, Any]:
    status, response_headers, raw = smoke.request(
        "POST",
        router_url + "/v1/completions",
        headers=headers,
        body={
            "model": "fake-model",
            "prompt": f"long soak loopback request {index}",
            "max_tokens": 1,
        },
    )
    body: dict[str, Any]
    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:  # noqa: BLE001
        body = {}
    text = body.get("choices", [{}])[0].get("text") if isinstance(body.get("choices"), list) else None
    error_type = body.get("error", {}).get("type") if isinstance(body.get("error"), dict) else None
    return {
        "status": status,
        "success": status == 200 and text == "ok" and response_headers.get("x-cache-router-worker") not in {"", "none"},
        "bounded_error": status in {429, 503} and error_type in {"rate_limit_error", "service_unavailable"},
        "worker_id": response_headers.get("x-cache-router-worker", ""),
        "request_id": response_headers.get("x-cache-router-request-id", ""),
        "error_type": error_type,
    }


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-long-soak-") as tmp:
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
            worker_urls = {
                worker_id: f"http://{server.server_address[0]}:{server.server_address[1]}" for worker_id, server in workers.items()
            }
            for worker_state in worker_states.values():
                worker_state.completion_delay_seconds = args.worker_delay_seconds

            router_args = smoke.router_args(worker_urls["worker-main"], root / "cache", root / "worker-main-slots")
            router_args.readiness_poll_interval = 0.0
            router_args.readiness_timeout = 2.0
            router_args.n_parallel = args.active_slots
            router_args.n_seq_max = args.active_slots
            router_args.queue_limit_per_worker = args.queue_limit_per_worker
            router_args.queue_wait_timeout = args.queue_wait_timeout
            workers_file = root / "workers.json"
            worker_rows = []
            for worker_id, worker_url in worker_urls.items():
                row = smoke.inventory_worker_entry(worker_id, worker_url, root / f"{worker_id}-slots")
                row["n_parallel"] = args.active_slots
                row["n_seq_max"] = args.active_slots
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

            started = time.perf_counter()
            next_request_at = started
            next_sample_at = started
            request_index = 0
            failures: list[dict[str, Any]] = []
            success_count = 0
            bounded_error_count = 0
            samples: list[dict[str, Any]] = []
            selected_workers: dict[str, int] = {worker_id: 0 for worker_id in worker_states}

            while True:
                now = time.perf_counter()
                elapsed = now - started
                if elapsed >= args.duration_seconds:
                    break
                if now >= next_sample_at:
                    with router_state.metrics_lock:
                        max_active_requests = int(router_state.metrics.get("max_active_requests", 0))
                    samples.append(
                        {
                            "elapsed_seconds": round(elapsed, 3),
                            "rss_bytes": rss_bytes(),
                            "requests": request_index,
                            "max_active_requests": max_active_requests,
                        }
                    )
                    next_sample_at = now + args.sample_interval_seconds
                if now >= next_request_at:
                    row = request_once(router_url, headers, request_index)
                    request_index += 1
                    if row["worker_id"] in selected_workers:
                        selected_workers[row["worker_id"]] += 1
                    if row["success"]:
                        success_count += 1
                    elif row["bounded_error"]:
                        bounded_error_count += 1
                    else:
                        failures.append(row)
                    next_request_at = now + args.request_interval_seconds
                time.sleep(min(args.poll_sleep_seconds, max(0.0, next_request_at - time.perf_counter())))

            samples.append(
                {
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "rss_bytes": rss_bytes(),
                    "requests": request_index,
                    "max_active_requests": int(router_state.metrics.get("max_active_requests", 0)),
                }
            )
            evaluation = evaluate_soak_result(
                samples=samples,
                success_count=success_count,
                bounded_error_count=bounded_error_count,
                failure_count=len(failures),
                duration_seconds=args.duration_seconds,
                baseline_after_seconds=args.baseline_after_seconds,
                max_rss_growth_per_hour=args.max_rss_growth_per_hour,
                final_rss_multiplier=args.final_rss_multiplier,
            )
            return {
                "ok": evaluation["ok"],
                "acceptance_ok": evaluation["acceptance_ok"],
                "acceptance_run_requested": evaluation["acceptance_run_requested"],
                "harness_smoke_ok": evaluation["harness_smoke_ok"],
                "full_acceptance_duration": evaluation["full_acceptance_duration"],
                "threshold_ok": evaluation["threshold_ok"],
                "duration_seconds": args.duration_seconds,
                "actual_duration_seconds": evaluation["actual_duration_seconds"],
                "baseline_after_seconds": args.baseline_after_seconds,
                "duration_ok": evaluation["duration_ok"],
                "baseline_delay_ok": evaluation["baseline_delay_ok"],
                "baseline_sample_found": evaluation["baseline_sample_found"],
                "post_baseline_window_ok": evaluation["post_baseline_window_ok"],
                "zero_failures_ok": evaluation["zero_failures_ok"],
                "sample_interval_seconds": args.sample_interval_seconds,
                "request_interval_seconds": args.request_interval_seconds,
                "requests": request_index,
                "successful_requests": success_count,
                "bounded_errors": bounded_error_count,
                "failures": len(failures),
                "selected_workers": selected_workers,
                "thresholds": {
                    "max_rss_growth_per_hour": args.max_rss_growth_per_hour,
                    "final_rss_multiplier": args.final_rss_multiplier,
                },
                "rss": evaluation["rss"],
                "rss_growth_ok": evaluation["rss_growth_ok"],
                "final_rss_ok": evaluation["final_rss_ok"],
                "sample_count": len(samples),
                "sample_preview": samples[:3] + samples[-3:] if len(samples) > 6 else samples,
                "scope": "offline loopback long-soak harness; short runs are harness smoke only, not 8-24 hour acceptance evidence",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--self-test", action="store_true", help="Run a short local harness smoke.")
    parser.add_argument("--duration-seconds", type=float, default=8 * 3600.0, help="Soak duration. Final gate expects at least 28800 seconds.")
    parser.add_argument("--baseline-after-seconds", type=float, default=3600.0, help="RSS baseline delay. Final gate expects at least 3600 seconds.")
    parser.add_argument("--sample-interval-seconds", type=float, default=60.0, help="RSS sample interval.")
    parser.add_argument("--request-interval-seconds", type=float, default=0.2, help="Delay between loopback requests.")
    parser.add_argument("--poll-sleep-seconds", type=float, default=0.02, help="Main loop sleep granularity.")
    parser.add_argument("--worker-delay-seconds", type=float, default=0.01, help="Fake worker completion delay.")
    parser.add_argument("--active-slots", type=int, default=2, help="Configured active slots per ready fake worker.")
    parser.add_argument("--queue-limit-per-worker", type=int, default=8, help="Router queue depth per worker.")
    parser.add_argument("--queue-wait-timeout", type=float, default=5.0, help="Router queue wait timeout.")
    parser.add_argument("--max-rss-growth-per-hour", type=float, default=0.01, help="Maximum RSS growth ratio per hour after baseline.")
    parser.add_argument("--final-rss-multiplier", type=float, default=1.10, help="Maximum final RSS divided by baseline RSS.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        args.duration_seconds = 1.0
        args.baseline_after_seconds = 0.0
        args.sample_interval_seconds = 0.2
        args.request_interval_seconds = 0.05
        args.max_rss_growth_per_hour = 1000000.0
        args.final_rss_multiplier = 1000000.0
    if (
        args.duration_seconds <= 0
        or args.baseline_after_seconds < 0
        or args.sample_interval_seconds <= 0
        or args.request_interval_seconds <= 0
        or args.poll_sleep_seconds <= 0
        or args.worker_delay_seconds < 0
        or args.active_slots <= 0
        or args.queue_limit_per_worker < 0
        or args.queue_wait_timeout <= 0
        or args.max_rss_growth_per_hour < 0
        or args.final_rss_multiplier <= 0
    ):
        raise SystemExit("durations, intervals, slots, queue limits, and thresholds must be valid positive values")
    result = run_probe(args)
    if args.self_test:
        acceptance_math = run_acceptance_math_self_test()
        result["acceptance_math_self_test"] = acceptance_math
        result["ok"] = bool(result["ok"] and acceptance_math["ok"])
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
