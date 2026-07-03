#!/usr/bin/env python3
"""Offline scheduler stress probe for Cachy Router.

The default run is intentionally long enough to satisfy the final acceptance
gate. It uses only loopback fake workers and does not contact private hosts.
"""

from __future__ import annotations

import argparse
import concurrent.futures
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
import validate_cache_router_contracts as contracts  # noqa: E402


class QuietRouterHandler(cache_router_daemon.RouterHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        return


def parse_json(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return value if isinstance(value, dict) else {}


def load_decision_schema() -> dict[str, Any]:
    return json.loads((PACKAGE_ROOT / "schemas/cache-router/cache-decision-event.schema.json").read_text(encoding="utf-8"))


def run_wave(
    *,
    router_url: str,
    headers: dict[str, str],
    wave_index: int,
    concurrent_requests: int,
) -> list[dict[str, Any]]:
    barrier = threading.Barrier(concurrent_requests)

    def one_request(index: int) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            barrier.wait(timeout=5.0)
            status, response_headers, raw = smoke.request(
                "POST",
                router_url + "/v1/completions",
                headers=headers,
                body={
                    "model": "fake-model",
                    "prompt": f"scheduler stress wave {wave_index} request {index}",
                    "max_tokens": 1,
                },
            )
            body = parse_json(raw)
            text = body.get("choices", [{}])[0].get("text")
            error_type = body.get("error", {}).get("type") if isinstance(body.get("error"), dict) else None
            worker_id = response_headers.get("x-cache-router-worker", "")
            return {
                "wave": wave_index,
                "index": index,
                "status": status,
                "success": status == 200 and text == "ok" and worker_id not in {"", "none"},
                "bounded_error": status in {429, 503} and error_type in {"rate_limit_error", "service_unavailable"},
                "worker_id": worker_id,
                "request_id": response_headers.get("x-cache-router-request-id", ""),
                "elapsed_ms": (time.perf_counter() - started) * 1000.0,
                "error_type": error_type,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "wave": wave_index,
                "index": index,
                "status": None,
                "success": False,
                "bounded_error": False,
                "worker_id": "",
                "request_id": "",
                "elapsed_ms": (time.perf_counter() - started) * 1000.0,
                "exception": f"{type(exc).__name__}: {exc}",
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        return list(executor.map(one_request, range(concurrent_requests)))


def decision_event_errors(
    *,
    router_url: str,
    headers: dict[str, str],
    request_id: str,
    schema: dict[str, Any],
    unavailable_worker_id: str,
) -> list[str]:
    status, _, raw = smoke.request("GET", router_url + f"/router/decisions?request_id={request_id}", headers=headers)
    if status != 200:
        return [f"{request_id}: /router/decisions returned HTTP {status}"]
    events = parse_json(raw).get("events", [])
    if not isinstance(events, list) or len(events) != 1:
        return [f"{request_id}: expected exactly one decision event, found {len(events) if isinstance(events, list) else 'non-list'}"]
    event = events[0]
    if not isinstance(event, dict):
        return [f"{request_id}: decision event is not an object"]
    errors = contracts.validate_decision_event(event, schema, request_id)
    if event.get("worker_id") == unavailable_worker_id:
        errors.append(f"{request_id}: unavailable worker was selected")
    scheduler = event.get("scheduler") if isinstance(event.get("scheduler"), dict) else {}
    candidates = scheduler.get("candidates") if isinstance(scheduler.get("candidates"), list) else []
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("worker_id") == unavailable_worker_id and candidate.get("eligible") is True:
            errors.append(f"{request_id}: unavailable worker was marked scheduler-eligible")
    return errors


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-scheduler-stress-") as tmp:
        root = Path(tmp)
        ready_states = {
            "worker-main": smoke.FakeWorkerState(),
            "worker-backup": smoke.FakeWorkerState(),
        }
        unavailable_state = smoke.FakeWorkerState()
        unavailable_state.healthy = False
        worker_states = {**ready_states, "worker-unavailable": unavailable_state}
        workers = {
            worker_id: smoke.start_server(smoke.FakeWorkerHandler, state_attr="fake_state", state=worker_state)
            for worker_id, worker_state in worker_states.items()
        }
        router: ThreadingHTTPServer | None = None
        router_state: cache_router_daemon.CacheRouterState | None = None
        try:
            for worker_state in ready_states.values():
                worker_state.completion_delay_seconds = args.worker_delay_seconds
            worker_urls = {
                worker_id: f"http://{server.server_address[0]}:{server.server_address[1]}" for worker_id, server in workers.items()
            }
            router_args = smoke.router_args(worker_urls["worker-main"], root / "cache", root / "worker-main-slots")
            router_args.readiness_poll_interval = 0.0
            router_args.readiness_timeout = 1.0
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
            decision_schema = load_decision_schema()
            ready_worker_count = len(ready_states)
            concurrent_requests = 2 * ready_worker_count * args.active_slots
            deadline = time.monotonic() + args.duration_seconds
            wave_index = 0
            results: list[dict[str, Any]] = []
            event_errors: list[str] = []
            selected_workers: dict[str, int] = {}
            max_event_checks = args.max_event_checks
            checked_events = 0

            while wave_index < args.min_waves or time.monotonic() < deadline:
                wave_results = run_wave(
                    router_url=router_url,
                    headers=headers,
                    wave_index=wave_index,
                    concurrent_requests=concurrent_requests,
                )
                results.extend(wave_results)
                for result in wave_results:
                    worker_id = str(result.get("worker_id") or "")
                    if worker_id:
                        selected_workers[worker_id] = selected_workers.get(worker_id, 0) + 1
                    request_id = str(result.get("request_id") or "")
                    if request_id and result.get("success") and checked_events < max_event_checks:
                        event_errors.extend(
                            decision_event_errors(
                                router_url=router_url,
                                headers=headers,
                                request_id=request_id,
                                schema=decision_schema,
                                unavailable_worker_id="worker-unavailable",
                            )
                        )
                        checked_events += 1
                wave_index += 1
                if args.wave_pause_seconds > 0:
                    time.sleep(args.wave_pause_seconds)

            exceptions = [row for row in results if row.get("exception")]
            successes = [row for row in results if row.get("success")]
            bounded_errors = [row for row in results if row.get("bounded_error")]
            bad_responses = [row for row in results if not row.get("success") and not row.get("bounded_error")]
            with router_state.metrics_lock:
                max_active_requests = int(router_state.metrics.get("max_active_requests", 0))
            active_after = {worker_id: router_state.worker_active_count(worker_id) for worker_id in worker_states}
            queued_after = {worker_id: router_state.worker_queue_depth(worker_id) for worker_id in worker_states}
            worker_request_counts = {worker_id: len(worker_state.requests) for worker_id, worker_state in worker_states.items()}
            unavailable_traffic = worker_request_counts.get("worker-unavailable", 0)
            ok = bool(
                not exceptions
                and not bad_responses
                and not event_errors
                and unavailable_traffic == 0
                and all(value == 0 for value in active_after.values())
                and all(value == 0 for value in queued_after.values())
                and max_active_requests >= ready_worker_count * args.active_slots
            )
            return {
                "ok": ok,
                "duration_seconds": args.duration_seconds,
                "waves": wave_index,
                "ready_workers": ready_worker_count,
                "active_slots": args.active_slots,
                "concurrent_requests_per_wave": concurrent_requests,
                "total_requests": len(results),
                "successful_completions": len(successes),
                "bounded_errors": len(bounded_errors),
                "bad_responses": len(bad_responses),
                "client_exceptions": len(exceptions),
                "decision_events_checked": checked_events,
                "decision_event_errors": event_errors[:20],
                "selected_workers": selected_workers,
                "worker_request_counts": worker_request_counts,
                "unavailable_worker_requests": unavailable_traffic,
                "max_active_requests": max_active_requests,
                "active_after": active_after,
                "queued_after": queued_after,
                "scope": "offline loopback scheduler/accounting stress; not live model generation or production load-balancing proof",
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
    parser.add_argument("--duration-seconds", type=float, default=600.0, help="Stress duration. The final acceptance gate expects at least 600 seconds.")
    parser.add_argument("--min-waves", type=int, default=1, help="Minimum waves to run even when duration is very short.")
    parser.add_argument("--active-slots", type=int, default=2, help="Configured active slots per ready loopback worker.")
    parser.add_argument("--queue-limit-per-worker", type=int, default=8, help="Router queue depth per worker.")
    parser.add_argument("--queue-wait-timeout", type=float, default=5.0, help="Queue wait timeout in seconds.")
    parser.add_argument("--worker-delay-seconds", type=float, default=0.05, help="Fake worker completion delay to force overlap.")
    parser.add_argument("--wave-pause-seconds", type=float, default=0.02, help="Pause between stress waves.")
    parser.add_argument("--max-event-checks", type=int, default=200, help="Maximum successful decision events to schema-check.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (
        args.duration_seconds <= 0
        or args.min_waves < 1
        or args.active_slots <= 0
        or args.queue_limit_per_worker < 0
        or args.queue_wait_timeout <= 0
        or args.worker_delay_seconds <= 0
        or args.wave_pause_seconds < 0
        or args.max_event_checks < 1
    ):
        raise SystemExit("duration, waves, slots, timeouts, and event checks must be positive")
    result = run_probe(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
