#!/usr/bin/env python3
"""Operator-run Core API endpoint matrix for Cachy Router.

The live mode contacts only the router URL supplied by the operator. It checks
`/v1/models`, then targets each configured worker/model pair with normal
OpenAI-compatible completion and chat requests through `cache_router.mode=bypass`.
The router must strip the extension before forwarding and return the selected
worker in router-owned headers.

The self-test starts loopback fake workers and a loopback router; it contacts no
private hosts.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
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


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    data = None
    request_headers = {"Accept-Encoding": "identity", **headers}
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
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
        return 0, {}, {"error": {"type": "transport_error", "message": str(exc)}}
    try:
        decoded = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception as exc:  # noqa: BLE001
        decoded = {"error": {"type": "invalid_json", "message": f"{type(exc).__name__}: {exc}"}}
    return status, response_headers, decoded if isinstance(decoded, dict) else {"value": decoded}


def auth_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = {}
    if args.api_key:
        headers["Authorization"] = f"Bearer {args.api_key}"
    if args.x_api_key:
        headers["X-API-Key"] = args.x_api_key
    return headers


def parse_worker_model(value: str) -> dict[str, str]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("worker-model entries must be worker_id:model")
    worker_id, model = value.split(":", 1)
    worker_id = worker_id.strip()
    model = model.strip()
    if not worker_id or not model:
        raise argparse.ArgumentTypeError("worker_id and model must be non-empty")
    return {"worker_id": worker_id, "model": model}


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_id_component(text: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in ".:-" else "-" for char in text)
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] or "id"


def read_worker_models_from_file(path: Path) -> list[dict[str, str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("workers") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError(f"{path}: expected top-level workers array")
    pairs: list[dict[str, str]] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise RuntimeError(f"{path}: worker row {index} is not an object")
        worker_id = row.get("worker_id")
        model = row.get("model")
        if not isinstance(worker_id, str) or not worker_id or not isinstance(model, str) or not model:
            raise RuntimeError(f"{path}: worker row {index} needs non-empty worker_id and model")
        pairs.append({"worker_id": worker_id, "model": model})
    return pairs


def model_ids_from_body(body: dict[str, Any]) -> list[str]:
    rows = body.get("data")
    if not isinstance(rows, list):
        return []
    model_ids: list[str] = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("id"), str):
            model_ids.append(row["id"])
    return sorted(set(model_ids))


def discover_worker_models(router_url: str, headers: dict[str, str], timeout: float) -> tuple[list[dict[str, str]], dict[str, Any]]:
    status, _, body = request_json("GET", f"{router_url}/router/workers", headers=headers, timeout=timeout)
    if status != 200:
        return [], {"status": status, "error": body.get("error", body)}
    rows = body.get("workers")
    if not isinstance(rows, list):
        return [], {"status": status, "error": "workers response has no workers array"}
    pairs: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        readiness = row.get("readiness") if isinstance(row.get("readiness"), dict) else {}
        if readiness.get("ok") is not True:
            continue
        worker_id = row.get("worker_id")
        model = row.get("model")
        if isinstance(worker_id, str) and isinstance(model, str) and worker_id and model:
            pairs.append({"worker_id": worker_id, "model": model})
    return pairs, {
        "status": status,
        "workers_seen": len(rows),
        "ready_pairs": len(pairs),
        "not_ready_workers": len(rows) - len(pairs),
    }


def completion_body(model: str, worker_id: str | None, *, max_tokens: int) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "prompt": "Reply with exactly: core api ok",
        "max_tokens": max_tokens,
        "temperature": 0,
        "stream": False,
    }
    if worker_id is not None:
        body["cache_router"] = {
            "mode": "bypass",
            "cache_id": f"endpoint-matrix-{safe_id_component(worker_id)}",
            "worker_id": worker_id,
            "allow_fallback": False,
        }
    return body


def chat_body(model: str, worker_id: str | None, *, max_tokens: int) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": "Reply with exactly: core api ok"}],
        "max_tokens": max_tokens,
        "temperature": 0,
        "stream": False,
    }
    if worker_id is not None:
        body["cache_router"] = {
            "mode": "bypass",
            "cache_id": f"endpoint-matrix-chat-{safe_id_component(worker_id)}",
            "worker_id": worker_id,
            "allow_fallback": False,
        }
    return body


def check_completion_response(status: int, response_headers: dict[str, str], body: dict[str, Any], worker_id: str | None) -> dict[str, Any]:
    choices = body.get("choices") if isinstance(body.get("choices"), list) else []
    text = choices[0].get("text") if choices and isinstance(choices[0], dict) else None
    selected_worker = response_headers.get("x-cache-router-worker", "")
    has_request_id = bool(response_headers.get("x-cache-router-request-id"))
    has_trace_id = bool(response_headers.get("x-cache-router-trace-id"))
    has_selected_worker = selected_worker not in {"", "none"}
    return {
        "ok": (
            status == 200
            and isinstance(text, str)
            and has_request_id
            and has_trace_id
            and has_selected_worker
            and (worker_id is None or selected_worker == worker_id)
        ),
        "status": status,
        "selected_worker": selected_worker,
        "has_selected_worker": has_selected_worker,
        "has_request_id": has_request_id,
        "has_trace_id": has_trace_id,
        "object": body.get("object"),
        "error_type": body.get("error", {}).get("type") if isinstance(body.get("error"), dict) else None,
    }


def check_chat_response(status: int, response_headers: dict[str, str], body: dict[str, Any], worker_id: str | None) -> dict[str, Any]:
    choices = body.get("choices") if isinstance(body.get("choices"), list) else []
    message = choices[0].get("message") if choices and isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    selected_worker = response_headers.get("x-cache-router-worker", "")
    has_request_id = bool(response_headers.get("x-cache-router-request-id"))
    has_trace_id = bool(response_headers.get("x-cache-router-trace-id"))
    has_selected_worker = selected_worker not in {"", "none"}
    return {
        "ok": (
            status == 200
            and isinstance(content, str)
            and has_request_id
            and has_trace_id
            and has_selected_worker
            and (worker_id is None or selected_worker == worker_id)
        ),
        "status": status,
        "selected_worker": selected_worker,
        "has_selected_worker": has_selected_worker,
        "has_request_id": has_request_id,
        "has_trace_id": has_trace_id,
        "object": body.get("object"),
        "error_type": body.get("error", {}).get("type") if isinstance(body.get("error"), dict) else None,
    }


def status_from_checks(*, model_check_ok: bool, worker_models: list[dict[str, str]], plain_failures: int, matrix_failures: int, allow_warming: bool) -> str:
    if not worker_models and allow_warming:
        return "not_ready"
    if model_check_ok and worker_models and plain_failures == 0 and matrix_failures == 0:
        return "pass"
    if model_check_ok and worker_models and (plain_failures > 0 or matrix_failures > 0):
        return "partial"
    return "fail"


def run_matrix(router_url: str, headers: dict[str, str], worker_models: list[dict[str, str]], *, timeout: float, max_tokens: int, allow_warming: bool) -> dict[str, Any]:
    started = time.perf_counter()
    status, model_headers, model_body = request_json("GET", f"{router_url}/v1/models", headers=headers, timeout=timeout)
    available_models = model_ids_from_body(model_body)
    model_selected_worker = model_headers.get("x-cache-router-worker", "")
    model_check = {
        "ok": (
            status == 200
            and bool(available_models)
            and bool(model_headers.get("x-cache-router-request-id"))
            and bool(model_headers.get("x-cache-router-trace-id"))
            and model_selected_worker == "none"
        ),
        "status": status,
        "model_count": len(available_models),
        "selected_worker": model_selected_worker,
        "has_worker_none": model_selected_worker == "none",
        "has_request_id": bool(model_headers.get("x-cache-router-request-id")),
        "has_trace_id": bool(model_headers.get("x-cache-router-trace-id")),
        "error_type": model_body.get("error", {}).get("type") if isinstance(model_body.get("error"), dict) else None,
    }
    models_to_probe = sorted(set(pair["model"] for pair in worker_models))
    plain_results: list[dict[str, Any]] = []
    for model in models_to_probe:
        completion_status, completion_headers, completion_raw = request_json(
            "POST",
            f"{router_url}/v1/completions",
            headers=headers,
            timeout=timeout,
            body=completion_body(model, None, max_tokens=max_tokens),
        )
        chat_status, chat_headers, chat_raw = request_json(
            "POST",
            f"{router_url}/v1/chat/completions",
            headers=headers,
            timeout=timeout,
            body=chat_body(model, None, max_tokens=max_tokens),
        )
        plain_results.append(
            {
                "model": model,
                "model_listed": model in available_models,
                "completion": check_completion_response(completion_status, completion_headers, completion_raw, None),
                "chat": check_chat_response(chat_status, chat_headers, chat_raw, None),
            }
        )

    results: list[dict[str, Any]] = []
    for pair in worker_models:
        worker_id = pair["worker_id"]
        model = pair["model"]
        completion_status, completion_headers, completion_raw = request_json(
            "POST",
            f"{router_url}/v1/completions",
            headers=headers,
            timeout=timeout,
            body=completion_body(model, worker_id, max_tokens=max_tokens),
        )
        chat_status, chat_headers, chat_raw = request_json(
            "POST",
            f"{router_url}/v1/chat/completions",
            headers=headers,
            timeout=timeout,
            body=chat_body(model, worker_id, max_tokens=max_tokens),
        )
        results.append(
            {
                "worker_id": worker_id,
                "model": model,
                "model_listed": model in available_models,
                "completion": check_completion_response(completion_status, completion_headers, completion_raw, worker_id),
                "chat": check_chat_response(chat_status, chat_headers, chat_raw, worker_id),
            }
        )
    matrix_failures = [
        row
        for row in results
        if not row["model_listed"] or not row["completion"]["ok"] or not row["chat"]["ok"]
    ]
    plain_failures = [
        row
        for row in plain_results
        if not row["model_listed"] or not row["completion"]["ok"] or not row["chat"]["ok"]
    ]
    status = status_from_checks(
        model_check_ok=bool(model_check["ok"]),
        worker_models=worker_models,
        plain_failures=len(plain_failures),
        matrix_failures=len(matrix_failures),
        allow_warming=allow_warming,
    )
    return {
        "schema_version": "2026-07-02.1",
        "status": status,
        "ok": status == "pass",
        "scope": "operator-supplied router Core API worker/model matrix; live deployment evidence, not offline proof",
        "created_utc": now_iso(),
        "model_check": model_check,
        "worker_model_pairs": len(worker_models),
        "model_count": len(models_to_probe),
        "plain_failures": len(plain_failures),
        "matrix_failures": len(matrix_failures),
        "failures": len(plain_failures) + len(matrix_failures),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "plain_results": plain_results,
        "results": results,
        "summary": {
            "models_http_status": model_check["status"],
            "plain_completion_pass": sum(1 for row in plain_results if row["completion"]["ok"]),
            "plain_chat_pass": sum(1 for row in plain_results if row["chat"]["ok"]),
            "matrix_total": len(results) * 2,
            "matrix_pass": sum(1 for row in results if row["completion"]["ok"]) + sum(1 for row in results if row["chat"]["ok"]),
            "matrix_fail": len(matrix_failures),
            "not_ready_workers": 0 if worker_models else None,
        },
        "raw_prompt_tracked": False,
        "raw_response_tracked": False,
        "notes": [
            "does not write raw prompts or responses to disk",
            "targets workers with cache_router.mode=bypass and allow_fallback=false",
            "requires /router/workers unless --worker-model entries are supplied",
        ],
    }


def run_live(args: argparse.Namespace) -> dict[str, Any]:
    router_url = args.router_url.rstrip("/")
    headers = auth_headers(args)
    worker_models = list(args.worker_model or [])
    discovery: dict[str, Any] = {"source": "cli"}
    if not worker_models and args.workers_file:
        worker_models = read_worker_models_from_file(Path(args.workers_file))
        discovery = {"source": "workers_file", "path": args.workers_file, "ready_pairs": len(worker_models)}
    if not worker_models:
        worker_models, discovery = discover_worker_models(router_url, headers, args.timeout)
        discovery["source"] = "/router/workers"
    result = run_matrix(router_url, headers, worker_models, timeout=args.timeout, max_tokens=args.max_tokens, allow_warming=args.allow_warming)
    result["discovery"] = discovery
    if discovery.get("source") == "/router/workers" and int(discovery.get("not_ready_workers") or 0) > 0:
        result["status"] = "not_ready" if args.allow_warming else "fail"
        result["ok"] = False
        result["failures"] = int(result.get("failures") or 0) + int(discovery.get("not_ready_workers") or 0)
        result["notes"].append("discovery found configured workers that were not route-ready; rerun with --workers-file for deployment-owned inventory proof")
    result["router_url"] = router_url
    if not args.no_output_files:
        out_dir = Path(args.out_dir) if args.out_dir else Path("runtime") / "cache-router-core-api-matrix" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_path = out_dir / "summary.json"
        summary_path.write_text(json.dumps({key: value for key, value in result.items() if key != "router_url"}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result["output_paths"] = {"summary": str(summary_path)}
    return result


def run_self_test() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cache-router-endpoint-matrix-") as tmp:
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
            worker_models_by_id = {
                "worker-main": "fake-model",
                "worker-backup": "fake-backup-model",
            }
            for worker_id, worker_state in worker_states.items():
                worker_state.model_ids = [worker_models_by_id[worker_id]]
            router_args = smoke.router_args(worker_urls["worker-main"], root / "cache", root / "worker-main-slots")
            router_args.readiness_poll_interval = 0.0
            workers_file = root / "workers.json"
            worker_rows = [
                smoke.inventory_worker_entry(
                    worker_id,
                    worker_url,
                    root / f"{worker_id}-slots",
                    model=worker_models_by_id[worker_id],
                )
                for worker_id, worker_url in worker_urls.items()
            ]
            workers_file.write_text(json.dumps({"workers": worker_rows}, sort_keys=True), encoding="utf-8")
            router_args.workers_file = str(workers_file)
            router_state = cache_router_daemon.CacheRouterState(router_args)
            router_state.poll_readiness_once()
            router = ThreadingHTTPServer(("127.0.0.1", 0), QuietRouterHandler)
            router.state = router_state  # type: ignore[attr-defined]
            thread = threading.Thread(target=router.serve_forever, daemon=True)
            thread.start()
            router_url = f"http://{router.server_address[0]}:{router.server_address[1]}"
            headers = {"Authorization": "Bearer secret-token"}
            worker_models, discovery = discover_worker_models(router_url, headers, timeout=5.0)
            result = run_matrix(router_url, headers, worker_models, timeout=5.0, max_tokens=1, allow_warming=False)
            all_probe_rows = list(result["plain_results"]) + list(result["results"])
            all_response_checks = [
                response
                for row in all_probe_rows
                for response in [row["completion"], row["chat"]]
            ]
            forwarded_before_negative = sum(len(state.requests) for state in worker_states.values())
            worker_states["worker-main"].models_ready = False
            router_state.poll_readiness_once()
            negative_status, negative_headers, negative_body = request_json(
                "POST",
                f"{router_url}/v1/completions",
                headers=headers,
                timeout=5.0,
                body=completion_body("fake-model", "worker-main", max_tokens=1),
            )
            forwarded_after_negative = sum(len(state.requests) for state in worker_states.values())
            negative_result = {
                "status": negative_status,
                "selected_worker": negative_headers.get("x-cache-router-worker", ""),
                "error_type": negative_body.get("error", {}).get("type") if isinstance(negative_body.get("error"), dict) else None,
                "forwarded_to_worker": forwarded_after_negative > forwarded_before_negative,
            }
            self_test_checks = {
                "discovers_two_worker_model_pairs": len(worker_models) == 2,
                "discovers_two_distinct_models": result["model_count"] == 2,
                "models_response_has_request_and_trace": bool(result["model_check"]["has_request_id"] and result["model_check"]["has_trace_id"]),
                "models_response_reports_no_selected_worker": result["model_check"].get("has_worker_none") is True,
                "plain_probe_count_matches_model_count": len(result["plain_results"]) == result["model_count"],
                "worker_matrix_count_matches_pairs": len(result["results"]) == result["worker_model_pairs"],
                "every_probe_has_request_trace_and_selected_worker": all(
                    response.get("has_request_id")
                    and response.get("has_trace_id")
                    and response.get("has_selected_worker")
                    for response in all_response_checks
                ),
                "fallback_disabled_worker_targets_selected_exact_worker": all(
                    row["completion"].get("selected_worker") == row["worker_id"]
                    and row["chat"].get("selected_worker") == row["worker_id"]
                    for row in result["results"]
                ),
                "fallback_disabled_unavailable_target_fails_closed": (
                    negative_status == 503
                    and negative_headers.get("x-cache-router-worker") == "none"
                    and negative_result["forwarded_to_worker"] is False
                ),
            }
            result["discovery"] = discovery
            result["negative_unavailable_target"] = negative_result
            result["scope"] = "self-test loopback router Core API matrix; no private hosts contacted"
            result["self_test_checks"] = self_test_checks
            if not all(self_test_checks.values()):
                result["status"] = "fail"
                result["ok"] = False
            return result
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
    parser.add_argument("--self-test", action="store_true", help="Run loopback self-test without contacting a supplied router.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--router-url", help="Operator-supplied Cachy Router URL, for example http://<router-lan-ip>:18080.")
    parser.add_argument("--api-key", help="Bearer token for router auth, if configured.")
    parser.add_argument("--x-api-key", help="X-API-Key value for router auth, if configured.")
    parser.add_argument("--workers-file", help="Operator deployment inventory to use when /router/workers is disabled or unavailable.")
    parser.add_argument("--worker-model", action="append", type=parse_worker_model, help="Explicit worker/model pair as worker_id:model. Repeatable.")
    parser.add_argument("--allow-warming", action="store_true", help="Report not_ready instead of fail when no worker/model pairs are ready.")
    parser.add_argument("--max-tokens", type=int, default=4, help="Maximum generated tokens for endpoint probes.")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds.")
    parser.add_argument("--out-dir", help="Ignored runtime directory for redacted summary.json.")
    parser.add_argument("--no-output-files", action="store_true", help="Do not write runtime summary.json in live mode.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        result = run_self_test()
    else:
        if not args.router_url:
            raise SystemExit("--router-url is required unless --self-test is used")
        if args.timeout <= 0 or args.max_tokens <= 0:
            raise SystemExit("--timeout and --max-tokens must be positive")
        result = run_live(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
