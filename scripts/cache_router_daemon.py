#!/usr/bin/env python3
"""Long-running OpenAI-compatible cache-router daemon.

This is the MVP router endpoint for trusted home-LAN cache-router experiments.
It exposes an OpenAI-compatible surface and routes cached completion requests
to configured router-managed llama.cpp workers.

Normal OpenAI requests pass through to the worker. Cache-accelerated requests
use a nonstandard `cache_router` object that is stripped before any backend
request is sent.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cache_router_transport import SlotTransport


SCHEMA_VERSION = "2026-06-30.1"
TENANT_HASH = hashlib.sha256(b"openai-cache-router-localhost-tenant").hexdigest()
CONVERSATION_HASH = hashlib.sha256(b"openai-cache-router-localhost-conversation").hexdigest()
POLICY_HASH = hashlib.sha256(b"openai-cache-router-localhost-policy").hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def read_jsonl_tail(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def load_auth_token(args: argparse.Namespace) -> str:
    token = (args.auth_token or "").strip()
    if token:
        return token
    if args.auth_token_file:
        path = Path(args.auth_token_file)
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()
    return ""


def http_request(
    method: str,
    url: str,
    *,
    payload: Any | None = None,
    timeout: float = 900.0,
    stream: bool = False,
) -> tuple[int, dict[str, str], bytes, float]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept-Encoding": "identity"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            elapsed = (time.perf_counter() - start) * 1000.0
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, hdrs, raw, elapsed
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        elapsed = (time.perf_counter() - start) * 1000.0
        hdrs = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
        return exc.code, hdrs, raw, elapsed


def json_request(method: str, url: str, *, payload: Any | None = None, timeout: float = 900.0) -> tuple[int, Any, float]:
    status, _, raw, elapsed = http_request(method, url, payload=payload, timeout=timeout)
    if not raw:
        return status, {}, elapsed
    try:
        return status, json.loads(raw.decode("utf-8", errors="replace")), elapsed
    except json.JSONDecodeError:
        return status, {"raw": raw.decode("utf-8", errors="replace")}, elapsed


def token_count(worker_url: str, text: str, timeout: float) -> int:
    for payload in ({"content": text, "add_special": False}, {"content": text}, {"prompt": text}):
        status, body, _ = json_request("POST", worker_url + "/tokenize", payload=payload, timeout=timeout)
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
    raise RuntimeError("/tokenize did not return a token list")


def completion(worker_url: str, prompt: str, *, n_predict: int, slot_id: int, timeout: float) -> tuple[dict[str, Any], float]:
    payload = {
        "prompt": prompt,
        "n_predict": n_predict,
        "temperature": 0.0,
        "top_k": 1,
        "cache_prompt": True,
        "id_slot": slot_id,
        "stream": False,
    }
    status, body, wall_ms = json_request("POST", worker_url + "/completion", payload=payload, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"worker /completion failed HTTP {status}: {body}")
    if not isinstance(body, dict):
        raise RuntimeError("worker /completion returned non-object JSON")
    body["_router_wall_ms"] = wall_ms
    return body, wall_ms


def slot_action(worker_url: str, slot_id: int, action: str, filename: str | None, timeout: float) -> tuple[dict[str, Any], float]:
    payload = {} if filename is None else {"filename": filename}
    status, body, wall_ms = json_request("POST", f"{worker_url}/slots/{slot_id}?action={action}", payload=payload, timeout=timeout)
    if status >= 400:
        raise RuntimeError(f"slot action {action} failed HTTP {status}: {body}")
    if not isinstance(body, dict):
        body = {"body": body}
    return body, wall_ms


def openai_completion_response(
    *,
    model: str,
    text: str,
    prompt_tokens: int,
    completion_tokens: int,
    cache_router: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "id": "cmpl-cache-router-" + hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16],
        "object": "text_completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"text": text, "index": 0, "finish_reason": "stop", "logprobs": None}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
    if cache_router is not None:
        response["cache_router"] = cache_router
    return response


def openai_chat_response(*, model: str, content: str, cache_router: dict[str, Any] | None = None) -> dict[str, Any]:
    response = {
        "id": "chatcmpl-cache-router-" + hashlib.sha256(f"{time.time_ns()}".encode()).hexdigest()[:16],
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
    if cache_router is not None:
        response["cache_router"] = cache_router
    return response


def slot_rows(slots: Any) -> list[dict[str, Any]]:
    if isinstance(slots, list):
        return [row for row in slots if isinstance(row, dict)]
    if isinstance(slots, dict):
        rows = slots.get("slots")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        if isinstance(slots.get("id"), int):
            return [slots]
    return []


def slot_availability(slots: Any, slot_id: int) -> dict[str, Any]:
    rows = slot_rows(slots)
    selected = next((row for row in rows if row.get("id") == slot_id), rows[0] if rows else None)
    if selected is None:
        return {"available": None, "busy_score": 1, "reason": "slot_state_unknown"}
    is_processing = selected.get("is_processing")
    next_tokens = selected.get("next_token") if isinstance(selected.get("next_token"), list) else []
    has_next = any(isinstance(row, dict) and row.get("has_next_token") is True for row in next_tokens)
    busy = is_processing is True or has_next
    return {
        "available": not busy,
        "busy_score": 2 if busy else 0,
        "reason": "busy" if busy else "idle",
        "is_processing": is_processing,
        "has_next_token": has_next,
        "n_prompt_tokens": selected.get("n_prompt_tokens"),
        "n_prompt_tokens_processed": selected.get("n_prompt_tokens_processed"),
    }


def row_value(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default


def path_like(value: Any) -> bool:
    text = str(value or "")
    return text.startswith("/") or text.endswith(".gguf")


def bool_value(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def int_value(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    return int(value)


@dataclass
class WorkerRuntime:
    worker_id: str
    url: str
    slot_id: int
    slot_save_path: str
    transport: SlotTransport
    model: str
    model_identity: str
    model_path: str
    model_file_size: int
    llama_server_path: str
    llama_server_version: str
    ctx_size: int
    cache_type_k: str
    cache_type_v: str
    mtp_enabled: bool
    spec_draft_model_identity: str
    spec_draft_model_path: str
    spec_draft_model_size: int

    def health(self) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            status, body, wall_ms = json_request("GET", self.url + "/health", timeout=5.0)
            return {"http_status": status, "body": body, "wall_ms": wall_ms, "ok": status == 200 and isinstance(body, dict) and body.get("status") == "ok"}
        except Exception as exc:  # noqa: BLE001
            return {"http_status": None, "error": repr(exc), "wall_ms": (time.perf_counter() - start) * 1000.0, "ok": False}

    def slots(self) -> Any:
        try:
            status, body, _ = json_request("GET", self.url + "/slots", timeout=5.0)
            return body if status == 200 else {"http_status": status, "body": body}
        except Exception as exc:  # noqa: BLE001
            return {"http_status": None, "error": repr(exc)}

    def availability(self) -> dict[str, Any]:
        return slot_availability(self.slots(), self.slot_id)

    def summary(self, state: "CacheRouterState", *, include_slots: bool = True) -> dict[str, Any]:
        row = {
            "worker_id": self.worker_id,
            "url": self.url,
            "health": self.health(),
            "availability": self.availability(),
            "slot_save_path": self.slot_save_path,
            "slot_id": self.slot_id,
            "transport": self.transport.describe(),
            "model": self.model,
            "model_identity": self.model_identity,
            "model_path": self.model_path,
            "model_file_size": self.model_file_size,
            "llama_server_path": self.llama_server_path,
            "llama_server_version": self.llama_server_version,
            "ctx_size": self.ctx_size,
            "cache_type_k": self.cache_type_k,
            "cache_type_v": self.cache_type_v,
            "mtp_enabled": self.mtp_enabled,
            "spec_draft_model_identity": self.spec_draft_model_identity,
            "spec_draft_model_path": self.spec_draft_model_path,
            "spec_draft_model_size": self.spec_draft_model_size,
        }
        if include_slots:
            row["slots"] = self.slots()
        return row


def worker_metadata_from_row(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    raw_model = row_value(row, "model_name", "alias", default="")
    legacy_model = row.get("model")
    if not raw_model and legacy_model and not path_like(legacy_model):
        raw_model = str(legacy_model)
    model = str(raw_model or args.model)

    model_path = str(row_value(row, "model_path", "model_file", default=""))
    if not model_path and legacy_model and path_like(legacy_model):
        model_path = str(legacy_model)
    model_path = model_path or args.model_path

    spec_path = str(row_value(row, "spec_draft_model_path", "mtp_model", "draft_model_path", default=args.spec_draft_model_path))
    return {
        "model": model,
        "model_identity": str(row_value(row, "model_identity", "model_hash", default=model_path)),
        "model_path": model_path,
        "model_file_size": int_value(row_value(row, "model_file_size", "model_size_bytes", default=args.model_file_size), args.model_file_size),
        "llama_server_path": str(row_value(row, "llama_server_path", "llama_server", default=args.llama_server_path)),
        "llama_server_version": str(row_value(row, "llama_server_version", "runtime_version", default=args.llama_server_version)),
        "ctx_size": int_value(row_value(row, "ctx_size", default=args.ctx_size), args.ctx_size),
        "cache_type_k": str(row_value(row, "cache_type_k", default=args.cache_type_k)),
        "cache_type_v": str(row_value(row, "cache_type_v", default=args.cache_type_v)),
        "mtp_enabled": bool_value(row_value(row, "mtp_enabled", default=args.mtp_enabled), args.mtp_enabled),
        "spec_draft_model_identity": str(row_value(row, "spec_draft_model_identity", "mtp_model_identity", "spec_draft_model_hash", default=spec_path)),
        "spec_draft_model_path": spec_path,
        "spec_draft_model_size": int_value(
            row_value(row, "spec_draft_model_size", "mtp_model_size", "mtp_model_size_bytes", default=args.spec_draft_model_size),
            args.spec_draft_model_size,
        ),
    }


def load_workers(args: argparse.Namespace) -> list[WorkerRuntime]:
    if args.workers_file:
        raw = read_json(Path(args.workers_file), {})
        rows = raw.get("workers") if isinstance(raw, dict) else raw
        if not isinstance(rows, list) or not rows:
            raise ValueError("--workers-file must contain a non-empty workers list")
        workers: list[WorkerRuntime] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("each worker entry must be an object")
            worker_id = str(row.get("worker_id") or "").strip()
            url = str(row.get("url") or row.get("worker_url") or "").rstrip("/")
            slot_dir = str(row.get("slot_save_path") or row.get("worker_slot_dir") or "").rstrip("/")
            if not worker_id or not url or not slot_dir:
                raise ValueError("worker entries require worker_id, worker_url/url, and slot_save_path/worker_slot_dir")
            if worker_id in seen:
                raise ValueError(f"duplicate worker_id in workers file: {worker_id}")
            seen.add(worker_id)
            transport_row = row.get("transport") if isinstance(row.get("transport"), dict) else {}
            transport_kind = str(transport_row.get("kind") or row.get("worker_transport") or "local")
            ssh_host = str(transport_row.get("ssh_host") or row.get("worker_ssh_host") or "")
            sidecar_url = str(transport_row.get("sidecar_url") or row.get("worker_sidecar_url") or "")
            metadata = worker_metadata_from_row(row, args)
            workers.append(
                WorkerRuntime(
                    worker_id=worker_id,
                    url=url,
                    slot_id=int(row.get("slot_id", args.slot_id)),
                    slot_save_path=slot_dir,
                    transport=SlotTransport(
                        worker_id=worker_id,
                        kind=transport_kind,
                        slot_dir=slot_dir,
                        ssh_host=ssh_host,
                        sidecar_url=sidecar_url,
                        ssh_config=str(transport_row.get("ssh_config") or args.ssh_config),
                        ssh_extra_args=str(transport_row.get("ssh_extra_args") or args.ssh_extra_args),
                        scp_extra_args=str(transport_row.get("scp_extra_args") or args.scp_extra_args),
                        timeout=args.timeout,
                    ),
                    **metadata,
                )
            )
        return workers

    return [
        WorkerRuntime(
            worker_id=args.worker_id,
            url=args.worker_url.rstrip("/"),
            slot_id=args.slot_id,
            slot_save_path=args.worker_slot_dir.rstrip("/"),
            transport=SlotTransport(
                worker_id=args.worker_id,
                kind=args.worker_transport,
                slot_dir=args.worker_slot_dir,
                ssh_host=args.worker_ssh_host,
                sidecar_url=args.worker_sidecar_url,
                ssh_config=args.ssh_config,
                ssh_extra_args=args.ssh_extra_args,
                scp_extra_args=args.scp_extra_args,
                timeout=args.timeout,
            ),
            model=args.model,
            model_identity=args.model_path,
            model_path=args.model_path,
            model_file_size=args.model_file_size,
            llama_server_path=args.llama_server_path,
            llama_server_version=args.llama_server_version,
            ctx_size=args.ctx_size,
            cache_type_k=args.cache_type_k,
            cache_type_v=args.cache_type_v,
            mtp_enabled=args.mtp_enabled,
            spec_draft_model_identity=args.spec_draft_model_path,
            spec_draft_model_path=args.spec_draft_model_path,
            spec_draft_model_size=args.spec_draft_model_size,
        )
    ]


class CacheRouterState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cache_root = Path(args.cache_root)
        self.router_store = self.cache_root / "router-store"
        self.blobs = self.router_store / "blobs"
        self.manifests = self.router_store / "manifests"
        self.registry_path = self.router_store / "registry.json"
        self.workers = load_workers(args)
        self.worker_by_id = {worker.worker_id: worker for worker in self.workers}
        self.default_worker_id = self.workers[0].worker_id
        self.events_path = self.cache_root / "router" / "logs" / "cache-router-events.jsonl"
        self.auth_token = load_auth_token(args)
        self.lock = threading.Lock()
        for path in [self.blobs, self.manifests, self.events_path.parent]:
            path.mkdir(parents=True, exist_ok=True)
        for worker in self.workers:
            worker.transport.ensure_slot_dir()

    def select_worker(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = False,
    ) -> WorkerRuntime:
        candidates = self.candidate_workers(preferred_worker_id, prefer_residency=prefer_residency, allow_fallback=allow_fallback)
        if not candidates:
            raise RuntimeError("no healthy cache-router worker available")
        return candidates[0]

    def ordered_workers(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = True,
    ) -> list[WorkerRuntime]:
        if preferred_worker_id:
            worker = self.worker_by_id.get(preferred_worker_id)
            if worker is None:
                raise KeyError(f"unknown worker_id: {preferred_worker_id}")
            candidates = [worker]
            if allow_fallback:
                candidates.extend(w for w in self.workers if w.worker_id != preferred_worker_id)
        elif prefer_residency:
            hot = [worker for worker in self.workers if prefer_residency.get(worker.worker_id) is True]
            cold = [worker for worker in self.workers if worker.worker_id not in {w.worker_id for w in hot}]
            candidates = hot + cold
        else:
            candidates = list(self.workers)
        return candidates

    def rank_worker(
        self,
        worker: WorkerRuntime,
        availability: dict[str, Any],
        ordered_index: dict[str, int],
        preferred_worker_id: str | None,
        prefer_residency: dict[str, Any] | None,
    ) -> tuple[int, int, int, int]:
        busy_score = int(availability.get("busy_score", 1))
        preferred_score = 0 if preferred_worker_id and worker.worker_id == preferred_worker_id else 1
        hot_score = 0 if prefer_residency and prefer_residency.get(worker.worker_id) is True else 1
        index_score = ordered_index.get(worker.worker_id, len(self.workers))
        if preferred_worker_id:
            return (busy_score, preferred_score, hot_score, index_score)
        return (busy_score, hot_score, preferred_score, index_score)

    def candidate_workers(
        self,
        preferred_worker_id: str | None = None,
        *,
        prefer_residency: dict[str, Any] | None = None,
        allow_fallback: bool = True,
    ) -> list[WorkerRuntime]:
        candidates = self.ordered_workers(preferred_worker_id, prefer_residency=prefer_residency, allow_fallback=allow_fallback)
        ordered_index = {worker.worker_id: index for index, worker in enumerate(candidates)}
        healthy: list[tuple[WorkerRuntime, dict[str, Any]]] = []
        last_health: dict[str, Any] | None = None
        for worker in candidates:
            health = worker.health()
            last_health = health
            if health.get("ok"):
                worker.transport.ensure_slot_dir()
                healthy.append((worker, worker.availability()))
        if not healthy:
            raise RuntimeError(f"no healthy cache-router worker available; last_health={last_health}")
        healthy.sort(key=lambda item: self.rank_worker(item[0], item[1], ordered_index, preferred_worker_id, prefer_residency))
        return [worker for worker, _ in healthy]

    def worker_health(self, worker: WorkerRuntime | None = None) -> dict[str, Any]:
        return (worker or self.workers[0]).health()

    def worker_summaries(self, *, include_slots: bool = True) -> list[dict[str, Any]]:
        return [worker.summary(self, include_slots=include_slots) for worker in self.workers]

    def worker_summary(self) -> dict[str, Any]:
        return self.workers[0].summary(self)

    def load_registry(self) -> dict[str, Any]:
        return read_json(self.registry_path, {"schema_version": "2026-07-01.1", "entries": []})

    def save_registry(self, registry: dict[str, Any]) -> None:
        registry["updated_at"] = now_iso()
        write_json(self.registry_path, registry)

    def find_entry(self, cache_id: str) -> dict[str, Any] | None:
        registry = self.load_registry()
        for entry in registry.get("entries", []):
            if entry.get("cache_id") == cache_id:
                return entry
        return None

    def cache_key_fields(self, cache_id: str, prefix_text: str, worker: WorkerRuntime) -> dict[str, Any]:
        prefix_tokens = token_count(worker.url, prefix_text, self.args.timeout)
        return {
            "cache_id": cache_id,
            "prefix_sha256": sha256_text(prefix_text),
            "prefix_token_count": prefix_tokens,
            "model_identity": worker.model_identity,
            "model_file_size": worker.model_file_size,
            "llama_server_version": worker.llama_server_version,
            "ctx_size": worker.ctx_size,
            "cache_type_k": worker.cache_type_k,
            "cache_type_v": worker.cache_type_v,
            "mtp_enabled": worker.mtp_enabled,
            "spec_draft_model_identity": worker.spec_draft_model_identity,
            "spec_draft_model_size": worker.spec_draft_model_size,
        }

    def worker_cache_compatibility_mismatch(self, manifest: dict[str, Any], worker: WorkerRuntime) -> str | None:
        expected = {
            "model_identity": worker.model_identity,
            "model_file_size": worker.model_file_size,
            "llama_server_version": worker.llama_server_version,
            "ctx_size": worker.ctx_size,
            "cache_type_k": worker.cache_type_k,
            "cache_type_v": worker.cache_type_v,
            "mtp_enabled": worker.mtp_enabled,
            "spec_draft_model_identity": worker.spec_draft_model_identity,
            "spec_draft_model_size": worker.spec_draft_model_size,
        }
        for field, worker_value in expected.items():
            manifest_value = manifest.get(field)
            if manifest_value in (None, ""):
                return f"manifest missing {field}"
            if manifest_value != worker_value:
                return f"{field} mismatch: manifest={manifest_value!r} worker={worker_value!r}"
        return None

    def emit_event(
        self,
        *,
        phase: str,
        decision: str,
        cache_id: str,
        cache_key_hash: str | None,
        manifest_id: str | None,
        cache_hit_level: str,
        compatibility_result: str,
        latency_ms: float | None,
        prompt_tokens: int | None,
        processed_prompt_tokens: int | None,
        cached_tokens: int | None,
        generated_tokens: int | None,
        prompt_tps: float | None,
        eval_tps: float | None,
        restore_latency_ms: float | None = None,
        hydration_latency_ms: float | None = None,
        fallback_required: bool = False,
        fallback_reason: str | None = None,
        worker_id: str | None = None,
        notes: str = "",
    ) -> None:
        reuse_ratio = cached_tokens / prompt_tokens if prompt_tokens and cached_tokens is not None else None
        if isinstance(reuse_ratio, (int, float)):
            reuse_ratio = max(0.0, min(1.0, float(reuse_ratio)))
        row = {
            "schema_version": SCHEMA_VERSION,
            "event_id": "evt-" + hashlib.sha256(f"{time.time_ns()}:{phase}:{cache_id}".encode()).hexdigest()[:16],
            "trace_id": cache_id,
            "request_id": cache_id,
            "request_hash": hashlib.sha256(cache_id.encode()).hexdigest(),
            "timestamp": now_iso(),
            "phase": phase,
            "decision": decision,
            "tenant_hash": TENANT_HASH,
            "conversation_hash": CONVERSATION_HASH,
            "scope": "conversation",
            "model_id": self.args.model,
            "worker_id": worker_id or self.default_worker_id,
            "cache_key_hash": cache_key_hash,
            "manifest_id": manifest_id,
            "cache_hit_level": cache_hit_level,
            "compatibility_result": compatibility_result,
            "validation_status": "validated" if compatibility_result == "match" else "not_applicable",
            "fallback_required": fallback_required,
            "fallback_reason": fallback_reason,
            "latency_ms": latency_ms,
            "metrics": {
                "decision_latency_ms": latency_ms,
                "registry_lookup_latency_ms": None,
                "hydration_latency_ms": hydration_latency_ms,
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
        append_jsonl(self.events_path, row)

    def build_cache(self, *, cache_id: str, prefix_text: str, refresh: bool = False, worker_id: str | None = None) -> dict[str, Any]:
        with self.lock:
            worker = self.select_worker(worker_id)
            key_fields = self.cache_key_fields(cache_id, prefix_text, worker)
            cache_key_hash = sha256_json(key_fields)
            existing = self.find_entry(cache_id)
            if existing and not refresh and existing.get("cache_key_hash") == cache_key_hash:
                return {"cache_id": cache_id, "cache_key_hash": cache_key_hash, "cache_exists": True, "entry": existing}

            slot_filename = f"cache-router-openai-{cache_key_hash[:16]}.slot"
            slot_action(worker.url, worker.slot_id, "erase", None, self.args.timeout)
            comp, build_wall_ms = completion(
                worker.url,
                prefix_text,
                n_predict=0,
                slot_id=worker.slot_id,
                timeout=self.args.timeout,
            )
            save_body, save_wall_ms = slot_action(worker.url, worker.slot_id, "save", slot_filename, self.args.timeout)
            slot_info = worker.transport.file_info(slot_filename, hash_file=True)
            if not slot_info.exists or not slot_info.sha256:
                raise RuntimeError(f"slot save did not create expected file: {worker.transport.display_slot_path(slot_filename)}")
            slot_hash = slot_info.sha256
            blob_path = self.blobs / slot_hash[:2] / f"{slot_hash}.slot"
            ingest_start = time.perf_counter()
            ingest = worker.transport.upload_to_router(slot_filename, blob_path)
            blob_hash = ingest["sha256"]
            ingest_wall_ms = (time.perf_counter() - ingest_start) * 1000.0
            if blob_hash != slot_hash:
                raise RuntimeError("router blob hash mismatch after ingest")

            manifest_id = "manifest-" + cache_key_hash[:16]
            manifest_path = self.manifests / f"{cache_key_hash}.json"
            manifest = {
                "schema_version": "2026-07-01.1",
                "cache_id": cache_id,
                "cache_key_hash": cache_key_hash,
                "manifest_id": manifest_id,
                "source_worker_id": worker.worker_id,
                "prefix_sha256": key_fields["prefix_sha256"],
                "prefix_token_count": key_fields["prefix_token_count"],
                "model": worker.model,
                "model_identity": worker.model_identity,
                "model_path": worker.model_path,
                "model_file_size": worker.model_file_size,
                "llama_server_path": worker.llama_server_path,
                "llama_server_version": worker.llama_server_version,
                "ctx_size": worker.ctx_size,
                "cache_type_k": worker.cache_type_k,
                "cache_type_v": worker.cache_type_v,
                "mtp_enabled": worker.mtp_enabled,
                "spec_draft_model_identity": worker.spec_draft_model_identity,
                "spec_draft_model_path": worker.spec_draft_model_path,
                "spec_draft_model_size": worker.spec_draft_model_size,
                "slot_file_sha256": slot_hash,
                "slot_file_size_bytes": ingest["size_bytes"],
                "slot_filename": slot_filename,
                "router_blob_path": str(blob_path),
                "worker_slot_path": worker.transport.display_slot_path(slot_filename),
                "worker_transport": worker.transport.describe(),
                "created_at": now_iso(),
                "last_used_at": None,
                "worker_residency": {worker.worker_id: True},
            }
            write_json(manifest_path, manifest)
            registry = self.load_registry()
            registry["entries"] = [row for row in registry.get("entries", []) if row.get("cache_id") != cache_id]
            registry["entries"].append(
                {
                    "cache_id": cache_id,
                    "cache_key_hash": cache_key_hash,
                    "manifest_id": manifest_id,
                    "manifest_path": str(manifest_path),
                    "router_blob_path": str(blob_path),
                    "slot_filename": slot_filename,
                    "slot_file_sha256": slot_hash,
                    "slot_file_size_bytes": ingest["size_bytes"],
                    "created_at": manifest["created_at"],
                    "last_used_at": None,
                    "source_worker_id": worker.worker_id,
                    "worker_residency": {worker.worker_id: True},
                }
            )
            self.save_registry(registry)
            timings = comp.get("timings") if isinstance(comp.get("timings"), dict) else {}
            self.emit_event(
                phase="cache_commit_published",
                decision="no_op",
                cache_id=cache_id,
                cache_key_hash=cache_key_hash,
                manifest_id=manifest_id,
                cache_hit_level="durable_blob",
                compatibility_result="match",
                latency_ms=ingest_wall_ms,
                prompt_tokens=comp.get("tokens_evaluated"),
                processed_prompt_tokens=comp.get("tokens_evaluated"),
                cached_tokens=comp.get("tokens_cached") or 0,
                generated_tokens=comp.get("tokens_predicted"),
                prompt_tps=timings.get("prompt_per_second"),
                eval_tps=timings.get("predicted_per_second"),
                worker_id=worker.worker_id,
                notes="Router daemon built prefix cache and published durable blob.",
            )
            return {
                "cache_id": cache_id,
                "worker_id": worker.worker_id,
                "cache_key_hash": cache_key_hash,
                "manifest_id": manifest_id,
                "prefix_tokens": key_fields["prefix_token_count"],
                "slot_filename": slot_filename,
                "slot_file_sha256": slot_hash,
                "slot_file_size_bytes": ingest["size_bytes"],
                "build_prompt_ms": timings.get("prompt_ms"),
                "build_wall_ms": build_wall_ms,
                "save_ms": (save_body.get("timings") or {}).get("save_ms"),
                "save_wall_ms": save_wall_ms,
                "ingest_ms": ingest_wall_ms,
                "router_blob_path": str(blob_path),
                "manifest_path": str(manifest_path),
                "n_saved": save_body.get("n_saved"),
                "completion": {
                    "tokens_evaluated": comp.get("tokens_evaluated"),
                    "tokens_predicted": comp.get("tokens_predicted"),
                    "tokens_cached": comp.get("tokens_cached"),
                    "timings": timings,
                },
            }

    def ensure_entry(self, cache_id: str) -> dict[str, Any]:
        entry = self.find_entry(cache_id)
        if not entry:
            raise KeyError(f"cache_id not found: {cache_id}")
        manifest_path = Path(entry["manifest_path"])
        manifest = read_json(manifest_path, {})
        if not manifest:
            raise RuntimeError(f"manifest missing or empty: {manifest_path}")
        return {"entry": entry, "manifest": manifest}

    def hydrate_if_needed(self, manifest: dict[str, Any], worker: WorkerRuntime) -> dict[str, Any]:
        slot_filename = manifest["slot_filename"]
        slot_info = worker.transport.file_info(slot_filename, hash_file=True)
        before_exists = slot_info.exists
        expected_hash = manifest["slot_file_sha256"]
        if before_exists and slot_info.sha256 == expected_hash:
            return {
                "performed": False,
                "dest_existed_before": True,
                "sha256_match": True,
                "wall_ms": 0.0,
                "worker_slot_path": slot_info.path,
                "transport": worker.transport.describe(),
                "worker_id": worker.worker_id,
            }
        start = time.perf_counter()
        blob_path = Path(manifest["router_blob_path"])
        if not blob_path.is_file():
            raise RuntimeError(f"router blob missing: {blob_path}")
        hydrated = worker.transport.hydrate_from_router(blob_path, slot_filename)
        return {
            "performed": True,
            "dest_existed_before": before_exists,
            "worker_slot_path": hydrated["dest"],
            "source_blob_path": str(blob_path),
            "dest_sha256": hydrated["sha256"],
            "source_sha256": expected_hash,
            "sha256_match": hydrated["sha256_match"],
            "size_bytes": hydrated["size_bytes"],
            "transport": worker.transport.describe(),
            "worker_id": worker.worker_id,
            "wall_ms": (time.perf_counter() - start) * 1000.0,
        }

    def use_cache(self, *, cache_id: str, suffix_text: str, max_tokens: int, allow_fallback: bool = True, worker_id: str | None = None) -> dict[str, Any]:
        with self.lock:
            loaded = self.ensure_entry(cache_id)
            entry = loaded["entry"]
            manifest = loaded["manifest"]
            residency = manifest.get("worker_residency") if isinstance(manifest.get("worker_residency"), dict) else {}
            ordered = self.ordered_workers(worker_id, prefer_residency=residency, allow_fallback=allow_fallback)
            first_choice_worker_id = ordered[0].worker_id if ordered else None
            candidates = self.candidate_workers(worker_id, prefer_residency=residency, allow_fallback=allow_fallback)
            attempts: list[dict[str, Any]] = []
            for worker in candidates:
                try:
                    mismatch = self.worker_cache_compatibility_mismatch(manifest, worker)
                    if mismatch:
                        raise RuntimeError(f"incompatible cache for worker {worker.worker_id}: {mismatch}")
                    hydrate = self.hydrate_if_needed(manifest, worker)
                    if not hydrate.get("sha256_match"):
                        raise RuntimeError("hydrated slot hash mismatch")
                    restore_body, restore_wall_ms = slot_action(worker.url, worker.slot_id, "restore", manifest["slot_filename"], self.args.timeout)
                    comp, wall_ms = completion(
                        worker.url,
                        suffix_text,
                        n_predict=max_tokens,
                        slot_id=worker.slot_id,
                        timeout=self.args.timeout,
                    )
                    timings = comp.get("timings") if isinstance(comp.get("timings"), dict) else {}
                    now = now_iso()
                    manifest["last_used_at"] = now
                    manifest.setdefault("worker_residency", {})[worker.worker_id] = True
                    write_json(Path(entry["manifest_path"]), manifest)
                    registry = self.load_registry()
                    for row in registry.get("entries", []):
                        if row.get("cache_id") == cache_id:
                            row["last_used_at"] = now
                            row.setdefault("worker_residency", {})[worker.worker_id] = True
                    self.save_registry(registry)
                    success_attempt = {
                        "worker_id": worker.worker_id,
                        "status": "success",
                        "cache_hit_level": "durable_blob" if hydrate.get("performed") else "local_nvme",
                        "hydration_performed": hydrate.get("performed"),
                    }
                    attempts.append(success_attempt)
                    self.emit_event(
                        phase="restore_validated",
                        decision="restore_then_generate",
                        cache_id=cache_id,
                        cache_key_hash=manifest["cache_key_hash"],
                        manifest_id=manifest.get("manifest_id"),
                        cache_hit_level="durable_blob" if hydrate.get("performed") else "local_nvme",
                        compatibility_result="match",
                        latency_ms=restore_wall_ms,
                        prompt_tokens=comp.get("tokens_evaluated"),
                        processed_prompt_tokens=comp.get("tokens_evaluated"),
                        cached_tokens=comp.get("tokens_cached"),
                        generated_tokens=comp.get("tokens_predicted"),
                        prompt_tps=timings.get("prompt_per_second"),
                        eval_tps=timings.get("predicted_per_second"),
                        restore_latency_ms=restore_wall_ms,
                        hydration_latency_ms=hydrate.get("wall_ms"),
                        worker_id=worker.worker_id,
                        notes="Router daemon restored cache and routed suffix-only request.",
                    )
                    return {
                        "cache_id": cache_id,
                        "worker_id": worker.worker_id,
                        "first_choice_worker_id": first_choice_worker_id,
                        "cache_key_hash": manifest["cache_key_hash"],
                        "manifest_id": manifest.get("manifest_id"),
                        "attempts": attempts,
                        "fallback_used": any(row.get("status") == "failed" for row in attempts)
                        or (first_choice_worker_id is not None and worker.worker_id != first_choice_worker_id),
                        "hydrate": hydrate,
                        "restore": {
                            "body": restore_body,
                            "wall_ms": restore_wall_ms,
                            "n_restored": restore_body.get("n_restored"),
                        },
                        "completion": {
                            "content": str(comp.get("content", "")),
                            "tokens_evaluated": comp.get("tokens_evaluated"),
                            "tokens_cached": comp.get("tokens_cached"),
                            "tokens_predicted": comp.get("tokens_predicted"),
                            "timings": timings,
                            "wall_ms": wall_ms,
                        },
                    }
                except Exception as exc:  # noqa: BLE001
                    error = f"{type(exc).__name__}: {exc}"
                    attempts.append({"worker_id": worker.worker_id, "status": "failed", "error": error[:500]})
                    manifest.setdefault("worker_residency", {})[worker.worker_id] = False
                    write_json(Path(entry["manifest_path"]), manifest)
                    registry = self.load_registry()
                    for row in registry.get("entries", []):
                        if row.get("cache_id") == cache_id:
                            row.setdefault("worker_residency", {})[worker.worker_id] = False
                    self.save_registry(registry)
                    self.emit_event(
                        phase="request_failed",
                        decision="fallback_after_restore_failure" if allow_fallback else "reject_capacity",
                        cache_id=cache_id,
                        cache_key_hash=manifest.get("cache_key_hash"),
                        manifest_id=manifest.get("manifest_id"),
                        cache_hit_level="durable_blob",
                        compatibility_result="match",
                        latency_ms=None,
                        prompt_tokens=None,
                        processed_prompt_tokens=None,
                        cached_tokens=None,
                        generated_tokens=None,
                        prompt_tps=None,
                        eval_tps=None,
                        fallback_required=True,
                        fallback_reason=error[:200],
                        worker_id=worker.worker_id,
                        notes="Cache restore/use attempt failed; router will try next eligible worker if available.",
                    )
                    if not allow_fallback:
                        break
            raise RuntimeError(f"cache restore/use failed on all candidate workers: {attempts}")


class RouterHandler(BaseHTTPRequestHandler):
    server_version = "CachyRouter/0.1"

    @property
    def state(self) -> CacheRouterState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("%s %s\n" % (now_iso(), fmt % args))

    def read_body(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc

    def send_json(self, status: int, body: Any) -> None:
        data = json.dumps(body, indent=None, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str, *, code: str = "cache_router_error") -> None:
        self.send_json(status, {"error": {"message": message, "type": code, "code": status}})

    def authorized(self) -> bool:
        token = self.state.auth_token
        if not token:
            return True
        auth = self.headers.get("Authorization", "")
        provided = ""
        if auth.lower().startswith("bearer "):
            provided = auth.split(" ", 1)[1].strip()
        if not provided:
            provided = (self.headers.get("X-API-Key") or self.headers.get("api-key") or "").strip()
        return hmac.compare_digest(provided, token)

    def require_auth(self) -> bool:
        if self.authorized():
            return True
        self.send_json(
            401,
            {
                "error": {
                    "message": "missing or invalid cache-router bearer token",
                    "type": "authentication_error",
                    "code": 401,
                }
            },
        )
        return False

    def proxy(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        preferred_worker_id: str | None = None,
        allow_fallback: bool = True,
    ) -> None:
        worker = self.state.select_worker(preferred_worker_id, allow_fallback=allow_fallback)
        availability = worker.availability()
        url = worker.url + path
        status, headers, raw, _ = http_request(method, url, payload=body, timeout=self.state.args.timeout)
        self.send_response(status)
        content_type = headers.get("content-type", "application/json")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Cache-Router-Worker", worker.worker_id)
        self.send_header("X-Cache-Router-Worker-Availability", str(availability.get("reason", "unknown")))
        self.send_header("X-Cache-Router-Worker-Busy-Score", str(availability.get("busy_score", "unknown")))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        try:
            if self.path == "/health":
                summaries = self.state.worker_summaries(include_slots=False)
                healthy = sum(1 for row in summaries if row.get("health", {}).get("ok"))
                body: dict[str, Any] = {"status": "ok" if healthy else "degraded"}
                if not self.state.auth_token or self.authorized():
                    body.update(
                        {
                            "router": {"pid": os.getpid(), "bind": self.state.args.host, "port": self.state.args.port},
                            "workers": summaries,
                            "worker_count": len(summaries),
                            "healthy_workers": healthy,
                            "cache_root": str(self.state.cache_root),
                        }
                    )
                self.send_json(200 if healthy else 503, body)
                return
            if not self.require_auth():
                return
            if self.path in {"/v1", "/v1/"}:
                self.send_json(
                    200,
                    {
                        "object": "cache_router.endpoint",
                        "status": "ok",
                        "base_url": "/v1",
                        "model": self.state.args.model,
                        "endpoints": [
                            "/v1/models",
                            "/v1/completions",
                            "/v1/chat/completions",
                            "/tokenize",
                            "/router/status",
                            "/router/workers",
                            "/router/cache",
                            "/router/decisions",
                        ],
                        "cache_extension": "optional cache_router object on completions/chat completions",
                    },
                )
            elif self.path == "/v1/models":
                self.send_json(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {
                                "id": self.state.args.model,
                                "object": "model",
                                "created": int(time.time()),
                                "owned_by": "llamacpp-cache-router",
                            }
                        ],
                    },
                )
            elif self.path == "/router/status":
                registry = self.state.load_registry()
                workers = self.state.worker_summaries(include_slots=False)
                self.send_json(
                    200,
                    {
                        "status": "ok",
                        "router": {"pid": os.getpid(), "bind": self.state.args.host, "port": self.state.args.port},
                        "workers": workers,
                        "worker_count": len(workers),
                        "healthy_workers": sum(1 for row in workers if row.get("health", {}).get("ok")),
                        "cache_root": str(self.state.cache_root),
                        "registry_entries": len(registry.get("entries", [])),
                    },
                )
            elif self.path == "/router/workers":
                workers = self.state.worker_summaries()
                self.send_json(200, {"workers": workers, "count": len(workers), "healthy": sum(1 for row in workers if row.get("health", {}).get("ok"))})
            elif self.path == "/router/cache":
                registry = self.state.load_registry()
                entries = []
                for row in registry.get("entries", []):
                    entries.append(
                        {
                            "cache_id": row.get("cache_id"),
                            "cache_key_hash": row.get("cache_key_hash"),
                            "manifest_id": row.get("manifest_id"),
                            "slot_file_sha256": row.get("slot_file_sha256"),
                            "slot_file_size_bytes": row.get("slot_file_size_bytes"),
                            "created_at": row.get("created_at"),
                            "last_used_at": row.get("last_used_at"),
                            "worker_residency": row.get("worker_residency"),
                        }
                    )
                self.send_json(200, {"entries": entries, "count": len(entries)})
            elif self.path == "/router/decisions":
                rows = read_jsonl_tail(self.state.events_path, 100)
                self.send_json(200, {"events": rows, "count": len(rows), "limit": 100})
            else:
                self.send_error_json(404, f"unknown path: {self.path}", code="not_found")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc))

    def handle_cached_completion(self, body: dict[str, Any], *, chat: bool) -> None:
        extension = body.get("cache_router") or {}
        if not isinstance(extension, dict):
            self.send_error_json(400, "cache_router must be an object")
            return
        if body.get("stream"):
            self.send_error_json(400, "cached mode is currently non-streaming; use stream=false")
            return
        mode = str(extension.get("mode", "auto"))
        cache_id = str(extension.get("cache_id") or "default")
        requested_worker_id = extension.get("worker_id")
        if requested_worker_id is not None:
            requested_worker_id = str(requested_worker_id)
        allow_fallback = extension.get("allow_fallback", True)
        if not isinstance(allow_fallback, bool):
            self.send_error_json(400, "cache_router.allow_fallback must be a boolean")
            return
        prefix_text = extension.get("prefix_text")
        suffix_text = extension.get("suffix_text")
        if suffix_text is None:
            suffix_text = body.get("prompt", "")
        if not isinstance(suffix_text, str):
            self.send_error_json(400, "suffix_text or prompt must be a string")
            return
        max_tokens = int(body.get("max_tokens") or body.get("max_completion_tokens") or 16)
        if mode == "bypass":
            clean = dict(body)
            clean.pop("cache_router", None)
            self.proxy(
                "POST",
                "/v1/chat/completions" if chat else "/v1/completions",
                clean,
                preferred_worker_id=requested_worker_id,
                allow_fallback=allow_fallback,
            )
            return
        try:
            metadata: dict[str, Any] = {"mode": mode, "cache_id": cache_id}
            if requested_worker_id:
                metadata["requested_worker_id"] = requested_worker_id
            metadata["allow_fallback"] = allow_fallback
            if mode in {"build", "refresh"}:
                if not isinstance(prefix_text, str) or not prefix_text:
                    self.send_error_json(400, "cache_router.prefix_text is required for build/refresh")
                    return
                metadata["build"] = self.state.build_cache(cache_id=cache_id, prefix_text=prefix_text, refresh=(mode == "refresh"), worker_id=requested_worker_id)
                text = json.dumps({"cache_built": True, "cache_id": cache_id})
                self.send_json(200, openai_chat_response(model=self.state.args.model, content=text, cache_router=metadata) if chat else openai_completion_response(model=self.state.args.model, text=text, prompt_tokens=0, completion_tokens=0, cache_router=metadata))
                return
            if mode == "auto":
                if self.state.find_entry(cache_id) is None:
                    if not isinstance(prefix_text, str) or not prefix_text:
                        self.send_error_json(400, "cache miss in auto mode requires cache_router.prefix_text")
                        return
                    metadata["build"] = self.state.build_cache(cache_id=cache_id, prefix_text=prefix_text, refresh=False, worker_id=requested_worker_id)
                use = self.state.use_cache(cache_id=cache_id, suffix_text=suffix_text, max_tokens=max_tokens, worker_id=requested_worker_id, allow_fallback=allow_fallback)
            elif mode == "use":
                use = self.state.use_cache(cache_id=cache_id, suffix_text=suffix_text, max_tokens=max_tokens, worker_id=requested_worker_id, allow_fallback=allow_fallback)
            else:
                self.send_error_json(400, f"unsupported cache_router mode: {mode}")
                return
            metadata["use"] = use
            content = use["completion"]["content"]
            prompt_tokens = int(use["completion"].get("tokens_evaluated") or 0)
            completion_tokens = int(use["completion"].get("tokens_predicted") or 0)
            if chat:
                response = openai_chat_response(model=self.state.args.model, content=content, cache_router=metadata)
                response["usage"] = {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                }
            else:
                response = openai_completion_response(
                    model=self.state.args.model,
                    text=content,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    cache_router=metadata,
                )
            self.send_json(200, response)
        except KeyError as exc:
            self.send_error_json(404, str(exc), code="cache_not_found")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc))

    def do_POST(self) -> None:  # noqa: N802
        try:
            if not self.require_auth():
                return
            body = self.read_body()
            if not isinstance(body, dict):
                self.send_error_json(400, "request body must be a JSON object")
                return
            if self.path == "/v1/completions":
                if "cache_router" in body:
                    self.handle_cached_completion(body, chat=False)
                else:
                    self.proxy("POST", self.path, body)
            elif self.path == "/v1/chat/completions":
                if "cache_router" in body:
                    self.handle_cached_completion(body, chat=True)
                else:
                    self.proxy("POST", self.path, body)
            elif self.path == "/tokenize":
                self.proxy("POST", self.path, body)
            else:
                self.send_error_json(404, f"unknown path: {self.path}", code="not_found")
        except ValueError as exc:
            self.send_error_json(400, str(exc), code="invalid_json")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--worker-url", default="http://127.0.0.1:18082")
    parser.add_argument("--worker-id", default="worker-main")
    parser.add_argument("--workers-file", default="", help="Optional JSON inventory with a workers list. If omitted, single-worker CLI flags are used.")
    parser.add_argument("--auth-token", default="", help="Optional bearer token for routers that should require client auth.")
    parser.add_argument("--auth-token-file", default="", help="Optional file containing a bearer token for client auth.")
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--worker-slot-dir", default="")
    parser.add_argument("--worker-transport", choices=["local", "ssh", "http"], default="local")
    parser.add_argument("--worker-ssh-host", default="")
    parser.add_argument("--worker-sidecar-url", default="")
    parser.add_argument("--ssh-config", default="")
    parser.add_argument("--ssh-extra-args", default="")
    parser.add_argument("--scp-extra-args", default="")
    parser.add_argument("--model", default="Step-3.7")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--model-file-size", type=int, default=0)
    parser.add_argument("--llama-server-path", required=True)
    parser.add_argument("--llama-server-version", default="unknown")
    parser.add_argument("--ctx-size", type=int, default=65536)
    parser.add_argument("--cache-type-k", default="q8_0")
    parser.add_argument("--cache-type-v", default="q8_0")
    parser.add_argument("--mtp-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--spec-draft-model-path", default="")
    parser.add_argument("--spec-draft-model-size", type=int, default=0)
    parser.add_argument("--slot-id", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=900.0)
    args = parser.parse_args()
    if not args.workers_file and not args.worker_slot_dir:
        parser.error("--worker-slot-dir is required unless --workers-file is provided")
    return args


def main() -> int:
    args = parse_args()
    state = CacheRouterState(args)
    server = ThreadingHTTPServer((args.host, args.port), RouterHandler)
    server.state = state  # type: ignore[attr-defined]
    print(json.dumps({"status": "starting", "pid": os.getpid(), "bind": args.host, "port": args.port, "cache_root": args.cache_root}), flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
