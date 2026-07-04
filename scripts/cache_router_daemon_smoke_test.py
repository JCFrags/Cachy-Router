#!/usr/bin/env python3
"""Offline smoke test for the cache-router daemon.

This starts a tiny in-process fake worker and a router bound to loopback. It
does not contact private hosts, does not require a model, and does not exercise
runtime slot restore correctness. It verifies the API/metrics/auth mechanics
that can be proven without live `llama-server` workers.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

import cache_router_daemon  # noqa: E402


class FakeWorkerState:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.slot_dir: Path | None = None
        self.healthy = True
        self.models_ready = True
        self.model_ids = ["fake-model"]
        self.completion_loading = False
        self.completion_delay_seconds = 0.0
        self.crash_next_openai_completion = False
        self.completion_tokens_evaluated = 1
        self.completion_tokens_cached = 16
        self.native_completion_contents: list[str] = []
        self.stream_delay_seconds = 0.0
        self.stream_finished = False
        self.slot_is_processing = False
        self.slot_has_next_token = False
        self.fail_restore = False
        self.tokenize_calls = 0
        self.llama_completion_calls = 0
        self.slot_restore_calls = 0
        self.slot_save_calls = 0
        self.slot_erase_calls = 0


class FakeSidecarState:
    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.healthy = True
        self.health_calls = 0


class FakeSidecarHandler(BaseHTTPRequestHandler):
    server_version = "CachyRouterFakeSidecar/0.1"

    @property
    def fake_sidecar_state(self) -> FakeSidecarState:
        return self.server.fake_sidecar_state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def send_json(self, status: int, body: Any) -> None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self.fake_sidecar_state.health_calls += 1
            if not self.fake_sidecar_state.healthy:
                self.send_json(503, {"ok": False, "status": "unhealthy", "worker_id": self.fake_sidecar_state.worker_id})
                return
            self.send_json(200, {"ok": True, "status": "ok", "worker_id": self.fake_sidecar_state.worker_id})
            return
        self.send_json(404, {"ok": False, "error": "not found"})


class FakeWorkerHandler(BaseHTTPRequestHandler):
    server_version = "CachyRouterFakeWorker/0.1"

    @property
    def fake_state(self) -> FakeWorkerState:
        return self.server.fake_state  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def send_json(self, status: int, body: Any) -> None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw.decode("utf-8"))
        if not isinstance(body, dict):
            raise ValueError("expected object body")
        self.fake_state.requests.append({"path": self.path, "body": body, "headers": {k.lower(): v for k, v in self.headers.items()}})
        return body

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            if not self.fake_state.healthy:
                self.send_json(503, {"status": "loading"})
                return
            self.send_json(200, {"status": "ok"})
            return
        if self.path == "/slots":
            self.send_json(
                200,
                {
                    "slots": [
                        {
                            "id": 0,
                            "is_processing": self.fake_state.slot_is_processing,
                            "next_token": [{"has_next_token": True, "n_decoded": 0, "n_remain": -1}]
                            if self.fake_state.slot_has_next_token
                            else [],
                            "n_prompt_tokens": 128 if self.fake_state.slot_is_processing or self.fake_state.slot_has_next_token else 0,
                            "n_prompt_tokens_processed": 64 if self.fake_state.slot_is_processing else 128 if self.fake_state.slot_has_next_token else 0,
                        }
                    ]
                },
            )
            return
        if self.path == "/v1/models":
            if not self.fake_state.models_ready:
                self.send_json(503, {"error": {"message": "Loading model", "type": "server_error"}, "status": "loading"})
                return
            self.send_json(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": model_id,
                            "object": "model",
                            "owned_by": "fake-worker",
                            "meta": {"size": 123, "architecture": "fake-arch", "n_params": 123456, "n_embd": 128, "n_ctx": 8192},
                        }
                        for model_id in self.fake_state.model_ids
                    ],
                },
            )
            return
        if self.path == "/props":
            self.send_json(
                200,
                {
                    "model_path": "/models/fake.gguf",
                    "model_alias": "fake-model",
                    "llama_server_version": "version: fake-server (abcdef123456)",
                    "chat_template": "{{ bos_token }}{% for message in messages %}{{ message.role }}: {{ message.content }}{% endfor %}",
                    "total_slots": 2,
                    "default_generation_settings": {
                        "flash_attn": True,
                        "rope_freq_base": 1000000,
                        "rope_freq_scale": 1,
                        "reasoning_format": "deepseek",
                    },
                },
            )
            return
        self.send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        body = self.read_json_body()
        if self.path == "/tokenize":
            self.fake_state.tokenize_calls += 1
            content = body.get("content", body.get("prompt", ""))
            token_count = max(1, len(str(content).split()))
            self.send_json(200, {"tokens": list(range(token_count))})
            return
        if self.path == "/completion":
            self.fake_state.llama_completion_calls += 1
            if self.fake_state.completion_loading:
                self.send_json(503, {"error": {"message": "Loading model", "type": "server_error"}, "status": "loading"})
                return
            n_predict = int(body.get("n_predict") or 1)
            content = self.fake_state.native_completion_contents.pop(0) if self.fake_state.native_completion_contents else "cached-ok"
            if body.get("stream") is True:
                self.fake_state.stream_finished = False
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                chunks = [
                    json.dumps(
                        {
                            "content": content[:1] or "c",
                            "tokens_evaluated": self.fake_state.completion_tokens_evaluated,
                            "tokens_cached": self.fake_state.completion_tokens_cached,
                            "tokens_predicted": 1,
                        },
                        sort_keys=True,
                    ).encode("utf-8"),
                    json.dumps(
                        {
                            "content": content[1:],
                            "tokens_evaluated": self.fake_state.completion_tokens_evaluated,
                            "tokens_cached": self.fake_state.completion_tokens_cached,
                            "tokens_predicted": n_predict,
                            "timings": {"prompt_per_second": 1000.0, "predicted_per_second": 100.0},
                        },
                        sort_keys=True,
                    ).encode("utf-8"),
                    b"[DONE]",
                ]
                for index, chunk in enumerate(chunks):
                    if index == 0 and self.fake_state.stream_delay_seconds > 0:
                        time.sleep(self.fake_state.stream_delay_seconds)
                    self.wfile.write(b"data: " + chunk + b"\n\n")
                    self.wfile.flush()
                self.fake_state.stream_finished = True
                return
            self.send_json(
                200,
                {
                    "content": content,
                    "tokens_evaluated": self.fake_state.completion_tokens_evaluated,
                    "tokens_cached": self.fake_state.completion_tokens_cached,
                    "tokens_predicted": n_predict,
                    "timings": {"prompt_per_second": 1000.0, "predicted_per_second": 100.0, "ttft_ms": 12.5},
                },
            )
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/slots/"):
            action = (urllib.parse.parse_qs(parsed.query).get("action") or [""])[0]
            if action == "restore":
                self.fake_state.slot_restore_calls += 1
                if self.fake_state.fail_restore:
                    self.send_json(500, {"error": "restore failed"})
                    return
                self.send_json(200, {"n_restored": 16, "filename": body.get("filename")})
                return
            if action == "save":
                self.fake_state.slot_save_calls += 1
                filename = str(body.get("filename") or "")
                if self.fake_state.slot_dir is not None and filename and "/" not in filename and filename not in {".", ".."}:
                    self.fake_state.slot_dir.mkdir(parents=True, exist_ok=True)
                    payload = f"fake-slot:{filename}:save-{self.fake_state.slot_save_calls}\n".encode("utf-8")
                    (self.fake_state.slot_dir / filename).write_bytes(payload)
                self.send_json(200, {"n_saved": 16, "filename": body.get("filename")})
                return
            if action == "erase":
                self.fake_state.slot_erase_calls += 1
                self.send_json(200, {"erased": True})
                return
            self.send_json(400, {"error": f"unsupported slot action: {action}"})
            return
        if self.path == "/v1/completions":
            if self.fake_state.crash_next_openai_completion:
                self.fake_state.crash_next_openai_completion = False
                self.fake_state.healthy = False
                self.close_connection = True
                with contextlib.suppress(OSError):
                    self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if self.fake_state.completion_loading:
                self.send_json(503, {"error": {"message": "Loading model", "type": "server_error"}, "status": "loading"})
                return
            if self.fake_state.completion_delay_seconds > 0:
                time.sleep(self.fake_state.completion_delay_seconds)
            if body.get("stream") is True:
                self.fake_state.stream_finished = False
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                chunks = [
                    b'data: {"id":"cmpl-fake","choices":[{"index":0,"text":"o"}]}\n\n',
                    b'data: {"id":"cmpl-fake","choices":[{"index":0,"text":"k"}]}\n\n',
                    b"data: [DONE]\n\n",
                ]
                for index, chunk in enumerate(chunks):
                    if index == 0 and self.fake_state.stream_delay_seconds > 0:
                        time.sleep(self.fake_state.stream_delay_seconds)
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    if index == 0 and self.fake_state.stream_delay_seconds > 0:
                        time.sleep(self.fake_state.stream_delay_seconds)
                self.fake_state.stream_finished = True
                return
            self.send_json(
                200,
                {
                    "id": "cmpl-fake",
                    "object": "text_completion",
                    "model": body.get("model", "fake-model"),
                    "choices": [{"index": 0, "text": "ok", "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                },
            )
            return
        if self.path == "/v1/chat/completions":
            if self.fake_state.completion_loading:
                self.send_json(503, {"error": {"message": "Loading model", "type": "server_error"}, "status": "loading"})
                return
            if body.get("stream") is True:
                self.fake_state.stream_finished = False
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                chunks = [
                    b'data: {"id":"chatcmpl-fake","choices":[{"index":0,"delta":{"content":"o"}}]}\n\n',
                    b'data: {"id":"chatcmpl-fake","choices":[{"index":0,"delta":{"content":"k"}}]}\n\n',
                    b"data: [DONE]\n\n",
                ]
                for index, chunk in enumerate(chunks):
                    if index == 0 and self.fake_state.stream_delay_seconds > 0:
                        time.sleep(self.fake_state.stream_delay_seconds)
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    if index == 0 and self.fake_state.stream_delay_seconds > 0:
                        time.sleep(self.fake_state.stream_delay_seconds)
                self.fake_state.stream_finished = True
                return
            self.send_json(
                200,
                {
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "model": body.get("model", "fake-model"),
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                },
            )
            return
        self.send_json(404, {"error": "not found"})


def start_server(handler: type[BaseHTTPRequestHandler], *, state_attr: str | None = None, state: Any | None = None) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    if state_attr:
        setattr(server, state_attr, state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def request(method: str, url: str, *, body: Any | None = None, headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req_headers = {"Accept-Encoding": "identity", **(headers or {})}
    if body is not None:
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()


def raw_request(method: str, url: str, *, raw_body: bytes, headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
    req_headers = {"Accept-Encoding": "identity", "Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=raw_body, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()


def streaming_request_probe(method: str, url: str, *, body: Any, headers: dict[str, str], state: FakeWorkerState) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req_headers = {"Accept-Encoding": "identity", "Content-Type": "application/json", **headers}
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        first_line = resp.readline()
        first_elapsed_ms = (time.perf_counter() - started) * 1000.0
        finished_at_first_line = state.stream_finished
        rest = resp.read()
        total_elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "status": resp.status,
            "headers": {k.lower(): v for k, v in resp.headers.items()},
            "first_line": first_line,
            "rest": rest,
            "first_elapsed_ms": first_elapsed_ms,
            "total_elapsed_ms": total_elapsed_ms,
            "finished_at_first_line": finished_at_first_line,
        }


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def assert_router_debug_headers(headers: dict[str, str], message: str, *, worker: str | None = "none") -> None:
    request_id = headers.get("x-cache-router-request-id", "")
    trace_id = headers.get("x-cache-router-trace-id", "")
    assert_true(request_id.startswith("req-"), f"{message}: missing router-owned request ID")
    assert_true(trace_id.startswith("trace-"), f"{message}: missing router-owned trace ID")
    if worker is not None:
        assert_true(headers.get("x-cache-router-worker") == worker, f"{message}: unexpected router worker header")


def current_rss_bytes() -> int | None:
    status_path = Path("/proc/self/status")
    if not status_path.exists():
        return None
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                return int(parts[1]) * 1024
            except ValueError:
                return None
    return None


def metric_has_line(metrics: str, name: str, labels: dict[str, str]) -> bool:
    prefix = name + "{"
    for line in metrics.splitlines():
        if not line.startswith(prefix):
            continue
        if all(f'{key}="{value}"' in line for key, value in labels.items()):
            return True
    return False


def metric_value(metrics: str, name: str, labels: dict[str, str]) -> float | None:
    prefix = name + "{"
    for line in metrics.splitlines():
        if not line.startswith(prefix):
            continue
        if all(f'{key}="{value}"' in line for key, value in labels.items()):
            try:
                return float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                return None
    return None


def state_metric_counter(state: cache_router_daemon.CacheRouterState, bucket: str, labels: dict[str, Any]) -> int:
    key = tuple(sorted((str(key), str(value)) for key, value in labels.items()))
    with state.metrics_lock:
        values = state.metrics.get(bucket, {})
        if not isinstance(values, dict):
            return 0
        return int(values.get(key, 0))


def total_worker_requests(states: list[FakeWorkerState]) -> int:
    return sum(len(state.requests) for state in states)


def fake_sha(label: str) -> str:
    return hashlib.sha256(f"cache-router-smoke:{label}".encode("utf-8")).hexdigest()


def strict_worker_metadata() -> dict[str, Any]:
    return {
        "model_architecture": "synthetic-architecture",
        "model_hash": fake_sha("model"),
        "gguf_tensor_manifest_hash": fake_sha("gguf-tensor-manifest"),
        "tokenizer_hash": fake_sha("tokenizer"),
        "chat_template_effective_hash": fake_sha("chat-template"),
        "tools_schema_hash": fake_sha("tools-schema"),
        "system_prompt_hash": fake_sha("system-prompt"),
        "special_token_policy": "synthetic-special-tokens-v1",
        "llama_cpp_source_commit": "fakecommit1234",
        "llama_cpp_cache_abi_version": "cache-abi-smoke-v1",
        "patchset_id": "none",
        "build_backend": "vulkan_radv",
        "gpu_backend_driver": "radv-smoke",
        "kv_unified_mode": True,
        "ctx_checkpoints_config": "ctx-checkpoints-smoke",
        "flash_attention_mode": "on",
        "rope_freq_base": "default",
        "rope_freq_scale": "default",
        "yarn_or_rope_scaling_metadata": "none",
        "reasoning_format": "deepseek",
        "jinja_template_mode": "enabled",
        "spec_draft_model_hash": "none",
        "spec_draft_config": "none",
        "n_parallel": 1,
        "n_seq_max": 1,
    }


def wait_until(predicate: Any, message: str, *, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def worker_cache_attempts(*states: FakeWorkerState) -> dict[str, int]:
    return {
        "restore": sum(state.slot_restore_calls for state in states),
        "completion": sum(state.llama_completion_calls for state in states),
    }


def openai_completion_requests(*states: FakeWorkerState) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for state in states:
        rows.extend(row for row in state.requests if row.get("path") == "/v1/completions")
    return rows


def seed_cache_entry(
    state: cache_router_daemon.CacheRouterState,
    cache_id: str,
    *,
    worker_id: str = "worker-main",
    slot_bytes: bytes | None = None,
    hot_local: bool = True,
    durable_blob: bool = True,
    prefix_text: str = "prefix",
    manifest_overrides: dict[str, Any] | None = None,
    registry_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    worker = state.worker_by_id[worker_id]
    payload = slot_bytes or (f"slot-payload-{cache_id}".encode("utf-8"))
    slot_hash = hashlib.sha256(payload).hexdigest()
    created_at = cache_router_daemon.now_iso()
    key_record: dict[str, Any] = {
        "cache_id": cache_id,
        "scope": "conversation",
        "tenant_hash": cache_router_daemon.TENANT_HASH,
        "conversation_hash": cache_router_daemon.CONVERSATION_HASH,
        "policy_id_hash": cache_router_daemon.POLICY_HASH,
        "prefix_sha256": hashlib.sha256(prefix_text.encode("utf-8")).hexdigest(),
        "prefix_token_count": max(1, len(prefix_text.split())),
        "model_identity": worker.model_identity,
        "model_file_size": worker.model_file_size,
        "llama_server_version": worker.llama_server_version,
        "ctx_size": worker.ctx_size,
        "cache_type_k": worker.cache_type_k,
        "cache_type_v": worker.cache_type_v,
        "mtp_enabled": worker.mtp_enabled,
        "spec_draft_model_identity": worker.spec_draft_model_identity,
        "spec_draft_model_size": worker.spec_draft_model_size,
        "model_architecture": worker.model_architecture,
        "model_hash": worker.model_hash,
        "gguf_tensor_manifest_hash": worker.gguf_tensor_manifest_hash,
        "tokenizer_hash": worker.tokenizer_hash,
        "chat_template_effective_hash": worker.chat_template_effective_hash,
        "tools_schema_hash": worker.tools_schema_hash,
        "system_prompt_hash": worker.system_prompt_hash,
        "special_token_policy": worker.special_token_policy,
        "llama_cpp_source_commit": worker.llama_cpp_source_commit,
        "llama_cpp_cache_abi_version": worker.llama_cpp_cache_abi_version,
        "patchset_id": worker.patchset_id,
        "build_backend": worker.build_backend,
        "gpu_backend_driver": worker.gpu_backend_driver,
        "kv_unified_mode": worker.kv_unified_mode,
        "ctx_checkpoints_config": worker.ctx_checkpoints_config,
        "flash_attention_mode": worker.flash_attention_mode,
        "rope_freq_base": worker.rope_freq_base,
        "rope_freq_scale": worker.rope_freq_scale,
        "yarn_or_rope_scaling_metadata": worker.yarn_or_rope_scaling_metadata,
        "reasoning_format": worker.reasoning_format,
        "jinja_template_mode": worker.jinja_template_mode,
        "spec_draft_model_hash": worker.spec_draft_model_hash,
        "spec_draft_config": worker.spec_draft_config,
        "n_parallel": worker.n_parallel,
        "n_seq_max": worker.n_seq_max,
    }
    if manifest_overrides:
        candidate_key_record = dict(key_record)
        for key, value in manifest_overrides.items():
            if key not in cache_router_daemon.CACHE_KEY_FIELDS:
                continue
            if value is None:
                candidate_key_record.pop(key, None)
            else:
                candidate_key_record[key] = value
        try:
            cache_router_daemon.cache_key_hash_from_record(candidate_key_record, label=f"seed cache {cache_id}")
        except RuntimeError:
            pass
        else:
            key_record = candidate_key_record
    cache_key_hash = cache_router_daemon.cache_key_hash_from_record(key_record, label=f"seed cache {cache_id}")
    manifest_id = "manifest-" + cache_key_hash[:16]
    slot_filename = f"cache-router-openai-{cache_key_hash[:16]}.slot"
    slot_path = Path(worker.slot_save_path) / slot_filename
    if hot_local:
        slot_path.parent.mkdir(parents=True, exist_ok=True)
        slot_path.write_bytes(payload)
    elif slot_path.exists():
        slot_path.unlink()
    blob_path = state.blobs / slot_hash[:2] / f"{slot_hash}.slot"
    if durable_blob:
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        blob_path.write_bytes(payload)
    elif blob_path.exists():
        blob_path.unlink()
    manifest_path = state.manifests / f"{cache_key_hash}.json"
    manifest: dict[str, Any] = {
        "schema_version": "2026-07-01.1",
        **key_record,
        "cache_key_hash": cache_key_hash,
        "manifest_id": manifest_id,
        "source_worker_id": worker.worker_id,
        "model": worker.model,
        "model_path": worker.model_path,
        "llama_server_path": worker.llama_server_path,
        "spec_draft_model_path": worker.spec_draft_model_path,
        "slot_file_sha256": slot_hash,
        "slot_file_size_bytes": len(payload),
        "slot_filename": slot_filename,
        "router_blob_path": str(blob_path),
        "worker_slot_path": worker.transport.display_slot_path(slot_filename),
        "worker_transport": worker.transport.describe(),
        "created_at": created_at,
        "last_used_at": None,
        "validation_status": "validated",
        "worker_residency": {worker.worker_id: hot_local},
    }
    if manifest_overrides:
        for key, value in manifest_overrides.items():
            if value is None:
                manifest.pop(key, None)
            else:
                manifest[key] = value
    cache_router_daemon.write_json(manifest_path, manifest)
    entry: dict[str, Any] = {
        "cache_id": cache_id,
        "cache_key_hash": cache_key_hash,
        "manifest_id": manifest_id,
        "manifest_path": str(manifest_path),
        "scope": manifest.get("scope"),
        "tenant_hash": manifest.get("tenant_hash"),
        "conversation_hash": manifest.get("conversation_hash"),
        "policy_id_hash": manifest.get("policy_id_hash"),
        "router_blob_path": str(blob_path),
        "slot_filename": slot_filename,
        "slot_file_sha256": slot_hash,
        "slot_file_size_bytes": len(payload),
        "created_at": created_at,
        "last_used_at": None,
        "source_worker_id": worker.worker_id,
        "validation_status": "validated",
        "worker_residency": {worker.worker_id: hot_local},
    }
    if registry_overrides:
        for key, value in registry_overrides.items():
            if value is None:
                entry.pop(key, None)
            else:
                entry[key] = value
    registry = state.load_registry()
    registry["entries"] = [row for row in registry.get("entries", []) if row.get("cache_id") != cache_id]
    registry["entries"].append(entry)
    state.save_registry(registry)
    return {"manifest": manifest, "entry": entry, "slot_path": slot_path, "blob_path": blob_path}


def registry_entry_for(state: cache_router_daemon.CacheRouterState, cache_id: str) -> dict[str, Any]:
    matches = [row for row in state.load_registry().get("entries", []) if row.get("cache_id") == cache_id]
    assert_true(len(matches) == 1, f"expected exactly one registry entry for {cache_id}")
    row = matches[0]
    assert_true(isinstance(row, dict), f"registry entry for {cache_id} should be an object")
    return row


def registry_entries_for(state: cache_router_daemon.CacheRouterState, cache_id: str) -> list[dict[str, Any]]:
    matches = [row for row in state.load_registry().get("entries", []) if row.get("cache_id") == cache_id]
    assert_true(all(isinstance(row, dict) for row in matches), f"registry entries for {cache_id} should be objects")
    return matches


def registry_entry_for_key(state: cache_router_daemon.CacheRouterState, cache_id: str, cache_key_hash: str) -> dict[str, Any]:
    matches = [row for row in registry_entries_for(state, cache_id) if row.get("cache_key_hash") == cache_key_hash]
    assert_true(len(matches) == 1, f"expected exactly one registry entry for {cache_id}/{cache_key_hash}")
    return matches[0]


def registry_audit_rows(state: cache_router_daemon.CacheRouterState) -> list[dict[str, Any]]:
    path = state.registry_audit_path
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def registry_wal_rows(state: cache_router_daemon.CacheRouterState) -> list[dict[str, Any]]:
    path = state.registry_wal_path
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def registry_audit_rows_for_request(state: cache_router_daemon.CacheRouterState, request_id: str) -> list[dict[str, Any]]:
    return [row for row in registry_audit_rows(state) if row.get("request_id") == request_id]


def assert_registry_audit_row(
    rows: list[dict[str, Any]],
    *,
    action: str,
    operation: str | None = None,
    outcome: str | None = None,
    message: str,
) -> dict[str, Any]:
    for row in rows:
        if action not in row.get("audit_actions", []):
            continue
        if operation is not None and row.get("operation") != operation:
            continue
        if outcome is not None and row.get("outcome") != outcome:
            continue
        assert_true((row.get("privacy") or {}).get("raw_prompt_logged") is False, f"{message}: raw prompt privacy flag should be false")
        assert_true((row.get("privacy") or {}).get("raw_cache_blob_path_logged") is False, f"{message}: raw blob path privacy flag should be false")
        return row
    raise AssertionError(message)


def router_args(worker_url: str, cache_root: Path, slot_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        host="127.0.0.1",
        port=0,
        worker_url=worker_url,
        worker_id="worker-main",
        workers_file="",
        auth_token="secret-token",
        auth_token_file="",
        cache_root=str(cache_root),
        production_mode=False,
        allow_production_admin_endpoints=False,
        auto_rebuild_registry=True,
        rebuild_registry=False,
        worker_slot_dir=str(slot_dir),
        worker_transport="local",
        worker_ssh_host="",
        worker_sidecar_url="",
        ssh_config="",
        ssh_extra_args="",
        scp_extra_args="",
        model="fake-model",
        model_path="/models/fake.gguf",
        model_file_size=123,
        model_architecture="synthetic-architecture",
        derive_strict_metadata=True,
        strict_metadata_force_runtime=False,
        strict_metadata_timeout=1.0,
        model_hash=fake_sha("model"),
        gguf_tensor_manifest_hash=fake_sha("gguf-tensor-manifest"),
        tokenizer_hash=fake_sha("tokenizer"),
        chat_template_effective_hash=fake_sha("chat-template"),
        tools_schema_hash=fake_sha("tools-schema"),
        system_prompt_hash=fake_sha("system-prompt"),
        special_token_policy="synthetic-special-tokens-v1",
        llama_server_path="/usr/bin/llama-server",
        llama_server_version="fake-commit",
        llama_cpp_source_commit="fakecommit1234",
        llama_cpp_cache_abi_version="cache-abi-smoke-v1",
        patchset_id="none",
        build_backend="vulkan_radv",
        gpu_backend_driver="radv-smoke",
        kv_unified_mode=True,
        ctx_size=4096,
        ctx_checkpoints_config="ctx-checkpoints-smoke",
        cache_type_k="q8_0",
        cache_type_v="q8_0",
        flash_attention_mode="on",
        rope_freq_base="default",
        rope_freq_scale="default",
        yarn_or_rope_scaling_metadata="none",
        reasoning_format="deepseek",
        jinja_template_mode="enabled",
        mtp_enabled=False,
        spec_draft_model_path="",
        spec_draft_model_size=0,
        spec_draft_model_hash="none",
        spec_draft_config="none",
        n_parallel=1,
        n_seq_max=1,
        slot_id=0,
        timeout=5.0,
        readiness_poll_interval=0.0,
        readiness_timeout=5.0,
        inventory_reload_interval=0.0,
        queue_limit_per_worker=0,
        queue_wait_timeout=0.0,
        disable_admin_endpoints=False,
    )


def inventory_worker_entry(worker_id: str, worker_url: str, slot_dir: Path, *, model: str = "fake-model", transport: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "worker_url": worker_url,
        "slot_save_path": str(slot_dir),
        "slot_id": 0,
        "model": model,
        "model_identity": "fake-model-identity",
        "model_path": "/models/fake.gguf",
        "model_file_size": 123,
        **strict_worker_metadata(),
        "llama_server_path": "/usr/bin/llama-server",
        "llama_server_version": "fake-commit",
        "ctx_size": 4096,
        "cache_type_k": "q8_0",
        "cache_type_v": "q8_0",
        "mtp_enabled": False,
        "transport": transport or {"kind": "local"},
    }


def write_inventory(path: Path, workers: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps({"workers": workers}, sort_keys=True), encoding="utf-8")


def daemon_parse_args(argv: list[str]) -> argparse.Namespace:
    original_argv = sys.argv[:]
    try:
        sys.argv = ["cache_router_daemon.py", *argv]
        return cache_router_daemon.parse_args()
    finally:
        sys.argv = original_argv


def assert_daemon_parse_fails(argv: list[str], message: str) -> None:
    original_argv = sys.argv[:]
    try:
        sys.argv = ["cache_router_daemon.py", *argv]
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                cache_router_daemon.parse_args()
        except SystemExit as exc:
            assert_true(exc.code != 0, message)
            return
        raise AssertionError(message)
    finally:
        sys.argv = original_argv


def crash_build_child_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Internal smoke-test child for crash-durable registry publication.")
    parser.add_argument("--worker-url", required=True)
    parser.add_argument("--workers-file", required=True)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--slot-dir", required=True)
    parser.add_argument("--cache-id", required=True)
    parser.add_argument("--prefix-text", required=True)
    ns = parser.parse_args(argv)

    args = router_args(ns.worker_url, Path(ns.cache_root), Path(ns.slot_dir))
    args.workers_file = ns.workers_file
    state = cache_router_daemon.CacheRouterState(args)

    def crash_save_registry(_registry: dict[str, Any]) -> None:
        os._exit(86)

    state.save_registry = crash_save_registry  # type: ignore[method-assign]
    state.build_cache(
        cache_id=ns.cache_id,
        prefix_text=ns.prefix_text,
        worker_id="worker-main",
        model="fake-model",
        request_id="req-crash-build-child",
        trace_id="trace-crash-build-child",
    )
    return 87


def main() -> int:
    fake_state = FakeWorkerState()
    fake_backup_state = FakeWorkerState()
    fake_added_state = FakeWorkerState()
    fake_incompatible_state = FakeWorkerState()
    fake_sidecar_state = FakeSidecarState("worker-http-sidecar")
    fake_worker = start_server(FakeWorkerHandler, state_attr="fake_state", state=fake_state)
    fake_backup_worker = start_server(FakeWorkerHandler, state_attr="fake_state", state=fake_backup_state)
    fake_added_worker = start_server(FakeWorkerHandler, state_attr="fake_state", state=fake_added_state)
    fake_incompatible_worker = start_server(FakeWorkerHandler, state_attr="fake_state", state=fake_incompatible_state)
    fake_sidecar = start_server(FakeSidecarHandler, state_attr="fake_sidecar_state", state=fake_sidecar_state)
    worker_host, worker_port = fake_worker.server_address[:2]
    worker_url = f"http://{worker_host}:{worker_port}"
    backup_host, backup_port = fake_backup_worker.server_address[:2]
    backup_url = f"http://{backup_host}:{backup_port}"
    added_host, added_port = fake_added_worker.server_address[:2]
    added_url = f"http://{added_host}:{added_port}"
    incompatible_host, incompatible_port = fake_incompatible_worker.server_address[:2]
    incompatible_url = f"http://{incompatible_host}:{incompatible_port}"
    sidecar_host, sidecar_port = fake_sidecar.server_address[:2]
    sidecar_url = f"http://{sidecar_host}:{sidecar_port}"
    router: ThreadingHTTPServer | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="cache-router-smoke-") as tmp:
            root = Path(tmp)
            fake_state.slot_dir = root / "worker-main-slots"
            fake_backup_state.slot_dir = root / "worker-backup-slots"
            fake_added_state.slot_dir = root / "worker-added-slots"
            fake_incompatible_state.slot_dir = root / "worker-incompatible-slots"
            derive_inventory = root / "derive-workers.json"
            write_inventory(
                derive_inventory,
                [
                    {
                        "worker_id": "worker-derive",
                        "worker_url": worker_url,
                        "slot_save_path": str(root / "worker-derive-slots"),
                        "slot_id": 0,
                        "model": "fake-model",
                        "model_identity": "hand-entered-stale-model-identity",
                        "model_path": "/models/fake.gguf",
                        "model_hash": fake_sha("hand-entered-stale-model"),
                        "llama_server_version": "hand-entered-stale-version",
                        "ctx_size": 111,
                        "kv_unified_mode": False,
                        "cache_type_k": "hand-entered-k",
                        "cache_type_v": "hand-entered-v",
                        "mtp_enabled": True,
                        "spec_draft_model_identity": "hand-entered-stale-draft",
                        "spec_draft_model_path": "/models/hand-entered-draft.gguf",
                        "spec_draft_model_size": 12345,
                        "spec_draft_model_hash": fake_sha("hand-entered-stale-draft"),
                        "spec_draft_config": "hand-entered-stale-draft-config",
                        "strict_metadata_force_runtime": True,
                        "transport": {"kind": "local"},
                    }
                ],
            )
            derive_args = router_args(worker_url, root / "derive-cache", root / "derive-slots")
            derive_args.workers_file = str(derive_inventory)
            derive_args.model_file_size = 0
            derive_args.model_architecture = "not_captured"
            derive_args.model_hash = "not_captured"
            derive_args.gguf_tensor_manifest_hash = "not_captured"
            derive_args.tokenizer_hash = "not_captured"
            derive_args.chat_template_effective_hash = "not_captured"
            derive_args.tools_schema_hash = "not_captured"
            derive_args.system_prompt_hash = "not_captured"
            derive_args.special_token_policy = "not_captured"
            derive_args.llama_server_version = "unknown"
            derive_args.llama_cpp_source_commit = "not_captured"
            derive_args.llama_cpp_cache_abi_version = "not_captured"
            derive_args.patchset_id = "not_captured"
            derive_args.build_backend = "not_captured"
            derive_args.gpu_backend_driver = "not_captured"
            derive_args.ctx_checkpoints_config = "not_captured"
            derive_args.flash_attention_mode = "not_captured"
            derive_args.rope_freq_base = "not_captured"
            derive_args.rope_freq_scale = "not_captured"
            derive_args.yarn_or_rope_scaling_metadata = "not_captured"
            derive_args.reasoning_format = "not_captured"
            derive_args.jinja_template_mode = "not_captured"
            derive_args.n_parallel = 0
            derive_args.n_seq_max = 0
            derive_state = cache_router_daemon.CacheRouterState(derive_args)
            derived_worker = derive_state.workers[0]
            assert_true(cache_router_daemon.is_sha256_hex(derived_worker.model_hash), "strict metadata derivation should compute model_hash")
            assert_true(derived_worker.model_hash != fake_sha("hand-entered-stale-model"), "force-runtime strict metadata should replace stale hand-entered model_hash")
            assert_true(
                derived_worker.model_identity != "hand-entered-stale-model-identity",
                "force-runtime strict metadata should replace stale hand-entered model_identity",
            )
            assert_true(derived_worker.llama_server_version == "version: fake-server (abcdef123456)", "force-runtime strict metadata should replace stale runtime version")
            assert_true(derived_worker.ctx_size == 8192, "force-runtime strict metadata should replace stale context size")
            assert_true(cache_router_daemon.is_sha256_hex(derived_worker.tokenizer_hash), "strict metadata derivation should compute tokenizer_hash")
            assert_true(derived_worker.llama_cpp_source_commit == "abcdef123456", "strict metadata derivation should parse runtime commit")
            assert_true(derived_worker.n_parallel == 2 and derived_worker.n_seq_max == 2, "strict metadata derivation should infer slot counts")
            assert_true(derived_worker.cache_type_k != "hand-entered-k", "force-runtime strict metadata should replace stale cache_type_k")
            assert_true(derived_worker.cache_type_v != "hand-entered-v", "force-runtime strict metadata should replace stale cache_type_v")
            assert_true(derived_worker.kv_unified_mode is True, "force-runtime strict metadata should replace stale kv_unified_mode")
            assert_true(derived_worker.spec_draft_config != "hand-entered-stale-draft-config", "force-runtime strict metadata should replace stale draft config")
            assert_true(derived_worker.spec_draft_model_hash != fake_sha("hand-entered-stale-draft"), "force-runtime strict metadata should replace stale draft hash")
            derive_variant_inventory = root / "derive-workers-variant.json"
            write_inventory(
                derive_variant_inventory,
                [
                    {
                        "worker_id": "worker-derive",
                        "worker_url": worker_url,
                        "slot_save_path": str(root / "worker-derive-slots"),
                        "slot_id": 0,
                        "model": "fake-model",
                        "model_identity": "another-hand-entered-model-identity",
                        "model_path": "/models/another-fake.gguf",
                        "model_hash": fake_sha("another-hand-entered-model"),
                        "llama_server_version": "another-hand-entered-version",
                        "ctx_size": 222,
                        "kv_unified_mode": False,
                        "cache_type_k": "another-hand-entered-k",
                        "cache_type_v": "another-hand-entered-v",
                        "mtp_enabled": True,
                        "spec_draft_model_identity": "another-hand-entered-draft",
                        "spec_draft_model_path": "/models/another-hand-entered-draft.gguf",
                        "spec_draft_model_size": 54321,
                        "spec_draft_model_hash": fake_sha("another-hand-entered-draft"),
                        "spec_draft_config": "another-hand-entered-draft-config",
                        "strict_metadata_force_runtime": True,
                        "transport": {"kind": "local"},
                    }
                ],
            )
            derive_variant_args = router_args(worker_url, root / "derive-cache-variant", root / "derive-slots-variant")
            derive_variant_args.workers_file = str(derive_variant_inventory)
            derive_variant_state = cache_router_daemon.CacheRouterState(derive_variant_args)
            derived_variant_worker = derive_variant_state.workers[0]
            assert_true(
                derived_variant_worker.llama_cpp_cache_abi_version == derived_worker.llama_cpp_cache_abi_version,
                "force-runtime ABI should not change when only hand-entered cache fields change",
            )
            assert_true(
                derived_variant_worker.spec_draft_model_hash == derived_worker.spec_draft_model_hash,
                "force-runtime draft hash should not change when only hand-entered draft fields change",
            )

            args = router_args(worker_url, root / "cache", root / "worker-slots")
            args.durable_blob_encryption_mode = "operator_managed_encrypted_filesystem"
            args.durable_blob_encryption_evidence_basis = "operator_attestation"
            args.durable_blob_encryption_volume_id_hash = fake_sha("smoke-encrypted-cache-root")
            args.durable_blob_encryption_key_owner = "operator"
            workers_file = root / "workers.json"
            workers_file.write_text(
                json.dumps(
                    {
                        "workers": [
                            {
                                "worker_id": "worker-main",
                                "worker_url": worker_url,
                                "slot_save_path": str(root / "worker-main-slots"),
                                "slot_id": 0,
                                "model": "fake-model",
                                "model_identity": "fake-model-identity",
                                "model_path": "/models/fake.gguf",
                                "model_file_size": 123,
                                **strict_worker_metadata(),
                                "llama_server_path": "/usr/bin/llama-server",
                                "llama_server_version": "fake-commit",
                                "ctx_size": 4096,
                                "cache_type_k": "q8_0",
                                "cache_type_v": "q8_0",
                                "mtp_enabled": False,
                                "transport": {"kind": "local"},
                            },
                            {
                                "worker_id": "worker-backup",
                                "worker_url": backup_url,
                                "slot_save_path": str(root / "worker-backup-slots"),
                                "slot_id": 0,
                                "model": "fake-model",
                                "model_identity": "fake-model-identity",
                                "model_path": "/models/fake.gguf",
                                "model_file_size": 123,
                                **strict_worker_metadata(),
                                "llama_server_path": "/usr/bin/llama-server",
                                "llama_server_version": "fake-commit",
                                "ctx_size": 4096,
                                "cache_type_k": "q8_0",
                                "cache_type_v": "q8_0",
                                "mtp_enabled": False,
                                "transport": {"kind": "local"},
                            },
                            {
                                "worker_id": "worker-incompatible",
                                "worker_url": incompatible_url,
                                "slot_save_path": str(root / "worker-incompatible-slots"),
                                "slot_id": 0,
                                "model": "incompatible-model",
                                "model_identity": "fake-model-identity",
                                "model_path": "/models/fake.gguf",
                                "model_file_size": 123,
                                **strict_worker_metadata(),
                                "llama_server_path": "/usr/bin/llama-server",
                                "llama_server_version": "fake-commit",
                                "ctx_size": 4096,
                                "cache_type_k": "q8_0",
                                "cache_type_v": "q8_0",
                                "mtp_enabled": False,
                                "transport": {"kind": "local"},
                            },
                            {
                                "worker_id": "worker-http-sidecar",
                                "worker_url": backup_url,
                                "slot_save_path": str(root / "worker-http-sidecar-slots"),
                                "slot_id": 0,
                                "model": "sidecar-only-model",
                                "model_identity": "fake-model-identity",
                                "model_path": "/models/fake.gguf",
                                "model_file_size": 123,
                                **strict_worker_metadata(),
                                "llama_server_path": "/usr/bin/llama-server",
                                "llama_server_version": "fake-commit",
                                "ctx_size": 4096,
                                "cache_type_k": "q8_0",
                                "cache_type_v": "q8_0",
                                "mtp_enabled": False,
                                "transport": {"kind": "http", "sidecar_url": sidecar_url},
                            },
                        ]
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            args.workers_file = str(workers_file)
            state = cache_router_daemon.CacheRouterState(args)
            worker_summary = state.worker_summaries(include_slots=False)[0]
            assert_true(worker_summary.get("strict_metadata_auto") is True, "worker summary should report runtime strict metadata derivation")
            assert_true(worker_summary.get("strict_metadata_force_runtime") is False, "worker summary should report default non-forced strict metadata derivation")
            assert_true(worker_summary.get("strict_metadata_source") == "runtime_fill_missing", "worker summary should distinguish runtime-filled strict metadata")
            assert_true(state.registry_audit_path == state.cache_root / "router-store" / "registry-audit.jsonl", "registry audit log should live under router-store in cache root")
            router = ThreadingHTTPServer(("127.0.0.1", 0), cache_router_daemon.RouterHandler)
            router.state = state  # type: ignore[attr-defined]
            thread = threading.Thread(target=router.serve_forever, daemon=True)
            thread.start()
            router_host, router_port = router.server_address[:2]
            base = f"http://{router_host}:{router_port}"

            status, headers, _ = request("GET", base + "/metrics")
            assert_true(status == 401, "metrics endpoint should require auth when router auth is configured")
            assert_router_debug_headers(headers, "unauthorized metrics response")
            status, headers, raw = request("GET", base + "/router/workers")
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 401, "admin inspection endpoint should require auth when router auth is configured")
            assert_true(body.get("error", {}).get("type") == "authentication_error", "unauthorized admin route should return authentication_error")
            assert_router_debug_headers(headers, "unauthorized admin response")

            auth = {"Authorization": "Bearer secret-token"}
            status, headers, raw = request("GET", base + "/health")
            health_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "unauthenticated health request should succeed")
            assert_router_debug_headers(headers, "health response")
            assert_true(health_body.get("security", {}).get("auth_required") is True, "health should expose non-secret auth-required posture")
            assert_true("workers" not in health_body, "unauthenticated auth-protected health should omit detailed worker rows")
            status, headers, raw = request("GET", base + "/metrics", headers=auth)
            metrics = raw.decode("utf-8")
            assert_true(status == 200, "authorized metrics request should succeed")
            assert_router_debug_headers(headers, "authorized metrics response")
            assert_true("cachy_router_active_requests" in metrics, "metrics should expose active request gauge")
            assert_true("cachy_router_worker_ready" in metrics, "metrics should expose worker readiness")
            assert_true("cachy_router_worker_sidecar_ready" in metrics, "metrics should expose sidecar readiness separately")

            status, headers, raw = request("GET", base + "/v1/models", headers=auth)
            models = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "healthy router should list configured models")
            assert_router_debug_headers(headers, "/v1/models response")
            assert_true(models.get("data", [{}])[0].get("id") == "fake-model", "/v1/models should include configured model")

            status, headers, raw = raw_request("POST", base + "/v1/completions", headers=auth, raw_body=b'{"model":')
            invalid_json = json.loads(raw.decode("utf-8"))
            assert_true(status == 400, "invalid JSON request should be rejected")
            assert_true(invalid_json.get("error", {}).get("type") == "invalid_json", "invalid JSON response should use invalid_json type")
            assert_router_debug_headers(headers, "invalid JSON response")

            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers={
                    "X-API-Key": "secret-token",
                    "X-Cache-Router-Worker": "client-forged-worker",
                    "X-Cache-Router-Request-ID": "client-forged-request",
                    "X-Cache-Router-Trace-ID": "client-forged-trace",
                },
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "normal completion should proxy through fake worker")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "proxy response should include selected worker header")
            first_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(first_request_id.startswith("req-") and first_request_id != "client-forged-request", "proxy response should include router-owned request ID")
            first_trace_id = headers.get("x-cache-router-trace-id", "")
            assert_true(first_trace_id.startswith("trace-") and first_trace_id != "client-forged-trace", "proxy response should include router-owned trace ID")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "completion response should come from fake worker")
            assert_true(fake_state.requests, "fake worker should record the first completion request")
            forwarded_headers = fake_state.requests[-1]["headers"]
            forwarded_internal_headers = [key for key in forwarded_headers if key.startswith("x-cache-router-")]
            assert_true(not forwarded_internal_headers, f"router should not forward client-supplied internal headers: {forwarded_internal_headers}")

            status, _, raw = request("GET", base + f"/router/decisions?request_id={first_request_id}", headers=auth)
            decisions = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "/router/decisions should return decision events")
            matching = decisions.get("events", [])
            assert_true(decisions.get("request_id") == first_request_id and len(matching) == 1, "decision request_id filter should return the matching event")
            first_event = matching[0]
            assert_true(first_event.get("worker_id") == "worker-main", "decision event should include selected worker")
            assert_true(first_event.get("phase") == "worker_selected", "decision event should classify normal worker selection")
            assert_true(first_event.get("decision") == "no_op", "normal proxy decision should be a non-cache no-op")
            assert_true(first_event.get("cache_hit_level") == "none", "normal proxy decision should not claim a cache hit")
            assert_true(first_event.get("fallback_required") is False, "initial normal proxy decision should not require fallback")
            assert_true(registry_audit_rows(state) == [], "normal pass-through should not create registry audit rows")
            first_scheduler = first_event.get("scheduler", {})
            first_candidates = first_scheduler.get("candidates", [])
            assert_true(first_scheduler.get("policy") == "availability_active_round_robin_v1", "normal decision should record scheduler policy")
            assert_true(first_scheduler.get("winner_worker_id") == "worker-main", "scheduler trace should record the winning worker")
            assert_true(isinstance(first_candidates, list) and len(first_candidates) >= 2, "scheduler trace should include candidate score inputs")
            assert_true(
                "worker-incompatible" not in {row.get("worker_id") for row in first_candidates},
                "normal model-lane routing should exclude configured workers for other models from the candidate trace",
            )
            assert_true(
                all({"worker_id", "eligible", "availability_reason", "busy_score", "active_requests", "rank"} <= set(row) for row in first_candidates),
                "scheduler trace should include bounded score inputs for every candidate",
            )
            assert_true(first_scheduler.get("winner_reason"), "scheduler trace should record why the winner won")
            assert_true(first_event.get("privacy", {}).get("raw_prompt_logged") is False, "decision event must not log raw prompts")
            assert_true(len(fake_incompatible_state.requests) == 0, "normal model-lane routing should not forward to an incompatible worker")

            status, _, raw = request("GET", base + "/router/workers", headers=auth)
            workers_body = json.loads(raw.decode("utf-8"))
            incompatible_worker = next(row for row in workers_body.get("workers", []) if row.get("worker_id") == "worker-incompatible")
            assert_true(status == 200, "/router/workers should report incompatible workers")
            assert_true(incompatible_worker.get("model") == "incompatible-model", "incompatible worker summary should retain its configured model")
            assert_true(incompatible_worker.get("model_readiness", {}).get("state") == "model_mismatch", "incompatible worker should be marked model_mismatch")
            assert_true(incompatible_worker.get("readiness", {}).get("ok") is False, "incompatible worker should not be route-ready")

            registry_before_bypass = state.load_registry()
            manifests_before_bypass = sorted(path.name for path in state.manifests.glob("*.json"))
            blobs_before_bypass = sorted(path.name for path in state.blobs.glob("*"))
            status, _, _ = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {"mode": "bypass", "cache_id": "strip-test"},
                },
            )
            assert_true(status == 200, "cache_router bypass should proxy successfully")
            forwarded = openai_completion_requests(fake_state, fake_backup_state)
            assert_true(len(forwarded) >= 2, "fake worker should receive proxied completion requests")
            assert_true("cache_router" not in forwarded[-1]["body"], "router must strip cache_router before forwarding bypass request")
            assert_true(state.load_registry() == registry_before_bypass, "bypass should not mutate registry entries")
            assert_true(sorted(path.name for path in state.manifests.glob("*.json")) == manifests_before_bypass, "bypass should not write manifests")
            assert_true(sorted(path.name for path in state.blobs.glob("*")) == blobs_before_bypass, "bypass should not write blobs")
            assert_true(registry_audit_rows(state) == [], "cache bypass should not create registry audit rows")

            build_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            build_erase_before = fake_state.slot_erase_calls
            build_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "build",
                        "cache_id": "build-success",
                        "prefix_text": "offline build prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            build_body = json.loads(raw.decode("utf-8"))
            build_meta = build_body.get("cache_router", {}).get("build", {})
            build_entry = registry_entry_for(state, "build-success")
            build_manifest = cache_router_daemon.read_json(Path(build_entry["manifest_path"]), {})
            build_slot_path = Path(str(build_manifest["worker_slot_path"]))
            build_blob_path = Path(str(build_manifest["router_blob_path"]))
            build_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 200, "cache mode=build should return an OpenAI-shaped success response")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "build response should identify the selected worker")
            assert_true(build_meta.get("cache_id") == "build-success", "build response should include cache id metadata")
            assert_true(build_entry.get("cache_key_hash") == build_meta.get("cache_key_hash"), "registry should publish the built cache key")
            assert_true(build_manifest.get("source_worker_id") == "worker-main", "built manifest should record source worker")
            assert_true(build_manifest.get("slot_file_sha256") == build_entry.get("slot_file_sha256"), "manifest and registry should agree on slot hash")
            assert_true(build_manifest.get("worker_residency", {}).get("worker-main") is True, "built manifest should mark local residency")
            assert_true(
                build_manifest.get("encryption_at_rest", {}).get("mode") == "operator_managed_encrypted_filesystem",
                "built manifest should carry operator-attested durable blob encryption metadata",
            )
            assert_true(
                build_entry.get("encryption_at_rest", {}).get("volume_id_hash") == fake_sha("smoke-encrypted-cache-root"),
                "registry entry should carry the redacted encrypted cache-root volume hash",
            )
            assert_true(build_slot_path.is_file(), "build should leave a local worker slot file")
            assert_true(build_blob_path.is_file(), "build should ingest a router-owned durable blob")
            assert_true(build_blob_path.stat().st_size == int(build_manifest["slot_file_size_bytes"]), "durable blob size should match manifest")
            assert_true(fake_state.slot_erase_calls == build_erase_before + 1, "build should erase the worker slot before prefill")
            assert_true(fake_state.slot_save_calls == build_save_before + 1, "build should save one worker slot")
            build_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(build_counts_after["completion"] == build_counts_before["completion"] + 1, "build should run one prefix prefill completion")
            assert_true(build_counts_after["restore"] == build_counts_before["restore"], "build should not restore a slot")
            build_audit_rows = registry_audit_rows_for_request(state, build_request_id)
            build_audit = assert_registry_audit_row(build_audit_rows, action="commit", operation="commit", outcome="success", message="build should append a registry audit commit row")
            assert_true(build_audit.get("cache_id") == "build-success", "build audit row should identify cache_id")
            assert_true(build_audit.get("cache_key_hash") == build_meta.get("cache_key_hash"), "build audit row should identify cache key")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={build_request_id}", headers=auth)
            build_decisions = json.loads(raw.decode("utf-8"))
            assert_true(status == 200 and len(build_decisions.get("events", [])) == 1, "build should write one decision event")
            assert_true(build_decisions["events"][0].get("metrics", {}).get("ttft_ms") == 12.5, "build decision event should preserve backend-reported TTFT")
            build_audit_text = state.registry_audit_path.read_text(encoding="utf-8")
            state.registry_path.unlink()
            rebuilt_missing = state.load_registry()
            assert_true(
                any(row.get("cache_id") == "build-success" for row in rebuilt_missing.get("entries", [])),
                "missing registry should rebuild the active cache from manifests",
            )
            state.registry_path.write_text("{not-json", encoding="utf-8")
            rebuilt_corrupt = state.load_registry()
            assert_true(
                any(row.get("cache_id") == "build-success" for row in rebuilt_corrupt.get("entries", [])),
                "corrupt registry should rebuild the active cache from manifests",
            )
            rebuild_missing_blob = seed_cache_entry(state, "rebuild-missing-blob", hot_local=False, durable_blob=False)
            rebuild_corrupt_blob = seed_cache_entry(state, "rebuild-corrupt-blob", hot_local=False, durable_blob=True)
            rebuild_corrupt_blob["blob_path"].write_bytes(b"corrupt-rebuild-blob")
            state.registry_path.unlink()
            rebuilt_blob_checked = state.load_registry()
            rebuilt_blob_cache_ids = {row.get("cache_id") for row in rebuilt_blob_checked.get("entries", [])}
            assert_true("build-success" in rebuilt_blob_cache_ids, "registry rebuild should keep entries with valid durable blobs")
            assert_true("rebuild-missing-blob" not in rebuilt_blob_cache_ids, "registry rebuild should skip manifests with missing durable blobs")
            assert_true("rebuild-corrupt-blob" not in rebuilt_blob_cache_ids, "registry rebuild should skip manifests with corrupt durable blobs")
            rebuild_rows = [
                row
                for row in registry_wal_rows(state)
                if row.get("operation") == "rebuild_registry" and row.get("reason") == "missing_registry"
            ]
            assert_true(any(int(row.get("skipped_count") or 0) >= 2 for row in rebuild_rows), "registry rebuild WAL should count skipped bad blob manifests")
            assert_true(str(rebuild_missing_blob["blob_path"]) not in state.registry_wal_path.read_text(encoding="utf-8"), "registry rebuild WAL should not log raw missing blob paths")
            assert_true(str(rebuild_corrupt_blob["blob_path"]) not in state.registry_wal_path.read_text(encoding="utf-8"), "registry rebuild WAL should not log raw corrupt blob paths")
            wal_text = state.registry_wal_path.read_text(encoding="utf-8")
            assert_true("missing_registry" in wal_text and "invalid_registry_json" in wal_text, "registry WAL should record rebuild reasons")
            state.append_registry_wal(
                operation="manifest_prepared",
                outcome="success",
                reason="smoke_committed_manifest",
                cache_id="build-success",
                cache_key_hash=build_meta.get("cache_key_hash"),
                manifest_id=build_meta.get("manifest_id"),
                worker_id="worker-main",
            )
            state.append_registry_wal(
                operation="registry_committed",
                outcome="success",
                reason="smoke_committed_manifest",
                cache_id="build-success",
                cache_key_hash=build_meta.get("cache_key_hash"),
                manifest_id=build_meta.get("manifest_id"),
                worker_id="worker-main",
            )
            settled_replay = state.replay_registry_wal(reason="smoke_committed_manifest")
            assert_true(
                settled_replay.get("action") == "noop" and settled_replay.get("pending_count") == 0,
                "registry WAL replay should treat registry_committed as a completed manifest transaction",
            )
            state.append_registry_wal(
                operation="manifest_prepared",
                outcome="success",
                reason="smoke_pending_manifest",
                cache_id="build-success",
                cache_key_hash=build_meta.get("cache_key_hash"),
                manifest_id=build_meta.get("manifest_id"),
                worker_id="worker-main",
            )
            registry_without_build = state.load_registry()
            registry_without_build["entries"] = [
                row for row in registry_without_build.get("entries", []) if row.get("cache_id") != "build-success"
            ]
            state.save_registry(registry_without_build)
            replay = state.replay_registry_wal(reason="smoke_pending_manifest")
            replayed_registry = state.load_registry()
            assert_true(replay.get("action") == "rebuilt", "registry WAL replay should rebuild when a prepared manifest is absent from the registry")
            assert_true(
                any(row.get("cache_id") == "build-success" for row in replayed_registry.get("entries", [])),
                "registry WAL replay should recover the missing manifest-backed registry entry",
            )
            crash_cache_root = root / "crash-cache"
            crash_slot_dir = root / "worker-added-slots"
            crash_workers_file = root / "crash-workers.json"
            crash_cache_id = "crash-before-registry-commit"
            crash_prefix = "crash durable manifest prefix"
            write_inventory(crash_workers_file, [inventory_worker_entry("worker-main", added_url, crash_slot_dir)])
            crash_run = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--crash-build-child",
                    "--worker-url",
                    added_url,
                    "--workers-file",
                    str(crash_workers_file),
                    "--cache-root",
                    str(crash_cache_root),
                    "--slot-dir",
                    str(crash_slot_dir),
                    "--cache-id",
                    crash_cache_id,
                    "--prefix-text",
                    crash_prefix,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
                check=False,
            )
            assert_true(
                crash_run.returncode == 86,
                f"crash injection child should exit at the injected registry commit point; returncode={crash_run.returncode} stderr={crash_run.stderr[-500:]}",
            )
            crash_manifests = sorted((crash_cache_root / "router-store" / "manifests").glob("*.json"))
            assert_true(len(crash_manifests) == 1, "crash injection should publish exactly one manifest before aborting registry commit")
            crash_manifest = cache_router_daemon.read_json(crash_manifests[0], {})
            assert_true(crash_manifest.get("cache_id") == crash_cache_id, "crash-published manifest should identify the cache")
            crash_blob_path = Path(str(crash_manifest.get("router_blob_path") or ""))
            assert_true(crash_blob_path.is_file(), "crash-published manifest should have a durable blob")
            crash_blob = crash_blob_path.read_bytes()
            assert_true(hashlib.sha256(crash_blob).hexdigest() == crash_manifest.get("slot_file_sha256"), "crash-published blob hash should match manifest")
            assert_true(len(crash_blob) == int(crash_manifest.get("slot_file_size_bytes") or -1), "crash-published blob size should match manifest")
            crash_wal_path = crash_cache_root / "router-store" / "registry-wal.jsonl"
            crash_wal_rows = [json.loads(line) for line in crash_wal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            crash_operations = [(row.get("operation"), row.get("outcome"), row.get("cache_id")) for row in crash_wal_rows]
            assert_true(
                ("manifest_prepare", "start", crash_cache_id) in crash_operations,
                "crash injection WAL should include manifest_prepare before abort",
            )
            assert_true(
                ("manifest_prepared", "success", crash_cache_id) in crash_operations,
                "crash injection WAL should include manifest_prepared before abort",
            )
            assert_true(
                not any(row.get("operation") == "registry_committed" and row.get("cache_id") == crash_cache_id for row in crash_wal_rows),
                "crash injection WAL should not include registry_committed after abort",
            )
            crash_registry = cache_router_daemon.read_json(crash_cache_root / "router-store" / "registry.json", {"entries": []})
            assert_true(
                not any(row.get("cache_id") == crash_cache_id for row in crash_registry.get("entries", []) if isinstance(row, dict)),
                "crash injection registry should not contain the cache before WAL replay",
            )
            crash_args = router_args(added_url, crash_cache_root, crash_slot_dir)
            crash_args.workers_file = str(crash_workers_file)
            crash_recovered_state = cache_router_daemon.CacheRouterState(crash_args)
            crash_recovered_entry = registry_entry_for(crash_recovered_state, crash_cache_id)
            assert_true(
                crash_recovered_entry.get("cache_key_hash") == crash_manifest.get("cache_key_hash"),
                "startup WAL replay should recover a manifest prepared before registry commit",
            )

            use_after_build_counts = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "seed": 777,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "build-success",
                        "prefix_text": "offline build prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            use_after_build_body = json.loads(raw.decode("utf-8"))
            use_after_build_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "cache mode=use should restore a cache built through the router")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "use response should identify the selected worker")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "use after build should be a local cache hit")
            assert_true(use_after_build_body.get("choices", [{}])[0].get("text") == "cached-ok", "use after build should return restored suffix generation output")
            assert_true(use_after_build_counts_after["restore"] == use_after_build_counts["restore"] + 1, "use after build should restore once")
            assert_true(use_after_build_counts_after["completion"] == use_after_build_counts["completion"] + 1, "use after build should generate once after restore")
            native_use_body = [row["body"] for row in fake_state.requests if row.get("path") == "/completion"][-1]
            assert_true(
                native_use_body.get("prompt") == "offline build prefixsuffix",
                "restored use should send full prefix+suffix prompt after slot restore by default",
            )
            assert_true(native_use_body.get("seed") == 777, "restored use should forward deterministic generation seed to native completion")
            assert_true(
                native_use_body.get("cache_prompt") is False,
                "restored use should disable llama.cpp prompt-prefix matching after slot restore by default",
            )
            use_completion = use_after_build_body.get("cache_router", {}).get("use", {}).get("completion", {})
            assert_true(
                use_completion.get("prompt_basis") == "full_prompt_after_slot_restore" and use_completion.get("cache_prompt") is False,
                "restored use metadata should report safe full-prompt restore mode by default",
            )
            use_after_build_request_id = headers.get("x-cache-router-request-id", "")
            use_after_build_audit_rows = registry_audit_rows_for_request(state, use_after_build_request_id)
            use_after_build_audit = assert_registry_audit_row(use_after_build_audit_rows, action="restore", operation="restore", outcome="success", message="use after build should append a registry audit restore row")
            assert_true("hit" in use_after_build_audit.get("audit_actions", []), "use after build audit row should record a cache hit")
            assert_true(state.registry_audit_path.read_text(encoding="utf-8").startswith(build_audit_text), "registry audit log should append without truncating earlier rows")
            use_wal_rows = [
                row
                for row in registry_wal_rows(state)
                if row.get("operation") == "restore_residency_commit" and row.get("cache_id") == "build-success" and row.get("outcome") == "success"
            ]
            assert_true(use_wal_rows, "use after build should append a restore residency WAL commit row")

            validation_cache = seed_cache_entry(state, "validation-mismatch", hot_local=True, durable_blob=True, prefix_text="validation prefix ")
            fake_state.native_completion_contents = ["bad-restored", "cold-ok"]
            validation_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            validation_erase_before = fake_state.slot_erase_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "validation suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "validation-mismatch",
                        "cache_key_hash": validation_cache["entry"]["cache_key_hash"],
                        "prefix_text": "validation prefix ",
                        "suffix_text": "validation suffix",
                        "worker_id": "worker-main",
                        "allow_fallback": True,
                        "restore_validation": "deterministic_recompute",
                    },
                },
            )
            validation_body = json.loads(raw.decode("utf-8"))
            validation_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            validation_meta = validation_body.get("cache_router", {}).get("use", {})
            validation_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 200, "restore validation mismatch should fail closed with same-worker cold recompute when fallback is allowed")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "validation recompute response should identify the selected worker")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "none", "validation recompute response should not expose a positive cache-hit level")
            assert_true(validation_body.get("choices", [{}])[0].get("text") == "cold-ok", "validation recompute should return cold recomputed output")
            assert_true(validation_meta.get("recomputed_cold") is True, "validation recompute metadata should mark cold recompute")
            assert_true(validation_meta.get("restore_validation", {}).get("status") == "fail", "validation metadata should record failed restore validation")
            assert_true(validation_meta.get("restore_validation", {}).get("text_match") is False, "validation metadata should record text mismatch")
            assert_true(validation_counts_after["restore"] == validation_counts_before["restore"] + 1, "validation mismatch should restore once before checking")
            assert_true(validation_counts_after["completion"] == validation_counts_before["completion"] + 2, "validation mismatch should run restored generation plus cold recompute")
            assert_true(fake_state.slot_erase_calls == validation_erase_before + 1, "validation mismatch should erase the slot before cold recompute")
            validation_registry = state.load_registry()
            validation_registry_entry = next(row for row in validation_registry.get("entries", []) if row.get("cache_id") == "validation-mismatch")
            assert_true(validation_registry_entry.get("validation_status") == "quarantined", "validation mismatch should quarantine the registry entry")
            assert_true(validation_registry_entry.get("quarantine_reason") == "restore_validation_failed", "validation mismatch should store bounded quarantine reason")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={validation_request_id}", headers=auth)
            validation_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(validation_events) == 1, "validation mismatch should write one correlated decision event")
            assert_true(validation_events[0].get("decision") == "fallback_after_restore_failure", "validation mismatch should record fallback-after-restore-failure decision")
            assert_true(validation_events[0].get("validation_status") == "quarantined", "validation mismatch decision should be quarantined")
            assert_true(validation_events[0].get("fallback_reason") == "restore_validation_failed", "validation mismatch decision should use bounded fallback reason")
            validation_audit_rows = registry_audit_rows_for_request(state, validation_request_id)
            assert_registry_audit_row(validation_audit_rows, action="restore", operation="restore", outcome="quarantined", message="validation mismatch should append a restore quarantine audit row")

            no_fallback_validation_cache = seed_cache_entry(
                state,
                "validation-mismatch-no-fallback",
                hot_local=True,
                durable_blob=True,
                prefix_text="validation no fallback prefix ",
            )
            fake_state.native_completion_contents = ["bad-restored-no-fallback", "cold-no-fallback"]
            no_fallback_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            no_fallback_erase_before = fake_state.slot_erase_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "validation suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "validation-mismatch-no-fallback",
                        "cache_key_hash": no_fallback_validation_cache["entry"]["cache_key_hash"],
                        "prefix_text": "validation no fallback prefix ",
                        "suffix_text": "validation suffix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                        "restore_validation": "deterministic_recompute",
                    },
                },
            )
            no_fallback_body = json.loads(raw.decode("utf-8"))
            no_fallback_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            no_fallback_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 500, "restore validation mismatch should not return cold recompute when fallback is disabled")
            assert_true("choices" not in no_fallback_body, "no-fallback validation mismatch should return an error instead of cold output")
            assert_true(no_fallback_counts_after["restore"] == no_fallback_counts_before["restore"] + 1, "no-fallback validation mismatch should restore once before checking")
            assert_true(no_fallback_counts_after["completion"] == no_fallback_counts_before["completion"] + 2, "no-fallback validation mismatch should run restored generation and validation recompute only")
            assert_true(fake_state.slot_erase_calls == no_fallback_erase_before + 1, "no-fallback validation mismatch should erase before validation recompute")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={no_fallback_request_id}", headers=auth)
            no_fallback_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200, "no-fallback validation mismatch decisions should be queryable")
            assert_true(
                any(row.get("fallback_reason") == "restore_validation_failed" and row.get("validation_status") == "quarantined" for row in no_fallback_events),
                "no-fallback validation mismatch should record quarantined restore validation failure",
            )
            assert_true(
                any(row.get("decision") == "reject_capacity" and row.get("fallback_reason") == "restore_validation_failed" for row in no_fallback_events),
                "no-fallback validation mismatch should record a rejected no-fallback cache use",
            )

            auto_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            auto_erase_before = fake_state.slot_erase_calls
            auto_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "auto suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "auto",
                        "cache_id": "auto-build-use",
                        "prefix_text": "offline auto prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            auto_body = json.loads(raw.decode("utf-8"))
            auto_meta = auto_body.get("cache_router", {})
            auto_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            auto_entry = registry_entry_for(state, "auto-build-use")
            assert_true(status == 200, "auto mode should build on a miss and then use the new cache")
            assert_true("build" in auto_meta and "use" in auto_meta, "auto miss should include both build and use metadata")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "auto miss should use the newly built local slot")
            assert_true(auto_entry.get("cache_key_hash") == auto_meta.get("build", {}).get("cache_key_hash"), "auto miss should publish the built cache key")
            assert_true(fake_state.slot_erase_calls == auto_erase_before + 1, "auto miss should erase the slot for build")
            assert_true(fake_state.slot_save_calls == auto_save_before + 1, "auto miss should save one slot for build")
            assert_true(auto_counts_after["restore"] == auto_counts_before["restore"] + 1, "auto miss should restore the built slot once")
            assert_true(auto_counts_after["completion"] == auto_counts_before["completion"] + 2, "auto miss should run build prefill and suffix generation")

            auto_hit_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            auto_hit_erase_before = fake_state.slot_erase_calls
            auto_hit_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "auto suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "auto",
                        "cache_id": "auto-build-use",
                        "prefix_text": "offline auto prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            auto_hit_body = json.loads(raw.decode("utf-8"))
            auto_hit_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "auto mode should use an existing compatible cache on hit")
            assert_true("build" not in auto_hit_body.get("cache_router", {}), "auto hit should not rebuild without a miss")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "auto hit should be a local cache hit")
            assert_true(fake_state.slot_erase_calls == auto_hit_erase_before, "auto hit should not erase a slot")
            assert_true(fake_state.slot_save_calls == auto_hit_save_before, "auto hit should not save a new slot")
            assert_true(auto_hit_counts_after["restore"] == auto_hit_counts_before["restore"] + 1, "auto hit should restore once")
            assert_true(auto_hit_counts_after["completion"] == auto_hit_counts_before["completion"] + 1, "auto hit should generate once after restore")

            auto_rebuild_old_key = auto_entry.get("cache_key_hash")
            auto_rebuild_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            auto_rebuild_erase_before = fake_state.slot_erase_calls
            auto_rebuild_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "auto suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "auto",
                        "cache_id": "auto-build-use",
                        "prefix_text": "offline auto replacement prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            auto_rebuild_body = json.loads(raw.decode("utf-8"))
            auto_rebuild_meta = auto_rebuild_body.get("cache_router", {})
            auto_rebuild_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            auto_rebuild_new_key = auto_rebuild_meta.get("build", {}).get("cache_key_hash")
            assert_true(isinstance(auto_rebuild_old_key, str) and auto_rebuild_old_key, "auto seed should expose the old branch cache key")
            assert_true(isinstance(auto_rebuild_new_key, str) and auto_rebuild_new_key, "auto different-prefix miss should expose the new branch cache key")
            auto_rebuild_entries = registry_entries_for(state, "auto-build-use")
            auto_rebuild_entry = registry_entry_for_key(state, "auto-build-use", auto_rebuild_new_key)
            auto_old_entry = registry_entry_for_key(state, "auto-build-use", auto_rebuild_old_key)
            assert_true(status == 200, "auto mode with same cache_id and different request prefix should build a new exact cache instead of restoring the old one")
            assert_true("build" in auto_rebuild_meta and "use" in auto_rebuild_meta, "auto different-prefix miss should include build and use metadata")
            assert_true(auto_rebuild_entry.get("cache_key_hash") != auto_rebuild_old_key, "auto different-prefix miss should publish a distinct branch key")
            assert_true(auto_old_entry.get("cache_key_hash") == auto_rebuild_old_key, "auto different-prefix miss should keep the older branch addressable")
            assert_true(len(auto_rebuild_entries) == 2, "auto different-prefix miss should retain old and new durable branches for the cache_id")
            assert_true(auto_rebuild_entry.get("cache_key_hash") == auto_rebuild_new_key, "auto different-prefix miss should publish the new build key")
            assert_true(fake_state.slot_erase_calls == auto_rebuild_erase_before + 1, "auto different-prefix miss should erase the slot for build")
            assert_true(fake_state.slot_save_calls == auto_rebuild_save_before + 1, "auto different-prefix miss should save one slot for build")
            assert_true(auto_rebuild_counts_after["restore"] == auto_rebuild_counts_before["restore"] + 1, "auto different-prefix miss should restore the new built slot once")
            assert_true(auto_rebuild_counts_after["completion"] == auto_rebuild_counts_before["completion"] + 2, "auto different-prefix miss should run build prefill and suffix generation")

            old_branch_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            old_branch_erase_before = fake_state.slot_erase_calls
            old_branch_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "old branch suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "auto-build-use",
                        "cache_key_hash": auto_rebuild_old_key,
                        "prefix_text": "offline auto prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            old_branch_body = json.loads(raw.decode("utf-8"))
            old_branch_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "old auto branch should remain restorable by exact cache_key_hash")
            assert_true(old_branch_body.get("cache_router", {}).get("cache_key_hash") == auto_rebuild_old_key, "old auto branch restore should report the old cache key")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "old auto branch restore should be a local cache hit")
            assert_true(fake_state.slot_erase_calls == old_branch_erase_before, "old auto branch restore should not erase a slot")
            assert_true(fake_state.slot_save_calls == old_branch_save_before, "old auto branch restore should not save a replacement slot")
            assert_true(old_branch_counts_after["restore"] == old_branch_counts_before["restore"] + 1, "old auto branch restore should restore once")
            assert_true(old_branch_counts_after["completion"] == old_branch_counts_before["completion"] + 1, "old auto branch restore should generate once after restore")

            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "build",
                        "cache_id": "refresh-success",
                        "prefix_text": "refresh original prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            refresh_seed_body = json.loads(raw.decode("utf-8"))
            refresh_old_key = refresh_seed_body.get("cache_router", {}).get("build", {}).get("cache_key_hash")
            refresh_old_entry = registry_entry_for(state, "refresh-success")
            refresh_old_manifest_path = Path(str(refresh_old_entry["manifest_path"]))
            refresh_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            refresh_erase_before = fake_state.slot_erase_calls
            refresh_save_before = fake_state.slot_save_calls
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "refresh",
                        "cache_id": "refresh-success",
                        "cache_key_hash": refresh_old_key,
                        "prefix_text": "refresh replacement prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            refresh_body = json.loads(raw.decode("utf-8"))
            refresh_meta = refresh_body.get("cache_router", {}).get("build", {})
            refresh_new_entry = registry_entry_for(state, "refresh-success")
            refresh_new_manifest_path = Path(str(refresh_new_entry["manifest_path"]))
            refresh_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "refresh should rebuild an existing cache when the supplied cache_key_hash matches")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "refresh response should identify the selected worker")
            assert_true(isinstance(refresh_old_key, str) and refresh_old_key, "seed build should expose an old cache key")
            assert_true(refresh_meta.get("cache_key_hash") != refresh_old_key, "refresh with a different prefix should publish a new cache key")
            assert_true(refresh_new_entry.get("cache_key_hash") == refresh_meta.get("cache_key_hash"), "refresh should replace the active registry row")
            assert_true(refresh_new_manifest_path != refresh_old_manifest_path, "refresh should publish a replacement manifest")
            assert_true(refresh_old_manifest_path.is_file(), "refresh should leave the old manifest auditable")
            assert_true(refresh_new_manifest_path.is_file(), "refresh should write the replacement manifest")
            assert_true(fake_state.slot_erase_calls == refresh_erase_before + 1, "refresh should erase the worker slot before replacement prefill")
            assert_true(fake_state.slot_save_calls == refresh_save_before + 1, "refresh should save one replacement slot")
            assert_true(refresh_counts_after["completion"] == refresh_counts_before["completion"] + 1, "refresh should run one replacement prefill completion")
            assert_true(refresh_counts_after["restore"] == refresh_counts_before["restore"], "refresh should not restore during rebuild")

            status, _, raw = request("GET", base + "/metrics", headers=auth)
            metrics = raw.decode("utf-8")
            assert_true(status == 200, "authorized metrics request after traffic should succeed")
            assert_true(metric_has_line(metrics, "cachy_router_requests_total", {"method": "POST", "path": "/v1/completions", "status": "200", "worker_id": "worker-main"}), "metrics should count main worker completions")
            assert_true(metric_has_line(metrics, "cachy_router_requests_total", {"method": "POST", "path": "/v1/completions", "status": "200", "worker_id": "worker-backup"}), "metrics should count backup worker completions")
            assert_true(metric_has_line(metrics, "cachy_router_worker_selected_total", {"reason": "idle", "worker_id": "worker-main"}), "metrics should count main worker selections")
            assert_true(metric_has_line(metrics, "cachy_router_worker_selected_total", {"reason": "idle", "worker_id": "worker-backup"}), "metrics should count backup worker selections")
            for metric_name in ("cachy_router_request_latency_ms", "cachy_router_routing_decision_latency_ms", "cachy_router_ttft_ms"):
                for quantile in ("p50", "p95", "p99"):
                    assert_true(
                        f'{metric_name}{{quantile="{quantile}"}}' in metrics,
                        f"metrics should expose {metric_name} {quantile}",
                    )
                assert_true(f"{metric_name}_count " in metrics, f"metrics should expose {metric_name}_count")
                assert_true(f"{metric_name}_sum " in metrics, f"metrics should expose {metric_name}_sum")

            status, headers, raw = request(
                "POST",
                base + "/v1/chat/completions",
                headers=auth,
                body={"model": "fake-model", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 1},
            )
            assert_true(status == 200, "normal chat completion should proxy through fake worker")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "chat proxy response should include selected worker header")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["message"]["content"] == "ok", "chat response should come from fake worker")

            tokenize_before = fake_state.tokenize_calls
            with state.scheduler_lock:
                state.cold_route_cursor = 0
            status, headers, raw = request(
                "POST",
                base + "/tokenize",
                headers=auth,
                body={"model": "fake-model", "content": "hello tokenize"},
            )
            assert_true(status == 200, "tokenize request should proxy through fake worker")
            assert_router_debug_headers(headers, "tokenize proxy response", worker="worker-main")
            assert_true(fake_state.tokenize_calls == tokenize_before + 1, "tokenize proxy should call the selected worker")
            assert_true(json.loads(raw.decode("utf-8")).get("tokens") == [0, 1], "tokenize proxy response should come from fake worker")

            fake_state.stream_delay_seconds = 0.5
            fake_state.stream_finished = False
            try:
                with state.scheduler_lock:
                    state.cold_route_cursor = 0
                stream_probe = streaming_request_probe(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "stream please", "max_tokens": 2, "stream": True},
                    state=fake_state,
                )
            finally:
                fake_state.stream_delay_seconds = 0.0
            stream_headers = stream_probe["headers"]
            assert_true(stream_probe["status"] == 200, "normal streaming completion should return HTTP 200")
            assert_router_debug_headers(stream_headers, "normal streaming completion", worker="worker-main")
            assert_true(stream_headers.get("content-type", "").startswith("text/event-stream"), "normal streaming completion should preserve event-stream content type")
            assert_true("content-length" not in stream_headers, "normal streaming completion should not buffer to compute Content-Length")
            assert_true(stream_probe["first_line"].startswith(b"data: "), "normal streaming completion should expose the first event line")
            assert_true(stream_probe["finished_at_first_line"] is False, "normal streaming completion should expose first bytes before backend stream finishes")
            assert_true(b"data: [DONE]" in stream_probe["rest"], "normal streaming completion should forward the stream terminator")
            assert_true(stream_probe["first_elapsed_ms"] >= 450.0, "stream probe should delay the first token enough to test TTFT")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={stream_headers.get('x-cache-router-request-id')}", headers=auth)
            stream_decisions = json.loads(raw.decode("utf-8"))
            assert_true(status == 200 and len(stream_decisions.get("events", [])) == 1, "streaming completion should write one decision event")
            stream_event = stream_decisions["events"][0]
            stream_ttft = stream_event.get("metrics", {}).get("ttft_ms")
            assert_true(isinstance(stream_ttft, (int, float)) and stream_ttft >= 450.0, "streaming decision event should record router-observed TTFT")

            fake_state.stream_finished = False
            with state.scheduler_lock:
                state.cold_route_cursor = 0
            bypass_stream_probe = streaming_request_probe(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "stream bypass please",
                    "max_tokens": 2,
                    "stream": True,
                    "cache_router": {"mode": "bypass", "worker_id": "worker-main", "allow_fallback": False},
                },
                state=fake_state,
            )
            bypass_stream_headers = bypass_stream_probe["headers"]
            assert_true(bypass_stream_probe["status"] == 200, "cache_router bypass streaming completion should return HTTP 200")
            assert_router_debug_headers(bypass_stream_headers, "cache_router bypass streaming completion", worker="worker-main")
            assert_true(bypass_stream_headers.get("content-type", "").startswith("text/event-stream"), "cache_router bypass streaming should preserve event-stream content type")
            assert_true(bypass_stream_probe["first_line"].startswith(b"data: "), "cache_router bypass streaming should expose the first event line")
            assert_true(b"data: [DONE]" in bypass_stream_probe["rest"], "cache_router bypass streaming should forward the stream terminator")
            assert_true("cache_router" not in fake_state.requests[-1]["body"], "cache_router bypass streaming must strip the extension before forwarding")

            fake_state.stream_finished = False
            with state.scheduler_lock:
                state.cold_route_cursor = 0
            chat_stream_probe = streaming_request_probe(
                "POST",
                base + "/v1/chat/completions",
                headers=auth,
                body={"model": "fake-model", "messages": [{"role": "user", "content": "stream please"}], "stream": True},
                state=fake_state,
            )
            chat_stream_headers = chat_stream_probe["headers"]
            assert_true(chat_stream_probe["status"] == 200, "normal streaming chat completion should return HTTP 200")
            assert_router_debug_headers(chat_stream_headers, "normal streaming chat completion", worker="worker-main")
            assert_true(chat_stream_headers.get("content-type", "").startswith("text/event-stream"), "normal streaming chat completion should preserve event-stream content type")
            assert_true("content-length" not in chat_stream_headers, "normal streaming chat completion should not buffer to compute Content-Length")
            assert_true(chat_stream_probe["first_line"].startswith(b"data: "), "normal streaming chat completion should expose the first event line")
            assert_true(b"data: [DONE]" in chat_stream_probe["rest"], "normal streaming chat completion should forward the stream terminator")

            cold_spread_workers: list[str] = []
            for _ in range(2):
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "cold spread", "max_tokens": 1},
                )
                assert_true(status == 200, "equal-score cold completion should route successfully")
                assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "cold spread response should come from a fake worker")
                cold_spread_workers.append(headers.get("x-cache-router-worker", ""))
            assert_true(set(cold_spread_workers) == {"worker-main", "worker-backup"}, "equal-score cold requests should distribute instead of pinning one worker")

            fake_state.completion_delay_seconds = 0.4
            long_request_result: dict[str, Any] = {}

            def long_main_request() -> None:
                long_request_result["response"] = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "held open", "max_tokens": 1},
                )

            with state.scheduler_lock:
                state.cold_route_cursor = 0
            long_thread = threading.Thread(target=long_main_request, daemon=True)
            long_thread.start()
            try:
                wait_until(lambda: state.worker_active_count("worker-main") == 1, "first worker should have one in-flight backend request")
                with state.scheduler_lock:
                    state.cold_route_cursor = 0
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "active count route", "max_tokens": 1},
                )
                assert_true(status == 200, "second request should route while the first worker is active")
                assert_true(headers.get("x-cache-router-worker") == "worker-backup", "active main worker should lose to idle backup even when round-robin would favor main")
                assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "active-count-routed response should come from fake worker")
                active_count_request_id = headers.get("x-cache-router-request-id", "")
            finally:
                fake_state.completion_delay_seconds = 0.0
                long_thread.join(timeout=2.0)
            assert_true("response" in long_request_result, "held-open request should finish")
            long_status, long_headers, long_raw = long_request_result["response"]
            assert_true(long_status == 200, "held-open request should complete successfully")
            assert_true(long_headers.get("x-cache-router-worker") == "worker-main", "held-open request should have selected main worker")
            assert_true(json.loads(long_raw.decode("utf-8"))["choices"][0]["text"] == "ok", "held-open response should come from fake worker")
            assert_true(state.worker_active_count("worker-main") == 0, "worker active count should be released after backend request")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={active_count_request_id}", headers=auth)
            active_count_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(active_count_events) == 1, "active-count route should have one correlated decision event")
            active_scheduler = active_count_events[0].get("scheduler", {})
            active_candidates = {row.get("worker_id"): row for row in active_scheduler.get("candidates", [])}
            assert_true(active_scheduler.get("winner_worker_id") == "worker-backup", "active-count scheduler trace should record backup as winner")
            assert_true(active_candidates.get("worker-main", {}).get("active_requests") == 1, "scheduler trace should include main worker active request count")
            assert_true(active_candidates.get("worker-backup", {}).get("active_requests") == 0, "scheduler trace should include backup worker active request count")

            fake_state.crash_next_openai_completion = True
            main_count_before_crash = len(fake_state.requests)
            backup_count_before_crash = len(fake_backup_state.requests)
            try:
                with state.scheduler_lock:
                    state.cold_route_cursor = 0
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "crash during request", "max_tokens": 1},
                )
                body = json.loads(raw.decode("utf-8"))
                assert_true(status == 200, "worker crash should fall back to a remaining ready worker")
                assert_true(headers.get("x-cache-router-worker") == "worker-backup", "worker crash fallback should report the backup worker")
                assert_true(body.get("choices", [{}])[0].get("text") == "ok", "worker crash fallback response should come from the backup worker")
                assert_true(len(fake_state.requests) == main_count_before_crash + 1, "crashed worker should receive exactly the failed request")
                assert_true(len(fake_backup_state.requests) == backup_count_before_crash + 1, "backup worker should receive the fallback request")
                crash_request_id = headers.get("x-cache-router-request-id", "")
                status, _, raw = request("GET", base + f"/router/decisions?request_id={crash_request_id}", headers=auth)
                crash_events = json.loads(raw.decode("utf-8")).get("events", [])
                assert_true(status == 200 and len(crash_events) == 1, "worker crash fallback should write one correlated decision event")
                crash_event = crash_events[0]
                assert_true(crash_event.get("worker_id") == "worker-backup", "worker crash decision event should name the fallback worker")
                assert_true(crash_event.get("fallback_required") is True, "worker crash decision event should mark fallback required")
                assert_true(crash_event.get("fallback_reason") == "worker_unavailable", "worker crash decision event should use a bounded fallback reason")

                main_count_after_crash = len(fake_state.requests)
                backup_count_after_crash = len(fake_backup_state.requests)
                with state.scheduler_lock:
                    state.cold_route_cursor = 0
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "after worker crash", "max_tokens": 1},
                )
                body = json.loads(raw.decode("utf-8"))
                assert_true(status == 200, "router should keep serving after a worker crash")
                assert_true(headers.get("x-cache-router-worker") == "worker-backup", "post-crash request should use the remaining ready worker")
                assert_true(body.get("choices", [{}])[0].get("text") == "ok", "post-crash response should come from the backup worker")
                assert_true(len(fake_state.requests) == main_count_after_crash, "post-crash routing should not forward to the down worker")
                assert_true(len(fake_backup_state.requests) == backup_count_after_crash + 1, "post-crash routing should forward to the backup worker")
            finally:
                fake_state.crash_next_openai_completion = False
                fake_state.healthy = True

            original_queue_limit = state.queue_limit_per_worker
            with state.worker_queue_condition:
                state.queue_limit_per_worker = 1
                state.worker_queue_depths["worker-main"] = 1
                state.worker_queue_depths["worker-backup"] = 0
                state.cold_route_cursor = 0
                state.worker_queue_condition.notify_all()
            try:
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "queue depth route", "max_tokens": 1},
                )
                assert_true(status == 200, "queue-depth-routed request should complete")
                assert_true(headers.get("x-cache-router-worker") == "worker-backup", "queued main worker should lose to lower-queue backup")
                assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "queue-depth-routed response should come from fake worker")
                queue_depth_request_id = headers.get("x-cache-router-request-id", "")
            finally:
                with state.worker_queue_condition:
                    state.worker_queue_depths["worker-main"] = 0
                    state.worker_queue_depths["worker-backup"] = 0
                    state.queue_limit_per_worker = original_queue_limit
                    state.worker_queue_condition.notify_all()
            status, _, raw = request("GET", base + f"/router/decisions?request_id={queue_depth_request_id}", headers=auth)
            queue_depth_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(queue_depth_events) == 1, "queue-depth route should have one correlated decision event")
            queue_scheduler = queue_depth_events[0].get("scheduler", {})
            queue_candidates = {row.get("worker_id"): row for row in queue_scheduler.get("candidates", [])}
            assert_true(queue_scheduler.get("winner_worker_id") == "worker-backup", "queue-depth scheduler trace should record backup as winner")
            assert_true("queue_depth" in queue_scheduler.get("rank_fields", []), "scheduler rank fields should include queue_depth")
            assert_true(queue_candidates.get("worker-main", {}).get("queue_depth") == 1, "scheduler trace should record queued main worker depth")
            assert_true(queue_candidates.get("worker-backup", {}).get("queue_depth") == 0, "scheduler trace should record idle backup queue depth")

            queue_worker_state = FakeWorkerState()
            queue_worker = start_server(FakeWorkerHandler, state_attr="fake_state", state=queue_worker_state)
            queue_router: ThreadingHTTPServer | None = None
            try:
                queue_host, queue_port = queue_worker.server_address[:2]
                queue_worker_url = f"http://{queue_host}:{queue_port}"
                queue_args = router_args(queue_worker_url, root / "queue-cache", root / "queue-worker-slots")
                queue_args.queue_limit_per_worker = 1
                queue_args.queue_wait_timeout = 3.0
                queue_state = cache_router_daemon.CacheRouterState(queue_args)
                queue_router = ThreadingHTTPServer(("127.0.0.1", 0), cache_router_daemon.RouterHandler)
                queue_router.state = queue_state  # type: ignore[attr-defined]
                queue_thread = threading.Thread(target=queue_router.serve_forever, daemon=True)
                queue_thread.start()
                queue_router_host, queue_router_port = queue_router.server_address[:2]
                queue_base = f"http://{queue_router_host}:{queue_router_port}"
                queue_worker_state.completion_delay_seconds = 2.0
                first_queue_result: dict[str, Any] = {}
                second_queue_result: dict[str, Any] = {}

                def first_queue_request() -> None:
                    first_queue_result["response"] = request(
                        "POST",
                        queue_base + "/v1/completions",
                        headers=auth,
                        body={"model": "fake-model", "prompt": "queue hold", "max_tokens": 1},
                    )

                def second_queue_request() -> None:
                    second_queue_result["response"] = request(
                        "POST",
                        queue_base + "/v1/completions",
                        headers=auth,
                        body={"model": "fake-model", "prompt": "queue wait", "max_tokens": 1},
                    )

                first_queue_thread = threading.Thread(target=first_queue_request, daemon=True)
                first_queue_thread.start()
                wait_until(lambda: queue_state.worker_active_count("worker-main") == 1, "queue smoke first request should occupy the only worker slot")
                second_queue_thread = threading.Thread(target=second_queue_request, daemon=True)
                second_queue_thread.start()
                wait_until(lambda: queue_state.worker_queue_depth("worker-main") == 1, "queue smoke second request should wait in the router queue")
                status, _, raw = request("GET", queue_base + "/metrics", headers=auth)
                queue_metrics_while_waiting = raw.decode("utf-8")
                assert_true(status == 200, "queue-enabled metrics should be readable while a request waits")
                assert_true(
                    metric_value(queue_metrics_while_waiting, "cachy_router_worker_queue_depth", {"enabled": "true", "worker_id": "worker-main"}) == 1.0,
                    "queue metrics should expose worker queue depth while a request waits",
                )
                forwarded_before_overflow = len(queue_worker_state.requests)
                overflow_started = time.perf_counter()
                status, headers, raw = request(
                    "POST",
                    queue_base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "queue overflow", "max_tokens": 1},
                )
                overflow_elapsed = time.perf_counter() - overflow_started
                overflow_body = json.loads(raw.decode("utf-8"))
                assert_true(status == 503, "queue-full request should return a bounded service-unavailable status")
                assert_true(overflow_elapsed < 1.0, "queue-full request should reject within one second")
                assert_true(overflow_body.get("error", {}).get("type") == "service_unavailable", "queue-full response should use an OpenAI-shaped service_unavailable error")
                assert_true(headers.get("x-cache-router-worker") == "none", "queue-full response should not report a selected worker")
                assert_true(headers.get("x-cache-router-request-id", "").startswith("req-"), "queue-full response should include a request ID")
                assert_true(headers.get("x-cache-router-trace-id", "").startswith("trace-"), "queue-full response should include a trace ID")
                assert_true(len(queue_worker_state.requests) == forwarded_before_overflow, "queue-full request should not be forwarded to the backend worker")
                rss_baseline = current_rss_bytes()
                assert_true(rss_baseline is not None and rss_baseline > 0, "queue overload smoke should be able to read current process RSS")
                rss_peak = int(rss_baseline or 0)
                rss_rejections_before = state_metric_counter(queue_state, "queue_rejected_total", {"worker_id": "worker-main", "reason": "queue_full"})
                rejected_count = 24
                max_reject_elapsed = 0.0
                for index in range(rejected_count):
                    assert_true(queue_state.worker_active_count("worker-main") == 1, "RSS overload should keep the worker slot occupied")
                    assert_true(queue_state.worker_queue_depth("worker-main") == 1, "RSS overload should keep the worker queue full")
                    started = time.perf_counter()
                    status, headers, raw = request(
                        "POST",
                        queue_base + "/v1/completions",
                        headers=auth,
                        body={"model": "fake-model", "prompt": f"queue overflow rss {index}", "max_tokens": 1},
                    )
                    reject_elapsed = time.perf_counter() - started
                    max_reject_elapsed = max(max_reject_elapsed, reject_elapsed)
                    overflow_body = json.loads(raw.decode("utf-8"))
                    assert_true(status == 503, "RSS overload queue-full request should return bounded service-unavailable")
                    assert_true(overflow_body.get("error", {}).get("type") == "service_unavailable", "RSS overload response should remain OpenAI-shaped")
                    assert_true(headers.get("x-cache-router-worker") == "none", "RSS overload response should not select a worker")
                    assert_true(len(queue_worker_state.requests) == forwarded_before_overflow, "RSS overload queue-full requests should not be forwarded to the backend worker")
                    current_rss = current_rss_bytes()
                    assert_true(current_rss is not None and current_rss > 0, "RSS overload smoke should keep reading process RSS")
                    rss_peak = max(rss_peak, int(current_rss or 0))
                rss_rejections_after = state_metric_counter(queue_state, "queue_rejected_total", {"worker_id": "worker-main", "reason": "queue_full"})
                assert_true(rss_rejections_after - rss_rejections_before == rejected_count, "RSS overload should count every queue-full rejection")
                assert_true(max_reject_elapsed < 1.0, "RSS overload queue-full requests should each reject within one second")
                assert_true(rss_peak <= int((rss_baseline or 0) * 1.10), "RSS overload queue-full rejections should stay within 110% of pre-overload RSS")
                first_queue_thread.join(timeout=5.0)
                second_queue_thread.join(timeout=5.0)
                assert_true("response" in first_queue_result and first_queue_result["response"][0] == 200, "queue smoke first request should complete")
                assert_true("response" in second_queue_result and second_queue_result["response"][0] == 200, "queued request should complete after waiting")
                status, _, raw = request("GET", queue_base + "/metrics", headers=auth)
                queue_metrics_after = raw.decode("utf-8")
                assert_true(status == 200, "queue-enabled metrics should be readable after queued traffic")
                assert_true("cachy_router_queue_wait_ms_count " in queue_metrics_after, "queue metrics should expose queue wait count")
                assert_true("cachy_router_queue_wait_ms_sum " in queue_metrics_after, "queue metrics should expose queue wait sum")
                assert_true(
                    metric_has_line(queue_metrics_after, "cachy_router_queue_rejected_total", {"reason": "queue_full", "worker_id": "worker-main"}),
                    "queue metrics should count queue-full rejections with a bounded reason",
                )
            finally:
                queue_worker_state.completion_delay_seconds = 0.0
                if queue_router is not None:
                    queue_router.shutdown()
                    queue_router.server_close()
                queue_worker.shutdown()
                queue_worker.server_close()

            forwarded_count = total_worker_requests([fake_state, fake_backup_state])
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "stream": True,
                    "cache_router": {"mode": "use", "cache_id": "stream-reject"},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 400, "cached stream request should be rejected clearly")
            assert_true("non-streaming" in body.get("error", {}).get("message", ""), "cached stream rejection should explain non-streaming mode")
            assert_true(headers.get("x-cache-router-request-id", "").startswith("req-"), "cached stream rejection should include request ID")
            assert_true(headers.get("x-cache-router-worker") == "none", "cached stream rejection should report no selected worker")
            assert_true(total_worker_requests([fake_state, fake_backup_state]) == forwarded_count, "cached stream rejection should not forward to workers")

            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {"mode": "bypass", "cache_id": "explicit-backup", "worker_id": "worker-backup", "allow_fallback": False},
                },
            )
            assert_true(status == 200, "explicit healthy worker_id should route to that worker when fallback is disabled")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "explicit backup route should select backup worker")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "explicit worker response should come from fake worker")
            assert_true(len(fake_state.requests) == main_count_before, "explicit backup route should not hit main worker")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "explicit backup route should hit backup worker once")
            assert_true("cache_router" not in fake_backup_state.requests[-1]["body"], "explicit bypass route should strip cache_router before forwarding")

            fake_state.models_ready = False
            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {"mode": "bypass", "cache_id": "no-fallback-warming", "worker_id": "worker-main", "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "unavailable explicit worker with allow_fallback=false should fail")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "no-fallback unavailable target should return service_unavailable")
            assert_true(len(fake_state.requests) == main_count_before, "warming explicit target should not receive forwarded completion")
            assert_true(len(fake_backup_state.requests) == backup_count_before, "allow_fallback=false should not hit backup worker")
            fake_state.models_ready = True

            fake_state.slot_is_processing = True
            fake_backup_state.slot_is_processing = False
            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "idle worker should serve normal request while first worker slot is busy")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "busy main worker should be skipped for idle backup")
            assert_true(headers.get("x-cache-router-worker-availability") == "idle", "selected idle worker should report idle availability")
            assert_true(headers.get("x-cache-router-worker-busy-score") == "0", "selected idle worker should report busy score 0")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "busy/idle response should come from backup worker")
            assert_true(len(fake_state.requests) == main_count_before, "busy main worker should not receive forwarded completion")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "idle backup should receive busy/idle request")
            fake_state.slot_is_processing = False

            fake_state.slot_has_next_token = True
            fake_backup_state.slot_has_next_token = False
            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "poisoned stalled-next-token worker should be skipped when backup is ready")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "stalled worker should not be selected")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "stalled-worker fallback response should come from backup")
            assert_true(len(fake_state.requests) == main_count_before, "stalled worker should not receive forwarded completion")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "backup should receive stalled-worker fallback request")
            status, _, raw = request("GET", base + "/router/workers", headers=auth)
            workers_body = json.loads(raw.decode("utf-8"))
            stalled_worker = next(row for row in workers_body.get("workers", []) if row.get("worker_id") == "worker-main")
            assert_true(status == 200, "/router/workers should still report poisoned workers for inspection")
            assert_true(stalled_worker.get("availability", {}).get("reason") == "stalled_next_token", "poisoned worker should expose stalled-next-token reason")
            assert_true(stalled_worker.get("availability", {}).get("poisoned") is True, "poisoned worker should be marked explicitly")
            assert_true(workers_body.get("route_ready") == 1, "/router/workers should count only non-poisoned route-ready workers")

            fake_backup_state.models_ready = False
            forwarded_count = total_worker_requests([fake_state, fake_backup_state])
            status, _, raw = request("GET", base + "/v1/models", headers=auth)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "/v1/models should fail when the only model-ready worker is poisoned")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "poisoned-only model response should be service_unavailable")
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "poisoned-only worker should not receive user traffic")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "poisoned-only route failure should be service_unavailable")
            assert_true(total_worker_requests([fake_state, fake_backup_state]) == forwarded_count, "poisoned-only route should not forward completions")
            fake_state.slot_has_next_token = False
            fake_backup_state.models_ready = True

            incompatible_count_before = len(fake_incompatible_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "bypass",
                        "cache_id": "incompatible-explicit-target",
                        "worker_id": "worker-incompatible",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "explicit incompatible worker target should fail when fallback is disabled")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "incompatible explicit target should return service_unavailable")
            assert_true(headers.get("x-cache-router-worker") == "none", "incompatible explicit target should report no selected worker")
            assert_true(len(fake_incompatible_state.requests) == incompatible_count_before, "explicit incompatible target should not receive forwarded completion")

            fake_state.completion_loading = True
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {"mode": "bypass", "cache_id": "preferred-fallback", "worker_id": "worker-main", "allow_fallback": True},
                },
            )
            fake_state.completion_loading = False
            assert_true(status == 200, "late loading 503 from the first worker should fall back to another ready worker")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "late loading fallback should select ready backup worker")
            fallback_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(fallback_request_id.startswith("req-"), "late loading fallback response should include request ID")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "late loading fallback response should come from backup worker")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "ready backup worker should receive late-loading fallback request")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={fallback_request_id}", headers=auth)
            fallback_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(fallback_events) == 1, "late loading fallback should have one correlated decision event")
            fallback_event = fallback_events[0]
            assert_true(fallback_event.get("worker_id") == "worker-backup", "fallback decision event should record backup worker")
            assert_true(fallback_event.get("fallback_required") is True, "fallback decision event should mark fallback required")
            assert_true(fallback_event.get("fallback_reason") == "worker_capacity", "fallback reason should be a bounded worker_capacity code")
            assert_true("cache_router" not in fake_backup_state.requests[-1]["body"], "preferred fallback bypass should strip cache_router before forwarding")

            forwarded_count = total_worker_requests([fake_state, fake_backup_state])
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "missing-model", "prompt": "hello", "max_tokens": 1},
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 404, "unknown model should return HTTP 404")
            assert_true(body.get("error", {}).get("type") == "model_not_found", "unknown model should return model_not_found")
            assert_true(body.get("error", {}).get("code") == 404, "unknown model should return numeric error code")
            assert_true(headers.get("x-cache-router-worker") == "none", "unknown model response should report no selected worker")
            assert_true(total_worker_requests([fake_state, fake_backup_state]) == forwarded_count, "unknown model should not be forwarded to worker")

            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "hello",
                    "max_tokens": 1,
                    "cache_router": {"mode": "bypass", "cache_id": "bad-worker-test", "worker_id": "missing-worker", "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 404, "unknown explicit worker should return HTTP 404")
            assert_true(body.get("error", {}).get("type") == "worker_not_found", "unknown explicit worker should return worker_not_found")
            assert_true(body.get("error", {}).get("code") == 404, "unknown explicit worker should return numeric error code")
            assert_true(headers.get("x-cache-router-worker") == "none", "unknown explicit worker response should report no selected worker")
            assert_true(total_worker_requests([fake_state, fake_backup_state]) == forwarded_count, "unknown explicit worker should not be forwarded")

            fake_state.models_ready = False
            status, _, raw = request("GET", base + "/router/workers", headers=auth)
            workers_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "/router/workers should still report worker state while model is warming")
            first_worker = workers_body.get("workers", [{}])[0]
            sidecar_worker = next(row for row in workers_body.get("workers", []) if row.get("worker_id") == "worker-http-sidecar")
            assert_true(first_worker.get("health", {}).get("ok") is True, "worker /health should remain separate from model readiness")
            assert_true(first_worker.get("model_readiness", {}).get("state") == "warming", "worker loading 503 should be classified as warming")
            assert_true(first_worker.get("sidecar_readiness", {}).get("state") == "not_applicable", "local transport should report sidecar readiness as not applicable")
            assert_true(first_worker.get("readiness", {}).get("ok") is False, "warming worker should not be route-ready")
            assert_true(sidecar_worker.get("model_readiness", {}).get("state") == "model_mismatch", "sidecar worker model readiness should remain separate from sidecar health")
            assert_true(sidecar_worker.get("sidecar_readiness", {}).get("ok") is True, "HTTP sidecar readiness should be tracked separately")
            assert_true(sidecar_worker.get("sidecar_readiness", {}).get("state") == "ready", "HTTP sidecar health should report ready when /health succeeds")
            assert_true(sidecar_worker.get("readiness", {}).get("ok") is False, "sidecar readiness should not override model readiness")
            fake_sidecar_state.healthy = False
            sidecar_readiness = state.worker_readiness(state.worker_by_id["worker-http-sidecar"], refresh=True)
            assert_true(sidecar_readiness.get("sidecar_readiness", {}).get("ok") is False, "sidecar outage should update sidecar readiness")
            assert_true(sidecar_readiness.get("model_readiness", {}).get("state") == "model_mismatch", "sidecar outage should not rewrite model readiness")
            fake_sidecar_state.healthy = True
            fake_sidecar_state.worker_id = "wrong-worker"
            sidecar_mismatch = state.worker_readiness(state.worker_by_id["worker-http-sidecar"], refresh=True)
            assert_true(sidecar_mismatch.get("sidecar_readiness", {}).get("state") == "worker_mismatch", "sidecar worker_id mismatch should be reported separately")
            assert_true(sidecar_mismatch.get("model_readiness", {}).get("state") == "model_mismatch", "sidecar worker_id mismatch should not rewrite model readiness")
            fake_sidecar_state.worker_id = "worker-http-sidecar"
            fake_sidecar_state.healthy = True
            state.worker_readiness(state.worker_by_id["worker-http-sidecar"], refresh=True)

            http_route_args = router_args(backup_url, root / "http-route-cache", root / "http-route-unused-slots")
            http_route_workers_file = root / "http-route-workers.json"
            http_route_workers_file.write_text(
                json.dumps(
                    {
                        "workers": [
                            {
                                "worker_id": "worker-http-route",
                                "worker_url": backup_url,
                                "slot_save_path": str(root / "worker-http-route-slots"),
                                "slot_id": 0,
                                "model": "fake-model",
                                "strict_metadata_auto": True,
                                "strict_metadata_force_runtime": True,
                                "model_identity": "fake-model-identity",
                                "model_path": "/models/fake.gguf",
                                "model_file_size": 123,
                                **strict_worker_metadata(),
                                "llama_server_path": "/usr/bin/llama-server",
                                "llama_server_version": "fake-commit",
                                "ctx_size": 4096,
                                "cache_type_k": "q8_0",
                                "cache_type_v": "q8_0",
                                "mtp_enabled": False,
                                "transport": {"kind": "http", "sidecar_url": sidecar_url},
                            }
                        ]
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            http_route_args.workers_file = str(http_route_workers_file)
            http_route_state = cache_router_daemon.CacheRouterState(http_route_args)
            http_route_summary = http_route_state.worker_summaries(include_slots=False)[0]
            assert_true(http_route_summary.get("strict_metadata_auto") is True, "forced worker summary should report strict metadata derivation")
            assert_true(http_route_summary.get("strict_metadata_force_runtime") is True, "forced worker summary should report runtime replacement of strict metadata")
            assert_true(http_route_summary.get("strict_metadata_source") == "runtime_forced", "forced worker summary should distinguish runtime-forced strict metadata")
            http_route_router = ThreadingHTTPServer(("127.0.0.1", 0), cache_router_daemon.RouterHandler)
            http_route_router.state = http_route_state  # type: ignore[attr-defined]
            http_route_thread = threading.Thread(target=http_route_router.serve_forever, daemon=True)
            http_route_thread.start()
            http_route_host, http_route_port = http_route_router.server_address[:2]
            http_route_base = f"http://{http_route_host}:{http_route_port}"
            try:
                fake_sidecar_state.healthy = False
                status, _, raw = request("GET", http_route_base + "/router/workers", headers=auth)
                http_workers_body = json.loads(raw.decode("utf-8"))
                http_worker = http_workers_body.get("workers", [{}])[0]
                assert_true(status == 200, "HTTP transport worker summary should be available while sidecar is unhealthy")
                assert_true(http_worker.get("model_readiness", {}).get("ok") is True, "HTTP transport model readiness should stay separate from sidecar health")
                assert_true(http_worker.get("sidecar_readiness", {}).get("ok") is False, "HTTP sidecar outage should be reported separately")
                assert_true(http_worker.get("readiness", {}).get("ok") is True, "sidecar outage should not clear normal route readiness")
                status, _, raw = request("GET", http_route_base + "/v1/models", headers=auth)
                assert_true(status == 200 and json.loads(raw.decode("utf-8")).get("data", [{}])[0].get("id") == "fake-model", "HTTP transport worker should still publish model readiness with sidecar down")
                sidecar_calls_before = fake_sidecar_state.health_calls
                status, headers, raw = request(
                    "POST",
                    http_route_base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
                )
                assert_true(status == 200, "normal pass-through should route for HTTP transport worker while sidecar is unhealthy")
                assert_true(headers.get("x-cache-router-worker") == "worker-http-route", "HTTP transport normal pass-through should select the model-ready worker")
                assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "HTTP transport normal pass-through should use the worker response")
                assert_true(fake_sidecar_state.health_calls == sidecar_calls_before, "normal pass-through should not probe the sidecar")
            finally:
                fake_sidecar_state.healthy = True
                http_route_router.shutdown()
                http_route_router.server_close()
                http_route_state.close()

            hot_workers_file = root / "hot-reload-workers.json"
            hot_args = router_args(worker_url, root / "hot-reload-cache", root / "hot-reload-unused-slots")
            hot_args.workers_file = str(hot_workers_file)
            hot_args.inventory_reload_interval = 0.1
            write_inventory(
                hot_workers_file,
                [
                    inventory_worker_entry("worker-main", worker_url, root / "hot-worker-main-slots"),
                    inventory_worker_entry("worker-backup", backup_url, root / "hot-worker-backup-slots"),
                ],
            )
            hot_state = cache_router_daemon.CacheRouterState(hot_args)
            hot_router = ThreadingHTTPServer(("127.0.0.1", 0), cache_router_daemon.RouterHandler)
            hot_router.state = hot_state  # type: ignore[attr-defined]
            hot_thread = threading.Thread(target=hot_router.serve_forever, daemon=True)
            hot_thread.start()
            hot_host, hot_port = hot_router.server_address[:2]
            hot_base = f"http://{hot_host}:{hot_port}"

            def hot_worker_rows() -> list[dict[str, Any]]:
                status, _, raw = request("GET", hot_base + "/router/workers", headers=auth)
                assert_true(status == 200, "hot-reload router should expose worker summaries")
                rows = json.loads(raw.decode("utf-8")).get("workers", [])
                assert_true(isinstance(rows, list), "hot-reload workers response should include a workers list")
                return rows

            def hot_worker_ids() -> set[str]:
                return {str(row.get("worker_id")) for row in hot_worker_rows()}

            try:
                status, _, raw = request("GET", hot_base + "/router/status", headers=auth)
                hot_status = json.loads(raw.decode("utf-8"))
                assert_true(status == 200, "hot-reload router status should succeed")
                assert_true(hot_status.get("inventory", {}).get("reload_interval_seconds") == 0.1, "router status should expose inventory reload interval")

                fake_added_state.models_ready = False
                added_count_before = len(fake_added_state.requests)
                write_inventory(
                    hot_workers_file,
                    [
                        inventory_worker_entry("worker-main", added_url, root / "hot-worker-main-repointed-slots"),
                        inventory_worker_entry("worker-backup", backup_url, root / "hot-worker-backup-slots"),
                    ],
                )
                wait_until(
                    lambda: next((row for row in hot_worker_rows() if row.get("worker_id") == "worker-main"), {}).get("url") == added_url,
                    "inventory hot reload should apply changed worker URL",
                    timeout=3.0,
                )
                changed_main = next(row for row in hot_worker_rows() if row.get("worker_id") == "worker-main")
                assert_true(changed_main.get("readiness", {}).get("ok") is False, "changed worker should not be route-ready until /v1/models passes")
                status, _, raw = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "hello",
                        "max_tokens": 1,
                        "cache_router": {"mode": "bypass", "cache_id": "hot-change-not-ready", "worker_id": "worker-main", "allow_fallback": False},
                    },
                )
                assert_true(status == 503, "changed worker should reject explicit routes while model readiness is false")
                assert_true(len(fake_added_state.requests) == added_count_before, "not-ready changed worker should not receive forwarded traffic")
                fake_added_state.models_ready = True
                status, headers, raw = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "hello",
                        "max_tokens": 1,
                        "cache_router": {"mode": "bypass", "cache_id": "hot-change-ready", "worker_id": "worker-main", "allow_fallback": False},
                    },
                )
                assert_true(status == 200, "changed worker should become eligible after /v1/models readiness passes")
                assert_true(headers.get("x-cache-router-worker") == "worker-main", "changed explicit worker route should keep the stable worker ID")
                assert_true(len(fake_added_state.requests) == added_count_before + 1, "changed worker URL should receive the ready route")

                write_inventory(
                    hot_workers_file,
                    [inventory_worker_entry("worker-backup", backup_url, root / "hot-worker-backup-slots")],
                )
                wait_until(lambda: "worker-main" not in hot_worker_ids(), "removed worker should disappear from hot-reloaded inventory", timeout=3.0)
                status, headers, raw = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "hello",
                        "max_tokens": 1,
                        "cache_router": {"mode": "bypass", "cache_id": "hot-removed-worker", "worker_id": "worker-main", "allow_fallback": False},
                    },
                )
                body = json.loads(raw.decode("utf-8"))
                assert_true(status == 404 and body.get("error", {}).get("type") == "worker_not_found", "removed worker should reject new explicit routes")
                backup_count_before = len(fake_backup_state.requests)
                status, headers, raw = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={"model": "fake-model", "prompt": "after remove", "max_tokens": 1},
                )
                assert_true(status == 200, "hot-reloaded router should keep serving remaining workers")
                assert_true(headers.get("x-cache-router-worker") == "worker-backup", "normal route after removal should use remaining worker")
                assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "remaining worker should receive normal route after removal")

                fake_added_state.models_ready = False
                write_inventory(
                    hot_workers_file,
                    [
                        inventory_worker_entry("worker-backup", backup_url, root / "hot-worker-backup-slots"),
                        inventory_worker_entry("worker-added", added_url, root / "hot-worker-added-slots"),
                    ],
                )
                wait_until(lambda: "worker-added" in hot_worker_ids(), "added worker should appear after inventory reload", timeout=3.0)
                added_row = next(row for row in hot_worker_rows() if row.get("worker_id") == "worker-added")
                assert_true(added_row.get("readiness", {}).get("ok") is False, "added worker should remain ineligible while model readiness is false")
                status, _, _ = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "hello",
                        "max_tokens": 1,
                        "cache_router": {"mode": "bypass", "cache_id": "hot-added-not-ready", "worker_id": "worker-added", "allow_fallback": False},
                    },
                )
                assert_true(status == 503, "added worker should reject explicit routes until /v1/models readiness passes")
                fake_added_state.models_ready = True
                status, headers, raw = request(
                    "POST",
                    hot_base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "hello",
                        "max_tokens": 1,
                        "cache_router": {"mode": "bypass", "cache_id": "hot-added-ready", "worker_id": "worker-added", "allow_fallback": False},
                    },
                )
                assert_true(status == 200, "added worker should become eligible after /v1/models readiness passes")
                assert_true(headers.get("x-cache-router-worker") == "worker-added", "added worker route should report the new worker ID")
            finally:
                fake_added_state.models_ready = True
                hot_router.shutdown()
                hot_router.server_close()
                hot_state.close()

            status, _, raw = request("GET", base + "/v1/models", headers=auth)
            models = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "/v1/models should still succeed when another worker is ready")
            assert_true(models.get("data", [{}])[0].get("id") == "fake-model", "/v1/models should list models from ready workers only")

            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "traffic should route to another ready worker while the first worker is warming")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "warming first worker should be skipped in favor of ready backup")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "backup completion response should come from fake worker")
            assert_true(len(fake_state.requests) == main_count_before, "model-warming worker should not receive forwarded completions")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "ready backup worker should receive the forwarded completion")

            fake_backup_state.models_ready = False
            forwarded_count = total_worker_requests([fake_state, fake_backup_state])
            status, _, raw = request("GET", base + "/v1/models", headers=auth)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "/v1/models should return 503 when no worker is model-ready")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "/v1/models all-warming response should be service_unavailable")

            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "all-warming workers should not receive user traffic")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "all-warming route failure should be service_unavailable")
            assert_true(total_worker_requests([fake_state, fake_backup_state]) == forwarded_count, "all-warming workers should not receive forwarded completions")

            fake_state.models_ready = True
            status, headers, _ = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "recovered worker should re-enter routing after /v1/models readiness passes")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "recovered worker response should include selected worker header")

            fake_backup_state.models_ready = True
            fake_state.healthy = False
            main_count_before = len(fake_state.requests)
            backup_count_before = len(fake_backup_state.requests)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "traffic should keep serving a ready backup when the first worker process is down")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "process-down first worker should be skipped in favor of ready backup")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "process-down fallback response should come from backup worker")
            assert_true(len(fake_state.requests) == main_count_before, "process-down worker should not receive forwarded completions")
            assert_true(len(fake_backup_state.requests) == backup_count_before + 1, "ready backup worker should receive process-down fallback request")

            fake_backup_state.healthy = False
            status, _, raw = request("GET", base + "/v1/models", headers=auth)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "/v1/models should return 503 when no worker is ready")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "/v1/models unhealthy response should be service_unavailable")

            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 503, "no ready worker should return HTTP 503")
            assert_true(body.get("error", {}).get("type") == "service_unavailable", "no ready worker should return OpenAI-shaped service_unavailable error")

            status, _, raw = request("GET", base + "/metrics", headers=auth)
            metrics = raw.decode("utf-8")
            assert_true(status == 200, "authorized metrics request after 503 should succeed")
            assert_true('cachy_router_requests_total{method="POST",path="/v1/completions",status="404"} 2' in metrics, "metrics should count model and worker 404s")
            assert_true('cachy_router_errors_total{method="POST",path="/v1/completions",status="404"} 2' in metrics, "metrics should count model and worker errors")
            assert_true('cachy_router_requests_total{method="POST",path="/v1/completions",status="503"} 5' in metrics, "metrics should count no-worker 503 responses")
            assert_true('cachy_router_errors_total{method="POST",path="/v1/completions",status="503"} 5' in metrics, "metrics should count no-worker errors")

            fake_state.healthy = True
            fake_state.models_ready = True
            poll_args = router_args(worker_url, root / "poll-cache", root / "poll-worker-slots")
            poll_args.readiness_poll_interval = 0.05
            poll_state = cache_router_daemon.CacheRouterState(poll_args)
            try:
                worker = poll_state.workers[0]
                wait_until(lambda: poll_state.worker_readiness(worker).get("ok") is True, "background readiness poller should mark worker ready")
                fake_state.models_ready = False
                wait_until(
                    lambda: poll_state.worker_readiness(worker).get("state") == "warming",
                    "background readiness poller should mark model-loading worker as warming within the poll interval",
                )
                fake_state.models_ready = True
                wait_until(lambda: poll_state.worker_readiness(worker).get("ok") is True, "background readiness poller should re-admit recovered worker")
            finally:
                poll_state.close()

            direct_args = [
                "--host",
                "0.0.0.0",
                "--port",
                "0",
                "--worker-url",
                worker_url,
                "--cache-root",
                str(root / "direct-parse-cache"),
                "--worker-slot-dir",
                str(root / "direct-parse-slots"),
                "--model-path",
                "/models/fake.gguf",
                "--llama-server-path",
                "/usr/bin/llama-server",
            ]
            assert_daemon_parse_fails(direct_args, "unauthenticated non-loopback daemon bind should require explicit trusted-LAN flag")
            parsed = daemon_parse_args([*direct_args, "--allow-unauthenticated-lan"])
            assert_true(parsed.host == "0.0.0.0" and parsed.allow_unauthenticated_lan is True, "explicit trusted-LAN flag should allow unauthenticated non-loopback daemon bind")
            parsed_disabled = daemon_parse_args([*direct_args, "--allow-unauthenticated-lan", "--disable-admin-endpoints"])
            assert_true(parsed_disabled.disable_admin_endpoints is True, "daemon parser should accept admin endpoint disable switch")
            assert_daemon_parse_fails([*direct_args, "--production-mode"], "production mode should require an auth token")
            assert_daemon_parse_fails(
                [*direct_args, "--production-mode", "--auth-token", "secret-token", "--allow-unauthenticated-lan"],
                "production mode should reject unauthenticated LAN override",
            )
            parsed_prod = daemon_parse_args([*direct_args, "--production-mode", "--auth-token", "secret-token"])
            assert_true(parsed_prod.disable_admin_endpoints is True, "production mode should disable admin endpoints by default")
            parsed_prod_admin = daemon_parse_args(
                [*direct_args, "--production-mode", "--auth-token", "secret-token", "--allow-production-admin-endpoints"]
            )
            assert_true(parsed_prod_admin.disable_admin_endpoints is False, "production mode should allow explicit authenticated admin endpoints")

            fake_state.healthy = True
            fake_state.models_ready = True
            fake_backup_state.healthy = True
            fake_backup_state.models_ready = True
            prod_args = router_args(worker_url, root / "production-cache", root / "production-slots")
            prod_args.production_mode = True
            prod_args.disable_admin_endpoints = True
            prod_state = cache_router_daemon.CacheRouterState(prod_args)
            prod_router = ThreadingHTTPServer(("127.0.0.1", 0), cache_router_daemon.RouterHandler)
            prod_router.state = prod_state  # type: ignore[attr-defined]
            prod_thread = threading.Thread(target=prod_router.serve_forever, daemon=True)
            prod_thread.start()
            prod_base = f"http://{prod_router.server_address[0]}:{prod_router.server_address[1]}"
            try:
                status, headers, raw = request("GET", prod_base + "/health")
                prod_health = json.loads(raw.decode("utf-8"))
                prod_security = prod_health.get("security", {})
                assert_true(status == 200, "production health should stay available without auth")
                assert_true(prod_security.get("auth_required") is True, "production health should report auth required")
                assert_true(prod_security.get("production_mode") is True, "production health should report production mode")
                assert_true(prod_security.get("admin_endpoints_enabled") is False, "production health should report admin endpoints disabled by default")
                assert_true("workers" not in prod_health, "unauthenticated production health should omit worker details")
                assert_router_debug_headers(headers, "production unauthenticated health response")

                status, headers, raw = request("GET", prod_base + "/v1/models")
                prod_unauth = json.loads(raw.decode("utf-8"))
                assert_true(status == 401, "production /v1/models should require auth")
                assert_true(prod_unauth.get("error", {}).get("type") == "authentication_error", "production unauthenticated /v1/models should return authentication_error")
                assert_router_debug_headers(headers, "production unauthenticated /v1/models response")

                status, headers, raw = request("GET", prod_base + "/health", headers=auth)
                prod_auth_health = json.loads(raw.decode("utf-8"))
                assert_true(status == 200 and prod_auth_health.get("worker_count") == 1, "authorized production health should include worker details")
                assert_router_debug_headers(headers, "production authorized health response")

                status, headers, raw = request("GET", prod_base + "/router/workers", headers=auth)
                prod_admin_disabled = json.loads(raw.decode("utf-8"))
                assert_true(status == 404, "production mode should disable admin inspection endpoints by default")
                assert_true(prod_admin_disabled.get("error", {}).get("type") == "not_found", "disabled production admin route should return not_found")
                assert_router_debug_headers(headers, "production disabled admin response")

                status, headers, raw = request("GET", prod_base + "/v1", headers=auth)
                prod_discovery = json.loads(raw.decode("utf-8"))
                assert_true(status == 200, "authenticated production /v1 discovery should succeed")
                assert_true("/router/workers" not in set(prod_discovery.get("endpoints", [])), "production /v1 should not advertise disabled admin endpoints")
                assert_router_debug_headers(headers, "production /v1 response")

                status, headers, raw = request(
                    "POST",
                    prod_base + "/v1/completions",
                    headers={"X-API-Key": "secret-token"},
                    body={"model": "fake-model", "prompt": "production hello", "max_tokens": 1},
                )
                prod_completion = json.loads(raw.decode("utf-8"))
                assert_true(status == 200, "production authenticated completion should proxy normally")
                assert_true(prod_completion.get("choices", [{}])[0].get("text") == "ok", "production completion should preserve OpenAI-shaped response")
                assert_router_debug_headers(headers, "production authenticated completion response", worker="worker-main")
            finally:
                prod_router.shutdown()
                prod_router.server_close()
                prod_state.close()

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            hot = seed_cache_entry(state, "hot-local-no-hydrate", hot_local=True, durable_blob=False)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "hot-local-no-hydrate",
                        "prefix_text": "prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            use_body = body.get("cache_router", {}).get("use", {})
            assert_true(status == 200, "hot local cached use should succeed without a durable blob")
            assert_true(headers.get("x-cache-router-worker") == "worker-main", "cached use response should include selected worker header")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "cached use response should include cache hit level header")
            hot_cache_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(hot_cache_request_id.startswith("req-"), "cached use response should include request ID")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={hot_cache_request_id}", headers=auth)
            hot_cache_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(hot_cache_events) == 1, "cached use response should correlate to one decision event")
            assert_true(hot_cache_events[0].get("phase") == "restore_validated", "cached use decision should record restore validation")
            assert_true(hot_cache_events[0].get("worker_id") == "worker-main", "cached use decision should record selected worker")
            assert_true(body.get("choices", [{}])[0].get("text") == "cached-ok", "cached use response should come from fake native completion")
            assert_true(use_body.get("hydrate", {}).get("performed") is False, "hot local cached use should not hydrate")
            assert_true(use_body.get("attempts", [{}])[-1].get("cache_hit_level") == "local_nvme", "hot local cached use should classify local_nvme")
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "hot local cached use should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "hot local cached use should generate once after restore")
            assert_true(not hot["blob_path"].exists(), "hot local test should not require a router durable blob")

            ttft_cache = seed_cache_entry(state, "hot-local-ttft-probe", hot_local=True, durable_blob=False, prefix_text="ttft prefix ")
            fake_state.stream_delay_seconds = 0.5
            fake_state.stream_finished = False
            ttft_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            try:
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": "hot-local-ttft-probe",
                            "prefix_text": "ttft prefix ",
                            "cache_key_hash": ttft_cache["entry"]["cache_key_hash"],
                            "worker_id": "worker-main",
                            "allow_fallback": False,
                            "measure_true_ttft": True,
                        },
                    },
                )
            finally:
                fake_state.stream_delay_seconds = 0.0
            ttft_body = json.loads(raw.decode("utf-8"))
            ttft_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            ttft_completion = ttft_body.get("cache_router", {}).get("use", {}).get("completion", {})
            ttft_timings = ttft_completion.get("timings", {}) if isinstance(ttft_completion, dict) else {}
            measured_ttft = ttft_completion.get("time_to_first_token_ms") if isinstance(ttft_completion, dict) else None
            assert_true(status == 200, "cached use with true TTFT probe should still return normal JSON success")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "TTFT cached use should preserve cache hit level header")
            assert_true(ttft_counts_after["restore"] == ttft_counts_before["restore"] + 2, "TTFT probe should restore once for generation and once for timing")
            assert_true(ttft_counts_after["completion"] == ttft_counts_before["completion"] + 2, "TTFT probe should run normal generation plus one streaming timing probe")
            assert_true(isinstance(measured_ttft, (int, float)) and measured_ttft >= 450.0, "cached TTFT probe should expose router-observed first-token timing")
            assert_true(ttft_timings.get("time_to_first_token_basis") == "router_observed_native_completion_stream_first_token", "cached TTFT probe should report timing basis")
            ttft_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={ttft_request_id}", headers=auth)
            ttft_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(ttft_events) == 1, "cached TTFT probe should write one correlated decision event")
            event_ttft = ttft_events[0].get("metrics", {}).get("ttft_ms")
            assert_true(isinstance(event_ttft, (int, float)) and event_ttft >= 450.0, "cached TTFT decision event should record router-observed TTFT")

            seed_cache_entry(state, "hot-busy-hydrate-idle", hot_local=True, durable_blob=True)
            fake_state.slot_is_processing = True
            fake_backup_state.slot_is_processing = False
            main_restore_before = fake_state.slot_restore_calls
            backup_restore_before = fake_backup_state.slot_restore_calls
            backup_completion_before = fake_backup_state.llama_completion_calls
            try:
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": "hot-busy-hydrate-idle",
                            "prefix_text": "prefix",
                            "allow_fallback": True,
                        },
                    },
                )
            finally:
                fake_state.slot_is_processing = False
                fake_backup_state.slot_is_processing = False
            body = json.loads(raw.decode("utf-8"))
            busy_locality_use = body.get("cache_router", {}).get("use", {})
            busy_locality_hydrate = busy_locality_use.get("hydrate", {})
            assert_true(status == 200, "cached use should route to an idle worker when the hot-local worker is busy")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "busy hot worker should lose to an idle compatible worker")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "durable_blob", "busy-locality override should hydrate from the durable blob")
            assert_true(busy_locality_use.get("first_choice_worker_id") == "worker-main", "hot local worker should remain the locality first choice")
            assert_true(busy_locality_use.get("fallback_used") is True, "busy-locality override should be recorded as a fallback from first choice")
            assert_true(busy_locality_hydrate.get("performed") is True, "idle backup should hydrate before restore when it has no local slot")
            assert_true(fake_state.slot_restore_calls == main_restore_before, "busy hot worker should not receive the cache restore")
            assert_true(fake_backup_state.slot_restore_calls == backup_restore_before + 1, "idle backup should receive exactly one cache restore")
            assert_true(fake_backup_state.llama_completion_calls == backup_completion_before + 1, "idle backup should generate after restore")
            busy_locality_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={busy_locality_request_id}", headers=auth)
            busy_locality_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(busy_locality_events) == 1, "busy-locality override should emit one correlated decision event")
            busy_locality_event = busy_locality_events[0]
            busy_locality_scheduler = busy_locality_event.get("scheduler", {})
            busy_locality_candidates = {row.get("worker_id"): row for row in busy_locality_scheduler.get("candidates", [])}
            assert_true(busy_locality_scheduler.get("winner_worker_id") == "worker-backup", "busy-locality scheduler trace should record backup as winner")
            assert_true(busy_locality_candidates.get("worker-main", {}).get("hot_residency") is True, "scheduler trace should mark main as hot-resident")
            assert_true(busy_locality_candidates.get("worker-main", {}).get("busy_score") == 2, "scheduler trace should record the hot worker busy score")
            assert_true(busy_locality_candidates.get("worker-backup", {}).get("hot_residency") is False, "scheduler trace should mark backup as non-hot")
            assert_true(busy_locality_candidates.get("worker-backup", {}).get("busy_score") == 0, "scheduler trace should record the backup idle score")
            assert_true(busy_locality_event.get("cache_hit_level") == "durable_blob", "busy-locality event should classify durable hydration")
            busy_locality_entry = registry_entry_for(state, "hot-busy-hydrate-idle")
            busy_locality_manifest = cache_router_daemon.read_json(Path(str(busy_locality_entry["manifest_path"])), {})
            assert_true(busy_locality_entry.get("worker_residency", {}).get("worker-backup") is True, "hydrated registry row should mark backup local residency")
            assert_true(busy_locality_manifest.get("worker_residency", {}).get("worker-backup") is True, "hydrated manifest should mark backup local residency")
            backup_local_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "hot-busy-hydrate-idle",
                        "prefix_text": "prefix",
                        "worker_id": "worker-backup",
                        "allow_fallback": False,
                    },
                },
            )
            backup_local_body = json.loads(raw.decode("utf-8"))
            backup_local_use = backup_local_body.get("cache_router", {}).get("use", {})
            backup_local_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "post-hydrate use on backup should succeed")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "post-hydrate local use should stay on backup")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "post-hydrate second use should become local_nvme")
            assert_true(backup_local_use.get("hydrate", {}).get("performed") is False, "post-hydrate second use should not hydrate again")
            assert_true(backup_local_counts_after["restore"] == backup_local_counts_before["restore"] + 1, "post-hydrate second use should restore once")
            assert_true(backup_local_counts_after["completion"] == backup_local_counts_before["completion"] + 1, "post-hydrate second use should generate once")

            seed_cache_entry(state, "hot-down-hydrate-backup", hot_local=True, durable_blob=True)
            unavailable_main_restore_before = fake_state.slot_restore_calls
            unavailable_backup_restore_before = fake_backup_state.slot_restore_calls
            unavailable_backup_completion_before = fake_backup_state.llama_completion_calls
            fake_state.healthy = False
            try:
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": "hot-down-hydrate-backup",
                            "prefix_text": "prefix",
                            "allow_fallback": True,
                        },
                    },
                )
            finally:
                fake_state.healthy = True
            unavailable_body = json.loads(raw.decode("utf-8"))
            unavailable_use = unavailable_body.get("cache_router", {}).get("use", {})
            assert_true(status == 200, "cached use should hydrate to backup when the hot-local worker is unavailable")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "unavailable hot worker should lose to ready backup")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "durable_blob", "unavailable-hot path should hydrate from the durable blob")
            assert_true(unavailable_use.get("hydrate", {}).get("performed") is True, "backup should hydrate when unavailable hot worker owns the local slot")
            assert_true(fake_state.slot_restore_calls == unavailable_main_restore_before, "unavailable hot worker should not receive restore")
            assert_true(fake_backup_state.slot_restore_calls == unavailable_backup_restore_before + 1, "backup should restore once for unavailable-hot path")
            assert_true(fake_backup_state.llama_completion_calls == unavailable_backup_completion_before + 1, "backup should generate once for unavailable-hot path")
            unavailable_entry = registry_entry_for(state, "hot-down-hydrate-backup")
            unavailable_manifest = cache_router_daemon.read_json(Path(str(unavailable_entry["manifest_path"])), {})
            assert_true(unavailable_entry.get("worker_residency", {}).get("worker-backup") is True, "unavailable-hot registry row should mark backup residency")
            assert_true(unavailable_manifest.get("worker_residency", {}).get("worker-backup") is True, "unavailable-hot manifest should mark backup residency")

            seed_cache_entry(state, "durable-hydrate-latency", hot_local=False, durable_blob=True)
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "durable-hydrate-latency",
                        "prefix_text": "prefix",
                        "worker_id": "worker-backup",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            hydrate_body = body.get("cache_router", {}).get("use", {}).get("hydrate", {})
            assert_true(status == 200, "durable cached use should hydrate to an explicitly selected compatible worker")
            assert_true(headers.get("x-cache-router-worker") == "worker-backup", "durable hydrate response should identify the selected worker")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "durable_blob", "durable hydrate response should classify durable blob hit level")
            assert_true(hydrate_body.get("performed") is True, "durable cached use should perform hydration when the selected worker has no local slot")
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "durable hydrated use should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "durable hydrated use should generate once after restore")
            durable_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={durable_request_id}", headers=auth)
            durable_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(durable_events) == 1, "durable hydrated use should emit one correlated decision event")
            durable_event = durable_events[0]
            durable_metrics = durable_event.get("metrics", {})
            assert_true(durable_event.get("cache_hit_level") == "durable_blob", "durable hydrated event should classify durable blob cache level")
            assert_true(durable_event.get("decision") == "restore_then_generate", "durable hydrated event should record restore then generate")
            assert_true(isinstance(durable_metrics.get("hydration_latency_ms"), (int, float)), "durable hydrated event should record hydration latency separately")
            assert_true(isinstance(durable_metrics.get("restore_latency_ms"), (int, float)), "durable hydrated event should record restore latency separately")
            assert_true(durable_metrics.get("full_reprocess_suspected") == "no", "durable hydrated event should classify token reuse as not full reprocess")
            durable_audit_rows = registry_audit_rows_for_request(state, durable_request_id)
            durable_audit = assert_registry_audit_row(durable_audit_rows, action="restore", operation="restore", outcome="success", message="durable hydrate should append a registry audit restore row")
            assert_true("hit" in durable_audit.get("audit_actions", []), "durable hydrate audit row should record a cache hit")
            assert_true(durable_audit.get("worker_id") == "worker-backup", "durable hydrate audit row should identify selected worker")
            durable_wal_rows = [
                row
                for row in registry_wal_rows(state)
                if row.get("operation") == "restore_residency_commit" and row.get("cache_id") == "durable-hydrate-latency" and row.get("worker_id") == "worker-backup"
            ]
            assert_true(durable_wal_rows, "durable hydrate should append a restore residency WAL commit row")

            hit_metric_labels = {
                "outcome": "hit",
                "decision": "restore_then_generate",
                "cache_hit_level": "local_nvme",
                "fallback_reason": "none",
                "validation_status": "validated",
            }
            hit_metric_before_replay = state_metric_counter(state, "cache_outcomes_total", hit_metric_labels)
            seed_cache_entry(state, "full-reprocess-suspected", hot_local=True, durable_blob=True)
            fake_state.completion_tokens_evaluated = 64
            fake_state.completion_tokens_cached = 0
            try:
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": "full-reprocess-suspected",
                            "prefix_text": "prefix",
                            "worker_id": "worker-main",
                            "allow_fallback": False,
                        },
                    },
                )
            finally:
                fake_state.completion_tokens_evaluated = 1
                fake_state.completion_tokens_cached = 16
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "replay-like restored request should still return the backend response")
            assert_true(body.get("choices", [{}])[0].get("text") == "cached-ok", "replay-like restored request should preserve backend output")
            replay_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={replay_request_id}", headers=auth)
            replay_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(replay_events) == 1, "replay-like restored request should emit one correlated decision event")
            replay_metrics = replay_events[0].get("metrics", {})
            assert_true(replay_metrics.get("full_reprocess_suspected") == "yes", "full-prompt restored request should be flagged as full reprocess suspected when cached tokens are absent")
            assert_true(replay_metrics.get("cached_tokens") == 0, "replay-like restored request should record zero cached tokens")
            assert_true(replay_metrics.get("processed_prompt_tokens") == 64, "replay-like restored request should record processed prompt tokens")
            hit_metric_after_replay = state_metric_counter(state, "cache_outcomes_total", hit_metric_labels)
            assert_true(hit_metric_after_replay == hit_metric_before_replay, "full prompt replay suspicion should not increment suffix-route cache-hit outcome")

            tenant_a_hash = fake_sha("tenant-a")
            tenant_b_hash = fake_sha("tenant-b")
            conversation_a_hash = fake_sha("conversation-a")
            conversation_b_hash = fake_sha("conversation-b")
            tenant_policy_hash = fake_sha("tenant-policy")
            tenant_policy_denied = seed_cache_entry(
                state,
                "tenant-policy-denied",
                hot_local=True,
                durable_blob=True,
                manifest_overrides={
                    "scope": "conversation",
                    "tenant_hash": tenant_a_hash,
                    "conversation_hash": conversation_a_hash,
                    "policy_id_hash": tenant_policy_hash,
                },
            )
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "tenant-policy-denied",
                        "cache_key_hash": tenant_policy_denied["entry"]["cache_key_hash"],
                        "allow_fallback": False,
                        "scope": "conversation",
                        "tenant_hash": tenant_b_hash,
                        "conversation_hash": conversation_b_hash,
                        "policy_id_hash": tenant_policy_hash,
                    },
                },
            )
            denied_body = json.loads(raw.decode("utf-8"))
            denied_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "cross-tenant cache use should be client-masked as a scoped cache miss")
            assert_true(denied_body.get("error", {}).get("type") == "cache_not_found", "policy-denied cache use should use the same client error type as a miss")
            assert_true("x-cache-router-cache-hit-level" not in headers, "policy-denied cache use should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "policy-denied cache use should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "policy-denied cache use should not cold-proxy")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={denied_request_id}", headers=auth)
            tenant_events = json.loads(raw.decode("utf-8")).get("events", [])
            tenant_denials = [row for row in tenant_events if row.get("decision") == "reject_policy"]
            assert_true(status == 200 and tenant_denials, "policy-denied cache use should write an admin denial event")
            tenant_denial = tenant_denials[0]
            assert_true(tenant_denial.get("worker_id") is None, "policy-denied cache use should be denied before worker selection")
            assert_true(tenant_denial.get("compatibility_result") == "policy_denied", "policy-denied event should use bounded compatibility result")
            assert_true(tenant_denial.get("cache_hit_level") == "registry_only", "policy-denied event should be registry-only in admin logs")
            assert_true(tenant_denial.get("validation_status") == "not_checked", "policy-denied event should not claim restore validation")
            assert_true(tenant_denial.get("policy_denial_reason") == "tenant_scope_mismatch", "policy-denied event should record a bounded tenant mismatch reason")
            assert_true("denial" in tenant_denial.get("audit_actions", []), "policy-denied event should include denial audit action")
            assert_true(tenant_denial.get("tenant_hash") == tenant_b_hash, "policy-denied event should record only the request tenant hash")
            assert_true((tenant_denial.get("privacy") or {}).get("raw_tenant_id_logged") is False, "policy-denied event should not log raw tenant IDs")
            denial_audit_rows = registry_audit_rows_for_request(state, denied_request_id)
            denial_audit = assert_registry_audit_row(denial_audit_rows, action="denial", operation="denial", outcome="denied", message="policy denial should append a registry audit denial row")
            assert_true(denial_audit.get("tenant_hash") == tenant_b_hash, "policy denial audit row should keep only the hashed request tenant")

            status, absent_headers, absent_raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "tenant-policy-absent",
                        "prefix_text": "prefix",
                        "allow_fallback": False,
                        "scope": "conversation",
                        "tenant_hash": tenant_b_hash,
                        "conversation_hash": conversation_b_hash,
                        "policy_id_hash": tenant_policy_hash,
                    },
                },
            )
            absent_body = json.loads(absent_raw.decode("utf-8"))
            assert_true(status == 404, "absent scoped cache use should return the same status as policy-denied use")
            assert_true(absent_body.get("error", {}).get("type") == denied_body.get("error", {}).get("type"), "policy-denied and absent cache use should share a client error type")
            assert_true("x-cache-router-cache-hit-level" not in absent_headers, "absent cache use should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "absent cache use should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "absent cache use should not cold-proxy")
            absent_request_id = absent_headers.get("x-cache-router-request-id", "")
            absent_audit_rows = registry_audit_rows_for_request(state, absent_request_id)
            absent_audit = assert_registry_audit_row(absent_audit_rows, action="miss", operation="miss", outcome="miss", message="absent cache use should append a registry audit miss row")
            assert_true("lookup" in absent_audit.get("audit_actions", []), "absent cache miss audit row should record lookup semantics")

            conversation_policy_denied = seed_cache_entry(
                state,
                "conversation-policy-denied",
                hot_local=True,
                durable_blob=True,
                manifest_overrides={
                    "scope": "conversation",
                    "tenant_hash": tenant_a_hash,
                    "conversation_hash": conversation_a_hash,
                    "policy_id_hash": tenant_policy_hash,
                },
            )
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "conversation-policy-denied",
                        "cache_key_hash": conversation_policy_denied["entry"]["cache_key_hash"],
                        "allow_fallback": False,
                        "scope": "conversation",
                        "tenant_hash": tenant_a_hash,
                        "conversation_hash": conversation_b_hash,
                        "policy_id_hash": tenant_policy_hash,
                    },
                },
            )
            conversation_denied_body = json.loads(raw.decode("utf-8"))
            conversation_denied_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "cross-conversation cache use should be client-masked as a scoped cache miss")
            assert_true(conversation_denied_body.get("error", {}).get("type") == "cache_not_found", "cross-conversation denial should use the same client error type as a miss")
            assert_true("x-cache-router-cache-hit-level" not in headers, "cross-conversation denial should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "cross-conversation denial should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "cross-conversation denial should not cold-proxy")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={conversation_denied_request_id}", headers=auth)
            conversation_events = json.loads(raw.decode("utf-8")).get("events", [])
            conversation_denials = [row for row in conversation_events if row.get("decision") == "reject_policy"]
            assert_true(status == 200 and conversation_denials, "cross-conversation denial should write an admin denial event")
            conversation_denial = conversation_denials[0]
            assert_true(conversation_denial.get("worker_id") is None, "cross-conversation denial should happen before worker selection")
            assert_true(conversation_denial.get("compatibility_result") == "policy_denied", "cross-conversation denial should use bounded compatibility result")
            assert_true(conversation_denial.get("policy_denial_reason") == "conversation_scope_mismatch", "cross-conversation denial should record a bounded conversation mismatch reason")
            assert_true("denial" in conversation_denial.get("audit_actions", []), "cross-conversation denial should include denial audit action")

            tenant_scope_allowed = seed_cache_entry(
                state,
                "tenant-scope-allowed",
                hot_local=True,
                durable_blob=True,
                manifest_overrides={
                    "scope": "tenant",
                    "tenant_hash": tenant_a_hash,
                    "conversation_hash": conversation_a_hash,
                    "policy_id_hash": tenant_policy_hash,
                },
            )
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "tenant-scope-allowed",
                        "cache_key_hash": tenant_scope_allowed["entry"]["cache_key_hash"],
                        "prefix_text": "prefix",
                        "allow_fallback": False,
                        "scope": "tenant",
                        "tenant_hash": tenant_a_hash,
                        "conversation_hash": conversation_b_hash,
                        "policy_id_hash": tenant_policy_hash,
                    },
                },
            )
            tenant_scope_body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "tenant-scope cache use should explicitly allow same-tenant cross-conversation reuse")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "tenant-scope reuse should expose cache hit only on successful cache use")
            assert_true(tenant_scope_body.get("choices", [{}])[0].get("text") == "cached-ok", "tenant-scope reuse should preserve backend output")
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "tenant-scope reuse should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "tenant-scope reuse should generate once after restore")
            tenant_scope_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={tenant_scope_request_id}", headers=auth)
            tenant_scope_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(tenant_scope_events) == 1, "tenant-scope reuse should emit one correlated decision event")
            assert_true(tenant_scope_events[0].get("scope") == "tenant", "tenant-scope reuse event should record tenant scope")
            assert_true(tenant_scope_events[0].get("conversation_hash") == conversation_b_hash, "tenant-scope reuse event should record the request conversation hash without using it as a tenant-scope cache key")
            assert_true(tenant_scope_events[0].get("decision") == "restore_then_generate", "tenant-scope reuse should record restore then generate")
            assert_true(not any(row.get("decision") == "reject_policy" for row in tenant_scope_events), "tenant-scope reuse should not emit policy denial")

            strict_key_cache = seed_cache_entry(state, "strict-key-exact", hot_local=True, durable_blob=True)
            strict_key_hash = strict_key_cache["entry"]["cache_key_hash"]
            assert_true(
                strict_key_hash == cache_router_daemon.cache_key_hash_from_record(strict_key_cache["manifest"], label="strict-key smoke manifest"),
                "seeded strict-key fixture should use the canonical manifest key material",
            )
            wrong_strict_key_hash = fake_sha("wrong-request-cache-key-hash")
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "cache_key_hash": strict_key_hash,
                        "prefix_text": "prefix",
                        "allow_fallback": False,
                    },
                },
            )
            strict_key_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "cache use with exact requested cache_key_hash and prefix_text should succeed")
            assert_true(strict_key_body.get("choices", [{}])[0].get("text") == "cached-ok", "exact cache_key_hash use should preserve backend output")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "exact cache_key_hash use should expose a successful cache-hit header")
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "exact cache_key_hash use should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "exact cache_key_hash use should generate once after restore")
            strict_key_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={strict_key_request_id}", headers=auth)
            strict_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(strict_key_events) == 1, "exact cache_key_hash use should emit one correlated event")
            assert_true(strict_key_events[0].get("cache_key_hash") == strict_key_hash, "exact cache_key_hash event should record the matched key")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "prefix_text": "prefix",
                        "allow_fallback": False,
                    },
                },
            )
            strict_derived_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "cache use should derive an exact strict key from request prefix and worker runtime")
            assert_true(strict_derived_body.get("cache_router", {}).get("cache_key_hash_basis") == "request_prefix_runtime", "derived strict-key use should report request-prefix basis")
            assert_true(strict_derived_body.get("cache_router", {}).get("cache_key_hash") == strict_key_hash, "derived strict-key use should select the seeded exact key")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "local_nvme", "derived strict-key use should be a local cache hit")
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "derived strict-key use should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "derived strict-key use should generate once after restore")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "prefix_text": "different prefix",
                        "allow_fallback": False,
                    },
                },
            )
            wrong_prefix_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 404, "same cache_id with different request prefix should be an exact-key miss")
            assert_true(wrong_prefix_body.get("error", {}).get("type") == "cache_not_found", "different-prefix strict miss should use cache_not_found")
            assert_true("x-cache-router-cache-hit-level" not in headers, "different-prefix strict miss should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "different-prefix strict miss should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "different-prefix strict miss should not cold-proxy")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "allow_fallback": False,
                    },
                },
            )
            missing_strict_lookup_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 400, "cache use without cache_key_hash or prefix_text should be rejected before lookup")
            assert_true(missing_strict_lookup_body.get("error", {}).get("type") == "invalid_request_error", "missing strict lookup material should use invalid_request_error")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "missing strict lookup material should not restore or generate")

            external_lease = state.acquire_registry_lease(
                operation="restore_hydrate",
                cache_id="strict-key-exact",
                cache_key_hash=strict_key_hash,
                manifest_id=strict_key_cache["manifest"].get("manifest_id"),
                owner_id="external-router-owner",
                ttl_seconds=60.0,
            )
            try:
                cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
                forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": "strict-key-exact",
                            "cache_key_hash": strict_key_hash,
                            "allow_fallback": False,
                        },
                    },
                )
                lease_conflict_body = json.loads(raw.decode("utf-8"))
                assert_true(status == 503, "active external registry lease should reject duplicate cache use with bounded capacity error")
                assert_true(lease_conflict_body.get("error", {}).get("type") == "service_unavailable", "registry lease conflict should use service_unavailable")
                assert_true("registry lease conflict" in lease_conflict_body.get("error", {}).get("message", ""), "registry lease conflict should name the lease reason")
                assert_true("x-cache-router-cache-hit-level" not in headers, "registry lease conflict should not expose a cache-hit header")
                assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "registry lease conflict should not restore or generate")
                assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "registry lease conflict should not cold-proxy")
                active_lease_ids = {
                    row.get("lease_id")
                    for row in state.load_registry_leases().get("leases", [])
                    if isinstance(row, dict)
                }
                assert_true(external_lease.get("lease_id") in active_lease_ids, "external registry lease should remain held after rejected duplicate use")
            finally:
                state.release_registry_lease(external_lease)
            assert_true(
                external_lease.get("lease_id")
                not in {
                    row.get("lease_id")
                    for row in state.load_registry_leases().get("leases", [])
                    if isinstance(row, dict)
                },
                "registry lease release should remove the external lease",
            )
            expired_lease = state.acquire_registry_lease(
                operation="build_upload",
                cache_id="strict-key-exact",
                cache_key_hash=strict_key_hash,
                manifest_id=strict_key_cache["manifest"].get("manifest_id"),
                owner_id="expired-router-owner",
                ttl_seconds=0.001,
            )
            time.sleep(0.01)
            assert_true(state.prune_expired_registry_leases() == 1, "registry lease prune should remove one expired lease")
            assert_true(
                expired_lease.get("lease_id")
                not in {
                    row.get("lease_id")
                    for row in state.load_registry_leases().get("leases", [])
                    if isinstance(row, dict)
                },
                "expired registry lease should not remain after cleanup",
            )
            assert_true(
                registry_entry_for(state, "strict-key-exact").get("cache_key_hash") == strict_key_hash,
                "expired registry lease cleanup should leave registry entries intact",
            )

            leased_build_prefix = "registry lease build prefix"
            leased_build_cache_id = "registry-lease-build"
            leased_build_fields = state.cache_key_fields(
                leased_build_cache_id,
                leased_build_prefix,
                state.worker_by_id["worker-main"],
                cache_policy=cache_router_daemon.default_cache_policy(),
            )
            leased_build_key_hash = cache_router_daemon.cache_key_hash_from_record(leased_build_fields, label="leased build smoke key")
            external_build_lease = state.acquire_registry_lease(
                operation="build_upload",
                cache_id=leased_build_cache_id,
                cache_key_hash=leased_build_key_hash,
                manifest_id="manifest-" + leased_build_key_hash[:16],
                owner_id="external-build-owner",
                ttl_seconds=60.0,
            )
            try:
                build_mutations_before = (
                    fake_state.llama_completion_calls,
                    fake_state.slot_erase_calls,
                    fake_state.slot_save_calls,
                    fake_backup_state.llama_completion_calls,
                    fake_backup_state.slot_erase_calls,
                    fake_backup_state.slot_save_calls,
                )
                status, headers, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "build",
                            "cache_id": leased_build_cache_id,
                            "prefix_text": leased_build_prefix,
                            "allow_fallback": False,
                        },
                    },
                )
                build_lease_body = json.loads(raw.decode("utf-8"))
                assert_true(status == 503, "active external registry lease should reject duplicate cache build/upload")
                assert_true(build_lease_body.get("error", {}).get("type") == "service_unavailable", "registry build lease conflict should use service_unavailable")
                assert_true("registry lease conflict" in build_lease_body.get("error", {}).get("message", ""), "registry build lease conflict should name the lease reason")
                assert_true("x-cache-router-cache-hit-level" not in headers, "registry build lease conflict should not expose a cache-hit header")
                assert_true(
                    (
                        fake_state.llama_completion_calls,
                        fake_state.slot_erase_calls,
                        fake_state.slot_save_calls,
                        fake_backup_state.llama_completion_calls,
                        fake_backup_state.slot_erase_calls,
                        fake_backup_state.slot_save_calls,
                    )
                    == build_mutations_before,
                    "registry build lease conflict should not prefill, erase, save, or upload a worker slot",
                )
                assert_true(state.find_entry(leased_build_cache_id) is None, "registry build lease conflict should not publish a registry entry")
            finally:
                state.release_registry_lease(external_build_lease)

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "cache_key_hash": wrong_strict_key_hash,
                        "allow_fallback": False,
                    },
                },
            )
            wrong_key_body = json.loads(raw.decode("utf-8"))
            wrong_key_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "wrong requested cache_key_hash should be treated as a cache miss")
            assert_true(wrong_key_body.get("error", {}).get("type") == "cache_not_found", "wrong cache_key_hash miss should use cache_not_found")
            assert_true("x-cache-router-cache-hit-level" not in headers, "wrong cache_key_hash miss should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "wrong cache_key_hash should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "wrong cache_key_hash should not cold-proxy")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={wrong_key_request_id}", headers=auth)
            wrong_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(wrong_key_events) == 1, "wrong cache_key_hash miss should write one correlated event")
            assert_true(wrong_key_events[0].get("cache_key_hash") == wrong_strict_key_hash, "wrong cache_key_hash miss event should record the requested key hash")
            assert_true(wrong_key_events[0].get("cache_hit_level") == "registry_only", "wrong cache_key_hash miss should stop at registry lookup")
            assert_true(wrong_key_events[0].get("compatibility_result") == "mismatch", "wrong cache_key_hash miss should be recorded as a mismatch")
            assert_true(wrong_key_events[0].get("fallback_reason") == "cache_key_mismatch", "wrong cache_key_hash miss should record cache_key_mismatch")
            assert_true(wrong_key_events[0].get("worker_id") is None, "wrong cache_key_hash miss should not name a selected worker")
            wrong_key_audit_rows = registry_audit_rows_for_request(state, wrong_key_request_id)
            wrong_key_audit = assert_registry_audit_row(wrong_key_audit_rows, action="fallback", operation="fallback", outcome="fallback", message="wrong cache_key_hash should append a registry audit fallback row")
            assert_true(wrong_key_audit.get("cache_key_hash") == wrong_strict_key_hash, "wrong cache_key_hash audit row should record requested hash")

            forged_cache_key_hash = fake_sha("forged-manifest-cache-key")
            forged_key_cache = seed_cache_entry(
                state,
                "strict-key-forged-manifest",
                hot_local=True,
                durable_blob=True,
                manifest_overrides={"cache_key_hash": forged_cache_key_hash},
                registry_overrides={"cache_key_hash": forged_cache_key_hash},
            )
            canonical_forged_key_hash = cache_router_daemon.cache_key_hash_from_record(
                forged_key_cache["manifest"],
                label="forged strict-key smoke manifest",
            )
            assert_true(canonical_forged_key_hash != forged_cache_key_hash, "forged cache key fixture must disagree with canonical material")
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-forged-manifest",
                        "cache_key_hash": forged_cache_key_hash,
                        "allow_fallback": False,
                    },
                },
            )
            forged_key_body = json.loads(raw.decode("utf-8"))
            forged_key_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "manifest cache_key_hash that does not match canonical key material should be treated as a cache miss")
            assert_true(forged_key_body.get("error", {}).get("type") == "cache_not_found", "forged manifest cache key should use cache_not_found")
            assert_true("x-cache-router-cache-hit-level" not in headers, "forged manifest cache key should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "forged manifest cache key should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "forged manifest cache key should not cold-proxy")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={forged_key_request_id}", headers=auth)
            forged_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(forged_key_events) == 1, "forged manifest cache key should write one correlated event")
            assert_true(forged_key_events[0].get("cache_key_hash") == canonical_forged_key_hash, "forged manifest event should record the recomputed canonical key")
            assert_true(forged_key_events[0].get("cache_hit_level") == "registry_only", "forged manifest cache key should stop at registry lookup")
            assert_true(forged_key_events[0].get("compatibility_result") == "mismatch", "forged manifest cache key should be recorded as a mismatch")
            assert_true(forged_key_events[0].get("fallback_reason") == "cache_key_mismatch", "forged manifest cache key should record cache_key_mismatch")
            assert_true(forged_key_events[0].get("worker_id") is None, "forged manifest cache key should not name a selected worker")
            forged_key_audit_rows = registry_audit_rows_for_request(state, forged_key_request_id)
            forged_key_audit = assert_registry_audit_row(forged_key_audit_rows, action="fallback", operation="fallback", outcome="fallback", message="forged manifest cache key should append a registry audit fallback row")
            assert_true(forged_key_audit.get("cache_key_hash") == canonical_forged_key_hash, "forged manifest audit row should record the recomputed canonical key")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            worker_mutations_before = (
                fake_state.tokenize_calls,
                fake_state.llama_completion_calls,
                fake_state.slot_erase_calls,
                fake_state.slot_save_calls,
                fake_backup_state.tokenize_calls,
                fake_backup_state.llama_completion_calls,
                fake_backup_state.slot_erase_calls,
                fake_backup_state.slot_save_calls,
            )
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "auto",
                        "cache_id": "strict-key-exact",
                        "cache_key_hash": wrong_strict_key_hash,
                        "prefix_text": "strict prefix",
                        "suffix_text": "suffix",
                        "allow_fallback": False,
                    },
                },
            )
            auto_wrong_key_body = json.loads(raw.decode("utf-8"))
            auto_wrong_key_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "auto with wrong requested cache_key_hash should not silently build")
            assert_true(auto_wrong_key_body.get("error", {}).get("type") == "cache_not_found", "auto wrong cache_key_hash should use cache_not_found")
            assert_true("x-cache-router-cache-hit-level" not in headers, "auto wrong cache_key_hash should not expose a cache-hit header")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "auto wrong cache_key_hash should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "auto wrong cache_key_hash should not cold-proxy when fallback is false")
            assert_true(
                (
                    fake_state.tokenize_calls,
                    fake_state.llama_completion_calls,
                    fake_state.slot_erase_calls,
                    fake_state.slot_save_calls,
                    fake_backup_state.tokenize_calls,
                    fake_backup_state.llama_completion_calls,
                    fake_backup_state.slot_erase_calls,
                    fake_backup_state.slot_save_calls,
                )
                == worker_mutations_before,
                "auto wrong cache_key_hash should not build or mutate worker slots",
            )
            status, _, raw = request("GET", base + f"/router/decisions?request_id={auto_wrong_key_request_id}", headers=auth)
            auto_wrong_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(auto_wrong_key_events) == 1, "auto wrong cache_key_hash should write one correlated event")
            assert_true(auto_wrong_key_events[0].get("compatibility_result") == "mismatch", "auto wrong cache_key_hash should record mismatch")
            assert_true(auto_wrong_key_events[0].get("fallback_reason") == "cache_key_mismatch", "auto wrong cache_key_hash should record cache_key_mismatch")
            assert_true(auto_wrong_key_events[0].get("worker_id") is None, "auto wrong cache_key_hash should not name a selected worker")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            worker_mutations_before = (
                fake_state.tokenize_calls,
                fake_state.llama_completion_calls,
                fake_state.slot_erase_calls,
                fake_state.slot_save_calls,
                fake_backup_state.tokenize_calls,
                fake_backup_state.llama_completion_calls,
                fake_backup_state.slot_erase_calls,
                fake_backup_state.slot_save_calls,
            )
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "refresh",
                        "cache_id": "strict-key-exact",
                        "cache_key_hash": wrong_strict_key_hash,
                        "prefix_text": "strict replacement prefix",
                        "allow_fallback": False,
                    },
                },
            )
            refresh_wrong_key_body = json.loads(raw.decode("utf-8"))
            refresh_wrong_key_request_id = headers.get("x-cache-router-request-id", "")
            assert_true(status == 404, "refresh with wrong requested cache_key_hash should fail before rebuilding")
            assert_true(refresh_wrong_key_body.get("error", {}).get("type") == "cache_not_found", "refresh wrong cache_key_hash should use cache_not_found")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "refresh wrong cache_key_hash should not restore or generate")
            assert_true(
                (
                    fake_state.tokenize_calls,
                    fake_state.llama_completion_calls,
                    fake_state.slot_erase_calls,
                    fake_state.slot_save_calls,
                    fake_backup_state.tokenize_calls,
                    fake_backup_state.llama_completion_calls,
                    fake_backup_state.slot_erase_calls,
                    fake_backup_state.slot_save_calls,
                )
                == worker_mutations_before,
                "refresh wrong cache_key_hash should not tokenize, erase, save, or run build prefill",
            )
            status, _, raw = request("GET", base + f"/router/decisions?request_id={refresh_wrong_key_request_id}", headers=auth)
            refresh_wrong_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(refresh_wrong_key_events) == 1, "refresh wrong cache_key_hash should write one correlated event")
            assert_true(refresh_wrong_key_events[0].get("compatibility_result") == "mismatch", "refresh wrong cache_key_hash should record mismatch")
            assert_true(refresh_wrong_key_events[0].get("fallback_reason") == "cache_key_mismatch", "refresh wrong cache_key_hash should record cache_key_mismatch")
            assert_true(refresh_wrong_key_events[0].get("worker_id") is None, "refresh wrong cache_key_hash should not name a selected worker")

            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "strict-key-exact",
                        "cache_key_hash": "not-a-sha256",
                        "allow_fallback": False,
                    },
                },
            )
            invalid_hash_body = json.loads(raw.decode("utf-8"))
            assert_true(status == 400, "malformed requested cache_key_hash should be rejected before lookup")
            assert_true(invalid_hash_body.get("error", {}).get("type") == "invalid_request_error", "malformed cache_key_hash should use invalid_request_error")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "malformed cache_key_hash should not restore or generate")

            seed_cache_entry(state, "missing-cache-key", hot_local=True, durable_blob=True, manifest_overrides={"cache_key_hash": None})
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {"mode": "use", "cache_id": "missing-cache-key", "prefix_text": "prefix", "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "manifest missing cache_key_hash should fail before restore")
            assert_true("manifest missing required fields: cache_key_hash" in body.get("error", {}).get("message", ""), "manifest validation error should be explicit")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "manifest validation failure should not restore or generate")
            missing_key_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={missing_key_request_id}", headers=auth)
            missing_key_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(missing_key_events) == 1, "manifest validation failure should write one correlated decision event")
            assert_true(missing_key_events[0].get("validation_status") == "quarantined", "manifest validation failure event should be quarantined")
            assert_true(missing_key_events[0].get("fallback_reason") == "manifest_quarantined", "manifest validation failure event should use bounded manifest reason")
            assert_true(missing_key_events[0].get("worker_id") is None, "manifest validation failure should not name a selected worker")

            bad_json = seed_cache_entry(state, "bad-json-manifest", hot_local=True, durable_blob=True)
            Path(bad_json["entry"]["manifest_path"]).write_text("{not-json", encoding="utf-8")
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {"mode": "use", "cache_id": "bad-json-manifest", "prefix_text": "prefix", "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "malformed manifest JSON should fail before restore")
            assert_true("manifest invalid JSON" in body.get("error", {}).get("message", ""), "malformed manifest error should be explicit")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "malformed manifest should not restore or generate")

            empty_manifest = seed_cache_entry(state, "empty-manifest", hot_local=True, durable_blob=True)
            cache_router_daemon.write_json(Path(empty_manifest["entry"]["manifest_path"]), {})
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {"mode": "use", "cache_id": "empty-manifest", "prefix_text": "prefix", "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "empty manifest should fail before restore")
            assert_true("manifest missing or empty" in body.get("error", {}).get("message", ""), "empty manifest error should be explicit")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "empty manifest should not restore or generate")

            wrong_ctx_cache = seed_cache_entry(state, "wrong-ctx", hot_local=True, durable_blob=True, manifest_overrides={"ctx_size": 8192})
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {"mode": "use", "cache_id": "wrong-ctx", "cache_key_hash": wrong_ctx_cache["entry"]["cache_key_hash"], "allow_fallback": False},
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "strict compatibility mismatch should fail before restore")
            assert_true("ctx_size mismatch" in body.get("error", {}).get("message", ""), "strict compatibility error should name the mismatched field")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "strict compatibility mismatch should not restore or generate")
            wrong_ctx_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={wrong_ctx_request_id}", headers=auth)
            wrong_ctx_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and wrong_ctx_events, "strict compatibility failure should write correlated decision events")
            assert_true(
                all(row.get("validation_status") == "quarantined" and row.get("fallback_reason") == "cache_key_mismatch" for row in wrong_ctx_events),
                "strict compatibility failure events should be quarantined cache_key_mismatch events",
            )

            strict_negative_cases = [
                ("wrong-model-architecture", "model_architecture", "different-architecture", "model_architecture mismatch"),
                ("wrong-model-hash", "model_hash", fake_sha("wrong-model"), "model_hash mismatch"),
                ("wrong-tokenizer-hash", "tokenizer_hash", fake_sha("wrong-tokenizer"), "tokenizer_hash mismatch"),
                ("wrong-chat-template-hash", "chat_template_effective_hash", fake_sha("wrong-chat-template"), "chat_template_effective_hash mismatch"),
                ("wrong-tools-schema-hash", "tools_schema_hash", fake_sha("wrong-tools"), "tools_schema_hash mismatch"),
                ("wrong-system-prompt-hash", "system_prompt_hash", fake_sha("wrong-system-prompt"), "system_prompt_hash mismatch"),
                ("wrong-special-token-policy", "special_token_policy", "different-special-policy", "special_token_policy mismatch"),
                ("wrong-llama-commit", "llama_cpp_source_commit", "differentcommit", "llama_cpp_source_commit mismatch"),
                ("wrong-cache-abi", "llama_cpp_cache_abi_version", "cache-abi-smoke-v2", "llama_cpp_cache_abi_version mismatch"),
                ("wrong-patchset", "patchset_id", "cache-router-patchset-v2", "patchset_id mismatch"),
                ("wrong-backend", "build_backend", "rocm_hip", "build_backend mismatch"),
                ("wrong-driver", "gpu_backend_driver", "different-driver", "gpu_backend_driver mismatch"),
                ("wrong-rope-base", "rope_freq_base", "1000000", "rope_freq_base mismatch"),
                ("wrong-rope-scale", "rope_freq_scale", "0.5", "rope_freq_scale mismatch"),
                ("wrong-yarn-metadata", "yarn_or_rope_scaling_metadata", "yarn-factor-2", "yarn_or_rope_scaling_metadata mismatch"),
                ("wrong-n-parallel", "n_parallel", 2, "n_parallel mismatch"),
                ("wrong-n-seq-max", "n_seq_max", 2, "n_seq_max mismatch"),
                ("missing-tokenizer-hash", "tokenizer_hash", None, "manifest missing required fields: tokenizer_hash"),
                ("unknown-tokenizer-hash", "tokenizer_hash", "not_captured", "tokenizer_hash cannot be 'not_captured'"),
                ("missing-special-token-policy", "special_token_policy", None, "manifest missing required fields: special_token_policy"),
                ("unknown-rope-base", "rope_freq_base", "not_captured", "rope_freq_base cannot be 'not_captured'"),
                ("bad-n-parallel", "n_parallel", 0, "n_parallel must be a positive integer"),
            ]
            for cache_id, field, bad_value, expected_error in strict_negative_cases:
                strict_negative_cache = seed_cache_entry(state, cache_id, hot_local=True, durable_blob=True, manifest_overrides={field: bad_value})
                cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
                status, _, raw = request(
                    "POST",
                    base + "/v1/completions",
                    headers=auth,
                    body={
                        "model": "fake-model",
                        "prompt": "suffix",
                        "max_tokens": 1,
                        "cache_router": {
                            "mode": "use",
                            "cache_id": cache_id,
                            "cache_key_hash": strict_negative_cache["entry"]["cache_key_hash"],
                            "allow_fallback": False,
                        },
                    },
                )
                body = json.loads(raw.decode("utf-8"))
                assert_true(status == 500, f"{field} strict negative should fail before restore")
                assert_true(expected_error in body.get("error", {}).get("message", ""), f"{field} error should name the strict-key problem")
                assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, f"{field} strict negative should not restore or generate")

            relocated_cache = seed_cache_entry(state, "relocated-content-address", hot_local=False, durable_blob=True, prefix_text="relocated prefix ")
            stale_blob_path = "/outside-cache-root/not-read.slot"
            relocated_manifest_path = Path(str(relocated_cache["entry"]["manifest_path"]))
            relocated_manifest = cache_router_daemon.read_json(relocated_manifest_path, {})
            relocated_manifest["router_blob_path"] = stale_blob_path
            cache_router_daemon.write_json(relocated_manifest_path, relocated_manifest)
            relocated_registry = state.load_registry()
            for row in relocated_registry.get("entries", []):
                if row.get("cache_id") == "relocated-content-address":
                    row["router_blob_path"] = stale_blob_path
            state.save_registry(relocated_registry)
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "relocated-content-address",
                        "cache_key_hash": relocated_cache["entry"]["cache_key_hash"],
                        "prefix_text": "relocated prefix ",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "hydrate should use the current content-addressed blob path when manifest router_blob_path is stale")
            assert_true(body.get("choices", [{}])[0].get("text") == "cached-ok", "relocated content-address hydrate should preserve suffix generation")
            assert_true(headers.get("x-cache-router-cache-hit-level") == "durable_blob", "relocated content-address hydrate should report durable_blob")
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "relocated content-address hydrate should restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"] + 1, "relocated content-address hydrate should generate once after restore")

            seed_cache_entry(state, "missing-blob", hot_local=False, durable_blob=False, prefix_text="prefix ")
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before_ids = {id(row) for row in openai_completion_requests(fake_state, fake_backup_state)}
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "missing-blob",
                        "prefix_text": "prefix ",
                        "suffix_text": "suffix",
                        "allow_fallback": True,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "missing durable blob should cold-prefill when fallback is allowed")
            assert_true(body.get("choices", [{}])[0].get("text") == "ok", "missing durable blob fallback should use normal OpenAI completion")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "missing durable blob should not restore or generate")
            new_forwarded = [row for row in openai_completion_requests(fake_state, fake_backup_state) if id(row) not in forwarded_before_ids]
            assert_true(len(new_forwarded) == 1, "missing durable blob fallback should make one cold OpenAI completion request")
            assert_true(new_forwarded[0]["body"].get("prompt") == "prefix suffix", "cold fallback should reconstruct prefix plus suffix prompt")
            assert_true("cache_router" not in new_forwarded[0]["body"], "cold fallback should strip cache_router before backend pass-through")
            missing_blob_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={missing_blob_request_id}", headers=auth)
            missing_blob_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(missing_blob_events) >= 2, "missing blob fallback should keep cache failure and cold proxy events correlated")
            assert_true(
                any(row.get("fallback_reason") == "hydration_failed" and row.get("validation_status") == "quarantined" for row in missing_blob_events),
                "missing blob fallback should record a quarantined hydration failure event",
            )
            assert_true(
                any(row.get("decision") == "cold_prefill" and row.get("cache_hit_level") == "none" for row in missing_blob_events),
                "missing blob fallback should record the cold prefill selection event",
            )

            missing_blob_no_prefix = seed_cache_entry(state, "missing-blob-no-prefix", hot_local=False, durable_blob=False)
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before = len(openai_completion_requests(fake_state, fake_backup_state))
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "missing-blob-no-prefix",
                        "cache_key_hash": missing_blob_no_prefix["entry"]["cache_key_hash"],
                        "allow_fallback": True,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "cache fallback without prefix_text should fail because the full prompt cannot be reconstructed")
            assert_true("prefix_text" in body.get("error", {}).get("message", ""), "no-prefix fallback error should explain the missing full-prompt input")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "no-prefix fallback failure should not restore or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_before, "no-prefix fallback failure should not proxy a suffix-only cold request")

            missing_blob_no_fallback = seed_cache_entry(state, "missing-blob-no-fallback", hot_local=False, durable_blob=False)
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "missing-blob-no-fallback",
                        "cache_key_hash": missing_blob_no_fallback["entry"]["cache_key_hash"],
                        "prefix_text": "prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "missing durable blob should fail when fallback is disabled")
            assert_true("router blob missing" in body.get("error", {}).get("message", ""), "missing durable blob no-fallback error should be explicit")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "missing durable blob without fallback should not restore or generate")

            corrupt = seed_cache_entry(state, "corrupt-blob", hot_local=False, durable_blob=True, prefix_text="prefix ")
            corrupt["blob_path"].write_bytes(b"corrupt-slot-bytes")
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "corrupt-blob",
                        "prefix_text": "prefix ",
                        "suffix_text": "suffix",
                        "allow_fallback": True,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "corrupt durable blob should cold-prefill when fallback is allowed")
            assert_true(body.get("choices", [{}])[0].get("text") == "ok", "corrupt durable blob fallback should use normal OpenAI completion")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_before, "corrupt durable blob should not restore or generate")
            corrupt_manifest = cache_router_daemon.read_json(Path(corrupt["entry"]["manifest_path"]), {})
            corrupt_registry = state.load_registry()
            corrupt_registry_entry = next((row for row in corrupt_registry.get("entries", []) if row.get("cache_id") == "corrupt-blob"), {})
            assert_true(corrupt_manifest.get("validation_status") == "quarantined", "corrupt durable blob should persist manifest quarantine")
            assert_true(corrupt_manifest.get("quarantine_reason") == "corrupt_blob", "corrupt durable blob manifest should store bounded reason enum")
            assert_true(corrupt_registry_entry.get("validation_status") == "quarantined", "corrupt durable blob should persist registry quarantine")
            assert_true(corrupt_registry_entry.get("quarantine_reason") == "corrupt_blob", "corrupt durable blob registry should store bounded reason enum")
            for worker_id in ("worker-main", "worker-backup"):
                assert_true(corrupt_manifest.get("worker_residency", {}).get(worker_id) is False, f"corrupt manifest should clear {worker_id} residency")
                assert_true(corrupt_registry_entry.get("worker_residency", {}).get(worker_id) is False, f"corrupt registry should clear {worker_id} residency")
            status, _, raw = request("GET", base + "/router/cache", headers=auth)
            cache_rows = json.loads(raw.decode("utf-8")).get("entries", [])
            admin_corrupt_row = next((row for row in cache_rows if row.get("cache_id") == "corrupt-blob"), {})
            assert_true(status == 200, "router cache admin endpoint should be available after corrupt blob quarantine")
            assert_true(admin_corrupt_row.get("validation_status") == "quarantined", "router cache admin endpoint should expose bounded quarantine status")
            assert_true(admin_corrupt_row.get("quarantine_reason") == "corrupt_blob", "router cache admin endpoint should expose bounded quarantine reason")
            assert_true(str(corrupt["blob_path"]) not in json.dumps(admin_corrupt_row), "router cache admin endpoint should not expose the raw corrupt blob path")
            corrupt_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={corrupt_request_id}", headers=auth)
            corrupt_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(corrupt_events) >= 2, "corrupt blob fallback should keep cache failure and cold proxy events correlated")
            assert_true(
                any(
                    row.get("decision") == "fallback_after_restore_failure"
                    and row.get("cache_hit_level") == "durable_blob"
                    and row.get("fallback_required") is True
                    and row.get("fallback_reason") == "hydration_failed"
                    and row.get("validation_status") == "quarantined"
                    and row.get("compatibility_result") == "match"
                    for row in corrupt_events
                ),
                "corrupt blob fallback should record a quarantined durable hydration failure",
            )
            assert_true(
                any(row.get("decision") == "cold_prefill" and row.get("cache_hit_level") == "none" and row.get("fallback_reason") == "hydration_failed" for row in corrupt_events),
                "corrupt blob fallback should record the cold prefill decision",
            )
            assert_true(
                all((row.get("privacy") or {}).get("raw_cache_blob_path_logged") is False for row in corrupt_events),
                "corrupt blob decision events should not mark raw blob paths as logged",
            )
            assert_true(str(corrupt["blob_path"]) not in json.dumps(corrupt_events), "corrupt blob decision events should not include the raw blob path")
            corrupt_audit_rows = registry_audit_rows_for_request(state, corrupt_request_id)
            corrupt_fallback_audit = assert_registry_audit_row(corrupt_audit_rows, action="fallback", operation="restore", outcome="quarantined", message="corrupt blob should append a registry audit fallback row")
            assert_true("restore" in corrupt_fallback_audit.get("audit_actions", []), "corrupt blob fallback audit row should record restore context")
            corrupt_quarantine_audit = assert_registry_audit_row(corrupt_audit_rows, action="commit", operation="quarantine", outcome="quarantined", message="corrupt blob should append a registry audit quarantine row")
            assert_true(corrupt_quarantine_audit.get("cache_id") == "corrupt-blob", "corrupt blob quarantine audit row should identify cache_id")
            corrupt_wal_rows = [
                row
                for row in registry_wal_rows(state)
                if row.get("operation") == "quarantine_commit" and row.get("cache_id") == "corrupt-blob" and row.get("outcome") == "quarantined"
            ]
            assert_true(corrupt_wal_rows, "corrupt blob quarantine should append a registry WAL quarantine commit row")
            corrupt_audit_text = state.registry_audit_path.read_text(encoding="utf-8")
            assert_true(str(corrupt["blob_path"]) not in corrupt_audit_text, "registry audit log should not include the raw corrupt blob path")
            assert_true(str(root) not in corrupt_audit_text, "registry audit log should not include the temp cache root path")
            assert_true("prefix " not in corrupt_audit_text and "suffix" not in corrupt_audit_text, "registry audit log should not include raw prompt text")
            status, _, raw = request("GET", base + "/metrics", headers=auth)
            assert_true(status == 200, "metrics should be available after corrupt blob fallback")
            metrics_after_corrupt = raw.decode("utf-8")
            assert_true(
                metric_has_line(
                    metrics_after_corrupt,
                    "cachy_router_cache_outcomes_total",
                    {
                        "outcome": "validation_failure",
                        "decision": "fallback_after_restore_failure",
                        "cache_hit_level": "durable_blob",
                        "fallback_reason": "hydration_failed",
                        "validation_status": "quarantined",
                    },
                ),
                "corrupt blob fallback should expose bounded validation-failure metric labels",
            )
            cache_counts_after_quarantine = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_after_quarantine = len(openai_completion_requests(fake_state, fake_backup_state))
            status, _, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "corrupt-blob",
                        "cache_key_hash": corrupt["entry"]["cache_key_hash"],
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 500, "repeat use of quarantined corrupt blob should fail when fallback is disabled")
            assert_true("quarantined" in body.get("error", {}).get("message", ""), "repeat corrupt blob error should name the quarantine state")
            assert_true(worker_cache_attempts(fake_state, fake_backup_state) == cache_counts_after_quarantine, "repeat corrupt blob use should not hydrate, restore, or generate")
            assert_true(len(openai_completion_requests(fake_state, fake_backup_state)) == forwarded_after_quarantine, "repeat corrupt blob use without fallback should not proxy cold")

            seed_cache_entry(state, "restore-fails-cold", hot_local=True, durable_blob=True, prefix_text="prefix ")
            fake_state.fail_restore = True
            fake_backup_state.fail_restore = True
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            forwarded_before_ids = {id(row) for row in openai_completion_requests(fake_state, fake_backup_state)}
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "restore-fails-cold",
                        "prefix_text": "prefix ",
                        "suffix_text": "suffix",
                        "allow_fallback": True,
                    },
                },
            )
            fake_state.fail_restore = False
            fake_backup_state.fail_restore = False
            body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 200, "restore failure should cold-prefill when fallback is allowed and prefix_text is present")
            assert_true(body.get("choices", [{}])[0].get("text") == "ok", "restore failure fallback should use normal OpenAI completion")
            assert_true(cache_counts_after["restore"] >= cache_counts_before["restore"] + 1, "restore failure fallback should attempt cache restore before cold prefill")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"], "restore failure fallback should not call native suffix generation from the failed cache path")
            new_forwarded = [row for row in openai_completion_requests(fake_state, fake_backup_state) if id(row) not in forwarded_before_ids]
            assert_true(len(new_forwarded) == 1, "restore failure fallback should make one cold OpenAI completion request")
            assert_true(new_forwarded[0]["body"].get("prompt") == "prefix suffix", "restore failure fallback should reconstruct prefix plus suffix prompt")
            restore_cold_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={restore_cold_request_id}", headers=auth)
            restore_cold_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(
                any(row.get("fallback_reason") == "restore_validation_failed" and row.get("validation_status") == "quarantined" for row in restore_cold_events),
                "restore failure fallback should record a quarantined restore validation failure",
            )
            assert_true(
                any(row.get("decision") == "cold_prefill" and row.get("cache_hit_level") == "none" for row in restore_cold_events),
                "restore failure fallback should record a cold prefill decision",
            )

            restore_fails = seed_cache_entry(state, "restore-fails", hot_local=True, durable_blob=True)
            fake_state.fail_restore = True
            cache_counts_before = worker_cache_attempts(fake_state, fake_backup_state)
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "restore-fails",
                        "cache_key_hash": restore_fails["entry"]["cache_key_hash"],
                        "prefix_text": "prefix",
                        "worker_id": "worker-main",
                        "allow_fallback": False,
                    },
                },
            )
            fake_state.fail_restore = False
            body = json.loads(raw.decode("utf-8"))
            cache_counts_after = worker_cache_attempts(fake_state, fake_backup_state)
            assert_true(status == 500, "slot restore failure with fallback disabled should return a controlled router error")
            assert_true(cache_counts_after["restore"] == cache_counts_before["restore"] + 1, "restore failure should attempt slot restore once")
            assert_true(cache_counts_after["completion"] == cache_counts_before["completion"], "restore failure should not call native completion")
            restore_fail_request_id = headers.get("x-cache-router-request-id", "")
            status, _, raw = request("GET", base + f"/router/decisions?request_id={restore_fail_request_id}", headers=auth)
            restore_fail_events = json.loads(raw.decode("utf-8")).get("events", [])
            assert_true(status == 200 and len(restore_fail_events) == 1, "restore failure should write one correlated decision event")
            assert_true(restore_fail_events[0].get("validation_status") == "quarantined", "restore failure event should mark cache validation quarantined")
            assert_true(restore_fail_events[0].get("fallback_reason") == "restore_validation_failed", "restore failure event should use bounded fallback reason")
            restore_fail_audit_rows = registry_audit_rows_for_request(state, restore_fail_request_id)
            restore_fail_audit = assert_registry_audit_row(restore_fail_audit_rows, action="restore", operation="restore", outcome="quarantined", message="restore failure should append a registry audit restore row")
            assert_true("fallback" in restore_fail_audit.get("audit_actions", []), "restore failure audit row should record fallback-required state")

            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={
                    "model": "fake-model",
                    "prompt": "suffix",
                    "max_tokens": 1,
                    "cache_router": {
                        "mode": "use",
                        "cache_id": "definitely-missing-cache-entry",
                        "prefix_text": "prefix",
                        "allow_fallback": False,
                    },
                },
            )
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 404, "unknown cache_id should return cache_not_found")
            assert_true(body.get("error", {}).get("type") == "cache_not_found", "unknown cache_id should use cache_not_found error type")
            unknown_request_id = headers.get("x-cache-router-request-id", "")
            unknown_audit_rows = registry_audit_rows_for_request(state, unknown_request_id)
            unknown_audit = assert_registry_audit_row(unknown_audit_rows, action="miss", operation="miss", outcome="miss", message="unknown cache_id should append a registry audit miss row")
            assert_true(unknown_audit.get("cache_id") == "definitely-missing-cache-entry", "unknown cache audit row should identify cache_id")

            status, _, raw = request("GET", base + "/metrics", headers=auth)
            metrics = raw.decode("utf-8")
            assert_true(status == 200, "authorized metrics request after cache scenarios should succeed")
            assert_true(
                metric_has_line(metrics, "cachy_router_cache_events_total", {"validation_status": "quarantined", "fallback_reason": "restore_validation_failed"}),
                "cache events metric should expose validation status and restore fallback reason",
            )
            assert_true(
                metric_has_line(metrics, "cachy_router_cache_outcomes_total", {"outcome": "validation_failure", "fallback_reason": "manifest_quarantined"}),
                "cache outcome metrics should expose pre-restore manifest validation failures",
            )
            for outcome, labels in {
                "hit": {"outcome": "hit", "decision": "restore_then_generate", "cache_hit_level": "local_nvme"},
                "miss": {"outcome": "miss", "fallback_reason": "none"},
                "hydrate": {"outcome": "hydrate", "fallback_reason": "hydration_failed"},
                "restore": {"outcome": "restore"},
                "fallback": {"outcome": "fallback"},
                "validation_failure": {"outcome": "validation_failure", "validation_status": "quarantined"},
            }.items():
                assert_true(
                    metric_has_line(metrics, "cachy_router_cache_outcomes_total", labels),
                    f"cache outcome metrics should expose {outcome} counts",
                )

            state.args.disable_admin_endpoints = True
            admin_paths = ("/router/status", "/router/workers", "/router/cache", "/router/decisions", "/router/decisions?request_id=req-none", "/metrics")
            for admin_path in admin_paths:
                status, headers, raw = request("GET", base + admin_path, headers=auth)
                body = json.loads(raw.decode("utf-8"))
                assert_true(status == 404, f"{admin_path} should return 404 when admin endpoints are disabled")
                assert_true(body.get("error", {}).get("type") == "not_found", f"{admin_path} disabled response should be not_found")
                assert_router_debug_headers(headers, f"{admin_path} disabled response")
            status, headers, raw = request("GET", base + "/v1", headers=auth)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200, "/v1 should remain available when admin endpoints are disabled")
            assert_router_debug_headers(headers, "/v1 discovery response")
            advertised = set(body.get("endpoints", []))
            disabled_admin = {"/router/status", "/router/workers", "/router/cache", "/router/decisions", "/metrics"}
            assert_true(not (advertised & disabled_admin), f"/v1 should not advertise disabled admin endpoints: {sorted(advertised & disabled_admin)}")
            status, headers, raw = request("GET", base + "/v1/models", headers=auth)
            body = json.loads(raw.decode("utf-8"))
            assert_true(status == 200 and body.get("data", [{}])[0].get("id") == "fake-model", "/v1/models should remain available when admin endpoints are disabled")
            assert_router_debug_headers(headers, "/v1/models response with admin disabled")
            status, headers, raw = request(
                "POST",
                base + "/v1/completions",
                headers=auth,
                body={"model": "fake-model", "prompt": "hello", "max_tokens": 1},
            )
            assert_true(status == 200, "normal completions should remain available when admin endpoints are disabled")
            assert_true(headers.get("x-cache-router-worker") in {"worker-main", "worker-backup"}, "normal completion should still route to a worker with admin disabled")
            assert_true(json.loads(raw.decode("utf-8"))["choices"][0]["text"] == "ok", "normal completion response should still come from fake worker")

            print(json.dumps({"ok": True, "fake_worker_url": worker_url, "router_base_url": base}, sort_keys=True))
            return 0
    finally:
        if router is not None:
            router.shutdown()
            router.server_close()
        fake_worker.shutdown()
        fake_worker.server_close()
        fake_backup_worker.shutdown()
        fake_backup_worker.server_close()
        fake_added_worker.shutdown()
        fake_added_worker.server_close()
        fake_incompatible_worker.shutdown()
        fake_incompatible_worker.server_close()
        fake_sidecar.shutdown()
        fake_sidecar.server_close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--crash-build-child":
        raise SystemExit(crash_build_child_main(sys.argv[2:]))
    raise SystemExit(main())
