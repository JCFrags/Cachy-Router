#!/usr/bin/env python3
"""Router-owned durable cache store plus worker-local hydration POC.

This is a same-host simulation of the intended cache-router architecture:

* worker-a saves a llama.cpp slot file to worker-local NVMe;
* the router ingests that file into a durable blob store and manifest registry;
* worker-b starts with an empty local slot directory;
* the router hydrates the durable blob back into worker-b's local slot path;
* worker-b restores the hydrated slot and serves only the suffix route.

The script is deliberately not a production router daemon. It is a controller
for a bounded live experiment and an offline copy/hash self-test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cache_router_one_node_poc import (
    SCHEMA_VERSION,
    append_jsonl,
    cache_event,
    completion,
    ensure_idle,
    generate_prefix,
    get_json,
    post_json,
    read_json,
    sha256_json,
    sha256_text,
    slot_action,
    summarize_completion,
    token_count,
    write_json,
)


DEFAULT_RUNTIME = ""
DEFAULT_MODEL = ""
DEFAULT_MTP_MODEL = ""
DEFAULT_REMOTE_CACHE_ROOT = ".cache/cachy-router/router-store-poc"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_with_hash(src: Path, dst: Path) -> dict[str, Any]:
    start = time.perf_counter()
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    with dst.open("ab") as fh:
        fh.flush()
    return {
        "src": str(src),
        "dst": str(dst),
        "size_bytes": dst.stat().st_size,
        "sha256": sha256_file(dst),
        "wall_ms": (time.perf_counter() - start) * 1000.0,
    }


def run_self_test() -> int:
    with tempfile.TemporaryDirectory(prefix="cache-router-store-self-test-") as td:
        root = Path(td)
        worker_a = root / "workers" / "worker-a" / "slots"
        worker_b = root / "workers" / "worker-b" / "slots"
        store = root / "router-store" / "blobs"
        manifests = root / "router-store" / "manifests"
        source = worker_a / "fake.slot"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes((b"fake-slot-payload-" * 4096) + b"end")
        source_hash = sha256_file(source)
        blob = store / source_hash[:2] / f"{source_hash}.slot"
        ingest = copy_with_hash(source, blob)
        hydrate = copy_with_hash(blob, worker_b / "fake.slot")
        manifest = {
            "schema_version": "2026-07-01.1",
            "cache_key_hash": hashlib.sha256(b"fake-cache-key").hexdigest(),
            "slot_file_sha256": source_hash,
            "slot_file_size_bytes": source.stat().st_size,
            "worker_source": "worker-a",
            "worker_residency": {"worker-a": True, "worker-b": True},
        }
        manifest_path = manifests / f"{manifest['cache_key_hash']}.json"
        write_json(manifest_path, manifest)
        ok = (
            ingest["sha256"] == source_hash
            and hydrate["sha256"] == source_hash
            and hydrate["size_bytes"] == source.stat().st_size
            and read_json(manifest_path, {})["slot_file_sha256"] == source_hash
        )
        print(json.dumps({"ok": ok, "ingest": ingest, "hydrate": hydrate}, indent=2, sort_keys=True))
        return 0 if ok else 1


def ssh_command_prefix() -> list[str]:
    """Return the SSH command prefix used by the cache-router controllers.

    The Strix Halo lab sometimes needs to bypass a controller-local SSH config
    file while preserving the same remote scripts. Use
    ``CACHE_ROUTER_SSH_CONFIG=/dev/null`` or ``CACHE_ROUTER_SSH_EXTRA_ARGS`` for
    that case instead of changing every call site.
    """
    cmd = ["ssh"]
    ssh_config = os.environ.get("CACHE_ROUTER_SSH_CONFIG", "").strip()
    if ssh_config:
        cmd.extend(["-F", ssh_config])
    extra = os.environ.get("CACHE_ROUTER_SSH_EXTRA_ARGS", "").strip()
    if extra:
        cmd.extend(shlex.split(extra))
    return cmd


def scp_command_prefix() -> list[str]:
    cmd = ["scp"]
    ssh_config = os.environ.get("CACHE_ROUTER_SSH_CONFIG", "").strip()
    if ssh_config:
        cmd.extend(["-F", ssh_config])
    extra = os.environ.get("CACHE_ROUTER_SCP_EXTRA_ARGS", "").strip()
    if extra:
        cmd.extend(shlex.split(extra))
    return cmd


def ssh_script(host: str, script: str, *, input_text: str = "", timeout: float = 60.0) -> str:
    proc = subprocess.run(
        ssh_command_prefix() + ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, "/bin/bash", "-s"],
        input=script if not input_text else input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ssh {host} failed rc={proc.returncode}:\n{proc.stdout}")
    return proc.stdout


def remote_json(host: str, script: str, payload: dict[str, Any] | None = None, *, timeout: float = 60.0) -> Any:
    if payload is None:
        input_text = script
    else:
        input_text = script + "\n__JSON_PAYLOAD__\n" + json.dumps(payload, sort_keys=True)
    out = ssh_script(host, input_text, timeout=timeout)
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"remote JSON parse failed: {exc}\n{out}") from exc


def remote_preflight(host: str, cache_root: str, runtime: str, model: str, mtp_model: str) -> dict[str, Any]:
    script = r'''
set -euo pipefail
python3 - "$@" <<'PY'
import json, os, shutil, socket, subprocess
cache_root, runtime, model, mtp = """CACHE_ROOT""", """RUNTIME""", """MODEL""", """MTP"""
def file_info(path):
    try:
        st = os.stat(path)
        return {"exists": True, "size": st.st_size}
    except FileNotFoundError:
        return {"exists": False, "size": None}
health = subprocess.run(["curl", "-fsS", "http://127.0.0.1:8081/health"], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
slots = subprocess.run(["curl", "-fsS", "http://127.0.0.1:8081/slots"], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
df = shutil.disk_usage(os.path.dirname(cache_root))
mem = {}
with open("/proc/meminfo", "r", encoding="utf-8") as fh:
    for line in fh:
        if line.startswith(("MemAvailable:", "SwapFree:")):
            key, rest = line.split(":", 1)
            mem[key] = rest.strip()
print(json.dumps({
    "hostname": socket.gethostname(),
    "user": os.environ.get("USER"),
    "cache_root": cache_root,
    "disk_free_bytes": df.free,
    "meminfo": mem,
    "runtime": file_info(runtime),
    "model": file_info(model),
    "mtp_model": file_info(mtp),
    "health_status": health.returncode,
    "health_body": health.stdout.strip()[:500],
    "slots_status": slots.returncode,
    "slots_body": slots.stdout.strip()[:1000],
}, sort_keys=True))
PY
'''
    script = (
        script.replace('"""CACHE_ROOT"""', json.dumps(cache_root))
        .replace('"""RUNTIME"""', json.dumps(runtime))
        .replace('"""MODEL"""', json.dumps(model))
        .replace('"""MTP"""', json.dumps(mtp_model))
    )
    return remote_json(host, script, timeout=60)


def remote_find_server_by_port(host: str, port: int) -> list[dict[str, Any]]:
    script = f'''
python3 - <<'PY'
import json, os
port = {port!r}
matches = []
for name in os.listdir("/proc"):
    if not name.isdigit():
        continue
    path = f"/proc/{{name}}/cmdline"
    try:
        raw = open(path, "rb").read()
    except Exception:
        continue
    if not raw:
        continue
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\\0") if part]
    if not argv or "llama-server" not in os.path.basename(argv[0]):
        continue
    found = False
    for i, arg in enumerate(argv):
        if arg == "--port" and i + 1 < len(argv) and argv[i + 1] == str(port):
            found = True
        if arg == f"--port={{port}}":
            found = True
    if found:
        try:
            start_ticks = open(f"/proc/{{name}}/stat", "r", encoding="utf-8").read().split()[21]
        except Exception:
            start_ticks = None
        matches.append({{"pid": int(name), "argv": argv, "start_ticks": start_ticks}})
print(json.dumps(matches, sort_keys=True))
PY
'''
    return remote_json(host, script, timeout=30)


def remote_start_process(host: str, argv: list[str], log_path: str, *, timeout: float = 60.0) -> dict[str, Any]:
    payload_text = json.dumps({"argv": argv, "log_path": log_path}, sort_keys=True)
    script = r'''
import json, os, subprocess, sys
payload = json.loads(PAYLOAD_TEXT)
argv = payload["argv"]
log_path = payload["log_path"]
os.makedirs(os.path.dirname(log_path), exist_ok=True)
fh = open(log_path, "ab", buffering=0)
proc = subprocess.Popen(argv, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
print(json.dumps({"pid": proc.pid, "log_path": log_path, "argv": argv}, sort_keys=True))
'''.replace("PAYLOAD_TEXT", repr(payload_text))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_stop_pid(host: str, pid: int, *, timeout: float = 60.0) -> dict[str, Any]:
    script = f'''
set -euo pipefail
pid={int(pid)}
if kill -0 "$pid" 2>/dev/null; then
  kill -TERM "$pid" 2>/dev/null || true
  for i in $(seq 1 120); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo '{{"pid":'$pid',"stopped":true,"signal":"TERM"}}'
      exit 0
    fi
    sleep 0.5
  done
  echo '{{"pid":'$pid',"stopped":false,"signal":"TERM"}}'
  exit 1
else
  echo '{{"pid":'$pid',"stopped":true,"already_absent":true}}'
fi
'''
    return remote_json(host, script, timeout=timeout)


def remote_prepare_run_dirs(host: str, paths: list[str]) -> dict[str, Any]:
    quoted = "\n".join(paths)
    script = f'''
set -euo pipefail
while IFS= read -r p; do
  [ -n "$p" ] && mkdir -p "$p"
done <<'EOF'
{quoted}
EOF
python3 - <<'PY'
import json
print(json.dumps({{"ok": True}}))
PY
'''
    return remote_json(host, script, timeout=60)


def remote_file_info(host: str, path: str, *, hash_file: bool = False, timeout: float = 120.0) -> dict[str, Any]:
    payload_text = json.dumps({"path": path, "hash_file": hash_file}, sort_keys=True)
    script = r'''
import hashlib, json, os, sys
payload = json.loads(PAYLOAD_TEXT)
path = payload["path"]
hash_file = payload["hash_file"]
out = {"path": path, "exists": os.path.exists(path)}
if out["exists"]:
    st = os.stat(path)
    out["size_bytes"] = st.st_size
    out["mtime"] = st.st_mtime
    if hash_file:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        out["sha256"] = h.hexdigest()
print(json.dumps(out, sort_keys=True))
'''.replace("PAYLOAD_TEXT", repr(payload_text))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_ingest_blob(
    host: str,
    *,
    source_path: str,
    blob_path: str,
    manifest_path: str,
    registry_path: str,
    manifest: dict[str, Any],
    timeout: float = 900.0,
) -> dict[str, Any]:
    payload_text = json.dumps(
        {
            "source_path": source_path,
            "blob_path": blob_path,
            "manifest_path": manifest_path,
            "registry_path": registry_path,
            "manifest": manifest,
            "now": now_iso(),
        },
        sort_keys=True,
    )
    script = r'''
import hashlib, json, os, shutil, sys, time
payload = json.loads(PAYLOAD_TEXT)
src = payload["source_path"]
blob = payload["blob_path"]
manifest_path = payload["manifest_path"]
registry_path = payload["registry_path"]
manifest = payload["manifest"]
def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
start = time.perf_counter()
os.makedirs(os.path.dirname(blob), exist_ok=True)
os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
os.makedirs(os.path.dirname(registry_path), exist_ok=True)
shutil.copyfile(src, blob)
with open(blob, "ab") as fh:
    fh.flush()
    os.fsync(fh.fileno())
blob_sha = file_hash(blob)
source_sha = file_hash(src)
st = os.stat(blob)
manifest = dict(manifest)
manifest.update({
    "router_blob_path": blob,
    "slot_file_sha256": blob_sha,
    "slot_file_size_bytes": st.st_size,
    "ingested_at": payload["now"],
})
tmp_manifest = manifest_path + ".tmp"
with open(tmp_manifest, "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2, sort_keys=True)
    fh.write("\n")
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp_manifest, manifest_path)
try:
    with open(registry_path, "r", encoding="utf-8") as fh:
        registry = json.load(fh)
except FileNotFoundError:
    registry = {"schema_version": "2026-07-01.1", "entries": []}
registry["entries"] = [row for row in registry.get("entries", []) if row.get("cache_key_hash") != manifest.get("cache_key_hash")]
registry["entries"].append({
    "cache_key_hash": manifest.get("cache_key_hash"),
    "manifest_path": manifest_path,
    "router_blob_path": blob,
    "slot_file_sha256": blob_sha,
    "slot_file_size_bytes": st.st_size,
    "worker_residency": manifest.get("worker_residency", {}),
    "updated_at": payload["now"],
})
tmp_registry = registry_path + ".tmp"
with open(tmp_registry, "w", encoding="utf-8") as fh:
    json.dump(registry, fh, indent=2, sort_keys=True)
    fh.write("\n")
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp_registry, registry_path)
print(json.dumps({
    "source_path": src,
    "source_sha256": source_sha,
    "router_blob_path": blob,
    "router_blob_sha256": blob_sha,
    "router_blob_size_bytes": st.st_size,
    "manifest_path": manifest_path,
    "registry_path": registry_path,
    "sha256_match": source_sha == blob_sha,
    "wall_ms": (time.perf_counter() - start) * 1000.0,
}, sort_keys=True))
'''.replace("PAYLOAD_TEXT", repr(payload_text))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_copy_file(host: str, *, source_path: str, dest_path: str, timeout: float = 900.0) -> dict[str, Any]:
    payload_text = json.dumps({"source_path": source_path, "dest_path": dest_path}, sort_keys=True)
    script = r'''
import hashlib, json, os, shutil, sys, time
payload = json.loads(PAYLOAD_TEXT)
src = payload["source_path"]
dst = payload["dest_path"]
def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
start = time.perf_counter()
before_exists = os.path.exists(dst)
os.makedirs(os.path.dirname(dst), exist_ok=True)
shutil.copyfile(src, dst)
with open(dst, "ab") as fh:
    fh.flush()
    os.fsync(fh.fileno())
src_sha = file_hash(src)
dst_sha = file_hash(dst)
st = os.stat(dst)
print(json.dumps({
    "source_path": src,
    "dest_path": dst,
    "dest_existed_before": before_exists,
    "source_sha256": src_sha,
    "dest_sha256": dst_sha,
    "sha256_match": src_sha == dst_sha,
    "size_bytes": st.st_size,
    "wall_ms": (time.perf_counter() - start) * 1000.0,
}, sort_keys=True))
'''.replace("PAYLOAD_TEXT", repr(payload_text))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_move_file(host: str, *, source_path: str, dest_path: str, timeout: float = 120.0) -> dict[str, Any]:
    payload_text = json.dumps({"source_path": source_path, "dest_path": dest_path}, sort_keys=True)
    script = r'''
import json, os, shutil, sys
payload = json.loads(PAYLOAD_TEXT)
src = payload["source_path"]
dst = payload["dest_path"]
before = os.path.exists(src)
os.makedirs(os.path.dirname(dst), exist_ok=True)
if before:
    shutil.move(src, dst)
print(json.dumps({"source_path": src, "dest_path": dst, "source_existed_before": before, "source_exists_after": os.path.exists(src), "dest_exists_after": os.path.exists(dst)}, sort_keys=True))
'''.replace("PAYLOAD_TEXT", repr(payload_text))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def wait_http_ok(base_url: str, timeout_s: float = 900.0) -> dict[str, Any]:
    start = time.perf_counter()
    last_error = ""
    while time.perf_counter() - start < timeout_s:
        try:
            status, body, _ = http_request("GET", base_url + "/health", timeout=5.0)
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                return {"ready": True, "elapsed_ms": (time.perf_counter() - start) * 1000.0, "body": body}
        except Exception as exc:  # noqa: BLE001 - diagnostic loop
            last_error = str(exc)
        time.sleep(1.0)
    raise RuntimeError(f"{base_url}/health did not become ready; last_error={last_error}")


def http_request(method: str, url: str, *, payload: dict[str, Any] | None = None, timeout: float) -> tuple[int, Any, float]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept-Encoding": "identity"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(raw) if raw else {}, (time.perf_counter() - start) * 1000.0
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body: Any = json.loads(raw)
        except json.JSONDecodeError:
            body = {"error_text": raw}
        return exc.code, body, (time.perf_counter() - start) * 1000.0


def wait_remote_8081(host: str, timeout_s: float = 900.0) -> dict[str, Any]:
    start = time.perf_counter()
    last = ""
    while time.perf_counter() - start < timeout_s:
        try:
            out = ssh_script(
                host,
                "curl -fsS http://127.0.0.1:8081/health",
                timeout=15,
            )
            body = json.loads(out)
            if body.get("status") == "ok":
                return {"ready": True, "elapsed_ms": (time.perf_counter() - start) * 1000.0, "body": body}
        except Exception as exc:  # noqa: BLE001 - diagnostic loop
            last = str(exc)
        time.sleep(1.0)
    raise RuntimeError(f"8081 did not become healthy; last={last}")


def start_tunnel(host: str, local_port: int, remote_port: int) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        ssh_command_prefix()
        + [
            "-o",
            "BatchMode=yes",
            "-o",
            "ExitOnForwardFailure=yes",
            "-N",
            "-L",
            f"{local_port}:127.0.0.1:{remote_port}",
            host,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.7)
    if proc.poll() is not None:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"ssh tunnel failed to start rc={proc.returncode}: {stderr}")
    return proc


def stop_tunnel(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def temp_server_argv(args: argparse.Namespace, *, worker_id: str, slot_dir: str) -> list[str]:
    argv = [
        args.llama_server,
        "-dev",
        "Vulkan0",
        "-m",
        args.model,
        "-c",
        str(args.ctx_size),
        "-ngl",
        "999",
        "-fa",
        "on",
        "--host",
        getattr(args, "worker_bind_host", "127.0.0.1"),
        "--port",
        str(args.temp_port),
        "-b",
        "2048",
        "-ub",
        "2048",
        "-ctk",
        "q8_0",
        "-ctv",
        "q8_0",
        "--no-mmap",
        "--direct-io",
        "-np",
        "1",
        "--alias",
        f"Step-3.7-router-store-{worker_id}",
        "--timeout",
        "200000",
        "--threads",
        "16",
        "--threads-http",
        "4",
        "--metrics",
        "--slots",
        "--slot-save-path",
        slot_dir,
        "--cache-ram",
        str(getattr(args, "cache_ram_mib", 0)),
        "--cache-prompt",
        "--cache-reuse",
        "0",
        "--slot-prompt-similarity",
        "0.7",
        "--jinja",
        "--kv-unified",
        "-cb",
        "--no-ui",
        "--no-cache-idle-slots",
        "--no-context-shift",
        "--ctx-checkpoints",
        str(getattr(args, "ctx_checkpoints", 0)),
        "--reasoning",
        "auto",
        "--reasoning-format",
        "deepseek",
    ]
    if getattr(args, "mtp_enabled", True):
        argv.extend(
            [
                "--spec-draft-model",
                args.mtp_model,
                "--spec-type",
                "draft-mtp",
                "--spec-draft-n-max",
                str(getattr(args, "spec_draft_n_max", 2)),
                "--spec-draft-n-min",
                str(getattr(args, "spec_draft_n_min", 0)),
                "--spec-draft-p-split",
                str(getattr(args, "spec_draft_p_split", "0.10")),
                "--spec-draft-p-min",
                str(getattr(args, "spec_draft_p_min", "0.60")),
                "--spec-draft-type-k",
                str(getattr(args, "spec_draft_type_k", "q8_0")),
                "--spec-draft-type-v",
                str(getattr(args, "spec_draft_type_v", "q8_0")),
                "--spec-draft-backend-sampling",
                "--spec-draft-ngl",
                str(getattr(args, "spec_draft_ngl", "all")),
            ]
        )
    argv.extend(["-fit", "off"])
    mmproj_model = str(getattr(args, "mmproj_model", "") or "")
    if mmproj_model:
        argv.extend(["--mmproj", mmproj_model, "--mmproj-offload"])
    return argv


def save_service_snapshot(path: Path, snapshot: dict[str, Any]) -> None:
    lines = [
        "Router-store hydration POC service snapshot",
        f"Captured UTC: {now_iso()}",
        "",
        f"Remote host: {snapshot.get('remote_host')}",
        "No sudo was used.",
        "",
        "Initial 8081 state:",
        f"- health: {snapshot.get('initial_health')}",
        f"- systemd unit active state: {snapshot.get('initial_unit_state')}",
        f"- captured server PIDs: {snapshot.get('initial_pids')}",
        "",
        "POC worker simulation:",
        f"- worker-a PID: {snapshot.get('worker_a_pid')}",
        f"- worker-b PID: {snapshot.get('worker_b_pid')}",
        f"- temp port: {snapshot.get('temp_port')}",
        "",
        "Final recovery:",
        f"- final 8081 health: {snapshot.get('final_health')}",
        f"- final 8081 slots: {snapshot.get('final_slots')}",
        f"- final systemd unit active state: {snapshot.get('final_unit_state')}",
        f"- final 8081 PIDs: {snapshot.get('final_pids')}",
        f"- temp port listener after cleanup: {snapshot.get('temp_port_listener')}",
        "",
        "Operational caveat:",
        "- If the final unit state is inactive but /health is ok, 8081 was restored as a manual user-owned process.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def update_reductions(results: dict[str, Any]) -> None:
    phases = results.get("phases") or {}
    cold_ms = (((phases.get("cold") or {}).get("completion") or {}).get("timings") or {}).get("prompt_ms")
    restored_ms = (((phases.get("hydrate_restore") or {}).get("completion") or {}).get("timings") or {}).get("prompt_ms")
    restore_ms = ((phases.get("hydrate_restore") or {}).get("restore") or {}).get("wall_ms")
    hydrate_ms = ((phases.get("hydrate_restore") or {}).get("hydrate") or {}).get("wall_ms")
    hot_ms = (((phases.get("hot_local_restore") or {}).get("completion") or {}).get("timings") or {}).get("prompt_ms")
    hot_restore_ms = ((phases.get("hot_local_restore") or {}).get("restore") or {}).get("wall_ms")
    reductions: dict[str, Any] = {
        "target_reduction_percent": 90.0,
        "cold_prompt_ms": cold_ms,
        "hydrated_route_prompt_ms": restored_ms,
        "hydration_ms": hydrate_ms,
        "restore_ms": restore_ms,
        "hot_local_route_prompt_ms": hot_ms,
        "hot_local_restore_ms": hot_restore_ms,
    }
    if isinstance(cold_ms, (int, float)) and cold_ms > 0:
        if isinstance(restored_ms, (int, float)):
            reductions["prompt_only_reduction_percent"] = 100.0 * (1.0 - restored_ms / cold_ms)
            if isinstance(restore_ms, (int, float)):
                reductions["restore_inclusive_reduction_percent"] = 100.0 * (1.0 - ((restore_ms + restored_ms) / cold_ms))
            if isinstance(hydrate_ms, (int, float)) and isinstance(restore_ms, (int, float)):
                reductions["hydrate_restore_inclusive_reduction_percent"] = 100.0 * (
                    1.0 - ((hydrate_ms + restore_ms + restored_ms) / cold_ms)
                )
        if isinstance(hot_ms, (int, float)):
            reductions["hot_local_prompt_only_reduction_percent"] = 100.0 * (1.0 - hot_ms / cold_ms)
            if isinstance(hot_restore_ms, (int, float)):
                reductions["hot_local_restore_inclusive_reduction_percent"] = 100.0 * (
                    1.0 - ((hot_restore_ms + hot_ms) / cold_ms)
                )
    results["reductions"] = reductions


def update_status(results: dict[str, Any]) -> None:
    phases = results.get("phases") or {}
    hydrate = phases.get("hydrate_restore") or {}
    hydrate_row = hydrate.get("hydrate") or {}
    restore_body = ((hydrate.get("restore") or {}).get("body") or {}) if isinstance(hydrate, dict) else {}
    reductions = results.get("reductions") or {}
    ok = (
        (phases.get("router_ingest") or {}).get("ingest", {}).get("sha256_match") is True
        and hydrate_row.get("sha256_match") is True
        and hydrate_row.get("dest_existed_before") is False
        and isinstance(restore_body.get("n_restored"), int)
        and restore_body.get("n_restored") > 0
    )
    prompt_reduction = reductions.get("prompt_only_reduction_percent")
    if ok and isinstance(prompt_reduction, (int, float)) and prompt_reduction >= 90.0:
        status = "success"
    elif ok:
        status = "partial_success"
    elif phases.get("router_ingest") and phases.get("hydrate_restore"):
        status = "diagnostic_failure"
    else:
        status = "blocked"
    results["status"] = status
    results["completed_phases"] = [name for name in ("cold", "build_cache", "router_ingest", "hydrate_restore", "hot_local_restore") if name in phases]


def make_inputs(base_url: str, args: argparse.Namespace, worker_a_slot_dir: str) -> dict[str, Any]:
    props = get_json(base_url, "/props", args.timeout)
    prefix, prefix_tokens, repeats = generate_prefix(base_url, args.target_tokens, args.timeout)
    suffix = (
        "\n\nRouter hydration query: Using only the cached public synthetic prefix above, "
        "reply with exactly: router store hydration ok\nAnswer:"
    )
    full_prompt = prefix + suffix
    suffix_tokens = token_count(base_url, suffix, args.timeout)
    full_tokens = token_count(base_url, full_prompt, args.timeout)
    model_id = Path(args.model).name
    key_fields = {
        "prefix_hash": sha256_text(prefix),
        "prefix_tokens": prefix_tokens,
        "model_id": model_id,
        "model_path": props.get("model_path"),
        "ctx_size": (props.get("default_generation_settings") or {}).get("n_ctx"),
        "llama_server_path": args.llama_server,
        "llama_server_version": args.runtime_id,
        "backend": "Vulkan0",
        "cache_type_k": "q8_0",
        "cache_type_v": "q8_0",
        "mtp_enabled": True,
        "spec_draft_model_path": args.mtp_model,
        "slot_save_path": worker_a_slot_dir,
    }
    cache_key_hash = sha256_json(key_fields)
    cache_filename = f"cache-router-store-{cache_key_hash[:16]}.slot"
    return {
        "props": props,
        "prefix": prefix,
        "suffix": suffix,
        "full_prompt": full_prompt,
        "prefix_tokens": prefix_tokens,
        "suffix_tokens": suffix_tokens,
        "full_tokens": full_tokens,
        "prefix_hash": key_fields["prefix_hash"],
        "suffix_hash": sha256_text(suffix),
        "full_prompt_hash": sha256_text(full_prompt),
        "repeats": repeats,
        "model_id": model_id,
        "key_fields": key_fields,
        "cache_key_hash": cache_key_hash,
        "cache_filename": cache_filename,
        "manifest_id": "manifest-" + cache_key_hash[:16],
    }


def run_all(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "results.json"
    events_path = out_dir / "cache-router-events.jsonl"
    snapshot_path = out_dir / "service-snapshot-redacted.txt"
    run_id = args.run_id or out_dir.name
    remote_root = args.remote_cache_root.rstrip("/")
    run_root = f"{remote_root}/runs/{run_id}"
    router_store = f"{remote_root}/router-store"
    worker_a_slots = f"{run_root}/workers/worker-a/slots"
    worker_b_slots = f"{run_root}/workers/worker-b/slots"
    worker_a_logs = f"{run_root}/workers/worker-a/logs"
    worker_b_logs = f"{run_root}/workers/worker-b/logs"
    isolated_slots = f"{run_root}/workers/worker-a/isolated-after-ingest"
    base_url = f"http://127.0.0.1:{args.temp_port}"

    service_snapshot: dict[str, Any] = {"remote_host": args.remote_host, "temp_port": args.temp_port}
    tunnel: subprocess.Popen[str] | None = None
    main_processes: list[dict[str, Any]] = []
    worker_a: dict[str, Any] | None = None
    worker_b: dict[str, Any] | None = None
    restored_main: dict[str, Any] | None = None
    results: dict[str, Any] = {
        "schema_version": "2026-07-01.1",
        "run_id": run_id,
        "created_utc": now_iso(),
        "remote_cache_root": remote_root,
        "router_store": router_store,
        "worker_simulation": {
            "worker_a_slot_save_path": worker_a_slots,
            "worker_b_slot_save_path": worker_b_slots,
            "one_node_simulation": True,
        },
    }

    try:
        preflight = remote_preflight(args.remote_host, remote_root, args.llama_server, args.model, args.mtp_model)
        results["preflight"] = preflight
        service_snapshot["initial_health"] = preflight.get("health_body")
        initial_unit = ssh_script(args.remote_host, "systemctl --no-pager --plain is-active llama-step37.service || true", timeout=15).strip()
        service_snapshot["initial_unit_state"] = initial_unit
        main_processes = remote_find_server_by_port(args.remote_host, 8081)
        service_snapshot["initial_pids"] = [p.get("pid") for p in main_processes]
        if main_processes:
            for proc in main_processes:
                remote_stop_pid(args.remote_host, int(proc["pid"]), timeout=90)

        remote_prepare_run_dirs(
            args.remote_host,
            [
                worker_a_slots,
                worker_b_slots,
                worker_a_logs,
                worker_b_logs,
                f"{router_store}/blobs",
                f"{router_store}/manifests",
                isolated_slots,
            ],
        )
        tunnel = start_tunnel(args.remote_host, args.temp_port, args.temp_port)

        worker_a_argv = temp_server_argv(args, worker_id="worker-a", slot_dir=worker_a_slots)
        worker_a = remote_start_process(args.remote_host, worker_a_argv, f"{worker_a_logs}/llama-server.log")
        service_snapshot["worker_a_pid"] = worker_a["pid"]
        worker_a_ready = wait_http_ok(base_url, args.ready_timeout)
        results["worker_a"] = {"start": worker_a, "ready": worker_a_ready}
        inputs = make_inputs(base_url, args, worker_a_slots)
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
            "manifest_id": inputs["manifest_id"],
            "cache_filename": inputs["cache_filename"],
        }

        ensure_idle(base_url, 0, args.timeout)
        cold = completion(base_url, inputs["full_prompt"], n_predict=args.n_predict, slot_id=0, cache_prompt=True, timeout=args.timeout)
        results.setdefault("phases", {})["cold"] = {
            "completed_utc": now_iso(),
            "input_tokens": inputs["full_tokens"],
            "completion": cold,
            "summary": summarize_completion(cold),
        }
        append_jsonl(
            events_path,
            cache_event(
                phase="cold_prefill_selected",
                decision="cold_prefill",
                trace_id=run_id,
                request_id="cold-full-prompt",
                request_hash=inputs["full_prompt_hash"],
                model_id=inputs["model_id"],
                worker_id="worker-a",
                cache_key_hash=inputs["cache_key_hash"],
                manifest_id=None,
                cache_hit_level="none",
                compatibility_result="miss",
                validation_status="not_applicable",
                fallback_required=True,
                fallback_reason="no_compatible_manifest",
                latency_ms=cold["timings"].get("prompt_ms"),
                prompt_tokens=cold.get("tokens_evaluated"),
                processed_prompt_tokens=cold.get("tokens_evaluated"),
                cached_tokens=cold.get("tokens_cached") or 0,
                generated_tokens=cold.get("tokens_predicted"),
                prompt_tps=cold["timings"].get("prompt_per_second"),
                eval_tps=cold["timings"].get("predicted_per_second"),
                notes="Cold full P+Q prompt before router-store cache hit.",
            ),
        )

        slot_action(base_url, 0, "erase", {}, args.timeout)
        build = completion(base_url, inputs["prefix"], n_predict=args.build_n_predict, slot_id=0, cache_prompt=True, timeout=args.timeout)
        save = slot_action(base_url, 0, "save", {"filename": inputs["cache_filename"]}, args.timeout)
        worker_a_slot_path = f"{worker_a_slots}/{inputs['cache_filename']}"
        worker_a_slot_info = remote_file_info(args.remote_host, worker_a_slot_path, hash_file=True, timeout=args.timeout)
        save["worker_a_slot_info"] = worker_a_slot_info
        results["phases"]["build_cache"] = {
            "completed_utc": now_iso(),
            "target_tokens": args.target_tokens,
            "actual_prefix_tokens": inputs["prefix_tokens"],
            "completion": build,
            "save": save,
            "summary": summarize_completion(build),
        }

        blob_path = f"{router_store}/blobs/{worker_a_slot_info['sha256'][:2]}/{worker_a_slot_info['sha256']}.slot"
        manifest_path = f"{router_store}/manifests/{inputs['cache_key_hash']}.json"
        registry_path = f"{router_store}/registry.json"
        manifest = {
            "schema_version": "2026-07-01.1",
            "cache_key_hash": inputs["cache_key_hash"],
            "prompt_prefix_hash": inputs["prefix_hash"],
            "prompt_token_count": inputs["prefix_tokens"],
            "model_path": args.model,
            "model_file_size": preflight.get("model", {}).get("size"),
            "llama_server_path": args.llama_server,
            "llama_server_version": args.runtime_id,
            "backend": "Vulkan0",
            "ctx_size": args.ctx_size,
            "worker_source": "worker-a",
            "worker_residency": {"worker-a": True, "worker-b": False},
            "created_at": now_iso(),
            "cache_type_k": "q8_0",
            "cache_type_v": "q8_0",
            "mtp_enabled": True,
            "spec_draft_model_path": args.mtp_model,
            "spec_draft_model_size": preflight.get("mtp_model", {}).get("size"),
            "slot_save_path": worker_a_slots,
            "cache_filename": inputs["cache_filename"],
        }
        ingest = remote_ingest_blob(
            args.remote_host,
            source_path=worker_a_slot_path,
            blob_path=blob_path,
            manifest_path=manifest_path,
            registry_path=registry_path,
            manifest=manifest,
            timeout=args.timeout,
        )
        results["phases"]["router_ingest"] = {
            "completed_utc": now_iso(),
            "ingest": ingest,
            "manifest": manifest,
        }
        append_jsonl(
            events_path,
            cache_event(
                phase="cache_commit_published",
                decision="no_op",
                trace_id=run_id,
                request_id="router-ingest",
                request_hash=inputs["prefix_hash"],
                model_id=inputs["model_id"],
                worker_id="worker-a",
                cache_key_hash=inputs["cache_key_hash"],
                manifest_id=inputs["manifest_id"],
                cache_hit_level="durable_blob",
                compatibility_result="match",
                validation_status="validated",
                fallback_required=False,
                fallback_reason=None,
                latency_ms=ingest.get("wall_ms"),
                prompt_tokens=build.get("tokens_evaluated"),
                processed_prompt_tokens=build.get("tokens_evaluated"),
                cached_tokens=build.get("tokens_cached") or 0,
                generated_tokens=build.get("tokens_predicted"),
                prompt_tps=build["timings"].get("prompt_per_second"),
                eval_tps=build["timings"].get("predicted_per_second"),
                notes="Router durable blob and registry entry published after worker-a slot save.",
            ),
        )

        isolated_path = f"{isolated_slots}/{inputs['cache_filename']}"
        isolated = remote_move_file(args.remote_host, source_path=worker_a_slot_path, dest_path=isolated_path, timeout=120)
        results["phases"]["router_ingest"]["worker_a_local_isolated"] = isolated

        remote_stop_pid(args.remote_host, int(worker_a["pid"]), timeout=90)
        worker_a = None
        worker_b_argv = temp_server_argv(args, worker_id="worker-b", slot_dir=worker_b_slots)
        worker_b = remote_start_process(args.remote_host, worker_b_argv, f"{worker_b_logs}/llama-server.log")
        service_snapshot["worker_b_pid"] = worker_b["pid"]
        worker_b_ready = wait_http_ok(base_url, args.ready_timeout)
        results["worker_b"] = {"start": worker_b, "ready": worker_b_ready}
        worker_b_slot_path = f"{worker_b_slots}/{inputs['cache_filename']}"
        before_hydrate = remote_file_info(args.remote_host, worker_b_slot_path, hash_file=False, timeout=60)
        missing_restore_status, missing_restore_body, missing_restore_ms = post_json(
            base_url,
            "/slots/0?action=restore",
            {"filename": inputs["cache_filename"]},
            args.timeout,
        )
        hydrate = remote_copy_file(args.remote_host, source_path=blob_path, dest_path=worker_b_slot_path, timeout=args.timeout)
        hydrated_info = remote_file_info(args.remote_host, worker_b_slot_path, hash_file=True, timeout=args.timeout)
        restore = slot_action(base_url, 0, "restore", {"filename": inputs["cache_filename"]}, args.timeout)
        restored = completion(base_url, inputs["suffix"], n_predict=args.n_predict, slot_id=0, cache_prompt=True, timeout=args.timeout)
        results["phases"]["hydrate_restore"] = {
            "completed_utc": now_iso(),
            "before_hydration": before_hydrate,
            "missing_restore_probe": {
                "http_status": missing_restore_status,
                "body": missing_restore_body,
                "wall_ms": missing_restore_ms,
                "expected_failure": missing_restore_status >= 400,
            },
            "hydrate": hydrate,
            "hydrated_file": hydrated_info,
            "restore": restore,
            "completion": restored,
            "summary": summarize_completion(restored),
        }
        append_jsonl(
            events_path,
            cache_event(
                phase="restore_validated",
                decision="restore_then_generate",
                trace_id=run_id,
                request_id="worker-b-hydrated-suffix-route",
                request_hash=inputs["suffix_hash"],
                model_id=inputs["model_id"],
                worker_id="worker-b",
                cache_key_hash=inputs["cache_key_hash"],
                manifest_id=inputs["manifest_id"],
                cache_hit_level="durable_blob",
                compatibility_result="match",
                validation_status="validated",
                fallback_required=False,
                fallback_reason=None,
                latency_ms=restore.get("wall_ms"),
                prompt_tokens=restored.get("tokens_evaluated"),
                processed_prompt_tokens=restored.get("tokens_evaluated"),
                cached_tokens=restored.get("tokens_cached"),
                generated_tokens=restored.get("tokens_predicted"),
                prompt_tps=restored["timings"].get("prompt_per_second"),
                eval_tps=restored["timings"].get("predicted_per_second"),
                restore_latency_ms=restore.get("wall_ms"),
                notes="Worker-b suffix-only route after router-store hydration.",
            ),
        )

        if args.hot_local_restore:
            slot_action(base_url, 0, "erase", {}, args.timeout)
            hot_restore = slot_action(base_url, 0, "restore", {"filename": inputs["cache_filename"]}, args.timeout)
            hot_completion = completion(base_url, inputs["suffix"], n_predict=args.n_predict, slot_id=0, cache_prompt=True, timeout=args.timeout)
            results["phases"]["hot_local_restore"] = {
                "completed_utc": now_iso(),
                "router_copy_performed": False,
                "restore": hot_restore,
                "completion": hot_completion,
                "summary": summarize_completion(hot_completion),
            }

        update_reductions(results)
        update_status(results)
        write_json(results_path, results)
        registry_local = {
            "cache_key_hash": inputs["cache_key_hash"],
            "manifest_id": inputs["manifest_id"],
            "router_blob_path": blob_path,
            "router_blob_sha256": ingest.get("router_blob_sha256"),
            "worker_residency": {
                "worker-a": "isolated_after_ingest",
                "worker-b": "hydrated_local_copy",
            },
            "registry_path": registry_path,
            "manifest_path": manifest_path,
        }
        write_json(out_dir / "registry-entry.json", registry_local)

    finally:
        cleanup_errors: list[str] = []
        try:
            if worker_a is not None:
                remote_stop_pid(args.remote_host, int(worker_a["pid"]), timeout=90)
        except Exception as exc:  # noqa: BLE001 - cleanup report
            cleanup_errors.append(f"worker-a stop failed: {exc}")
        try:
            if worker_b is not None:
                remote_stop_pid(args.remote_host, int(worker_b["pid"]), timeout=90)
        except Exception as exc:  # noqa: BLE001 - cleanup report
            cleanup_errors.append(f"worker-b stop failed: {exc}")
        stop_tunnel(tunnel)
        try:
            if main_processes:
                still = remote_find_server_by_port(args.remote_host, 8081)
                if not still:
                    restored_main = remote_start_process(
                        args.remote_host,
                        main_processes[0]["argv"],
                        f"{run_root}/restored-8081.log",
                        timeout=60,
                    )
                    service_snapshot["restored_8081_pid"] = restored_main["pid"]
                    wait_remote_8081(args.remote_host, args.ready_timeout)
        except Exception as exc:  # noqa: BLE001 - cleanup report
            cleanup_errors.append(f"8081 restore failed: {exc}")
        try:
            final_health = ssh_script(args.remote_host, "curl -fsS http://127.0.0.1:8081/health || true", timeout=15).strip()
            final_slots = ssh_script(args.remote_host, "curl -fsS http://127.0.0.1:8081/slots || true", timeout=15).strip()
            final_unit = ssh_script(args.remote_host, "systemctl --no-pager --plain is-active llama-step37.service || true", timeout=15).strip()
            final_pids = remote_find_server_by_port(args.remote_host, 8081)
            port_listener = ssh_script(
                args.remote_host,
                f"if command -v ss >/dev/null 2>&1; then ss -ltnp 'sport = :{args.temp_port}' || true; fi",
                timeout=15,
            ).strip()
            service_snapshot.update(
                {
                    "final_health": final_health,
                    "final_slots": final_slots,
                    "final_unit_state": final_unit,
                    "final_pids": [p.get("pid") for p in final_pids],
                    "temp_port_listener": port_listener,
                    "cleanup_errors": cleanup_errors,
                }
            )
            save_service_snapshot(snapshot_path, service_snapshot)
        except Exception as exc:  # noqa: BLE001 - final diagnostics
            cleanup_errors.append(f"final snapshot failed: {exc}")
        if cleanup_errors:
            existing = read_json(results_path, {})
            existing["cleanup_errors"] = cleanup_errors
            existing.setdefault("status", "unsafe_to_continue")
            write_json(results_path, existing)
            raise RuntimeError("; ".join(cleanup_errors))

    final_results = read_json(results_path, {})
    print_summary(final_results)
    return 0 if final_results.get("status") in {"success", "partial_success"} else 1


def print_summary(results: dict[str, Any]) -> None:
    print(f"status={results.get('status')}")
    phases = results.get("phases") or {}
    print("phase,tokens_evaluated,tokens_cached,prompt_ms,total_wall_ms")
    for name in ("cold", "build_cache", "hydrate_restore", "hot_local_restore"):
        row = phases.get(name) or {}
        summary = row.get("summary") or {}
        print(",".join(str(x) for x in [name, summary.get("tokens_evaluated"), summary.get("tokens_cached"), summary.get("prompt_ms"), summary.get("wall_ms")]))
    for key, value in sorted((results.get("reductions") or {}).items()):
        print(f"{key}={value}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run an offline local copy/hash/manifest self-test and exit")
    parser.add_argument("--phase", choices=["all", "preflight", "cold-build-ingest", "hydrate-restore", "hot-local-restore"], default="all")
    parser.add_argument("--remote-host", default="")
    parser.add_argument("--temp-port", type=int, default=18082)
    parser.add_argument("--target-tokens", type=int, default=30000)
    parser.add_argument("--ctx-size", type=int, default=65536)
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--remote-cache-root", default=DEFAULT_REMOTE_CACHE_ROOT)
    parser.add_argument("--llama-server", default=DEFAULT_RUNTIME)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--mtp-model", default=DEFAULT_MTP_MODEL)
    parser.add_argument("--runtime-id", default="llama-server-9828-ebd048fc5-temp-ctx65536-mtp-router-store")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--n-predict", type=int, default=16)
    parser.add_argument("--build-n-predict", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--ready-timeout", type=float, default=900.0)
    parser.add_argument("--hot-local-restore", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    if args.self_test:
        return run_self_test()
    missing = [name for name in ["remote_host", "llama_server", "model", "mtp_model"] if not getattr(args, name)]
    if missing:
        raise SystemExit("live runs require: " + ", ".join("--" + name.replace("_", "-") for name in missing))
    if args.phase == "preflight":
        print(json.dumps(remote_preflight(args.remote_host, args.remote_cache_root, args.llama_server, args.model, args.mtp_model), indent=2, sort_keys=True))
        return 0
    if args.phase != "all":
        raise SystemExit("partial live phases are intentionally not supported yet; run --phase all")
    if not args.out_dir:
        raise SystemExit("--out-dir is required for live runs")
    return run_all(args)


if __name__ == "__main__":
    raise SystemExit(main())
