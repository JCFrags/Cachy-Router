#!/usr/bin/env python3
"""Manage and test an OpenAI-compatible cache-router stack.

This controller stages ``cache_router_daemon.py`` on the remote host, starts a
router-managed llama-server worker with ``--slot-save-path``, starts the
OpenAI-compatible router daemon, and runs the live endpoint proof through the
router port.

Worker commands manage one worker per invocation. Router commands can stage a
JSON worker inventory, so a deployment can start small and grow to any number
of compatible worker nodes.

It intentionally owns only the PID files it writes. It may stop the legacy
8081 llama-server process during the experiment window for memory headroom, but
it records the original argv and the ``stop`` command restores that process.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

from cache_router_one_node_poc import generate_prefix, sha256_text, write_json  # noqa: E402
from cache_router_store_hydration_poc import (  # noqa: E402
    DEFAULT_MODEL,
    DEFAULT_MTP_MODEL,
    DEFAULT_RUNTIME,
    http_request,
    remote_file_info,
    remote_find_server_by_port,
    remote_json,
    remote_prepare_run_dirs,
    scp_command_prefix,
    remote_start_process,
    remote_stop_pid,
    ssh_script,
    start_tunnel,
    stop_tunnel,
    temp_server_argv,
)


DEFAULT_REMOTE_CACHE_ROOT = "~/.cache/cachy-router"
DEFAULT_MODEL_NAME = "Step-3.7"
DEFAULT_ROUTER_PORT = 18080
DEFAULT_WORKER_PORT = 18082
DEFAULT_CTX_SIZE = 65536
DATE_TAG = datetime.now().strftime("%Y-%m-%d")
MUTATING_COMMANDS = {"start", "restart", "restart-router", "test", "start-worker", "stop-worker", "start-workers", "stop-workers"}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def is_loopback_bind(host: str) -> bool:
    return host in {"127.0.0.1", "localhost", "::1"}


def remote_paths(args: argparse.Namespace) -> dict[str, str]:
    root = args.remote_cache_root.rstrip("/")
    worker_id = getattr(args, "worker_id", "worker-main")
    worker_slots = (getattr(args, "worker_slot_dir_override", "") or f"{root}/workers/{worker_id}/slots").rstrip("/")
    return {
        "root": root,
        "router_store": f"{root}/router-store",
        "blobs": f"{root}/router-store/blobs",
        "manifests": f"{root}/router-store/manifests",
        "registry": f"{root}/router-store/registry.json",
        "worker_slots": worker_slots,
        "worker_logs": f"{root}/workers/{worker_id}/logs",
        "sidecar_logs": f"{root}/workers/{worker_id}/logs",
        "router_dir": f"{root}/router",
        "router_logs": f"{root}/router/logs",
        "pid_dir": f"{root}/router/pid",
        "router_pid": f"{root}/router/pid/router.pid",
        "worker_pid": f"{root}/router/pid/worker.pid",
        "sidecar_pid": f"{root}/router/pid/{worker_id}-sidecar.pid",
        "legacy": f"{root}/router/pid/legacy-8081.json",
        "auth_token": f"{root}/router/auth-token.txt",
        "daemon_remote": f"{root}/router/cache_router_daemon.py",
        "sidecar_remote": f"{root}/router/cache_router_worker_sidecar.py",
        "workers_file": f"{root}/router/workers.json",
        "isolated_slots": f"{root}/workers/{worker_id}/isolated-for-hydration",
    }


def load_worker_inventory(path: str) -> list[dict[str, Any]]:
    if not path:
        raise ValueError("--workers-file is required for inventory worker commands")
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = raw.get("workers") if isinstance(raw, dict) else raw
    if not isinstance(rows, list) or not rows:
        raise ValueError("--workers-file must contain a non-empty workers list")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("worker inventory rows must be objects")
        worker_id = str(row.get("worker_id") or "").strip()
        if not worker_id:
            raise ValueError("worker inventory row is missing worker_id")
        if worker_id in seen:
            raise ValueError(f"duplicate worker_id in worker inventory: {worker_id}")
        seen.add(worker_id)
        result.append(row)
    return result


def url_port(value: str, default: int) -> int:
    if not value:
        return default
    parsed = urllib.parse.urlparse(value)
    return parsed.port or default


def inventory_worker_args(args: argparse.Namespace, row: dict[str, Any]) -> argparse.Namespace:
    transport = row.get("transport") if isinstance(row.get("transport"), dict) else {}
    worker_url = str(row.get("worker_url") or row.get("url") or "")
    sidecar_url = str(transport.get("sidecar_url") or row.get("worker_sidecar_url") or "")
    ns = argparse.Namespace(**vars(args))
    ns.remote_host = str(row.get("ssh_host") or transport.get("ssh_host") or row.get("worker_ssh_host") or ns.remote_host)
    ns.worker_id = str(row["worker_id"])
    ns.worker_port = url_port(worker_url, ns.worker_port)
    ns.worker_transport = str(transport.get("kind") or row.get("worker_transport") or ns.worker_transport)
    ns.worker_ssh_host = str(transport.get("ssh_host") or row.get("worker_ssh_host") or ns.remote_host)
    ns.worker_sidecar_url = sidecar_url
    ns.sidecar_port = url_port(sidecar_url, ns.sidecar_port)
    ns.worker_slot_dir_override = str(row.get("slot_save_path") or row.get("worker_slot_dir") or "").rstrip("/")
    if not ns.worker_slot_dir_override:
        raise ValueError(f"worker {ns.worker_id} is missing slot_save_path")
    ns.llama_server = str(row.get("llama_server_path") or row.get("llama_server") or ns.llama_server)
    ns.model = str(row.get("model_path") or row.get("model_file") or row.get("model") or ns.model)
    ns.mtp_model = str(row.get("spec_draft_model_path") or row.get("mtp_model") or row.get("draft_model_path") or ns.mtp_model)
    if row.get("ctx_size") not in (None, ""):
        ns.ctx_size = int(row["ctx_size"])
    return ns


def remote_exists_process(host: str, pid: int) -> bool:
    script = f"kill -0 {int(pid)} >/dev/null 2>&1 && echo true || echo false"
    return ssh_script(host, script, timeout=15).strip() == "true"


def remote_read_text(host: str, path: str, *, default: str = "", timeout: float = 30.0) -> str:
    payload = json.dumps({"path": path, "default": default}, sort_keys=True)
    script = r'''
import json, os
payload = json.loads(PAYLOAD)
try:
    print(open(payload["path"], "r", encoding="utf-8").read(), end="")
except FileNotFoundError:
    print(payload["default"], end="")
'''.replace("PAYLOAD", repr(payload))
    return ssh_script(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_write_text(host: str, path: str, text: str, *, timeout: float = 30.0) -> None:
    payload = json.dumps({"path": path, "text": text}, sort_keys=True)
    script = r'''
import json, os
payload = json.loads(PAYLOAD)
os.makedirs(os.path.dirname(payload["path"]), exist_ok=True)
tmp = payload["path"] + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    fh.write(payload["text"])
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp, payload["path"])
print("{}")
'''.replace("PAYLOAD", repr(payload))
    ssh_script(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_unlink(host: str, path: str, *, timeout: float = 30.0) -> None:
    payload = json.dumps({"path": path}, sort_keys=True)
    script = r'''
import json, os
payload = json.loads(PAYLOAD)
try:
    os.unlink(payload["path"])
except FileNotFoundError:
    pass
print("{}")
'''.replace("PAYLOAD", repr(payload))
    ssh_script(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_move_if_exists(host: str, source: str, dest: str, *, timeout: float = 60.0) -> dict[str, Any]:
    payload = json.dumps({"source": source, "dest": dest}, sort_keys=True)
    script = r'''
import json, os, shutil
payload = json.loads(PAYLOAD)
source = payload["source"]
dest = payload["dest"]
before = os.path.exists(source)
os.makedirs(os.path.dirname(dest), exist_ok=True)
if before:
    if os.path.exists(dest):
        os.unlink(dest)
    shutil.move(source, dest)
print(json.dumps({
    "source": source,
    "dest": dest,
    "source_existed_before": before,
    "source_exists_after": os.path.exists(source),
    "dest_exists_after": os.path.exists(dest),
}, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=timeout)


def remote_health(host: str, port: int, *, timeout: float = 20.0) -> dict[str, Any]:
    script = f'''
python3 - <<'PY'
import json, time, urllib.error, urllib.request
url = "http://127.0.0.1:{int(port)}/health"
start = time.perf_counter()
try:
    with urllib.request.urlopen(url, timeout={float(timeout)!r}) as resp:
        raw = resp.read().decode("utf-8", "replace")
        body = json.loads(raw) if raw else {{}}
        print(json.dumps({{"ok": resp.status == 200, "status": resp.status, "body": body, "wall_ms": (time.perf_counter() - start) * 1000.0}}, sort_keys=True))
except Exception as exc:
    print(json.dumps({{"ok": False, "error": repr(exc), "wall_ms": (time.perf_counter() - start) * 1000.0}}, sort_keys=True))
PY
'''
    return remote_json(host, script, timeout=timeout + 10)


def wait_remote_health(host: str, port: int, *, timeout_s: float) -> dict[str, Any]:
    start = time.perf_counter()
    last: dict[str, Any] = {}
    while time.perf_counter() - start < timeout_s:
        last = remote_health(host, port, timeout=5.0)
        if last.get("ok") and isinstance(last.get("body"), dict) and last["body"].get("status") == "ok":
            last["elapsed_ms"] = (time.perf_counter() - start) * 1000.0
            return last
        time.sleep(1.0)
    raise RuntimeError(f"remote port {port} did not become healthy; last={last}")


def remote_pid_from_file(host: str, path: str) -> int | None:
    text = remote_read_text(host, path, default="").strip()
    if not text:
        return None
    try:
        pid = int(text)
    except ValueError:
        return None
    return pid if remote_exists_process(host, pid) else None


def remote_runtime_version(host: str, runtime: str) -> str:
    payload = json.dumps({"runtime": runtime}, sort_keys=True)
    script = r'''
import json, subprocess
payload = json.loads(PAYLOAD)
cmd = [payload["runtime"], "--version"]
try:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20, check=False)
    print(json.dumps({"version": proc.stdout.strip().splitlines()[0] if proc.stdout.strip() else "unknown", "rc": proc.returncode}, sort_keys=True))
except Exception as exc:
    print(json.dumps({"version": "unknown", "error": repr(exc)}, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    row = remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=30)
    return str(row.get("version") or "unknown")


def ensure_router_auth_token(args: argparse.Namespace) -> str:
    paths = remote_paths(args)
    payload = json.dumps({"path": paths["auth_token"]}, sort_keys=True)
    script = r'''
import json, os, secrets, stat
payload = json.loads(PAYLOAD)
path = payload["path"]
os.makedirs(os.path.dirname(path), exist_ok=True)
if not os.path.exists(path) or not open(path, "r", encoding="utf-8").read().strip():
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(secrets.token_urlsafe(32) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
    os.replace(tmp, path)
else:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
print(json.dumps({"path": path, "exists": True}, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    row = remote_json(args.remote_host, "python3 - <<'PY'\n" + script + "PY\n", timeout=30)
    return str(row["path"])


def read_router_auth_token(args: argparse.Namespace) -> str:
    if not args.router_auth:
        return ""
    return remote_read_text(args.remote_host, remote_paths(args)["auth_token"], default="", timeout=30).strip()


def remote_snapshot(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    model_info = remote_file_info(args.remote_host, args.model, hash_file=False, timeout=60)
    mtp_info = remote_file_info(args.remote_host, args.mtp_model, hash_file=False, timeout=60)
    root_parent = str(Path(paths["root"]).parent)
    payload = json.dumps({"root_parent": root_parent}, sort_keys=True)
    script = r'''
import json, os, shutil, socket, subprocess
payload = json.loads(PAYLOAD)
df = shutil.disk_usage(payload["root_parent"])
mem = {}
with open("/proc/meminfo", "r", encoding="utf-8") as fh:
    for line in fh:
        if line.startswith(("MemAvailable:", "SwapFree:")):
            key, rest = line.split(":", 1)
            mem[key] = rest.strip()
unit = subprocess.run(["systemctl", "--no-pager", "--plain", "is-active", "llama-step37.service"], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
print(json.dumps({
    "hostname": socket.gethostname(),
    "user": os.environ.get("USER"),
    "disk_free_bytes": df.free,
    "meminfo": mem,
    "llama_step37_unit": unit.stdout.strip(),
}, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    base = remote_json(args.remote_host, "python3 - <<'PY'\n" + script + "PY\n", timeout=30)
    base.update(
        {
            "runtime_path": args.llama_server,
            "runtime_version": remote_runtime_version(args.remote_host, args.llama_server),
            "model_path": args.model,
            "model": model_info,
            "mtp_model_path": args.mtp_model,
            "mtp_model": mtp_info,
            "legacy_8081_pids": remote_find_server_by_port(args.remote_host, 8081),
            "router_health": remote_health(args.remote_host, args.router_port, timeout=5.0),
            "worker_health": remote_health(args.remote_host, args.worker_port, timeout=5.0),
        }
    )
    return base


def stage_daemon(args: argparse.Namespace) -> None:
    paths = remote_paths(args)
    remote_prepare_run_dirs(args.remote_host, [paths["router_dir"], paths["router_logs"], paths["pid_dir"]])
    staged = {
        PACKAGE_ROOT / "scripts" / "cache_router_daemon.py": paths["daemon_remote"],
        PACKAGE_ROOT / "scripts" / "cache_router_transport.py": f"{paths['router_dir']}/cache_router_transport.py",
    }
    if args.workers_file:
        staged[Path(args.workers_file)] = paths["workers_file"]
    for source, dest in staged.items():
        if not source.is_file():
            raise RuntimeError(f"failed to stage {source}: file does not exist")
        proc = subprocess.run(
            scp_command_prefix()
            + [
                "-q",
                "-o",
                "BatchMode=yes",
                str(source),
                f"{args.remote_host}:{dest}",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"failed to stage {source.name}: {proc.stdout}")
    ssh_script(args.remote_host, f"chmod 700 {paths['daemon_remote']!r} {paths['router_dir'] + '/cache_router_transport.py'!r}", timeout=15)


def stage_sidecar(args: argparse.Namespace) -> None:
    paths = remote_paths(args)
    remote_prepare_run_dirs(args.remote_host, [paths["router_dir"], paths["sidecar_logs"], paths["pid_dir"]])
    source = PACKAGE_ROOT / "scripts" / "cache_router_worker_sidecar.py"
    if not source.is_file():
        raise RuntimeError(f"failed to stage {source}: file does not exist")
    proc = subprocess.run(
        scp_command_prefix()
        + [
            "-q",
            "-o",
            "BatchMode=yes",
            str(source),
            f"{args.remote_host}:{paths['sidecar_remote']}",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"failed to stage {source.name}: {proc.stdout}")
    ssh_script(args.remote_host, f"chmod 700 {paths['sidecar_remote']!r}", timeout=15)


def make_worker_argv(args: argparse.Namespace) -> list[str]:
    ns = SimpleNamespace(
        llama_server=args.llama_server,
        model=args.model,
        ctx_size=args.ctx_size,
        temp_port=args.worker_port,
        mtp_model=args.mtp_model,
        worker_bind_host=args.worker_bind_host,
    )
    return temp_server_argv(ns, worker_id=args.worker_id, slot_dir=remote_paths(args)["worker_slots"])


def make_sidecar_argv(args: argparse.Namespace) -> list[str]:
    paths = remote_paths(args)
    return [
        "python3",
        paths["sidecar_remote"],
        "--host",
        args.sidecar_bind_host,
        "--port",
        str(args.sidecar_port),
        "--worker-id",
        args.worker_id,
        "--slot-dir",
        paths["worker_slots"],
    ]


def make_router_argv(args: argparse.Namespace, snapshot: dict[str, Any]) -> list[str]:
    paths = remote_paths(args)
    argv = [
        "python3",
        paths["daemon_remote"],
        "--host",
        args.router_host,
        "--port",
        str(args.router_port),
        "--worker-url",
        f"http://127.0.0.1:{args.worker_port}",
        "--worker-id",
        args.worker_id,
        "--cache-root",
        paths["root"],
        "--worker-slot-dir",
        paths["worker_slots"],
        "--worker-transport",
        args.worker_transport,
        "--model",
        DEFAULT_MODEL_NAME,
        "--model-path",
        args.model,
        "--model-file-size",
        str((snapshot.get("model") or {}).get("size_bytes") or (snapshot.get("model") or {}).get("size") or 0),
        "--llama-server-path",
        args.llama_server,
        "--llama-server-version",
        str(snapshot.get("runtime_version") or "unknown"),
        "--ctx-size",
        str(args.ctx_size),
        "--cache-type-k",
        "q8_0",
        "--cache-type-v",
        "q8_0",
        "--mtp-enabled",
        "--spec-draft-model-path",
        args.mtp_model,
        "--spec-draft-model-size",
        str((snapshot.get("mtp_model") or {}).get("size_bytes") or (snapshot.get("mtp_model") or {}).get("size") or 0),
        "--slot-id",
        "0",
        "--timeout",
        str(args.timeout),
    ]
    if args.router_auth:
        argv.extend(["--auth-token-file", paths["auth_token"]])
    if args.workers_file:
        argv.extend(["--workers-file", paths["workers_file"]])
    if args.worker_ssh_host:
        argv.extend(["--worker-ssh-host", args.worker_ssh_host])
    if args.worker_sidecar_url:
        argv.extend(["--worker-sidecar-url", args.worker_sidecar_url])
    if args.ssh_config:
        argv.extend(["--ssh-config", args.ssh_config])
    if args.ssh_extra_args:
        argv.extend(["--ssh-extra-args", args.ssh_extra_args])
    if args.scp_extra_args:
        argv.extend(["--scp-extra-args", args.scp_extra_args])
    return argv


def save_pid(host: str, path: str, pid: int) -> None:
    remote_write_text(host, path, f"{int(pid)}\n", timeout=20)


def stop_owned_pid(host: str, pid_path: str, *, timeout: float = 90.0) -> dict[str, Any]:
    pid = remote_pid_from_file(host, pid_path)
    if pid is None:
        remote_unlink(host, pid_path)
        return {"pid_path": pid_path, "stopped": True, "already_absent": True}
    result = remote_stop_pid(host, pid, timeout=timeout)
    remote_unlink(host, pid_path)
    return result


def start_worker(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    remote_prepare_run_dirs(args.remote_host, [paths["worker_slots"], paths["worker_logs"], paths["pid_dir"]])
    worker = remote_start_process(
        args.remote_host,
        make_worker_argv(args),
        f"{paths['worker_logs']}/llama-server.log",
        timeout=60,
    )
    save_pid(args.remote_host, paths["worker_pid"], int(worker["pid"]))
    ready = wait_remote_health(args.remote_host, args.worker_port, timeout_s=args.ready_timeout)
    return {"start": worker, "ready": ready}


def start_sidecar(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    stage_sidecar(args)
    remote_prepare_run_dirs(args.remote_host, [paths["worker_slots"], paths["sidecar_logs"], paths["pid_dir"]])
    existing = remote_pid_from_file(args.remote_host, paths["sidecar_pid"])
    if existing:
        remote_stop_pid(args.remote_host, existing, timeout=30)
        remote_unlink(args.remote_host, paths["sidecar_pid"])
    port_pids = remote_find_server_by_port(args.remote_host, args.sidecar_port)
    if port_pids:
        raise RuntimeError(f"sidecar port {args.sidecar_port} is already in use on {args.remote_host}: {port_pids}")
    sidecar = remote_start_process(
        args.remote_host,
        make_sidecar_argv(args),
        f"{paths['sidecar_logs']}/cache-router-worker-sidecar.log",
        timeout=30,
    )
    save_pid(args.remote_host, paths["sidecar_pid"], int(sidecar["pid"]))
    ready = wait_remote_health(args.remote_host, args.sidecar_port, timeout_s=30)
    return {"start": sidecar, "ready": ready}


def worker_only_status(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    worker_pid = remote_pid_from_file(args.remote_host, paths["worker_pid"])
    sidecar_pid = remote_pid_from_file(args.remote_host, paths["sidecar_pid"])
    return {
        "remote_host": args.remote_host,
        "worker_id": args.worker_id,
        "worker_bind_host": args.worker_bind_host,
        "worker_port": args.worker_port,
        "worker_pid": worker_pid,
        "sidecar_bind_host": args.sidecar_bind_host,
        "sidecar_port": args.sidecar_port,
        "sidecar_pid": sidecar_pid,
        "slot_save_path": paths["worker_slots"],
        "health": remote_health(args.remote_host, args.worker_port, timeout=5.0),
        "sidecar_health": remote_health(args.remote_host, args.sidecar_port, timeout=5.0),
        "processes_on_port": remote_find_server_by_port(args.remote_host, args.worker_port),
        "sidecar_processes_on_port": remote_find_server_by_port(args.remote_host, args.sidecar_port),
        "log": f"{paths['worker_logs']}/llama-server.log",
        "sidecar_log": f"{paths['sidecar_logs']}/cache-router-worker-sidecar.log",
    }


def start_worker_only(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    existing_owned = remote_pid_from_file(args.remote_host, paths["worker_pid"])
    if existing_owned:
        remote_stop_pid(args.remote_host, existing_owned, timeout=90)
        remote_unlink(args.remote_host, paths["worker_pid"])
    port_pids = remote_find_server_by_port(args.remote_host, args.worker_port)
    if port_pids:
        raise RuntimeError(f"worker port {args.worker_port} is already in use on {args.remote_host}: {port_pids}")
    model_info = remote_file_info(args.remote_host, args.model, hash_file=False, timeout=60)
    mtp_info = remote_file_info(args.remote_host, args.mtp_model, hash_file=False, timeout=60)
    if not model_info.get("exists"):
        raise RuntimeError(f"model path missing on {args.remote_host}: {args.model}")
    if not mtp_info.get("exists"):
        raise RuntimeError(f"MTP model path missing on {args.remote_host}: {args.mtp_model}")
    worker = start_worker(args)
    sidecar = start_sidecar(args) if args.start_sidecar else {"skipped": True}
    status = worker_only_status(args)
    return {"model": model_info, "mtp_model": mtp_info, "worker": worker, "sidecar": sidecar, "status": status}


def stop_worker_only(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    result = {
        "worker": stop_owned_pid(args.remote_host, paths["worker_pid"], timeout=90),
        "sidecar": stop_owned_pid(args.remote_host, paths["sidecar_pid"], timeout=30),
    }
    result["status"] = worker_only_status(args)
    return result


def workers_status(args: argparse.Namespace) -> dict[str, Any]:
    rows = load_worker_inventory(args.workers_file)
    results = []
    for row in rows:
        worker_args = inventory_worker_args(args, row)
        try:
            status = worker_only_status(worker_args)
            results.append({"worker_id": worker_args.worker_id, "remote_host": worker_args.remote_host, "ok": True, "status": status})
        except Exception as exc:  # noqa: BLE001
            results.append({"worker_id": str(row.get("worker_id")), "remote_host": str(row.get("ssh_host") or ""), "ok": False, "error": repr(exc)})
    return {
        "workers_file": args.workers_file,
        "count": len(results),
        "healthy": sum(1 for row in results if row.get("status", {}).get("health", {}).get("ok")),
        "sidecars_healthy": sum(1 for row in results if row.get("status", {}).get("sidecar_health", {}).get("ok")),
        "workers": results,
    }


def start_workers(args: argparse.Namespace) -> dict[str, Any]:
    rows = load_worker_inventory(args.workers_file)
    results = []
    for row in rows:
        worker_args = inventory_worker_args(args, row)
        results.append({"worker_id": worker_args.worker_id, "remote_host": worker_args.remote_host, "result": start_worker_only(worker_args)})
    return {"workers_file": args.workers_file, "count": len(results), "workers": results, "status": workers_status(args)}


def stop_workers(args: argparse.Namespace) -> dict[str, Any]:
    rows = load_worker_inventory(args.workers_file)
    results = []
    for row in rows:
        worker_args = inventory_worker_args(args, row)
        results.append({"worker_id": worker_args.worker_id, "remote_host": worker_args.remote_host, "result": stop_worker_only(worker_args)})
    return {"workers_file": args.workers_file, "count": len(results), "workers": results, "status": workers_status(args)}


def start_router(args: argparse.Namespace, snapshot: dict[str, Any]) -> dict[str, Any]:
    paths = remote_paths(args)
    if args.router_auth:
        ensure_router_auth_token(args)
    router = remote_start_process(
        args.remote_host,
        make_router_argv(args, snapshot),
        f"{paths['router_logs']}/cache-router-daemon.log",
        timeout=60,
    )
    save_pid(args.remote_host, paths["router_pid"], int(router["pid"]))
    ready = wait_remote_health(args.remote_host, args.router_port, timeout_s=args.ready_timeout)
    return {"start": router, "ready": ready}


def start_stack(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    snapshot = remote_snapshot(args)
    remote_prepare_run_dirs(
        args.remote_host,
        [paths["blobs"], paths["manifests"], paths["worker_slots"], paths["worker_logs"], paths["router_logs"], paths["pid_dir"], paths["isolated_slots"]],
    )
    stage_daemon(args)
    legacy = {
        "captured_at": now_iso(),
        "unit_state": snapshot.get("llama_step37_unit"),
        "pids": snapshot.get("legacy_8081_pids") or [],
    }
    if legacy["pids"]:
        remote_write_text(args.remote_host, paths["legacy"], json_dumps(legacy), timeout=20)
    if legacy["pids"] and args.stop_legacy_8081:
        for proc in legacy["pids"]:
            remote_stop_pid(args.remote_host, int(proc["pid"]), timeout=90)

    existing_router = remote_pid_from_file(args.remote_host, paths["router_pid"])
    existing_worker = remote_pid_from_file(args.remote_host, paths["worker_pid"])
    if existing_router:
        remote_stop_pid(args.remote_host, existing_router, timeout=60)
        remote_unlink(args.remote_host, paths["router_pid"])
    if existing_worker:
        remote_stop_pid(args.remote_host, existing_worker, timeout=90)
        remote_unlink(args.remote_host, paths["worker_pid"])

    worker = start_worker(args)
    sidecar = start_sidecar(args) if args.start_sidecar and args.worker_transport == "http" else {"skipped": True}
    router = start_router(args, snapshot)
    return {"snapshot": snapshot, "legacy": legacy, "worker": worker, "sidecar": sidecar, "router": router, "status": status_stack(args)}


def restore_legacy_if_needed(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    legacy_text = remote_read_text(args.remote_host, paths["legacy"], default="")
    if not legacy_text.strip():
        return {"restored": False, "reason": "no legacy record"}
    try:
        legacy = json.loads(legacy_text)
    except json.JSONDecodeError:
        return {"restored": False, "reason": "legacy record invalid"}
    current = remote_find_server_by_port(args.remote_host, 8081)
    if current:
        return {"restored": False, "reason": "8081 already running", "pids": current}
    pids = legacy.get("pids") or []
    if not pids:
        return {"restored": False, "reason": "legacy record has no pids"}
    restored = remote_start_process(
        args.remote_host,
        pids[0]["argv"],
        f"{paths['router_logs']}/restored-8081.log",
        timeout=60,
    )
    ready = wait_remote_health(args.remote_host, 8081, timeout_s=args.ready_timeout)
    return {"restored": True, "start": restored, "ready": ready}


def stop_stack(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    result = {
        "router": stop_owned_pid(args.remote_host, paths["router_pid"], timeout=60),
        "worker": stop_owned_pid(args.remote_host, paths["worker_pid"], timeout=90),
        "sidecar": stop_owned_pid(args.remote_host, paths["sidecar_pid"], timeout=30),
    }
    if args.restore_legacy_8081:
        result["legacy_restore"] = restore_legacy_if_needed(args)
    result["status"] = status_stack(args)
    return result


def restart_router_only(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    snapshot = remote_snapshot(args)
    stage_daemon(args)
    existing_router = remote_pid_from_file(args.remote_host, paths["router_pid"])
    stopped: dict[str, Any]
    if existing_router:
        stopped = remote_stop_pid(args.remote_host, existing_router, timeout=60)
        remote_unlink(args.remote_host, paths["router_pid"])
    else:
        stopped = {"already_absent": True}
    router = start_router(args, snapshot)
    return {"snapshot": snapshot, "stopped_router": stopped, "router": router, "status": status_stack(args)}


def status_stack(args: argparse.Namespace) -> dict[str, Any]:
    paths = remote_paths(args)
    router_pid = remote_pid_from_file(args.remote_host, paths["router_pid"])
    worker_pid = remote_pid_from_file(args.remote_host, paths["worker_pid"])
    return {
        "remote_host": args.remote_host,
        "cache_root": paths["root"],
        "router": {
            "bind": args.router_host,
            "port": args.router_port,
            "pid": router_pid,
            "health": remote_health(args.remote_host, args.router_port, timeout=5.0),
            "log": f"{paths['router_logs']}/cache-router-daemon.log",
        },
        "worker": {
            "port": args.worker_port,
            "pid": worker_pid,
            "health": remote_health(args.remote_host, args.worker_port, timeout=5.0),
            "slot_save_path": paths["worker_slots"],
            "log": f"{paths['worker_logs']}/llama-server.log",
        },
        "sidecar": {
            "bind": args.sidecar_bind_host,
            "port": args.sidecar_port,
            "pid": remote_pid_from_file(args.remote_host, paths["sidecar_pid"]),
            "health": remote_health(args.remote_host, args.sidecar_port, timeout=5.0),
            "log": f"{paths['sidecar_logs']}/cache-router-worker-sidecar.log",
        },
        "legacy_8081": {
            "pids": remote_find_server_by_port(args.remote_host, 8081),
            "health": remote_health(args.remote_host, 8081, timeout=5.0),
            "record_path": paths["legacy"],
        },
    }


def restart_worker_for_local_miss(args: argparse.Namespace, slot_filename: str) -> dict[str, Any]:
    paths = remote_paths(args)
    old_pid = remote_pid_from_file(args.remote_host, paths["worker_pid"])
    if old_pid is None:
        raise RuntimeError("cannot restart worker: owned worker pid is absent")
    stop = remote_stop_pid(args.remote_host, old_pid, timeout=90)
    remote_unlink(args.remote_host, paths["worker_pid"])
    worker_slot_path = f"{paths['worker_slots']}/{slot_filename}"
    isolated_path = f"{paths['isolated_slots']}/{int(time.time())}-{slot_filename}"
    isolate = remote_move_if_exists(args.remote_host, worker_slot_path, isolated_path, timeout=120)
    before = remote_file_info(args.remote_host, worker_slot_path, hash_file=False, timeout=60)
    worker = start_worker(args)
    new_pid = worker["start"]["pid"]
    return {
        "old_worker_pid": old_pid,
        "new_worker_pid": new_pid,
        "old_new_pid_differ": old_pid != new_pid,
        "stop": stop,
        "isolate": isolate,
        "worker_slot_before_hydration": before,
        "restart": worker,
    }


def local_http_request(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: float,
    auth_token: str = "",
) -> tuple[int, Any, float]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept-Encoding": "identity"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
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


def router_post(base_url: str, path: str, payload: dict[str, Any], timeout: float, *, auth_token: str = "") -> tuple[int, Any, float]:
    status, body, wall_ms = local_http_request("POST", base_url.rstrip("/") + path, payload=payload, timeout=timeout, auth_token=auth_token)
    if status >= 400:
        raise RuntimeError(f"{path} failed HTTP {status}: {body}")
    return status, body, wall_ms


def router_get(base_url: str, path: str, timeout: float, *, auth_token: str = "") -> tuple[int, Any, float]:
    status, body, wall_ms = local_http_request("GET", base_url.rstrip("/") + path, timeout=timeout, auth_token=auth_token)
    if status >= 400:
        raise RuntimeError(f"{path} failed HTTP {status}: {body}")
    return status, body, wall_ms


def write_service_snapshot(path: Path, args: argparse.Namespace, status: dict[str, Any], snapshot: dict[str, Any]) -> None:
    lines = [
        "OpenAI cache-router endpoint service snapshot",
        f"Captured UTC: {now_iso()}",
        "",
        f"Remote host: {args.remote_host}",
        "No sudo was used.",
        "",
        "Router stack:",
        f"- router bind: {args.router_host}:{args.router_port}",
        f"- worker bind: 127.0.0.1:{args.worker_port}",
        f"- cache root: {args.remote_cache_root}",
        f"- API key required: {'yes' if args.router_auth else 'no'}",
        f"- router PID: {status.get('router', {}).get('pid')}",
        f"- worker PID: {status.get('worker', {}).get('pid')}",
        f"- worker slot-save-path: {status.get('worker', {}).get('slot_save_path')}",
        "",
        "Legacy 8081 state:",
        f"- unit state at start: {snapshot.get('llama_step37_unit')}",
        f"- captured legacy pids at start: {[p.get('pid') for p in snapshot.get('legacy_8081_pids', [])]}",
        f"- final legacy pids: {[p.get('pid') for p in status.get('legacy_8081', {}).get('pids', [])]}",
        f"- final legacy health ok: {status.get('legacy_8081', {}).get('health', {}).get('ok')}",
        "",
        "Client access:",
        f"- local tunnel option: ssh -N -L {args.router_port}:127.0.0.1:{args.router_port} {args.remote_host}",
        f"- OpenAI base URL via tunnel: http://127.0.0.1:{args.router_port}/v1",
        f"- LAN base URL if bound to 0.0.0.0: http://<router-lan-ip>:{args.router_port}/v1",
        "- model: Step-3.7",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_curl_examples(path: Path, args: argparse.Namespace) -> None:
    text = f"""# Cache Router Endpoint Curl Examples

Tunnel from the local workstation:

```bash
ssh -N -L {args.router_port}:127.0.0.1:{args.router_port} {args.remote_host}
```

Health:

```bash
curl -fsS http://127.0.0.1:{args.router_port}/health | python3 -m json.tool
```

LAN mode is intentionally no-key for trusted home-network testing.

OpenAI chat pass-through:

```bash
curl -fsS http://127.0.0.1:{args.router_port}/v1/chat/completions \\
  -H 'Content-Type: application/json' \\
  -d '{{"model":"Step-3.7","messages":[{{"role":"user","content":"Reply with exactly: router chat ok"}}],"max_tokens":8,"temperature":0}}'
```

OpenAI completion pass-through:

```bash
curl -fsS http://127.0.0.1:{args.router_port}/v1/completions \\
  -H 'Content-Type: application/json' \\
  -d '{{"model":"Step-3.7","prompt":"Reply with exactly: router completion ok\\nAnswer:","max_tokens":8,"temperature":0}}'
```

Cached suffix route uses the nonstandard `cache_router` extension. The router
removes that field before calling llama.cpp. Large `prefix_text` is omitted here
because tracked docs should not contain the full synthetic prompt.
"""
    path.write_text(text, encoding="utf-8")


def summarize_results_markdown(results: dict[str, Any]) -> str:
    tests = results.get("tests", {})
    build = tests.get("cache_build", {}).get("response", {}).get("cache_router", {}).get("build", {})
    restart = tests.get("worker_restart_local_miss", {})
    use = tests.get("cached_use", {}).get("response", {}).get("cache_router", {}).get("use", {})
    hot = tests.get("hot_local_second_use", {}).get("response", {}).get("cache_router", {}).get("use", {})
    reductions = results.get("reductions", {})
    lines = [
        "# OpenAI Cache Router Endpoint POC",
        "",
        f"Captured UTC: `{results.get('created_utc')}`",
        "",
        "Status: `success` if the router endpoint accepted OpenAI-compatible requests, built a durable cache blob, restarted the worker with no local slot, hydrated from the router store, restored the slot, and reduced suffix-route prompt processing by at least 90%.",
        "",
        f"Conclusion: `{results.get('status')}`",
        "",
        "## Endpoint",
        "",
        f"- Remote router bind: `{results.get('router_host')}:{results.get('router_port')}`",
        f"- Worker bind: `127.0.0.1:{results.get('worker_port')}`",
        f"- Tunnel: `ssh -N -L {results.get('router_port')}:127.0.0.1:{results.get('router_port')} {results.get('remote_host')}`",
        f"- OpenAI base URL: `http://127.0.0.1:{results.get('router_port')}/v1`",
        "- Model: `Step-3.7`",
        "",
        "## Cache Build Through Router",
        "",
        f"- Prefix tokens: `{build.get('prefix_tokens')}`",
        f"- Build prompt ms: `{build.get('build_prompt_ms')}`",
        f"- Save ms: `{build.get('save_ms')}`",
        f"- Save wall ms: `{build.get('save_wall_ms')}`",
        f"- Ingest ms: `{build.get('ingest_ms')}`",
        f"- Blob SHA256: `{build.get('slot_file_sha256')}`",
        f"- Blob size bytes: `{build.get('slot_file_size_bytes')}`",
        "",
        "## Worker Restart And Hydration",
        "",
        f"- Old worker PID: `{restart.get('old_worker_pid')}`",
        f"- New worker PID: `{restart.get('new_worker_pid')}`",
        f"- PID changed: `{restart.get('old_new_pid_differ')}`",
        f"- Worker slot absent before hydration: `{not restart.get('worker_slot_before_hydration', {}).get('exists', True)}`",
        f"- Hydration performed: `{use.get('hydrate', {}).get('performed')}`",
        f"- Hydration SHA256 match: `{use.get('hydrate', {}).get('sha256_match')}`",
        "",
        "## Cached Use Through Router",
        "",
        f"- n_restored: `{use.get('restore', {}).get('n_restored')}`",
        f"- Restore ms: `{use.get('restore', {}).get('wall_ms')}`",
        f"- Suffix prompt tokens: `{use.get('completion', {}).get('tokens_evaluated')}`",
        f"- Suffix prompt ms: `{use.get('completion', {}).get('timings', {}).get('prompt_ms')}`",
        f"- Total request ms: `{tests.get('cached_use', {}).get('wall_ms')}`",
        f"- Response preview: `{tests.get('cached_use', {}).get('text_preview')}`",
        "",
        "## Hot Local Second Use",
        "",
        f"- Hydration performed: `{hot.get('hydrate', {}).get('performed')}`",
        f"- Restore ms: `{hot.get('restore', {}).get('wall_ms')}`",
        f"- Total request ms: `{tests.get('hot_local_second_use', {}).get('wall_ms')}`",
        "",
        "## Reduction",
        "",
        f"- Prompt-only reduction percent: `{reductions.get('prompt_only_reduction_percent')}`",
        f"- Restore-inclusive reduction percent: `{reductions.get('restore_inclusive_reduction_percent')}`",
        f"- Hydrate+restore-inclusive reduction percent: `{reductions.get('hydrate_restore_inclusive_reduction_percent')}`",
        f"- Target reached: `{reductions.get('target_reached')}`",
        "",
        "## Interpretation",
        "",
        "This is a one-PC endpoint proof. It does not claim distributed or two-node behavior. The accelerated path is explicit suffix routing after restoring a saved prefix slot; full-prompt replay is not treated as a cache hit.",
    ]
    return "\n".join(lines) + "\n"


def update_reductions(results: dict[str, Any]) -> None:
    build = results["tests"]["cache_build"]["response"]["cache_router"]["build"]
    use = results["tests"]["cached_use"]["response"]["cache_router"]["use"]
    cold_ms = build.get("build_prompt_ms")
    suffix_ms = use.get("completion", {}).get("timings", {}).get("prompt_ms")
    restore_ms = use.get("restore", {}).get("wall_ms")
    hydrate_ms = use.get("hydrate", {}).get("wall_ms")
    reductions: dict[str, Any] = {
        "baseline_prompt_ms": cold_ms,
        "cached_suffix_prompt_ms": suffix_ms,
        "restore_ms": restore_ms,
        "hydrate_ms": hydrate_ms,
        "target_percent": 90.0,
    }
    if isinstance(cold_ms, (int, float)) and cold_ms > 0 and isinstance(suffix_ms, (int, float)):
        reductions["prompt_only_reduction_percent"] = 100.0 * (1.0 - suffix_ms / cold_ms)
        if isinstance(restore_ms, (int, float)):
            reductions["restore_inclusive_reduction_percent"] = 100.0 * (1.0 - (restore_ms + suffix_ms) / cold_ms)
            if isinstance(hydrate_ms, (int, float)):
                reductions["hydrate_restore_inclusive_reduction_percent"] = 100.0 * (
                    1.0 - (hydrate_ms + restore_ms + suffix_ms) / cold_ms
                )
        reductions["target_reached"] = reductions.get("prompt_only_reduction_percent", 0.0) >= 90.0
    else:
        reductions["target_reached"] = False
    results["reductions"] = reductions


def run_endpoint_test(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir or PACKAGE_ROOT / "data" / "cache_router_poc" / f"{DATE_TAG}-openai-router-endpoint")
    out_dir.mkdir(parents=True, exist_ok=True)
    start_result = start_stack(args)
    paths = remote_paths(args)
    status_before = status_stack(args)
    router_tunnel = None
    worker_tunnel = None
    results: dict[str, Any] = {
        "schema_version": "2026-07-01.1",
        "created_utc": now_iso(),
        "remote_host": args.remote_host,
        "router_host": args.router_host,
        "router_port": args.router_port,
        "worker_port": args.worker_port,
        "remote_cache_root": args.remote_cache_root,
        "start": start_result,
        "status_before_tests": status_before,
        "tests": {},
    }
    try:
        router_tunnel = start_tunnel(args.remote_host, args.local_router_port, args.router_port)
        worker_tunnel = start_tunnel(args.remote_host, args.local_worker_port, args.worker_port)
        router_base = f"http://127.0.0.1:{args.local_router_port}"
        worker_base = f"http://127.0.0.1:{args.local_worker_port}"
        auth_token = read_router_auth_token(args)

        for name, path in [("health", "/health"), ("models", "/v1/models"), ("router_status", "/router/status")]:
            _, body, wall_ms = router_get(router_base, path, args.timeout, auth_token=auth_token)
            results["tests"][name] = {"response": body, "wall_ms": wall_ms}

        chat_payload = {
            "model": DEFAULT_MODEL_NAME,
            "messages": [{"role": "user", "content": "Reply with exactly: router chat ok"}],
            "max_tokens": 8,
            "temperature": 0,
        }
        _, chat_body, chat_ms = router_post(router_base, "/v1/chat/completions", chat_payload, args.timeout, auth_token=auth_token)
        results["tests"]["chat_passthrough"] = {"response": chat_body, "wall_ms": chat_ms}

        completion_payload = {
            "model": DEFAULT_MODEL_NAME,
            "prompt": "Reply with exactly: router completion ok\nAnswer:",
            "max_tokens": 8,
            "temperature": 0,
        }
        _, completion_body, completion_ms = router_post(router_base, "/v1/completions", completion_payload, args.timeout, auth_token=auth_token)
        results["tests"]["completion_passthrough"] = {"response": completion_body, "wall_ms": completion_ms}

        prefix, prefix_tokens, repeats = generate_prefix(worker_base, args.target_tokens, args.timeout)
        suffix = "\n\nRouter endpoint cache query: Answer with exactly: router cache ok\nAnswer:"
        cache_id = args.cache_id
        results["prompt"] = {
            "prefix_hash": sha256_text(prefix),
            "prefix_tokens": prefix_tokens,
            "prefix_chars": len(prefix),
            "prefix_repeats": repeats,
            "suffix_hash": sha256_text(suffix),
            "raw_prompt_tracked": False,
        }

        build_payload = {
            "model": DEFAULT_MODEL_NAME,
            "prompt": "",
            "max_tokens": 1,
            "temperature": 0,
            "cache_router": {
                "mode": "refresh",
                "cache_id": cache_id,
                "prefix_text": prefix,
                "suffix_text": "",
                "target": "suffix_route",
            },
        }
        _, build_body, build_ms = router_post(router_base, "/v1/completions", build_payload, args.timeout, auth_token=auth_token)
        results["tests"]["cache_build"] = {"response": build_body, "wall_ms": build_ms}
        build_meta = build_body["cache_router"]["build"]
        slot_filename = build_meta["slot_filename"]

        restart = restart_worker_for_local_miss(args, slot_filename)
        results["tests"]["worker_restart_local_miss"] = restart

        use_payload = {
            "model": DEFAULT_MODEL_NAME,
            "prompt": suffix,
            "max_tokens": 16,
            "temperature": 0,
            "cache_router": {
                "mode": "use",
                "cache_id": cache_id,
                "suffix_text": suffix,
                "target": "suffix_route",
            },
        }
        _, use_body, use_ms = router_post(router_base, "/v1/completions", use_payload, args.timeout, auth_token=auth_token)
        results["tests"]["cached_use"] = {
            "response": use_body,
            "wall_ms": use_ms,
            "text_preview": (use_body.get("choices") or [{}])[0].get("text", "")[:200],
        }

        _, hot_body, hot_ms = router_post(router_base, "/v1/completions", use_payload, args.timeout, auth_token=auth_token)
        results["tests"]["hot_local_second_use"] = {
            "response": hot_body,
            "wall_ms": hot_ms,
            "text_preview": (hot_body.get("choices") or [{}])[0].get("text", "")[:200],
        }

        update_reductions(results)
        use_meta = use_body["cache_router"]["use"]
        restart_meta = results["tests"]["worker_restart_local_miss"]
        target_ok = bool(results["reductions"].get("target_reached"))
        mechanics_ok = (
            use_meta.get("hydrate", {}).get("performed") is True
            and use_meta.get("hydrate", {}).get("sha256_match") is True
            and restart_meta.get("old_new_pid_differ") is True
            and not restart_meta.get("worker_slot_before_hydration", {}).get("exists", True)
            and isinstance(use_meta.get("restore", {}).get("n_restored"), int)
            and use_meta["restore"]["n_restored"] > 0
        )
        results["status"] = "success" if mechanics_ok and target_ok else "partial_success" if mechanics_ok else "diagnostic_failure"
        results["status_after_tests"] = status_stack(args)

        remote_events = remote_read_text(args.remote_host, f"{paths['router_logs']}/cache-router-events.jsonl", default="", timeout=30)
        (out_dir / "cache-router-events.jsonl").write_text(remote_events, encoding="utf-8")
        write_json(out_dir / "results.json", results)
        (out_dir / "README.md").write_text(summarize_results_markdown(results), encoding="utf-8")
        write_curl_examples(out_dir / "curl-examples.md", args)
        write_service_snapshot(out_dir / "service-snapshot-redacted.txt", args, results["status_after_tests"], start_result["snapshot"])
        print(json_dumps({"status": results["status"], "out_dir": str(out_dir), "reductions": results["reductions"], "stack": results["status_after_tests"]}))
        return results
    finally:
        stop_tunnel(worker_tunnel)
        stop_tunnel(router_tunnel)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--remote-host", default="", help="SSH host for this live stack command.")
        p.add_argument(
            "--ssh-config",
            default=os.environ.get("CACHE_ROUTER_SSH_CONFIG", ""),
            help="Optional SSH config file for controller-side SSH/SCP calls, for example /dev/null to bypass a broken system include.",
        )
        p.add_argument(
            "--ssh-extra-args",
            default=os.environ.get("CACHE_ROUTER_SSH_EXTRA_ARGS", ""),
            help="Optional extra SSH args parsed with shlex for controller-side SSH calls.",
        )
        p.add_argument(
            "--scp-extra-args",
            default=os.environ.get("CACHE_ROUTER_SCP_EXTRA_ARGS", ""),
            help="Optional extra SCP args parsed with shlex for daemon staging.",
        )
        p.add_argument("--remote-cache-root", default=DEFAULT_REMOTE_CACHE_ROOT)
        p.add_argument("--router-host", default="127.0.0.1")
        p.add_argument("--router-auth", action=argparse.BooleanOptionalAction, default=None)
        p.add_argument(
            "--allow-unauthenticated-lan",
            action="store_true",
            help="Required with --router-host 0.0.0.0 and --no-router-auth. Use only on a trusted private LAN.",
        )
        p.add_argument("--router-port", type=int, default=DEFAULT_ROUTER_PORT)
        p.add_argument("--worker-port", type=int, default=DEFAULT_WORKER_PORT)
        p.add_argument("--worker-id", default="worker-main")
        p.add_argument("--workers-file", default="", help="Optional local JSON worker inventory to stage for the router daemon.")
        p.add_argument("--worker-bind-host", default="127.0.0.1", help="Bind address for router-managed llama-server workers started by this tool.")
        p.add_argument("--worker-transport", choices=["local", "ssh", "http"], default="local")
        p.add_argument("--worker-ssh-host", default="")
        p.add_argument("--worker-sidecar-url", default="", help="Sidecar URL for --worker-transport http.")
        p.add_argument("--sidecar-bind-host", default="127.0.0.1", help="Bind address for the worker-local slot sidecar.")
        p.add_argument("--sidecar-port", type=int, default=18083)
        p.add_argument("--start-sidecar", action=argparse.BooleanOptionalAction, default=True)
        p.add_argument("--llama-server", default=DEFAULT_RUNTIME)
        p.add_argument("--model", default=DEFAULT_MODEL)
        p.add_argument("--mtp-model", default=DEFAULT_MTP_MODEL)
        p.add_argument("--ctx-size", type=int, default=DEFAULT_CTX_SIZE)
        p.add_argument("--timeout", type=float, default=900.0)
        p.add_argument("--ready-timeout", type=float, default=900.0)

    p_status = sub.add_parser("status", help="show remote router stack status")
    add_common(p_status)

    p_start = sub.add_parser("start", help="start worker and router")
    add_common(p_start)
    p_start.add_argument("--stop-legacy-8081", action=argparse.BooleanOptionalAction, default=True)

    p_stop = sub.add_parser("stop", help="stop owned worker/router and restore recorded legacy 8081 process")
    add_common(p_stop)
    p_stop.add_argument("--restore-legacy-8081", action=argparse.BooleanOptionalAction, default=True)

    p_restart = sub.add_parser("restart", help="restart worker/router stack")
    add_common(p_restart)
    p_restart.add_argument("--stop-legacy-8081", action=argparse.BooleanOptionalAction, default=True)
    p_restart.add_argument("--restore-legacy-8081", action=argparse.BooleanOptionalAction, default=False)

    p_restart_router = sub.add_parser("restart-router", help="stage and restart only the cache-router daemon, leaving the worker loaded")
    add_common(p_restart_router)
    p_restart_router.add_argument("--stop-legacy-8081", action=argparse.BooleanOptionalAction, default=False)
    p_restart_router.add_argument("--restore-legacy-8081", action=argparse.BooleanOptionalAction, default=False)

    p_test = sub.add_parser("test", help="start stack and run OpenAI endpoint POC")
    add_common(p_test)
    p_test.add_argument("--stop-legacy-8081", action=argparse.BooleanOptionalAction, default=True)
    p_test.add_argument("--target-tokens", type=int, default=30000)
    p_test.add_argument("--cache-id", default="demo-30k-prefix")
    p_test.add_argument("--out-dir", default="")
    p_test.add_argument("--local-router-port", type=int, default=DEFAULT_ROUTER_PORT)
    p_test.add_argument("--local-worker-port", type=int, default=DEFAULT_WORKER_PORT)

    p_worker_status = sub.add_parser("worker-status", help="show status for one router-managed worker on --remote-host")
    add_common(p_worker_status)

    p_start_worker = sub.add_parser("start-worker", help="start only one router-managed worker on --remote-host")
    add_common(p_start_worker)

    p_stop_worker = sub.add_parser("stop-worker", help="stop only the owned router-managed worker on --remote-host")
    add_common(p_stop_worker)

    p_workers_status = sub.add_parser("workers-status", help="show status for every worker in --workers-file")
    add_common(p_workers_status)

    p_start_workers = sub.add_parser("start-workers", help="start every router-managed worker in --workers-file")
    add_common(p_start_workers)

    p_stop_workers = sub.add_parser("stop-workers", help="stop every owned router-managed worker in --workers-file")
    add_common(p_stop_workers)

    args = parser.parse_args()
    if args.ssh_config:
        os.environ["CACHE_ROUTER_SSH_CONFIG"] = args.ssh_config
    if args.ssh_extra_args:
        os.environ["CACHE_ROUTER_SSH_EXTRA_ARGS"] = args.ssh_extra_args
    if args.scp_extra_args:
        os.environ["CACHE_ROUTER_SCP_EXTRA_ARGS"] = args.scp_extra_args
    if args.router_auth is None:
        args.router_auth = False
    if (
        args.command in MUTATING_COMMANDS
        and not is_loopback_bind(args.router_host)
        and not args.router_auth
        and not args.allow_unauthenticated_lan
    ):
        raise SystemExit(
            "--router-host is not loopback and the router is in no-key mode. "
            "Use --allow-unauthenticated-lan for an explicit trusted home-LAN "
            "no-key endpoint."
        )
    if (
        args.command in {"start-worker", "start-workers", "start", "restart", "test"}
        and args.start_sidecar
        and not is_loopback_bind(args.sidecar_bind_host)
        and not args.allow_unauthenticated_lan
    ):
        raise SystemExit(
            "--sidecar-bind-host is not loopback. Use --allow-unauthenticated-lan "
            "for an explicit trusted-LAN worker sidecar, or bind the sidecar to loopback."
        )
    if hasattr(args, "remote_host") and not args.remote_host:
        raise SystemExit("--remote-host is required for live stack commands")
    if args.command in {"status", "restart-router", "worker-status", "start-worker", "stop-worker", "workers-status", "start-workers", "stop-workers"}:
        args.stop_legacy_8081 = False
        args.restore_legacy_8081 = False
    elif args.command == "start":
        args.restore_legacy_8081 = False
    elif args.command == "test":
        args.restore_legacy_8081 = False
    return args


def main() -> int:
    args = parse_args()
    if args.command == "status":
        print(json_dumps(status_stack(args)))
        return 0
    if args.command == "start":
        print(json_dumps(start_stack(args)))
        return 0
    if args.command == "stop":
        print(json_dumps(stop_stack(args)))
        return 0
    if args.command == "restart":
        stop_stack(args)
        print(json_dumps(start_stack(args)))
        return 0
    if args.command == "restart-router":
        print(json_dumps(restart_router_only(args)))
        return 0
    if args.command == "test":
        results = run_endpoint_test(args)
        return 0 if results.get("status") in {"success", "partial_success"} else 1
    if args.command == "worker-status":
        print(json_dumps(worker_only_status(args)))
        return 0
    if args.command == "start-worker":
        print(json_dumps(start_worker_only(args)))
        return 0
    if args.command == "stop-worker":
        print(json_dumps(stop_worker_only(args)))
        return 0
    if args.command == "workers-status":
        print(json_dumps(workers_status(args)))
        return 0
    if args.command == "start-workers":
        print(json_dumps(start_workers(args)))
        return 0
    if args.command == "stop-workers":
        print(json_dumps(stop_workers(args)))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
