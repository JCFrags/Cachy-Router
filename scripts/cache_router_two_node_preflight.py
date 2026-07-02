#!/usr/bin/env python3
"""Read-only two-node cache-router parity preflight.

This script does not start, stop, or reconfigure services. It checks the two
Strix Halo hosts for the runtime/model/cache metadata that must match before a
cross-node slot-cache restore test is allowed.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

from cache_router_store_hydration_poc import (  # noqa: E402
    DEFAULT_MODEL,
    DEFAULT_MTP_MODEL,
    DEFAULT_RUNTIME,
    remote_file_info,
    remote_find_server_by_port,
    remote_json,
)


DEFAULT_RIGHT_RUNTIME = ""
DEFAULT_RIGHT_MODEL = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def endpoint(host: str, port: int, path: str) -> dict[str, Any]:
    payload = json.dumps({"port": port, "path": path}, sort_keys=True)
    script = r'''
import json, time, urllib.error, urllib.request
payload = json.loads(PAYLOAD)
url = f"http://127.0.0.1:{payload['port']}{payload['path']}"
start = time.perf_counter()
try:
    with urllib.request.urlopen(url, timeout=5) as resp:
        raw = resp.read(500).decode("utf-8", "replace")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"raw": raw}
        print(json.dumps({"ok": resp.status == 200, "status": resp.status, "body": body, "wall_ms": (time.perf_counter() - start) * 1000.0}, sort_keys=True))
except Exception as exc:
    print(json.dumps({"ok": False, "error": repr(exc), "wall_ms": (time.perf_counter() - start) * 1000.0}, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=20)


def host_basics(host: str) -> dict[str, Any]:
    script = r'''
import json, os, platform, shutil, socket
mem = {}
try:
    with open("/proc/meminfo", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith(("MemTotal:", "MemAvailable:", "SwapTotal:", "SwapFree:")):
                key, rest = line.split(":", 1)
                mem[key] = rest.strip()
except FileNotFoundError:
    pass
try:
    disk = shutil.disk_usage(os.path.expanduser("~/.cache"))
    disk_row = {"free_bytes": disk.free, "total_bytes": disk.total}
except FileNotFoundError:
    disk_row = {}
print(json.dumps({
    "hostname": socket.gethostname(),
    "user": os.environ.get("USER"),
    "kernel": platform.release(),
    "machine": platform.machine(),
    "meminfo": mem,
    "cache_disk": disk_row,
}, sort_keys=True))
'''
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=20)


def runtime_version(host: str, runtime: str) -> dict[str, Any]:
    payload = json.dumps({"runtime": runtime}, sort_keys=True)
    script = r'''
import json, os, subprocess
payload = json.loads(PAYLOAD)
runtime = payload["runtime"]
out = {"path": runtime, "exists": os.path.exists(runtime)}
if out["exists"]:
    try:
        proc = subprocess.run([runtime, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20, check=False)
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        out.update({"returncode": proc.returncode, "version": lines[0] if lines else "unknown"})
    except Exception as exc:
        out.update({"error": repr(exc), "version": "unknown"})
print(json.dumps(out, sort_keys=True))
'''.replace("PAYLOAD", repr(payload))
    return remote_json(host, "python3 - <<'PY'\n" + script + "PY\n", timeout=30)


def flag_value(argv: list[str], names: list[str]) -> str | None:
    for i, arg in enumerate(argv):
        for name in names:
            if arg == name and i + 1 < len(argv):
                return argv[i + 1]
            if arg.startswith(name + "="):
                return arg.split("=", 1)[1]
    return None


def flag_present(argv: list[str], names: list[str]) -> bool:
    return any(arg in names for arg in argv)


def process_summary(proc: dict[str, Any]) -> dict[str, Any]:
    argv = [str(item) for item in proc.get("argv", [])]
    return {
        "pid": proc.get("pid"),
        "start_ticks": proc.get("start_ticks"),
        "model_arg": flag_value(argv, ["-m", "--model"]),
        "ctx_size": flag_value(argv, ["-c", "--ctx-size"]),
        "batch_size": flag_value(argv, ["-b", "--batch-size"]),
        "ubatch_size": flag_value(argv, ["-ub", "--ubatch-size"]),
        "cache_type_k": flag_value(argv, ["-ctk", "--cache-type-k"]),
        "cache_type_v": flag_value(argv, ["-ctv", "--cache-type-v"]),
        "slot_save_path": flag_value(argv, ["--slot-save-path"]),
        "spec_type": flag_value(argv, ["--spec-type"]),
        "spec_draft_model": flag_value(argv, ["--spec-draft-model"]),
        "spec_draft_n_max": flag_value(argv, ["--spec-draft-n-max"]),
        "kv_unified": flag_present(argv, ["--kv-unified"]),
        "cache_prompt": flag_present(argv, ["--cache-prompt"]),
        "no_context_shift": flag_present(argv, ["--no-context-shift"]),
        "raw_argc": len(argv),
    }


def collect_host(
    *,
    host: str,
    role: str,
    runtime: str,
    model: str,
    mtp_model: str,
    hash_models: bool,
) -> dict[str, Any]:
    ports = {port: remote_find_server_by_port(host, port) for port in [8081, 18080, 18082]}
    processes = {
        str(port): [process_summary(proc) for proc in rows]
        for port, rows in ports.items()
    }
    active_worker = None
    for port in ["18082", "8081"]:
        if processes.get(port):
            active_worker = {"port": int(port), **processes[port][0]}
            break
    return {
        "role": role,
        "ssh_target": host,
        "basics": host_basics(host),
        "runtime": runtime_version(host, runtime),
        "model": remote_file_info(host, model, hash_file=hash_models, timeout=900 if hash_models else 60),
        "mtp_model": remote_file_info(host, mtp_model, hash_file=hash_models, timeout=300 if hash_models else 60),
        "ports": {
            "8081": {"processes": processes["8081"], "health": endpoint(host, 8081, "/health")},
            "18080": {"processes": processes["18080"], "health": endpoint(host, 18080, "/health")},
            "18082": {"processes": processes["18082"], "health": endpoint(host, 18082, "/health")},
        },
        "active_worker": active_worker,
        "expected_paths": {
            "runtime": runtime,
            "model": model,
            "mtp_model": mtp_model,
        },
    }


def same(a: Any, b: Any) -> bool:
    return a is not None and b is not None and a == b


def compare(left: dict[str, Any], right: dict[str, Any], *, hash_models: bool) -> dict[str, Any]:
    l_worker = left.get("active_worker") or {}
    r_worker = right.get("active_worker") or {}
    checks = []

    def add(name: str, ok: bool, left_value: Any, right_value: Any, critical: bool = True) -> None:
        checks.append({"name": name, "ok": bool(ok), "left": left_value, "right": right_value, "critical": critical})

    add("runtime_exists", left["runtime"].get("exists") is True and right["runtime"].get("exists") is True, left["runtime"].get("exists"), right["runtime"].get("exists"))
    add("runtime_version_match", same(left["runtime"].get("version"), right["runtime"].get("version")), left["runtime"].get("version"), right["runtime"].get("version"))
    add("model_exists", left["model"].get("exists") is True and right["model"].get("exists") is True, left["model"].get("exists"), right["model"].get("exists"))
    add("model_size_match", same(left["model"].get("size_bytes"), right["model"].get("size_bytes")), left["model"].get("size_bytes"), right["model"].get("size_bytes"))
    if hash_models:
        add("model_sha256_match", same(left["model"].get("sha256"), right["model"].get("sha256")), left["model"].get("sha256"), right["model"].get("sha256"))
    add("mtp_exists", left["mtp_model"].get("exists") is True and right["mtp_model"].get("exists") is True, left["mtp_model"].get("exists"), right["mtp_model"].get("exists"))
    add("mtp_size_match", same(left["mtp_model"].get("size_bytes"), right["mtp_model"].get("size_bytes")), left["mtp_model"].get("size_bytes"), right["mtp_model"].get("size_bytes"))
    if hash_models:
        add("mtp_sha256_match", same(left["mtp_model"].get("sha256"), right["mtp_model"].get("sha256")), left["mtp_model"].get("sha256"), right["mtp_model"].get("sha256"))
    add("both_have_active_worker", bool(l_worker) and bool(r_worker), l_worker.get("port"), r_worker.get("port"))
    for field in [
        "ctx_size",
        "batch_size",
        "ubatch_size",
        "cache_type_k",
        "cache_type_v",
        "spec_type",
        "spec_draft_n_max",
        "kv_unified",
        "cache_prompt",
        "no_context_shift",
    ]:
        add(f"active_worker_{field}_match", same(l_worker.get(field), r_worker.get(field)), l_worker.get(field), r_worker.get(field))
    add("both_have_slot_save_path", bool(l_worker.get("slot_save_path")) and bool(r_worker.get("slot_save_path")), l_worker.get("slot_save_path"), r_worker.get("slot_save_path"))
    add("active_worker_model_arg_match", same(l_worker.get("model_arg"), r_worker.get("model_arg")), l_worker.get("model_arg"), r_worker.get("model_arg"), critical=False)
    add("active_worker_spec_draft_model_arg_match", same(l_worker.get("spec_draft_model"), r_worker.get("spec_draft_model")), l_worker.get("spec_draft_model"), r_worker.get("spec_draft_model"), critical=False)

    failed_critical = [row for row in checks if row["critical"] and not row["ok"]]
    return {
        "ready_for_cross_node_restore": not failed_critical,
        "failed_critical_checks": [row["name"] for row in failed_critical],
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-host", default="")
    parser.add_argument("--right-host", default="")
    parser.add_argument("--left-runtime", default=DEFAULT_RUNTIME)
    parser.add_argument("--right-runtime", default=DEFAULT_RIGHT_RUNTIME)
    parser.add_argument("--left-model", default=DEFAULT_MODEL)
    parser.add_argument("--right-model", default=DEFAULT_RIGHT_MODEL)
    parser.add_argument("--left-mtp-model", default=DEFAULT_MTP_MODEL)
    parser.add_argument("--right-mtp-model", default=DEFAULT_MTP_MODEL)
    parser.add_argument("--hash-models", action="store_true", help="Hash model files; this is read-only but can take time.")
    parser.add_argument("--out", type=Path, help="Optional path for the JSON report.")
    parser.add_argument("--fail-on-not-ready", action="store_true", help="Exit 2 if critical parity checks fail.")
    args = parser.parse_args()
    missing = [
        name
        for name in ["left_host", "right_host", "left_runtime", "right_runtime", "left_model", "right_model", "left_mtp_model", "right_mtp_model"]
        if not getattr(args, name)
    ]
    if missing:
        raise SystemExit("live preflight requires: " + ", ".join("--" + name.replace("_", "-") for name in missing))

    report = {
        "schema_version": "2026-07-01.1",
        "created_utc": now_iso(),
        "mutation": "none",
        "hosts": {
            "left": collect_host(
                host=args.left_host,
                role="left",
                runtime=args.left_runtime,
                model=args.left_model,
                mtp_model=args.left_mtp_model,
                hash_models=args.hash_models,
            ),
            "right": collect_host(
                host=args.right_host,
                role="right",
                runtime=args.right_runtime,
                model=args.right_model,
                mtp_model=args.right_mtp_model,
                hash_models=args.hash_models,
            ),
        },
        "hash_models": args.hash_models,
    }
    report["comparison"] = compare(report["hosts"]["left"], report["hosts"]["right"], hash_models=args.hash_models)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json_dumps(report), encoding="utf-8")
    print(json_dumps(report))
    if args.fail_on_not_ready and not report["comparison"]["ready_for_cross_node_restore"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
