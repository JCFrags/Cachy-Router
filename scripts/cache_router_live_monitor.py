#!/usr/bin/env python3
"""Long-running live monitor for the trusted-LAN Cachy-Router stack.

The monitor is intentionally read-only: it samples health endpoints and log
tails, but never sends generation requests.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ERROR_PATTERNS = (
    "ErrorDeviceLost",
    "decode() failed",
    "vk::Queue::submit",
    "send_error",
    "context is lost",
    "device wedged",
    "ring comp_",
    "Out of memory",
    "oom-kill",
    "Killed process",
)


REMOTE_PROBE = r'''
import glob, json, os, shlex, subprocess

host = os.environ.get("CACHE_ROUTER_MONITOR_HOST", "")
worker_id = os.environ.get("CACHE_ROUTER_MONITOR_WORKER_ID") or host
home = os.path.expanduser(os.environ.get("CACHE_ROUTER_MONITOR_REMOTE_HOME") or "~")
remote_user = os.environ.get("CACHE_ROUTER_MONITOR_REMOTE_USER") or ""
router_log_host = os.environ.get("CACHE_ROUTER_MONITOR_ROUTER_LOG_HOST") or ""
port_regex = os.environ.get("CACHE_ROUTER_MONITOR_PORT_REGEX") or ""
worker_log = f"{home}/.cache/cachy-router/workers/{worker_id}/logs/llama-server.log"
sidecar_log = f"{home}/.cache/cachy-router/workers/{worker_id}/logs/cache-router-worker-sidecar.log"
router_log = f"{home}/.cache/cachy-router/router/logs/cache-router-daemon.log"
patterns = (
    "ErrorDeviceLost",
    "decode() failed",
    "vk::Queue::submit",
    "send_error",
    "context is lost",
    "device wedged",
    "ring comp_",
    "Out of memory",
    "oom-kill",
    "Killed process",
)

def run(argv, timeout=8):
    proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, check=False)
    return {"rc": proc.returncode, "out": proc.stdout.strip()}

def read_tail(path, max_bytes=120000):
    try:
        with open(path, "rb") as fh:
            try:
                fh.seek(-max_bytes, os.SEEK_END)
            except OSError:
                fh.seek(0)
            return fh.read().decode("utf-8", "replace")
    except FileNotFoundError:
        return ""

def matching_lines(text):
    out = []
    for line in text.splitlines():
        if any(pattern in line for pattern in patterns):
            out.append(line[-1000:])
    return out[-30:]

def meminfo():
    out = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(("MemAvailable:", "MemTotal:", "SwapFree:", "SwapTotal:")):
                    key, value = line.split(":", 1)
                    out[key] = value.strip()
    except OSError as exc:
        out["error"] = repr(exc)
    return out

def temps():
    out = {}
    for input_path in sorted(glob.glob("/sys/class/hwmon/hwmon*/temp*_input")):
        label_path = input_path.replace("_input", "_label")
        name_path = os.path.join(os.path.dirname(input_path), "name")
        try:
            with open(input_path, "r", encoding="utf-8") as fh:
                raw = fh.read().strip()
            with open(name_path, "r", encoding="utf-8") as fh:
                chip = fh.read().strip()
            try:
                with open(label_path, "r", encoding="utf-8") as fh:
                    label = fh.read().strip()
            except OSError:
                label = os.path.basename(input_path).removesuffix("_input")
            out[f"{chip}:{label}:{input_path}"] = round(int(raw) / 1000.0, 1)
        except (OSError, ValueError) as exc:
            out[input_path] = f"ERROR: {exc!r}"
    return out

gpu = {}
for path in sorted(glob.glob("/sys/class/drm/card*/device/gpu_busy_percent") + glob.glob("/sys/class/drm/card*/device/power_dpm_force_performance_level")):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            gpu[path] = fh.read().strip()
    except OSError as exc:
        gpu[path] = f"ERROR: {exc!r}"

worker_tail = read_tail(worker_log)
router_tail = read_tail(router_log) if router_log_host and host == router_log_host else ""
dmesg = run(["sudo", "-n", "dmesg", "-T"], timeout=8)
dmesg_tail = "\n".join(dmesg.get("out", "").splitlines()[-300:])
process_args = ["pgrep", "-a"]
if remote_user:
    process_args.extend(["-u", remote_user])
process_args.extend(["-f", "llama-server|cache_router_daemon|cache_router_worker_sidecar"])
ports = {"rc": 0, "out": ""}
if port_regex:
    ports = run(["bash", "-lc", f"ss -ltnp 2>/dev/null | grep -E {shlex.quote(port_regex)} || true"])

print(json.dumps({
    "host": host,
    "worker_id": worker_id,
    "processes": run(process_args),
    "ports": ports,
    "gpu": gpu,
    "temps_c": temps(),
    "meminfo": meminfo(),
    "worker_log": {
        "path": worker_log,
        "error_lines": matching_lines(worker_tail),
    },
    "sidecar_log": {
        "path": sidecar_log,
        "tail_lines": read_tail(sidecar_log, 20000).splitlines()[-10:],
    },
    "router_log": {
        "path": router_log if router_log_host and host == router_log_host else "",
        "error_lines": matching_lines(router_tail) if router_log_host and host == router_log_host else [],
    },
    "kernel": {
        "rc": dmesg.get("rc"),
        "error_lines": matching_lines(dmesg_tail),
    },
}, sort_keys=True))
'''


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_json(url: str, timeout: float = 8.0) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
        return {
            "ok": True,
            "status": resp.status,
            "wall_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "body": json.loads(raw.decode("utf-8", "replace")),
        }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:2000]
        return {"ok": False, "status": exc.code, "wall_ms": round((time.perf_counter() - started) * 1000.0, 3), "body": body}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": None, "wall_ms": round((time.perf_counter() - started) * 1000.0, 3), "error": repr(exc)}


def parse_host_spec(value: str) -> tuple[str, str]:
    """Return (ssh_host, worker_id) for either HOST or HOST=WORKER_ID."""
    if "=" not in value:
        return value, value
    ssh_host, worker_id = value.split("=", 1)
    return ssh_host.strip(), worker_id.strip() or ssh_host.strip()


def _export_line(name: str, value: str) -> str:
    return f"export {name}={shlex.quote(value)}\n"


def ssh_probe(
    host: str,
    *,
    worker_id: str,
    remote_home: str,
    remote_user: str,
    router_log_host: str,
    port_regex: str,
    timeout: float = 20.0,
) -> dict[str, Any]:
    script = "".join(
        [
            _export_line("CACHE_ROUTER_MONITOR_HOST", host),
            _export_line("CACHE_ROUTER_MONITOR_WORKER_ID", worker_id),
            _export_line("CACHE_ROUTER_MONITOR_REMOTE_HOME", remote_home),
            _export_line("CACHE_ROUTER_MONITOR_REMOTE_USER", remote_user),
            _export_line("CACHE_ROUTER_MONITOR_ROUTER_LOG_HOST", router_log_host),
            _export_line("CACHE_ROUTER_MONITOR_PORT_REGEX", port_regex),
            "python3 - <<'PY'\n",
            REMOTE_PROBE,
            "PY\n",
        ]
    )
    started = time.perf_counter()
    proc = subprocess.run(
        ["ssh", host, "bash", "-s"],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    result: dict[str, Any] = {
        "ok": proc.returncode == 0,
        "rc": proc.returncode,
        "wall_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }
    try:
        result["body"] = json.loads(proc.stdout)
    except json.JSONDecodeError:
        result["stdout"] = proc.stdout[-4000:]
    return result


def collect(args: argparse.Namespace) -> dict[str, Any]:
    base = args.router_url.rstrip("/")
    sample = {
        "ts": utc_now(),
        "router_url": base,
        "router_health": http_json(f"{base}/health"),
        "router_workers": http_json(f"{base}/router/workers"),
        "hosts": {},
    }
    for host_spec in args.host:
        host, worker_id = parse_host_spec(host_spec)
        sample["hosts"][host] = ssh_probe(
            host,
            worker_id=worker_id,
            remote_home=args.remote_home,
            remote_user=args.remote_user,
            router_log_host=args.router_log_host,
            port_regex=args.port_regex,
        )
    return sample


def _mem_kib(value: str | None) -> int | None:
    if not value:
        return None
    parts = value.split()
    if not parts:
        return None
    try:
        return int(parts[0])
    except ValueError:
        return None


def alerts_for(
    sample: dict[str, Any],
    seen: set[str],
    *,
    expected_worker_ids: list[str],
    quarantined_hosts: set[str],
    suppress_existing: bool,
    min_mem_available_gb: float,
    max_temp_c: float,
    expected_gpu_dpm: str | None,
) -> list[str]:
    alerts: list[str] = []
    expected_worker_count = len(expected_worker_ids)
    health = sample.get("router_health", {})
    health_body = health.get("body") if isinstance(health.get("body"), dict) else {}
    if not health.get("ok") or health_body.get("status") != "ok":
        alerts.append(f"router health not ok: {health}")
    if expected_worker_count and health_body.get("worker_count") != expected_worker_count:
        alerts.append(f"router worker_count is {health_body.get('worker_count')}, expected {expected_worker_count}")
    if expected_worker_count and health_body.get("healthy_workers") != expected_worker_count:
        alerts.append(f"router healthy_workers is {health_body.get('healthy_workers')}, expected {expected_worker_count}")
    if expected_worker_count and "route_ready_workers" in health_body and health_body.get("route_ready_workers") != expected_worker_count:
        alerts.append(f"router route_ready_workers is {health_body.get('route_ready_workers')}, expected {expected_worker_count}")

    workers_body = sample.get("router_workers", {}).get("body", {})
    if expected_worker_count and isinstance(workers_body, dict) and "route_ready" in workers_body and workers_body.get("route_ready") != expected_worker_count:
        alerts.append(f"router route_ready is {workers_body.get('route_ready')}, expected {expected_worker_count}")
    workers = workers_body.get("workers", []) if isinstance(workers_body, dict) else []
    worker_ids = [row.get("worker_id") for row in workers if isinstance(row, dict)]
    if expected_worker_ids and sorted(worker_ids) != sorted(expected_worker_ids):
        alerts.append(f"router workers are {worker_ids}, expected {expected_worker_ids}")
    for row in workers:
        if not isinstance(row, dict):
            continue
        availability = row.get("availability") if isinstance(row.get("availability"), dict) else {}
        if availability.get("poisoned") is True:
            alerts.append(f"router worker {row.get('worker_id')} poisoned: availability={availability}")

    for host, probe in sample.get("hosts", {}).items():
        body = probe.get("body") if isinstance(probe.get("body"), dict) else {}
        if not probe.get("ok"):
            alerts.append(f"{host} probe failed: {probe}")
            continue
        if host in quarantined_hosts:
            proc_text = body.get("processes", {}).get("out", "")
            port_text = body.get("ports", {}).get("out", "")
            if "llama-server" in proc_text or "cache_router_worker_sidecar" in proc_text or port_text:
                alerts.append(f"{host} model process or monitored port returned while quarantined")
        mem_available_kib = _mem_kib(body.get("meminfo", {}).get("MemAvailable"))
        if mem_available_kib is not None and mem_available_kib < min_mem_available_gb * 1024 * 1024:
            alerts.append(f"{host} MemAvailable low: {mem_available_kib / 1024 / 1024:.1f} GiB < {min_mem_available_gb:.1f} GiB")
        for path, value in body.get("gpu", {}).items():
            if expected_gpu_dpm and path.endswith("power_dpm_force_performance_level") and value != expected_gpu_dpm:
                alerts.append(f"{host} GPU DPM is {value!r}, expected {expected_gpu_dpm!r}")
        for sensor, value in body.get("temps_c", {}).items():
            if isinstance(value, (int, float)) and value >= max_temp_c:
                alerts.append(f"{host} temperature high: {sensor}={value:.1f}C >= {max_temp_c:.1f}C")
        for section in ("worker_log", "router_log", "kernel"):
            for line in body.get(section, {}).get("error_lines", []):
                key = f"{host}:{section}:{line}"
                if key in seen:
                    continue
                seen.add(key)
                if not suppress_existing:
                    alerts.append(f"{host} {section}: {line}")
    return alerts


def write_jsonl(path: Path, value: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(value, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--router-url", default="http://127.0.0.1:18080")
    parser.add_argument(
        "--host",
        action="append",
        default=[],
        help="SSH host to probe. Use HOST=WORKER_ID when the router worker id differs from the SSH host.",
    )
    parser.add_argument("--expected-worker-id", action="append", default=[])
    parser.add_argument("--quarantined-host", action="append", default=[])
    parser.add_argument("--router-log-host", default="")
    parser.add_argument("--remote-home", default="~")
    parser.add_argument("--remote-user", default="")
    parser.add_argument("--port-regex", default=":(18080|18081|18082|18083|8081)")
    parser.add_argument("--expected-gpu-dpm", default="high")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--out-dir", default="monitoring/live")
    parser.add_argument("--min-mem-available-gb", type=float, default=4.0)
    parser.add_argument("--max-temp-c", type=float, default=95.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    samples_path = out_dir / "samples.jsonl"
    alerts_path = out_dir / "alerts.log"
    status_path = out_dir / "status.json"
    pid_path = out_dir / "monitor.pid"
    pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")

    seen: set[str] = set()
    first = True
    while True:
        sample = collect(args)
        write_jsonl(samples_path, sample)
        alerts = alerts_for(
            sample,
            seen,
            expected_worker_ids=args.expected_worker_id,
            quarantined_hosts=set(args.quarantined_host),
            suppress_existing=first,
            min_mem_available_gb=args.min_mem_available_gb,
            max_temp_c=args.max_temp_c,
            expected_gpu_dpm=args.expected_gpu_dpm or None,
        )
        status = {"ts": sample["ts"], "pid": os.getpid(), "samples_path": str(samples_path), "alerts_path": str(alerts_path), "alerts": alerts}
        status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if alerts:
            with alerts_path.open("a", encoding="utf-8") as fh:
                for alert in alerts:
                    fh.write(f"{sample['ts']} {alert}\n")
        print(json.dumps(status, sort_keys=True), flush=True)
        if args.once:
            return 0 if not alerts else 2
        first = False
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
