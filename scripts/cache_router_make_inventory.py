#!/usr/bin/env python3
"""Generate a cache-router worker inventory from simple worker specs.

This is an offline setup helper for fresh checkouts. It does not contact SSH,
start services, read model files, or mutate remote hosts.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


SCHEMA_VERSION = "2026-07-01.2"
WORKER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=False) + "\n"


def fail(message: str) -> None:
    raise SystemExit(message)


def normalize_lan_host(value: str) -> str:
    raw = value.strip()
    if not raw:
        fail("worker LAN host/url must not be empty")
    if raw.startswith(("http://", "https://")):
        parsed = urlparse(raw)
        if not parsed.hostname:
            fail(f"invalid worker URL: {raw}")
        return parsed.hostname
    if "/" in raw:
        fail(f"worker LAN host must be a host/IP, not a path: {raw}")
    return raw


def parse_worker_spec(spec: str) -> tuple[str, str, str]:
    """Parse worker specs.

    Supported forms:
      worker-id=lan-host
      worker-id@ssh-host=lan-host
    """

    if "=" not in spec:
        fail(f"worker spec must be worker-id=lan-host or worker-id@ssh-host=lan-host: {spec}")
    left, lan = spec.split("=", 1)
    if "@" in left:
        worker_id, ssh_host = left.split("@", 1)
    else:
        worker_id = left
        ssh_host = left
    worker_id = worker_id.strip()
    ssh_host = ssh_host.strip()
    lan_host = normalize_lan_host(lan)
    if not WORKER_ID_RE.match(worker_id):
        fail(f"invalid worker_id {worker_id!r}; use letters, numbers, dot, underscore, and dash")
    if not ssh_host or any(char.isspace() for char in ssh_host):
        fail(f"invalid ssh host for worker {worker_id!r}: {ssh_host!r}")
    return worker_id, ssh_host, lan_host


def add_optional(row: dict[str, Any], key: str, value: Any) -> None:
    if value not in (None, ""):
        row[key] = value


def build_inventory(args: argparse.Namespace) -> dict[str, Any]:
    seen_ids: set[str] = set()
    workers: list[dict[str, Any]] = []
    cache_root = args.cache_root.rstrip("/")
    for spec in args.worker:
        worker_id, ssh_host, lan_host = parse_worker_spec(spec)
        if worker_id in seen_ids:
            fail(f"duplicate worker_id: {worker_id}")
        seen_ids.add(worker_id)
        slot_path = f"{cache_root}/workers/{worker_id}/slots"
        row: dict[str, Any] = {
            "worker_id": worker_id,
            "ssh_host": ssh_host,
            "worker_url": f"http://{lan_host}:{args.worker_port}",
            "slot_save_path": slot_path,
            "slot_id": args.slot_id,
            "transport": {
                "kind": args.transport,
                "sidecar_url": f"http://{lan_host}:{args.sidecar_port}",
            },
        }
        add_optional(row, "llama_server", args.llama_server)
        add_optional(row, "llama_server_path", args.llama_server)
        add_optional(row, "model", args.model)
        add_optional(row, "model_path", args.model)
        add_optional(row, "model_identity", args.model_identity)
        add_optional(row, "mtp_model", args.mtp_model)
        add_optional(row, "spec_draft_model_path", args.mtp_model)
        add_optional(row, "spec_draft_model_identity", args.mtp_model_identity)
        add_optional(row, "ctx_size", args.ctx_size)
        workers.append(row)
    if not workers:
        fail("at least one --worker is required")
    return {"schema_version": args.schema_version, "workers": workers}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worker",
        action="append",
        default=[],
        metavar="ID[@SSH_HOST]=LAN_HOST",
        help="Add one worker. Repeat for N workers, e.g. --worker worker-a=192.168.1.20 --worker worker-b@worker-b-ssh=192.168.1.21.",
    )
    parser.add_argument("--output", default="-", help="Output path, or '-' for stdout.")
    parser.add_argument("--force", action="store_true", help="Overwrite --output if it already exists.")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION)
    parser.add_argument("--cache-root", default=".cache/cachy-router")
    parser.add_argument("--worker-port", type=int, default=18082)
    parser.add_argument("--sidecar-port", type=int, default=18083)
    parser.add_argument("--slot-id", type=int, default=0)
    parser.add_argument("--transport", choices=["http"], default="http")
    parser.add_argument("--llama-server", default="", help="Optional path copied into each worker row for setup-doctor commands.")
    parser.add_argument("--model", default="", help="Optional main GGUF path copied into each worker row.")
    parser.add_argument("--model-identity", default="", help="Optional shared model compatibility identity copied into each worker row.")
    parser.add_argument("--mtp-model", default="", help="Optional MTP/draft GGUF path copied into each worker row.")
    parser.add_argument("--mtp-model-identity", default="", help="Optional shared MTP/draft compatibility identity copied into each worker row.")
    parser.add_argument("--ctx-size", type=int, default=None, help="Optional context size copied into each worker row.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inventory = build_inventory(args)
    output = json_dumps(inventory)
    if args.output == "-":
        sys.stdout.write(output)
        return 0
    out_path = Path(args.output)
    if out_path.exists() and not args.force:
        fail(f"{out_path} already exists; pass --force to overwrite")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(output, encoding="utf-8")
    print(f"wrote {out_path} with {len(inventory['workers'])} workers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
