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


SCHEMA_VERSION = "2026-07-01.3"
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
            "strict_metadata_auto": True,
            "strict_metadata_force_runtime": bool(args.strict_metadata_force_runtime),
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
        add_optional(row, "model_architecture", args.model_architecture)
        add_optional(row, "model_hash", args.model_hash)
        add_optional(row, "gguf_tensor_manifest_hash", args.gguf_tensor_manifest_hash)
        add_optional(row, "tokenizer_hash", args.tokenizer_hash)
        add_optional(row, "chat_template_effective_hash", args.chat_template_effective_hash)
        add_optional(row, "tools_schema_hash", args.tools_schema_hash)
        add_optional(row, "system_prompt_hash", args.system_prompt_hash)
        add_optional(row, "special_token_policy", args.special_token_policy)
        add_optional(row, "mtp_model", args.mtp_model)
        add_optional(row, "mtp_enabled", args.mtp_enabled)
        add_optional(row, "spec_draft_model_path", args.mtp_model)
        add_optional(row, "spec_draft_model_identity", args.mtp_model_identity)
        add_optional(row, "spec_draft_model_hash", args.spec_draft_model_hash)
        add_optional(row, "spec_draft_config", args.spec_draft_config)
        add_optional(row, "llama_cpp_source_commit", args.llama_cpp_source_commit)
        add_optional(row, "llama_cpp_cache_abi_version", args.llama_cpp_cache_abi_version)
        add_optional(row, "patchset_id", args.patchset_id)
        add_optional(row, "build_backend", args.build_backend)
        add_optional(row, "gpu_backend_driver", args.gpu_backend_driver)
        add_optional(row, "kv_unified_mode", args.kv_unified_mode)
        add_optional(row, "ctx_checkpoints_config", args.ctx_checkpoints_config)
        add_optional(row, "ctx_size", args.ctx_size)
        add_optional(row, "cache_type_k", args.cache_type_k)
        add_optional(row, "cache_type_v", args.cache_type_v)
        add_optional(row, "flash_attention_mode", args.flash_attention_mode)
        add_optional(row, "rope_freq_base", args.rope_freq_base)
        add_optional(row, "rope_freq_scale", args.rope_freq_scale)
        add_optional(row, "yarn_or_rope_scaling_metadata", args.yarn_or_rope_scaling_metadata)
        add_optional(row, "reasoning_format", args.reasoning_format)
        add_optional(row, "jinja_template_mode", args.jinja_template_mode)
        add_optional(row, "n_parallel", args.n_parallel)
        add_optional(row, "n_seq_max", args.n_seq_max)
        workers.append(row)
    if not workers:
        fail("at least one --worker is required")
    return {
        "schema_version": args.schema_version,
        "cache_storage": {
            "cache_root": cache_root,
            "durable_blob_encryption_at_rest": {
                "required": True,
                "mode": args.durable_blob_encryption_mode,
                "evidence_basis": args.durable_blob_encryption_evidence_basis,
                "volume_id_hash": args.durable_blob_encryption_volume_id_hash,
                "key_owner": args.durable_blob_encryption_key_owner,
            },
        },
        "workers": workers,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worker",
        action="append",
        default=[],
        metavar="ID[@SSH_HOST]=LAN_HOST",
        help="Add one worker. Repeat for N workers, e.g. --worker worker-a=<worker-a-lan-ip> --worker worker-b@worker-b-ssh=<worker-b-lan-ip>.",
    )
    parser.add_argument("--output", default="-", help="Output path, or '-' for stdout.")
    parser.add_argument("--force", action="store_true", help="Overwrite --output if it already exists.")
    parser.add_argument("--schema-version", default=SCHEMA_VERSION)
    parser.add_argument("--cache-root", default="/home/<user>/.cache/strix-halo-cache-router")
    parser.add_argument("--durable-blob-encryption-mode", default="operator_managed_encrypted_filesystem", choices=["operator_managed_encrypted_filesystem", "platform_encrypted_volume"], help="Operator-declared encryption-at-rest mode for router-owned durable blobs.")
    parser.add_argument("--durable-blob-encryption-evidence-basis", default="operator_attestation", choices=["operator_attestation", "setup_doctor_metadata"], help="How the inventory records the encrypted-cache-root evidence basis.")
    parser.add_argument("--durable-blob-encryption-volume-id-hash", default="<encrypted-volume-id-sha256>", help="SHA-256 digest of the encrypted volume identifier; do not place raw volume names, keys, or credentials here.")
    parser.add_argument("--durable-blob-encryption-key-owner", default="operator", help="Short non-secret label for the operator/platform that owns the encryption key.")
    parser.add_argument("--worker-port", type=int, default=18082)
    parser.add_argument("--sidecar-port", type=int, default=18083)
    parser.add_argument("--slot-id", type=int, default=0)
    parser.add_argument("--transport", choices=["http"], default="http")
    parser.add_argument(
        "--strict-metadata-force-runtime",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ask the daemon to replace hand-entered strict metadata with runtime-derived strict fields at startup.",
    )
    parser.add_argument("--llama-server", default="", help="Optional path copied into each worker row for setup-doctor commands.")
    parser.add_argument("--model", default="", help="Optional main GGUF path copied into each worker row.")
    parser.add_argument("--model-identity", default="", help="Optional shared model compatibility identity copied into each worker row.")
    parser.add_argument("--model-architecture", default="", help="Optional model architecture identifier for strict cache reuse.")
    parser.add_argument("--model-hash", default="", help="Optional SHA-256 of the main model bytes for strict cache reuse.")
    parser.add_argument("--gguf-tensor-manifest-hash", default="", help="Optional SHA-256 of the GGUF tensor manifest for strict cache reuse.")
    parser.add_argument("--tokenizer-hash", default="", help="Optional SHA-256 of the effective tokenizer for strict cache reuse.")
    parser.add_argument("--chat-template-effective-hash", default="", help="Optional SHA-256 of the rendered effective chat template for strict cache reuse.")
    parser.add_argument("--tools-schema-hash", default="", help="Optional SHA-256 of the effective tools schema, or a canonical empty-tools hash.")
    parser.add_argument("--system-prompt-hash", default="", help="Optional SHA-256 of the effective system prompt, or a canonical empty-system hash.")
    parser.add_argument("--special-token-policy", default="", help="Optional special-token policy identifier for strict cache reuse.")
    parser.add_argument("--mtp-model", default="", help="Optional MTP/draft GGUF path copied into each worker row.")
    parser.add_argument("--mtp-model-identity", default="", help="Optional shared MTP/draft compatibility identity copied into each worker row.")
    parser.add_argument("--mtp-enabled", action=argparse.BooleanOptionalAction, default=None, help="Optional MTP/speculative decoding enablement copied into each worker row.")
    parser.add_argument("--spec-draft-model-hash", default="", help="Optional SHA-256 of the draft model bytes, or 'none' when MTP is disabled.")
    parser.add_argument("--spec-draft-config", default="", help="Optional draft/MTP config identifier, or 'none' when MTP is disabled.")
    parser.add_argument("--llama-cpp-source-commit", default="", help="Optional llama.cpp source commit for strict cache reuse.")
    parser.add_argument("--llama-cpp-cache-abi-version", default="", help="Optional llama.cpp cache ABI version for strict cache reuse.")
    parser.add_argument("--patchset-id", default="", help="Optional local llama.cpp patchset identifier for strict cache reuse.")
    parser.add_argument("--build-backend", default="", help="Optional build backend lane such as vulkan_radv or rocm_hip.")
    parser.add_argument("--gpu-backend-driver", default="", help="Optional GPU backend driver lane for strict cache reuse.")
    parser.add_argument("--kv-unified-mode", action=argparse.BooleanOptionalAction, default=None, help="Optional KV unified mode copied into each worker row.")
    parser.add_argument("--ctx-checkpoints-config", default="", help="Optional ctx-checkpoints configuration identifier.")
    parser.add_argument("--ctx-size", type=int, default=None, help="Optional context size copied into each worker row.")
    parser.add_argument("--cache-type-k", default="", help="Optional K-cache type copied into each worker row.")
    parser.add_argument("--cache-type-v", default="", help="Optional V-cache type copied into each worker row.")
    parser.add_argument("--flash-attention-mode", default="", help="Optional flash-attention mode identifier.")
    parser.add_argument("--rope-freq-base", default="", help="Optional rope frequency base identifier/value for strict cache reuse.")
    parser.add_argument("--rope-freq-scale", default="", help="Optional rope frequency scale identifier/value for strict cache reuse.")
    parser.add_argument("--yarn-or-rope-scaling-metadata", default="", help="Optional YaRN/RoPE scaling metadata identifier for strict cache reuse.")
    parser.add_argument("--reasoning-format", default="", help="Optional reasoning format identifier.")
    parser.add_argument("--jinja-template-mode", default="", help="Optional Jinja template mode identifier.")
    parser.add_argument("--n-parallel", type=int, default=None, help="Optional llama.cpp n_parallel lane copied into each worker row.")
    parser.add_argument("--n-seq-max", type=int, default=None, help="Optional llama.cpp n_seq_max lane copied into each worker row.")
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
