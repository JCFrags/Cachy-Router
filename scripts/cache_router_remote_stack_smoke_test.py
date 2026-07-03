#!/usr/bin/env python3
"""Offline smoke tests for cache_router_remote_stack command construction."""

from __future__ import annotations

from types import SimpleNamespace

import cache_router_remote_stack as stack


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def argv_value(argv: list[str], flag: str) -> str | None:
    try:
        index = argv.index(flag)
    except ValueError:
        return None
    if index + 1 >= len(argv):
        return None
    return argv[index + 1]


def base_args(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "remote_host": "worker-a",
        "remote_cache_root": "/tmp/cache-router",
        "worker_slot_dir_override": "",
        "worker_id": "worker-a",
        "worker_port": 18082,
        "worker_bind_host": "127.0.0.1",
        "worker_transport": "http",
        "worker_ssh_host": "",
        "worker_sidecar_url": "",
        "sidecar_bind_host": "127.0.0.1",
        "sidecar_port": 18083,
        "start_sidecar": True,
        "router_host": "127.0.0.1",
        "router_port": 18080,
        "router_auth": False,
        "production_router_mode": False,
        "allow_production_router_admin_endpoints": False,
        "disable_router_admin_endpoints": False,
        "allow_unauthenticated_lan": False,
        "strict_metadata_force_runtime": True,
        "llama_server": "/opt/llama.cpp/llama-server",
        "model": "/models/model.gguf",
        "mtp_enabled": True,
        "mtp_model": "/models/draft.gguf",
        "mmproj_model": "",
        "ctx_size": 4096,
        "cache_ram_mib": 0,
        "ctx_checkpoints": 0,
        "spec_draft_n_max": 2,
        "spec_draft_n_min": 0,
        "spec_draft_p_split": "0.10",
        "spec_draft_p_min": "0.60",
        "spec_draft_type_k": "q8_0",
        "spec_draft_type_v": "q8_0",
        "spec_draft_ngl": "all",
        "timeout": 30.0,
        "ready_timeout": 30.0,
        "workers_file": "",
        "ssh_config": "",
        "ssh_extra_args": "",
        "scp_extra_args": "",
        "durable_blob_encryption_mode": "",
        "durable_blob_encryption_evidence_basis": "",
        "durable_blob_encryption_volume_id_hash": "",
        "durable_blob_encryption_key_owner": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def main() -> int:
    mtp_args = base_args()
    mtp_worker_argv = stack.make_worker_argv(mtp_args)
    assert_true("--spec-draft-model" in mtp_worker_argv, "MTP worker argv should include draft model flag")
    assert_true(argv_value(mtp_worker_argv, "--spec-draft-model") == "/models/draft.gguf", "MTP draft path mismatch")

    no_mtp_args = base_args(mtp_enabled=False)
    no_mtp_worker_argv = stack.make_worker_argv(no_mtp_args)
    assert_true("--spec-draft-model" not in no_mtp_worker_argv, "no-MTP worker argv must not load a draft model")
    assert_true("--spec-type" not in no_mtp_worker_argv, "no-MTP worker argv must not set speculative type")
    assert_true("-fit" in no_mtp_worker_argv, "no-MTP worker argv should preserve fit flag")

    snapshot = {
        "runtime_version": "llama.cpp-test",
        "model": {"size_bytes": 123},
        "mtp_model": {"size_bytes": 456},
    }
    mtp_router_argv = stack.make_router_argv(mtp_args, snapshot)
    assert_true("--mtp-enabled" in mtp_router_argv, "MTP router argv should advertise MTP")
    assert_true(argv_value(mtp_router_argv, "--spec-draft-model-path") == "/models/draft.gguf", "MTP router draft path mismatch")
    assert_true(argv_value(mtp_router_argv, "--spec-draft-model-size") == "456", "MTP router draft size mismatch")

    no_mtp_router_argv = stack.make_router_argv(no_mtp_args, snapshot)
    assert_true("--no-mtp-enabled" in no_mtp_router_argv, "no-MTP router argv should advertise no MTP")
    assert_true("--mtp-enabled" not in no_mtp_router_argv, "no-MTP router argv should not advertise MTP")
    assert_true(argv_value(no_mtp_router_argv, "--spec-draft-model-path") == "none", "no-MTP router draft path should be none")
    assert_true(argv_value(no_mtp_router_argv, "--spec-draft-model-size") == "0", "no-MTP router draft size should be zero")

    inventory_args = stack.inventory_worker_args(
        base_args(mtp_enabled=True),
        {
            "worker_id": "worker-b",
            "worker_url": "http://127.0.0.1:18092",
            "slot_save_path": "/tmp/cache-router/workers/worker-b/slots",
            "mtp_enabled": False,
            "transport": {"kind": "http", "sidecar_url": "http://127.0.0.1:18093"},
        },
    )
    assert_true(inventory_args.mtp_enabled is False, "inventory mtp_enabled=false should override CLI default")
    assert_true(inventory_args.worker_port == 18092, "inventory worker port should be derived from worker_url")
    assert_true(inventory_args.sidecar_port == 18093, "inventory sidecar port should be derived from sidecar_url")

    print('{"ok": true, "checks": 13}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
