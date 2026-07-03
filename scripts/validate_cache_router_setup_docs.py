#!/usr/bin/env python3
"""Validate setup docs against the offline fresh-checkout setup contract.

This linter intentionally proves a narrow documentation claim:

- public docs show a fresh-checkout offline setup path;
- the documented setup doctor path runs without contacting live hosts;
- live runtime commands are marked as operator-supplied trusted-LAN operations;
- examples use placeholders instead of private deployment coordinates.

It does not prove that any live worker deployment is reachable or correct.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
SETUP_DOC = ROOT / "docs" / "architecture" / "cache-router-setup.md"
CONFIG_README = ROOT / "configs" / "cache-router" / "README.md"
GITIGNORE = ROOT / ".gitignore"
MAKEFILE = ROOT / "Makefile"
SETUP_DOCTOR = ROOT / "scripts" / "cache_router_setup_doctor.py"
SETUP_DOCTOR_MATRIX = ROOT / "scripts" / "cache_router_setup_doctor_matrix_test.py"
REMOTE_STACK = ROOT / "scripts" / "cache_router_remote_stack.py"
WORKER_SIDECAR = ROOT / "scripts" / "cache_router_worker_sidecar.py"
WORKERS_EXAMPLE = ROOT / "configs" / "cache-router" / "workers.example.json"

PRIVATE_COORDINATE_RE = re.compile(r"\b(?:10|192\.168|172\.(?:1[6-9]|2\d|3[01]))(?:\.\d{1,3}){2}\b")
PRIVATE_HOST_HINTS = {
    "private-worker-",
    "private-operator-user",
    "/home/privateuser",
    "REAL_HF_TOKEN",
    "REAL_OPENAI_API_KEY",
}

REQUIRED_SETUP_SNIPPETS = [
    "Recommended setup flow for a fresh GitHub checkout:",
    "Validate an inventory without touching live hosts:",
    "After workers and the router are running, add `--live`",
    "Commands in the runtime sections below are live operations.",
    "operator-supplied deployment inventory",
    "The default examples below bind the router, workers, and sidecars to",
    "Do not use these flags on a public interface or untrusted network.",
    "There is no intended two-worker limit.",
    "configs/cache-router/<deployment>.workers.json",
    "operator-managed encrypted cache root",
    "durable_blob_encryption_at_rest",
    "volume_id_hash",
]

REQUIRED_README_SNIPPETS = [
    "Run the offline checks with `make check`.",
    "Validate an inventory without touching live hosts:",
    "The following commands are live operations.",
    "operator-supplied deployment inventory",
    "Do not expose a router, worker, or sidecar directly to the public internet or any untrusted network.",
    "configs/cache-router/<deployment>.workers.json",
    "operator-managed encrypted cache root",
    "durable_blob_encryption_at_rest",
]

REQUIRED_CONFIG_SNIPPETS = [
    "Use `workers.example.json` as the public template for new deployments.",
    "Do not commit local deployment inventories unless they are sanitized examples.",
    "Files matching `configs/cache-router/*.local.json`,",
    "`configs/cache-router/*.workers.json` are ignored by default.",
    "durable_blob_encryption_at_rest",
    "operator-managed encrypted cache root",
]


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def squash_ws(text: str) -> str:
    return " ".join(text.split())


def fenced_blocks(markdown: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    fence_lang: str | None = None
    lines: list[str] = []
    for line in markdown.splitlines():
        if line.startswith("```"):
            if fence_lang is None:
                fence_lang = line[3:].strip()
                lines = []
            else:
                blocks.append((fence_lang, "\n".join(lines)))
                fence_lang = None
                lines = []
            continue
        if fence_lang is not None:
            lines.append(line)
    return blocks


def command_blocks(markdown: str, needle: str) -> list[str]:
    return [body for lang, body in fenced_blocks(markdown) if lang in {"bash", "text", ""} and needle in body]


def run_setup_doctor() -> tuple[dict[str, Any] | None, list[str]]:
    cmd = [
        sys.executable,
        str(SETUP_DOCTOR),
        "--workers-file",
        str(WORKERS_EXAMPLE.relative_to(ROOT)),
        "--json",
    ]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    errors: list[str] = []
    if proc.returncode != 0:
        errors.append(f"setup doctor exited {proc.returncode}: {proc.stderr.strip() or proc.stdout.strip()}")
        return None, errors
    try:
        summary = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        errors.append(f"setup doctor did not print JSON: {exc}")
        return None, errors
    if not isinstance(summary, dict):
        errors.append("setup doctor JSON summary must be an object")
        return None, errors
    return summary, errors


def run_setup_doctor_matrix() -> tuple[dict[str, Any] | None, list[str]]:
    cmd = [sys.executable, str(SETUP_DOCTOR_MATRIX)]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    errors: list[str] = []
    if proc.returncode != 0:
        errors.append(f"setup doctor matrix exited {proc.returncode}: {proc.stderr.strip() or proc.stdout.strip()}")
        return None, errors
    try:
        summary = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        errors.append(f"setup doctor matrix did not print JSON: {exc}")
        return None, errors
    if not isinstance(summary, dict):
        errors.append("setup doctor matrix JSON summary must be an object")
        return None, errors
    return summary, errors


def validate_setup_doctor_summary(summary: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if summary.get("ok") is not True:
        errors.append("offline setup doctor against workers.example.json should be ok")
    if summary.get("failures") != 0:
        errors.append("offline setup doctor against workers.example.json should have zero failures")
    if int(summary.get("worker_count") or 0) < 2:
        errors.append("workers.example.json should demonstrate an N-worker inventory with at least two rows")
    storage = summary.get("cache_storage")
    if not isinstance(storage, dict) or storage.get("declared") is not True:
        errors.append("setup doctor summary should include declared cache_storage from workers.example.json")
    else:
        encryption = storage.get("encryption_at_rest")
        if not isinstance(encryption, dict):
            errors.append("setup doctor summary should include durable blob encryption_at_rest metadata")
        else:
            if encryption.get("required") is not True:
                errors.append("setup doctor cache_storage encryption_at_rest.required should be true")
            if encryption.get("mode") != "operator_managed_encrypted_filesystem":
                errors.append("setup doctor cache_storage encryption_at_rest.mode should be operator_managed_encrypted_filesystem")
            if encryption.get("evidence_basis") != "operator_attestation":
                errors.append("setup doctor cache_storage encryption_at_rest.evidence_basis should be operator_attestation")
    if summary.get("live_checks") not in ({}, None):
        errors.append("offline setup doctor proof must not run live checks")
    commands = summary.get("commands")
    if not isinstance(commands, dict):
        errors.append("setup doctor summary must include generated commands")
        return errors
    runtime_notice = str(summary.get("runtime_notice") or "")
    if "live operations" not in runtime_notice or "operator-supplied deployment inventory" not in runtime_notice:
        errors.append("setup doctor summary should include a live-operation runtime_notice")
    for key in ["start_workers", "start_router", "check_router"]:
        if key not in commands:
            errors.append(f"setup doctor summary missing command {key}")
    start_router = str(commands.get("start_router") or "")
    if "--allow-unauthenticated-lan" not in start_router or "--no-router-auth" not in start_router:
        errors.append("setup doctor start-router command should make trusted-LAN unauthenticated mode explicit")
    return errors


def validate_setup_doctor_matrix_summary(summary: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if summary.get("ok") is not True:
        errors.append("setup doctor matrix should be ok")
    if summary.get("valid_inventory_counts") != [1, 2, 3, 8]:
        errors.append("setup doctor matrix should prove valid inventory counts [1, 2, 3, 8]")
    cache_storage = summary.get("cache_storage_encryption")
    if not isinstance(cache_storage, dict):
        errors.append("setup doctor matrix should include cache_storage_encryption")
    else:
        if cache_storage.get("valid_ok") is not True:
            errors.append("setup doctor matrix cache_storage valid case should pass")
        if cache_storage.get("placeholder_warns") is not True:
            errors.append("setup doctor matrix cache_storage placeholder volume hash should warn")
        broken_storage = cache_storage.get("broken_cases")
        if not isinstance(broken_storage, dict):
            errors.append("setup doctor matrix cache_storage should include broken_cases")
        else:
            for case in [
                "missing_encryption_metadata",
                "plaintext_mode",
                "required_false",
                "bad_evidence_basis",
                "bad_volume_hash",
                "blank_key_owner",
            ]:
                if case not in broken_storage:
                    errors.append(f"setup doctor matrix missing cache_storage encryption case {case}")
    broken = summary.get("broken_cases")
    if not isinstance(broken, dict):
        errors.append("setup doctor matrix summary should include broken_cases")
    else:
        for case in [
            "duplicate_worker_id",
            "invalid_worker_url",
            "invalid_http_sidecar_url",
            "missing_slot_path",
            "newline_slot_path",
            "relative_slot_path",
            "path_traversal",
            "bad_transport",
            "missing_ssh_host",
        ]:
            if case not in broken:
                errors.append(f"setup doctor matrix missing broken inventory case {case}")
    live = summary.get("live_checks")
    if not isinstance(live, dict):
        errors.append("setup doctor matrix summary should include live_checks")
    else:
        failures = live.get("failure_cases")
        if not isinstance(failures, dict):
            errors.append("setup doctor matrix live_checks should include failure_cases")
        else:
            for case in ["live_unreachable_router", "live_unreachable_worker", "live_unreachable_http_sidecar"]:
                if case not in failures:
                    errors.append(f"setup doctor matrix missing live failure case {case}")
    return errors


def validate_workers_example() -> list[str]:
    errors: list[str] = []
    try:
        raw = json.loads(WORKERS_EXAMPLE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{rel(WORKERS_EXAMPLE)}: invalid JSON: {exc}"]
    workers = raw.get("workers") if isinstance(raw, dict) else None
    storage = raw.get("cache_storage") if isinstance(raw, dict) else None
    if not isinstance(storage, dict):
        errors.append(f"{rel(WORKERS_EXAMPLE)}: expected top-level cache_storage object")
    else:
        cache_root = storage.get("cache_root")
        if not isinstance(cache_root, str) or not cache_root.startswith("/home/<user>/"):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: cache_storage.cache_root should be an absolute placeholder path")
        encryption = storage.get("durable_blob_encryption_at_rest")
        if not isinstance(encryption, dict):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: cache_storage.durable_blob_encryption_at_rest should be an object")
        else:
            if encryption.get("required") is not True:
                errors.append(f"{rel(WORKERS_EXAMPLE)}: durable blob encryption must be required")
            if encryption.get("mode") != "operator_managed_encrypted_filesystem":
                errors.append(f"{rel(WORKERS_EXAMPLE)}: durable blob encryption mode should be operator_managed_encrypted_filesystem")
            if encryption.get("evidence_basis") != "operator_attestation":
                errors.append(f"{rel(WORKERS_EXAMPLE)}: durable blob encryption evidence_basis should be operator_attestation")
            if encryption.get("volume_id_hash") != "<encrypted-volume-id-sha256>":
                errors.append(f"{rel(WORKERS_EXAMPLE)}: durable blob encryption volume_id_hash should remain a public placeholder")
            if encryption.get("key_owner") != "operator":
                errors.append(f"{rel(WORKERS_EXAMPLE)}: durable blob encryption key_owner should be the generic operator label")
    if not isinstance(workers, list) or len(workers) < 2:
        return [f"{rel(WORKERS_EXAMPLE)}: expected at least two public example workers"]
    seen_ids: set[str] = set()
    for index, row in enumerate(workers):
        if not isinstance(row, dict):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}] must be an object")
            continue
        worker_id = row.get("worker_id")
        if worker_id in seen_ids:
            errors.append(f"{rel(WORKERS_EXAMPLE)}: duplicate worker_id {worker_id!r}")
        if not isinstance(worker_id, str) or not re.fullmatch(r"worker-[a-z0-9-]+", worker_id):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].worker_id should be generic, found {worker_id!r}")
        seen_ids.add(str(worker_id))
        for field in ["ssh_host", "worker_url"]:
            value = row.get(field)
            if not isinstance(value, str) or "<" not in value or ">" not in value:
                errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].{field} should remain a public placeholder")
        if row.get("strict_metadata_auto") is not True:
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].strict_metadata_auto should be true")
        if row.get("strict_metadata_force_runtime") is not True:
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].strict_metadata_force_runtime should be true")
        for field in ["model_hash", "gguf_tensor_manifest_hash", "tokenizer_hash", "chat_template_effective_hash", "spec_draft_model_hash", "spec_draft_config"]:
            if field in row:
                errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].{field} should be omitted when runtime-forced strict metadata is enabled")
        slot_path = row.get("slot_save_path")
        if not isinstance(slot_path, str) or not slot_path.startswith("/home/<user>/"):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].slot_save_path should be an absolute placeholder path")
        transport = row.get("transport")
        if not isinstance(transport, dict) or transport.get("kind") != "http":
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].transport.kind should be http")
        elif "<" not in str(transport.get("sidecar_url", "")):
            errors.append(f"{rel(WORKERS_EXAMPLE)}: workers[{index}].transport.sidecar_url should be a placeholder")
    errors.extend(validate_no_private_coordinates(WORKERS_EXAMPLE, WORKERS_EXAMPLE.read_text(encoding="utf-8")))
    return errors


def validate_gitignore_and_makefile() -> list[str]:
    errors: list[str] = []
    gitignore = read(GITIGNORE)
    config_doc = read(CONFIG_README)
    for pattern in ["configs/cache-router/*.local.json", "configs/cache-router/local*.json", "configs/cache-router/*.workers.json"]:
        if pattern not in gitignore:
            errors.append(f"{rel(GITIGNORE)}: missing local inventory ignore pattern {pattern}")
        if pattern not in config_doc:
            errors.append(f"{rel(CONFIG_README)}: missing documented local inventory ignore pattern {pattern}")
    makefile = read(MAKEFILE)
    for command in [
        "scripts/cache_router_setup_doctor.py --workers-file configs/cache-router/workers.example.json --json",
        "scripts/cache_router_setup_doctor_matrix_test.py",
        "scripts/validate_cache_router_setup_docs.py",
    ]:
        if command not in makefile:
            errors.append(f"{rel(MAKEFILE)}: make check missing {command}")
    check_target = makefile.split("check:", 1)[1] if "check:" in makefile else makefile
    for forbidden in ["cache_router_remote_stack.py", "--live", " ssh ", " scp ", "curl "]:
        if forbidden in check_target:
            errors.append(f"{rel(MAKEFILE)}: make check should not contain live/remote command token {forbidden!r}")
    return errors


def validate_runtime_guard_sources() -> list[str]:
    errors: list[str] = []
    remote_stack = read(REMOTE_STACK)
    for snippet in [
        "--worker-bind-host is not loopback",
        "for an explicit trusted-LAN llama-server worker",
        "for an explicit trusted-LAN worker sidecar",
        "Use --allow-unauthenticated-lan for an explicit trusted home-LAN",
    ]:
        if snippet not in remote_stack:
            errors.append(f"{rel(REMOTE_STACK)}: missing trusted-LAN guard snippet {snippet!r}")
    sidecar = read(WORKER_SIDECAR)
    for snippet in [
        "--allow-unauthenticated-lan",
        "--host is not loopback",
        "explicit trusted-LAN sidecar",
    ]:
        if snippet not in sidecar:
            errors.append(f"{rel(WORKER_SIDECAR)}: missing trusted-LAN sidecar guard snippet {snippet!r}")
    return errors


def validate_required_snippets(path: Path, text: str, snippets: list[str]) -> list[str]:
    normalized = squash_ws(text)
    return [f"{rel(path)}: missing required setup text {snippet!r}" for snippet in snippets if squash_ws(snippet) not in normalized]


def validate_markdown_commands(path: Path, text: str) -> list[str]:
    errors: list[str] = []
    doctor_blocks = command_blocks(text, "cache_router_setup_doctor.py")
    if path == SETUP_DOC and not doctor_blocks:
        errors.append(f"{rel(path)}: missing setup doctor command block")
    for block in doctor_blocks:
        if "--live" in block:
            errors.append(f"{rel(path)}: offline setup doctor command block must not include --live")
        uses_deployment_inventory = "--workers-file configs/cache-router/<deployment>.workers.json" in block
        uses_public_example_check = "--workers-file configs/cache-router/workers.example.json" in block and "--json" in block
        if not uses_deployment_inventory and not uses_public_example_check:
            errors.append(f"{rel(path)}: setup doctor command should use configs/cache-router/<deployment>.workers.json")

    runtime_blocks = command_blocks(text, "cache_router_remote_stack.py")
    for block in runtime_blocks:
        if "--workers-file" in block and "configs/cache-router/<deployment>.workers.json" not in block:
            errors.append(f"{rel(path)}: runtime command should use the placeholder deployment inventory")
        starts_runtime = any(
            command in block
            for command in [
                "cache_router_remote_stack.py start-workers",
                "cache_router_remote_stack.py start-worker",
                "cache_router_remote_stack.py restart-router",
            ]
        )
        authenticated_router = "--router-auth" in block or "--production-router-mode" in block
        if starts_runtime and "0.0.0.0" in block and "--allow-unauthenticated-lan" not in block and not authenticated_router:
            errors.append(f"{rel(path)}: unauthenticated 0.0.0.0 runtime command must include --allow-unauthenticated-lan")
        if starts_runtime and "--remote-host <" not in block and "start-workers" not in block:
            errors.append(f"{rel(path)}: remote runtime command should use placeholder remote-host values")
    return errors


def validate_no_private_coordinates(path: Path, text: str) -> list[str]:
    errors: list[str] = []
    for match in PRIVATE_COORDINATE_RE.finditer(text):
        errors.append(f"{rel(path)}: private LAN address should be a placeholder, found {match.group(0)!r}")
    lowered = text.lower()
    for hint in sorted(PRIVATE_HOST_HINTS):
        if hint.lower() in lowered:
            errors.append(f"{rel(path)}: private or secret-looking token {hint!r} should not appear in setup docs")
    return errors


def validate() -> dict[str, Any]:
    errors: list[str] = []
    setup_text = read(SETUP_DOC)
    readme_text = read(README)
    config_text = read(CONFIG_README)

    errors.extend(validate_required_snippets(SETUP_DOC, setup_text, REQUIRED_SETUP_SNIPPETS))
    errors.extend(validate_required_snippets(README, readme_text, REQUIRED_README_SNIPPETS))
    errors.extend(validate_required_snippets(CONFIG_README, config_text, REQUIRED_CONFIG_SNIPPETS))
    for path, text in [(SETUP_DOC, setup_text), (README, readme_text), (CONFIG_README, config_text)]:
        errors.extend(validate_markdown_commands(path, text))
        errors.extend(validate_no_private_coordinates(path, text))

    summary, doctor_errors = run_setup_doctor()
    errors.extend(doctor_errors)
    if summary is not None:
        errors.extend(validate_setup_doctor_summary(summary))
    matrix_summary, matrix_errors = run_setup_doctor_matrix()
    errors.extend(matrix_errors)
    if matrix_summary is not None:
        errors.extend(validate_setup_doctor_matrix_summary(matrix_summary))
    errors.extend(validate_workers_example())
    errors.extend(validate_gitignore_and_makefile())
    errors.extend(validate_runtime_guard_sources())

    return {
        "ok": not errors,
        "errors": errors,
        "setup_doc": rel(SETUP_DOC),
        "readme": rel(README),
        "config_readme": rel(CONFIG_README),
        "workers_file": rel(WORKERS_EXAMPLE),
        "offline_doctor_ok": bool(summary and summary.get("ok") is True and summary.get("failures") == 0),
        "matrix_ok": bool(matrix_summary and matrix_summary.get("ok") is True),
        "worker_count": int(summary.get("worker_count") or 0) if isinstance(summary, dict) else 0,
        "runtime_command_blocks": len(command_blocks(setup_text, "cache_router_remote_stack.py"))
        + len(command_blocks(readme_text, "cache_router_remote_stack.py")),
    }


def main() -> int:
    result = validate()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
