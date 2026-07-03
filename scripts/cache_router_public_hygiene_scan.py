#!/usr/bin/env python3
"""Scan the public Cachy Router tree for private artifacts and secrets.

The public tree is the set of tracked files plus unignored new files. Ignored
operator-local inventories can exist in a working copy, but they must not be
publishable by default.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


sys.dont_write_bytecode = True

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_PATH_PARTS = {
    "build",
    "cache_blobs",
    "dist",
    "full_prompts",
    "full_transcripts",
    "logs",
    "private_workspaces",
    "raw_cache",
    "raw_logs",
    "raw_prompts",
    "raw_requests",
    "raw_responses",
    "raw_slots",
    "raw_workspaces",
    "runtime",
    "slot_files",
    "ssd_cache",
    "transcripts",
}
FORBIDDEN_BASENAME_GLOBS = [
    ".env",
    ".env.*",
    "*.env",
    "*.key",
    "*.pem",
    "auth-token.txt",
    "auth.json",
    "credentials.json",
    "provider_config.json",
    "secrets.*",
    "token.json",
]
PRIVATE_INVENTORY_GLOBS = [
    "*.local.json",
    "local*.json",
    "*.workers.json",
]
FORBIDDEN_SUFFIXES = {
    ".7z",
    ".bin",
    ".ckpt",
    ".gguf",
    ".gz",
    ".onnx",
    ".pt",
    ".pth",
    ".safetensors",
    ".slot",
    ".tar",
    ".tgz",
    ".zip",
}
SAFE_SUFFIX_EXCEPTIONS = {
    ".py",
    ".md",
    ".json",
    ".jsonl",
    ".schema",
    ".txt",
}
CONTENT_PATTERNS = [
    ("private_worker_hostname", re.compile(r"\b(?:private|internal|operator)-(?:host|worker)-\d+\b")),
    ("private_username_path", re.compile(r"/home/(?!<user>)[A-Za-z0-9._-]+/")),
    ("private_user_path", re.compile(r"/Users/(?!<user>)[A-Za-z0-9._-]+/")),
    ("private_lan_ip", re.compile(r"\b(?:10|192\.168|172\.(?:1[6-9]|2\d|3[0-1]))(?:\.\d{1,3}){2}\b")),
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("huggingface_token", re.compile(r"\bhf_[A-Za-z0-9]{20,}\b")),
    ("github_token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b")),
    ("github_fine_grained_token", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{40,}\b")),
    ("anthropic_api_key", re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("basic_auth_url", re.compile(r"https?://[^/\s:@]+:[^@\s/]+@")),
    ("authorization_bearer_value", re.compile(r"\bauthorization\s*[:=]\s*bearer\s+(?!<|\$|secret-token\b)[A-Za-z0-9._~+/=-]{20,}", re.I)),
    ("private_key", re.compile(r"BEGIN (?:OPENSSH|RSA|DSA|EC|PRIVATE) PRIVATE KEY")),
]
ASSIGNMENT_RE = re.compile(
    r"""(?ix)
    \b(?P<key>
        api[_-]?key|
        access[_-]?token|
        auth[_-]?token|
        password|
        secret|
        credential|
        authorization
    )\b
    \s*[:=]\s*
    (?P<quote>["']?)
    (?P<value>[^"',\s#}\]]+)
    """,
)

SAFE_PLACEHOLDER_FRAGMENTS = {
    "<router-lan-ip>",
    "<worker-a-lan-ip>",
    "<worker-b-lan-ip>",
    "<worker-lan-ip>",
    "<worker-a-ssh-alias>",
    "<worker-b-ssh-alias>",
    "<router-ssh-alias>",
    "<user>",
    "secret-token",
    "OPENAI_KEY_PLACEHOLDER",
}
SAFE_ASSIGNMENT_VALUES = {
    "",
    "None",
    "null",
    "false",
    "true",
    "secret-token",
    "<token>",
    "<router-token>",
    "<api-key>",
    "<auth-token>",
    "<secret>",
    "${TOKEN}",
    "$TOKEN",
}
MAX_EVIDENCE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class PublicFile:
    path: str
    text: str


@dataclass(frozen=True)
class Finding:
    path: str
    line: int | None
    code: str
    message: str

    def as_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "path": self.path,
            "code": self.code,
            "message": self.message,
        }
        if self.line is not None:
            row["line"] = self.line
        return row


def rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(PACKAGE_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def is_text_file(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix in SAFE_SUFFIX_EXCEPTIONS:
        return True
    return suffix == "" or suffix in {".example", ".gitignore"}


def git_public_paths(root: Path) -> list[str]:
    cmd = ["git", "ls-files", "--cached", "--others", "--exclude-standard"]
    proc = subprocess.run(cmd, cwd=root, text=True, capture_output=True, check=True)
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip())


def git_tracked_ignored_paths(root: Path) -> list[str]:
    cmd = ["git", "ls-files", "-ci", "--exclude-standard"]
    proc = subprocess.run(cmd, cwd=root, text=True, capture_output=True, check=True)
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip())


def fallback_public_paths(root: Path) -> list[str]:
    paths: list[str] = []
    ignored_parts = {".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".cache", ".venv", "venv"}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError:
            continue
        if any(part in ignored_parts for part in Path(relative).parts):
            continue
        paths.append(relative)
    return sorted(paths)


def public_paths(root: Path) -> list[str]:
    try:
        return git_public_paths(root)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return fallback_public_paths(root)


def load_public_files(root: Path) -> tuple[list[PublicFile], list[Finding]]:
    files: list[PublicFile] = []
    findings: list[Finding] = []
    for relative in public_paths(root):
        path = root / relative
        path_findings = scan_path(relative)
        findings.extend(path_findings)
        if path_findings:
            continue
        if relative.startswith("evidence/") and path.stat().st_size > MAX_EVIDENCE_BYTES:
            findings.append(Finding(relative, None, "oversized_evidence_file", f"public evidence file exceeds {MAX_EVIDENCE_BYTES} bytes"))
            continue
        if relative.startswith("evidence/") and not is_text_file(path):
            findings.append(Finding(relative, None, "binary_evidence_file", "public evidence files must be small UTF-8 text summaries"))
            continue
        if not path.is_file() or not is_text_file(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            findings.append(Finding(relative, None, "binary_public_file", "public file is not UTF-8 text"))
            continue
        files.append(PublicFile(relative, text))
    return files, findings


def scan_path(relative: str) -> list[Finding]:
    findings: list[Finding] = []
    parts = Path(relative).parts
    basename = Path(relative).name
    if any(part in FORBIDDEN_PATH_PARTS for part in parts):
        findings.append(Finding(relative, None, "forbidden_artifact_directory", "public path is inside a raw/private artifact directory"))
    for pattern in FORBIDDEN_BASENAME_GLOBS:
        if fnmatch.fnmatch(basename, pattern):
            findings.append(Finding(relative, None, "forbidden_artifact_filename", f"public filename matches {pattern}"))
    suffix = Path(relative).suffix.lower()
    if suffix in FORBIDDEN_SUFFIXES:
        findings.append(Finding(relative, None, "forbidden_artifact_suffix", f"public file suffix {suffix} is reserved for private artifacts or archives"))
    if relative.startswith("configs/cache-router/") and any(fnmatch.fnmatch(basename, pattern) for pattern in PRIVATE_INVENTORY_GLOBS):
        findings.append(Finding(relative, None, "private_inventory_filename", "deployment inventories must stay local and ignored"))
    return findings


def line_allowed(line: str) -> bool:
    return any(fragment in line for fragment in SAFE_PLACEHOLDER_FRAGMENTS)


def scan_content(public_file: PublicFile) -> list[Finding]:
    findings: list[Finding] = []
    for lineno, line in enumerate(public_file.text.splitlines(), start=1):
        if line_allowed(line):
            continue
        for code, pattern in CONTENT_PATTERNS:
            if pattern.search(line):
                findings.append(Finding(public_file.path, lineno, code, "public text contains private topology, local user path, or secret-looking material"))
        assignment = ASSIGNMENT_RE.search(line)
        if assignment:
            value = assignment.group("value").strip().strip("\"'")
            runtime_expression = any(char in value for char in "()[]{}") or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?", value)
            if not runtime_expression and value not in SAFE_ASSIGNMENT_VALUES and not value.startswith("<") and len(value) >= 12:
                findings.append(Finding(public_file.path, lineno, "secret_assignment_value", "public text assigns a non-placeholder secret-like value"))
    return findings


def scan(root: Path) -> tuple[dict[str, Any], list[Finding]]:
    files, findings = load_public_files(root)
    try:
        tracked_ignored = git_tracked_ignored_paths(root)
    except (subprocess.CalledProcessError, FileNotFoundError):
        tracked_ignored = []
    for relative in tracked_ignored:
        findings.append(Finding(relative, None, "tracked_ignored_file", "file is tracked even though it matches ignore rules"))
    for public_file in files:
        findings.extend(scan_content(public_file))
    summary = {
        "ok": not findings,
        "root": rel(root),
        "public_paths": len(public_paths(root)),
        "text_files_scanned": len(files),
        "findings": len(findings),
    }
    return summary, findings


def self_test() -> list[str]:
    errors: list[str] = []
    bad_samples = [
        PublicFile("README.md", "worker http://" + "10." + "0.0.21:18082\n"),
        PublicFile("README.md", "worker internal-" + "host-2\n"),
        PublicFile("README.md", "model lives at /home/" + "privateuser/models/model.gguf\n"),
        PublicFile("README.md", "token sk-" + "a" * 32 + "\n"),
        PublicFile("README.md", "api_key=actual-" + "secret-value-123\n"),
        PublicFile("README.md", "-----BEGIN OPENSSH " + "PRIVATE KEY-----\n"),
    ]
    for sample in bad_samples:
        if not scan_content(sample):
            errors.append(f"self-test failed to reject {sample.text.strip()!r}")
    if not scan_path("configs/cache-router/demo.workers.json"):
        errors.append("self-test failed to reject deployment inventory filename")
    if not scan_path("evidence/raw_logs/run.log"):
        errors.append("self-test failed to reject raw artifact path")
    good_samples = [
        PublicFile("configs/cache-router/workers.example.json", '"worker_url": "http://<worker-a-lan-ip>:18082"\n'),
        PublicFile("scripts/cache_router_daemon_smoke_test.py", 'auth_token="secret-token"\n'),
        PublicFile("docs/architecture/examples/negative/cache-router-negative-fixtures.jsonl", "OPENAI_KEY_PLACEHOLDER\n"),
    ]
    for sample in good_samples:
        findings = scan_content(sample) + scan_path(sample.path)
        if findings:
            errors.append(f"self-test rejected safe placeholder sample {sample.path}: {[row.as_dict() for row in findings]}")
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(PACKAGE_ROOT), help="Repository root to scan.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable output.")
    parser.add_argument("--self-test", action="store_true", help="Run built-in detector tests before scanning.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors = self_test() if args.self_test else []
    summary, findings = scan(Path(args.root).resolve())
    output = {
        **summary,
        "self_test_ok": not errors,
        "self_test_errors": errors,
        "findings_detail": [finding.as_dict() for finding in findings],
    }
    ok = summary["ok"] and not errors
    if args.json:
        print(json.dumps({**output, "ok": ok}, indent=2, sort_keys=True))
    else:
        print(f"public hygiene scan: {'ok' if ok else 'fail'}")
        print(f"public_paths: {summary['public_paths']}")
        print(f"text_files_scanned: {summary['text_files_scanned']}")
        for error in errors:
            print(f"self-test error: {error}", file=sys.stderr)
        for finding in findings:
            location = finding.path if finding.line is None else f"{finding.path}:{finding.line}"
            print(f"error: {location}: {finding.code}: {finding.message}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
