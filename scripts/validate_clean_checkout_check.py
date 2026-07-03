#!/usr/bin/env python3
"""Prove `make check` from a clean committed checkout.

This is a release proof, not a normal development check. It refuses to run from
a dirty worktree, creates a temporary detached worktree at HEAD, runs
`make check` there, and removes the temporary worktree afterward. It does not
contact remote hosts or write artifacts into the repository.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def run(cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)


def repo_root() -> Path:
    result = run(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd())
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "not a git repository")
    return Path(result.stdout.strip())


def dirty_rows(root: Path) -> list[str]:
    result = run(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=root)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "git status failed")
    return [line for line in result.stdout.splitlines() if line.strip()]


def prove_clean_checkout(root: Path, *, make_target: str) -> dict[str, Any]:
    dirty = dirty_rows(root)
    if dirty:
        return {
            "ok": False,
            "reason": "worktree_dirty",
            "dirty_entries": len(dirty),
            "dirty_preview": dirty[:20],
            "scope": "clean-checkout proof requires all intended files to be tracked and committed",
        }

    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    temp_parent = Path(tempfile.mkdtemp(prefix="cache-router-clean-checkout-"))
    worktree = temp_parent / "Cachy-Router"
    add_result = run(["git", "worktree", "add", "--detach", str(worktree), "HEAD"], cwd=root)
    remove_result: subprocess.CompletedProcess[str] | None = None
    check_result: subprocess.CompletedProcess[str] | None = None
    try:
        if add_result.returncode != 0:
            return {
                "ok": False,
                "reason": "worktree_add_failed",
                "stderr": add_result.stderr[-4000:],
                "scope": "temporary local worktree proof",
            }
        check_result = run(["make", make_target], cwd=worktree, env=env)
        return {
            "ok": check_result.returncode == 0,
            "reason": "make_check_passed" if check_result.returncode == 0 else "make_check_failed",
            "make_target": make_target,
            "stdout_tail": check_result.stdout[-4000:],
            "stderr_tail": check_result.stderr[-4000:],
            "scope": "temporary local worktree proof at committed HEAD; no remote hosts contacted",
        }
    finally:
        if worktree.exists():
            remove_result = run(["git", "worktree", "remove", "--force", str(worktree)], cwd=root)
            if remove_result.returncode != 0:
                print(remove_result.stderr[-4000:], file=sys.stderr)
        try:
            temp_parent.rmdir()
        except OSError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--make-target", default="check", help="Make target to run in the temporary checkout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root()
    result = prove_clean_checkout(root, make_target=args.make_target)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"clean-checkout proof: {'ok' if result['ok'] else 'fail'}")
        print(f"reason: {result.get('reason')}")
        if result.get("dirty_entries"):
            print(f"dirty_entries: {result['dirty_entries']}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
