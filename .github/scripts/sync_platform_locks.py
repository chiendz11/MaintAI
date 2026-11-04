#!/usr/bin/env python3
"""Sync application source commit SHA and file hashes to platform lock manifests.

Used by CI and local maintenance to ensure deterministic provenance across
MaintAI, MaintAI-gitops, and Maint-infrastructure.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


def get_git_sha(repo_dir: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def sync_app_source_lock(lock_path: Path, app_dir: Path, target_sha: str) -> int:
    if not lock_path.is_file():
        print(f"[WARN] Lock file not found: {lock_path}", file=sys.stderr)
        return 0

    data = json.loads(lock_path.read_text(encoding="utf-8"))
    old_sha = data.get("source_git_sha", "unknown")
    data["source_git_sha"] = target_sha
    data["captured_on"] = datetime.now(UTC).strftime("%Y-%m-%d")

    updated_count = 0
    missing_count = 0
    files_map = data.get("files", {})

    for rel_path, old_hash in files_map.items():
        target_file = app_dir / rel_path
        if not target_file.is_file():
            print(f"  [MISSING] {rel_path} does not exist in app repo")
            missing_count += 1
            continue
        new_hash = hashlib.sha256(target_file.read_bytes()).hexdigest()
        if new_hash != old_hash:
            files_map[rel_path] = new_hash
            updated_count += 1

    lock_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(
        f"[OK] {lock_path.name}: SHA updated {old_sha[:7]} -> {target_sha[:7]} "
        f"({updated_count} files changed, {missing_count} missing)"
    )
    return updated_count


def sync_risk_policy_lock(lock_path: Path, app_dir: Path, target_sha: str) -> None:
    if not lock_path.is_file():
        return

    data = json.loads(lock_path.read_text(encoding="utf-8"))
    old_sha = data.get("source_git_sha", "unknown")
    data["source_git_sha"] = target_sha

    rel_path = data.get("path")
    if rel_path:
        target_file = app_dir / rel_path
        if target_file.is_file():
            data["sha256"] = hashlib.sha256(target_file.read_bytes()).hexdigest()

    lock_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"[OK] {lock_path.name}: SHA updated {old_sha[:7]} -> {target_sha[:7]}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync app commit SHA and file hashes to platform lock files."
    )
    parser.add_argument(
        "--app-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Path to MaintAI repo root (default: repo root containing this script)",
    )
    parser.add_argument(
        "--gitops-dir",
        type=Path,
        default=None,
        help="Path to MaintAI-gitops repo root",
    )
    parser.add_argument(
        "--infra-dir",
        type=Path,
        default=None,
        help="Path to Maint-infrastructure repo root",
    )
    parser.add_argument(
        "--sha",
        type=str,
        default=None,
        help="Target commit SHA (default: git rev-parse HEAD of app-dir)",
    )

    args = parser.parse_args()
    app_dir = args.app_dir.resolve()
    target_sha = args.sha or get_git_sha(app_dir)

    print(f"Syncing platform locks to MaintAI commit: {target_sha}")

    if args.gitops_dir:
        gitops_dir = args.gitops_dir.resolve()
        sync_app_source_lock(gitops_dir / "config/local/app-source.lock.json", app_dir, target_sha)
        sync_risk_policy_lock(
            gitops_dir / "config/local/risk-policy.lock.json", app_dir, target_sha
        )

    if args.infra_dir:
        infra_dir = args.infra_dir.resolve()
        sync_app_source_lock(infra_dir / "local/versions/app-source.lock.json", app_dir, target_sha)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
