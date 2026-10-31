#!/usr/bin/env python3
"""Update image tags in MaintAI-gitops for Cloud Production deployment."""

import argparse
import sys
from pathlib import Path

import yaml


def update_gitops_images(
    gitops_dir: Path,
    registry: str,
    tag: str,
    services: list[str] | None = None,
) -> bool:
    """Update container image overrides in environments/local/apps/kustomization.yaml."""
    kustomize_path = gitops_dir / "environments/local/apps/kustomization.yaml"
    if not kustomize_path.is_file():
        print(f"Error: {kustomize_path} does not exist.", file=sys.stderr)
        return False

    with open(kustomize_path) as f:
        data = yaml.safe_load(f)

    changed = False
    images = data.get("images", [])
    for entry in images:
        name = entry.get("name", "").split("/")[-1]
        if not name or name == "dev-tools":
            continue
        if services and name not in services:
            continue

        target_name = f"{registry}/maintai-{name}"
        if entry.get("newName") != target_name or entry.get("newTag") != tag:
            entry["newName"] = target_name
            entry["newTag"] = tag
            changed = True
            print(f"Updated {name} -> {target_name}:{tag}")

    if changed:
        with open(kustomize_path, "w") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        print(f"Saved changes to {kustomize_path}")
    else:
        print("No image tag changes needed.")

    return changed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update image tags in MaintAI-gitops kustomization.yaml"
    )
    parser.add_argument(
        "--gitops-dir",
        type=Path,
        required=True,
        help="Path to MaintAI-gitops repository root",
    )
    parser.add_argument(
        "--registry",
        default="ghcr.io/chiendz11",
        help="Container registry prefix (default: ghcr.io/chiendz11)",
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="Image tag to apply (e.g. sha-2b34c16 or latest)",
    )
    parser.add_argument(
        "--services",
        nargs="*",
        help="Optional specific services to update",
    )
    args = parser.parse_args()

    success = update_gitops_images(
        gitops_dir=args.gitops_dir,
        registry=args.registry.rstrip("/"),
        tag=args.tag,
        services=args.services,
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
