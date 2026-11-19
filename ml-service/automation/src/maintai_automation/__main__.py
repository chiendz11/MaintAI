"""Unified CLI entrypoint for `python -m maintai_automation`."""

from __future__ import annotations

import argparse
from pathlib import Path

from maintai_automation import (
    fetch_artifacts,
    normalize,
    prepare,
    promote,
    publish,
    save_result,
)


def _register_release_failed(subparsers) -> None:
    parser = subparsers.add_parser("release-failed", help="Argo Workflow onExit handler")
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--status", default="")


def _run_release_failed(args: argparse.Namespace) -> int:
    if args.status == "Succeeded":
        return 0
    print(
        f"Workflow {args.owner} exited with status={args.status or 'Failed'} "
        f"(workspace={args.directory}); no Champion or shadow mutation performed."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MaintAI MLOps automation and MinIO artifact toolchain"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    normalize.register_parser(subparsers)
    prepare.register_parser(subparsers)
    save_result.register_parser(subparsers)
    publish.register_parser(subparsers)
    fetch_artifacts.register_parser(subparsers)
    promote.register_parser(subparsers)
    _register_release_failed(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    dispatch = {
        "normalize": normalize.run_cli,
        "prepare": prepare.run_cli,
        "save-result": save_result.run_cli,
        "publish": publish.run_cli,
        "fetch-artifacts": fetch_artifacts.run_cli,
        "promote-shadow": promote.run_cli,
        "release-failed": _run_release_failed,
    }
    return dispatch[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
