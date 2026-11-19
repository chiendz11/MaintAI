"""CLI entrypoint for standardized batch training (`python -m maintai_training`)."""

from __future__ import annotations

import argparse
from pathlib import Path

from maintai_training.runner import run_training


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MaintAI approved dataset batch trainer")
    parser.add_argument(
        "--context", required=True, type=Path, help="Prepared training context JSON"
    )
    parser.add_argument("--output", required=True, type=Path, help="Output model-result JSON path")
    args = parser.parse_args(argv)
    result = run_training(args.context, args.output)
    print(
        f"Trained {result['model_version']} on {result['dataset_version']} "
        f"(recall={result['validation_metrics']['recall_failure']:.4f}, "
        f"precision={result['validation_metrics']['precision_failure']:.4f})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
