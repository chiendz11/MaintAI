#!/usr/bin/env python3
"""Standalone entrypoint for GitHub Actions `promote-shadow.yml`."""

from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from maintai_automation.__main__ import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(["promote-shadow", *sys.argv[1:]]))
