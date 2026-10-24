"""Explicit reviewed file import; no HTTP provisioning endpoint or seed defaults."""

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .registry import RegistryRepository


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Provision Business registry from a reviewed JSON list"
    )
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args(argv)
    try:
        dsn = os.environ.get("BUSINESS_DATABASE_URL")
        if not dsn:
            raise ValueError("BUSINESS_DATABASE_URL is required")
        with args.manifest.open("rb") as source:
            raw = source.read(1048577)
        if len(raw) > 1048576:
            raise ValueError("Provisioning manifest exceeds one MiB")
        machines = json.loads(raw)
        repo = RegistryRepository(
            lambda: psycopg.connect(
                dsn, row_factory=dict_row, connect_timeout=2, options="-c statement_timeout=5000"
            )
        )
        print(json.dumps({"machines_processed": repo.provision(machines)}))
        return 0
    except Exception as exc:
        print(f"Business provision failed: {type(exc).__name__}", file=sys.stderr)
        return 1
