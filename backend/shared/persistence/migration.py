"""Explicit owner migrations; API startup never creates or upgrades schema."""

import argparse
import os
import re
import sys
from importlib.resources import files

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


def upgrade(dsn, *, schema=None):
    url = make_url(dsn)
    if url.get_backend_name() not in {"postgres", "postgresql"}:
        raise ValueError("Business migrations require PostgreSQL")
    args = {}
    if schema is not None:
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]{0,62}", schema):
            raise ValueError("Invalid explicit owner schema")
        args["options"] = "-csearch_path=" + schema
    engine = create_engine(url.set(drivername="postgresql+psycopg"), connect_args=args)
    config = Config()
    config.set_main_option("script_location", str(files("backend.migrations")))
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Explicit Business owner migration job")
    parser.add_argument("operation", choices=["upgrade"])
    parser.parse_args(argv)
    try:
        dsn = os.environ.get("BUSINESS_DATABASE_URL")
        if not dsn:
            raise ValueError("BUSINESS_DATABASE_URL is required")
        upgrade(dsn)
        print("Business registry migration applied")
        return 0
    except Exception as exc:
        print(f"Business migration failed: {type(exc).__name__}", file=sys.stderr)
        return 1
