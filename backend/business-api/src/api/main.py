"""Business registry-only runtime; no consumer/projection implementation is implied."""

import argparse
import logging
import sys

import psycopg
import uvicorn
from backend.shared.persistence.registry import RegistryRepository
from psycopg.rows import dict_row

from .app import create_app
from .auth import IdentityVerifier, jwks_fetcher
from .config import Settings


def main(argv=None):
    parser = argparse.ArgumentParser(description="Business API M2.R registry lookup only")
    parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        repository = RegistryRepository(
            lambda: psycopg.connect(
                settings.database_url,
                row_factory=dict_row,
                connect_timeout=2,
                options="-c statement_timeout=3000",
            )
        )
        verifier = IdentityVerifier(
            issuer=settings.issuer,
            audience=settings.audience,
            roles_claim=settings.roles_claim,
            machines_claim=settings.machines_claim,
            fetch_jwks=jwks_fetcher(settings.jwks_url),
        )
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        uvicorn.run(
            create_app(repository, verifier),
            host=settings.host,
            port=settings.port,
            access_log=False,
            log_config=None,
            timeout_graceful_shutdown=10,
        )
        return 0
    except Exception as exc:
        print(f"Business registry failed: {type(exc).__name__}", file=sys.stderr)
        return 1
