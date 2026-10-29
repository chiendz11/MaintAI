"""Business REST/SSE runtime, owner-local projections and Processing HTTP facade."""

import argparse
import logging
import sys

import httpx
import psycopg
import uvicorn
from backend.shared.alerts.repository import AlertRepository
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from business_processing_client import ProcessingStatusClient
from business_sse.hub import NotificationHub
from psycopg.rows import dict_row

from .app import create_app
from .auth import IdentityVerifier, jwks_fetcher
from .config import Settings


def main(argv=None):
    parser = argparse.ArgumentParser(description="Business REST/SSE snapshots and event status API")
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
        status_client = None
        if settings.processing_read_url:
            status_client = ProcessingStatusClient(
                httpx.AsyncClient(trust_env=False, follow_redirects=False),
                base_url=settings.processing_read_url,
                token_file=settings.processing_token_file,
            )
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        alerts = AlertRepository(repository.connection_factory, channel=settings.sse.alert_channel)
        projections = ProjectionRepository(
            repository.connection_factory, alert_reader=alerts.active
        )
        uvicorn.run(
            create_app(
                repository,
                verifier,
                status_client=status_client,
                projections=projections,
                alerts=alerts,
                notifications=NotificationHub(
                    projections,
                    dsn=settings.database_url,
                    settings=settings.sse,
                    alert_repository=alerts,
                ),
            ),
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
