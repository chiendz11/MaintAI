"""Internal SELECT-only status server; migrations and credentials are explicit."""

import argparse
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description="Processing internal event status API")
    parser.parse_args(argv)
    # Keep --help usable in transport-only SDK environments.
    import psycopg
    import uvicorn
    from processing.shared.persistence.status import StatusRepository
    from psycopg.rows import dict_row

    from .app import create_app
    from .auth import IdentityVerifier, jwks_fetcher
    from .config import Settings

    try:
        settings = Settings.from_env()
        repository = StatusRepository(
            lambda: psycopg.connect(
                settings.database_url,
                row_factory=dict_row,
                connect_timeout=2,
                options="-c statement_timeout=3000 -c default_transaction_read_only=on",
            )
        )
        verifier = IdentityVerifier(
            issuer=settings.issuer,
            audience=settings.audience,
            roles_claim=settings.roles_claim,
            machines_claim=settings.machines_claim,
            fetch_jwks=jwks_fetcher(settings.jwks_url),
        )
        uvicorn.run(
            create_app(repository, verifier),
            host=settings.host,
            port=settings.port,
            access_log=False,
            timeout_graceful_shutdown=10,
        )
        return 0
    except Exception as exc:
        print(f"Processing Read API failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
