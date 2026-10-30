"""Internal SELECT-only status server; migrations and credentials are explicit."""

import argparse
import json
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description="Processing internal event status API")
    parser.add_argument("--retention-dry-run", action="store_true")
    parser.add_argument("--before", help="Exclusive timezone-aware UTC cutoff for dry-run")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args(argv)
    if args.retention_dry_run != bool(args.before):
        parser.error("--retention-dry-run and --before must be supplied together")
    # Keep --help usable in transport-only SDK environments.
    import uvicorn
    from processing.shared.history_meta.pool import Connections
    from processing.shared.persistence.status import StatusRepository

    from .app import create_app
    from .auth import IdentityVerifier, jwks_fetcher
    from .config import Settings
    from .history_runtime import build

    try:
        settings = Settings.from_env()
        source_pool = Connections(settings.database_url, readonly=True)
        repository = StatusRepository(source_pool)
        runtime = build(settings, source_pool) if settings.history else None
        if args.retention_dry_run:
            from processing.shared.history_meta.retention import dry_run

            try:
                if runtime is None:
                    raise ValueError("Dry-run requires explicit M6 metadata configuration")
                print(json.dumps(dry_run(runtime.history, before=args.before, limit=args.limit)))
            finally:
                if runtime is not None:
                    runtime.close()
                source_pool.close()
            return 0
        verifier = IdentityVerifier(
            issuer=settings.issuer,
            audience=settings.audience,
            roles_claim=settings.roles_claim,
            machines_claim=settings.machines_claim,
            fetch_jwks=jwks_fetcher(settings.jwks_url),
        )
        try:
            uvicorn.run(
                create_app(repository, verifier, history_runtime=runtime),
                host=settings.host,
                port=settings.port,
                access_log=False,
                timeout_graceful_shutdown=10,
            )
        finally:
            if runtime is not None:
                runtime.close()
            source_pool.close()
        return 0
    except Exception as exc:
        print(f"Processing Read API failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
