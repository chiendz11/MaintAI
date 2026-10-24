"""Explicit environment-configured process entrypoint."""

import os
import sys

import uvicorn

from .app import create_app
from .config import AppSettings
from .observability import configure_logging


def main():
    try:
        settings = AppSettings.from_env(os.environ)
    except ValueError:
        print("Invalid ingestion configuration", file=sys.stderr)
        raise SystemExit(2) from None
    configure_logging()
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        access_log=False,
        timeout_graceful_shutdown=int(
            settings.body_timeout + settings.kafka.delivery_timeout_ms / 1000 + 10
        ),
    )


if __name__ == "__main__":
    main()
