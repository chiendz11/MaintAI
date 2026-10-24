"""Entrypoint. verify-config is offline; serve requires real Kafka/S3 configuration."""

import argparse
import json
import logging
import signal
import sys
import threading

from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.errors import ConfigurationError

from . import __version__
from .settings import Settings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "verify-config", help="Validate configuration/contracts without network I/O"
    )
    commands.add_parser("serve", help="Run all archive groups and management HTTP in one process")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        registry = ContractRegistry()
        if args.command == "verify-config":
            print(
                json.dumps(
                    {
                        "service": "storage-consumer",
                        "version": __version__,
                        "roles": settings.roles,
                        "contract_fixtures": registry.check_examples(),
                        "microbatch_records": 1,
                        "bucket_configured": True,
                        "custom_s3_endpoint": settings.endpoint_url is not None,
                        "network_checked": False,
                    }
                )
            )
            return 0
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
        logger = logging.getLogger("maintai.storage")
        logger.setLevel(logging.INFO)
        stop = threading.Event()

        def stopping(*_args):
            stop.set()

        previous = {s: signal.signal(s, stopping) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            from .runtime import Runtime

            return Runtime(settings, contracts=registry).run(stop)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    except Exception as exc:
        # No provider exception text/traceback, raw data, config or credential values.
        failure = {"event": "storage_command_failed", "error_type": type(exc).__name__}
        if isinstance(exc, ConfigurationError):
            failure["reason"] = str(exc)  # Our validators use fixed text, never input values.
        print(json.dumps(failure), file=sys.stderr)
        return 1
