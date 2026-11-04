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
    commands.add_parser("validate-catalog", help="Validate packaged M6 contract examples offline")
    commands.add_parser("catalog-sync", help="Publish one bounded Kafka/raw catalog reconciliation")
    commands.add_parser(
        "catalog-rebuild", help="Explicitly restore catalog entries from raw manifests"
    )
    commands.add_parser(
        "catalog-inspect", help="Print current catalog identity/coverage, no raw payload"
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "validate-catalog":
            from importlib.resources import files

            from .catalog import sha256, validate

            resources = files("maintai_storage")
            manifest = json.loads(resources.joinpath("schemas/manifest.json").read_text())
            for name, expected in manifest["files"].items():
                if sha256(resources.joinpath(name).read_bytes()) != expected:
                    raise ConfigurationError("Packaged catalog contract checksum mismatch")
            examples = json.loads(resources.joinpath("examples/catalog.json").read_text())
            for kind, document in examples.items():
                validate(document, kind)
            print(
                json.dumps(
                    {
                        "catalog_schema_version": "1.0.0",
                        "validated": len(examples),
                        "network_checked": False,
                    }
                )
            )
            return 0
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
                        "catalog_enabled": settings.catalog_enabled,
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
            if args.command.startswith("catalog-"):
                from confluent_kafka.admin import AdminClient

                from .catalog import CatalogStore
                from .catalog_runtime import CatalogRuntime, KafkaAudit

                if settings.allow_unversioned or settings.archive_outcomes:
                    raise ConfigurationError("M6 catalog requires versioned raw-only configuration")
                s3 = settings.create_s3_client()
                try:
                    if s3.get_bucket_versioning(Bucket=settings.bucket).get("Status") != "Enabled":
                        raise ConfigurationError("M6 catalog requires bucket versioning")
                    if args.command == "catalog-inspect":
                        current, _ = CatalogStore(
                            s3,
                            settings.bucket,
                            raw_prefix=settings.prefix,
                            prefix=settings.catalog_prefix,
                        ).current()
                        if current is None:
                            raise FileNotFoundError("Catalog has not been published")
                        pointer, generation = current
                        print(
                            json.dumps(
                                {
                                    "generation_id": pointer["generation_id"],
                                    "partitions": generation["partitions"],
                                }
                            )
                        )
                    else:
                        audit = KafkaAudit(settings, AdminClient(settings.kafka.client_config()))
                        catalog = CatalogRuntime(settings, s3, audit)
                        try:
                            result = (
                                catalog.rebuild(stop)
                                if args.command == "catalog-rebuild"
                                else catalog.sync(stop)
                            )
                        finally:
                            audit.close()
                        print(json.dumps({"command": args.command, "result": result}))
                    return 0
                finally:
                    s3.close()
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
