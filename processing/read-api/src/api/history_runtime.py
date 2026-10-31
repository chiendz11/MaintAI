"""Construct M6 inside Read API with separate bounded credential pools."""

import base64
from pathlib import Path

import boto3
from botocore.config import Config
from maintai_messaging.contracts import strict_json
from processing.shared.history_meta.catalog import Catalog
from processing.shared.history_meta.charts import Charts
from processing.shared.history_meta.exports import Exports
from processing.shared.history_meta.link import ProcessingLink
from processing.shared.history_meta.persistence import Metadata, check_source_permissions
from processing.shared.history_meta.pool import Connections
from processing.shared.history_meta.schemas import Schemas
from processing.shared.history_meta.security import CursorKeys, DelegationVerifier
from processing.shared.history_meta.snapshots import History
from processing.shared.history_meta.storage import Objects

from .auth import jwks_fetcher
from .history_api import HistoryRuntime


def cursor_keys(path):
    raw = Path(path).read_bytes()
    if len(raw) > 16384:
        raise ValueError("Cursor key file exceeds size bound")
    try:
        value = strict_json(raw)
        if set(value) != {"active", "keys"} or not isinstance(value["keys"], dict):
            raise ValueError("Invalid key document")
        decoded = {
            key: base64.b64decode(secret, validate=True) for key, secret in value["keys"].items()
        }
        return CursorKeys(decoded, active=value["active"])
    except Exception:
        raise ValueError("Invalid explicit cursor key document") from None


def build(settings, source_pool):
    config = settings.history
    metadata_pool = Connections(config.metadata_url, readonly=False, size=4)
    job_pool = Connections(config.metadata_url, readonly=False, size=2)
    catalog_pool = Connections(config.metadata_url, readonly=False, size=2)
    resources = [source_pool, metadata_pool, job_pool, catalog_pool]
    try:
        schemas = Schemas()
        keys = cursor_keys(config.cursor_key_file)
        meta = Metadata(metadata_pool, config.meta_schema)
        with source_pool() as connection:
            source_schema = check_source_permissions(connection)
        with metadata_pool() as connection:
            meta.check_permissions(connection, source_schema=source_schema)
        objects = None
        if config.raw_bucket:
            client = boto3.client(
                "s3",
                endpoint_url=config.s3_endpoint,
                region_name=config.s3_region,
                config=Config(
                    connect_timeout=2,
                    read_timeout=3,
                    retries={"total_max_attempts": 1},
                    s3={"addressing_style": "path"},
                ),
            )
            resources.append(client)
            objects = Objects(
                client, raw_bucket=config.raw_bucket, export_bucket=config.export_bucket
            )
        catalog = (
            Catalog(
                Metadata(catalog_pool, config.meta_schema),
                objects,
                schemas,
                processing_link=ProcessingLink(source_pool),
            )
            if objects
            else None
        )
        history = History(source_pool, meta, keys, catalog=catalog)
        job_history = History(
            source_pool, Metadata(job_pool, config.meta_schema), keys, catalog=catalog
        )
        delegation = DelegationVerifier(
            issuer=config.delegation_issuer, fetch_jwks=jwks_fetcher(config.delegation_jwks_url)
        )
        runtime = HistoryRuntime(
            history,
            delegation,
            Exports(job_history, objects),
            Charts(history),
            schemas,
            resources=resources,
        )
        return runtime
    except Exception:
        for resource in resources:
            resource.close()
        raise
