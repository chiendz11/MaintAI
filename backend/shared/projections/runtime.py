"""Build recovery dependencies with its own owner connection factory and IO budgets."""

import boto3
from botocore.config import Config
from maintai_messaging.postgres import connect
from maintai_storage.catalog import CatalogStore

from backend.shared.projections.rebuild import RebuildRepository
from backend.shared.projections.runner import RebuildRunner
from backend.shared.projections.sources import OutcomeSource, RawSource


def build_runner(settings, stop):
    config = settings.rebuild

    def factory():
        return connect(config.database_url)

    repository = RebuildRepository(factory)
    s3 = boto3.client(
        "s3",
        region_name=config.region,
        endpoint_url=config.endpoint,
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}),
    )
    return RebuildRunner(
        repository,
        RawSource(repository, CatalogStore(s3, config.bucket)),
        OutcomeSource(settings.processing_read_url, settings.processing_token_file),
        stop,
    )
