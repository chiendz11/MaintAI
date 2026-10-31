"""Native Storage deltas exercise Processing indexing against versioned MinIO."""

import copy
from datetime import UTC, datetime

import pytest
from maintai_messaging.archive import S3RecordArchive
from maintai_storage.catalog import CatalogPublisher, CatalogStore
from processing.shared.history_meta.catalog import Catalog
from processing.shared.history_meta.persistence import Metadata, metadata_schema
from processing.shared.history_meta.schemas import Schemas
from processing.shared.history_meta.security import HistoryError
from processing.worker.tests.test_worker_transaction import source

from . import test_history_objects as object_tests
from .test_history_objects import ROOT

objects = object_tests.objects


pytestmark = pytest.mark.integration


def provider(client, bucket):
    store = CatalogStore(client, bucket)
    now = datetime.now(UTC).isoformat()
    coverage = [
        dict(
            topic="sensor.readings",
            topic_id="native-topic",
            partition=p,
            start_offset=0,
            archived_next_offset=0,
            indexed_next_offset=0,
            captured_log_end_offset=0,
            captured_at=now,
            gaps=[],
            status="COMPLETE",
        )
        for p in range(12)
    ]
    return store, CatalogPublisher(store), coverage


def receipt(store, offset):
    event, record = source(offset=offset)
    S3RecordArchive(store.s3, store.bucket)(event, record)
    return store.receipt("sensor.readings", 0, offset)


def make_catalog(database, objects):
    return Catalog(
        Metadata(database.factory, metadata_schema(database.schema)),
        objects,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )


def query(generation):
    return dict(
        machine_id="M-001",
        **{"from": "2026-10-01T00:00:00Z", "to": "2026-10-31T00:00:00Z"},
        catalog_generation=generation,
    )


def publish(publisher, coverage, entries=(), cluster="native-cluster"):
    if entries:
        coverage[0].update(
            archived_next_offset=max(e["offset"] for e in entries) + 1,
            indexed_next_offset=max(e["offset"] for e in entries) + 1,
            captured_log_end_offset=max(e["offset"] for e in entries) + 1,
        )
    coverage[0]["captured_at"] = datetime.now(UTC).isoformat()
    return publisher.publish(list(entries), coverage, cluster)["generation_id"]


def test_native_delta_preserves_receipts_and_pinned_generation(migrated_database, objects):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    first = publish(publisher, coverage, [receipt(store, 1)])
    catalog = make_catalog(migrated_database, objects)
    assert catalog.sync() == first
    assert len(list(catalog.records(query(first)))) == 1
    second = publish(
        publisher, coverage, [receipt(store, 3)]
    )  # Kafka offsets need not be adjacent.
    assert catalog.sync() == second
    assert [r["kafka"]["offset"] for r in catalog.records(query(second))] == [3, 1]
    assert [r["kafka"]["offset"] for r in catalog.records(query(first))] == [1]
    assert "topic_id" not in catalog.coverage({})["partition_coverage"][0]


def test_native_large_chain_resumes_after_restart(migrated_database, objects):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    for _ in range(102):
        last = publish(publisher, coverage)
    catalog = make_catalog(migrated_database, objects)
    assert catalog.sync() is None
    with pytest.raises(HistoryError):
        catalog.coverage({})
    catalog = make_catalog(migrated_database, objects)
    for _ in range(20):
        if catalog.sync() == last:
            break
    else:
        pytest.fail("Native chain did not resume to completion")
    assert len(list(catalog.records(query(last)))) == 1


def test_native_duplicate_conflict_is_not_last_write_wins(migrated_database, objects):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    entry = receipt(store, 1)
    first = publish(publisher, coverage, [entry])
    catalog = make_catalog(migrated_database, objects)
    assert catalog.sync() == first
    second = publish(publisher, coverage, [entry])
    assert catalog.sync() == second
    assert len(list(catalog.records(query(second)))) == 1
    changed = copy.deepcopy(entry)
    changed["raw_object_ref"]["sha256"] = "0" * 64
    publish(publisher, coverage, [changed])
    with pytest.raises(HistoryError):
        catalog.sync()


def test_real_storage_runtime_feeds_processing_raw_history(migrated_database):
    import os
    import time

    from maintai_messaging.config import KafkaSettings
    from maintai_messaging.producer import KafkaPublisher
    from processing.tests.storage_provider import StorageProvider

    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:40092")
    settings = KafkaSettings(
        bootstrap_servers=bootstrap, security_protocol="PLAINTEXT", local_mode=True
    )
    provider = StorageProvider(bootstrap)
    try:
        with KafkaPublisher(settings, "ingestion") as publisher:
            event, _ = source()
            delivery = publisher.publish("sensor.readings", event.payload)
        provider.archive(event, delivery)
        catalog = make_catalog(migrated_database, provider.objects)
        for _ in range(30):
            generation = catalog.sync()
            if generation is not None:
                break
            time.sleep(0.1)
        else:
            pytest.fail("Native provider import did not finish")
        rows = list(catalog.records(query(generation)))
        assert any(row["event_id"] == event.message_id for row in rows)
        assert all(row["validation_status"] == "UNVERIFIED" for row in rows)
        assert catalog.coverage({})["catalog_generation"] == generation
    finally:
        provider.close()


def install_generation(store, document):
    from uuid import uuid4

    from maintai_messaging.contracts import canonical_json

    document["generation_id"] = uuid4().hex + uuid4().hex
    reference = store.immutable(
        "raw-catalog/v1/generations/" + document["generation_id"] + ".json", document
    )
    pointer = dict(
        schema_version="1.0.0", generation_id=document["generation_id"], generation_ref=reference
    )
    store.s3.put_object(
        Bucket=store.bucket, Key=store.prefix + "/current.json", Body=canonical_json(pointer)
    )
    return pointer


@pytest.mark.parametrize(
    "mutation", ["cluster", "topic", "start", "regression", "orphan", "duplicate_partition"]
)
def test_native_changed_source_or_broken_chain_fails_closed(migrated_database, objects, mutation):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    first = publish(publisher, coverage, [receipt(store, 1)])
    catalog = make_catalog(migrated_database, objects)
    catalog.sync()
    current, _ = store.current()
    doc = copy.deepcopy(current[1])
    doc["segments"] = []
    doc["parent_ref"] = current[0]["generation_ref"]
    if mutation == "cluster":
        doc["source_cluster_id"] = "different-cluster"
    elif mutation == "topic":
        doc["partitions"][0]["topic_id"] = "different-topic"
    elif mutation == "start":
        doc["partitions"][0]["start_offset"] = 1
    elif mutation == "regression":
        for field in ("archived_next_offset", "indexed_next_offset", "captured_log_end_offset"):
            doc["partitions"][0][field] = 0
    elif mutation == "orphan":
        doc["parent_ref"] = None
    else:
        doc["partitions"].append(copy.deepcopy(doc["partitions"][0]))
    install_generation(store, doc)
    with pytest.raises(HistoryError):
        catalog.sync()
    with pytest.raises(HistoryError):
        catalog.coverage({})
    # Already materialized/pinned old membership must not be silently rewritten.
    assert len(list(catalog.records(query(first)))) == 1


@pytest.mark.parametrize("state", ["LAGGING", "UNKNOWN", "missing", "stale"])
def test_native_incomplete_or_stale_coverage_never_returns_empty_success(
    migrated_database, objects, state
):
    from datetime import timedelta

    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage)
    current, _ = store.current()
    doc = copy.deepcopy(current[1])
    if state == "missing":
        doc["partitions"].pop()
    elif state == "stale":
        doc["published_at"] = (datetime.now(UTC) - timedelta(seconds=121)).isoformat()
    else:
        doc["partitions"][0]["status"] = state
    install_generation(store, doc)
    catalog = make_catalog(migrated_database, objects)
    catalog.sync()
    with pytest.raises(HistoryError):
        catalog.coverage({})


def test_native_lease_fences_stale_importer(migrated_database, objects):
    from psycopg import sql

    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    catalog = make_catalog(migrated_database, objects)
    current, _ = store.current()
    owner, fence, pending, active = catalog.index.claim(*current)
    assert make_catalog(migrated_database, objects).sync() is None
    with migrated_database.factory() as connection:
        connection.execute(
            sql.SQL("UPDATE {} SET lease_until=clock_timestamp()-interval '1 second'").format(
                catalog.meta.table("native_catalog_heads")
            )
        )
    assert make_catalog(migrated_database, objects).sync() == pending["generation_id"]
    with pytest.raises(HistoryError):
        catalog.index.activate(owner, fence, pending)


def test_native_source_change_during_initial_resume_is_rejected(migrated_database, objects):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    first, _ = store.current()
    publish(publisher, coverage, [receipt(store, 3)])
    catalog = make_catalog(migrated_database, objects)
    read = catalog.store.read_ref

    def interrupted(reference, **kwargs):
        if reference == first[0]["generation_ref"]:
            raise OSError("Test outage after a durable segment checkpoint")
        return read(reference, **kwargs)

    catalog.store.read_ref = interrupted
    with pytest.raises(HistoryError):
        catalog.sync()
    current, _ = store.current()
    changed = copy.deepcopy(current[1])
    changed["source_cluster_id"] = "recreated-during-import"
    install_generation(store, changed)
    resumed = make_catalog(migrated_database, objects)
    with pytest.raises(HistoryError):
        resumed.sync()


def test_native_resumed_segment_and_new_pointer_keep_frozen_membership(migrated_database, objects):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    first = publish(publisher, coverage, [receipt(store, 1)])
    root, _ = store.current()
    target = publish(publisher, coverage, [receipt(store, 3)])
    catalog = make_catalog(migrated_database, objects)
    original = catalog.store.read_ref

    def interrupted(reference, **kwargs):
        if reference == root[0]["generation_ref"]:
            raise OSError("Test outage between durable segments")
        return original(reference, **kwargs)

    catalog.store.read_ref = interrupted
    with pytest.raises(HistoryError):
        catalog.sync()
    newest = publish(publisher, coverage, [receipt(store, 5)])
    resumed = make_catalog(migrated_database, objects)
    assert resumed.sync() == target
    assert [r["kafka"]["offset"] for r in resumed.records(query(target))] == [3, 1]
    assert resumed.sync() == newest
    assert [r["kafka"]["offset"] for r in resumed.records(query(newest))] == [5, 3, 1]
    # Parent encountered during bootstrap is not itself an activated public generation.
    with pytest.raises(HistoryError):
        list(resumed.records(query(first)))


def test_upgrade_from_history_v3_preserves_snapshot_and_job_holds(owner_database):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from psycopg import sql
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url

    db = owner_database
    config = Config()
    config.set_main_option(
        "script_location", str(Path(__file__).resolve().parents[2] / "migrations")
    )
    engine = create_engine(
        make_url(db.dsn).set(drivername="postgresql+psycopg"),
        connect_args={"options": f"-csearch_path={db.schema}"},
    )
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "processing_0003_history")
        schema = metadata_schema(db.schema)
        with db.factory() as connection:
            connection.execute(
                sql.SQL("""INSERT INTO {}.snapshots
                (snapshot_id,actor,scope_sha256,query_sha256,query,collection,state,coverage,
                 as_of,expires_at,row_count) VALUES
                ('existing','actor','scope','query',%s,'PROCESSING','READY',%s,
                 clock_timestamp(),clock_timestamp()+interval '10 minutes',1)""").format(
                    sql.Identifier(schema)
                ),
                (b"{}", b"{}"),
            )
            connection.execute(
                sql.SQL("""INSERT INTO {}.snapshot_members
                (snapshot_id,ordinal,entry_id,payload,payload_sha256)
                VALUES('existing',0,'record',%s,'sha')""").format(sql.Identifier(schema)),
                (b"{}",),
            )
            connection.execute(
                sql.SQL("""INSERT INTO {}.export_jobs
                (job_id,actor,scope_sha256,idempotency_key,request_sha256,request,snapshot_id,
                 status,mode,next_attempt_at,deadline,created_at,updated_at) VALUES
                ('existing-job','actor','scope','key','sha',%s,'existing','QUEUED','JOB',
                 clock_timestamp(),clock_timestamp()+interval '30 minutes',
                 clock_timestamp(),clock_timestamp())""").format(sql.Identifier(schema)),
                (b"{}",),
            )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        with db.factory() as connection:
            assert connection.execute(
                sql.SQL(
                    "SELECT holds_input FROM {}.export_jobs WHERE job_id='existing-job'"
                ).format(sql.Identifier(schema))
            ).fetchone()["holds_input"]
            assert (
                connection.execute(
                    sql.SQL(
                        "SELECT payload FROM {}.snapshot_members WHERE snapshot_id='existing'"
                    ).format(sql.Identifier(schema))
                ).fetchone()["payload"]
                == b"{}"
            )
            assert (
                connection.execute(
                    sql.SQL("SELECT count(*) AS n FROM {}.native_catalog_heads").format(
                        sql.Identifier(schema)
                    )
                ).fetchone()["n"]
                == 0
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("outcome", ["stale", "corrupt"])
def test_slow_observer_cannot_block_newer_verified_head(migrated_database, objects, outcome):
    from maintai_messaging.errors import IdentityConflict

    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    slow = make_catalog(migrated_database, objects)
    slow.sync()
    stale = store.current()
    latest = publish(publisher, coverage, [receipt(store, 3)])

    def delayed_current():
        # Another replica finishes while this replica's S3 response is in flight.
        assert make_catalog(migrated_database, objects).sync() == latest
        if outcome == "corrupt":
            raise IdentityConflict("Delayed malformed response from an older observation")
        return stale

    slow.store.current = delayed_current
    if outcome == "corrupt":
        with pytest.raises(HistoryError):
            slow.sync()
    else:
        assert slow.sync() is None
    assert slow.coverage({})["catalog_generation"] == latest


@pytest.mark.parametrize("failure", ["missing", "checksum"])
def test_native_unreadable_parent_never_activates_partial_index(
    migrated_database, objects, failure
):
    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    current, _ = store.current()
    doc = copy.deepcopy(current[1])
    doc["parent_ref"] = copy.deepcopy(current[0]["generation_ref"])
    if failure == "missing":
        doc["parent_ref"]["key"] = "raw-catalog/v1/generations/missing.json"
    else:
        doc["parent_ref"]["sha256"] = "0" * 64
    pointer = install_generation(store, doc)
    catalog = make_catalog(migrated_database, objects)
    with pytest.raises(HistoryError):
        catalog.sync()
    with pytest.raises(HistoryError):
        catalog.coverage({})
    with pytest.raises(HistoryError):
        list(catalog.records(query(pointer["generation_id"])))


def test_native_traversal_cycle_is_rejected_by_persisted_visited_set(migrated_database, objects):
    from maintai_messaging.contracts import canonical_json, strict_json

    client, bucket, objects = objects
    store, publisher, coverage = provider(client, bucket)
    publish(publisher, coverage, [receipt(store, 1)])
    root, _ = store.current()
    publish(publisher, coverage, [receipt(store, 3)])
    head, _ = store.current()
    catalog = make_catalog(migrated_database, objects)
    original = catalog.store.read_ref

    def cyclic(reference, **kwargs):
        raw = original(reference, **kwargs)
        if reference == root[0]["generation_ref"]:
            # Index-level defense in depth: simulate a broken helper returning a cycle.
            # A real S3 mutation is rejected earlier by the provider's SHA checks.
            doc = strict_json(raw)
            doc["parent_ref"] = head[0]["generation_ref"]
            return canonical_json(doc)
        return raw

    catalog.store.read_ref = cyclic
    with pytest.raises(HistoryError):
        catalog.sync()
    with pytest.raises(HistoryError):
        catalog.coverage({})
