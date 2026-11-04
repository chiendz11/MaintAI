"""Durable export intent and holds are separate from interactive snapshot expiry."""

from datetime import timedelta

import pytest
from processing.shared.history_meta.exports import Exports
from processing.shared.history_meta.security import HistoryError

from .test_history_objects import objects  # noqa: F401
from .test_history_snapshots import ACTOR, NOW, QUERY, create, insert

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("filter", [{"model_version": "fixture"}, {"result_kind": "DATA_INVALID"}])
def test_export_rejects_omitting_filter_of_reused_snapshot(migrated_database, filter):
    history = create(migrated_database)
    page = history.page(ACTOR, "M-001", {**QUERY, **filter})
    with pytest.raises(HistoryError) as error:
        Exports(history, None).accept(ACTOR, request(snapshot_id=page["snapshot_id"]), "filtered")
    assert error.value.code == "SNAPSHOT_QUERY_CONFLICT"


def request(**changes):
    return {
        "profile": "M6-HISTORY-01/1.0.0",
        "machine_ids": ["M-001"],
        "from": "2026-10-28T00:00:00Z",
        "to": "2026-10-30T00:00:00Z",
        "resource": "PROCESSING",
        "format": "CSV",
        **changes,
    }


def test_small_csv_without_object_store_is_lossless_and_idempotent(migrated_database):
    insert(migrated_database, "=formula", 1)
    exports = Exports(create(migrated_database), None)
    status, first = exports.accept(ACTOR, request(), "one")
    assert status == 200
    text = first.read().decode()
    first.close()
    assert "'=formula" in text
    status, again = exports.accept(ACTOR, request(), "one")
    assert again.read().decode() == text
    again.close()
    with pytest.raises(HistoryError) as exc:
        exports.accept(ACTOR, request(format="PARQUET"), "one")
    assert exc.value.status == 409


def test_artifact_job_needs_object_store_before_accept(migrated_database):
    exports = Exports(create(migrated_database), None)
    with pytest.raises(HistoryError) as exc:
        exports.accept(ACTOR, request(format="PARQUET"), "one")
    assert exc.value.status == 503


def test_job_input_hold_outlives_interactive_snapshot_and_rechecks_download(
    migrated_database,
    objects,  # noqa: F811
):
    _, _, store = objects
    insert(migrated_database, "valid", 1)
    history = create(migrated_database)
    exports = Exports(history, store)
    status, job = exports.accept(ACTOR, request(format="PARQUET"), "job")
    assert status == 202 and job["status"] == "QUEUED"
    history.clock = lambda: NOW + timedelta(minutes=11)
    from processing.shared.history_meta.expiry import expire_metadata

    assert expire_metadata(history.meta, now=history.clock())["snapshots_released"] == 0
    assert exports.run_once()
    done = exports.get(ACTOR, job["job_id"])
    assert done["status"] == "SUCCEEDED"
    assert done["artifact_ref"]["version_id"]
    import io
    import json

    import pyarrow.parquet as pq

    values = pq.read_table(io.BytesIO(store.read(done["artifact_ref"]))).to_pylist()
    assert json.loads(values[0]["record"])["source_event_id"] == "valid"
    history.clock = lambda: NOW + timedelta(minutes=12)
    assert expire_metadata(history.meta, now=history.clock())["snapshots_released"] == 1
    assert exports.get(ACTOR, job["job_id"])["status"] == "SUCCEEDED"
    history.clock = lambda: NOW + timedelta(days=2)
    with pytest.raises(HistoryError) as exc:
        exports.download(ACTOR, job["job_id"])
    assert exc.value.status == 410


def test_stream_does_not_resurrect_expired_snapshot_under_same_key(migrated_database):
    history = create(migrated_database)
    exports = Exports(history, None)
    _, stream = exports.accept(ACTOR, request(), "stream")
    stream.close()
    history.clock = lambda: NOW + timedelta(minutes=11)
    with pytest.raises(HistoryError) as exc:
        exports.accept(ACTOR, request(), "stream")
    assert exc.value.status == 410


def test_second_runner_recovers_after_crash_following_artifact_ack(migrated_database, objects):  # noqa: F811
    _, _, store = objects
    insert(migrated_database, "recover", 1)
    first_history = create(migrated_database)
    first = Exports(first_history, store)
    _, job = first.accept(ACTOR, request(format="PARQUET"), "recovery-intent")
    upload = store.put_export
    acknowledged = []

    def crash_after_ack(*args):
        reference = upload(*args)
        acknowledged.append(reference)
        raise SystemExit("injected worker crash after durable S3 ACK")

    store.put_export = crash_after_ack
    try:
        with pytest.raises(SystemExit):
            first.run_once()
    finally:
        store.put_export = upload
    assert first.get(ACTOR, job["job_id"])["status"] == "RUNNING"
    second_history = create(migrated_database)
    second_history.clock = lambda: NOW + timedelta(seconds=31)
    second = Exports(second_history, store)
    assert second.run_once()
    completed = second.get(ACTOR, job["job_id"])
    assert completed["status"] == "SUCCEEDED"
    assert completed["artifact_ref"] == acknowledged[0]
    with second.meta.factory() as connection:
        from psycopg import sql

        row = connection.execute(
            sql.SQL("SELECT attempts,fencing_token FROM {} WHERE job_id=%s").format(
                second.meta.table("export_jobs")
            ),
            (job["job_id"],),
        ).fetchone()
    assert row == {"attempts": 2, "fencing_token": 2}
