"""Durable owner operations for original-source Business projection rebuilds."""

from datetime import UTC, datetime
from uuid import uuid4

from maintai_messaging.contracts import canonical_json

from backend.shared.projections.generations import current, gate, registry_hash


class RebuildRepository:
    def __init__(self, factory):
        self.factory = factory

    def start(self, *, expected_active, operator, source_cluster, groups, deadline, journal_budget):
        if (
            not all(
                isinstance(s, str) and s.strip()
                for s in (operator, source_cluster, expected_active)
            )
            or set(groups) != {"sensor.readings", "inference.results"}
            or any(not isinstance(s, str) or not s.strip() for s in groups.values())
            or not isinstance(deadline, datetime)
            or deadline.tzinfo is None
            or deadline <= datetime.now(UTC)
            or type(journal_budget) is not int
            or not 0 < journal_budget < 2**63
        ):
            raise ValueError(
                "Explicit scope, deadline and positive journal byte budget are required"
            )
        job, generation = uuid4().hex, uuid4().hex
        with self.factory() as connection, connection.transaction():
            gate(connection, exclusive=True)
            if current(connection) != expected_active:
                raise ValueError("Active generation changed")
            control = connection.execute("SELECT building_job FROM projection_control").fetchone()
            if control["building_job"] is not None:
                raise ValueError("A projection rebuild is already active")
            scope_hash, machines = registry_hash(connection)
            manifest = {
                "schema_version": "1.0.0",
                "operator": operator,
                "source_cluster": source_cluster,
                "groups": groups,
                "machines": machines,
                "expected_active": expected_active,
                "deadline": deadline.isoformat(),
                "journal_budget": journal_budget,
            }
            connection.execute(
                "INSERT INTO projection_generations(generation_id,state) VALUES(%s,'BUILDING')",
                (generation,),
            )
            connection.execute(
                """INSERT INTO projection_rebuild_jobs
                (job_id,generation_id,expected_active,status,input,registry_sha256,deadline,
                 journal_budget,checkpoint) VALUES(%s,%s,%s,'QUEUED',%s,%s,%s,%s,%s)""",
                (
                    job,
                    generation,
                    expected_active,
                    canonical_json(manifest),
                    scope_hash,
                    deadline,
                    journal_budget,
                    canonical_json({}),
                ),
            )
            connection.execute(
                "UPDATE projection_control SET building_job=%s WHERE singleton", (job,)
            )
        return job

    def status(self, identifier):
        with self.factory() as connection:
            row = connection.execute(
                "SELECT * FROM projection_rebuild_jobs WHERE job_id=%s", (identifier,)
            ).fetchone()
        if row is None:
            raise ValueError("Unknown rebuild job")
        return row

    @staticmethod
    def _locked(connection, identifier, *, lease=None):
        row = connection.execute(
            "SELECT * FROM projection_rebuild_jobs WHERE job_id=%s FOR UPDATE", (identifier,)
        ).fetchone()
        if row is None or row["status"] not in {"QUEUED", "RUNNING", "VERIFIED"}:
            raise ValueError("Rebuild job is terminal or unknown")
        now = connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]
        if row["deadline"] <= now:
            raise ValueError("Rebuild deadline exceeded")
        if lease is not None and (
            row["lease_owner"] != lease["lease_owner"]
            or row["fencing_token"] != lease["fencing_token"]
            or row["lease_until"] is None
            or row["lease_until"] <= now
        ):
            raise ValueError("Rebuild lease is stale")
        return row

    def claim(self, owner):
        with self.factory() as connection, connection.transaction():
            self._expire(connection)
            row = connection.execute("""SELECT job_id FROM projection_rebuild_jobs
                WHERE status IN ('QUEUED','RUNNING') AND deadline>clock_timestamp()
                AND (lease_until IS NULL OR lease_until<clock_timestamp())
                ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if row is None:
                return None
            return connection.execute(
                """UPDATE projection_rebuild_jobs SET status='RUNNING',
                lease_owner=%s,lease_until=clock_timestamp()+interval '90 seconds',
                fencing_token=fencing_token+1,updated_at=clock_timestamp() WHERE job_id=%s
                RETURNING *""",
                (owner, row["job_id"]),
            ).fetchone()

    def heartbeat(self, lease):
        with self.factory() as connection, connection.transaction():
            self._locked(connection, lease["job_id"], lease=lease)
            connection.execute(
                "UPDATE projection_rebuild_jobs SET lease_until=clock_timestamp()+"
                "interval '90 seconds' WHERE job_id=%s",
                (lease["job_id"],),
            )

    @staticmethod
    def _terminal(connection, row, status, error):
        connection.execute(
            """UPDATE projection_rebuild_jobs SET status=%s,last_error=%s,
            lease_until=NULL,fencing_token=fencing_token+1,updated_at=clock_timestamp()
            WHERE job_id=%s""",
            (status, error, row["job_id"]),
        )
        connection.execute(
            "UPDATE projection_generations SET state='FAILED' WHERE generation_id=%s",
            (row["generation_id"],),
        )
        connection.execute(
            "UPDATE projection_control SET building_job=NULL WHERE building_job=%s",
            (row["job_id"],),
        )

    @classmethod
    def _expire(cls, connection):
        rows = connection.execute("""SELECT * FROM projection_rebuild_jobs WHERE
            status IN ('QUEUED','RUNNING','VERIFIED') AND deadline<=clock_timestamp()
            FOR UPDATE""").fetchall()
        for row in rows:
            cls._terminal(connection, row, "FAILED", "DEADLINE_EXCEEDED")

    def fail(self, identifier, code, *, lease=None):
        if code not in {
            "SOURCE_CORRUPT",
            "SOURCE_UNAVAILABLE",
            "REGISTRY_CHANGED",
            "DEADLINE_EXCEEDED",
        }:
            raise ValueError("Unknown bounded rebuild failure code")
        with self.factory() as connection, connection.transaction():
            gate(connection, exclusive=True)
            row = connection.execute(
                "SELECT * FROM projection_rebuild_jobs WHERE job_id=%s FOR UPDATE", (identifier,)
            ).fetchone()
            if row and row["status"] in {"QUEUED", "RUNNING", "VERIFIED"}:
                if lease is not None:
                    self._locked(connection, identifier, lease=lease)
                self._terminal(connection, row, "FAILED", code)

    def release(self, lease):
        try:
            with self.factory() as connection, connection.transaction():
                connection.execute(
                    """UPDATE projection_rebuild_jobs SET lease_until=NULL
                    WHERE job_id=%s AND lease_owner=%s AND fencing_token=%s""",
                    (lease["job_id"], lease["lease_owner"], lease["fencing_token"]),
                )
        except Exception:
            pass  # Expiration allows takeover if the DB is temporarily unavailable.

    def abort(self, identifier):
        with self.factory() as connection, connection.transaction():
            gate(connection, exclusive=True)
            row = connection.execute(
                "SELECT * FROM projection_rebuild_jobs WHERE job_id=%s FOR UPDATE", (identifier,)
            ).fetchone()
            if row is None or row["status"] not in {"QUEUED", "RUNNING", "VERIFIED"}:
                raise ValueError("Cannot abort terminal job")
            self._terminal(connection, row, "ABORTED", "OPERATOR_ABORT")

    def resume(self, identifier):
        with self.factory() as connection, connection.transaction():
            row = self._locked(connection, identifier)
            if row["lease_until"] is not None:
                now = connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]
                if row["lease_until"] > now:
                    raise ValueError("Job still has a live lease")
            connection.execute(
                "UPDATE projection_rebuild_jobs SET status='QUEUED',last_error=NULL "
                "WHERE job_id=%s",
                (identifier,),
            )

    def scan_members(self, lease):
        import json

        with self.factory() as connection, connection.transaction():
            row = self._locked(connection, lease["job_id"], lease=lease)
            if row["baseline_done"]:
                return 0
            checkpoint = json.loads(bytes(row["checkpoint"]))
            cursor = checkpoint.get("membership", ["", "", ""])
            groups = json.loads(bytes(row["input"]))["groups"]
            members = connection.execute(
                """SELECT i.* FROM maintai_kafka_inbox i
                WHERE ((topic='sensor.readings' AND consumer_group=%s)
                    OR (topic='inference.results' AND consumer_group=%s))
                AND (consumer_group,topic,message_id)>(%s,%s,%s)
                AND NOT EXISTS(SELECT 1 FROM projection_journal j WHERE j.job_id=%s
                    AND j.topic=i.topic AND j.message_id=i.message_id)
                ORDER BY consumer_group,topic,message_id LIMIT 32""",
                (groups["sensor.readings"], groups["inference.results"], *cursor, row["job_id"]),
            ).fetchall()
            for member in members:
                connection.execute(
                    """INSERT INTO projection_members(job_id,consumer_group,topic,
                    message_id,identity_sha256,source_partition,source_offset,schema_version)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        row["job_id"],
                        *[
                            member[k]
                            for k in (
                                "consumer_group",
                                "topic",
                                "message_id",
                                "identity_sha256",
                                "source_partition",
                                "source_offset",
                                "schema_version",
                            )
                        ],
                    ),
                )
            if members:
                checkpoint["membership"] = [
                    members[-1][k] for k in ("consumer_group", "topic", "message_id")
                ]
            connection.execute(
                "UPDATE projection_rebuild_jobs SET checkpoint=%s,baseline_done=%s WHERE job_id=%s",
                (canonical_json(checkpoint), len(members) < 32, row["job_id"]),
            )
            return len(members)

    def pending(self, lease):
        with self.factory() as connection, connection.transaction():
            self._locked(connection, lease["job_id"], lease=lease)
            return connection.execute(
                """SELECT * FROM projection_members WHERE job_id=%s
                AND NOT applied ORDER BY consumer_group,topic,message_id LIMIT 32""",
                (lease["job_id"],),
            ).fetchall()

    @staticmethod
    def _apply(connection, row, event):
        import json

        from backend.shared.ordering.identity import guard_source
        from backend.shared.projections.projector import load, persist
        from backend.shared.projections.reducer import reduce_projection

        if event.key not in json.loads(bytes(row["input"]))["machines"]:
            raise ValueError("Registry scope changed")
        generation = row["generation_id"]
        validation = guard_source(connection, event, generation=generation)
        result = reduce_projection(
            load(connection, event.key, generation=generation), event, source_validation=validation
        )
        persist(connection, result, generation=generation)

    def apply_member(self, lease, member, event, source_ref):
        if (
            event.topic,
            event.message_id,
            event.identity_sha256,
            event.payload["schema_version"],
        ) != (
            member["topic"],
            member["message_id"],
            member["identity_sha256"],
            member["schema_version"],
        ):
            raise ValueError("Original source identity differs from expected receipt")
        with self.factory() as connection, connection.transaction():
            row = self._locked(connection, lease["job_id"], lease=lease)
            actual = connection.execute(
                """SELECT * FROM projection_members WHERE job_id=%s
                AND consumer_group=%s AND topic=%s AND message_id=%s FOR UPDATE""",
                (row["job_id"], member["consumer_group"], member["topic"], member["message_id"]),
            ).fetchone()
            if actual is None:
                raise ValueError("Unexpected source member")
            from backend.shared.projections.sources import verify

            verify(event, actual)
            if actual["applied"]:
                return
            self._apply(connection, row, event)
            connection.execute(
                """UPDATE projection_members SET applied=TRUE,source_ref=%s
                WHERE job_id=%s AND consumer_group=%s AND topic=%s AND message_id=%s""",
                (
                    canonical_json(source_ref),
                    row["job_id"],
                    member["consumer_group"],
                    member["topic"],
                    member["message_id"],
                ),
            )

    @staticmethod
    def _tail(connection, row, *, limit=32):
        import hashlib

        from maintai_messaging.contracts import default_contracts

        rows = connection.execute(
            """WITH candidates AS MATERIALIZED (
                SELECT journal_id,octet_length(wire) AS bytes FROM projection_journal
                WHERE job_id=%s AND journal_id>%s ORDER BY journal_id LIMIT %s
            ), bounded AS (
                SELECT journal_id,sum(bytes) OVER(ORDER BY journal_id) AS bytes FROM candidates
            ) SELECT j.* FROM projection_journal j JOIN bounded b USING(journal_id)
            WHERE b.bytes<=8388608 ORDER BY j.journal_id""",
            (row["job_id"], row["applied_journal"], limit),
        ).fetchall()
        # Keep the replay batch below the wire memory budget.
        size = 0
        applied = 0
        for item in rows:
            size += len(item["wire"])
            if size > 8 * 1024 * 1024:
                break
            wire = bytes(item["wire"])
            if hashlib.sha256(wire).hexdigest() != item["wire_sha256"]:
                raise ValueError("Journal wire checksum differs")
            event = default_contracts().decode(item["topic"], wire)
            if (event.message_id, event.identity_sha256) != (
                item["message_id"],
                item["identity_sha256"],
            ):
                raise ValueError("Journal identity differs")
            RebuildRepository._apply(connection, row, event)
            connection.execute(
                "UPDATE projection_rebuild_jobs SET applied_journal=%s,"
                "applied_count=applied_count+1 "
                "WHERE job_id=%s",
                (item["journal_id"], row["job_id"]),
            )
            applied += 1
        return applied

    def catch_up(self, lease):
        with self.factory() as connection, connection.transaction():
            row = self._locked(connection, lease["job_id"], lease=lease)
            return self._tail(connection, row)

    def verify(self, identifier, *, lease=None):
        import time

        from backend.shared.projections.errors import retryable
        from backend.shared.projections.sources import SourcePending

        verification = None
        try:
            # Connect before taking a live gate. The long read snapshot keeps no writer lock.
            with (
                self.factory() as snapshot,
                self.factory() as boundary,
                self.factory() as connection,
            ):
                with snapshot.transaction():
                    snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    started = time.monotonic()
                    with boundary.transaction():
                        boundary.execute("SET LOCAL transaction_timeout='3000ms'")
                        boundary.execute("SET LOCAL lock_timeout='3000ms'")
                        gate(boundary, exclusive=True)
                        with connection.transaction():
                            connection.execute("SET LOCAL transaction_timeout='3000ms'")
                            row = self._locked(connection, identifier, lease=lease)
                            self._tail(connection, row)
                            progress = connection.execute(
                                "SELECT * FROM projection_rebuild_jobs WHERE job_id=%s",
                                (identifier,),
                            ).fetchone()
                            if connection.execute(
                                "SELECT 1 FROM projection_journal WHERE job_id=%s "
                                "AND journal_id>%s LIMIT 1",
                                (identifier, progress["applied_journal"]),
                            ).fetchone():
                                raise SourcePending("Journal needs another bounded catch-up pass")
                            if progress["journal_count"] != progress["applied_count"]:
                                raise ValueError("Journal has a missing unapplied record")
                            if (
                                not row["baseline_done"]
                                or connection.execute(
                                    "SELECT 1 FROM projection_members WHERE job_id=%s "
                                    "AND NOT applied LIMIT 1",
                                    (identifier,),
                                ).fetchone()
                            ):
                                raise ValueError(
                                    "Expected source membership has not been completely applied"
                                )
                            if registry_hash(connection)[0] != row["registry_sha256"]:
                                raise ValueError("Registry scope changed")
                            verification = connection.execute(
                                """UPDATE projection_rebuild_jobs SET status='RUNNING',
                                lease_owner=%s,lease_until=clock_timestamp()+interval '90 seconds',
                                fencing_token=fencing_token+1 WHERE job_id=%s RETURNING *""",
                                ("verify-" + uuid4().hex, identifier),
                            ).fetchone()
                        # The shadow commit is now visible and no later live receipt can commit yet.
                        snapshot.execute("SELECT pg_current_snapshot()")
                        if time.monotonic() - started > 2.5:
                            raise SourcePending("Verification snapshot gate exceeded short budget")
                    self._verify_rows(row["generation_id"], verification, snapshot)
            with self.factory() as connection, connection.transaction():
                self._locked(connection, identifier, lease=verification)
                connection.execute(
                    """UPDATE projection_rebuild_jobs SET status='VERIFIED',lease_until=NULL,
                    fencing_token=fencing_token+1 WHERE job_id=%s""",
                    (identifier,),
                )
        except Exception as exc:
            if verification is not None:
                if retryable(exc) or isinstance(exc, SourcePending):
                    self.release(verification)
                    raise SourcePending(
                        "Verification infrastructure temporarily unavailable"
                    ) from exc
                try:
                    self.fail(identifier, "SOURCE_CORRUPT", lease=verification)
                except Exception:
                    pass
            raise

    def _verify_rows(self, generation, lease, connection):
        import time

        from psycopg import sql

        from backend.shared.projections.projector import row_hash
        from backend.shared.projections.reducer import TABLES

        renewed = time.monotonic()

        def heartbeat():
            nonlocal renewed
            if time.monotonic() - renewed >= 15:
                self.heartbeat(lease)
                renewed = time.monotonic()

        machine, sequence = "", 0
        while True:
            heartbeat()
            rows = connection.execute(
                """SELECT b.machine_id,b.sequence_number,
                (p.machine_id IS NULL OR p.source_event_id<>b.source_event_id
                 OR p.stream_epoch<>b.stream_epoch
                 OR (b.reading_sha256 IS NOT NULL
                     AND p.reading_sha256 IS DISTINCT FROM b.reading_sha256)
                 OR (b.outcome_sha256 IS NOT NULL
                     AND p.outcome_sha256 IS DISTINCT FROM b.outcome_sha256)) AS mismatch
                FROM business_source_inbox b LEFT JOIN projection_source_inbox p
                ON p.generation_id=%s AND p.machine_id=b.machine_id
                AND p.sequence_number=b.sequence_number
                WHERE (b.machine_id,b.sequence_number)>(%s,%s)
                ORDER BY b.machine_id,b.sequence_number LIMIT 32""",
                (generation, machine, sequence),
            ).fetchall()
            if any(row["mismatch"] for row in rows):
                raise ValueError("Expected source ledger membership is incomplete or contradictory")
            if len(rows) < 32:
                break
            machine, sequence = rows[-1]["machine_id"], rows[-1]["sequence_number"]
        for axis in TABLES:
            # A server cursor transfers one snapshot at a time, never 32 large raw snapshots.
            with connection.cursor(name="verify_" + uuid4().hex) as cursor:
                cursor.itersize = 1
                cursor.execute(
                    sql.SQL("""SELECT p.*,c.sha256 AS expected_hash FROM {} p
                    LEFT JOIN projection_row_checksums c ON c.generation_id=p.generation_id
                    AND c.machine_id=p.machine_id AND c.axis=%s
                    WHERE p.generation_id=%s ORDER BY p.machine_id""").format(
                        sql.Identifier("projection_" + axis)
                    ),
                    (axis, generation),
                )
                for value in cursor:
                    heartbeat()
                    if row_hash(value) != value["expected_hash"]:
                        raise ValueError("Shadow projection checksum differs from reducer output")
            count = connection.execute(
                sql.SQL("SELECT count(*) AS n FROM {} WHERE generation_id=%s").format(
                    sql.Identifier("projection_" + axis)
                ),
                (generation,),
            ).fetchone()["n"]
            expected = connection.execute(
                "SELECT count(*) AS n FROM projection_row_checksums "
                "WHERE generation_id=%s AND axis=%s",
                (generation, axis),
            ).fetchone()["n"]
            if count != expected:
                raise ValueError("Shadow projection row is missing")

    def activate(self, identifier, *, expected_active):
        import time

        from backend.shared.copilot.repository import emit

        started = time.monotonic()
        with self.factory() as connection, connection.transaction():
            connection.execute("SET LOCAL transaction_timeout='3000ms'")
            connection.execute("SET LOCAL statement_timeout='3000ms'")
            connection.execute("SET LOCAL lock_timeout='3000ms'")
            gate(connection, exclusive=True)
            row = self._locked(connection, identifier)
            if (
                row["status"] != "VERIFIED"
                or row["expected_active"] != expected_active
                or current(connection) != expected_active
            ):
                raise ValueError(
                    "Activation requires verified input and unchanged active generation"
                )
            if registry_hash(connection)[0] != row["registry_sha256"]:
                raise ValueError("Registry scope changed")
            # Exclusive gate establishes a committed journal frontier, including all live callbacks.
            high = connection.execute(
                "SELECT COALESCE(max(journal_id),0) AS n FROM projection_journal WHERE job_id=%s",
                (identifier,),
            ).fetchone()["n"]
            self._tail(connection, row)
            applied = connection.execute(
                "SELECT applied_journal FROM projection_rebuild_jobs WHERE job_id=%s", (identifier,)
            ).fetchone()["applied_journal"]
            counts = connection.execute(
                "SELECT journal_count,applied_count FROM projection_rebuild_jobs WHERE job_id=%s",
                (identifier,),
            ).fetchone()
            if counts["journal_count"] != counts["applied_count"]:
                raise ValueError("Journal has a missing unapplied record")
            if applied != high or time.monotonic() - started > 2.5:
                raise ValueError(
                    "Journal tail exceeds short activation budget; catch up and verify again"
                )
            connection.execute(
                "UPDATE projection_generations SET deactivated_at=clock_timestamp() "
                "WHERE generation_id=%s",
                (expected_active,),
            )
            connection.execute(
                "UPDATE projection_generations SET state='ACTIVE',activated_at=clock_timestamp() "
                "WHERE generation_id=%s",
                (row["generation_id"],),
            )
            connection.execute(
                "UPDATE projection_control SET active_generation=%s,building_job=NULL "
                "WHERE singleton",
                (row["generation_id"],),
            )
            connection.execute(
                "UPDATE projection_rebuild_jobs SET status='SUCCEEDED',lease_until=NULL,"
                "fencing_token=fencing_token+1 WHERE job_id=%s",
                (identifier,),
            )
            emit(connection, "projection.resync", None, {"generation_id": row["generation_id"]})
            if time.monotonic() - started > 3:
                raise ValueError("Activation transaction exceeded three seconds")
            return row["generation_id"]
