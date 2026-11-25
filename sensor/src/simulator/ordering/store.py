"""Single-controller SQLite journal: atomic sequence + payload + control receipts.

The database must live on a persistent local volume. A process lock deliberately
rejects multiple controllers; SQLite on shared/network filesystems is unsupported.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from simulator.logging import log


def utc(value: float | None = None) -> str:
    from datetime import UTC, datetime

    return (
        datetime.fromtimestamp(time.time() if value is None else value, UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class ControlError(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code


class StateStore:
    SCHEMA_VERSION = "2"

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.closed = False
        self._rr_offset = 0
        self.file_lock = path.with_suffix(path.suffix + ".lock").open("a+")
        try:
            fcntl.flock(self.file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file_lock.close()
            raise ValueError("Another Sensor controller owns this state volume") from None
        try:
            self.db = sqlite3.connect(
                path, timeout=5, check_same_thread=False, isolation_level=None
            )
            os.chmod(path, 0o600)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS machines (
                    machine_id TEXT PRIMARY KEY, stream_state TEXT NOT NULL,
                    scenario TEXT NOT NULL, control_version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL, last_controlled_by TEXT NOT NULL,
                    sequence_number INTEGER NOT NULL, row_cursor INTEGER NOT NULL,
                    scenario_step INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS spool (
                    id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                    machine_id TEXT NOT NULL, payload TEXT NOT NULL, size INTEGER NOT NULL,
                    created_at REAL NOT NULL, next_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending',
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS spool_due ON spool(status, next_at, id);
                CREATE INDEX IF NOT EXISTS spool_machine_status_id ON spool(machine_id, status, id);
                CREATE INDEX IF NOT EXISTS spool_pending_created ON spool(status, created_at);
                CREATE TABLE IF NOT EXISTS spool_quarantine (
                    id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                    machine_id TEXT NOT NULL, payload TEXT NOT NULL, size INTEGER NOT NULL,
                    created_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL, error TEXT, quarantined_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS spool_quarantine_status_id
                    ON spool_quarantine(status, id);
                CREATE TABLE IF NOT EXISTS spool_stats (
                    machine_id TEXT PRIMARY KEY,
                    pending_count INTEGER NOT NULL DEFAULT 0,
                    pending_bytes INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS commands (
                    actor TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    response TEXT NOT NULL, created_at REAL NOT NULL,
                    PRIMARY KEY(actor,key)
                );
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, actor TEXT NOT NULL,
                    action TEXT NOT NULL, target TEXT NOT NULL, detail TEXT NOT NULL
                );
            """)
            with self.transaction() as db:
                old = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
                if old is None:
                    db.execute(
                        "INSERT OR IGNORE INTO meta VALUES ('schema_version', ?)",
                        (self.SCHEMA_VERSION,),
                    )
                    db.execute("INSERT OR IGNORE INTO spool_stats VALUES ('__global__', 0, 0)")
                elif old[0] == "1":
                    self._migrate_v1_to_v2(db)
                elif old[0] == self.SCHEMA_VERSION:
                    db.execute("INSERT OR IGNORE INTO spool_stats VALUES ('__global__', 0, 0)")
                else:
                    raise ValueError(
                        "Unsupported Sensor state schema; explicit migration is required"
                    )
        except Exception:
            self.close()
            raise

    @staticmethod
    def _migrate_v1_to_v2(db: sqlite3.Connection) -> None:
        """Idempotent startup migration from v1 spool table to v2 quarantine + O(1) stats."""
        now = time.time()
        rows = db.execute(
            "SELECT * FROM spool WHERE status IN ('expired', 'rejected') ORDER BY id"
        ).fetchall()
        for row in rows:
            db.execute(
                "INSERT OR IGNORE INTO spool_quarantine "
                "(event_id,machine_id,payload,size,created_at,attempts,status,error,quarantined_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    row["event_id"],
                    row["machine_id"],
                    row["payload"],
                    row["size"],
                    row["created_at"],
                    row["attempts"],
                    row["status"],
                    row["error"],
                    now,
                ),
            )
        if rows:
            db.execute("DELETE FROM spool WHERE status IN ('expired', 'rejected')")
        db.execute("DELETE FROM spool_stats")
        total = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM spool WHERE status='pending'"
        ).fetchone()
        db.execute("INSERT INTO spool_stats VALUES ('__global__', ?, ?)", (total[0], total[1]))
        for per_m in db.execute(
            "SELECT machine_id, COUNT(*), COALESCE(SUM(size), 0) "
            "FROM spool WHERE status='pending' GROUP BY machine_id"
        ):
            db.execute("INSERT INTO spool_stats VALUES (?, ?, ?)", (per_m[0], per_m[1], per_m[2]))
        db.execute("INSERT OR REPLACE INTO meta VALUES ('schema_version', '2')")

    @staticmethod
    def _adjust_stats(
        db: sqlite3.Connection, machine_id: str, delta_count: int, delta_bytes: int
    ) -> None:
        if delta_count == 0 and delta_bytes == 0:
            return
        for key in ("__global__", machine_id):
            db.execute(
                "INSERT INTO spool_stats(machine_id, pending_count, pending_bytes) VALUES (?,?,?) "
                "ON CONFLICT(machine_id) DO UPDATE SET "
                "pending_count=MAX(0, pending_count + excluded.pending_count), "
                "pending_bytes=MAX(0, pending_bytes + excluded.pending_bytes)",
                (key, max(0, delta_count), max(0, delta_bytes)),
            ) if delta_count > 0 or delta_bytes > 0 else db.execute(
                "UPDATE spool_stats SET "
                "pending_count=MAX(0, pending_count + ?), "
                "pending_bytes=MAX(0, pending_bytes + ?) WHERE machine_id=?",
                (delta_count, delta_bytes, key),
            )

    @staticmethod
    def _trim_quarantine(db: sqlite3.Connection, max_records: int) -> None:
        row = db.execute("SELECT COUNT(*) FROM spool_quarantine").fetchone()
        excess = row[0] - max_records
        if excess > 0:
            db.execute(
                "DELETE FROM spool_quarantine WHERE id IN "
                "(SELECT id FROM spool_quarantine ORDER BY id LIMIT ?)",
                (excess,),
            )

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def initialize(self, count: int, scenario: str, initial: str, identity: str):
        with self.transaction() as db:
            old = db.execute("SELECT value FROM meta WHERE key='source_identity'").fetchone()
            if old and old[0] != identity:
                raise ValueError(
                    "Dataset/generation configuration differs from persisted state; "
                    "use an explicit migration or new state volume"
                )
            db.execute("INSERT OR IGNORE INTO meta VALUES ('source_identity',?)", (identity,))
            for i in range(count):
                db.execute(
                    "INSERT OR IGNORE INTO machines VALUES (?,?,?,?,?,?,?,?,?)",
                    (f"M-{i + 1:03d}", initial, scenario, 0, utc(), "system", 0, i, 0),
                )

    @staticmethod
    def public(row) -> dict:
        return {
            **{
                key: row[key]
                for key in (
                    "machine_id",
                    "stream_state",
                    "scenario",
                    "control_version",
                    "updated_at",
                    "last_controlled_by",
                )
            },
            "source": "SIMULATOR_CONTROL",
        }

    def list(self) -> list[dict]:
        with self.transaction() as db:
            return [
                self.public(r) for r in db.execute("SELECT * FROM machines ORDER BY machine_id")
            ]

    def get(self, machine_id: str) -> dict:
        with self.transaction() as db:
            row = db.execute("SELECT * FROM machines WHERE machine_id=?", (machine_id,)).fetchone()
            if not row:
                raise ControlError(404, "MACHINE_NOT_FOUND")
            return self.public(row)

    def command(
        self,
        actor: str,
        key: str,
        machine: str,
        action: str,
        body: dict,
        scenarios: set[str],
        receipt_limit: int,
    ) -> dict:
        fingerprint = hashlib.sha256(canonical([machine, action, body]).encode()).hexdigest()
        with self.transaction() as db:
            receipt = db.execute(
                "SELECT * FROM commands WHERE actor=? AND key=?", (actor, key)
            ).fetchone()
            if receipt:
                if receipt["fingerprint"] != fingerprint:
                    raise ControlError(409, "IDEMPOTENCY_CONFLICT")
                return json.loads(receipt["response"])
            row = db.execute("SELECT * FROM machines WHERE machine_id=?", (machine,)).fetchone()
            if not row:
                raise ControlError(404, "MACHINE_NOT_FOUND")
            if row["control_version"] != body["expected_control_version"]:
                raise ControlError(409, "CONTROL_VERSION_CONFLICT")
            if action == "scenario" and body["scenario"] not in scenarios:
                raise ControlError(422, "UNSUPPORTED_SCENARIO")
            if db.execute("SELECT COUNT(*) FROM commands").fetchone()[0] >= receipt_limit:
                # Do not expire a key then silently execute it again.
                raise ControlError(503, "CONTROL_RECEIPT_CAPACITY")
            state = (
                "PAUSED"
                if action == "pause"
                else "RUNNING"
                if action == "resume"
                else row["stream_state"]
            )
            scenario = body["scenario"] if action == "scenario" else row["scenario"]
            changed = state != row["stream_state"] or scenario != row["scenario"]
            if changed:
                db.execute(
                    "UPDATE machines SET stream_state=?,scenario=?,"
                    "control_version=control_version+1,updated_at=?,last_controlled_by=?,"
                    "scenario_step=? WHERE machine_id=?",
                    (
                        state,
                        scenario,
                        utc(),
                        actor,
                        0 if scenario != row["scenario"] else row["scenario_step"],
                        machine,
                    ),
                )
            response = self.public(
                db.execute("SELECT * FROM machines WHERE machine_id=?", (machine,)).fetchone()
            )
            db.execute(
                "INSERT INTO commands VALUES (?,?,?,?,?)",
                (actor, key, fingerprint, canonical(response), time.time()),
            )
            db.execute(
                "INSERT INTO audit(created_at,actor,action,target,detail) VALUES (?,?,?,?,?)",
                (
                    utc(),
                    actor,
                    action,
                    machine,
                    canonical(
                        {"key": key, "body": body, "control_version": response["control_version"]}
                    ),
                ),
            )
            return response

    def generate_many(
        self,
        machines: list[str] | tuple[str, ...],
        build,
        *,
        max_records: int,
        max_bytes: int,
        max_records_per_machine: int | None = None,
        on_drop=None,
        now: float | None = None,
    ) -> list[dict]:
        if not machines:
            return []
        now = time.time() if now is None else now
        unique = tuple(dict.fromkeys(machines))
        placeholders = ",".join("?" for _ in unique)

        def record_drop(reason: str, machine_id: str, pending_count: int, pending_bytes: int):
            if on_drop is not None:
                on_drop(reason)
            log(
                "sensor_spool_dropped",
                level=logging.WARNING,
                reason=reason,
                machine_id=machine_id,
                pending_records=pending_count,
                pending_bytes=pending_bytes,
            )

        with self.lock:
            running = self.db.execute(
                f"SELECT COUNT(*) FROM machines WHERE machine_id IN ({placeholders}) "
                "AND stream_state='RUNNING'",
                unique,
            ).fetchone()[0]
            if not running:
                return []
            ordered = list(machines)
            if len(ordered) > 1:
                offset = self._rr_offset % len(ordered)
                ordered = ordered[offset:] + ordered[:offset]
                self._rr_offset = (offset + 1) % len(ordered)
            with self.transaction() as db:
                global_row = db.execute(
                    "SELECT pending_count, pending_bytes FROM spool_stats "
                    "WHERE machine_id='__global__'"
                ).fetchone()
                count = global_row["pending_count"] if global_row else 0
                size = global_row["pending_bytes"] if global_row else 0
                if count >= max_records:
                    record_drop("global_records", ordered[0], count, size)
                    return []
                per_machine_counts = {
                    r["machine_id"]: r["pending_count"]
                    for r in db.execute(
                        f"SELECT machine_id, pending_count FROM spool_stats "
                        f"WHERE machine_id IN ({placeholders})",
                        unique,
                    )
                }
                machine_total = db.execute("SELECT COUNT(*) FROM machines").fetchone()[0]
                rows = {
                    r["machine_id"]: dict(r)
                    for r in db.execute(
                        f"SELECT * FROM machines WHERE machine_id IN ({placeholders})", unique
                    )
                }
                events: list[dict] = []
                for machine in ordered:
                    if count >= max_records:
                        record_drop("global_records", machine, count, size)
                        break
                    row = rows.get(machine)
                    if not row or row["stream_state"] != "RUNNING":
                        continue
                    if (
                        max_records_per_machine is not None
                        and per_machine_counts.get(machine, 0) >= max_records_per_machine
                    ):
                        record_drop("machine_records", machine, count, size)
                        continue
                    payload = build(dict(row))
                    if payload is None:
                        continue
                    sequence = row["sequence_number"] + 1
                    payload.update(sequence_number=sequence)
                    payload.setdefault("event_id", f"evt_{uuid4()}")
                    payload.setdefault("correlation_id", f"cor_{uuid4()}")
                    encoded = canonical(payload)
                    byte_size = len(encoded.encode())
                    if size + byte_size > max_bytes:
                        record_drop("global_bytes", machine, count, size)
                        continue
                    db.execute(
                        "INSERT INTO spool(event_id,machine_id,payload,size,created_at,next_at) "
                        "VALUES (?,?,?,?,?,?)",
                        (payload["event_id"], machine, encoded, byte_size, now, now),
                    )
                    db.execute(
                        "UPDATE machines SET "
                        "sequence_number=?,row_cursor=row_cursor+?,scenario_step=scenario_step+1 "
                        "WHERE machine_id=?",
                        (sequence, machine_total, machine),
                    )
                    self._adjust_stats(db, machine, 1, byte_size)
                    row["sequence_number"] = sequence
                    row["row_cursor"] += machine_total
                    row["scenario_step"] += 1
                    per_machine_counts[machine] = per_machine_counts.get(machine, 0) + 1
                    count += 1
                    size += byte_size
                    events.append(payload)
                return events

    def generate(
        self,
        machine: str,
        build,
        *,
        max_records: int,
        max_bytes: int,
        max_records_per_machine: int | None = None,
        on_drop=None,
        now: float | None = None,
    ) -> dict | None:
        events = self.generate_many(
            [machine],
            build,
            max_records=max_records,
            max_bytes=max_bytes,
            max_records_per_machine=max_records_per_machine,
            on_drop=on_drop,
            now=now,
        )
        return events[0] if events else None

    def due(
        self,
        now: float,
        ttl: float,
        limit: int,
        *,
        exclude_machines: tuple[str, ...] | list[str] = (),
        quarantine_max_records: int = 10_000,
    ) -> list[dict]:
        with self.transaction() as db:
            expired_rows = db.execute(
                "SELECT * FROM spool WHERE status='pending' AND created_at<? ORDER BY id",
                (now - ttl,),
            ).fetchall()
            if expired_rows:
                for row in expired_rows:
                    db.execute(
                        "INSERT OR REPLACE INTO spool_quarantine "
                        "(event_id,machine_id,payload,size,created_at,attempts,"
                        "status,error,quarantined_at) VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            row["event_id"],
                            row["machine_id"],
                            row["payload"],
                            row["size"],
                            row["created_at"],
                            row["attempts"],
                            "expired",
                            "TTL_EXPIRED",
                            now,
                        ),
                    )
                    self._adjust_stats(db, row["machine_id"], -1, -row["size"])
                db.execute(
                    "DELETE FROM spool WHERE status='pending' AND created_at<?", (now - ttl,)
                )
                self._trim_quarantine(db, quarantine_max_records)
            excluded = tuple(dict.fromkeys(exclude_machines))
            clause = ""
            params: list[object] = [now]
            if excluded:
                placeholders = ",".join("?" for _ in excluded)
                clause = f"AND m.machine_id NOT IN ({placeholders}) "
                params.extend(excluded)
            params.append(limit)
            # One oldest pending row per machine; another machine's backoff never blocks this one.
            rows = db.execute(
                f"""SELECT s.* FROM machines m
                JOIN spool s ON s.id = (
                    SELECT id FROM spool
                    WHERE machine_id = m.machine_id AND status = 'pending'
                    ORDER BY id LIMIT 1
                )
                WHERE s.next_at<=? {clause}
                AND NOT EXISTS (SELECT 1 FROM spool blocked WHERE blocked.machine_id=m.machine_id
                    AND blocked.status='auth')
                ORDER BY s.id LIMIT ?""",
                params,
            )
            return [dict(row) for row in rows]

    def finish(
        self,
        event_id: str,
        *,
        status: str,
        error: str | None = None,
        next_at: float = 0,
        quarantine_max_records: int = 10_000,
    ):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM spool WHERE event_id=?", (event_id,)).fetchone()
            if not row:
                return
            was_pending = row["status"] == "pending"
            if status == "accepted":
                db.execute("DELETE FROM spool WHERE event_id=?", (event_id,))
                if was_pending:
                    self._adjust_stats(db, row["machine_id"], -1, -row["size"])
            elif status == "rejected":
                db.execute(
                    "INSERT OR REPLACE INTO spool_quarantine "
                    "(event_id,machine_id,payload,size,created_at,attempts,"
                    "status,error,quarantined_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        row["event_id"],
                        row["machine_id"],
                        row["payload"],
                        row["size"],
                        row["created_at"],
                        row["attempts"] + 1,
                        "rejected",
                        error,
                        time.time(),
                    ),
                )
                db.execute("DELETE FROM spool WHERE event_id=?", (event_id,))
                if was_pending:
                    self._adjust_stats(db, row["machine_id"], -1, -row["size"])
                self._trim_quarantine(db, quarantine_max_records)
            else:
                db.execute(
                    "UPDATE spool SET status=?,error=?,next_at=?,attempts=attempts+1 WHERE "
                    "event_id=?",
                    (status, error, next_at, event_id),
                )
                if was_pending and status != "pending":
                    self._adjust_stats(db, row["machine_id"], -1, -row["size"])

    def summary(self) -> dict:
        with self.transaction() as db:
            stats = db.execute(
                "SELECT pending_count, pending_bytes FROM spool_stats WHERE machine_id='__global__'"
            ).fetchone()
            pending_count = stats["pending_count"] if stats else 0
            pending_bytes = stats["pending_bytes"] if stats else 0
            counts: dict[str, int] = {}
            if pending_count > 0:
                counts["pending"] = pending_count
            for row in db.execute(
                "SELECT status, COUNT(*) FROM spool WHERE status!='pending' GROUP BY status"
            ):
                counts[row[0]] = row[1]
            for row in db.execute("SELECT status, COUNT(*) FROM spool_quarantine GROUP BY status"):
                counts[row[0]] = counts.get(row[0], 0) + row[1]
            oldest = db.execute(
                "SELECT MIN(created_at) FROM spool WHERE status='pending'"
            ).fetchone()[0]
            return {"counts": counts, "bytes": pending_bytes, "oldest": oldest}

    def inspect(self) -> list[dict]:
        with self.transaction() as db:
            rows = [
                dict(r)
                for r in db.execute(
                    "SELECT event_id,machine_id,status,attempts,error,created_at FROM "
                    "spool ORDER BY id LIMIT 100"
                )
            ]
            if len(rows) < 100:
                rows.extend(
                    dict(r)
                    for r in db.execute(
                        "SELECT event_id,machine_id,status,attempts,error,created_at FROM "
                        "spool_quarantine ORDER BY id LIMIT ?",
                        (100 - len(rows),),
                    )
                )
            return rows

    def repair(self, event_id: str, action: str, reason: str):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM spool WHERE event_id=?", (event_id,)).fetchone()
            from_quarantine = False
            if not row:
                row = db.execute(
                    "SELECT * FROM spool_quarantine WHERE event_id=?", (event_id,)
                ).fetchone()
                from_quarantine = True
            if not row:
                raise ValueError("Spool event not found")
            prev_status = row["status"]
            if action == "retry":
                now = time.time()
                if from_quarantine:
                    db.execute(
                        "INSERT INTO spool(event_id,machine_id,payload,size,created_at,"
                        "next_at,attempts,status,error) VALUES (?,?,?,?,?,?,?,?,NULL)",
                        (
                            row["event_id"],
                            row["machine_id"],
                            row["payload"],
                            row["size"],
                            now,
                            now,
                            row["attempts"],
                            "pending",
                        ),
                    )
                    db.execute("DELETE FROM spool_quarantine WHERE event_id=?", (event_id,))
                    self._adjust_stats(db, row["machine_id"], 1, row["size"])
                else:
                    db.execute(
                        "UPDATE spool SET status='pending',error=NULL,next_at=?,created_at=? "
                        "WHERE event_id=?",
                        (now, now, event_id),
                    )
                    if prev_status != "pending":
                        self._adjust_stats(db, row["machine_id"], 1, row["size"])
            else:
                if from_quarantine:
                    db.execute("DELETE FROM spool_quarantine WHERE event_id=?", (event_id,))
                else:
                    db.execute("DELETE FROM spool WHERE event_id=?", (event_id,))
                    if prev_status == "pending":
                        self._adjust_stats(db, row["machine_id"], -1, -row["size"])
            db.execute(
                "INSERT INTO audit(created_at,actor,action,target,detail) VALUES (?,?,?,?,?)",
                (
                    utc(),
                    "local-maintainer",
                    "spool-" + action,
                    event_id,
                    canonical({"reason": reason, "previous_status": prev_status}),
                ),
            )

    def close(self):
        if not self.closed:
            if hasattr(self, "db"):
                self.db.close()
            fcntl.flock(self.file_lock, fcntl.LOCK_UN)
            self.file_lock.close()
            self.closed = True
