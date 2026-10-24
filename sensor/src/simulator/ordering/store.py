"""Single-controller SQLite journal: atomic sequence + payload + control receipts.

The database must live on a persistent local volume. A process lock deliberately
rejects multiple controllers; SQLite on shared/network filesystems is unsupported.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


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
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.closed = False
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
            self.db.execute("PRAGMA synchronous=FULL")
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
                if old and old[0] != "1":
                    raise ValueError(
                        "Unsupported Sensor state schema; explicit migration is required"
                    )
                db.execute("INSERT OR IGNORE INTO meta VALUES ('schema_version','1')")
        except Exception:
            self.close()
            raise

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

    def generate(
        self, machine: str, build, *, max_records: int, max_bytes: int, now: float | None = None
    ) -> dict | None:
        now = time.time() if now is None else now
        with self.transaction() as db:
            row = db.execute("SELECT * FROM machines WHERE machine_id=?", (machine,)).fetchone()
            if not row or row["stream_state"] != "RUNNING":
                return None
            count, size = db.execute("SELECT COUNT(*),COALESCE(SUM(size),0) FROM spool").fetchone()
            if count >= max_records:
                return None
            payload = build(dict(row))
            if payload is None:
                return None
            sequence = row["sequence_number"] + 1
            payload.update(sequence_number=sequence)
            payload.setdefault("event_id", f"evt_{uuid4()}")
            payload.setdefault("correlation_id", f"cor_{uuid4()}")
            encoded = canonical(payload)
            byte_size = len(encoded.encode())
            if size + byte_size > max_bytes:
                return None
            db.execute(
                "INSERT INTO spool(event_id,machine_id,payload,size,created_at,next_at) "
                "VALUES (?,?,?,?,?,?)",
                (payload["event_id"], machine, encoded, byte_size, now, now),
            )
            db.execute(
                "UPDATE machines SET "
                "sequence_number=?,row_cursor=row_cursor+?,scenario_step=scenario_step+1 "
                "WHERE machine_id=?",
                (sequence, db.execute("SELECT COUNT(*) FROM machines").fetchone()[0], machine),
            )
            return payload

    def due(self, now: float, ttl: float, limit: int) -> list[dict]:
        with self.transaction() as db:
            db.execute(
                "UPDATE spool SET status='expired',error='TTL_EXPIRED' WHERE "
                "status='pending' AND created_at<?",
                (now - ttl,),
            )
            # One oldest pending row per machine; another machine's backoff never blocks this one.
            rows = db.execute(
                """SELECT s.* FROM spool s WHERE status='pending' AND next_at<=?
                AND NOT EXISTS (SELECT 1 FROM spool blocked WHERE blocked.machine_id=s.machine_id
                    AND blocked.status='auth')
                AND NOT EXISTS (SELECT 1 FROM spool older WHERE older.machine_id=s.machine_id
                    AND older.status='pending' AND older.id<s.id) ORDER BY s.id LIMIT ?""",
                (now, limit),
            )
            return [dict(row) for row in rows]

    def finish(self, event_id: str, *, status: str, error: str | None = None, next_at: float = 0):
        with self.transaction() as db:
            if status == "accepted":
                db.execute("DELETE FROM spool WHERE event_id=?", (event_id,))
            else:
                db.execute(
                    "UPDATE spool SET status=?,error=?,next_at=?,attempts=attempts+1 WHERE "
                    "event_id=?",
                    (status, error, next_at, event_id),
                )

    def summary(self) -> dict:
        with self.transaction() as db:
            counts = dict(db.execute("SELECT status,COUNT(*) FROM spool GROUP BY status"))
            row = db.execute(
                "SELECT COALESCE(SUM(size),0),MIN(CASE WHEN status='pending' THEN "
                "created_at END) FROM spool"
            ).fetchone()
            return {"counts": counts, "bytes": row[0], "oldest": row[1]}

    def inspect(self) -> list[dict]:
        with self.transaction() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT event_id,machine_id,status,attempts,error,created_at FROM "
                    "spool ORDER BY id LIMIT 100"
                )
            ]

    def repair(self, event_id: str, action: str, reason: str):
        with self.transaction() as db:
            row = db.execute("SELECT status FROM spool WHERE event_id=?", (event_id,)).fetchone()
            if not row:
                raise ValueError("Spool event not found")
            if action == "retry":
                db.execute(
                    "UPDATE spool SET status='pending',error=NULL,next_at=?,created_at=? "
                    "WHERE event_id=?",
                    (time.time(), time.time(), event_id),
                )
            else:
                db.execute("DELETE FROM spool WHERE event_id=?", (event_id,))
            db.execute(
                "INSERT INTO audit(created_at,actor,action,target,detail) VALUES (?,?,?,?,?)",
                (
                    utc(),
                    "local-maintainer",
                    "spool-" + action,
                    event_id,
                    canonical({"reason": reason, "previous_status": row[0]}),
                ),
            )

    def close(self):
        if not self.closed:
            if hasattr(self, "db"):
                self.db.close()
            fcntl.flock(self.file_lock, fcntl.LOCK_UN)
            self.file_lock.close()
            self.closed = True
