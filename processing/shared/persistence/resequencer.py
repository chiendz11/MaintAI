"""Durable source-order Admission Ledger and Alert Resequencer for Hybrid Ordering."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from maintai_messaging.errors import ConfigurationError, IdentityConflict
from maintai_messaging.ordering import require_ordering


@dataclass(frozen=True)
class AdmissionTicket:
    admission_seq: int | None
    sequence_number: int
    replayed: bool = False
    legacy_terminal: bool = False


class AlertResequencer:
    """Serializes alert.signals emission per machine while allowing parallel inference.

    Canonical Lock Hierarchy (strict outer -> inner):
      1. maintai_processing_assignments (FOR SHARE)
      2. processing_admission_cursor (FOR UPDATE)
      3. maintai_processing_machine_cursor (FOR UPDATE)
      4. processing_source_cursor (FOR UPDATE)
      5. maintai_outbox_keys (FOR UPDATE)
    """

    @staticmethod
    def _ensure_admission_cursor(connection, *, consumer_group, machine_id, topic, partition):
        row = connection.execute(
            "SELECT * FROM processing_admission_cursor "
            "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
            (consumer_group, machine_id),
        ).fetchone()
        if row is not None:
            return row

        machine_cur = connection.execute(
            "SELECT * FROM maintai_processing_machine_cursor "
            "WHERE consumer_group=%s AND machine_id=%s",
            (consumer_group, machine_id),
        ).fetchone()
        source_cur = connection.execute(
            "SELECT * FROM processing_source_cursor WHERE machine_id=%s",
            (machine_id,),
        ).fetchone()
        if source_cur is None and connection.execute(
            "SELECT 1 FROM sensor_events WHERE machine_id=%s LIMIT 1", (machine_id,)
        ).fetchone():
            raise ConfigurationError(
                "Legacy owner history requires reviewed source cursor bootstrap"
            )

        seed_seq = -1
        seed_event = None
        seed_topic = topic
        seed_partition = partition
        if machine_cur is not None:
            seed_seq = int(machine_cur["last_sequence"])
            seed_event = machine_cur["last_event_id"]
            seed_topic = machine_cur["topic"]
            seed_partition = machine_cur["source_partition"]
        elif source_cur is not None:
            seed_topic = source_cur["topic"]
            seed_partition = source_cur["source_partition"]

        connection.execute(
            """INSERT INTO processing_admission_cursor
            (consumer_group,machine_id,topic,source_partition,last_admitted_sequence,
             last_admitted_event_id,next_admission_seq,alert_frontier_seq)
            VALUES (%s,%s,%s,%s,%s,%s,1,0) ON CONFLICT DO NOTHING""",
            (consumer_group, machine_id, seed_topic, seed_partition, seed_seq, seed_event),
        )
        row = connection.execute(
            "SELECT * FROM processing_admission_cursor "
            "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
            (consumer_group, machine_id),
        ).fetchone()
        if source_cur is None:
            connection.execute(
                """INSERT INTO processing_source_cursor
                (machine_id,topic,source_partition) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
                (machine_id, seed_topic, seed_partition),
            )
        return row

    def admit(self, connection, *, consumer_group, event, record) -> AdmissionTicket:
        require_ordering(connection)
        machine_id = event.key
        sequence = event.payload["sequence_number"]
        cursor = self._ensure_admission_cursor(
            connection,
            consumer_group=consumer_group,
            machine_id=machine_id,
            topic=record.topic,
            partition=record.partition,
        )

        inbox_row = connection.execute(
            "SELECT identity_sha256 FROM maintai_kafka_inbox "
            "WHERE consumer_group=%s AND topic=%s AND message_id=%s",
            (consumer_group, event.topic, event.message_id),
        ).fetchone()
        if inbox_row and inbox_row["identity_sha256"] != event.identity_sha256:
            raise IdentityConflict("Reading identity has different canonical content")

        existing = connection.execute(
            "SELECT admission_seq,sequence_number,identity_sha256,disposition "
            "FROM processing_alert_ledger WHERE consumer_group=%s AND topic=%s AND message_id=%s",
            (consumer_group, event.topic, event.message_id),
        ).fetchone()
        if existing is not None:
            if existing["identity_sha256"] != event.identity_sha256:
                raise IdentityConflict("Admission ledger identity has different canonical content")
            replayed = inbox_row is not None or existing["disposition"] != "PENDING"
            return AdmissionTicket(
                admission_seq=int(existing["admission_seq"]),
                sequence_number=int(existing["sequence_number"]),
                replayed=replayed,
            )

        if (cursor["topic"], cursor["source_partition"]) != (record.topic, record.partition):
            raise IdentityConflict("Machine admission source partition requires reconciliation")

        if inbox_row is not None:
            return AdmissionTicket(
                admission_seq=None,
                sequence_number=int(sequence),
                replayed=True,
                legacy_terminal=True,
            )

        owner_row = connection.execute(
            """SELECT e.payload_hash, p.result_id FROM sensor_events e
            LEFT JOIN processing_results p ON p.event_id=e.event_id
            WHERE e.event_id=%s""",
            (event.message_id,),
        ).fetchone()
        if owner_row is not None:
            if owner_row["payload_hash"] != event.identity_sha256:
                raise IdentityConflict("Event identity has different canonical content")
            if owner_row["result_id"] is not None and sequence <= cursor["last_admitted_sequence"]:
                return AdmissionTicket(
                    admission_seq=None,
                    sequence_number=int(sequence),
                    replayed=True,
                    legacy_terminal=True,
                )

        if sequence <= cursor["last_admitted_sequence"]:
            raise IdentityConflict("Machine source order requires reconciliation")

        conflict_seq = connection.execute(
            "SELECT event_id FROM sensor_events WHERE machine_id=%s AND sequence_number=%s "
            "AND event_id <> %s LIMIT 1",
            (machine_id, sequence, event.message_id),
        ).fetchone()
        if conflict_seq is not None:
            raise IdentityConflict("Source sequence already belongs to a different event")

        admission_seq = int(cursor["next_admission_seq"])
        connection.execute(
            """INSERT INTO processing_alert_ledger
            (consumer_group,machine_id,admission_seq,topic,message_id,source_partition,
             source_offset,sequence_number,stream_epoch,identity_sha256,disposition)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')""",
            (
                consumer_group,
                machine_id,
                admission_seq,
                event.topic,
                event.message_id,
                record.partition,
                record.offset,
                sequence,
                event.payload["stream_epoch"],
                event.identity_sha256,
            ),
        )
        connection.execute(
            """UPDATE processing_admission_cursor
            SET last_admitted_sequence=%s,last_admitted_event_id=%s,
                next_admission_seq=next_admission_seq+1,updated_at=clock_timestamp()
            WHERE consumer_group=%s AND machine_id=%s""",
            (sequence, event.message_id, consumer_group, machine_id),
        )
        return AdmissionTicket(
            admission_seq=admission_seq,
            sequence_number=int(sequence),
            replayed=False,
        )

    def record_disposition(
        self,
        connection,
        *,
        consumer_group: str,
        machine_id: str,
        admission_seq: int,
        outcome: dict,
        should_alert: bool,
        factory,
        contracts,
        unavailable_alert_disposition: str = "BLOCKED",
    ) -> str:
        kind = outcome["kind"]
        signal_id = None
        signal_payload = None
        signal_sha256 = None
        if kind == "PREDICTION_COMPLETED":
            if should_alert:
                disposition = "SIGNAL_READY"
                reason = "POLICY_ALERT_REQUIRED"
                signal = factory.alert_signal(outcome)
                if signal is None:
                    raise IdentityConflict("Alert policy requires a valid signal")
                encoded_sig = contracts.encode("alert.signals", signal)
                signal_id = encoded_sig.message_id
                signal_payload = encoded_sig.wire_bytes
                signal_sha256 = hashlib.sha256(encoded_sig.wire_bytes).hexdigest()
            else:
                disposition = "NO_SIGNAL"
                reason = "POLICY_NON_ALERTING"
        elif kind == "DATA_INVALID":
            disposition = "NO_SIGNAL"
            reason = "DATA_INVALID_NON_ALERTING"
        elif kind == "INFERENCE_UNAVAILABLE":
            if unavailable_alert_disposition == "NO_SIGNAL":
                disposition = "NO_SIGNAL"
                reason = "UNAVAILABLE_OPT_IN_NO_SIGNAL"
            else:
                disposition = "BLOCKED"
                reason = "INFERENCE_UNAVAILABLE"
        else:
            raise IdentityConflict(f"Unsupported terminal outcome kind: {kind}")

        updated = connection.execute(
            """UPDATE processing_alert_ledger
            SET disposition=%s,disposition_reason=%s,signal_id=%s,signal_payload=%s,
                signal_sha256=%s,outcome_kind=%s,decided_at=clock_timestamp()
            WHERE consumer_group=%s AND machine_id=%s AND admission_seq=%s
              AND disposition='PENDING'
            RETURNING disposition""",
            (
                disposition,
                reason,
                signal_id,
                signal_payload,
                signal_sha256,
                kind,
                consumer_group,
                machine_id,
                admission_seq,
            ),
        ).fetchone()
        if updated is None:
            existing = connection.execute(
                "SELECT disposition,signal_sha256,outcome_kind FROM processing_alert_ledger "
                "WHERE consumer_group=%s AND machine_id=%s AND admission_seq=%s",
                (consumer_group, machine_id, admission_seq),
            ).fetchone()
            if existing is None:
                raise IdentityConflict("Missing admission ledger entry during Stage C")
            if existing["outcome_kind"] not in (None, kind) or (
                signal_sha256 is not None and existing["signal_sha256"] != signal_sha256
            ):
                raise IdentityConflict("Conflicting alert disposition update")
            return existing["disposition"]
        return updated["disposition"]

    def drain_frontier(
        self,
        connection,
        *,
        consumer_group: str,
        machine_id: str,
        topic: str,
        source_partition: int,
        contracts,
    ) -> dict:
        require_ordering(connection)
        cursor = connection.execute(
            "SELECT * FROM processing_admission_cursor "
            "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
            (consumer_group, machine_id),
        ).fetchone()
        if cursor is None:
            return {"emitted": 0, "resolved_no_signal": 0, "stalled_reason": None, "frontier": 0}

        frontier = int(cursor["alert_frontier_seq"])
        rows = connection.execute(
            "SELECT * FROM processing_alert_ledger "
            "WHERE consumer_group=%s AND machine_id=%s AND admission_seq > %s "
            "ORDER BY admission_seq ASC FOR UPDATE",
            (consumer_group, machine_id, frontier),
        ).fetchall()

        expected = frontier + 1
        ready_rows = []
        already_done_frontier = frontier
        stalled_reason = None
        for row in rows:
            adm_seq = int(row["admission_seq"])
            if adm_seq != expected:
                stalled_reason = "ADMISSION_GAP"
                break
            disp = row["disposition"]
            if disp in ("PENDING", "BLOCKED"):
                stalled_reason = disp
                break
            if disp in ("EMITTED", "RESOLVED_NO_SIGNAL"):
                if not ready_rows:
                    already_done_frontier = adm_seq
                expected += 1
                continue
            if disp in ("SIGNAL_READY", "NO_SIGNAL"):
                ready_rows.append(row)
                expected += 1
            else:
                stalled_reason = disp
                break

        emitted = 0
        resolved_no_signal = 0
        highest_frontier = already_done_frontier
        if ready_rows:
            connection.execute(
                "INSERT INTO maintai_processing_machine_cursor "
                "(consumer_group,machine_id,topic,source_partition) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (consumer_group, machine_id, topic, source_partition),
            )
            machine_cur = connection.execute(
                "SELECT * FROM maintai_processing_machine_cursor "
                "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
                (consumer_group, machine_id),
            ).fetchone()
            connection.execute(
                "INSERT INTO processing_source_cursor "
                "(machine_id,topic,source_partition) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (machine_id, topic, source_partition),
            )
            source_cur = connection.execute(
                "SELECT * FROM processing_source_cursor WHERE machine_id=%s FOR UPDATE",
                (machine_id,),
            ).fetchone()
            highest_seq = max(
                int(machine_cur["last_sequence"]),
                int(source_cur["last_sequence"]),
            )
            highest_event_id = machine_cur["last_event_id"]

            for row in ready_rows:
                adm_seq = int(row["admission_seq"])
                src_seq = int(row["sequence_number"])
                if row["disposition"] == "SIGNAL_READY":
                    raw_sig = bytes(row["signal_payload"])
                    digest = hashlib.sha256(raw_sig).hexdigest()
                    if digest != row["signal_sha256"]:
                        raise IdentityConflict("Stored alert signal checksum mismatch")
                    signal_id = row["signal_id"]
                    sig_event = contracts.decode("alert.signals", raw_sig, writer=True)
                    if sig_event.key != machine_id or sig_event.message_id != signal_id:
                        raise IdentityConflict("Stored alert signal identity mismatch")

                    connection.execute(
                        "INSERT INTO maintai_outbox_keys(topic,message_key) VALUES (%s,%s) "
                        "ON CONFLICT DO NOTHING",
                        ("alert.signals", machine_id),
                    )
                    connection.execute(
                        "SELECT last_allocated FROM maintai_outbox_keys "
                        "WHERE topic=%s AND message_key=%s FOR UPDATE",
                        ("alert.signals", machine_id),
                    ).fetchone()
                    old = connection.execute(
                        "SELECT payload_sha256 FROM maintai_kafka_outbox "
                        "WHERE topic=%s AND message_id=%s",
                        ("alert.signals", signal_id),
                    ).fetchone()
                    if old:
                        if old["payload_sha256"] != digest:
                            raise IdentityConflict(
                                "Outbox alert signal has different immutable content"
                            )
                    else:
                        order = connection.execute(
                            "UPDATE maintai_outbox_keys SET last_allocated=last_allocated+1 "
                            "WHERE topic=%s AND message_key=%s RETURNING last_allocated",
                            ("alert.signals", machine_id),
                        ).fetchone()["last_allocated"]
                        connection.execute(
                            """INSERT INTO maintai_kafka_outbox
                            (topic,message_id,message_key,payload,payload_sha256,dispatch_seq,
                             source_sequence_number)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            (
                                "alert.signals",
                                signal_id,
                                machine_id,
                                raw_sig,
                                digest,
                                order,
                                src_seq,
                            ),
                        )
                    connection.execute(
                        "UPDATE processing_alert_ledger "
                        "SET disposition='EMITTED',resolved_at=clock_timestamp() "
                        "WHERE consumer_group=%s AND machine_id=%s AND admission_seq=%s",
                        (consumer_group, machine_id, adm_seq),
                    )
                    emitted += 1
                else:
                    connection.execute(
                        "UPDATE processing_alert_ledger "
                        "SET disposition='RESOLVED_NO_SIGNAL',resolved_at=clock_timestamp() "
                        "WHERE consumer_group=%s AND machine_id=%s AND admission_seq=%s",
                        (consumer_group, machine_id, adm_seq),
                    )
                    resolved_no_signal += 1
                if src_seq > highest_seq:
                    highest_seq = src_seq
                    highest_event_id = row["message_id"]
                highest_frontier = adm_seq

            if highest_seq > int(machine_cur["last_sequence"]):
                connection.execute(
                    "UPDATE maintai_processing_machine_cursor "
                    "SET last_sequence=%s,last_event_id=%s "
                    "WHERE consumer_group=%s AND machine_id=%s",
                    (highest_seq, highest_event_id, consumer_group, machine_id),
                )
            if highest_seq > int(source_cur["last_sequence"]):
                connection.execute(
                    "UPDATE processing_source_cursor SET last_sequence=%s,last_event_id=%s "
                    "WHERE machine_id=%s",
                    (highest_seq, highest_event_id, machine_id),
                )

        if highest_frontier > frontier:
            connection.execute(
                "UPDATE processing_admission_cursor "
                "SET alert_frontier_seq=%s,updated_at=clock_timestamp() "
                "WHERE consumer_group=%s AND machine_id=%s",
                (highest_frontier, consumer_group, machine_id),
            )

        return {
            "emitted": emitted,
            "resolved_no_signal": resolved_no_signal,
            "stalled_reason": stalled_reason,
            "frontier": highest_frontier,
        }

    def resolve_blocked(
        self,
        connection,
        *,
        consumer_group: str,
        machine_id: str,
        admission_seq: int,
        action: str,
        actor: str,
        note: str,
        contracts,
    ) -> dict:
        if action != "SKIP_NO_SIGNAL":
            raise ValueError("Unsupported BLOCKED resolution action; expected SKIP_NO_SIGNAL")
        if not actor or not actor.strip():
            raise ValueError("Operator actor is required for BLOCKED resolution audit")
        require_ordering(connection)
        cursor = connection.execute(
            "SELECT * FROM processing_admission_cursor "
            "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
            (consumer_group, machine_id),
        ).fetchone()
        if cursor is None:
            raise IdentityConflict("Admission cursor does not exist for blocked entry")
        row = connection.execute(
            """UPDATE processing_alert_ledger
            SET disposition='NO_SIGNAL',
                disposition_reason='OPERATOR_RESOLVED_NO_SIGNAL',
                operator_actor=%s,
                operator_note=%s
            WHERE consumer_group=%s AND machine_id=%s AND admission_seq=%s
              AND disposition='BLOCKED'
            RETURNING topic,source_partition""",
            (actor.strip(), note or "", consumer_group, machine_id, admission_seq),
        ).fetchone()
        if row is None:
            raise IdentityConflict("Alert ledger entry is not in BLOCKED state")
        return self.drain_frontier(
            connection,
            consumer_group=consumer_group,
            machine_id=machine_id,
            topic=row["topic"],
            source_partition=row["source_partition"],
            contracts=contracts,
        )

    @staticmethod
    def list_blocked(connection, *, consumer_group: str | None = None) -> list[dict]:
        if consumer_group is None:
            return connection.execute(
                "SELECT consumer_group,machine_id,admission_seq,message_id,sequence_number,"
                "disposition_reason,outcome_kind,admitted_at,decided_at "
                "FROM processing_alert_ledger WHERE disposition='BLOCKED' "
                "ORDER BY decided_at,machine_id,admission_seq"
            ).fetchall()
        return connection.execute(
            "SELECT consumer_group,machine_id,admission_seq,message_id,sequence_number,"
            "disposition_reason,outcome_kind,admitted_at,decided_at "
            "FROM processing_alert_ledger WHERE consumer_group=%s AND disposition='BLOCKED' "
            "ORDER BY decided_at,machine_id,admission_seq",
            (consumer_group,),
        ).fetchall()
