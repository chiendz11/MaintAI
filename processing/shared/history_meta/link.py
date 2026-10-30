"""Join archived receipts to verified hot evidence without trusting event ID alone."""

from processing.shared.persistence.evidence import history_record


class ProcessingLink:
    def __init__(self, source_factory):
        self.source_factory = source_factory

    def __call__(self, event):
        with self.source_factory() as connection:
            row = connection.execute(
                """SELECT e.*,r.result_id,r.kind,r.outcome,r.outcome_sha256,
                r.completed_at AS processed_at,
                p.prediction_id,p.label,p.failure_probability,p.model_threshold,p.model_identity,
                d.risk_decision_id,d.risk_score,d.policy_version,d.policy_sha256,d.machine_state
                FROM sensor_events e JOIN processing_results r USING(event_id)
                LEFT JOIN predictions p USING(event_id) LEFT JOIN risk_decisions d USING(event_id)
                WHERE e.event_id=%s AND e.machine_id=%s AND e.payload_hash=%s""",
                (event.payload["event_id"], event.payload["machine_id"], event.identity_sha256),
            ).fetchone()
        if row is None:
            return None
        value = history_record(row)
        outcome = value["outcome"]
        markers = []
        if outcome["kind"] == "PREDICTION_COMPLETED":
            markers.append(
                {
                    "kind": "PREDICTION",
                    "reference_id": outcome["prediction_id"],
                    "source_event_id": event.payload["event_id"],
                    "position_at": event.payload["received_at"],
                    "source_received_at": event.payload["received_at"],
                    "source_server_accepted_at": value["source_server_accepted_at"],
                    "processed_at": row["processed_at"].isoformat(),
                }
            )
        return {
            "result_id": value["result_id"],
            "validation_status": outcome["validation_status"],
            "validation_errors": outcome.get("validation_errors", []),
            "_markers": markers,
        }
