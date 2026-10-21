"""Backend-owned table stand-ins for an isolated test DB; never imported by the worker."""

import psycopg
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from drift_monitor.models import Features


def create_fixture_tables(url):
    with psycopg.connect(url) as conn:
        conn.execute("""
            CREATE TABLE machines (machine_id text PRIMARY KEY, state text NOT NULL);
            CREATE TABLE sensor_events (
                event_id text PRIMARY KEY, machine_id text REFERENCES machines(machine_id),
                received_at timestamptz NOT NULL, source text NOT NULL,
                features_json jsonb NOT NULL, validation_status text NOT NULL,
                validation_errors_json jsonb NOT NULL DEFAULT '[]',
                validator_version text NOT NULL DEFAULT 'fixture-contract-v1',
                correlation_id text, created_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE TABLE predictions (
                prediction_id text PRIMARY KEY,
                event_id text UNIQUE REFERENCES sensor_events(event_id),
                label text NOT NULL, failure_probability double precision NOT NULL,
                model_version text NOT NULL, preprocessor_version text,
                model_threshold double precision, inference_ms double precision,
                predicted_at timestamptz NOT NULL DEFAULT now(), correlation_id text
            );
        """)


def seed_events(url, events):
    with psycopg.connect(url) as conn:
        for event in events:
            status = event.get("validation_status", "VALID")
            try:
                Features.model_validate(event["features"])
            except ValidationError:
                status = "INVALID"
            machine_id = event.get("machine_id", "M-001")
            conn.execute(
                "INSERT INTO machines VALUES (%s,'NORMAL') ON CONFLICT DO NOTHING", (machine_id,)
            )
            conn.execute(
                "INSERT INTO sensor_events (event_id,machine_id,received_at,source,features_json,"
                "validation_status) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    event["event_id"],
                    machine_id,
                    event["received_at"],
                    event.get("source", "AI4I_SIMULATOR"),
                    Jsonb(event["features"]),
                    status,
                ),
            )


def wire_events(samples):
    return [
        {
            "event_id": row.event_id,
            "received_at": row.received_at.isoformat(),
            "features": row.features.model_dump(),
        }
        for row in samples
    ]
