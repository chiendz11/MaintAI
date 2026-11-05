"""The same pure state transition is used by live and recovery writers."""

import itertools
import json
from pathlib import Path

import pytest
from backend.shared.projections.reducer import reduce_projection
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict

ROOT = Path(__file__).resolve().parents[3]


def event(name, sequence):
    value = json.loads((ROOT / f"contracts/kafka/examples/{name}.json").read_text())
    value.update(event_id=f"e-{sequence}", sequence_number=sequence)
    if "result_id" in value:
        value["result_id"] += f"-{sequence}"
    topic = "sensor.readings" if name == "sensor-reading" else "inference.results"
    return default_contracts().decode(topic, canonical_json(value))


def test_arrival_permutations_converge_without_mutating_the_input():
    events = [
        event("sensor-reading", 2**80),
        event("completed", 2**80),
        event("invalid", 2**80 + 1),
    ]
    outputs = []
    for order in itertools.permutations(events):
        rows = {}
        validations = {}
        for item in order:
            before = dict(rows)
            if item.topic == "inference.results":
                validations[item.source_event_id] = item.payload["validation_status"]
            result = reduce_projection(
                rows, item, source_validation=validations.get(item.source_event_id)
            )
            assert rows == before
            rows = result.rows
        outputs.append(rows)
    assert all(rows == outputs[0] for rows in outputs)
    assert json.loads(outputs[0]["latest_processing"]["snapshot"])["processing_status"] == "INVALID"
    assert outputs[0]["latest_valid_prediction"]["sequence_number"] == 2**80


def test_pending_duplicate_does_not_replace_original_receipt():
    reading = event("sensor-reading", 1)
    first = reduce_projection({}, reading, source_validation=None)
    repeated = reduce_projection(first.rows, reading, source_validation=None)
    assert not repeated.changed
    assert not repeated.telemetry_visible


def test_same_sequence_contradiction_is_rejected():
    first = reduce_projection({}, event("completed", 1), source_validation=None)
    with pytest.raises(IdentityConflict):
        reduce_projection(first.rows, event("unavailable", 1), source_validation=None)
