import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from simulator.dataset import DatasetError, DatasetReader
from simulator.event_factory import EventFactory
from simulator.models import Features, SensorEvent, sensor_event_schema


def factory(**overrides):
    return EventFactory(
        **{
            "seed": 42,
            "machine_count": 5,
            "interval_ms": 1000,
            "start_time": datetime(2026, 10, 12, 4, 30, 15, tzinfo=UTC),
            "run_id": "test-run",
            **overrides,
        }
    )


def test_ai4i_mapping_and_canonical_wire_contract(sample_csv, validator):
    records = list(DatasetReader(sample_csv))
    assert len(records) == 5
    record = records[0]
    assert record.features.model_dump() == {
        "product_type": "L",
        "air_temperature_k": 298.1,
        "process_temperature_k": 308.6,
        "rotational_speed_rpm": 1551.0,
        "torque_nm": 42.8,
        "tool_wear_min": 0.0,
    }
    assert records[-1].ground_truth == 1
    payload = factory().create(record.features, 0).model_dump(mode="json")
    validator.validate(payload)
    assert set(payload) == {"event_id", "machine_id", "received_at", "source", "features"}
    assert payload["source"] == "AI4I_SIMULATOR"
    assert payload["received_at"] == "2026-10-12T04:30:15Z"
    forbidden = {
        "machine_failure",
        "Machine failure",
        "TWF",
        "HDF",
        "PWF",
        "OSF",
        "RNF",
        "UDI",
        "Product ID",
        "event_id",
        "machine_id",
        "received_at",
    }
    assert forbidden.isdisjoint(payload["features"])


def test_seeded_round_robin_payload_clock_and_run_identity(sample_csv):
    features = next(iter(DatasetReader(sample_csv))).features
    first, second = factory(), factory()
    one = [first.create(features, i) for i in range(50)]
    two = [second.create(features, i) for i in range(50)]
    assert one == two
    assert len({event.event_id for event in one}) == 50
    machine_order = [event.machine_id for event in one[:5]]
    assert set(machine_order) == {f"M-{i:03d}" for i in range(1, 6)}
    assert [event.machine_id for event in one] == machine_order * 10
    assert one[-1].received_at - one[0].received_at == timedelta(milliseconds=9800)
    assert one[5].received_at - one[0].received_at == timedelta(seconds=1)
    assert factory(run_id="other-run").create(features, 0).event_id != one[0].event_id
    assert factory(run_id=None).create(features, 0).event_id != one[0].event_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("rotational_speed_rpm", 0),
        ("rotational_speed_rpm", -1),
        ("tool_wear_min", -1),
        ("torque_nm", float("inf")),
        ("air_temperature_k", float("nan")),
        ("process_temperature_k", float("-inf")),
        ("torque_nm", "42.8"),
        ("tool_wear_min", True),
        ("product_type", "X"),
    ],
)
def test_features_reject_bad_contract_values(sample_csv, field, value):
    payload = next(iter(DatasetReader(sample_csv))).features.model_dump()
    payload[field] = value
    with pytest.raises(ValidationError):
        Features.model_validate(payload)


def test_extra_labels_and_missing_source_rejected(sample_csv, validator):
    payload = (
        factory().create(next(iter(DatasetReader(sample_csv))).features, 0).model_dump(mode="json")
    )
    payload["features"]["TWF"] = 1
    assert not validator.is_valid(payload)
    with pytest.raises(ValidationError):
        SensorEvent.model_validate_json(json.dumps(payload))
    del payload["features"]["TWF"]
    del payload["source"]
    assert not validator.is_valid(payload)
    with pytest.raises(ValidationError):
        SensorEvent.model_validate_json(json.dumps(payload))


def test_naive_timestamp_rejected(sample_csv, validator):
    payload = (
        factory().create(next(iter(DatasetReader(sample_csv))).features, 0).model_dump(mode="json")
    )
    payload["received_at"] = "2026-10-12T04:30:15"
    assert not validator.is_valid(payload)
    with pytest.raises(ValidationError):
        SensorEvent.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize(
    "change",
    [
        lambda text: text.replace("Torque [Nm]", "Unknown torque"),
        lambda text: text.replace(",1551,", ",NaN,", 1),
        lambda text: text.replace(",1551,", ",0,", 1),
        lambda text: text.replace(",42.8,", ",missing,", 1),
        lambda text: text.splitlines()[0] + "\n",
    ],
)
def test_bad_original_dataset_is_reported_not_imputed(sample_csv, tmp_path, change):
    path = tmp_path / "bad.csv"
    path.write_text(change(sample_csv.read_text()))
    with pytest.raises(DatasetError):
        list(DatasetReader(path))


def test_missing_dataset_is_actionable(tmp_path):
    with pytest.raises(DatasetError, match="Cannot read dataset"):
        list(DatasetReader(tmp_path / "missing.csv"))


def test_committed_contract_matches_export(validator):
    assert validator.schema == sensor_event_schema()
