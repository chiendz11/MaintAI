from datetime import UTC, datetime, timedelta

import pytest

from drift_monitor.config import DriftConfig
from drift_monitor.models import Features, RuntimeSample
from drift_monitor.reference import build_reference


@pytest.fixture
def samples():
    rows = []
    for i in range(500):
        rows.append(
            RuntimeSample(
                event_id=f"evt_fixture_{i:04d}",
                received_at=datetime(2026, 10, 12, tzinfo=UTC) + timedelta(seconds=i),
                features=Features(
                    product_type=["L", "L", "L", "M", "H"][i % 5],
                    air_temperature_k=298 + i % 5,
                    process_temperature_k=308 + i % 5,
                    rotational_speed_rpm=1400 + (i % 5) * 50,
                    torque_nm=30 + (i % 5) * 5,
                    tool_wear_min=(i % 5) * 10,
                ),
            )
        )
    return rows


@pytest.fixture
def reference(samples):
    return build_reference(
        [sample.features for sample in samples],
        model_version="fixture-model-v1",
        dataset_version="fixture-ai4i-v1",
        reference_source="test_fixture",
    )


@pytest.fixture
def config(reference, tmp_path):
    path = tmp_path / "reference.json"
    path.write_text(reference.model_dump_json())
    return DriftConfig(
        reference_path=path,
        model_version="fixture-model-v1",
        dataset_version="fixture-ai4i-v1",
    )
