import pytest

from simulator.dataset import DatasetReader
from simulator.event_factory import EventFactory
from simulator.fault_injection import FaultInjector, FaultMode
from simulator.scenarios import ScenarioError, ScenarioLoader, ScenarioTransformer


def transformer(scenario_dir, name, seed=None):
    return ScenarioTransformer(ScenarioLoader.load(scenario_dir / f"{name}.yaml"), seed=seed)


@pytest.mark.parametrize("name", ["normal", "sudden-drift", "gradual-drift", "categorical-drift"])
def test_builtin_scenarios_stay_contract_valid(sample_csv, scenario_dir, validator, name):
    features = next(iter(DatasetReader(sample_csv))).features
    transform = transformer(scenario_dir, name)
    factory = EventFactory(seed=42, machine_count=5, interval_ms=1000)
    for sequence in range(min(transform.scenario.event_count, 1000)):
        shifted, _ = transform.apply(features, sequence)
        validator.validate(factory.create(shifted, sequence).model_dump(mode="json"))
    assert features.torque_nm == 42.8  # Original data remains untouched.


def test_normal_is_exact_negative_control(sample_csv, scenario_dir):
    records = list(DatasetReader(sample_csv))
    normal = transformer(scenario_dir, "normal")
    assert [normal.apply(row.features, i)[0] for i, row in enumerate(records)] == [
        row.features for row in records
    ]


def test_sudden_and_gradual_phase_boundaries(sample_csv, scenario_dir):
    features = next(iter(DatasetReader(sample_csv))).features
    sudden = transformer(scenario_dir, "sudden-drift")
    gradual = transformer(scenario_dir, "gradual-drift")
    assert sudden.apply(features, 299)[0] == features
    assert sudden.apply(features, 300)[0].torque_nm == pytest.approx(50.8)
    assert gradual.apply(features, 299)[0] == features
    assert gradual.apply(features, 300)[0] == features
    assert gradual.apply(features, 999)[0].torque_nm == pytest.approx(50.8)
    assert gradual.apply(features, 999)[0].rotational_speed_rpm == pytest.approx(1551 * 0.93)
    middle = gradual.apply(features, 650)[0]
    assert 42.8 < middle.torque_nm < 50.8
    with pytest.raises(ScenarioError, match="exhausted"):
        gradual.apply(features, 1000)


def test_categorical_distribution_seed_and_numeric_preservation(sample_csv, scenario_dir):
    features = next(iter(DatasetReader(sample_csv))).features
    one = transformer(scenario_dir, "categorical-drift")
    two = transformer(scenario_dir, "categorical-drift")
    first = [one.apply(features, i)[0] for i in range(1000)]
    second = [two.apply(features, i)[0] for i in range(1000)]
    assert first == second
    assert all(value.product_type == "L" for value in first[:300])
    h_share = sum(value.product_type == "H" for value in first[300:]) / 700
    assert 0.70 < h_share < 0.90
    assert all(value.torque_nm == features.torque_nm for value in first)
    different_seed = transformer(scenario_dir, "categorical-drift", seed=13)
    assert first != [different_seed.apply(features, i)[0] for i in range(1000)]


@pytest.mark.parametrize(
    "transform",
    [
        "Machine failure: {type: shift, delta: 1}",
        "torque_nm: {type: categorical_mix, probabilities: {L: 1}}",
        "product_type: {type: shift, delta: 1}",
        "product_type: {type: categorical_mix, probabilities: {L: 0.1}}",
        "torque_nm: {type: scale, factor: -1}",
    ],
)
def test_bad_scenario_config_fails_early(tmp_path, transform):
    path = tmp_path / "bad.yaml"
    path.write_text(
        f"name: bad\nseed: 42\nphases:\n  - name: bad\n    events: 2\n"
        f"    transforms:\n      {transform}\n"
    )
    with pytest.raises(ScenarioError):
        ScenarioLoader.load(path)


def test_invalid_drift_transform_is_not_clipped(sample_csv, tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(
        "name: bad\nseed: 42\nphases:\n  - name: bad\n    events: 1\n"
        "    transforms:\n      rotational_speed_rpm: {type: shift, delta: -5000}\n"
    )
    transform = ScenarioTransformer(ScenarioLoader.load(path))
    with pytest.raises(ScenarioError, match="invalid features"):
        transform.apply(next(iter(DatasetReader(sample_csv))).features, 0)


@pytest.mark.parametrize("mode", list(FaultMode))
def test_fault_injection_is_separate_and_non_mutating(sample_csv, validator, mode):
    features = next(iter(DatasetReader(sample_csv))).features
    factory = EventFactory(seed=42, machine_count=1, interval_ms=0)
    event = factory.create(features, 0).model_dump(mode="json")
    before = factory.create(features, 0).features.model_dump()
    injected = FaultInjector(mode, every=2).apply(event, 1)
    assert event["features"] == before
    assert injected.payload["event_id"] == event["event_id"]
    assert validator.is_valid(injected.payload) == (not injected.expect_rejection)
    assert injected.duplicate == (mode == FaultMode.DUPLICATE_EVENT)
    assert FaultInjector(mode, every=2).apply(event, 0).payload == event
