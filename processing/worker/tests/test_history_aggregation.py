"""Pure chart math: no DB, source timestamp selection, snapshots or HTTP wiring."""

import math
import sys
from datetime import UTC, datetime, timedelta, timezone

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import default_contracts
from processing.shared.history import aggregate_samples

START = datetime(2026, 10, 29, tzinfo=UTC)


def aggregate(samples=(), *, seconds=3, **options):
    return aggregate_samples(
        samples, start=START, end=START + timedelta(seconds=seconds), **options
    )


def test_preserves_spikes_and_empty_buckets_without_interpolation():
    result = aggregate(
        [(START, 1), (START + timedelta(milliseconds=100), 100), (START, 1)],
        resolution="1s",
    )
    assert result.resolution == "1s"
    assert len(result.buckets) == 3
    assert result.buckets[0] == {
        "start": START.isoformat(),
        "end": (START + timedelta(seconds=1)).isoformat(),
        "min": 1,
        "max": 100,
        "avg": 34,
        "count": 3,
        "invalid_count": 0,
    }
    assert result.buckets[1]["min"] is None
    assert result.buckets[1]["max"] is None
    assert result.buckets[1]["avg"] is None
    assert result.buckets[1]["count"] == 0


@pytest.mark.parametrize("value", [None, "12", True, False, {}, math.nan, math.inf, -math.inf])
def test_invalid_values_are_counted_without_numeric_coercion(value):
    bucket = aggregate([(START, value)], resolution="1s").buckets[0]
    assert bucket["count"] == 0
    assert bucket["invalid_count"] == 1
    assert bucket["avg"] is None


def test_range_and_bucket_boundaries_are_inclusive_exclusive():
    result = aggregate(
        [
            (START - timedelta(microseconds=1), 99),
            (START, 1),
            (START + timedelta(seconds=1), 2),
            (START + timedelta(seconds=3), 99),
        ],
        resolution="1s",
    )
    assert [b["count"] for b in result.buckets] == [1, 1, 0]
    assert [b["max"] for b in result.buckets] == [1, 2, None]


def test_utc_grid_clips_partial_first_and_last_buckets():
    start = START + timedelta(milliseconds=500)
    end = START + timedelta(seconds=2, milliseconds=200)
    result = aggregate_samples((), start=start, end=end, resolution="1s")
    assert len(result.buckets) == 3
    assert result.buckets[0]["start"] == start.isoformat()
    assert result.buckets[0]["end"] == (START + timedelta(seconds=1)).isoformat()
    assert result.buckets[-1]["end"] == end.isoformat()


def test_offset_aware_inputs_use_same_utc_bucket():
    local = START.astimezone(timezone(timedelta(hours=7)))
    result = aggregate_samples(
        [(local, 2)], start=local, end=local + timedelta(seconds=1), resolution="1s"
    )
    assert result.buckets[0]["start"] == START.isoformat()
    assert result.buckets[0]["count"] == 1


def test_auto_selects_smallest_resolution_within_budget():
    result = aggregate(seconds=31, max_points=4)
    assert result.resolution == "10s"
    assert len(result.buckets) == 4


def test_explicit_resolution_over_budget_is_rejected_before_reading_samples():
    def forbidden():
        pytest.fail("Over-budget input must not be consumed")
        yield

    with pytest.raises(ValueError, match="budget"):
        aggregate(forbidden(), seconds=31, resolution="1s", max_points=4)


@pytest.mark.parametrize("max_points", [0, -1, 2001, True, 1.5])
def test_invalid_point_budgets_are_rejected(max_points):
    with pytest.raises(ValueError):
        aggregate(max_points=max_points)


@pytest.mark.parametrize("resolution", ["", "2s", "1w"])
def test_unknown_resolutions_are_rejected(resolution):
    with pytest.raises(ValueError):
        aggregate(resolution=resolution)


@pytest.mark.parametrize("seconds", [0, -1])
def test_empty_or_reversed_ranges_are_rejected(seconds):
    with pytest.raises(ValueError):
        aggregate(seconds=seconds)


def test_naive_timestamps_are_rejected_for_ranges_and_samples():
    with pytest.raises(ValueError):
        aggregate_samples((), start=START.replace(tzinfo=None), end=START)
    with pytest.raises(ValueError):
        aggregate([(START.replace(tzinfo=None), 1)])


def test_auto_rejects_range_exceeding_largest_resolution_budget():
    with pytest.raises(ValueError, match="budget"):
        aggregate(seconds=3 * 86400, max_points=2)


def test_large_finite_values_have_finite_average():
    bucket = aggregate([(START, 1e308), (START, 1e308)], resolution="1s").buckets[0]
    assert bucket["avg"] == pytest.approx(1e308)
    assert math.isfinite(bucket["avg"])


def test_subnormal_average_stays_between_minimum_and_maximum():
    bucket = aggregate([(START, 5e-324), (START, 5e-324)]).buckets[0]
    assert bucket["min"] == bucket["avg"] == bucket["max"] == 5e-324


@pytest.mark.parametrize(
    "values",
    [
        [1.0, sys.float_info.max, -sys.float_info.max],
        [-sys.float_info.max, sys.float_info.max, 1.0],
    ],
)
def test_cancellation_preserves_average_without_input_order_dependence(values):
    bucket = aggregate([(START, value) for value in values]).buckets[0]
    assert bucket["avg"] == pytest.approx(1 / 3)


def test_last_datetime_bucket_does_not_overflow_when_clipping():
    start = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
    end = datetime.max.replace(tzinfo=UTC)
    result = aggregate_samples([(start, 2)], start=start, end=end, resolution="1s")
    assert len(result.buckets) == 1
    assert result.buckets[0]["end"] == end.isoformat()
    assert result.buckets[0]["avg"] == 2


def test_unsorted_one_shot_samples_are_not_sorted_or_materialized():
    def samples():
        yield START + timedelta(seconds=1), 7
        yield START, 3
        yield START + timedelta(seconds=1), 1

    result = aggregate(samples(), resolution="1s")
    assert [b["avg"] for b in result.buckets] == [3, 4, None]


def test_buckets_match_release_pinned_schema():
    contracts = default_contracts()
    validator = Draft202012Validator(
        {
            "$ref": (contracts.root / "v1.3/schemas/runtime.schema.json").as_uri()
            + "#/$defs/ChartBucket"
        },
        registry=contracts.registry,
        format_checker=FormatChecker(),
    )
    for bucket in aggregate([(START, 2)], resolution="1s").buckets:
        validator.validate(bucket)
