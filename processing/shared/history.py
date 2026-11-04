"""Pure HISTORY-01 chart aggregation, independent of source selection and storage.

The caller supplies already selected samples and owns their timestamp/metric
semantics. This module does not create snapshots, establish coverage, read DB/S3
or expose an API. Storage/runtime integration requires the M6 contract freeze.
"""

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

RESOLUTIONS = {
    "1s": timedelta(seconds=1),
    "10s": timedelta(seconds=10),
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# Every finite binary64 value is an integer multiple of the smallest subnormal.
# Sum these integer units, then round once when computing the final mean.
FLOAT_SCALE = 1 << 1074


@dataclass(frozen=True)
class AggregatedSeries:
    resolution: str
    buckets: list[dict]


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Chart timestamps must include a UTC offset")
    return value.astimezone(UTC)


def aggregate_samples(
    samples: Iterable[tuple[datetime, object]],
    *,
    start: datetime,
    end: datetime,
    resolution: str = "auto",
    max_points: int = 1000,
) -> AggregatedSeries:
    """Aggregate a single pass of numeric values into bounded UTC grid buckets.

    Buckets intersecting the requested [start,end) are clipped to that range.
    Invalid values count separately; booleans/strings are never coerced. Memory
    depends on max_points, not the number/order of supplied samples. The caller
    must enforce its input/query budget before invoking this pure calculation.
    """
    start, end = _utc(start), _utc(end)
    if start >= end:
        raise ValueError("Chart range must have start before end")
    if type(max_points) is not int or not 1 <= max_points <= 2000:
        raise ValueError("Chart point budget must be between 1 and 2000")
    if resolution != "auto" and resolution not in RESOLUTIONS:
        raise ValueError("Unsupported chart resolution")

    for name, step in RESOLUTIONS.items():
        if resolution != "auto" and name != resolution:
            continue
        first = start - (start - EPOCH) % step
        count = (end - first - timedelta(microseconds=1)) // step + 1
        if count <= max_points:
            break
    else:
        raise ValueError("Chart range exceeds point budget")
    if count > max_points:
        raise ValueError("Chart resolution exceeds point budget")

    buckets = [
        {
            "start": max(start, first + i * step).isoformat(),
            "end": (end if i == count - 1 else first + (i + 1) * step).isoformat(),
            "min": None,
            "max": None,
            "avg": None,
            "count": 0,
            "invalid_count": 0,
        }
        for i in range(count)
    ]
    totals = [0] * count
    for timestamp, raw in samples:
        timestamp = _utc(timestamp)
        if not start <= timestamp < end:
            continue
        index = (timestamp - first) // step
        bucket = buckets[index]
        value = None
        if type(raw) in (int, float):
            try:
                numeric = float(raw)
                if math.isfinite(numeric):
                    value = numeric
            except OverflowError:
                pass
        if value is None:
            bucket["invalid_count"] += 1
            continue
        previous = bucket["count"]
        bucket["count"] += 1
        bucket["min"] = value if previous == 0 else min(bucket["min"], value)
        bucket["max"] = value if previous == 0 else max(bucket["max"], value)
        numerator, denominator = value.as_integer_ratio()
        totals[index] += numerator * (FLOAT_SCALE // denominator)
    for bucket, total in zip(buckets, totals, strict=True):
        if bucket["count"]:
            bucket["avg"] = total / (bucket["count"] * FLOAT_SCALE)
    return AggregatedSeries(name, buckets)
