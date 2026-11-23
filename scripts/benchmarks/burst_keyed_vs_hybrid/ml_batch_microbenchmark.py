#!/usr/bin/env python3
"""Separate Batch Inference micro-benchmark on MaintAI HGB model (§6.2.C).

Measures per-batch latency, per-sample latency, total sample throughput, and
exact numerical equivalence vs batch=1. Strictly independent of Hybrid Scheduler.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "ml-service/shared/preprocessing"))

FEATURES = [
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
]


def load_rows(limit: int = 512) -> list[dict]:
    rows = []
    with (ROOT / "sensor/data/ai4i2020.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            rows.append(
                {
                    "product_type": row["Type"],
                    "air_temperature_k": float(row["Air temperature [K]"]),
                    "process_temperature_k": float(row["Process temperature [K]"]),
                    "rotational_speed_rpm": float(row["Rotational speed [rpm]"]),
                    "torque_nm": float(row["Torque [Nm]"]),
                    "tool_wear_min": float(row["Tool wear [min]"]),
                }
            )
            if len(rows) >= limit:
                break
    return rows


def to_numpy_batch(rows: list[dict]) -> np.ndarray:
    n = len(rows)
    x = np.zeros((n, 11), dtype=np.float64)
    k = 2.0 * math.pi / 60.0
    cat_map = {"L": 0, "M": 1, "H": 2}
    for i, r in enumerate(rows):
        x[i, cat_map[r["product_type"]]] = 1.0
        air = r["air_temperature_k"]
        proc = r["process_temperature_k"]
        rpm = r["rotational_speed_rpm"]
        trq = r["torque_nm"]
        wear = r["tool_wear_min"]
        x[i, 3] = air
        x[i, 4] = proc
        x[i, 5] = rpm
        x[i, 6] = trq
        x[i, 7] = wear
        x[i, 8] = proc - air
        x[i, 9] = trq * rpm * k
        x[i, 10] = wear * trq
    return x


def pct(vals: list[float], p: float) -> float:
    s = sorted(vals)
    idx = (p / 100.0) * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    return round(s[lo] + (idx - lo) * (s[hi] - s[lo]), 4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir",
        default=str(ROOT / "ml-service/artifacts/integration/hgb-interactions-m56w5ff4"),
    )
    parser.add_argument("--batches", nargs="+", type=int, default=[1, 4, 8, 16])
    parser.add_argument("--iters", type=int, default=300)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    pipeline = joblib.load(Path(args.model_dir) / "pipeline.joblib")
    estimator = pipeline.steps[-1][1]
    rows = load_rows()

    # Verify strict batch vs single equivalence
    ref_probs = pipeline.predict_proba(pd.DataFrame(rows[:16], columns=FEATURES))[:, 1]
    np_probs = estimator.predict_proba(to_numpy_batch(rows[:16]))[:, 1]
    max_diff = float(np.max(np.abs(ref_probs - np_probs)))
    assert max_diff <= 1e-12, f"Batch parity failed: {max_diff}"

    # Warmup
    for _ in range(20):
        pipeline.predict_proba(pd.DataFrame(rows[:4], columns=FEATURES))
        estimator.predict_proba(to_numpy_batch(rows[:4]))

    results = []
    for b in args.batches:
        batch_rows = [rows[i % len(rows)] for i in range(b)]
        df_times = []
        np_times = []
        for _ in range(args.iters):
            t0 = time.perf_counter()
            pipeline.predict_proba(pd.DataFrame(batch_rows, columns=FEATURES))
            df_times.append((time.perf_counter() - t0) * 1000.0)
        for _ in range(args.iters):
            t0 = time.perf_counter()
            estimator.predict_proba(to_numpy_batch(batch_rows))
            np_times.append((time.perf_counter() - t0) * 1000.0)

        df_mean = statistics.fmean(df_times)
        np_mean = statistics.fmean(np_times)
        results.append(
            {
                "batch_size": b,
                "iters": args.iters,
                "dataframe_pipeline": {
                    "batch_mean_ms": round(df_mean, 4),
                    "batch_p50_ms": pct(df_times, 50),
                    "batch_p95_ms": pct(df_times, 95),
                    "per_sample_mean_ms": round(df_mean / b, 4),
                    "throughput_samples_per_sec": round(1000.0 * b / df_mean, 2),
                },
                "numpy_fastpath": {
                    "batch_mean_ms": round(np_mean, 4),
                    "batch_p50_ms": pct(np_times, 50),
                    "batch_p95_ms": pct(np_times, 95),
                    "per_sample_mean_ms": round(np_mean / b, 4),
                    "throughput_samples_per_sec": round(1000.0 * b / np_mean, 2),
                },
            }
        )
        print(
            f"[ML-B1] batch={b:2d}: df_mean={df_mean:.3f}ms ({1000*b/df_mean:.1f} samp/s) | "
            f"np_mean={np_mean:.3f}ms ({1000*b/np_mean:.1f} samp/s, {np_mean/b:.3f} ms/samp)",
            flush=True,
        )

    out_p = Path(args.out)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(json.dumps({"max_abs_diff": max_diff, "results": results}, indent=2) + "\n")


if __name__ == "__main__":
    main()
