#!/usr/bin/env python3
"""Real ML Service (HGB) concurrency & capacity benchmark against live Docker containers.

Drives `/api/v1/predict` using `httpx.AsyncClient` with keep-alive connections matching
`processing/shared/inference_client/client.py`, collects HTTP/server latency, queueing delay,
Prometheus `/metrics`, cgroup v2 CPU/memory stats, and `nvidia-smi` GPU telemetry.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
DATASET_PATH = ROOT / "sensor/data/ai4i2020.csv"
EXAMPLE_REQ_PATH = ROOT / "contracts/v1.3/examples/http/ml/predict-request.json"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    idx = (pct / 100.0) * (len(ordered) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return round(ordered[lo] * (1.0 - frac) + ordered[hi] * frac, 3)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 3),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 3),
    }


def load_samples(limit: int = 500) -> list[dict]:
    if DATASET_PATH.is_file():
        rows: list[dict] = []
        with DATASET_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for idx, row in enumerate(reader):
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
        if rows:
            return rows
    example = json.loads(EXAMPLE_REQ_PATH.read_text(encoding="utf-8"))
    return [example["features"]]


def read_container_cgroup(container: str) -> dict[str, int | float | None]:
    if not container:
        return {}
    try:
        out = subprocess.check_output(
            [
                "docker",
                "exec",
                container,
                "sh",
                "-c",
                "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true; "
                "echo MEM=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0)",
            ],
            text=True,
            timeout=5,
        )
    except Exception:
        return {}
    stats: dict[str, int | float | None] = {}
    for line in out.splitlines():
        if line.startswith("MEM="):
            try:
                stats["memory_bytes"] = int(line.split("=", 1)[1].strip())
            except ValueError:
                pass
            continue
        parts = line.split()
        if len(parts) == 2:
            try:
                stats[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return stats


def read_nvidia_smi() -> dict:
    try:
        gpu_out = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=name,utilization.gpu,utilization.memory,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            timeout=5,
        ).strip()
        proc_out = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-compute-apps=pid,process_name,used_memory",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            timeout=5,
        ).strip()
        return {
            "gpu_summary": gpu_out.splitlines(),
            "compute_processes": [line for line in proc_out.splitlines() if line.strip()],
        }
    except Exception as exc:
        return {"error": str(exc)}


async def fetch_prom_metrics(client: httpx.AsyncClient, url: str) -> dict[str, float]:
    resp = await client.get(f"{url.rstrip('/')}/metrics", timeout=5.0)
    resp.raise_for_status()
    parsed: dict[str, float] = {}
    for line in resp.text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+([0-9eE.+\-]+)$", line)
        if m:
            parsed[m.group(1)] = float(m.group(2))
    return parsed


async def run_burst(
    client: httpx.AsyncClient,
    urls: list[str],
    samples: list[dict],
    *,
    concurrency: int,
    count: int,
    prefix: str,
) -> dict:
    http_ms: list[float] = []
    infer_ms: list[float] = []
    overhead_ms: list[float] = []
    status_counts: dict[str, int] = {}
    errors: list[str] = []
    timeouts = 0
    predictions: dict[str, int] = {"NORMAL": 0, "FAILURE": 0}
    counter = 0
    counter_lock = asyncio.Lock()

    async def worker():
        nonlocal counter, timeouts
        while True:
            async with counter_lock:
                if counter >= count:
                    return
                idx = counter
                counter += 1
            base_url = urls[idx % len(urls)].rstrip("/")
            sample = samples[idx % len(samples)]
            payload = {
                "event_id": f"{prefix}_{idx:06d}",
                "machine_id": f"M-{(idx % 50) + 1:03d}",
                "features": sample,
            }
            started = time.perf_counter()
            try:
                resp = await client.post(
                    f"{base_url}/api/v1/predict",
                    json=payload,
                    headers={
                        "X-Correlation-ID": f"cor_{prefix}_{idx:06d}",
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                )
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                code = str(resp.status_code)
                status_counts[code] = status_counts.get(code, 0) + 1
                if resp.status_code == 200:
                    data = resp.json()
                    srv_ms = float(data["inference_ms"])
                    http_ms.append(elapsed_ms)
                    infer_ms.append(srv_ms)
                    overhead_ms.append(max(0.0, elapsed_ms - srv_ms))
                    lbl = data["prediction"]
                    predictions[lbl] = predictions.get(lbl, 0) + 1
                else:
                    errors.append(f"HTTP {resp.status_code}: {resp.text[:160]}")
            except httpx.TimeoutException:
                timeouts += 1
                status_counts["TIMEOUT"] = status_counts.get("TIMEOUT", 0) + 1
            except Exception as exc:
                errors.append(repr(exc))
                status_counts["EXC"] = status_counts.get("EXC", 0) + 1

    wall_start = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    wall_elapsed_s = time.perf_counter() - wall_start
    success = status_counts.get("200", 0)
    return {
        "wall_elapsed_s": round(wall_elapsed_s, 4),
        "total_requests": count,
        "success_count": success,
        "error_count": count - success - timeouts,
        "timeout_count": timeouts,
        "throughput_rps": round(count / wall_elapsed_s, 3) if wall_elapsed_s > 0 else 0.0,
        "success_rps": round(success / wall_elapsed_s, 3) if wall_elapsed_s > 0 else 0.0,
        "status_counts": status_counts,
        "predictions": predictions,
        "http_ms": summarize(http_ms),
        "server_inference_ms": summarize(infer_ms),
        "queue_overhead_ms": summarize(overhead_ms),
        "sample_errors": errors[:5],
    }


async def benchmark_suite(
    urls: list[str],
    containers: list[str],
    concurrencies: list[int],
    *,
    warmup: int,
    requests_per_run: int,
    runs: int,
    timeout_seconds: float,
    auto_12: bool,
) -> dict:
    samples = load_samples()
    limits = httpx.Limits(max_connections=128, max_keepalive_connections=128)
    suite_results: list[dict] = []
    gpu_samples: list[dict] = []

    async with httpx.AsyncClient(limits=limits, timeout=timeout_seconds) as client:
        # Readiness & identity check
        identities = []
        for idx, u in enumerate(urls):
            ready = await client.get(f"{u.rstrip('/')}/health/ready", timeout=5.0)
            ready.raise_for_status()
            probe = await client.post(
                f"{u.rstrip('/')}/api/v1/predict",
                json={
                    "event_id": f"probe_init_{idx}",
                    "machine_id": "M-001",
                    "features": samples[0],
                },
            )
            probe.raise_for_status()
            identities.append(probe.json())

        c_queue = list(concurrencies)
        idx_c = 0
        while idx_c < len(c_queue):
            conc = c_queue[idx_c]
            idx_c += 1
            run_records = []
            for run_idx in range(1, runs + 1):
                if warmup > 0:
                    await run_burst(
                        client,
                        urls,
                        samples,
                        concurrency=min(conc, warmup),
                        count=warmup,
                        prefix=f"warm_c{conc}_r{run_idx}",
                    )
                cg_before = {c: read_container_cgroup(c) for c in containers}
                prom_before = [await fetch_prom_metrics(client, u) for u in urls]
                gpu_before = read_nvidia_smi()

                burst = await run_burst(
                    client,
                    urls,
                    samples,
                    concurrency=conc,
                    count=requests_per_run,
                    prefix=f"c{conc}_r{run_idx}",
                )

                gpu_after = read_nvidia_smi()
                prom_after = [await fetch_prom_metrics(client, u) for u in urls]
                cg_after = {c: read_container_cgroup(c) for c in containers}

                cg_deltas = {}
                for c in containers:
                    b, a = cg_before.get(c, {}), cg_after.get(c, {})
                    if b and a and "usage_usec" in a and "usage_usec" in b:
                        du = a["usage_usec"] - b["usage_usec"]
                        wall_us = burst["wall_elapsed_s"] * 1_000_000.0
                        cg_deltas[c] = {
                            "cpu_cores_avg": round(du / wall_us, 4) if wall_us > 0 else 0.0,
                            "cpu_pct_of_one_core": round((du / wall_us) * 100.0, 2)
                            if wall_us > 0
                            else 0.0,
                            "delta_nr_throttled": a.get("nr_throttled", 0)
                            - b.get("nr_throttled", 0),
                            "delta_throttled_ms": round(
                                (a.get("throttled_usec", 0) - b.get("throttled_usec", 0)) / 1000.0, 3
                            ),
                            "rss_mib": round(a.get("memory_bytes", 0) / (1024 * 1024), 2),
                        }
                prom_sum_delta = 0.0
                prom_count_delta = 0.0
                for pb, pa in zip(prom_before, prom_after):
                    prom_sum_delta += pa.get("maintai_ml_inference_duration_seconds_sum", 0.0) - pb.get(
                        "maintai_ml_inference_duration_seconds_sum", 0.0
                    )
                    prom_count_delta += pa.get(
                        "maintai_ml_inference_duration_seconds_count", 0.0
                    ) - pb.get("maintai_ml_inference_duration_seconds_count", 0.0)

                burst["cgroup"] = cg_deltas
                burst["prom_avg_inference_ms"] = (
                    round((prom_sum_delta / prom_count_delta) * 1000.0, 3)
                    if prom_count_delta > 0
                    else None
                )
                burst["gpu_before_after"] = {"before": gpu_before, "after": gpu_after}
                run_records.append(burst)
                gpu_samples.append({"concurrency": conc, "run": run_idx, "after": gpu_after})
                print(
                    f"[c={conc} run={run_idx}/{runs}] "
                    f"rps={burst['throughput_rps']:.2f} "
                    f"http_p50={burst['http_ms']['p50']}ms p95={burst['http_ms']['p95']}ms p99={burst['http_ms']['p99']}ms "
                    f"srv_p50={burst['server_inference_ms']['p50']}ms q_p95={burst['queue_overhead_ms']['p95']}ms "
                    f"err={burst['error_count']} to={burst['timeout_count']} "
                    f"cpu={ {k: v['cpu_pct_of_one_core'] for k, v in cg_deltas.items()} } "
                    f"throttled={ {k: v['delta_nr_throttled'] for k, v in cg_deltas.items()} }",
                    flush=True,
                )

            agg = {
                "concurrency": conc,
                "throughput_rps_mean": round(
                    statistics.fmean(r["throughput_rps"] for r in run_records), 3
                ),
                "throughput_rps_runs": [r["throughput_rps"] for r in run_records],
                "http_p50_mean": round(
                    statistics.fmean(r["http_ms"]["p50"] for r in run_records), 3
                ),
                "http_p95_mean": round(
                    statistics.fmean(r["http_ms"]["p95"] for r in run_records), 3
                ),
                "http_p99_mean": round(
                    statistics.fmean(r["http_ms"]["p99"] for r in run_records), 3
                ),
                "server_inf_p50_mean": round(
                    statistics.fmean(r["server_inference_ms"]["p50"] for r in run_records), 3
                ),
                "server_inf_p95_mean": round(
                    statistics.fmean(r["server_inference_ms"]["p95"] for r in run_records), 3
                ),
                "queue_overhead_p95_mean": round(
                    statistics.fmean(r["queue_overhead_ms"]["p95"] for r in run_records), 3
                ),
                "total_errors": sum(r["error_count"] for r in run_records),
                "total_timeouts": sum(r["timeout_count"] for r in run_records),
                "runs": run_records,
            }
            if containers:
                agg["cpu_pct_sum_mean"] = round(
                    statistics.fmean(
                        sum(v["cpu_pct_of_one_core"] for v in r["cgroup"].values())
                        for r in run_records
                    ),
                    2,
                )
                agg["rss_mib_max"] = max(
                    (v["rss_mib"] for r in run_records for v in r["cgroup"].values()),
                    default=0.0,
                )
                agg["total_throttled_periods"] = sum(
                    v["delta_nr_throttled"] for r in run_records for v in r["cgroup"].values()
                )
            suite_results.append(agg)

            if (
                auto_12
                and conc == 8
                and 12 not in c_queue
                and agg["total_errors"] == 0
                and agg["total_timeouts"] == 0
                and agg["http_p95_mean"] < 1500.0
            ):
                c_queue.append(12)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "urls": urls,
        "containers": containers,
        "probe_identities": identities,
        "results": suite_results,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True, help="Comma-separated ML service base URLs")
    parser.add_argument("--containers", default="", help="Comma-separated container names")
    parser.add_argument("--concurrencies", default="1,2,4,6,8")
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--auto-12", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    urls = [u.strip() for u in args.urls.split(",") if u.strip()]
    containers = [c.strip() for c in args.containers.split(",") if c.strip()]
    concurrencies = [int(x.strip()) for x in args.concurrencies.split(",") if x.strip()]

    data = asyncio.run(
        benchmark_suite(
            urls,
            containers,
            concurrencies,
            warmup=args.warmup,
            requests_per_run=args.requests,
            runs=args.runs,
            timeout_seconds=args.timeout,
            auto_12=args.auto_12,
        )
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
