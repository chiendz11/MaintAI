#!/usr/bin/env python3
"""Standalone ML Service Capacity Audit with pure host cgroup v2 CPU/throttling/RAM measurement."""

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


def load_samples(limit: int = 1000) -> list[dict]:
    rows: list[dict] = []
    with DATASET_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
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


def summarize(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None, "min": None}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 3),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
    }


def read_host_proc_stat() -> tuple[int, int]:
    parts = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    vals = [int(x) for x in parts[:8]]
    return vals[3] + vals[4], sum(vals)


def read_host_cgroup(cid: str) -> dict:
    cg = Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope")
    cpu_stat = {}
    for line in (cg / "cpu.stat").read_text().splitlines():
        k, v = line.split()
        cpu_stat[k] = int(v)
    mem_current = int((cg / "memory.current").read_text().strip())
    mem_stat = {}
    for line in (cg / "memory.stat").read_text().splitlines():
        k, v = line.split()
        mem_stat[k] = int(v)
    cpu_max = (cg / "cpu.max").read_text().strip()
    return {
        "ts": time.monotonic(),
        "cpu_stat": cpu_stat,
        "cpu_max": cpu_max,
        "mem_current": mem_current,
        "mem_anon": mem_stat.get("anon", 0),
        "mem_file": mem_stat.get("file", 0),
        "host_stat": read_host_proc_stat(),
    }


def diff_host_cgroup(b: dict, a: dict, cpus_limit: float) -> dict:
    dt = max(1e-6, a["ts"] - b["ts"])
    du = max(0, a["cpu_stat"].get("usage_usec", 0) - b["cpu_stat"].get("usage_usec", 0))
    duser = max(0, a["cpu_stat"].get("user_usec", 0) - b["cpu_stat"].get("user_usec", 0))
    dsys = max(0, a["cpu_stat"].get("system_usec", 0) - b["cpu_stat"].get("system_usec", 0))
    np_d = max(0, a["cpu_stat"].get("nr_periods", 0) - b["cpu_stat"].get("nr_periods", 0))
    nt_d = max(0, a["cpu_stat"].get("nr_throttled", 0) - b["cpu_stat"].get("nr_throttled", 0))
    tu_d = max(0, a["cpu_stat"].get("throttled_usec", 0) - b["cpu_stat"].get("throttled_usec", 0))
    hi0, ht0 = b["host_stat"]
    hi1, ht1 = a["host_stat"]
    ht_d = max(1, ht1 - ht0)
    hi_d = max(0, hi1 - hi0)
    cores = (du / 1e6) / dt
    return {
        "window_s": round(dt, 4),
        "cpu_max": a["cpu_max"],
        "cpu_cores": round(cores, 4),
        "user_cores": round((duser / 1e6) / dt, 4),
        "system_cores": round((dsys / 1e6) / dt, 4),
        "cpu_pct_of_limit": round(100.0 * cores / cpus_limit, 2) if cpus_limit > 0 else None,
        "nr_periods": np_d,
        "nr_throttled": nt_d,
        "throttled_ratio_pct": round(100.0 * nt_d / np_d, 2) if np_d > 0 else 0.0,
        "throttled_ms": round(tu_d / 1000.0, 2),
        "mem_current_mib": round(a["mem_current"] / (1024 * 1024), 2),
        "mem_anon_mib": round(a["mem_anon"] / (1024 * 1024), 2),
        "host_cpu_pct": round(100.0 * (1.0 - hi_d / ht_d), 2),
    }


async def run_window(
    url: str,
    samples: list[dict],
    cid: str,
    cpus_limit: float,
    concurrency: int,
    duration_s: float,
    tag: str,
) -> dict:
    http_ms: list[float] = []
    infer_ms: list[float] = []
    queue_ms: list[float] = []
    status_counts: dict[str, int] = {}
    errors: list[str] = []
    inflight_cur = 0
    inflight_peak = 0
    idx = 0
    stop_at = time.monotonic() + duration_s

    limits = httpx.Limits(max_connections=max(64, concurrency * 2), max_keepalive_connections=max(32, concurrency * 2))
    async with httpx.AsyncClient(limits=limits, timeout=15.0) as client:
        cg_before = read_host_cgroup(cid)
        wall0 = time.perf_counter()

        async def worker():
            nonlocal idx, inflight_cur, inflight_peak
            while time.monotonic() < stop_at:
                i = idx
                idx += 1
                sample = samples[i % len(samples)]
                payload = {
                    "event_id": f"{tag}_c{concurrency}_{i}",
                    "machine_id": f"M-{(i % 50) + 1:03d}",
                    "features": sample,
                }
                inflight_cur += 1
                if inflight_cur > inflight_peak:
                    inflight_peak = inflight_cur
                t0 = time.perf_counter()
                try:
                    resp = await client.post(
                        f"{url}/api/v1/predict",
                        json=payload,
                        headers={"Accept": "application/json", "X-Correlation-ID": f"cor_{tag}_{i}"},
                    )
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    code = str(resp.status_code)
                    status_counts[code] = status_counts.get(code, 0) + 1
                    if resp.status_code == 200:
                        srv = float(resp.json()["inference_ms"])
                        http_ms.append(dt_ms)
                        infer_ms.append(srv)
                        queue_ms.append(max(0.0, dt_ms - srv))
                    else:
                        if len(errors) < 5:
                            errors.append(f"HTTP {resp.status_code}: {resp.text[:150]}")
                except Exception as exc:
                    k = "TIMEOUT" if isinstance(exc, httpx.TimeoutException) else "EXC"
                    status_counts[k] = status_counts.get(k, 0) + 1
                    if len(errors) < 5:
                        errors.append(repr(exc))
                finally:
                    inflight_cur -= 1

        await asyncio.gather(*(worker() for _ in range(concurrency)))
        wall_elapsed = time.perf_counter() - wall0
        cg_after = read_host_cgroup(cid)

    cg_diff = diff_host_cgroup(cg_before, cg_after, cpus_limit)
    total = sum(status_counts.values())
    ok = status_counts.get("200", 0)
    return {
        "concurrency": concurrency,
        "peak_inflight": inflight_peak,
        "wall_elapsed_s": round(wall_elapsed, 3),
        "total_requests": total,
        "success_count": ok,
        "error_count": total - ok,
        "error_rate_pct": round(100.0 * (total - ok) / total, 3) if total else 0.0,
        "rps": round(total / wall_elapsed, 2),
        "success_rps": round(ok / wall_elapsed, 2),
        "status_counts": status_counts,
        "http_latency_ms": summarize(http_ms),
        "server_inference_ms": summarize(infer_ms),
        "queue_wait_ms": summarize(queue_ms),
        "cgroup": cg_diff,
        "errors_sample": errors,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument(
        "--model-dir",
        default=str(ROOT / "ml-service/artifacts/integration/hgb-interactions-m56w5ff4"),
    )
    parser.add_argument("--cpus", type=float, default=1.0)
    parser.add_argument("--memory", default="1g")
    parser.add_argument("--env", nargs="*", default=[])
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--concurrencies", nargs="+", type=int, default=[1, 2, 4, 8, 12, 24])
    parser.add_argument("--warmup-seconds", type=int, default=10)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    samples = load_samples()
    cname = f"ml-audit-{args.tag}-{int(time.time())}"
    model_dir = str(Path(args.model_dir).resolve())
    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        cname,
        "-p",
        "127.0.0.1::8001",
        f"--cpus={args.cpus}",
        f"--memory={args.memory}",
        "-v",
        f"{model_dir}:/models/current:ro",
        "-e",
        "MAINTAI_LOCAL_ONLY=1",
        "-e",
        "MODEL_DIR=/models/current",
    ]
    for kv in args.env:
        cmd.extend(["-e", kv])
    cmd.append(args.image)
    if args.workers is not None:
        cmd.extend(
            [
                "python",
                "-m",
                "uvicorn",
                "local_baseline:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8001",
                "--workers",
                str(args.workers),
                "--no-proxy-headers",
            ]
        )

    cid = subprocess.check_output(cmd, text=True).strip()
    try:
        port_line = subprocess.check_output(["docker", "port", cid, "8001/tcp"], text=True).strip()
        host_port = int(port_line.rsplit(":", 1)[-1])
        base_url = f"http://127.0.0.1:{host_port}"

        # Wait for /health/ready == 200 and measure startup latency
        t0 = time.perf_counter()
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            try:
                r = httpx.get(f"{base_url}/health/ready", timeout=2.0)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        else:
            logs = subprocess.check_output(["docker", "logs", cid], text=True, stderr=subprocess.STDOUT)
            raise RuntimeError(f"Container {cid} did not become ready:\n{logs}")
        startup_ready_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        async def run_all():
            # Warmup phase
            if args.warmup_seconds > 0:
                await run_window(base_url, samples, cid, args.cpus, 4, args.warmup_seconds, f"{args.tag}_warm")
                await asyncio.sleep(1.0)
            results = []
            for c in args.concurrencies:
                res = await run_window(base_url, samples, cid, args.cpus, c, args.duration_seconds, args.tag)
                print(
                    f"[{args.tag}] c={c:2d}: rps={res['success_rps']:6.2f} "
                    f"http_p50={res['http_latency_ms']['p50']}ms p95={res['http_latency_ms']['p95']}ms p99={res['http_latency_ms']['p99']}ms "
                    f"srv_p95={res['server_inference_ms']['p95']}ms q_p95={res['queue_wait_ms']['p95']}ms "
                    f"cpu={res['cgroup']['cpu_cores']}c ({res['cgroup']['cpu_pct_of_limit']}%) "
                    f"thr={res['cgroup']['throttled_ratio_pct']}% mem={res['cgroup']['mem_current_mib']}MiB host={res['cgroup']['host_cpu_pct']}%",
                    flush=True,
                )
                results.append(res)
                await asyncio.sleep(1.0)
            return results

        sweep = asyncio.run(run_all())
        out_doc = {
            "tag": args.tag,
            "image": args.image,
            "cpus_limit": args.cpus,
            "memory_limit": args.memory,
            "env": args.env,
            "workers": args.workers,
            "duration_per_level_s": args.duration_seconds,
            "startup_ready_ms": startup_ready_ms,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "concurrencies": sweep,
        }
        out_p = Path(args.out)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(out_doc, indent=2) + "\n")
    finally:
        subprocess.run(["docker", "rm", "-f", cid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
