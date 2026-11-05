"""Benchmark HTTP local bằng validation inputs; không đánh giá accuracy."""

import argparse
import json
import platform
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8003")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()

    if args.requests < 20 or args.concurrency < 1:
        raise ValueError("Use at least 20 requests and positive concurrency.")

    base = args.url.rstrip("/")
    bundle = ROOT / "ml-service/artifacts/integration/hgb-interactions-m56w5ff4"
    metadata = json.loads(
        (bundle / "metadata.json").read_text(encoding="utf-8")
    )

    with urllib.request.urlopen(base + "/health/ready", timeout=10) as response:
        ready = json.loads(response.read())
    if ready.get("checks", {}).get("model_loaded") is not True:
        raise RuntimeError("Model is not ready.")

    split = json.loads((
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    ).read_text(encoding="utf-8"))

    mapping = {
        "Type": "product_type",
        "Air temperature [K]": "air_temperature_k",
        "Process temperature [K]": "process_temperature_k",
        "Rotational speed [rpm]": "rotational_speed_rpm",
        "Torque [Nm]": "torque_nm",
        "Tool wear [min]": "tool_wear_min",
    }
    frame = (
        pd.read_csv(ROOT / "sensor/data/ai4i2020.csv")
        .iloc[split["indices"]["validation"]][list(mapping)]
        .rename(columns=mapping)
        .reset_index(drop=True)
    )

    # Cùng seed -> cùng chuỗi input giữa các mức concurrency.
    positions = np.random.default_rng(42).integers(
        0, len(frame), size=args.requests + 10
    )
    run_id = uuid4().hex

    def send(sequence):
        row = frame.iloc[int(positions[sequence])]
        features = {"product_type": str(row["product_type"])}
        features.update({
            name: float(row[name]) for name in list(mapping.values())[1:]
        })
        event_id = f"bench_{run_id}_{sequence}"
        payload = {
            "event_id": event_id,
            "machine_id": "BENCH-HGB-001",
            "features": features,
        }
        request = urllib.request.Request(
            base + "/api/v1/predict",
            data=json.dumps(payload, allow_nan=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        start = perf_counter()
        status = None
        body = None
        error = None

        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = response.status
                body = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            status = exc.code
            error = exc.read().decode("utf-8", errors="replace")[:500]
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        elapsed_ms = (perf_counter() - start) * 1000
        valid = False
        server_ms = None

        if status == 200 and isinstance(body, dict):
            score = body.get("failure_probability")
            threshold = body.get("model_threshold")
            server_ms = body.get("inference_ms")

            numeric_score = (
                type(score) in (int, float)
                and np.isfinite(score)
                and 0 <= score <= 1
            )
            expected_label = (
                "FAILURE" if numeric_score and score >= metadata["threshold"]
                else "NORMAL"
            )
            valid = (
                numeric_score
                and body.get("event_id") == event_id
                and body.get("model_version") == metadata["model_version"]
                and body.get("preprocessor_version") == metadata["preprocessor_version"]
                and threshold == metadata["threshold"]
                and body.get("prediction") == expected_label
                and type(server_ms) in (int, float)
                and np.isfinite(server_ms)
                and server_ms >= 0
            )
            if not valid:
                error = "Response identity or prediction semantics mismatch."

        return {
            "sequence": sequence,
            "http_status": status,
            "valid_response": bool(valid),
            "http_elapsed_ms": elapsed_ms,
            "server_inference_ms": server_ms if valid else None,
            "error": error,
        }

    # Warm-up tuần tự, không tính vào benchmark.
    for sequence in range(10):
        result = send(sequence)
        if not result["valid_response"]:
            raise RuntimeError(f"Warm-up failed: {result}")

    start = perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        results = list(executor.map(send, range(10, args.requests + 10)))
    wall_seconds = perf_counter() - start

    successes = [item for item in results if item["valid_response"]]

    def percentiles(values):
        if not values:
            return None
        return {
            "p50": float(np.percentile(values, 50)),
            "p95": float(np.percentile(values, 95)),
            "p99": float(np.percentile(values, 99)),
            "max": float(np.max(values)),
        }

    report = {
        "scope": "local_http_benchmark_not_production_capacity",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "url": base,
        "model_version": metadata["model_version"],
        "threshold": metadata["threshold"],
        "client_platform": platform.platform(),
        "client_python": platform.python_version(),
        "requests": args.requests,
        "warmup_requests": 10,
        "concurrency": args.concurrency,
        "timeout_seconds": 10,
        "client_retries": 0,
        "successful_valid_responses": len(successes),
        "failed_or_invalid_responses": len(results) - len(successes),
        "wall_seconds": wall_seconds,
        "successful_requests_per_second": len(successes) / wall_seconds,
        "http_ms_successful": percentiles([
            item["http_elapsed_ms"] for item in successes
        ]),
        "http_ms_all_attempts": percentiles([
            item["http_elapsed_ms"] for item in results
        ]),
        "server_inference_ms_successful": percentiles([
            item["server_inference_ms"] for item in successes
        ]),
        "test_set_used": False,
        "limitation": (
            "Small local closed-loop test using urllib without explicit "
            "connection pooling. Results depend on client, Docker and host load."
        ),
    }

    output_root = ROOT / "ml-service/artifacts/integration_checks"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="http-benchmark-", dir=output_root))

    (output / "summary.json").write_text(
        json.dumps(report, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(results).to_csv(output / "requests.csv", index=False)

    print(json.dumps(report, indent=2))
    print("Output:", output)

    if len(successes) != len(results):
        raise SystemExit("Benchmark recorded failed/invalid requests; inspect output.")


if __name__ == "__main__":
    main()