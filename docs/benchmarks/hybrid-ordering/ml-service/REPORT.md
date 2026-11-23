# Phase 1 Report: Real ML Service (HGB) Audit & Capacity Planning

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Model Artifact:** `ml-service/artifacts/integration/hgb-interactions-m56w5ff4/` (`hgb-interactions-candidate-v1`, `sensor-interactions-onehot-v1`, threshold `0.08943418823732646`, `HistGradientBoostingClassifier(max_iter=300, max_leaf_nodes=31)`, `scikit-learn==1.9.1`)
- **Container Image:** Built directly from [Dockerfile](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/Dockerfile) (`maintai-ml-bench:hgb-local`, `python:3.12-slim`)

---

## 1. Architecture & Code Audit

1. **Serving Runtime ([local_baseline.py](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/api/local_baseline.py), [Dockerfile](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/Dockerfile)):**
   - Process command: `python -m uvicorn local_baseline:app --host 0.0.0.0 --port 8001 --workers 1 --no-proxy-headers`.
   - Thread env in GitOps ([kustomization.yaml](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/kustomization.yaml)): `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`.
   - `POST /api/v1/predict` is defined as synchronous `def predict(...)`. FastAPI executes it on Starlette/AnyIO's default worker threadpool (**40 threads per uvicorn worker process**), with **no internal concurrency semaphore or queue cap**.
   - Every single request instantiates a fresh `pd.DataFrame([body.features.model_dump()], columns=FEATURES)`, validates all fields via [SensorInteractions](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/shared/preprocessing/feature_interactions.py), runs `ColumnTransformer`, runs `HistGradientBoostingClassifier.predict_proba`, emits Prometheus histograms/counters, and writes a JSON log line via `LOGGER.info(...)`.
2. **GPU vs CPU Verification:**
   - Serving image installs only CPU wheels ([requirements-serving.txt](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/requirements-serving.txt): `scikit-learn==1.9.1`, `pandas==3.1.0`, `numpy==2.5.4`, `scipy==1.18.1`, `joblib==1.6.0`). There is **no CUDA, PyTorch, or GPU execution backend**.
   - **Host GPU (`NVIDIA GeForce RTX 2050`, 4096 MiB VRAM)** showed zero ML compute processes (`nvidia-smi --query-compute-apps`) during all benchmarks; 100% of HGB inference ran on CPU (`AMD Ryzen 7 7435HS`, 8C/16T).
3. **Local GitOps Configuration Reality Check:**
   - In [MaintAI-gitops/environments/local/apps/kustomization.yaml](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/kustomization.yaml), `ml-service` (champion, HPA `minReplicas: 2, maxReplicas: 2`) currently mounts `MODEL_DIR=/local/models/logreg-baseline-v0`, while `ml-service-shadow` (HPA `minReplicas: 1, maxReplicas: 1`) mounts `MODEL_DIR=/local/models/hgb-interactions-candidate-v1`.
   - `ml-service-mirror` ([ml-traffic-mirror.yaml](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/ml-traffic-mirror.yaml)) mirrors `5%` of champion requests to `ml-service-shadow` with **`proxy_read_timeout 50ms`** (line 64). While healthy single-concurrency HGB completes under `13ms` p95 on native Linux CPU here, any queue buildup (`c >= 4`) or slower VM/Docker environment (`56.84ms` p95 cited in [HGB_HANDOFF.md](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/HGB_HANDOFF.md)) exceeds `50ms` and drops shadow requests.

---

## 2. Internal Pipeline Micro-Profile (100 Calls after 10 Warmup)

Measured inside `maintai-ml-hgb-bench` with `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`:

| Pipeline Stage | Mean (ms) | p50 (ms) | p95 (ms) | Share of Total (p50) |
| --- | ---: | ---: | ---: | ---: |
| `pd.DataFrame([sample], columns=FEATURES)` | 0.315 | 0.317 | 0.409 | 4.7% |
| `SensorInteractions.transform` (`feature_interactions.py`) | 1.870 | 1.848 | 2.272 | 27.3% |
| `ColumnTransformer` (`preprocessing`) | 2.003 | 1.924 | 2.823 | 28.4% |
| `HistGradientBoostingClassifier.predict_proba` | 2.703 | 2.680 | 3.188 | 39.6% |
| **Total `pipeline.predict_proba`** | **6.891** | **6.774** | **8.497** | **100.0%** |

> ~60.4% of single-request inference latency is per-row `pandas.DataFrame` + scikit-learn validation/transformer overhead; ~39.6% is HGB tree evaluation.

---

## 3. HTTP Load Benchmarks (`ml_service_benchmark.py`, 10 warmup + 100 measured × 3 runs)

Harness: [ml_service_benchmark.py](file:///home/chienqt/Dev/newBTL/MaintAI/scripts/benchmarks/hybrid_ordering/ml_service_benchmark.py) using keep-alive `httpx.AsyncClient` (`timeout=2.0s`), sampling varied rows from `sensor/data/ai4i2020.csv`.

### 3.1 Single Replica — Unbounded CPU (`--memory=1g`, `--workers 1`)

Source: [results_1rep_unbounded.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/ml-service/results_1rep_unbounded.json)

| Concurrency | Mean RPS | HTTP p50 (ms) | HTTP p95 (ms) | HTTP p99 (ms) | Server `inf_ms` p50 | Server `inf_ms` p95 | Queue Overhead p95 (ms) | Avg CPU (% of 1 core) | Peak RSS (MiB) | Errors / Timeouts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 90.05 | 10.68 | 12.83 | 14.71 | 7.30 | 8.98 | 4.11 | 89.02% | 135.58 | 0 / 0 |
| 2 | 97.80 | 19.86 | 24.71 | 35.25 | 11.68 | 18.24 | 12.79 | 105.25% | 136.38 | 0 / 0 |
| 4 | **101.41** | 38.90 | 54.67 | 61.82 | 18.51 | 34.92 | 33.16 | 111.83% | 136.69 | 0 / 0 |
| 6 | 90.04 | 65.87 | 94.91 | 119.39 | 29.09 | 57.50 | 59.48 | 112.98% | 136.91 | 0 / 0 |
| 8 | 103.61 | 74.84 | 123.88 | 133.99 | 31.99 | 66.36 | 76.53 | 114.42% | 137.33 | 0 / 0 |
| 12 | 97.38 | 124.51 | 178.05 | 206.53 | 33.76 | 103.01 | 131.84 | 114.68% | 138.01 | 0 / 0 |

### 3.2 Single Replica — CPU Capped at 1 Core (`--cpus=1.0 --memory=1g`)

Source: [results_1rep_1cpu.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/ml-service/results_1rep_1cpu.json)

| Concurrency | Mean RPS | HTTP p50 (ms) | HTTP p95 (ms) | Server `inf_ms` p50 | Avg CPU (% of 1 core) | `nr_throttled` (sum 3 runs) | Errors / Timeouts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 107.64 | 9.04 | 11.30 | 6.14 | 89.55% | 0 | 0 / 0 |
| 2 | 110.59 | 17.51 | 24.48 | 10.21 | 103.78% | **24** | 0 / 0 |
| 4 | 104.84 | 38.20 | 54.14 | 18.07 | 103.69% | **25** | 0 / 0 |

### 3.3 Two Replicas — Round-Robin (`--workers 1` each, `--memory=1g`)

Source: [results_2rep_unbounded.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/ml-service/results_2rep_unbounded.json)

| Total Concurrency (Per-Pod) | Mean RPS | HTTP p50 (ms) | HTTP p95 (ms) | HTTP p99 (ms) | Server `inf_ms` p50 | Queue Overhead p95 (ms) | Total CPU (% of 1 core) | Peak RSS / Pod (MiB) | Errors / Timeouts |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 (`1`/pod) | 188.07 | 9.60 | 12.90 | 31.56 | 6.28 | 5.12 | 171.30% | 133.12 | 0 / 0 |
| 4 (`2`/pod) | 202.47 | 19.68 | 25.06 | 28.23 | 10.28 | 13.51 | 211.03% | 133.27 | 0 / 0 |
| 8 (`4`/pod) | 197.56 | 39.51 | 60.32 | 73.36 | 16.10 | 35.82 | 225.63% | 133.58 | 0 / 0 |
| 12 (`6`/pod) | **205.46** | 56.65 | 83.65 | 93.19 | 22.29 | 57.95 | 227.43% | 134.17 | 0 / 0 |

---

## 4. Key Findings & Capacity Planning Recommendations

1. **GIL Serialization Causes Single-Pod Saturation at ~95–101 RPS:**
   - Because `uvicorn --workers 1` runs Python in one process and `POST /api/v1/predict` is CPU-bound Python + Cython inside one interpreter, the container cannot use more than **~1.12–1.15 CPU cores** even on a 16-thread host.
   - Pushing concurrency above `2` per pod does **not** increase throughput (~101 RPS at `c=4`, ~97 RPS at `c=12`), but **quadruples to 14x HTTP p95 latency** (`12.8ms -> 54.7ms -> 178.1ms`) and inflates server-side `inference_ms` from `7.3ms` to `33.8ms` due to thread contention on the GIL.
2. **CFS CPU Throttling Hazard:**
   - Even `--cpus=1.0` triggers CFS throttling (`24–25` periods over just 300 requests) as soon as concurrency `>= 2`, because AnyIO threadpool + FastAPI/uvicorn asyncio loop + pandas together spike >1.0 core. Kubernetes deployments must avoid tight CPU limits (`limits.cpu` should remain unset or `>= 1500m` if policy requires a limit; GitOps currently omits `limits.cpu`, which is correct).
3. **Knee of Curve & Optimal Concurrency per Pod:**
   - **Optimal per-ML-pod concurrency:** **`1–2` concurrent requests/pod** (`p95 <= 25ms`, full throughput without tail blow-up).
4. **Is ML Service a Bottleneck for the 50 RPS Official Workload?**
   - **No (on this hardware with 2 replicas).** Under Little's Law at `50 RPS` and ~`10ms` latency, average in-flight requests are only `50 × 0.010 = 0.50` total (`0.25/pod`). Peak throughput across 2 pods reaches **`~188–205 RPS`** (~**3.8–4.0x headroom** over 50 RPS, and **~1.8–2.0x headroom** even if 1 pod dies in an N+1 failure).
   - *Important qualification:* On slower VMs / single-threaded cloud vCPUs (e.g., throttled laptop VM in earlier [HGB_HANDOFF.md](file:///home/chienqt/Dev/newBTL/MaintAI/ml-service/inference/HGB_HANDOFF.md) showing `29 RPS` per pod, `56.8ms` p95), 2 pods give `~58 RPS` total — barely at the limit for 50 RPS and insufficient for N+1! Therefore, **real k3d multi-pod testing (Phase 5)** MUST validate actual pod throughput inside k3d with `ml-service` + `ml-service-mirror`.
5. **Memory Sizing:**
   - Measured peak RSS with real HGB loaded is **`132.4 – 138.0 MiB`**. GitOps `requests.memory: 128Mi` is slightly below steady-state working set (`~138 MiB`); recommend **`requests.memory: 256Mi, limits.memory: 512Mi`** (saving 512 MiB cap overhead vs `1Gi` while preventing scheduling under-allocation), `requests.cpu: 250m–500m` depending on environment.
6. **Replica Formula for Target Load (50 RPS):**
   - Assuming safe per-pod sustained utilization at **70%** of ~95 RPS single-pod ceiling (`safe_per_pod ≈ 65 RPS` on Ryzen-class vCPU, latency p95 < 25ms):
     - `N_required = ceil(50 / 65) = 1`
     - `N+1` (recommended minimum for zero-downtime rolling updates & pod loss): **`2` replicas** (matches current `ml-service-hpa min/max = 2`).
     - Safe cluster-wide `processing-worker` ML concurrency at `2` ML replicas: `PROCESSING_MAX_TOTAL_INFLIGHT` total across worker pods should keep average per-ML-pod inflight `<= 4` (e.g., total worker inflight `<= 8` when 2 ML replicas are active) so tail p95 stays `< 60ms`.
