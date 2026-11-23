# Phase 5 Report: k3d Multi-Pod Baseline Benchmark (`per_key=1`, 50 RPS)

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Cluster / Testbed:** `k3d-maintai-local` (K3s `v1.35.5+k3s1`, single worker/server node on `AMD Ryzen 7 7435HS` 8C/16T, `15 GiB` RAM, local host GPU `NVIDIA GeForce RTX 2050` unused/not attached to inference pods)
- **Workload Harness:** [k3d_real_benchmark.py](../../../../scripts/benchmarks/hybrid_ordering/k3d_real_benchmark.py) (`5s` warm-up + `15s` measurement × `3` runs per workload; drain timeout `90s`; per-machine `HTTP POST` FIFO sender awaiting `202 Accepted` before sending `seq + 1`)
- **Raw Data:**
  - Experiment A (Fast Champion ML — `logreg-baseline-v0`): [expA_fast_ml.json](./expA_fast_ml.json) (`6` runs)
  - Experiment B (Real Candidate ML — `hgb-interactions-candidate-v1`): [expB_real_hgb.json](./expB_real_hgb.json) (`6` runs)

> [!IMPORTANT]
> **Environment Parity Disclosure (`synchronous_commit=off` & Local Model Identity Fixture):**
> On `k3d-maintai-local`, PostgreSQL 17 (`local/platform/postgres.yaml`) runs with `synchronous_commit=off`, `max_connections=300`, `shared_buffers=512MB`, and single-broker Apache Kafka `4.0.0` (`12` partitions per topic, `replication-factor=1`). Experiment B routes `processing-worker` to `ml-service-shadow` using the temporary benchmark identity fixture (`/bench-fixtures/model-identity.json` matching `hgb-interactions-candidate-v1`). Consequently, absolute latency on `k3d` cannot be treated as equivalent to AWS RDS `synchronous_commit=on` multi-AZ or multi-broker MSK.

---

## 1. Test Configuration (Baseline Multi-Pod Topology)

All baseline k3d runs executed with compiled Opt 1 + Opt 3 images (`:hybrid-bench`) while keeping Hybrid Ordering disabled (`per_key=1`) and single-record Kafka offset commits (`KAFKA_COMMIT_BATCH_SIZE=1`):

- **Feature Flags & Limits:** `PROCESSING_HYBRID_ORDERING_ENABLED=false`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_MAX_INFLIGHT_PER_KEY=1`, `PROCESSING_MAX_TOTAL_INFLIGHT=8`, `KAFKA_COMMIT_BATCH_SIZE=1`.
- **Replicas Deployed:** `ingestion-api=2`, `processing-worker=6`, `processing-outbox-relay=2`, `business-consumer=4`, `business-api=1`, `processing-read-api=1`, `storage-consumer=1`, `ml-service=2` (LogReg), `ml-service-shadow=2` requested for Exp B (note: `ml-service-shadow-hpa` had `maxReplicas=4` at 80% CPU of `100m`, so HPA scaled `ml-service-shadow` from `2 -> 4` pods starting at Exp B WL=A run 3 and throughout WL=C).
- **Official 50 RPS Workloads:**
  - **Workload A (Normal Load):** `50` machines (`M-001`..`M-050`) × `1.0 RPS` = `50.0 RPS` (`1,000` events over `20s`; `750` measured in the `15s` window).
  - **Workload C (Hot Machine):** `M-001` at `16.0 RPS` + `34` background machines (`M-002`..`M-035`) × `1.0 RPS` = `50.0 RPS`.

---

## 2. Throughput, Latency & Correctness Summary (3-Run Means)

### 2.1 Experiment A — Champion LogReg (`ml-service`, 2 Pods)

| Metric | Workload A (`pk=1`) | Workload C (`pk=1`) |
| --- | ---: | ---: |
| Sent / Proc Outcomes / Biz Preds (win RPS) | `50.0` / `50.023` / `50.0` | `50.0` / `50.0` / `50.0` |
| Drained / Mean Drain Time | `Yes (3/3)` / `0.049 s` | `Yes (3/3)` / `0.058 s` |
| Ingress HTTP `p50` / `p95` / `p99` (ms) | `7.223` / `8.492` / `9.313` | `7.361` / `9.701` / `11.732` |
| `accept_to_proc` `p50` / `p95` / `p99` (ms) | `14.247` / `19.138` / `23.402` | `15.404` / `21.701` / `27.183` |
| `hot_accept_to_proc` `p50` / `p95` (ms) | `15.794` / `20.034` | `16.369` / `22.516` |
| `proc_to_outbox` `p50` / `p95` / `p99` (ms) | `17.225` / `23.581` / `25.840` | `17.337` / `24.467` / `28.395` |
| `outbox_to_biz_pred` `p50` / `p95` / `p99` (ms) | `1.074` / `1.762` / `3.183` | `1.187` / `6.998` / `10.878` |
| **Hot (`M-001`) E2E `p50` / `p95` / `p99` (ms)** | `35.038` / **`42.068`** / `44.650` | `36.387` / **`48.691`** / `59.250` |
| **Background E2E `p50` / `p95` / `p99` (ms)** | `33.047` / **`41.513`** / `46.459` | `34.249` / **`45.287`** / `53.754` |
| Max Kafka Consumer Lag (all 4 groups) | `0` | `0` |
| Correctness Violations (`total_violations`) | **`0`** | **`0`** |

Per-run range (`hot_e2e p95` / `bg_e2e p95`):
- **WL=A:** Run 1 `40.598 / 39.614 ms`, Run 2 `44.864 / 41.871 ms`, Run 3 `40.741 / 43.055 ms`.
- **WL=C:** Run 1 `44.902 / 41.961 ms`, Run 2 `54.661 / 49.033 ms`, Run 3 `46.509 / 44.867 ms`.

### 2.2 Experiment B — Real HGB (`ml-service-shadow`, 2 -> 4 Pods via HPA)

| Metric | Workload A (`pk=1`) | Workload C (`pk=1`) |
| --- | ---: | ---: |
| Sent / Proc Outcomes / Biz Preds (win RPS) | `50.0` / `50.023` / `50.0` | `50.0` / `50.0` / `50.0` |
| Drained / Mean Drain Time | `Yes (3/3)` / `0.350 s` | `Yes (3/3)` / `0.070 s` |
| Ingress HTTP `p50` / `p95` / `p99` (ms) | `7.288` / `9.288` / `11.523` | `7.384` / `9.519` / `11.722` |
| `accept_to_proc` `p50` / `p95` / `p99` (ms) | `18.732` / `26.772` / `36.051` | `20.395` / `32.379` / `39.375` |
| `hot_accept_to_proc` `p50` / `p95` (ms) | `20.586` / `27.190` | `22.467` / `32.393` |
| `proc_to_outbox` `p50` / `p95` / `p99` (ms) | `17.939` / `25.473` / `29.543` | `17.707` / `24.363` / `28.138` |
| `outbox_to_biz_pred` `p50` / `p95` / `p99` (ms) | `1.164` / `1.924` / `4.191` | `1.180` / `7.185` / `11.255` |
| **Hot (`M-001`) E2E `p50` / `p95` / `p99` (ms)** | `41.325` / **`52.751`** / `56.907` | `42.961` / **`56.710`** / `72.256` |
| **Background E2E `p50` / `p95` / `p99` (ms)** | `38.297` / **`51.417`** / `61.686` | `39.439` / **`53.437`** / `60.990` |
| Max Kafka Consumer Lag (all 4 groups) | `0` | `0` |
| Correctness Violations (`total_violations`) | **`0`** | **`0`** |

Per-run range (`hot_e2e p95` / `bg_e2e p95` / `ml-shadow` observed pods):
- **WL=A:** Run 1 `45.863 / 46.232 ms` (`2` pods), Run 2 `61.947 / 61.220 ms` (`2` pods), Run 3 `50.444 / 46.800 ms` (`4` pods after HPA scale-up).
- **WL=C:** Run 1 `60.784 / 56.340 ms` (`4` pods), Run 2 `54.063 / 51.029 ms` (`4` pods), Run 3 `55.282 / 52.943 ms` (`4` pods).

---

## 3. PostgreSQL & Kubernetes Resource Consumption

### 3.1 PostgreSQL Activity (`proc_db` / `biz_db`, Mean Across 3 Runs)

| Scenario | `proc_conns_peak` | `biz_conns_peak` | `proc_db` `xact_commit` | `biz_db` `xact_commit` | `proc_db` `blks_hit` / `blks_read` |
| --- | ---: | ---: | ---: | ---: | ---: |
| Exp A — WL=A (`LogReg`) | `29` | `26` | `9,755.0` | `2,196.7` | `374,066.0` / `192.0` |
| Exp A — WL=C (`LogReg`) | `30` | `27` | `9,908.3` | `2,209.7` | `371,496.0` / `4,722.3` |
| Exp B — WL=A (`Real HGB`) | `33` | `27` | `9,858.3` | `2,208.0` | `396,020.7` / `1,838.7` |
| Exp B — WL=C (`Real HGB`) | `33` | `27` | `9,854.7` | `2,179.0` | `389,147.3` / `1,076.7` |

### 3.2 Cluster & Pipeline Pod CPU / Memory (Aggregated Group Mean & Peak)

| Deployment / StatefulSet | Exp A WL=A (CPU mean / peak, Mem pk) | Exp A WL=C (CPU mean / peak, Mem pk) | Exp B WL=A (CPU mean / peak, Mem pk) | Exp B WL=C (CPU mean / peak, Mem pk) |
| --- | ---: | ---: | ---: | ---: |
| `ingestion-api` (2 pods) | `132.0m` / `234m`, `154 Mi` | `214.5m` / `226m`, `154 Mi` | `162.9m` / `253m`, `164 Mi` | `210.9m` / `233m`, `163 Mi` |
| `processing-worker` (6 pods) | `605.1m` / `691m`, `523 Mi` | `652.3m` / `733m`, `524 Mi` | `650.8m` / `785m`, `523 Mi` | `701.9m` / `767m`, `526 Mi` |
| `ml-service` (2 pods) | `203.2m` / `308m`, `253 Mi` | `308.6m` / `343m`, `253 Mi` | `5.3m` / `6m`, `257 Mi` (idle) | `4.6m` / `6m`, `266 Mi` (idle) |
| `ml-service-shadow` (1–4 pods) | `2.6m` / `3m`, `131 Mi` (1 pod) | `3.0m` / `3m`, `131 Mi` (1 pod) | `475.3m` / `1,053m`, `538 Mi` (2->4) | `565.7m` / `729m`, `539 Mi` (4) |
| `processing-outbox-relay` (2 pods) | `236.9m` / `322m`, `175 Mi` | `310.9m` / `318m`, `175 Mi` | `264.7m` / `327m`, `179 Mi` | `316.2m` / `325m`, `178 Mi` |
| `business-consumer` (4 pods) | `349.5m` / `563m`, `304 Mi` | `531.9m` / `562m`, `306 Mi` | `413.1m` / `601m`, `306 Mi` | `540.4m` / `601m`, `306 Mi` |
| `storage-consumer` (1 pod) | `402.3m` / `590m`, `95 Mi` | `571.9m` / `588m`, `95 Mi` | `339.7m` / `571m`, `96 Mi` | `575.2m` / `613m`, `96 Mi` |
| `postgres` (1 pod) | `274.3m` / `443m`, `345 Mi` | `450.0m` / `520m`, `400 Mi` | `395.0m` / `598m`, `559 Mi` | `483.8m` / `548m`, `656 Mi` |
| `kafka` (1 pod) | `182.1m` / `259m`, `1,012 Mi` | `181.1m` / `191m`, `1,005 Mi` | `146.5m` / `217m`, `1,036 Mi` | `181.6m` / `197m`, `1,009 Mi` |
| **Pipeline Total (per-run range)** | `986.7m–2,751.5m`, `1,805–1,826 Mi` | `2,772.4m–2,918.5m`, `1,827–1,829 Mi` | `1,178.4m–3,384.8m`, `1,958–2,255 Mi` | `2,856.2m–3,557.5m`, `2,257–2,265 Mi` |
| **All Cluster Pods (per-run range)** | `1,333.8m–3,772.7m`, `4,252–4,414 Mi` (`30` pods) | `3,749.2m–3,876.7m`, `4,448–4,532 Mi` (`30` pods) | `1,516.3m–4,466.0m`, `4,879–5,217 Mi` (`31–33` pods) | `3,845.7m–4,585.9m`, `5,143–5,251 Mi` (`33` pods) |

---

## 4. Key Takeaways for Baseline on Real k3d

1. **No Head-of-Line Collapse on 50 RPS with Fast CPU Models:** Because native Linux CPU inference for LogReg (`~1–3 ms`) and HGB (`~6.8 ms` single-req / `~10–13 ms` at `c=1..2`) completes well inside `M-001`'s `62.5 ms` (`16 RPS`) inter-arrival interval, **Baseline (`per_key=1`)** achieved `50.00 RPS` on BOTH Workload A and Workload C with `hot_e2e_accept_to_biz p95 = 48.69 ms` (LogReg) and `56.71 ms` (Real HGB), `0` Kafka lag, and `0` correctness violations.
2. **Real HGB vs ControlledMLProvider Contrast:** Unlike `ControlledMLProvider` (`25 ms` mean + `10–28%` `300 ms` tails in Phases 0/2/4, where `M-001` backed up past `max_pending_per_key=64` and stalled `per_key=1` at `35.5 RPS`), clean CPU HGB without artificial `300 ms` stalls does **not** saturate `per_key=1` at `16 RPS` per hot machine.
