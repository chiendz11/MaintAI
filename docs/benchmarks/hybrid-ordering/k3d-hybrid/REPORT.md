# Phase 5 Report: k3d Multi-Pod Hybrid Ordering, Replica Sweeps & Pod-Kill Chaos

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Cluster / Testbed:** `k3d-maintai-local` (K3s `v1.35.5+k3s1`, `AMD Ryzen 7 7435HS` 8C/16T, `15 GiB` RAM, RTX 2050 idle/unused, PG 17 `synchronous_commit=off`, Kafka 4.0.0 `12` partitions)
- **Raw Data:**
  - Experiment A (Champion LogReg, `pk=2, inflight=8, batch=16`): [expA_fast_ml.json](./expA_fast_ml.json) (`6` runs)
  - Experiment B (Real HGB, `pk=2, inflight=8` & `pk=4, inflight=16`, `batch=16`): [expB_real_hgb.json](./expB_real_hgb.json) (`9` runs)
  - Replica Sweeps (`15` runs on WL=C HGB, `pk=4, inflight=16, batch=16`): [replica_sweeps.json](./replica_sweeps.json)
  - Pod-Kill Chaos (`3` runs on WL=C HGB, `pk=4, inflight=16, batch=16`): [pod_kill_tests.json](./pod_kill_tests.json)

---

## 1. Experiment A & B: Hybrid Ordering vs Baseline Topology (3-Run Means)

### 1.1 Experiment A — Champion LogReg (`ml-service`, 2 Pods)

Config: `PROCESSING_HYBRID_ORDERING_ENABLED=true`, `per_key=2`, `max_total_inflight=8` per worker, `KAFKA_COMMIT_BATCH_SIZE=16`, `worker=6, relay=2, biz=4, ing=2`.

| Metric | Hybrid A (`pk=2, batch=16`) | Hybrid C (`pk=2, batch=16`) | Delta vs k3d Baseline |
| --- | ---: | ---: | --- |
| Sent / Proc / Biz (win RPS) | `50.0` / `50.0` / `50.0` | `50.0` / `50.0` / `50.0` | Parity (`50.0 RPS`) |
| Drained / Mean Drain Time | `Yes` / `0.058 s` | `Yes` / `0.063 s` | `+5–9 ms` |
| Ingress HTTP `p50` / `p95` (ms) | `7.207` / `8.418` | `7.435` / `9.476` | Unchanged |
| `accept_to_proc` `p50` / `p95` (ms) | `14.027` / `20.240` | `15.691` / `22.404` | `+0.7–1.1 ms` |
| `proc_to_outbox` `p50` / `p95` (ms) | `18.658` / `26.954` | `19.074` / `27.575` | `+3.1–3.4 ms` p95 (resequencer) |
| `outbox_to_biz_pred` `p50` / `p95` (ms) | `1.111` / `2.757` | `1.222` / `5.380` | `-1.62 ms` p95 on C (`batch=16`) |
| **Hot (`M-001`) E2E `p50` / `p95` (ms)** | `36.661` / **`50.201`** | `38.391` / **`49.580`** | `+8.13 ms` on A / `+0.89 ms` on C |
| **Bg E2E `p50` / `p95` (ms)** | `34.599` / **`44.499`** | `36.459` / **`47.530`** | `+2.99 ms` on A / `+2.24 ms` on C |
| `proc_db` `xact_commit` / `conns_pk` | **`7,848.3`** / `27` | **`7,777.0`** / `27` | **`-20%` fewer commits** vs Base (`9,755–9,908`) |
| Max Kafka Lag / Violations | `0` / **`0`** | `0` / **`0`** | `0` violations |

### 1.2 Experiment B — Real HGB (`ml-service-shadow`)

> [!NOTE]
> **Actual Active `ml-service-shadow` Pod Count:** In `wlA-pk2` and `wlC-pk2`, `ml-service-shadow-hpa` scaled from `2 -> 3` (`wlA` Run 3) and `4` pods (`wlC`). In `wlC-pk4`, the cluster had just reset so **only `2` HGB pods** handled the `15s` burst (`peak CPU = 1,337m`), cleanly isolating `per_key=4` contention on `2` HGB pods!

| Metric | Hybrid A (`pk=2, infl=8`) | Hybrid C (`pk=2, infl=8`) | Hybrid C (`pk=4, infl=16`) |
| --- | ---: | ---: | ---: |
| Observed Active HGB Pods | `2–3` | `4` (via HPA) | **`2`** |
| Sent / Proc / Biz (win RPS) | `50.0` / `50.0` / `50.0` | `50.0` / `49.977` / `49.977` | `50.0` / `50.0` / `49.933` |
| Drained / Mean Drain Time | `Yes` / `0.087 s` | `Yes` / `0.089 s` | `Yes` / `0.115 s` |
| Ingress HTTP `p50` / `p95` (ms) | `7.252` / `8.539` | `7.397` / `9.560` | `10.046` / `17.613` |
| `accept_to_proc` `p50` / `p95` / `p99` (ms) | `18.472` / `25.066` / `29.688` | `20.512` / `29.897` / `37.492` | **`30.624`** / **`73.811`** / `107.554` |
| `proc_to_outbox` `p50` / `p95` (ms) | `18.856` / `27.196` | `19.216` / `28.078` | `27.457` / `49.910` |
| `outbox_to_biz_pred` `p50` / `p95` (ms) | `1.166` / `4.154` | `1.180` / `5.402` | `1.545` / `7.321` |
| **Hot (`M-001`) E2E `p50` / `p95` / `p99` (ms)** | `43.994` / **`51.147`** / `53.848` | `43.415` / **`57.157`** / `68.127` | `66.495` / **`138.365`** / `183.915` |
| **Bg E2E `p50` / `p95` / `p99` (ms)** | `39.394` / **`49.837`** / `55.497` | `41.424` / **`54.021`** / `65.034` | `59.575` / **`113.073`** / `153.295` |
| `ml-service-shadow` CPU mean / peak | `366.9m` / `612m` | `485.4m` / `520m` | **`606.0m`** / **`1,337m`** |
| `processing-worker` CPU mean / peak | `561.0m` / `734m` | `633.2m` / `707m` | `883.8m` / `1,184m` |
| `proc_conns_pk` / `biz_conns_pk` | `28` / `27` | `29` / `28` | `30` / `28` |
| `proc_db` / `biz_db` `xact_commit` | `7,798.7` / `2,425.3` | `7,798.3` / `2,176.7` | `7,444.0` / `2,289.3` |
| Max Kafka Lag / Violations | `0` / **`0`** | `0` / **`0`** | `0` / **`0`** |

- **`per_key=4, inflight=16` on `2` HGB Pods Degrades Latency (`2.4×` Worse p95):** Compared to `pk=2` (`57.16 ms`), pushing `pk=4` (`M-001` burst concurrency into 2 single-process Python HGB pods) triggers CPU/GIL threadpool contention inside `ml-service-shadow` (`cpu_peak = 1,337m`, `hot_e2e p95 = 138.37 ms`, `run 2 = 177.18 ms`) and inflates worker CPU (`883.8m / 1,184m`).

---

## 2. Single-Variable Replica Sweeps (15 Runs, WL=C 50 RPS, Real HGB, `pk=4, inflight=16, batch=16`)

All sweeps held other variables constant except HPA auto-expansion on `ml-service-shadow-hpa` (`maxReplicas=4`), noted explicitly below (`ml_cfg` vs `ml_obs` active pods). Every single run drained completely (`0` Kafka lag, `0` violations).

| Sweep Component | Target Config (`ml_cfg` / `ml_obs` / `pw` / `relay` / `biz` / `batch` / `ing`) | Proc RPS | Biz RPS | `proc_p95` (ms) | `outbox_p95` (ms) | `k2b_p50 / p95` (ms) | `hot_p50 / p95` (ms) | `bg_p95` (ms) | Component CPU mean / peak (m) | Component Mem peak (Mi) | PG Conns (`proc` / `biz`) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ml-service-shadow` | **`1`** / `1` (`pw=6, r=2, b=4, bt=16, i=2`) | **48.00** | **48.13** | **2,840.90** | 121.65 | `1.89 / 14.10` | `1,695.6 /` **`3,108.0`** | 615.01 | `282.8m / 354m` | `127` | `38 / 29` |
| `ml-service-shadow` | **`2`** / `2` (`pw=6, r=2, b=4, bt=16, i=2`) | 49.93 | 50.00 | 84.17 | 56.25 | `1.68 / 8.92` | `76.0 / 175.79` | 124.48 | `959.4m / 1,258m` | `255` | `38 / 29` |
| `ml-service-shadow` | **`3`** / `4` (`pw=6, r=2, b=4, bt=16, i=2`) | 50.00 | 50.07 | **43.54** | 38.86 | `1.60 / 6.71` | `59.95 /` **`89.91`** | 77.42 | `962.9m / 1,070m` | `510` | `38 / 29` |
| `processing-worker` | `2` / `4` / **`pw=1`** / `2` / `4` / `16` / `2` | **47.07** | **47.20** | **1,669.60** | 135.73 | `1.53 / 7.03` | `1,352.2 /` **`1,601.2`** | **1,842.01** | `339.7m / 637m` | `92` | `26 / 29` |
| `processing-worker` | `2` / `4` / **`pw=2`** / `2` / `4` / `16` / `2` | 50.00 | 50.00 | 48.38 | 41.01 | `1.49 / 6.32` | `63.81 / 95.55` | 86.02 | `824.1m / 893m` | `180` | `32 / 29` |
| `processing-worker` | `2` / `4` / **`pw=4`** / `2` / `4` / `16` / `2` | 50.00 | 50.13 | 47.98 | 36.16 | `1.44 / 5.99` | `59.58 / 82.48` | 75.74 | `829.6m / 915m` | `353` | `38 / 29` |
| `processing-worker` | `2` / `4` / **`pw=6`** / `2` / `4` / `16` / `2` | 50.07 | 50.07 | **40.04** | 33.93 | `1.38 / 6.04` | `54.77 /` **`76.73`** | 68.25 | `726.8m / 833m` | `530` | `40 / 29` |
| `processing-outbox-relay` | `2` / `2` / `6` / **`relay=1`** / `4` / `16` / `2` | 50.00 | 50.00 | 56.18 | **48.25** | `1.41 / 5.39` | `62.52 / 118.65` | 93.85 | `183.3m / 192m` | `87` | `38 / 29` |
| `processing-outbox-relay` | `2` / `4` / `6` / **`relay=2`** / `4` / `16` / `2` | 50.00 | 50.07 | 54.10 | **40.19** | `1.43 / 6.45` | `55.54 / 106.72` | 91.10 | `392.6m / 412m` | `171` | `40 / 29` |
| `business-consumer` | `2` / `4` / `6` / `2` / **`biz=1, bt=1`** / `2` | 50.13 | 50.13 | 65.14 | 44.49 | **`6.23 / 138.24`** | `59.96 /` **`230.08`** | **205.19** | `437.7m / 778m` | `84` | `42 / 20` |
| `business-consumer` | `2` / `4` / `6` / `2` / **`biz=1, bt=16`** / `2` | 50.00 | 50.13 | 45.42 | 33.33 | **`1.80 / 10.05`** | `59.52 /` **`84.08`** | **76.57** | `658.0m / 658m` | `75` | `42 / 19` |
| `business-consumer` | `2` / `4` / `6` / `2` / **`biz=2, bt=16`** / `2` | 50.00 | 50.07 | 40.78 | 34.66 | `1.54 / 7.23` | `54.55 / 75.41` | 71.84 | `569.9m / 632m` | `150` | `42 / 22` |
| `business-consumer` | `2` / `4` / `6` / `2` / **`biz=4, bt=16`** / `2` | 50.00 | 50.00 | 36.79 | 31.60 | `1.25 / 5.44` | `49.68 /` **`68.93`** | 63.39 | `602.5m / 633m` | `300` | `42 / 28` |
| `ingestion-api` | `2` / `4` / `6` / `2` / `4` / `16` / **`ing=1`** | 50.00 | 49.93 | 43.69 | 34.40 | `1.34 / 5.87` | `53.87 / 82.44` | 72.98 | `147.3m / 202m` | `77` | `42 / 28` |
| `ingestion-api` | `2` / `4` / `6` / `2` / `4` / `16` / **`ing=2`** | 50.00 | 50.07 | 35.81 | 31.68 | `1.32 / 5.91` | `49.98 /` **`66.88`** | 62.98 | `246.2m / 352m` | `154` | `42 / 28` |

> Sweep Highlights:
> - **`ml-service-shadow=1`** cannot keep up with WL=C at `pk=4` (`48.00 RPS`, `hot_p95 = 3,108 ms`), proving `1` HGB pod fails both capacity and `N+1` HA under skewed bursts. `2` HGB pods sustain `50 RPS` (`175.79 ms` at `pk=4`), and `4` pods halve `hot_p95` (`89.91 ms`).
> - **`processing-worker=1`** saturates at **`47.07 RPS`** (`hot_p95 = 1,601.19 ms`, `bg_p95 = 1,842.01 ms`). Scaling to **`2`** restores `50.0 RPS` (`95.55 ms`); **`4–6`** reduces `hot_p95` smoothly (`82.48 -> 76.73 ms`) and distributes 12 Kafka partitions cleanly.
> - **`business-consumer=1`:** Opt 3 (`batch=16`) alone drops `k2b_p95` by **`-92.7%`** (`138.24 -> 10.05 ms`) and `hot_p95` from `230.08 -> 84.08 ms` on a single pod! Adding a `2nd` pod covers HA (`75.41 ms`).

---

## 3. Pod-Kill Chaos Tests (WL=C 50 RPS, Real HGB, `pk=4, inflight=16, batch=16`)

Mid-measurement pod kills injected via Kubernetes API while sustaining full `50.0 RPS` Workload C (`1,000` events per run):

| Victim Pod Killed | Recovery (s) | Drain (s) | Proc / Biz Win RPS | HTTP p95 (ms) | Hot E2E p50 / p95 / max (ms) | Bg E2E p50 / p95 / max (ms) | `k2b` p99 / max (ms) | Outcome Kinds / Violations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `processing-worker` (`-57svj`, 6->5->6) | **`10.675`** | `0.118` | `50.20` / `50.00` | `14.69` | `58.44 / 222.79 / 407.65` | `52.53 / 124.86 / 1,449.18` | `12.79 / 22.92` | `1,000` `PREDICTION_COMPLETED` / **`0`** |
| `business-consumer` (`-cp44k`, 4->3->4) | **`7.545`** | `0.125` | `50.07` / `50.00` | `14.60` | `61.79 / 155.48 / 206.48` | `55.05 / 172.18 / 2,479.12` | `1,523.44 / 2,423.25` | `1,000` `PREDICTION_COMPLETED` / **`0`** |
| `ml-service-shadow` (`-gmb69`, 4->3->4) | **`10.808`** | `0.123` | `50.00` / `50.00` | `12.86` | `55.86 / 114.85 / 208.13` | `50.60 / 97.89 / 216.66` | `10.66 / 20.32` | `1,000` `PREDICTION_COMPLETED` / **`0`** |

All 3 single-pod failures recovered automatically within **`7.5 – 10.8 s`** with **`1,000/1,000` `PREDICTION_COMPLETED`**, **`0` duplicate alerts**, **`0` stale prediction overwrites**, **`0` OOMKills**, and **`0` Kafka lag remaining** at drain. During `business-consumer` rebalance (`7.545 s`), outbox-published events buffered safely in Kafka (`k2b max = 2,423.25 ms`, `bg max = 2,479.12 ms`) and caught up in `0.125 s`.

---

## 4. Critical Operational & Safety Findings Exposed on k3d

Multi-pod k3d execution surfaced **3 real distributed-systems operational hazards** that controlled single-process harnesses could not expose:

1. **Fail-Closed Serving Model Identity Guard ([model_identity.py](../../../../processing/shared/inference_client/model_identity.py#L18-L29)):**
   - When `processing-worker` first called `ml-service-shadow` (`hgb-interactions-candidate-v1`) while still configured with the default LogReg fixture (`logreg-baseline-v0`), `resolve_model_identity` deterministically threw `ValueError("Serving model identity does not match prediction response")` and refused to record misattributed predictions (`0` corrupted rows). **Implication:** Any shadow/champion model cutover in GitOps must atomically rotate the serving-bound identity artifact alongside `ML_BASE_URL`.
2. **`TxA` `PENDING` Ledger Hole vs Blind Kafka Offset Reset Deadlock:**
   - In Hybrid Ordering, interrupting a stream after `TxA` writes `PENDING` rows to `processing_alert_ledger` and then resetting Kafka offsets `--to-latest` leaves orphaned `PENDING` slots below active incoming `admission_seq` numbers. Subsequent `SIGNAL_READY` events (`420` rows across `28` machines in an interrupted pre-benchmark trial) stall strictly at `alert_frontier_seq` so alert ordering is **never** violated. **Operational Rule:** Operators must NEVER run `--reset-offsets --to-latest` on an active `stream_epoch` without rotating `stream_epoch` or explicitly reconciling interrupted `PENDING` ledger slots first.
3. **Multi-Replica HTTP-to-Kafka Per-Machine Ordering Race (`EVENT_CONFLICT` Poison Pill):**
   - Initially, driving `M-001` at `16 RPS` with concurrent unserialized async `POST`s across `2` `ingestion-api` replicas caused `seq=N+1` to enter Kafka partition 7 at an earlier offset than `seq=N`. Because MaintAI enforces strict monotonic per-machine source sequencing, `processing-worker` rejected the inverted record as `EVENT_CONFLICT` (`processing_conflicts`) and halted fail-closed (`CrashLoopBackOff`) instead of corrupting state.
   - **Rule Confirmed:** Every upstream producer (`sensor` or edge gateway) MUST serialize HTTP requests **per `machine_id`** (`await 202 Kafka ACK` before transmitting `seq + 1`), as enforced after updating [k3d_real_benchmark.py](../../../../scripts/benchmarks/hybrid_ordering/k3d_real_benchmark.py) with per-machine `asyncio.Queue` FIFO senders (`0` conflicts across all subsequent runs).
