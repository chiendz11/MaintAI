# Final Comparison & Recommendation: Baseline vs Hybrid Ordering on MaintAI

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Evidence Corpus:**
  - Phase 1 ML Service Audit & HGB Sweep: [ml-service/REPORT.md](./ml-service/REPORT.md)
  - Phase 2 & 4 Database Bottleneck + Opt 1/2: [database/REPORT.md](./database/REPORT.md)
  - Phase 3 & 4 Business Consumer + Opt 3: [business-consumer/REPORT.md](./business-consumer/REPORT.md)
  - Phase 5 k3d Baseline (`per_key=1`, `batch=1`): [k3d-baseline/REPORT.md](./k3d-baseline/REPORT.md)
  - Phase 5 k3d Hybrid, Replica Sweeps & Chaos: [k3d-hybrid/REPORT.md](./k3d-hybrid/REPORT.md)

---

## 1. Cross-Environment Matrix (`50 RPS` Official Workloads A & C)

| Dimension / Variant | ML Model / Commit Mode | Mode (`per_key`, `batch`) | WL=A Proc / Biz (RPS) | WL=A Hot / Bg E2E p95 (ms) | WL=C Proc / Biz (RPS) | WL=C Hot / Bg E2E p95 (ms) | Violations |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| **Controlled Single-Worker (Initial)** | `ControlledMLProvider` (`25ms + 10% 300ms`), `sync_commit=on` | Baseline (`pk=1, b=1`) | `49.98 / 45.18` | `2,066 / 3,514` | **`35.53` / `35.38`** | **`29,301` / `17,723`** | `0` |
| **Controlled Single-Worker (Initial)** | Same | Hybrid (`pk=2, b=1`) | **`45.87` / `34.76`** | `4,349 / 15,001` | `43.58 / 35.73` | `8,311 / 12,090` | `0` |
| **Controlled Single-Worker (`Opt 1`)** | Same + `sensor_events` index & SQL pruning | Hybrid (`pk=2, b=1`) | `50.18 / 44.00` | `2,384 / 7,772` | `46.78 / 42.98` | `5,791 / 5,195` | `0` |
| **Controlled Single-Worker (`Opt 1 + Opt 3`)** | Same + Wake-on-done & `batch=16` | Hybrid (`pk=2, b=16`) | **`51.05` / `51.58`** | **`634` / `1,183`** | `46.69 / 46.91` | `4,844 / 1,165` | `0` |
| **Controlled Single-Worker (`Opt 1 + Opt 3`)** | Same | Hybrid (`pk=4, infl=16, b=16`) | — | — | **`50.47` / `50.98`** | **`1,114` / `988`** (`-96.2%`) | `0` |
| **k3d Multi-Pod (Exp A — Champion)** | Real CPU LogReg (`6 pw, 2 ing, 2 ml, 4 biz`), `sync_commit=off` | Baseline (`pk=1, b=1`) | `50.02 / 50.00` | **`42.07` / `41.51`** | `50.00 / 50.00` | **`48.69` / `45.29`** | `0` |
| **k3d Multi-Pod (Exp A — Champion)** | Same | Hybrid (`pk=2, infl=8, b=16`) | `50.00 / 50.00` | `50.20 / 44.50` | `50.00 / 50.00` | `49.58 / 47.53` | `0` |
| **k3d Multi-Pod (Exp B — Candidate)** | Real CPU HGB (`6 pw, 2 ing, 2–4 ml, 4 biz`), `sync_commit=off` | Baseline (`pk=1, b=1`) | `50.02 / 50.00` | **`52.75` / `51.42`** | `50.00 / 50.00` | **`56.71` / `53.44`** | `0` |
| **k3d Multi-Pod (Exp B — Candidate)** | Same | Hybrid (`pk=2, infl=8, b=16`) | `50.00 / 50.00` | `51.15 / 49.84` | `49.98 / 49.98` | `57.16 / 54.02` | `0` |
| **k3d Multi-Pod (Exp B — `pk=4`)** | Real CPU HGB (`2` active pods, `cpu_pk=1,337m`), `sync_commit=off` | Hybrid (`pk=4, infl=16, b=16`) | — | — | `50.00 / 49.93` | **`138.37` / `113.07`** | `0` |

> **Scope Caveat:** Real k3d runs inherited `synchronous_commit=off` from `MaintAI-gitops/local/platform/postgres.yaml` and used a benchmark identity fixture (`/bench-fixtures/model-identity.json`). Production AWS RDS defaults to `synchronous_commit=on` over network EBS/AZ links, where `Opt 1` + `Opt 3` commit reduction matters far more than on local tmpfs/host disk.

---

## 2. Structural Synthesis Across All 5 Phases

1. **ControlledMLProvider vs Real CPU HGB Explained:**
   - The `ControlledMLProvider` simulation injected `25 ms` mean latency plus a **`10–28%` tail of `300 ms`** requests. At `16 RPS` (`62.5 ms` inter-arrival) on `M-001`, a `300 ms` stall caused `4.8` new readings to queue behind each slow reading, overwhelming `per_key=1` (`35.53 RPS`, `29.3 s` p95).
   - Real CPU `HistGradientBoostingClassifier` (`hgb-interactions-candidate-v1`) on native Linux x86_64 takes only **`6.77 ms` p50 (`8.50 ms` p95)** in-process and **`10.68 ms` p50 (`12.83 ms` p95)** via HTTP at `c=1` (`90.05 RPS` ceiling/pod; `188–205 RPS` on 2 pods). At `16 RPS` (`62.5 ms` budget), `per_key=1` finishes each reading ~`5×` faster than the arrival interval, so Head-of-Line queuing simply never triggers.
2. **Why Hybrid Still Has Architectural Value (And Where It Hurts):**
   - When real or future ML inference exhibits `mean >= 50–80 ms` or frequent `>= 200 ms` tails (e.g., cross-AZ hops, heavier tree/neural ensembles, or `ml-service-mirror` saturation), Hybrid `pk=2..4` is transformative (**`35.53 -> 50.47 RPS`**, **`-96.2%` Hot E2E p95** `29.3 s -> 1.11 s` on `ControlledMLProvider`).
   - Conversely, when ML is fast (`< 20 ms`), setting `per_key=2` adds `AlertResequencer` overhead (`+1–8 ms` p95), while aggressive **`per_key=4` against `2` single-worker Python HGB pods worsens latency (`57.16 -> 138.37 ms`)** because `M-001`'s parallel burst trips Starlette threadpool GIL contention (`cpu_peak=1,337m`).
3. **Permanent, Unconditional Wins (`Opt 1` & `Opt 3`):**
   - **Opt 1 (`11f9e9f`)** added the missing `sensor_events(machine_id, sequence_number)` index (`Seq Scan 92 buffers -> Index Scan 0.013 ms`) and removed duplicate SQL checks (`TxA`, `StageB_prepare`, `drain_frontier`).
   - **Opt 3 (`8f3a88d`)** eliminated `4.3–5.2 ms` idle poll wake delay on `ConsumerRunner` and introduced opt-in `KAFKA_COMMIT_BATCH_SIZE=16` / `KAFKA_COMMIT_MAX_WAIT_MS=25`, lifting `1`-pod `business-consumer` capacity past `50 RPS` (`hot_e2e p95`: `230.08 -> 84.08 ms` on k3d; `k2b_p95`: `1,614 -> 45.18 ms` controlled). Both Opt 1 & Opt 3 strictly improve **Baseline** as well as **Hybrid**.

---

## 3. Final Decision: **Conditional Adopt (Option 2)**

1. **Unconditionally Keep `Opt 1` + `Opt 3` (Enabled in Runtime Code):**
   - Keep `sensor_events_machine_seq` index, SQL pruning, `ConsumerRunner` wake-on-done, and `poll(0)` backpressure safety fix permanently on `feat/hybrid-ordering`.
   - Enable `KAFKA_COMMIT_BATCH_SIZE=16` and `KAFKA_COMMIT_MAX_WAIT_MS=25` selectively on **`business-consumer`** (keeping default `1` in SDK & `storage-consumer`).
2. **Conditionally Adopt Hybrid Ordering (Default OFF; Targeted Activation Only):**
   - Keep **`PROCESSING_HYBRID_ORDERING_ENABLED=false` (`max_inflight_per_key=1`) as default** for the current fast CPU `logreg-baseline-v0` (`~2 ms`) and `hgb-interactions-candidate-v1` (`~7–13 ms`) workloads on multi-pod Kubernetes, because Baseline already hits `50.0 RPS` on Workload A and Workload C with equal/lower latency (`42–57 ms` p95) and simpler operational failure modes (no `PENDING` ledger hole on bad offset resets).
   - **Activation Criterion:** Turn on `PROCESSING_HYBRID_ORDERING_ENABLED=true` with **`max_inflight_per_key=2`** (hard max `4` ONLY with `>= 4` ML replicas) only if telemetry shows **hot-machine arrival rate × ML inference p95 >= `0.70`** (i.e., `lambda_hot * W_ml_p95 >= 0.7`, such as `M-001` `>= 16 RPS` paired with `>= 45–50 ms` ML p95 or frequent `200–300 ms` tail spikes).

---

## 4. Direct Answers to the 8 Core Executive Questions

| # | Question | Verified Answer |
|---|---|---|
| **1** | **ML Service thực sự chịu được bao nhiêu concurrent requests?** | `1` HGB pod (`1` uvicorn worker, `OMP=1`, no GPU — `RTX 2050` unused) peaks at **`90–101 RPS`** (`c=1` `p95=12.83ms`, `c=2` `p95=24.71ms`). Above `c=4` (`p95=54.67ms`), Python GIL (`114.7%` CPU) turns added concurrency into queueing; at `1 CPU` limit `nr_throttled=24–25`. **Safe per-pod limit: `c=1..2` (`~65–80 RPS`), `<= 4` hard cap.** `2` HGB pods deliver `188–205 RPS` (`p95=12.90–25.06ms`). |
| **2** | **Processing Worker nên có bao nhiêu tasks mỗi Pod và toàn cluster?** | Under `Opt 1` + fast ML, `1` worker pod handles `~47 RPS` (`hot_p95=1,601ms`); `2` pods hit `50 RPS`. Recommended cluster topology for `50 RPS` HA (`N+1`/`N+2`): **`4–6` worker pods** (must be `<= 12` Kafka partitions), **`per_key=1` (Baseline) or `2` (Hybrid opt-in)**, **`PROCESSING_MAX_TOTAL_INFLIGHT=4–8/pod`** (`max 16–32` cluster-wide matching ML capacity `<= 4/ML pod`). Never exceed `ML_replicas × 4`. |
| **3** | **Workload A suy giảm do nguyên nhân nào?** | Initial unoptimized Hybrid regressed WL=A (`50.0 -> 41.2 RPS`) because: (**a**) `KeyedConsumerRunner` ran `12 SQL` (`TxA_admit`) **serially per record on the single Kafka poll thread**, stalling polling `46.6–69.3%`; (**b**) missing `sensor_events(machine_id, sequence_number)` index triggered **2 full `Seq Scan`s per reading** (`3.6×` PG buffer hits); (**c**) `TxC` ran `31.5 SQL`, doubling WAL fsync contention under `synchronous_commit=on`. Resolved by `Opt 1` (`11f9e9f`) -> WL=A recovered to `50.18 RPS` (`51.05` with `Opt 3`). |
| **4** | **TxA/TxC có thực sự là bottleneck không?** | **YES — before Optimization 1.** `TxA_admit` (`12 SQL`, `61.6%` poll stall) and `TxC_persist` (`31.5 SQL`, `2× Seq Scan` on `sensor_events`, duplicate `require_ordering` & cursor reads) were root causes #1 and #2. Locking (`pg_locks NOT granted = 0`) was NOT the bottleneck, and disabling `synchronous_commit=off` still left `56.16%` poll stall. After `Opt 1`, `proc_db` transactions dropped `~20%` (`~9,858 -> ~7,800` per 1k events on k3d) and bottleneck vanished at `50 RPS`. |
| **5** | **Business Prediction Consumer có phải bottleneck không?** | **YES at `1` replica (`batch=1`), in BOTH Baseline and Hybrid.** `1` pod saturated at **`40.82–43.56 RPS`** due to per-record synchronous Kafka commit (`~5.7ms`) + `poll(10ms)` wake delay (`4.3–5.2ms` p50), making up `~55–60%` of loop time. **Fixed by `Opt 3` (`8f3a88d`):** Wake-on-done (`0.16ms` delay) + `KAFKA_COMMIT_BATCH_SIZE=16` cut `k2b_p95` **`-92.7%`** on k3d (`138.24 -> 10.05ms` at `1` pod; `hot_p95: 230.08 -> 84.08ms`). |
| **6** | **Hybrid có tốt hơn Baseline trên k3d đa-pod không?** | **On fast LogReg & current CPU HGB at `50 RPS`, NO.** Baseline (`pk=1`) already achieves `50.0 RPS` on WL=A & WL=C (`hot_p95 = 42.1–56.7ms`) vs Hybrid `pk=2` (`50.2–57.2ms`), while `pk=4` on 2 HGB pods degrades to **`138.37ms` p95**. Hybrid wins decisively **only** when ML suffers high latency/tails (`ControlledMLProvider 300ms`: `35.5 -> 50.5 RPS`, `-96.2%` p95). |
| **7** | **Cấu hình concurrency và CPU/RAM cuối cùng nên là bao nhiêu?** | **Default (`Baseline + Opt 1 + Opt 3`):** `hybrid=false, pk=1, max_active_keys=8, max_total_inflight=4–8/pod`, `biz_commit_batch=16`. **Hybrid Conditional Profile (`when ML p95 >= 45–50ms`):** `hybrid=true, pk=2, max_total_inflight=4–8/pod`. **Prod `50 RPS` HA (`N+1`/`N+2`):** `ingestion-api=2` (`150m/500m`, `192/384Mi`), `processing-worker=4` (`300m/1000m`, `256/512Mi`), `ml-service LogReg=2` (`200m/1000m`, `256/512Mi`) hoặc **`HGB CPU=3`** (`500m/1500m`, `256/512Mi`), `outbox-relay=2` (`200m/500m`, `192/384Mi`), `business-consumer=3` (`250m/1000m`, `256/512Mi`) — see full matrix in [CAPACITY_PLANNING.md](./CAPACITY_PLANNING.md) & [DEPLOYMENT_SIZING.md](../capacity-planning/DEPLOYMENT_SIZING.md). |
| **8** | **Có đủ bằng chứng để merge Hybrid vào main chưa?** | **Chưa để bật mặc định (Default OFF), nhưng đã đủ kiểm chứng kỹ thuật cho nhánh code (`Opt 1 + Opt 3 + Hybrid behind flag=false`) sau review:** (a) Opt 3 code (`8f3a88d`) passed `516` tests (`10` skipped) + `48` unit tests re-verified post-doc cleanup, cùng **`0` violations qua `45` k3d runs** (`12` baseline + `15` hybrid + `15` sweeps + `3` chaos pod kills); (b) tuy nhiên k3d mới test `50 RPS` (`15s` burst, `synchronous_commit=off`, benchmark identity fixture) — chưa test Production RDS `synchronous_commit=on` Multi-AZ hay `100+ RPS`, và `pk=4` chưa mang lại lợi ích trên HGB CPU hiện tại. **Khuyến nghị:** Có thể PR/review giữ `PROCESSING_HYBRID_ORDERING_ENABLED=false`, giữ `Opt 1` + `Opt 3` (`KAFKA_COMMIT_BATCH_SIZE=16` cho `business-consumer`), không bật Hybrid hay thay GitOps production tự động. |
