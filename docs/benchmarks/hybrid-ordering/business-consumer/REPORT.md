# Phase 3 Report: Business Prediction Consumer Bottleneck Investigation

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Architecture Audited:** [ConsumerRunner](file:///home/chienqt/Dev/newBTL/MaintAI/messaging/src/maintai_messaging/consumer.py), [runtime.py](file:///home/chienqt/Dev/newBTL/MaintAI/backend/business-consumer/src/runtime.py), [handler.py](file:///home/chienqt/Dev/newBTL/MaintAI/backend/shared/inbox/handler.py), [projector.py](file:///home/chienqt/Dev/newBTL/MaintAI/backend/shared/projections/projector.py), [identity.py](file:///home/chienqt/Dev/newBTL/MaintAI/backend/shared/ordering/identity.py), [reducer.py](file:///home/chienqt/Dev/newBTL/MaintAI/backend/shared/projections/reducer.py), [ConnectionPool(size=8)](file:///home/chienqt/Dev/newBTL/MaintAI/backend/shared/persistence/pool.py)
- **Harness & Raw Data:** [business_consumer_benchmark.py](file:///home/chienqt/Dev/newBTL/MaintAI/scripts/benchmarks/hybrid_ordering/business_consumer_benchmark.py), [results.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/business-consumer/results.json) (21 runs: 3 runs × 7 configs), [lag_samples.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/business-consumer/lag_samples.json) (6 lag-profiled runs)

---

## 1. Root Cause Summary

**Is `business-prediction` a real bottleneck when running at 1 replica? YES — equally in BOTH Baseline and Hybrid.**
In our single-process controlled benchmarks (`1` `business-consumer` replica), `business-prediction` saturated at **`40.82 – 43.56 RPS`** against a `50 RPS` outcome stream because **one `ConsumerRunner` spends ~`20–24 ms` per record (`~42–50 RPS` hard serial ceiling)** across three sequential stages:

1. **Per-Record Synchronous Kafka Commit (~`5.70–5.76 ms` p50, ~`5.8 ms` mean):**
   - `ConsumerRunner.run()` commits every single offset individually with `self.client.commit(offsets=[...], asynchronous=False)` immediately after each DB commit ([consumer.py](file:///home/chienqt/Dev/newBTL/MaintAI/messaging/src/maintai_messaging/consumer.py#L181-L186)).
   - That costs **~`5.7 ms` per message** (broker roundtrip) on top of the DB transaction!
2. **Event-Loop Wake-Up Delay Inside `poll(poll_interval_seconds)` (~`4.3–5.2 ms` p50, ~`9.8 ms` p95):**
   - While the 1-thread worker executes (`future` running), the poll thread repeatedly calls `self.client.poll(0.01)` (`10ms` local, `100ms` default if `KAFKA_POLL_INTERVAL_SECONDS` unset!). Once `librdkafka` has prefetched or reaches EOF, `poll(0.01)` blocks waiting for the next broker fetch timeout even though the DB transaction already completed on the worker thread!
   - Measured **`wake_delay_ms = 4.34 – 5.24 ms` p50 (`9.37 – 9.91 ms` p95)** of pure idle waiting per record!
   - Combined, **`kcommit` (~5.7ms) + `wake_delay` (~4.9ms) = ~10.6ms/message (~55–60% of loop time!)** is non-DB overhead!
3. **Projector DB Transaction (`7.1–12.9 ms` p50, `25–34 ms` p95) on Shared `ConnectionPool(size=8)`:**
   - Each `business-prediction` message opens 1 DB tx executing ~`10` SQL statements: `INSERT maintai_kafka_inbox`, `SELECT machines FOR UPDATE`, `guard_source` (`INSERT/SELECT FOR UPDATE/UPDATE business_source_inbox`), `load` (`3` separate SELECTs on `latest_telemetry`, `latest_processing`, `latest_valid_prediction`), `reduce_projection`, `INSERT ... ON CONFLICT DO UPDATE` per changed table, and `SELECT pg_notify(...)`.
   - Note: `ConnectionPool(size=8)` shared across `business-live`, `business-prediction`, and `business-alert` had **negligible pool wait (`p95 <= 0.019 ms`)** even at `4` replicas (`3 × 4 = 12` consumer threads across 4 pools). `Alert` (`business-alert`) only fires on ~`10%` of events (`~5/s`) with `15–19ms` p50 (`+Evidence` HTTP/DB fetch) and caused zero backlog.

---

## 2. End-to-End Funnel & Latency Breakdown (3 Runs / Config)

### 2.1 Funnel & Throughput (`target = 50.00 RPS`)

| Config | `biz_replicas` | Sent RPS | Admitted RPS | Outcomes RPS | Outbox Pub RPS | Pred Consumed RPS | Biz Pred Commit RPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline WL=A | 1 | 50.00 | 50.00 | 49.96 | 49.71 | **40.82** | **40.82** |
| Hybrid WL=A | 1 | 50.00 | 50.20 | 50.22 | 50.25 | **42.58** | **42.58** |
| Hybrid WL=A | **2** | 50.00 | 49.58 | 49.64 | 49.49 | **49.51** | **49.51** |
| Hybrid WL=A | **4** | 50.00 | 50.76 | 51.00 | 51.07 | **51.00** | **51.00** |
| Hybrid WL=C | 1 | 50.00 | 46.20 | 44.98 | 44.91 | **43.56** | **43.56** |
| Hybrid WL=C | **2** | 50.00 | 47.96 | 45.67 | 45.84 | **45.87** | **45.87** |
| Hybrid WL=C | **4** | 50.00 | 46.04 | 45.31 | 45.13 | **45.18** | **45.16** |

> On Workload A (`50.0 outcomes/s`), scaling `business-consumer` from `1 -> 2` replicas eliminates the prediction consumer deficit completely (`42.58 -> 49.51/s`, matching upstream `49.64/s`). On Workload C, Hybrid's upstream `processing-worker` ceiling (~`45.3–46.2/s` before Phase 4 optimizations) is below `2`-replica Business Consumer capacity (~`88 RPS`), so `2` replicas drain `100%` of produced outcomes (`45.87 / 45.67 RPS`).

### 2.2 `business-prediction` Latency, Kafka Lag & Loop Timings

| Config | Replicas | `kafka_pub -> biz_commit` p50 / p95 (ms) | Handler Wall p50 (ms) | DB Tx p50 (ms) | DB Commit p50 (ms) | Kafka Sync Commit p50 (ms) | Wake Delay p50 / p95 (ms) | Pending QLen p50 / max | `max_pred_lag` (msgs, 1-run) | `pg_notify` p50 / p95 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline WL=A | 1 | 1,664 / **10,993** | 9.63 | 8.31 | 0.62 | 5.73 | 5.09 / 9.70 | 40.7 / 63 | **155** (growing) | 0.56 / 1.67 |
| Hybrid WL=A | 1 | 1,572 / **8,761** | 11.27 | 9.71 | 0.69 | 5.72 | 4.98 / 9.77 | 42.8 / 63 | **139** (growing) | 0.58 / 1.60 |
| Hybrid WL=A | **2** | **18.56 / 53.38** | 13.97 | 12.45 | 0.93 | 5.70 | 4.34 / 9.83 | **0.0 / 4** | **3** (flat) | 0.65 / 1.82 |
| Hybrid WL=A | **4** | **12.01 / 33.11** | 11.75 | 10.34 | 0.89 | 5.72 | 4.60 / 9.91 | **0.0 / 2** | **2** (flat) | 0.68 / 1.97 |
| Hybrid WL=C | 1 | 854 / **1,614** | 10.83 | 9.34 | 0.67 | 5.70 | 4.70 / 9.37 | 37.7 / 63 | **89** | 0.58 / 1.64 |
| Hybrid WL=C | **2** | **19.14 / 66.13** | 13.31 | 11.75 | 0.88 | 5.73 | 4.69 / 9.85 | **0.7 / 5** | **3** (flat) | 0.65 / 1.83 |
| Hybrid WL=C | **4** | **12.80 / 42.25** | 12.70 | 11.33 | 1.40 | 5.73 | 4.60 / 9.86 | **0.0 / 3** | — | 1.21 / 3.02 |

### 2.3 Per-Partition Lag Profile (`lag_samples.json`, `inference.results`, 12 partitions)

- **`1` replica (`Hybrid A`):** Lag climbs steadily `35 -> 54 -> 78 -> 100 -> 139` over `15s`; all 12 partitions accumulate backlog (`p0..p11` end at `5–29` msgs/partition; similarly `Baseline A` climbs `80 -> 155`, `max_partition_lag = 48`). Note `business-live` (`sensor.readings` serial loop) also hits `max_lag = 139–171` at `1` replica!
- **`2` replicas (`Hybrid A & C`):** Each replica owns `6` partitions. `max_pred_lag <= 3` across the entire 15s run, ending at **`0–1`** messages total (`live_lag <= 32` max, draining to `0` at window end).
- **`4` replicas (`Hybrid A` — current GitOps KEDA count):** `max_pred_lag <= 2`, `end_pred_lag = 0`. Note GitOps KEDA runs `minReplicas: 4, maxReplicas: 4` on local cluster (`3` partitions/pod across `12` partitions), meaning GitOps `business-consumer` already has **~`3.5×` throughput headroom (~`175 RPS`)** for `50 RPS` once distributed across `4` pods — EXCEPT our single-process harness ran only `1` replica!

### 2.4 Projector Substep Breakdown & Out-of-Order Sequence Guard Correctness

Measured inside `Projector.__call__` on `business-prediction` (`Hybrid WL=A, 2 replicas` / `Hybrid WL=C, 2 replicas`):

- `lock_machine_ms`: `0.56–0.65 ms` p50 (`2.38–3.20 ms` p95)
- `guard_source_ms`: `2.18–2.29 ms` p50 (`6.02–6.44 ms` p95)
- `load_3tables_ms` (`3` separate SELECTs): `2.06–2.29 ms` p50 (`6.45–6.66 ms` p95)
- `reduce_ms`: `0.11–0.13 ms` p50 (`0.20–0.21 ms` p95)
- `persist_upsert_ms`: `1.81–2.37 ms` p50 (`6.61–6.99 ms` p95)
- `pg_notify_ms` + live `LISTEN` delivery: `0.51–0.61 ms` DB call (`0.65 ms` p50 / `1.83 ms` p95 end-to-end delivery from commit to SSE listener, **`100%` notifications received: e.g. `2000/2000` in WL=A, `1929/1929` in WL=C**).
- **Out-of-Order Prediction Rate & Sequence Guard:**
  - **Workload A:** `0.0%` out-of-order (`per_key=2` only applies to same-machine concurrency; 1 RPS/machine rarely overlaps).
  - **Workload C (`M-001` at 16 RPS with 10% slow inference):** **`5.15% – 6.27%`** of predictions (`34 – 42` events per `15s` window) arrived out-of-order at `business-prediction`! Sequence Guard preserved monotonically increasing `latest_valid_prediction.sequence_number` on **`100%`** of skips (**`0` `wrong_latest_sequence`** across all 27 runs).

---

## 3. Target Optimization for Phase 4 (`ConsumerRunner`)

Without altering Sequence Guard, `ConsumerRunner`'s single-in-flight contract per pod, or at-least-once durable DB commit guarantees:

1. **Eliminate `wake_delay` (`~5 ms` wasted/record):** Wake the poll/dispatch coordinator immediately when the worker future completes (or use non-blocking `poll(0)` when prefetch/future work finishes instead of sleeping `poll_interval_seconds`).
2. **Safe Opt-In / Bounded Batched Kafka Offset Commit (`commit_batch_size`, default=1 or configurable `16` with `50ms` max delay + flush on idle/revoke/shutdown):** Only commit offsets **already durably committed in PostgreSQL**, coalescing up to `N` durable DB receipts into one synchronous `client.commit(..., asynchronous=False)` (`5.7ms / 16 ≈ 0.36ms` amortized!), while maintaining idempotent `maintai_kafka_inbox` + Sequence Guard safety on crash replay.
   - Expected result: lifts single-replica `ConsumerRunner` capacity from **`~42 RPS -> ~85–110 RPS`**, letting even `1` replica keep up with `50 RPS` and doubling `2`/`4` replica headroom without adding DB locks or removing Sequence Guard!

---

## 4. Phase 4 Before / After Results (Optimization 3)

Implemented in [config.py](file:///home/chienqt/Dev/newBTL/MaintAI/messaging/src/maintai_messaging/config.py) (`KAFKA_COMMIT_BATCH_SIZE`, default `1`, range `1..64`; `KAFKA_COMMIT_MAX_WAIT_MS`, default `25`, range `1..1000`) and [consumer.py](file:///home/chienqt/Dev/newBTL/MaintAI/messaging/src/maintai_messaging/consumer.py):
- **Wake-on-Done (Always On):** `_poll(0)` non-blocking prefetch + `done_event.wait(min(poll_interval_seconds, 0.002))` triggered immediately by `future.add_done_callback(...)`, plus strict overflow check on `poll(0)` even when backpressured so no record can ever be silently dropped.
- **Bounded Batched Commit (Opt-in):** Tracks durable receipts per `(topic, partition)` for the active assignment generation, flushing synchronously via `client.commit(..., asynchronous=False)` when `count >= commit_batch_size`, `age >= commit_max_wait_ms`, queue goes idle (`poll` returns `None`), or runner stops (and clearing uncommitted buffers on rebalance `_revoked`). Default `KAFKA_COMMIT_BATCH_SIZE=1` preserves identical 1-by-1 commit behavior for `storage-consumer` and un-opted consumers.

### 4.1 Isolated Business Consumer Benchmark (Before Opt 3 vs Wake-on-Done Only vs Wake + `batch=16`)

Data files: [biz_wake_batch1.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/phase4-opt3/biz_wake_batch1.json) and [biz_wake_batch16.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/phase4-opt3/biz_wake_batch16.json) (`3` runs per configuration, `target = 50.0 RPS`):

| Variant | `biz_replicas` | `commit_batch` | Outcomes RPS | Pred Commit RPS | `kafka_pub -> biz_commit` p50 / p95 (ms) | Wake Delay p50 / p95 (ms) | Commit Calls (15s) | Pending QLen p50 | `max_pred_lag` | OOO % / `wrong` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before Opt 3 (`Baseline A`) | 1 | 1 | 49.96 | **40.82** | 1,664 / **10,993** | 5.09 / 9.70 | ~612+ | 40.7 | **155** (growing) | 0.0% / 0 |
| Before Opt 3 (`Hybrid A`) | 1 | 1 | 50.22 | **42.58** | 1,572 / **8,761** | 4.98 / 9.77 | ~639+ | 42.8 | **139** (growing) | 0.0% / 0 |
| **`Opt 3` Wake-only (`Hybrid A`)** | 1 | 1 | 50.73 | **52.09** | 240.19 / **574.75** | **0.16 / 0.78** (-31×) | 781.7 | 12.33 | 41 | 0.0% / 0 |
| **`Opt 3` Wake + `batch=16` (`Baseline A`)** | 1 | 16 | 49.76 | **48.40** | **16.24 / 78.70** | **0.15 / 1.06** | **231.7** | **1.0** | **7** (flat) | 0.0% / 0 |
| **`Opt 3` Wake + `batch=16` (`Hybrid A`)** | 1 | 16 | 49.93 | **50.05** | **17.28 / 47.41** | **0.16 / 1.08** | **257.0** | **0.67** | **5** (flat) | 0.0% / 0 |
| **`Opt 3` Wake + `batch=16` (`Hybrid A`)** | **2** | 16 | 50.62 | **50.38** | **11.77 / 28.89** | **0.15 / 1.18** | 448.0 | **0.0** | **4** (flat) | 0.0% / 0 |
| Before Opt 3 (`Hybrid C`) | 1 | 1 | 44.98 | 43.56 | 854 / **1,614** | 4.70 / 9.37 | ~653+ | 37.7 | 89 | 5.6% / 0 |
| **`Opt 3` Wake-only (`Hybrid C`)** | 1 | 1 | 46.73 | 46.09 | 160.24 / **453.52** | **0.17 / 1.06** | 691.0 | 8.33 | 28 | 6.13% / 0 |
| **`Opt 3` Wake + `batch=16` (`Hybrid C`)** | 1 | 16 | 46.20 | 45.89 | **16.64 / 45.18** | **0.18 / 1.15** | **256.7** | **1.0** | **5** (flat) | 5.76% / 0 |
| **`Opt 3` Wake + `batch=16` (`Hybrid C`)** | **2** | 16 | 45.98 | 45.56 | **10.42 / 28.03** | **0.16 / 1.18** | 404.3 | **0.0** | **5** (flat) | 6.09% / 0 |

- Even at `1` replica on full `50 RPS` Workload A, **Wake + `batch=16`** cuts `kafka_pub -> biz_commit` p95 from **`8,761 ms` -> `47.41 ms` (`-99.5%`)**, shrinks `max_pred_lag` from `139` (unbounded growth) to `5` (flat), and reduces sync broker commits by **~`67%` (`781.7 -> 257.0`)** in `15s`. With `2` replicas, p95 settles at **`28.0–28.9 ms` (`max_lag <= 5`)**.

### 4.2 Full Controlled 50 RPS Pipeline Verification (`Opt 1 + Opt 3` Combined)

Mean across `3` runs on `controlled_benchmark.py` (`commit_batch_size=16, commit_max_wait_ms=25`), raw CSVs in [controlled_batch16](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/phase4-opt3/controlled_batch16/summary.csv) and [controlled_pk4](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/phase4-opt3/controlled_pk4/summary.csv):

| Run Series | Workload / `per_key` | Admitted RPS | Outcomes RPS | Preds RPS | Hot e2e p50 / p95 (ms) | Bg e2e p50 / p95 (ms) | DB Tx p95 (ms) | Violations |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline-50rps` | A / `pk=1` | 50.00 | 49.98 | 45.18 | 1,561 / 2,066 | 1,626 / 3,514 | 40.28 | 0 |
| `candidate-50rps` (initial) | A / `pk=2` | 46.02 | 45.87 | 34.76 | 3,357 / 4,349 | 3,670 / 15,001 | 59.77 | 0 |
| **`Opt 1 + Opt 3`** | **A / `pk=2`** | **50.87** | **51.05** | **51.58** | **290 / 634** (-85% vs init, **-69% vs base**) | **601 / 1,183** (-92% vs init, **-66% vs base**) | **43.37** | **0** |
| `baseline-50rps` | C / `pk=1` | 35.53 | 35.53 | 35.38 | 21,756 / 29,301 | 489 / 17,723 | 38.89 | 0 |
| `candidate-50rps` (initial) | C / `pk=2` | 45.38 | 43.58 | 35.73 | 7,411 / 8,311 | 3,564 / 12,090 | 65.35 | 0 |
| `Opt 1 + Opt 3` | C / `pk=2` | 47.78 | 46.69 | 46.91 | 3,931 / 4,844 | **562 / 1,165** (-90%) | 46.65 | 0 |
| **`Opt 1 + Opt 3`** | **C / `pk=4`** (`inflight=16`) | **50.16** | **50.47** | **50.98** | **579 / 1,114** (**-96.2% vs base**, -86.6% vs init) | **532 / 988** (**-94.4% vs base**) | **44.22** | **0** |

- Why `per_key=2` capped at `46.7 RPS` on Workload C while `per_key=4` achieved `50.47 RPS`: in `ControlledMLProvider`, deterministic `(seq % 5 == 1)` (`20%`) plus `10%` random yields **~`28%` `300 ms` responses on `M-001`** (`mean ML = 102 ms` + `TxC ~35 ms` = `137 ms`), giving `per_key=2` a Little's Law ceiling of `2 / 0.137 = 14.6 RPS < 16 RPS`. With `per_key=4` (`4 / 0.137 = 29.2 RPS` headroom), Workload C sustains full **`50.47 RPS`** with `hot_e2e p95 = 1.11 s` (down from **`29.3 s`** in Baseline!) and **`0` correctness violations**.
