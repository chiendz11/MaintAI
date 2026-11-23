# Phase 2 Report: TxA & TxC Database Bottleneck Investigation

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering`
- **Database Engine:** PostgreSQL `17.12` (`maintai-m2-postgres-1`, `synchronous_commit=on`, `wal_sync_method=fdatasync`, `shared_buffers=128MB`, `max_connections=100`, `track_wal_io_timing=off`)
- **Kafka Broker:** Apache Kafka `4.0.0` (12 partitions per topic)
- **Workload Harness:** [db_bottleneck_benchmark.py](file:///home/chienqt/Dev/newBTL/MaintAI/scripts/benchmarks/hybrid_ordering/db_bottleneck_benchmark.py) (`ControlledMLProvider`: `base=25ms`, `jitter=5ms`, `slow_prob=0.10`, `slow_ms=300ms`, `ml_max_concurrency=12`; `5s` warmup + `15s` measurement × `3` runs at `50 RPS`)
- **Raw Data:** [results.json](file:///home/chienqt/Dev/newBTL/MaintAI/docs/benchmarks/hybrid-ordering/database/results.json)

---

## 1. Executive Summary of Root Causes

We reproduced and instrumented the exact cause of Hybrid Ordering's **Workload A (`50×1`) throughput regression** (`50.00 -> 41.35 RPS` mean outcomes) and why **Workload C (`M-001×16 + 34×1`) capped at `46.33 RPS`** (< `50 RPS` target):

1. **Root Cause #1 (~70–85% of worker slowdown): Poll-thread serialization + excessive synchronous SQL roundtrips (`12 SQL` in `TxA_admit` + `31.5 SQL` in `TxC_persist` + `1 SQL` redundant `StageB_prepare`).**
   - In [KeyedConsumerRunner.run](file:///home/chienqt/Dev/newBTL/MaintAI/messaging/src/maintai_messaging/keyed.py#L383-L400), every single reading is polled **1 record at a time** and synchronously runs `TxA_admit` (`12` separate SQL roundtrips) directly on the single Kafka poll thread before `_collect`, `_commit`, or `_dispatch` can advance.
   - `TxA_admit` stalled the poll thread for **`46.6% – 69.3%` (mean `61.6%`)** of total wall time on Workload A (`p50 = 14.31ms`, `p95 = 31.55ms` per record!). At 50 RPS (`20ms` inter-arrival budget), any burst above ~`15ms` per `TxA_admit` pushes poll-cycle time past `20ms` and drops admission throughput straight to **`35.9–38.1 RPS`** (Workload A runs 2–3).
2. **Root Cause #2 (CPU & buffer-hit amplification): Missing index on `sensor_events(machine_id, sequence_number)` + redundant checks in `TxA`, `StageB_prepare`, and `TxC/drain_frontier`.**
   - PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)` proves that `SELECT event_id FROM sensor_events WHERE machine_id=%s AND sequence_number=%s AND event_id <> %s LIMIT 1` performs a **full `Seq Scan on sensor_events`** twice per reading (`TxA` and `TxC`), scanning `92` shared buffers / ~`1,000` rows even at 15s of load.
   - Consequently, `proc_db.blks_hit` surged from `107,012` (Baseline C, ~`198` hits/outcome) to **`491,190`** (Hybrid C, ~`708` hits/outcome, **3.6×** buffer work) and pushed PG CPU from `36.7–39.2%` (Baseline A) to **`43.2–51.5%`** (Hybrid A).
   - `TxC_persist` runs **`30.4–31.5 SQL` statements per reading** (`p50 = 32.7–43.8ms`, `p95 = 67.3–82.7ms`), vs `Baseline_tx` averaging `12.7` SQL per tx (`~25.4` across its pre-prepare + persist txs). Inside `TxC`, `drain_frontier` alone consumes `8.98–13.44ms` p50 (`24.0–30.6ms` p95) due to duplicate `require_ordering()`, duplicate `processing_admission_cursor FOR UPDATE`, and per-row cursor/ledger roundtrips.
3. **Root Cause #3 (`synchronous_commit` WAL fsync is Secondary for Poll Stall, NOT Primary):**
   - Running Hybrid Workload A with non-default session `synchronous_commit=off` cut `wal_sync/s` by **`98.3%` (`355.1 -> 5.87 /s`)** and PG block write IOPS by **`95.5%` (`10,493 -> 474`)**, restoring admitted rate to `49.80 RPS` by shaving ~`0.5ms/tx` of commit wait (`TxA cmt_p50: 0.91 -> 0.39ms`).
   - **However, `txa_stall` STILL remained `56.16%` (`8,424ms / 15s`, `TxA p50 = 10.13ms`, `exec_p50 = 8.42ms`) and `TxC p50` remained `31.44ms` (`exec_p50 = 25.95ms`)!** Thus disabling durability does NOT eliminate the architectural bottleneck (12 + 31.5 serial SQL roundtrips); reducing SQL roundtrips and batching poll-thread admission fixes the root cause without sacrificing WAL durability.
4. **Cross-Layer Finding (`business-prediction` Bottleneck):**
   - Even in Baseline Workload A (`50.07 outcomes/s`, `0%` poll stall), single-replica `business-prediction` (`preds_rps`) saturated at **`43.13 – 46.60 RPS` (mean `44.38`)**. Connection pool wait was near zero across all pools (`p95 <= 0.036ms`), isolating the prediction drain deficit directly to single-replica `ConsumerRunner` throughput (investigated in Phase 3).

---

## 2. Quantitative Comparison (Baseline vs Hybrid vs Opt-In `synchronous_commit=off`)

### 2.1 Throughput, End-to-End p95, Poll Stall & PG Internals (3 Runs Each, Except Opt-In `1` Run)

| Scenario | Admitted RPS | Outcomes RPS | Preds RPS | Hot p95 (ms) | Bg p95 (ms) | TxA Poll Stall % | PG CPU % | `wal_sync/s` | `wal_sync/outcome` | Commits/Outcome | `proc_db.blks_hit` | PG IO Write IOPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Baseline WL=A (`on`)** | **50.00** (all 50.0) | **50.07** | 44.38 | 1715–3172 | 1964–7623 | 0.00% | 38.10% | 382.8 | 7.65 | 8.90 | 329,396 | 11,468 |
| **Hybrid WL=A (`on`)** | **41.35** `[50.0, 35.9, 38.1]` | **41.20** | 36.84 | 849–8127 | 1455–13614 | **61.59%** `[46.6–69.3]` | **47.52%** | 271.7 | 6.54 | 7.69 | 451,140 | 8,233 |
| **Baseline WL=C (`on`)** | **36.07** | **36.07** | 35.78 | 25217–27880 | 14214–17320 | 0.00% | 32.83% | 312.6 | 8.67 | 9.88 | 107,012 | 9,461 |
| **Hybrid WL=C (`on`)** | **47.04** `[46.6–47.6]` | **46.33** | 45.93 | **5319–5638** (-80%) | **1840–3198** (-85%) | **53.28%** `[50.2–57.6]` | 41.52% | 321.6 | 6.94 | 8.07 | 491,190 | 9,752 |
| **Hybrid WL=A (`off`, opt-in)** | **49.80** | **49.87** | 41.87 | 2482 | 6767 | **56.16%** | 42.77% | **5.87** | 0.12 | 7.80 | 402,772 | **474** |

### 2.2 Per-Phase Transaction Timings & SQL Statement Counts (Mean Across Runs)

| Phase | Mode / WL | Stmts / Tx | Pool Wait p95 (ms) | SQL Exec p50 (ms) | Commit p50 (ms) | Total Tx p50 (ms) | Total Tx p95 (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `TxA_admit` (Poll Thread) | Hybrid WL=A (`on`) | **12.00** | 0.026 | 11.59 | 0.91 | **14.31** | **31.55** |
| `TxA_admit` (Poll Thread) | Hybrid WL=C (`on`) | **12.00** | 0.015 | 7.69 | 0.57 | **9.49** | **25.94** |
| `TxA_admit` (Poll Thread) | Hybrid WL=A (`off`) | **12.00** | 0.014 | **8.42** | 0.39 | **10.13** | **23.23** |
| `StageB_prepare` (Worker) | Baseline A / Hybrid A | **1.00** | 0.011 / 0.027 | 1.51 / 2.54 | 0.00 | 1.52 / 2.56 | 5.79 / 6.85 |
| `Baseline_durable_replay` | Baseline WL=A | 5.00 | 0.022 | 3.95 | 0.83 | 6.17 | 18.38 |
| `Baseline_tx` (pre + persist) | Baseline WL=A | 12.74 (×2/reading) | 0.020 | 8.90 | 0.83 | 13.02 | 58.45 |
| `TxC_persist` (Worker) | Hybrid WL=A (`on`) | **31.49** | 0.031 | **36.13** | 1.09 | **43.80** | **82.66** |
| `TxC_persist` (Worker) | Hybrid WL=C (`on`) | **30.36** | 0.016 | **27.12** | 0.77 | **32.73** | **67.34** |
| `TxC_persist` (Worker) | Hybrid WL=A (`off`) | **31.48** | 0.016 | **25.95** | 0.59 | **31.44** | **68.52** |
| `Relay_claim` | Baseline A / Hybrid A | 4.32 / 4.54 | 0.022 / 0.020 | 1.66 / 3.71 | 0.44 / 0.77 | 2.59 / 5.58 | 12.38 / 15.08 |
| `Relay_finish` | Baseline A / Hybrid A | 5.00 / 5.00 | 0.023 / 0.030 | 2.02 / 4.15 | 0.48 / 0.79 | 3.13 / 6.27 | 11.78 / 15.17 |
| `Biz_live` | Baseline A / Hybrid A | 9.88 / 9.95 | 0.023 / 0.029 | 5.19 / 10.43 | 0.51 / 1.00 | 6.92 / 13.52 | 25.88 / 29.54 |
| `Biz_pred` | Baseline A / Hybrid A | 10.77 / 10.21 | 0.023 / 0.031 | 5.34 / 10.37 | 0.50 / 0.95 | 7.11 / 13.28 | 27.64 / 28.86 |
| `Biz_alert` | Baseline A / Hybrid A | 9.90 / 10.23 | 0.020 / 0.025 | 7.69 / 13.31 | 0.82 / 1.18 | 13.91 / 22.94 | 34.49 / 47.77 |

### 2.3 Hybrid Substep Breakdown (Worker & Resequencer)

| Substep | Hybrid WL=A (`on`) p50 / p95 | Hybrid WL=C (`on`) p50 / p95 | Hybrid WL=A (`off`) p50 / p95 |
| --- | ---: | ---: | ---: |
| `resequencer.admit` (inside `TxA`) | 8.28 / 19.80 ms | 5.63 / 17.11 ms | 6.23 / 14.52 ms |
| `persist_hybrid` total function (inside `TxC`) | 34.03 / 66.09 ms | 24.70 / 52.50 ms | 24.38 / 54.49 ms |
| `resequencer.record_disposition` (inside `TxC`) | 1.07 / 6.57 ms | 0.85 / 4.91 ms | 0.81 / 4.49 ms |
| `resequencer.drain_frontier` (inside `TxC`) | **13.44 / 30.62 ms** | **8.98 / 23.97 ms** | **9.44 / 25.33 ms** |

### 2.4 Lock Contention & Wait Events

- `ConnectionPool(size=12)` wait time was negligible across all runs (`p95 <= 0.036ms`).
- `pg_locks` (`NOT granted`) recorded **0** lock wait samples in `100ms` polling intervals across all runs; `pg_stat_activity` showed only `3–4` transient `Lock:transactionid` samples and `1–5` `IO:WalSync` samples over `15s` windows.
- Thus, the slowdown is **not** coarse lock blocking on `processing_admission_cursor`; it is **serial poll-thread roundtrip latency + SQL count (`12 + 1 + 31.5 = 44.5` statements/reading) + missing index scans**.

---

## 3. `EXPLAIN (ANALYZE, BUFFERS)` Findings & Index Gaps

Captured on populated `processing_db` (~1,000 events after Hybrid Workload C):

1. **Missing Index — Full `Seq Scan` on `sensor_events` twice per reading:**
   - Query (`admit` & `persist_hybrid`): `SELECT event_id FROM sensor_events WHERE machine_id=$1 AND sequence_number=$2 AND event_id <> $3 LIMIT 1`
   - Actual Plan: `Seq Scan on sensor_events (actual time=0.266..0.266 rows=1 loops=1)`, `Rows Removed by Filter: 999`, **`Buffers: shared hit=92`**.
   - Existing indexes on `sensor_events`: only `sensor_events_pkey (event_id)` and `sensor_events_machine_source_time (machine_id, source_server_accepted_at DESC, event_id DESC)`. **There is no index covering `(machine_id, sequence_number)`.** Every reading pays `O(N)` table scan twice (`184` buffer reads per reading at `1,000` rows, scaling linearly with history size).
2. **Other Hot Queries:**
   - `txa_inbox_lookup` (`maintai_kafka_inbox_pkey`): `Index Scan`, `0.025ms`, 3 buffers.
   - `txa_ledger_lookup_by_msgid` (`processing_alert_ledger_consumer_group_topic_message_id_key`): `Index Scan`, `0.031ms`, 3 buffers.
   - `txc_drain_frontier_scan` (`admission_seq > frontier ORDER BY admission_seq`): Uses `Seq Scan + Sort` when `frontier=0` is passed for an empty-to-full scan, whereas incremental `admission_seq > frontier` at head returns 1–2 unresolved rows; `processing_alert_ledger` already has `pkey (consumer_group, machine_id, admission_seq)` and partial index `processing_alert_ledger_unresolved`.
   - `relay_claim_lateral`: Uses `Index Scan using maintai_outbox_dispatch_order` (`0.060ms`), 1 buffer on keys table.

---

## 4. Non-Default Durability Trade-off Analysis (`synchronous_commit`)

| Setting | Default? | Crash Loss Risk vs Kafka Offset | Observed `TxA` Stall | Observed Outcomes RPS (WL=A) | Recommendation |
| --- | --- | --- | --- | --- | --- |
| `synchronous_commit=on` | **YES (Default)** | Zero acknowledged DB data loss after commit; safe-prefix offset matches durable WAL | 46.6%–69.3% | 41.20 (unoptimized) | **Keep `on` as strict default** |
| `synchronous_commit=off` (session opt-in) | **NO** | Recent committed tx (~200ms–600ms WAL flush window) can vanish on OS crash even if committed Kafka offset advanced! Could silently lose `processing_alert_ledger` or `processing_results` records after offset commit! | **56.16% (still present)** | 49.87 | **DO NOT use as default**; unsafe against Kafka offset commit on DB crash, and only masks `<0.6ms` commit while leaving 56% SQL stall untouched |

---

## 5. Ranked Optimization Plan Derived from Data (for Phase 4)

1. **Optimization 1 (Highest ROI / Zero Durability Trade-off) — Add `sensor_events(machine_id, sequence_number)` index & prune redundant SQL in `TxA`, `StageB`, `TxC`:**
   - Eliminate duplicate `require_ordering()` calls (saves 2 SQL in `TxA` and 2 SQL in `TxC/drain_frontier`).
   - Skip `StageB_prepare` DB checkout + query when `ticket.replayed is False` and `ticket.legacy_terminal is False` (`TxA` already verified inbox/ledger/sensor_events).
   - Add covering index on `sensor_events(machine_id, sequence_number)` so sequence uniqueness checks become `O(log N)` index lookups instead of full `Seq Scan`s, and skip redundant sequence check in `TxC` when guarded by `TxA` or unique/advisory check + index.
   - Pass locked `processing_admission_cursor` state into `drain_frontier` (avoiding duplicate `FOR UPDATE` select) and skip redundant `INSERT ... ON CONFLICT` on `maintai_processing_machine_cursor` / `processing_source_cursor` when already initialized (`existing row -> direct SELECT ... FOR UPDATE`).
2. **Optimization 2 (Eliminate Serial Poll-Thread Barrier) — Bounded `admit_batch()` on Poll Thread:**
   - Poll up to a bounded micro-batch (`e.g., max 16` ready records from librdkafka via `poll`/non-blocking drain) and execute durable `admit_batch()` inside **ONE fenced PG transaction** (with deterministic machine ordering and safe fallback to per-record admission on any conflict/error). Reduces `TxA` transactions and poll-thread stalls by **5–16×** at 50 RPS while keeping `synchronous_commit=on` and 100% durable admission before dispatch!
