# Official Candidate 50 RPS Report: Hybrid Ordering (`MaintAI`)

- **Ngày thực hiện:** 2026-11-23
- **Nhánh:** `feat/hybrid-ordering` (`per_key=2`, `max_total_inflight=8`, `hybrid_ordering_enabled=true`)
- **Đối chiếu Baseline 50 RPS:** [../baseline-50rps/REPORT.md](../baseline-50rps/REPORT.md) · **So sánh tổng hợp:** [../COMPARISON_50RPS.md](../COMPARISON_50RPS.md)

> [!IMPORTANT]
> **Phân biệt phạm vi benchmark:** Giống hệt `baseline-50rps`, bộ benchmark này chạy trên **Controlled PostgreSQL 17 + Kafka 4.0.0 + `ControlledMLProvider`** đơn-worker trên Compose (`synchronous_commit=on`), **không phải** cụm k3d đa-pod hay model ML/GPU thật. Toàn bộ biến khác (`ml_max_concurrency=12`, latency/jitter/slow prob, `proc_pool_size=12`, `biz_pool_size=12`, RNG seed `1000+run_idx`, warm-up `5s` + measure `15s` × `3` runs) được giữ nguyên 100% so với `baseline-50rps`.

---

## 1. Cấu hình Candidate A (`per_key=2, total=8`)

Thông số chạy trong [controlled_benchmark.py](../../../../scripts/benchmarks/hybrid_ordering/controlled_benchmark.py) và [benchmark_config.json](./benchmark_config.json):

- **Hybrid Scheduler / Worker:** `mode=hybrid`, `PROCESSING_HYBRID_ORDERING_ENABLED=true`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_MAX_INFLIGHT_PER_KEY=2`, `PROCESSING_MAX_TOTAL_INFLIGHT=8`, `PROCESSING_UNAVAILABLE_ALERT_DISPOSITION=BLOCKED`, `max_pending_records=512`, `max_pending_per_key=64`, `max_uncommitted_per_partition=256`.
- **Workloads (`50.0 RPS`):**
  - **Official Workload A:** `50` machines × `1.0 reading/s` = `50.0 RPS`.
  - **Official Workload C:** `M-001` (`16.0 readings/s`) + `34` background machines × `1.0 reading/s` = `50.0 RPS`.

---

## 2. Kết quả Benchmark (`candidate-50rps`)

Số liệu đầy đủ cả 6 runs trích nguyên văn từ [summary.csv](./summary.csv) (`errors=0`, `correctness_violations=0`, `cpu_throttled=0`, `unresolved_alert_ledger=0` sau drain `90s` trên toàn bộ `1000/1000` readings):

| Workload | Run | Target RPS | Admitted RPS | Outcomes RPS | Preds RPS (Biz) | Inf p50 / p95 / p99 (ms) | Hot `M-001` E2E p50 / p95 / p99 (ms) | BG E2E p50 / p95 / p99 (ms) | DB Tx p95 (ms) | Outbox p95 (ms) | Alert p95 (ms) | ML Peak | Max Pending | Max Outbox | PG Conns | PG Locks | CPU (%) | RSS (MB) | Violations |
|---|---:|---:|---:|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A | 1 | 50.0 | 47.80 | 47.40 | 36.00 | 26.60 / 302.27 / 304.63 | 2513.67 / 3375.09 / 3460.89 | 2743.69 / 13934.89 / 16790.65 | 60.65 | 117.71 | 42.83 | 8 | 25 | 9 | 15 | 1 | 123.95 | 132.44 | 0 |
| A | 2 | 50.0 | 44.87 | 44.80 | 33.00 | 26.16 / 301.11 / 304.90 | 3598.47 / 4833.15 / 5150.45 | 3975.32 / 16230.63 / 19538.31 | 63.90 | 90.26 | 40.11 | 8 | 20 | 7 | 14 | 0 | 125.94 | 136.12 | 0 |
| A | 3 | 50.0 | 45.40 | 45.40 | 35.27 | 26.05 / 301.63 / 304.74 | 3958.76 / 4839.62 / 5244.02 | 4290.32 / 14837.34 / 18068.97 | 54.76 | 160.39 | 33.50 | 8 | 12 | 9 | 14 | 0 | 121.89 | 139.06 | 0 |
| C | 1 | 50.0 | 46.73 | 45.93 | 37.40 | 26.57 / 303.15 / 304.92 | 6826.16 / 7637.17 / 7777.35 | 2824.90 / 12556.97 / 15720.66 | 58.54 | 139.10 | 35.71 | 8 | 88 | 9 | 14 | 2 | 119.56 | 141.93 | 0 |
| C | 2 | 50.0 | 46.80 | 45.33 | 36.73 | 26.42 / 302.87 / 304.92 | 6428.27 / 7394.41 / 7569.09 | 2826.28 / 10728.84 / 14265.03 | 67.68 | 182.31 | 40.05 | 8 | 92 | 14 | 14 | 1 | 119.66 | 143.98 | 0 |
| C | 3 | 50.0 | 42.60 | 39.47 | 33.07 | 26.72 / 302.20 / 304.93 | 8979.71 / 9901.37 / 10290.50 | 5041.23 / 12982.70 / 16931.50 | 69.84 | 291.15 | 45.23 | 8 | 86 | 11 | 15 | 1 | 125.48 | 146.23 | 0 |

Chi tiết trung gian từ Run 1 ([workload_A_run1.json](./workload_A_run1.json), [workload_C_run1.json](./workload_C_run1.json)):
- **Workload A (Run 1):** `max_per_key_inflight = 1`, `max_alert_reorder_depth = 20`, `alert_frontier_wait p50/p95/p99 = 8.323 / 20.581 / 33.537 ms`, `inference_to_db_commit p95 = 76.331 ms`, `kafka_publish_to_business_db p50/p95 = 1825.699 / 13038.195 ms`, `frontier_stall_s = 0.0`.
- **Workload C (Run 1):** `max_per_key_inflight = 2`, `max_alert_reorder_depth = 70`, `alert_frontier_wait p50/p95/p99 = 11.642 / 197.846 / 264.221 ms`, `inference_to_db_commit p95 = 75.059 ms`, `kafka_publish_to_business_db p50/p95 = 1624.178 / 10497.512 ms`, `frontier_stall_s = 0.0`.

---

## 3. Nhận xét Nhanh Candidate 50 RPS

1. **Correctness 100% đạt chuẩn:** Toàn bộ `6/6` runs đều có `0` missing outcomes, `0` duplicate outcomes/alerts, `0` stale projection updates, `0` alert sequence violations trên Kafka broker, `0` unsafe offset commits, và `0` bản ghi kẹt trong `processing_alert_ledger` sau drain.
2. **Workload C (`Hot Machine M-001`) cải thiện vượt bậc dù giữ tổng concurrency = 8:** Cho phép `M-001` chạy `max_inflight_per_key = 2` giúp giảm `hot_e2e_p95` từ **`28.11–30.72s`** (Baseline) xuống **`7.39–9.90s`** (**giảm `-68%` đến `-74%`**), đồng thời tăng `admitted_rps` lên `42.60–46.80`.
3. **Workload A (`Normal 50×1 RPS`) xuất hiện suy giảm throughput/latency trên harness đơn-PG:** Do mỗi reading cần thêm Stage A `TxA` (poll thread) + Stage C `TxC` trên cùng một instance Compose PostgreSQL 17 `synchronous_commit=on`, tạo áp lực WAL fsync làm tốc độ tiêu thụ của serial `business-prediction` consumer giảm xuống `33.00–36.00 preds/s` và giữ `admitted_rps` ở `44.87–47.80 rps`. Phân tích đối chiếu đầy đủ nằm trong [../COMPARISON_50RPS.md](../COMPARISON_50RPS.md).
