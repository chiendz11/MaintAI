# Official Baseline 50 RPS Report: Hybrid Ordering (`MaintAI`)

- **Ngày thực hiện:** 2026-11-23
- **Nhánh:** `feat/hybrid-ordering` (chạy trên mã nguồn Processing/Business gốc **trước** khi triển khai Hybrid Ordering)
- **Bộ benchmark cũ (lịch sử, giữ nguyên không xóa):** [../baseline/REPORT.md](../baseline/REPORT.md) (50/100/65 RPS)

> [!IMPORTANT]
> **Phân biệt phạm vi benchmark:** Kết quả dưới đây sử dụng **`ControlledMLProvider`** (giả lập ML latency) kết hợp với **PostgreSQL 17 và Kafka 4.0.0 thật** trên Docker Compose đơn-tiến-trình (`processing/worker/tests/compose.yaml`), **không phải** cụm đa-pod k3d hay model ML/GPU thực tế. Bộ số liệu này đo hành vi scheduler, transaction DB, Outbox relay và ordering invariants trong môi trường kiểm soát.

---

## 1. Cấu hình Benchmark & Định nghĩa Official 50 RPS Workloads

Thông số trích từ [benchmark_config.json](./benchmark_config.json) và [controlled_benchmark.py](../../../../scripts/benchmarks/hybrid_ordering/controlled_benchmark.py):

- **Scheduler / Worker (Baseline):** `mode=baseline`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_MAX_INFLIGHT_PER_KEY=1`, `PROCESSING_MAX_TOTAL_INFLIGHT=8`, `max_pending_records=512`, `max_pending_per_key=64`, `max_uncommitted_per_partition=256`.
- **Database Pools:** `proc_pool_size=12`, `biz_pool_size=12` (PostgreSQL 17 mặc định `synchronous_commit=on`).
- **ML Simulator (`ControlledMLProvider`):** `ml_max_concurrency=12`, `ml_latency_ms=25.0 ± 5.0 ms` jitter, `ml_slow_prob=0.10` (`300.0 ms`), cộng tail cố định cho `M-001` mỗi 5 readings (`seq % 5 == 1`), deterministic RNG seed `1000 + run_idx`.
- **Chu kỳ chạy mỗi Run:** `5s` warm-up + `15s` measurement × `3` runs mỗi workload, drain tối đa `90s` sau measurement window (`total_sent_with_warmup = 1000`, `window_sent = 750`).
- **Official Workload A (Normal Load — `50.0 RPS`):** `50` machines (`M-001..M-050`), mỗi machine `1.0 reading/s`.
- **Official Workload C (Hot Machine — `50.0 RPS`):** `M-001` gửi `16.0 readings/s` + `34` background machines (`M-002..M-035`), mỗi machine `1.0 reading/s`.

---

## 2. Kết quả Benchmark Chính thức (`baseline-50rps`)

Số liệu đầy đủ cả 6 runs trích từ [summary.csv](./summary.csv) (`errors=0`, `correctness_violations=0`, `pg_locks=0`, `cpu_throttled=0` trên tất cả 6 runs):

| Workload | Run | Target RPS | Admitted RPS | Outcomes RPS | Preds RPS (Biz) | Inf p50 / p95 / p99 (ms) | Hot `M-001` E2E p50 / p95 / p99 (ms) | BG E2E p50 / p95 / p99 (ms) | DB Tx p95 (ms) | Outbox p95 (ms) | Alert p95 (ms) | ML Peak | Max Pending | Max Outbox | PG Conns | CPU (%) | RSS (MB) | Violations |
|---|---:|---:|---:|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A | 1 | 50.0 | 50.00 | 50.00 | 44.47 | 26.40 / 301.89 / 304.49 | 1936.00 / 2224.01 / 2248.88 | 1784.32 / 5634.50 / 8499.44 | 41.13 | 92.45 | 33.56 | 8 | 50 | 10 | 15 | 100.20 | 131.80 | 0 |
| A | 2 | 50.0 | 50.00 | 49.93 | 45.13 | 26.15 / 300.92 / 304.60 | 1231.56 / 2043.14 / 2083.38 | 1476.13 / 2405.47 / 5216.00 | 40.49 | 127.49 | 34.62 | 8 | 44 | 15 | 14 | 97.50 | 134.90 | 0 |
| A | 3 | 50.0 | 50.00 | 50.00 | 45.93 | 26.11 / 301.32 / 304.44 | 1515.32 / 1931.26 / 1982.39 | 1618.10 / 2501.06 / 3144.07 | 39.22 | 139.83 | 30.89 | 8 | 48 | 13 | 14 | 98.50 | 137.21 | 0 |
| C | 1 | 50.0 | 35.67 | 35.67 | 35.67 | 26.61 / 302.75 / 304.80 | 20939.85 / 29077.13 / 29751.88 | 452.50 / 16356.47 / 20830.28 | 36.21 | 161.58 | 41.15 | 8 | 102 | 13 | 14 | 80.96 | 139.55 | 0 |
| C | 2 | 50.0 | 35.60 | 35.60 | 35.07 | 26.57 / 302.64 / 304.90 | 21843.41 / 28108.40 / 28550.94 | 546.57 / 18102.00 / 21301.06 | 42.44 | 164.42 | 46.13 | 8 | 112 | 11 | 16 | 92.77 | 141.54 | 0 |
| C | 3 | 50.0 | 35.33 | 35.33 | 35.40 | 26.49 / 302.45 / 304.55 | 22484.58 / 30716.19 / 31192.80 | 469.18 / 18709.82 / 22433.30 | 38.01 | 162.54 | 42.27 | 8 | 110 | 14 | 15 | 82.24 | 143.54 | 0 |

Bổ sung chặng trung gian từ Run 1 ([workload_A_run1.json](./workload_A_run1.json), [workload_C_run1.json](./workload_C_run1.json)):
- **Workload A (Run 1):** `inference_to_db_commit p95 = 64.683 ms`, `kafka_publish_to_business_db p50 = 1304.613 ms / p95 = 5155.574 ms`.
- **Workload C (Run 1):** `inference_to_db_commit p95 = 57.583 ms`, `kafka_publish_to_business_db p50 = 54.791 ms / p95 = 161.704 ms`.

---

## 3. Phân tích Chính từ Baseline 50 RPS

1. **Workload A (50×1 RPS):** Đạt đúng **`50.0 admitted/outcomes RPS`**, không tích queue (`max_pending = 44–50`), `hot_e2e_p95 = 1.93 – 2.22 giây`, `bg_e2e_p95 = 2.41 – 5.63 giây`.
2. **Workload C (`M-001` 16 RPS + `34×1 RPS` = 50 RPS):**
   - Do `per_key = 1`, `M-001` bị nghẽn Head-of-Line mỗi khi gặp reading `300 ms` (`hot_e2e_p95 = 28.11 – 30.72 giây`, `p99` tới `31.19 giây`).
   - Hàng đợi riêng của `M-001` vượt ngưỡng `max_pending_per_key = 64` và kích hoạt backpressure pause trên Kafka partition chứa `M-001`, làm chậm luôn các background machines chung partition (`bg_e2e_p95` vọt lên **`16.36 – 18.71 giây`**) và kéo tổng thông lượng toàn worker tụt xuống còn **`35.33 – 35.67 RPS`** dù mục tiêu chỉ là `50 RPS`.
   - Sau measurement window, cả `1000/1000` readings (kể cả warm-up) đều drain sạch trong `90s` với **`0` correctness violations**.
