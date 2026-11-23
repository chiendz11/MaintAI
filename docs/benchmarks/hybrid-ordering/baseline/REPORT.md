# Phase 0 Baseline Report: Hybrid Ordering Architecture (`MaintAI`)

- **Ngày thực hiện:** 2026-11-23
- **Mục tiêu Phase 0:** Chụp trạng thái baseline, giải quyết 5 điểm thiết kế bắt buộc trong [HYBRID_ORDERING_ADR.md](../../../architecture/HYBRID_ORDERING_ADR.md), xác nhận toàn bộ baseline tests pass trên mã nguồn gốc chưa sửa, và đo benchmark controlled PostgreSQL 17 + Kafka 4.0.0 làm chuẩn so sánh trước/sau.

---

## 1. Environment Snapshot & Branch Isolation

| Thành phần / Repo | Nhánh hiện tại | Baseline Commit SHA | Ghi chú trạng thái |
|---|---|---|---|
| `MaintAI` | `feat/hybrid-ordering` | `57169209bd959156e1c923c79b672b4d98a871ce` | Chứa ADR, benchmark harness và Phase 0 artifacts; giữ nguyên thay đổi local chưa commit (`sensor/src/simulator/ordering/store.py`, `.kube/`) |
| `MaintAI-gitops` | `feat/hybrid-ordering` | `09a382b03b87900b2859c8d0de56cb6a97dfafac` | Giữ nguyên `local/platform/postgres.yaml`; chưa áp dụng thay đổi core ở Phase 0 |
| `Maint-infrastructure` | `main` | `047c231837fc7c6500c308a6f18f2dec594402b3` | Chưa chỉnh sửa (`Phase 0–6` giữ nguyên trừ khi lock check yêu cầu) |

- **Snapshot Kubernetes Kustomize local (`MaintAI-gitops/environments/local`):** [rendered-workloads-summary.yaml](./rendered-workloads-summary.yaml) và [environment.json](./environment.json) ghi nhận `15` Deployments và `22` ConfigMaps (đã lọc bỏ secrets).
  - Trong manifest local baseline (`processing-worker-config-bmhc6ht2ft`), `PROCESSING_EXECUTION_MODE=keyed`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_MAX_UNCOMMITTED_PER_PARTITION=256`.
  - Lưu ý cụm `k3d-maintai-local` đang ở trạng thái dừng (`Exited (137)`) trước khi bắt đầu Phase 0; toàn bộ kiểm thử tích hợp và benchmark controlled trong Phase 0 chạy trực tiếp trên **Docker Compose PostgreSQL 17 (`127.0.0.1:35432`) + Kafka 4.0.0 (`127.0.0.1:39092`)** (`processing/worker/tests/compose.yaml`, không phải đa pod k3d).

---

## 2. Phase 0 Baseline Integration & Unit Tests

Toàn bộ bộ kiểm thử baseline chạy trên mã nguồn gốc của `MaintAI@57169209bd959156e1c923c79b672b4d98a871ce` (trước mọi thay đổi core) với PostgreSQL 17 + Kafka 4.0.0 thật (`MAINTAI_KAFKA_INTEGRATION=1 MAINTAI_POSTGRES_INTEGRATION=1`):

| Suite | Phạm vi kiểm thử | Kết quả | Thời gian |
|---|---|---|---:|
| `messaging` | `messaging/tests/unit`, `messaging/tests/integration/test_processing_ordering.py`, `messaging/tests/integration/test_durable_delivery.py` | **214 passed, 0 failed, 0 skipped** | 26.46s |
| `processing` | `processing/worker/tests/{test_keyed_application,test_worker_transaction,test_migrations,test_permissions,test_config}.py` và `processing/outbox-relay/tests/{test_config,test_cli_modes,test_relay_recovery}.py` | **101 passed, 0 failed, 0 skipped** | 13.04s |
| `backend` | `backend/business-consumer/tests/{test_projections,test_projection_reducer,test_alert_lifecycle,test_alert_migration,test_alert_permissions_metrics,test_kafka_runtime,test_runtime}.py` | **41 passed, 0 failed, 0 skipped** | 8.05s |
| **Tổng Phase 0** | Tất cả unit + integration tests liên quan pipeline Kafka/PG | **356 passed, 0 failed, 0 skipped** | **47.55s** |

---

## 3. Controlled Benchmark Methodology & Cấu hình

Harness kiểm soát: [controlled_benchmark.py](../../../../scripts/benchmarks/hybrid_ordering/controlled_benchmark.py), cấu hình chạy đã lưu trong [benchmark_config.json](./benchmark_config.json):

- **Kiến trúc trong harness:** Chạy đồng thời các thành phần thật của MaintAI trong cùng tiến trình (`KeyedConsumerRunner` + `KeyedProcessingHandler` với `PROCESSING_MAX_ACTIVE_KEYS=8`, `max_inflight_per_key=1`, `max_pending_records=512`, `max_uncommitted_per_partition=256`; `OrderedOutboxRelay`; `ProjectionHandler` + `AlertHandler` + `AlertRepository`), kết nối tới DB PostgreSQL 17 riêng từng run (`bench_proc_*`, `bench_biz_*`) và topics 12 partitions (`sensor.readings`, `inference.results`, `alert.signals`) trên Kafka 4.0.0.
- **ML Simulator có kiểm soát (`ControlledMLProvider`):**
  - `ml_max_concurrency = 12`, `ml_latency_ms = 25.0 ± 5.0 ms` jitter.
  - Xác suất tail latency `ml_slow_prob = 0.10` (`ml_slow_ms = 300.0 ms`), cộng thêm tail xác định cho hot machine `M-001` mỗi 5 readings (`seq % 5 == 1`).
  - ~10% readings kích hoạt ứng viên cảnh báo (`torque_nm = 62.5` khi `(seq + hash(machine_id)) % 10 == 0` -> `prob = 0.88`, `FAILURE`, `HIGH`/`CRITICAL` theo `demo-risk-v1.json`).
- **Ba kịch bản tải (`50` machines `M-001`..`M-050`, mỗi run gồm `5s` warm-up + `15s` measurement × `3` runs, drain tối đa `90s`):**
  - **Workload A (Nominal Steady State):** `50` machines × `1.0 rps/machine` = **`50.0 rps`**.
  - **Workload B (Double Throughput Saturation):** `50` machines × `2.0 rps/machine` = **`100.0 rps`**.
  - **Workload C (Hot-Machine Head-of-Line):** `M-001` tại **`16.0 rps`** + `49` background machines × `1.0 rps` = **`65.0 rps`**.

---

## 4. Kết quả Baseline Benchmark (Controlled PG 17 + Kafka 4.0.0)

Dữ liệu đầy đủ của cả 9 lần chạy trích trực tiếp từ [summary.csv](./summary.csv) (`0` lỗi xử lý, `0` vi phạm correctness trên tất cả 9 runs):

| Workload | Run | Target RPS | Admitted RPS | Outcomes RPS | Preds RPS (Biz) | Inf p50 (ms) | Inf p95 (ms) | Hot (`M-001`) E2E p50 (ms) | Hot (`M-001`) E2E p95 (ms) | Hot (`M-001`) E2E p99 (ms) | BG E2E p50 (ms) | BG E2E p95 (ms) | DB Tx p95 (ms) | Outbox p95 (ms) | Alert p95 (ms) | ML Peak / 12 | Max Pending / 512 | Max Outbox | PG Conns | PG Locks | CPU (%) | RSS (MB) | Violations |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A | 1 | 50.0 | 50.00 | 50.00 | 44.73 | 26.501 | 301.572 | 1769.913 | 2370.644 | 2399.132 | 1752.327 | 4286.776 | 38.932 | 145.674 | 34.068 | 8 | 50 | 9 | 14 | 1 | 100.65 | 131.61 | 0 |
| A | 2 | 50.0 | 50.00 | 49.80 | 43.80 | 26.173 | 300.766 | 2011.209 | 2839.134 | 2964.324 | 1978.158 | 9480.728 | 41.081 | 249.452 | 39.772 | 8 | 50 | 18 | 15 | 0 | 99.36 | 134.82 | 0 |
| A | 3 | 50.0 | 50.00 | 49.87 | 44.53 | 26.005 | 301.364 | 2180.718 | 2443.198 | 2445.129 | 1971.132 | 8345.850 | 40.176 | 219.258 | 46.710 | 8 | 46 | 16 | 15 | 0 | 100.88 | 137.10 | 0 |
| B | 1 | 100.0 | 63.47 | 63.40 | 36.13 | 26.293 | 301.247 | 20028.666 | 25058.631 | 25510.855 | 10542.771 | 30969.116 | 39.727 | 526.642 | 36.299 | 8 | 506 | 42 | 13 | 0 | 121.55 | 143.71 | 0 |
| B | 2 | 100.0 | 64.67 | 64.47 | 35.93 | 25.999 | 300.051 | 20494.484 | 25116.650 | 25568.094 | 12920.902 | 32336.274 | 46.702 | 2653.215 | 36.792 | 8 | 507 | 161 | 14 | 1 | 122.82 | 148.92 | 0 |
| B | 3 | 100.0 | 57.80 | 57.80 | 32.40 | 26.263 | 301.351 | 19816.172 | 24727.936 | 25137.647 | 14851.389 | 33926.248 | 44.659 | 3969.160 | 39.391 | 8 | 508 | 227 | 14 | 1 | 123.45 | 152.79 | 0 |
| C | 1 | 65.0 | 46.80 | 46.80 | 44.73 | 26.659 | 302.206 | 26871.438 | 34486.488 | 35212.682 | 1925.565 | 22023.482 | 40.905 | 164.449 | 35.830 | 8 | 129 | 14 | 14 | 0 | 96.90 | 152.50 | 0 |
| C | 2 | 65.0 | 47.67 | 47.67 | 46.80 | 26.628 | 302.218 | 23456.604 | 30925.549 | 31516.925 | 1386.350 | 18799.120 | 37.517 | 132.236 | 27.311 | 8 | 125 | 16 | 14 | 0 | 99.49 | 152.85 | 0 |
| C | 3 | 65.0 | 47.53 | 47.53 | 48.20 | 26.291 | 302.263 | 24494.490 | 34012.011 | 34444.351 | 993.928 | 19775.825 | 34.789 | 111.854 | 29.129 | 8 | 123 | 10 | 14 | 0 | 97.28 | 153.16 | 0 |

Chi tiết chặng trung gian trích từ Run 1 của mỗi workload ([workload_A_run1.json](./workload_A_run1.json), [workload_B_run1.json](./workload_B_run1.json), [workload_C_run1.json](./workload_C_run1.json)):
- **Workload A (Run 1):** `inference_to_db_commit p95 = 66.34 ms`, `kafka_publish_to_business_db p50 = 1282.713 ms / p95 = 3678.704 ms`.
- **Workload B (Run 1):** `inference_to_db_commit p95 = 63.928 ms`, `kafka_publish_to_business_db p50 = 2612.362 ms / p95 = 25794.191 ms`.
- **Workload C (Run 1):** `inference_to_db_commit p95 = 66.802 ms`, `kafka_publish_to_business_db p50 = 1197.051 ms / p95 = 3489.768 ms`.

---

## 5. Phân tích Bottleneck & Phát hiện Kỹ thuật từ Baseline

1. **Nghẽn cổ chai Workload B (Double Throughput — 100 rps):**
   - Với `max_active_keys=8` và `max_inflight_per_key=1`, concurrency thực tế tới ML luôn bị kẹt tối đa tại **`ml_peak = 8/12` slots** (không tận dụng hết 12 slots ML đang rảnh).
   - Hệ quả: tốc độ tiêu thụ của Processing Worker bị ghìm xuống còn **`57.80 – 64.67 outcomes/s`** (thấp hơn mục tiêu `100 rps`), hàng đợi trong RAM chạm ngưỡng backpressure **`506 – 508 / 512 pending records`**, và `hot_e2e_p95` vọt lên **`24.73 – 25.12 giây`**.
2. **Nghẽn cổ chai Head-of-Line ở Workload C (`M-001` tại `16 rps`):**
   - Một reading chậm (`300 ms` tail) của `M-001` chặn toàn bộ readings theo sau của `M-001` do `per_key=1`. Với tốc độ gửi `16 rps` kết hợp tail cố định mỗi 5 readings (`seq % 5 == 1`) + 10% tail ngẫu nhiên, hàng đợi của `M-001` tích tụ (`max_pending = 123 – 129`), đẩy `hot_e2e_p95` của `M-001` lên **`30.93 – 34.49 giây`** (`p99` tới **`35.21 giây`**).
   - Trên lần chạy nháp ban đầu với timeout drain `30s`, `M-001` ở Workload C chưa kịp xả hết trong `30s` (còn dư `14 / 43 / 38` readings); sau khi nâng `drain_deadline` lên `90.0s`, toàn bộ readings xả sạch với **`0` correctness violations**, xác nhận vấn đề nằm ở hiệu năng hàng đợi chứ không phải lỗi mất bản ghi.
3. **Điểm cần lưu ý ở Business Consumer & PostgreSQL `synchronous_commit`:**
   - Trong `run1` của Workload A/B/C, giai đoạn `db_commit_to_kafka_publish` (`outbox`) ngắn ở A/1 (`145.674 ms`) và C/1 (`164.449 ms`), tăng lên ở B (`526.642 – 3969.160 ms`, `max_outbox = 42 – 227`), nhưng `kafka_publish_to_business_db` bị dồn hàng ở Business Consumer (`3.68s` p95 ở A/1, `25.79s` p95 ở B/1, `3.49s` p95 ở C/1) khi Compose PG17 chạy chế độ mặc định (`synchronous_commit=on`) với 1 serial `business-prediction` consumer (`preds_rps = 32.40 – 48.20 rps`).
   - Trong toàn bộ 9 runs baseline, `pg_locks` tối đa chỉ `0–1`, và sẽ là chỉ báo trọng yếu ở Phase 5 để kiểm chứng Stage A/C mới không gây deadlock hay lock contention dưới `per_key = 2, 4`.

---

## 6. Giới hạn Phạm vi Baseline Controlled vs Full k3d Cluster

Báo cáo Phase 0 này phản ánh **một benchmark controlled đơn-worker trên PG 17 + Kafka 4.0.0 Compose** nhằm cách ly ảnh hưởng thuật toán scheduler (`per_key=1` vs `2, 4`), **không đại diện đầy đủ cho vận hành cụm k3d đa replica**:
- Cụm `k3d-maintai-local` gồm `6` `processing-worker` pods, `2` `ml-service` pods (`uvicorn --workers 1`, AnyIO threadpool ~`40` threads/pod), `2` `processing-outbox-relay` pods, `4` `business-consumer` pods và `postgres-0` cấu hình `synchronous_commit=off` ([local/platform/postgres.yaml](../../../../../MaintAI-gitops/local/platform/postgres.yaml)).
- Để đo đầy đủ mạng pod-to-pod, CPU throttling, `AnyIO` queueing ở `ml-service` và endurance (`5m` warm-up + `10m` measure × `3` runs), cần chạy [k3d_full_benchmark.sh](../../../../scripts/benchmarks/hybrid_ordering/k3d_full_benchmark.sh) khi cụm `k3d-maintai-local` được khởi động lại.
