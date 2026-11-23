# Báo cáo Kỹ thuật & Benchmark: Burst + Keyed FIFO vs Burst + Hybrid Processing + ML Service Capacity Optimization — MaintAI

> **Phân loại bằng chứng trong tài liệu:** Mỗi nhận định và số liệu được gắn nhãn nguồn rõ ràng:
> - **`[Measured]`**: đo trực tiếp từ benchmark chạy thật trên cluster k3d hoặc Docker standalone (lưu JSON thô trong `docs/benchmarks/burst-keyed-vs-hybrid/`).
> - **`[Code-inspection]`**: đọc trực tiếp từ source code, Dockerfile, GitOps YAML hoặc `metadata.json`.
> - **`[Analytical estimate]`**: tính toán suy diễn có kiểm chứng từ số liệu đo và mô hình hàng đợi / Amdahl / Little's Law.
> - **`[Hypothesis]`**: giả thuyết cơ chế chưa tách biến kiểm chứng độc lập.
> - **`[NOT_MEASURED]`**: chưa đo trong phạm vi đợt benchmark này (ví dụ production AWS, dataset/model khác, hoặc HTTP Batch end-to-end).

---

## 1. Executive Summary

Đợt kiểm thử này đánh giá câu hỏi cốt lõi trên toàn chuỗi xử lý thật (`Sensor Engine -> HTTPS Ingress -> Ingestion API -> Kafka -> Processing Worker -> ML Service -> Outbox Relay -> Business Consumer -> PostgreSQL -> Business API -> Playwright Chromium SSE`): **với workload Burst 50 máy đồng bộ (`50 readings/cycle`, `1 reading/machine/s`), kiến trúc `Hybrid Ordering` có giúp prediction xuất hiện trên UI sớm hơn, giảm 50-machine spread hay cải thiện p95/p99 so với `Keyed FIFO` không, và `ML Service` có phải bottleneck thực sự hay không?**

### Kết luận chính

1. **`ML Service` (`hgb-interactions-candidate-v1`) ĐÍCH THỰC là bottleneck chi phối Round 1 (`[Measured]`)**:
   - Với cấu hình local baseline (`2` Pods `ml-service-shadow`, `--workers 1`, ảnh `sha-94248e49...`, `pd.DataFrame` + `ColumnTransformer` cho từng request đơn), `ml_server p95 = 242.3–251.4 ms` (Prometheus server histogram p95 `305.1–344.5 ms`) và `ml_client p95 = 476.2–480.1 ms` `[Measured]`.
   - Kiểm toán độc lập Docker (`[Measured]`): 1 process Uvicorn chạy HGB bị nghẽn bởi **single-row `pd.DataFrame` + `ColumnTransformer` overhead (~60% CPU/latency)** và **Python GIL (bị trần ở `1.122 cores` và `111.6 RPS` ngay cả khi cấp `2 CPUs`)**. Tăng `OMP_NUM_THREADS=OPENBLAS_NUM_THREADS=MKL_NUM_THREADS=1` giúp tăng nhẹ `+4.5%..+11%` RPS trên 1 CPU (`93.8–98.7 RPS`), nhưng cấu hình `--workers 2` trên 1 Pod gây **+45 ms accept-delay trên shared listen socket tại concurrency thấp (`c=1..2`, p50 nhảy `9.56 ms -> 56.20 ms`)** — do đó **scale ngang Kubernetes Pods (`--workers 1`) vượt trội hoàn toàn so với tăng workers nội bộ trong Pod**.
2. **Tối ưu code ML (`K1` Verified NumPy Fast-Path) + scale `3` Pods ML giảm `-21.3%..-22.9%` E2E UI p95 (`[Measured]`)**:
   - Thêm `_FastInferencePath` zero-DataFrame thuần NumPy vào [local_baseline.py](../../ml-service/inference/api/local_baseline.py) (có kiểm chứng parity `< 1e-12` qua 3 profile `L/M/H` khi startup và warm-up `10` lần trước `/health/ready`, **không chạm** [feature_interactions.py](../../ml-service/shared/preprocessing/feature_interactions.py) hay `pipeline.joblib`): throughput standalone tăng gấp đôi từ `89.8 RPS` lên **`180.7–183.9 RPS` trên 1 CPU** (`215.8 RPS` trên 2 CPUs) `[Measured]`.
   - Round 2 trên k3d (`3` Pods `maintai-ml-bench:hgb-opt`, `OMP=OPENBLAS=MKL=1`, `--workers 1`) giảm `ml_server p95` xuống **`47.8–54.2 ms` (`-78%..-81%`)**, giảm `ml_client p95` xuống **`117.2–184.8 ms`**, giảm `sample_to_ui p95` từ **`781.0 / 797.1 ms` xuống `613.6 / 614.6 ms` (`-21.4% / -22.9%`)**, đồng thời giảm tổng CPU của ML từ **`~0.99 cores` (2 Pods) xuống `~0.54 cores` (3 Pods, `-45%`)** `[Measured]`.
3. **`Hybrid Ordering` KHÔNG thắng `Keyed FIFO` trên burst đồng đều 50 máy (`1 reading/machine/s`) (`[Measured]`)**:
   - Round 1 (ML nghẽn): Hybrid `B1 (16/4/16)` **chậm hơn** Keyed FIFO `A1 (16/1/16)` `+16.0 ms` ở `sample_to_ui p95` (`797.1 vs 781.0 ms`) và `+15.4 ms` ở `sample_to_biz p95` (`719.8 vs 704.4 ms`).
   - Round 2 (đã tối ưu ML): Keyed FIFO `A2` và Hybrid `B2` **hòa thống kê (`~1 ms` chênh lệch)**: `sample_to_ui p95 = 613.6 ms (A2)` vs `614.6 ms (B2)`; `sample_to_biz p95 = 531.7 ms (A2)` vs `531.8 ms (B2)`; `first_prediction_ui p50 = 95.4 ms (A2)` vs `95.9 ms (B2)`; `cycle_completion_ui p95 = 680.5 ms (A2)` vs `673.6 ms (B2)`.
   - Nguyên nhân gốc: ở tải chuẩn `1 reading/machine/s`, độ sâu backlog per-key là `<= 1` (`max_per_key_inflight_peak = 1` ở mọi run `B1`/`B2`), nên `per_key = 4` không được kích hoạt, trong khi Hybrid vẫn phải trả thêm **Stage A Admission TxA (`p95 = 24.4–46.8 ms`)**, **Stage C `AlertResequencer` (`persist p95 = 87.7–171.7 ms`)**, và drain lâu hơn (`1.32–1.61 s` vs `0.90–1.05 s`).
4. **Correctness đạt 100% ở mọi mô hình và khi kill pod (`[Measured]`)**:
   - Tất cả `15` run 5 phút (`45 × 16,550` readings tổng cộng cho Round 1+2 + `ML-S1` + `2` bài recovery kill `processing-worker` và `ml-service-shadow` pod giữa burst) đều đạt **`total_violations = 0`**, `missing_outcomes = 0`, `wrong_latest_sequence = 0`, `duplicate_active_alerts = 0`, `proc_conflicts_delta = 0`, `biz_conflicts_delta = 0`, PG `deadlocks = 0`, `lock_wait_samples = 0`.
   - Test end-to-end mới `test_r101_critical_slow_and_r102_normal_fast_end_to_end_ordering` trên PostgreSQL 17 + Kafka 4.0.0 thật chứng minh kịch bản `R101 CRITICAL` (chậm) + `R102 NORMAL` (nhanh) giữ đúng UI `NORMAL (seq=102)` và alert `OPEN CRITICAL (seq=101)` mà không auto-resolve sai `[Measured]`.
5. **Khuyến nghị dứt khoát**:
   - **Giữ `PROCESSING_HYBRID_ORDERING_ENABLED=false` (`Keyed FIFO`) làm mặc định**, nâng `PROCESSING_MAX_ACTIVE_KEYS=16` (đồng bộ `max_total_inflight=16`), giữ nguyên `K1` NumPy fast-path + `OMP=OPENBLAS=MKL=1` + `--workers 1` và scale **`2–3` Pods ML**.
   - **Chỉ bật Hybrid** cho workload có **hot-machine / replay backlog nhiều readings trên cùng một `machine_id`**.

### Bảng tổng hợp các cấu hình chính (Aggregate trung bình 3 runs × 5 phút mỗi preset) `[Measured]`

| Metric | A0 (`8/1/8`, ML-Base 2p) | A1 (`16/1/16`, ML-Base 2p) | B1 (`16/4/16`, ML-Base 2p) | A2 (`16/1/16`, ML-OPT 3p) | B2 (`16/4/16`, ML-OPT 3p) |
|---|---:|---:|---:|---:|---:|
| Violations / Missing / Conflicts | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| Drained 100% (median drain s) | Yes (0.721 s) | Yes (0.735 s) | Yes (1.153 s) | Yes (1.047 s) | Yes (1.335 s) |
| `sample_to_biz` p50 / p95 / p99 (ms) | 523.7 / 697.0 / 756.2 | 534.5 / 704.4 / 763.9 | 548.6 / 719.8 / 780.6 | **387.2 / 531.7 / 587.9** | **385.6 / 531.8 / 580.2** |
| `sample_to_ui` p50 / p95 / p99 (ms) | 603.7 / 774.8 / 837.1 | 617.6 / 781.0 / 840.8 | 629.2 / 797.1 / 860.7 | **494.8 / 613.6 / 671.1** | **501.0 / 614.6 / 664.3** |
| First pred on UI p50 (ms) | 113.4 | 139.1 | 151.8 | **95.4** | **95.9** |
| 50-machine cycle completion UI p95 (ms) | 846.1 | 854.1 | 869.1 | 680.5 | **673.6** |
| 50-machine spread UI p95 (ms) | 723.7 | 720.9 | 729.1 | 575.3 | **564.9** |
| ML server duration p95 (`predicted_at`/ts) (ms) | 242.3 | 248.6 | 251.4 | 54.2 | **47.8** |
| ML client roundtrip p95 Prom (ms) | 476.2 | 479.7 | 480.1 | 184.8 | **117.2** |
| ML Pod CPU total (cgroup v2 median cores) | 0.963 | 0.985 | 0.993 | **0.536** | **0.539** |
| Worker Pod CPU total (cgroup v2 median cores) | 1.202 | 1.225 | 1.262 | 1.310 | 1.308 |

---

## 2. Repository, Branch, Commit SHA và Trạng thái Tuân thủ

Kiểm tra trực tiếp ba repository tại thời điểm thực thi và kết thúc `[Measured]`:

| Repository | Đường dẫn local | Branch | Base HEAD SHA | Quyền tác động | Trạng thái tuân thủ |
|---|---|---|---|---|---|
| `MaintAI` | `.` (`/home/chienqt/Dev/newBTL/MaintAI`) | `feat/hybrid-ordering` | `e53ad7a88a6c94e1c1a17b65031d949fa0ce7810` | Được phép sửa & commit | Chỉ sửa simulator, tests benchmark/ML `local_baseline.py`, harness & report |
| `MaintAI-gitops` | `../MaintAI-gitops` | `feat/hybrid-ordering` | `4ea6821e115cf773f8306cca50ee0ddf69fb2005` | **READ-ONLY** | Có thay đổi sẵn `local/platform/postgres.yaml` của user; không chỉnh sửa thêm |
| `Maint-infrastructure` | `../Maint-infrastructure` | `main` | `047c231837fc7c6500c308a6f18f2dec594402b3` | **READ-ONLY** | Working tree sạch |

Các bất biến bảo vệ hiện trạng đã được giữ nguyên `[Code-inspection]`:
- File uncommitted sẵn của user [sensor/src/simulator/ordering/store.py](../../sensor/src/simulator/ordering/store.py) và thư mục `.kube/` **không bị ghi đè hoặc stage**.
- [ml-service/shared/preprocessing/feature_interactions.py](../../ml-service/shared/preprocessing/feature_interactions.py) (`transformer_sha256 = 34a9f8f5a74f8ae32215b56174d96743207ea554558a11a08bba0623ecfffc6c`) và model artifact `pipeline.joblib` **không bị chỉnh sửa** (bảo toàn startup SHA-256 verification).
- `PROCESSING_HYBRID_ORDERING_ENABLED=false` được giữ làm mặc định trong GitOps ([kustomization.yaml](../../../MaintAI-gitops/environments/local/apps/kustomization.yaml)) lẫn code ([config.py](../../messaging/src/maintai_messaging/config.py)).
- Không merge vào `main` và không triển khai lên AWS production.
- Cụm `k3d-maintai-local` đã được khôi phục nguyên trạng theo snapshot trước benchmark (ArgoCD `syncPolicy`, KEDA `ScaledObject`, HPAs, deployments, `sensor` replica `1`, xóa `ConfigMap/bench-hgb-identity`).

---

## 3. Kiến trúc Burst + Keyed FIFO

Trong chế độ mặc định (`PROCESSING_EXECUTION_MODE=keyed`, `PROCESSING_HYBRID_ORDERING_ENABLED=false`) `[Code-inspection]`:

```text
Kafka (sensor.readings, 12 partitions, key=machine_id)
  └─> KeyedConsumerRunner (6 worker pods; 1 poll thread / pod)
        ├─ Partition -> active_keys scheduler (per-key FIFO queue, max_active_keys=N)
        └─ DaemonExecutor (effective_max_total_inflight threads / pod; max_inflight_per_key = 1)
              └─ 1 Reading -> Single TX + HTTP flow:
                   1. Check cached terminal outcome
                   2. FeatureValidator DQ + HTTPMachineRegistry (30s cache) -> ML POST /api/v1/predict (timeout 2s, retry 1x on 5xx)
                   3. Single PG TX: _fence + processing_source_cursor FOR UPDATE + claim/persist + outbox (inference.results, alert.signals)
        └─ OffsetCoordinator: chỉ commit prefix liên tục đã hoàn tất bền vững trên từng partition
```

Đặc điểm quyết định khi gặp burst 50 máy `[Code-inspection]` + `[Analytical estimate]`:
- Với `6` worker pods và `12` Kafka partitions (`~2 partitions/pod`), **cả 50 `machine_id` được hash trải đều qua 12 partitions** (trung bình `~4.17 máy/partition`, `~8.33 máy/pod`). Tuy nhiên, partition skew ngẫu nhiên có thể khiến một số worker nhận `> 8` máy hoạt động đồng thời (`active_keys_peak` tổng 6 pods đạt tới `44–49` ở `A0/A1`).
- Tại cấu hình mặc định `PROCESSING_MAX_ACTIVE_KEYS=8` (`A0`), nếu 1 pod được gán `9–12` máy hoặc bị kẹt head-of-line ngắn trên một partition, phần key thứ `9+` trên pod đó phải chờ trong nội bộ `KeyedConsumerRunner`.
- Mỗi reading hợp lệ chỉ chạy **1 transaction PG ghi + outbox** (`persist_prepared`), không tạo transaction admission riêng và không cần table `processing_alert_ledger` (`max_inflight_per_key=1` đảm bảo thứ tự tự nhiên theo từng `machine_id`).

---

## 4. Kiến trúc Burst + Hybrid Ordering

Khi bật `PROCESSING_HYBRID_ORDERING_ENABLED=true` (`KeyedProcessingHandler._handle_hybrid` trong [handler.py](../../processing/worker/src/consumer/handler.py)) `[Code-inspection]`:

```text
Kafka (sensor.readings) -> KeyedConsumerRunner
  ├─ Stage A (Admit - Poll/Worker fencing TX):
  │    _fence + AlertResequencer.admit(): cấp `admission_seq` đơn điệu theo machine trong PG
  │    (`processing_admission_cursor`, `processing_alert_ledger` trạng thái `PENDING`).
  ├─ Stage B (Parallel Compute - ngoài DB transaction):
  │    FeatureValidator DQ -> HTTPMachineRegistry -> ML POST /api/v1/predict
  │    Cho phép tối đa `max_inflight_per_key` readings CÙNG một `machine_id` inference đồng thời,
  │    và tổng tối đa `max_total_inflight` tasks trên toàn Pod.
  └─ Stage C (Persist & Ordered Alert Resequencing - Fenced PG TX):
       pg_advisory_xact_lock(machine_id, sequence) + _fence
       -> insert `maintai_kafka_inbox`, persist `sensor_events`/`processing_results`/`risk_decisions`
       -> `outbox.enqueue("inference.results", ...)` (có thể đi sớm ngay khi inference xong!)
       -> `AlertResequencer.record_disposition()` + `drain_frontier()` phát `alert.signals` ĐÚNG thứ tự `admission_seq`.
```

### Ánh xạ tham số cấu hình thực tế `[Code-inspection]`

Theo `KeyedSettings` trong [config.py](../../messaging/src/maintai_messaging/config.py#L16-L97):

| Tham số yêu cầu | Biến môi trường thực tế trong MaintAI | Ràng buộc trong `KeyedSettings` | Giá trị trong A0 / A1 / B1 / A2 / B2 |
|---|---|---|---|
| Scheduler Mode | `PROCESSING_EXECUTION_MODE` + `PROCESSING_HYBRID_ORDERING_ENABLED` | `keyed` + `false` (FIFO) hoặc `true` (Hybrid) | `keyed` + `false` (A0, A1, A2) / `true` (B1, B2) |
| Active keys / Pod | `PROCESSING_MAX_ACTIVE_KEYS` | `>= 1`, `<= max_pending_records (512)` | `8` (A0) / `16` (A1, B1, A2, B2) |
| Per-key in-flight | `PROCESSING_MAX_INFLIGHT_PER_KEY` | `> 1` bắt buộc bật `hybrid_ordering_enabled=true` | `1` (A0, A1, A2) / `4` (B1, B2) |
| Global in-flight / Pod | `PROCESSING_MAX_TOTAL_INFLIGHT` | Nếu bỏ trống thì `effective = min(active*per_key, 512)`; luôn `<= 512` | `8` (A0) / `16` (A1, B1, A2, B2) |
| Executor / Thread Pool | `DaemonExecutor` ([keyed.py](../../messaging/src/maintai_messaging/keyed.py#L25-L62)) + `httpx.AsyncClient` ([main.py](../../processing/worker/src/consumer/main.py#L56-L85)) | Pool worker threads = `effective_max_total_inflight`; 1 asyncio event loop chung bridg qua `asyncio.run_coroutine_threadsafe` | `8` threads (A0) / `16` threads (A1, B1, A2, B2) |
| Kafka Commit Batching | `KAFKA_COMMIT_BATCH_SIZE` / `KAFKA_COMMIT_MAX_WAIT_MS` | Cố định công bằng giữa A & B trong benchmark | `16` / `25 ms` (cố định mọi preset A0/A1/B1/A2/B2) |
| DB Pool Size / Pod | `ConnectionPool(size=max(12, min(32, effective_max_total_inflight + 4)))` | Tự co giãn theo `effective_max_total_inflight` | `12` (A0) / `20` (A1, B1, A2, B2) |

Lưu ý thiết kế quan trọng (`[Code-inspection]`): `KeyedProcessingHandler` dùng **cùng budget `max_total_inflight = 16` ở cả `A1` và `B1` (và `A2`/`B2`)**, bảo đảm so sánh công bằng về concurrency toàn cục theo §4.

---

## 5. Burst Sensor Simulator và Độ dàn trải Kafka (Arrival Spread)

### 5.1. Thay đổi trong Sensor Simulator `[Code-inspection]`

Đã bổ sung chế độ lấy mẫu `synchronized` / `burst` trong [settings.py](../../sensor/src/simulator/settings.py#L92-L93) (`SIM_SAMPLING_MODE`, giữ mặc định `"staggered"`) và [engine.py](../../sensor/src/simulator/scheduling/engine.py#L236-L275):
- **Giữ nguyên hoàn toàn hành vi mặc định `staggered`**: offset đều từng máy theo `i * interval / total`.
- **Chế độ `synchronized` / `burst`**: mỗi chu kỳ 1 giây tạo đúng 50 readings cho `M-001..M-050` với cùng một `cycle_id` và `sampled_at`, ghi atomic vào SQLite WAL spool qua `store.generate_many(...)`, sau đó `dispatch_once()` giao cho `ThreadPoolExecutor(send_workers=32)` gửi **50 Individual HTTPS POST requests** (KHÔNG dùng HTTP Batch), giữ per-machine in-flight = 1 để bảo toàn FIFO trên cạnh (`store.py` giữ nguyên bất biến).
- **Instrumentation trace JSONL** (`SIM_BURST_TRACE_PATH`): ghi chính xác `cycle_id`, `sampled_at`, `edge_persisted_at`, `sent_at`, `ingestion_ack_at`, `server_accepted_at`, `outcome`, `status_code`, `attempts`, `send_latency_ms`.

Kiểm thử simulator: `.venv/bin/pytest sensor/tests/ -q` -> **140/140 passed** (`[Measured]`).

### 5.2. Burst tại nguồn vs phân bố đến Kafka thực tế `[Measured]`

Burst đồng bộ tại sensor **không tạo thành một điểm thời gian đơn tại Processing**. Dữ liệu đo thực tế qua toàn bộ `9 × 301 = 2,709` chu kỳ trong Round 1 và `6 × 301 = 1,806` chu kỳ trong Round 2 (scan trực tiếp `CreateTime` và partition từ broker Kafka `sensor.readings`):

| Giai đoạn tiền xử lý trước Kafka | Round 1 (`A0/A1/B1` median runs) | Round 2 (`A2/B2` median runs) | Ý nghĩa |
|---|---|---|---|
| `sample_to_edge_spool` (50 máy ghi SQLite WAL) | p50 `4.14–4.20 ms`, p95 `5.52–5.87 ms` | p50 `4.16–4.19 ms`, p95 `5.84–6.04 ms` | 50 readings thực sự sinh đồng bộ trong ~4–6 ms |
| `edge_to_ingestion_ack` (Individual HTTPS POST) | p50 `183.7–186.7 ms`, p95 `267.2–274.8 ms` | p50 `197.3–199.5 ms`, p95 `296.4–299.1 ms` | TLS + `32` send workers + `2` Ingress/API pods xếp hàng gửi |
| **Kafka `arrival_spread_per_cycle`** (`max - min` CreateTime trong 50 msgs) | **p50 `223–226 ms`, p95 `309–318 ms`, max `366–507 ms`** | **p50 `242–246 ms`, p95 `341–343 ms`, max `439–524 ms`** | 50 messages trải rộng tự nhiên trên **~230–345 ms**, dùng **12/12 partitions** |
| HTTP 502 retryable tại Ingestion (trên `16,550` requests/run) | `1–5` events/run (`0.006–0.030%`) | `1–4` events/run (`0.006–0.024%`) | Được SQLite spool retry thành công 100% -> **`202` = `16,550/16,550`** |

**Kết luận tiền-Processing `[Measured]`:** Individual HTTP POST + `send_workers=32` + Ingress TLS tự tạo ra cửa sổ dàn trải **~225–350 ms** tại đầu vào Kafka. Ở tải 50 RPS đồng đều, mỗi máy chỉ có tối đa `1` reading trong chu kỳ 1s đó.

---

## 6. Round 1 Scheduler Benchmark (`A0` vs `A1` vs `B1` trên Fixed Baseline ML)

Round 1 chạy trên cấu hình cố định: `ml-service-shadow` ảnh `ghcr.io/chiendz11/maintai-ml-service:sha-94248e49bd84b112162244f62b25fd466cbf82f4`, **`2` Pods** (`min=max=2`), `--workers 1`, `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `processing-worker=6`, `business-consumer=4`, `KAFKA_COMMIT_BATCH_SIZE=16`, `KAFKA_COMMIT_MAX_WAIT_MS=25`. Mỗi preset chạy **3 lần độc lập × (30s warmup + 300s measure)**.

### 6.1. Chi tiết từng lần chạy (Round 1) `[Measured]`

| Preset | Run | Cycles / Sent | Drain (s) | SSE Cov (%) | `sample->biz` p50/p95/p99 (ms) | `sample->ui` p50/p95/p99 (ms) | First UI p50 (ms) | Cycle UI p95 (ms) | Spread 50 UI p95 (ms) | ML Srv p95 ts/Prom (ms) | ML Cli p95 Prom (ms) | ML CPU (cores) | PW CPU (cores) | Viol |
|---|---:|---:|---:|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| **A0** | 1 | 301 / 16550 | 0.693 | 89.37% | 529.8 / 671.8 / 732.7 | 611.1 / 751.0 / 813.9 | 113.3 | 832.4 | 696.8 | 232.8 / 292.8 | 472.9 | 0.959 | 1.202 | 0 |
| **A0** | 2 | 301 / 16550 | 0.744 | 89.16% | 540.8 / 683.5 / 734.0 | 621.5 / 760.9 / 813.6 | 114.3 | 824.6 | 710.6 | 240.7 / 305.1 | 474.4 | 0.978 | 1.221 | 0 |
| **A0** | 3 | 301 / 16550 | 0.721 | 89.39% | 500.5 / 735.9 / 801.8 | 578.4 / 812.6 / 883.8 | 112.7 | 881.2 | 763.6 | 253.4 / 341.0 | 481.3 | 0.963 | 1.182 | 0 |
| **A1** | 1 | 301 / 16550 | 0.660 | 89.06% | 542.2 / 690.5 / 749.7 | 623.2 / 764.7 / 825.0 | 151.4 | 842.1 | 693.8 | 246.5 / 326.2 | 477.3 | 0.988 | 1.226 | 0 |
| **A1** | 2 | 301 / 16550 | 0.930 | 89.18% | 534.4 / 700.7 / 766.2 | 619.5 / 780.2 / 845.9 | 163.4 | 850.4 | 708.2 | 247.6 / 322.8 | 477.5 | 0.985 | 1.225 | 0 |
| **A1** | 3 | 301 / 16550 | 0.735 | 89.34% | 527.0 / 721.9 / 775.9 | 610.2 / 798.2 / 851.4 | 102.5 | 869.7 | 760.8 | 251.7 / 340.9 | 484.2 | 0.974 | 1.214 | 0 |
| **B1** | 1 | 301 / 16550 | 1.153 | 89.37% | 535.2 / 740.7 / 811.0 | 614.7 / 818.5 / 896.4 | 151.8 | 898.7 | 751.8 | 255.8 / 355.5 | 481.5 | 0.999 | 1.235 | 0 |
| **B1** | 2 | 301 / 16550 | 1.223 | 89.37% | 558.8 / 704.4 / 765.8 | 637.6 / 780.4 / 842.5 | 161.8 | 850.8 | 698.0 | 249.2 / 338.2 | 479.3 | 0.987 | 1.270 | 0 |
| **B1** | 3 | 301 / 16550 | 1.044 | 89.37% | 551.9 / 714.2 / 765.1 | 635.3 / 792.4 / 843.2 | 141.9 | 857.8 | 737.6 | 251.7 / 344.5 | 479.4 | 0.993 | 1.262 | 0 |

### 6.2. So sánh chính Round 1 (`A1` vs `B1`, cùng global budget `16`) `[Measured]`

- `sample_to_ui`: `B1` tăng **+11.6 ms p50** (`629.2 vs 617.6 ms`), **+16.0 ms p95** (`797.1 vs 781.0 ms`), **+19.9 ms p99** (`860.7 vs 840.8 ms`).
- `sample_to_biz` (độ phủ 100.0% mọi `15,050` readings/run): `B1` tăng **+14.1 ms p50** (`548.6 vs 534.5 ms`), **+15.4 ms p95** (`719.8 vs 704.4 ms`), **+16.7 ms p99** (`780.6 vs 763.9 ms`).
- `first_prediction_ui p50`: `B1` chậm hơn `A1` **+12.7 ms** (`151.8 vs 139.1 ms`); `A0` là `113.4 ms` (ít cạnh tranh nội bộ ML hơn khi chỉ đẩy tối đa `8` tasks/pod).
- `drain_seconds`: `B1` cần **1.04–1.22 s** (so với `0.66–0.93 s` của `A1`), dù `PG locks = 0` và `max_per_key_inflight_peak = 1`.
- **Tổng kết Round 1:** Trên ML Service chưa tối ưu (`ml_client p95 ~480 ms`, `ml_server p95 ~248–251 ms`), tăng concurrency Processing không giúp cải thiện E2E vì cả 2 Pods ML đã bão hòa gần 1 core total (`~0.96–0.99 cores`) ngay tại burst 50 RPS; đồng thời Hybrid `B1` chịu thêm overhead TxA (`admission p95 = 23.7–24.4 ms`) và Stage C (`persist p95 = 87.7–99.1 ms`).

---

## 7. ML Service Capacity Audit

### 7.1. Source & GitOps Audit (§5.1) `[Code-inspection]`

Kiểm tra trực tiếp [ml-service/inference/Dockerfile](../../ml-service/inference/Dockerfile), [local_baseline.py](../../ml-service/inference/api/local_baseline.py), [requirements-serving.txt](../../ml-service/inference/requirements-serving.txt), [metadata.json](../../ml-service/artifacts/integration/hgb-interactions-m56w5ff4/metadata.json) và GitOps ([base/ml-service/deployment.yaml](../../../MaintAI-gitops/base/ml-service/deployment.yaml), [environments/local/apps/ml-service-shadow.yaml](../../../MaintAI-gitops/environments/local/apps/ml-service-shadow.yaml), [environments/local/apps/hpa.yaml](../../../MaintAI-gitops/environments/local/apps/hpa.yaml), [environments/local/apps/kustomization.yaml](../../../MaintAI-gitops/environments/local/apps/kustomization.yaml)):

- **Model**: `HistGradientBoostingClassifier` (`hgb-interactions-candidate-v1`, `max_iter=300`, `max_leaf_nodes=31`, `min_samples_leaf=10`, `l2_regularization=1.0`, threshold `0.08943418823732646`, `sklearn==1.9.1`). Pipeline gồm `SensorInteractions` -> `ColumnTransformer(OneHotEncoder + StandardScaler + passthrough)` -> `HistGradientBoostingClassifier`. Chạy hoàn toàn trên **CPU** (`python:3.12-slim`).
- **Uvicorn / FastAPI runtime**: `CMD ["python", "-m", "uvicorn", "local_baseline:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1", "--no-proxy-headers"]`.
- **FastAPI route handler**: `def predict(...)` ở dòng 499 ([local_baseline.py](../../ml-service/inference/api/local_baseline.py#L499)) là **sync endpoint** -> Starlette/AnyIO mặc định đẩy vào threadpool **40 threads** (`anyio.to_thread.current_default_thread_limiter().total_tokens == 40`). Vì cùng nằm trong 1 process Python nên mọi thread đều tranh giành **Python GIL**.
- **Thread env vars**: trong GitOps ConfigMap `ml-service-shadow-config` có `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1` (nhưng thiếu `MKL_NUM_THREADS=1`); khi chạy Docker baseline không truyền env (`ML-0`), C/OpenMP/OpenBLAS dùng mặc định đa luồng của thư viện native trên host 16 vCPU.
- **Model loading & Readiness**: model được kiểm chứng SHA-256 + `feature_interactions.py` hash trong `lifespan`, chỉ mở `/health/ready` trả `200` khi `STATE["pipeline"]` và `STATE["metadata"]` khác `None`. Tuy nhiên trước tối ưu `K1`, `startupProbe` chỉ chạy `1` probe inference đơn.
- **HTTP Client từ Processing**: `processing-worker` dùng 1 `httpx.AsyncClient(trust_env=False)` chung mỗi Pod (`client.py`: timeout cứng `<= 2.0 s`, `follow_redirects=False`, stream identity, tối đa `2` attempts trên `5xx` hoặc transport error), nối tới Kubernetes `Service/ml-service-shadow` (ClusterIP TCP-level iptables/IPVS load balancing).
- **Resources & Replicas trong GitOps**: `requests: cpu=100m, memory=128Mi`, `limits: memory=1Gi` (**KHÔNG khai báo `limits.cpu` trên local** -> cgroup `cpu.max = "max 100000"`). `ml-service-hpa` là `min=2, max=2`; `ml-service-shadow-hpa` là `min=1, max=1` (khi benchmark Round 1 pinned `min=max=2` theo reference config).

### 7.2. Benchmark ML Service Độc lập trên Host cgroup v2 (`ML-0` Baseline) `[Measured]`

Chạy bằng [ml_capacity_audit.py](../../scripts/benchmarks/burst_keyed_vs_hybrid/ml_capacity_audit.py) trên host `AMD Ryzen 7 7435HS` (`8C/16T`, `15 GiB RAM`): container `maintai-ml-bench:hgb-local`, `--cpus=1.0` (`cpu.max = 100000 100000`), `--memory=1g`, **không truyền OMP/BLAS env**, payload AI4I thật (`hgb-interactions-m56w5ff4`), 60s mỗi mức concurrency:

| Concurrency | Peak In-flight | RPS / Success RPS | Error Rate | HTTP Latency p50 / p95 / p99 (ms) | Server Infer p50 / p95 (ms) | Queue/GIL Wait p50 / p95 (ms) | cgroup CPU (cores) | CPU / Limit (%) | Throttled Periods / Total (`Ratio %`) | Throttled Time (ms) | Mem (MiB) | Host CPU (%) |
|---:|---:|---:|---:|---|---|---|---:|---:|---|---:|---:|---:|
| **1** | 1 | **89.75** | 0.0% | 10.90 / 12.95 / 14.49 | 7.55 / 9.15 | 3.35 / 4.01 | 0.867 | 86.7% | 0 / 600 (**0.0%**) | 0 | 133.0 | 22.8% |
| **2** | 2 | **88.96** | 0.0% | 21.82 / 29.95 / 36.55 | 14.23 / 21.28 | 7.54 / 14.02 | 0.999 | 99.9% | 532 / 600 (**88.7%**) | 2,078 | 113.4 | 23.9% |
| **4** | 4 | **88.31** | 0.0% | 44.30 / 64.83 / 80.88 | 22.74 / 43.21 | 20.64 / 38.08 | 1.000 | 100.0% | 595 / 600 (**99.2%**) | 7,100 | 111.8 | 23.9% |
| **8** | 8 | **89.04** | 0.0% | 90.97 / 126.23 / 157.80 | 38.78 / 75.49 | 49.20 / 84.78 | 1.000 | 100.0% | 600 / 601 (**99.8%**) | 8,930 | 112.9 | 22.8% |
| **12** | 12 | **89.90** | 0.0% | 135.43 / 186.06 / 214.50 | 46.91 / 103.91 | 82.93 / 133.98 | 1.000 | 100.0% | 598 / 601 (**99.5%**) | 9,073 | 112.8 | 21.7% |
| **24** | 24 | **87.58** | 0.0% | 282.33 / 363.01 / 400.50 | 67.85 / 186.61 | 202.21 / 280.95 | 1.000 | 100.0% | 601 / 602 (**99.8%**) | 9,662 | 116.2 | 23.2% |

**Nhận xét Audit ML-0 (`[Measured]`):**
1. **Throughput bão hòa của 1 Pod (1 CPU) = `88–90 RPS`** — **bão hòa NGAY TỪ `c = 2`** (`88.7%` throttling!). Tại `c >= 2`, throughput phẳng hoàn toàn (`~89 RPS`) nhưng `http_p95` phình tuyến tính theo Little's Law (`12.95 ms -> 29.95 -> 64.83 -> 126.23 -> 186.06 -> 363.01 ms`).
2. **RAM rất nhẹ** (`112–133 MiB`, ổn định không leak).
3. Concurrency hiệu quả trên **1 ML Pod (`--workers 1`) chỉ nên là `c = 1..2`** (giữ `c <= 4` làm trần cảnh báo). Với `6` worker pods × `16` max inflight có thể dồn tối đa tới `96` concurrent calls trong burst cực đại, `2` ML pods chịu tới `c=24–48` đồng thời chính là lý do `ml_client p95` nhảy lên `~480 ms` ở Round 1!

---

## 8. ML Service Configuration Optimization (`ML-C1`, `ML-C2`)

Áp dụng nguyên tắc **thay đổi từng biến một (One-Variable-At-A-Time)** trong 60s mỗi mức concurrency (`[Measured]`):

### 8.1. Bước `ML-C1`: Khóa `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1` `[Measured]`

| Biến thể | Env / Limit | RPS (c=1 / c=4 / c=8 / c=24) | HTTP p95 ms (c=1 / c=8 / c=24) | Server p95 ms (c=1 / c=8) | CPU Cores Peak | Throttled Ratio Max | Mem (MiB) | Host CPU % |
|---|---|---:|---|---|---:|---:|---:|---|
| **`ML-0`** (1 CPU, default env) | none, `1.0` CPU | 89.8 / 88.3 / 89.0 / 87.6 | 12.95 / 126.23 / 363.01 | 9.15 / 75.49 | 1.000 | 99.8% | 112–133 | 21.7–23.9% |
| **`ML-C1`** (1 CPU, `OMP=1`) | `OMP/BLAS/MKL=1`, `1.0` CPU | **93.8 / 98.0 / 98.7 / 95.8** | **12.27 / 115.56 / 339.03** | **8.62 / 69.78** | 1.000 | 99.8% | 134–178 | 15.4–21.0% |
| **`ML-C1-2CPU`** (2 CPU, `workers=1`) | `OMP/BLAS/MKL=1`, `2.0` CPU | **100.3 / 108.3 / 111.6 / 106.9** | **12.13 / 100.61 / 304.71** | **8.41 / 60.39** | **1.122** (`56.1%`) | **0.0%** | 119–138 | 15.8–17.6% |

**Phát hiện quan trọng từ `ML-C1-2CPU` (`[Measured]`):**
- Tắt thread oversubscription của OpenMP/OpenBLAS giúp `ML-C1` (1 CPU) tăng **+4.5% đến +10.8% RPS** và giảm tiêu thụ CPU toàn host (`15.4–21.0%` vs `21.7–23.9%`) do bớt spin/futex của worker OpenMP khi chạy batch nhỏ 1 dòng.
- Tuy nhiên, **khi nới giới hạn lên `2.0 CPUs` nhưng giữ `--workers 1` (`ML-C1-2CPU`)**, container chỉ dùng tối đa **`1.122 cores` (`56.1%` của 2 CPU), `0%` CPU throttling, và kịch trần ở `111.6 RPS` (`c=8`)**. Đây là **bằng chứng trực nghiệm cho Python GIL**: một process Python đơn với `def predict()` sync threads bị khóa chặt quanh ~1.12 CPU core (1 core cho Python interpreter + ~0.12 core cho Pydantic/C/event-loop/socket).

### 8.2. Bước `ML-C2`: Thử `--workers 2` trên 2 CPUs (`OMP=OPENBLAS=MKL=1`) `[Measured]`

| Concurrency | `ML-C1-2CPU` (`--workers 1`): RPS / HTTP p50 / HTTP p95 / CPU cores | `ML-C2-2CPU` (`--workers 2`): RPS / HTTP p50 / HTTP p95 / CPU cores | Chênh lệch (`C2` vs `C1`) |
|---:|---|---|---|
| **1** | `100.26` / `9.56 ms` / `12.13 ms` / `0.865c` | **`17.74`** / **`56.20 ms`** / **`58.65 ms`** / `0.251c` | **`-82.3%` RPS, `+46.6 ms` p50!** |
| **2** | `104.28` / `18.89 ms` / `23.39 ms` / `1.033c` | **`35.12`** / **`56.97 ms`** / **`59.43 ms`** / `0.496c` | **`-66.3%` RPS, `+38.1 ms` p50!** |
| **4** | `108.33` / `36.45 ms` / `49.60 ms` / `1.095c` | `62.13` / `61.82 ms` / `75.01 ms` / `0.793c` | `-42.6%` RPS, `+25.4 ms` p50 |
| **8** | `111.62` / `71.70 ms` / `100.61 ms` / `1.122c` | `112.25` / `69.41 ms` / `88.88 ms` / `1.179c` | Hòa (`+0.6%`) |
| **12** | `109.76` / `111.50 ms` / `152.71 ms` / `1.122c` | `151.19` / `78.93 ms` / `105.83 ms` / `1.585c` | `+37.7%` RPS |
| **24** | `106.85` / `231.39 ms` / `304.71 ms` / `1.121c` | **`184.45`** / `129.43 ms` / `182.33 ms` / `1.976c` (`78.9%` thr) | **`+72.6%` RPS, gấp `~2.3×` RAM (`270–350 MiB`)** |

**Kết luận `ML-C2` (`[Measured]`):** Uvicorn multi-worker (`--workers 2`) chia sẻ một socket listen chung trong nhân OS và dùng cơ chế accept không cân bằng giữa 2 event loops dưới Linux/Python (`queue_wait p50 = 44.9–45.3 ms` tại `c=1..2` dù server inference chỉ `11.4 ms`), **phá hủy tail/median latency ở tải thấp-trung bình** (`18–62 RPS`) và tăng gấp >2× RAM (`350 MiB`). Vì mỗi Kubernetes Pod đã chạy độc lập mỗi Pod một listen socket riêng với memory nhẹ (`120–145 MiB`), **giữ `--workers 1` và scale ngang bằng nhiều Kubernetes Pods** (mỗi Pod 1 worker độc lập) là phương án tối ưu duy nhất không bị dính `accept` stall.

---

## 9. ML Service Code Optimization (`ML-K1` — Verified NumPy Fast-Path)

### 9.1. Chẩn đoán code bottleneck `[Code-inspection]` + `[Measured]`

Khi thực thi từng request đơn lẻ, hàm gốc [local_baseline.py](../../ml-service/inference/api/local_baseline.py#L516-L520) khởi tạo một `pd.DataFrame([body.features.model_dump()], columns=FEATURES)` hoàn toàn mới rồi chạy `Pipeline`:
1. `SensorInteractions.transform(df)`: kiểm tra schema/cột Pandas, clone DataFrame, tạo Series/array trung gian.
2. `ColumnTransformer`: tách subset DataFrame, kiểm tra tên cột trên `OneHotEncoder` + `StandardScaler` + `passthrough`, rồi ghép mảng mới.
3. Gọi `HistGradientBoostingClassifier.predict_proba(X)` — bản thân hàm Cython của sklearn `predict_proba` thực chất chỉ mất **`~1.8–2.1 ms`**, trong khi `pd.DataFrame` + `ColumnTransformer` chiếm tới **`~5–6 ms` (gần 70% CPU của worker thread)** và sinh rất nhiều object Python tạm thời gây áp lực GIL + GC!

### 9.2. Giải pháp tối ưu `K1` trong `ml-service/inference/api/local_baseline.py` `[Code-inspection]`

Trong [local_baseline.py](../../ml-service/inference/api/local_baseline.py#L66-L185):
- `_FastInferencePath.predict_proba_single(features)` đọc trực tiếp 6 thuộc tính đã được Pydantic validate (`Features` model) từ request body: tính đúng 3 derived features (`temperature_difference_k`, `mechanical_power_w`, `wear_torque_interaction` dùng `2 * math.pi / 60`), kiểm tra hữu hạn (`math.isfinite`), ghi trực tiếp vào `np.zeros((1, total_cols), dtype=np.float64)` theo offset/mean/scale trích xuất một lần lúc startup từ `ColumnTransformer.transformers_`, rồi gọi thẳng `estimator.predict_proba(x)`.
- **Không sửa `ml-service/shared/preprocessing/feature_interactions.py` hay `pipeline.joblib`** -> toàn bộ SHA-256 verification tại startup giữ nguyên 100%.
- **An toàn tuyệt đối (`Fail-safe parity & fallback`)**: `_build_verified_fast_path(pipeline)` chỉ kích hoạt fast-path sau khi kiểm tra cấu trúc pipeline và đối chiếu `abs(pipeline.predict_proba(df) - fast_path.predict_proba_single(f)) <= 1e-12` trên cả 3 profile `"L"`, `"M"`, `"H"`. Nếu bất kỳ cấu trúc lạ/mock pipeline hoặc sai số vượt `1e-12` xảy ra, tự động fallback về `pd.DataFrame` pipeline gốc. đồng thời thực hiện `10` lượt warm-up trước khi `/health/ready` mở.
- Unit test parity & fallback được bổ sung vào [test_local_baseline.py](../../ml-service/inference/tests/test_local_baseline.py) -> **`34/34 PASSED`** (`[Measured]`).

### 9.3. Kết quả đo `ML-K1` trên Container Độc lập (`maintai-ml-bench:hgb-opt`, `OMP=OPENBLAS=MKL=1`, `--workers 1`) `[Measured]`

| Limit & Concurrency | `ML-C1` (DataFrame): RPS / HTTP p50 / HTTP p95 / Server p95 / CPU cores | **`ML-K1` (NumPy Fast-Path): RPS / HTTP p50 / HTTP p95 / Server p95 / CPU cores** | Mức cải thiện (`K1` vs `C1`) | Throttling (`K1`) |
|---|---|---|---|---|
| **1 CPU, `c=1`** | 93.77 / 10.43 ms / 12.27 ms / 8.62 ms / 0.866c | **180.74 / 5.40 ms / 6.64 ms / 3.36 ms / 0.780c** | **+92.8% RPS, -45.9% HTTP p95, -61.0% Server p95** | `0 / 600 (0.0%)` |
| **1 CPU, `c=2`** | 93.87 / 20.71 ms / 28.33 ms / 19.74 ms / 0.999c | **183.91 / 10.23 ms / 17.19 ms / 9.28 ms / 0.999c** | **+95.9% RPS, -39.3% HTTP p95** | `587 / 600 (97.8%)` |
| **1 CPU, `c=4`** | 98.01 / 39.63 ms / 59.49 ms / 39.78 ms / 1.000c | **179.02 / 19.83 ms / 38.78 ms / 25.59 ms / 1.000c** | **+82.7% RPS, -34.8% HTTP p95** | `599 / 600 (99.8%)` |
| **1 CPU, `c=8`** | 98.66 / 81.59 ms / 115.56 ms / 69.78 ms / 1.000c | **178.23 / 43.52 ms / 69.06 ms / 40.18 ms / 1.000c** | **+80.6% RPS, -40.2% HTTP p95** | `600 / 600 (100.0%)` |
| **2 CPU, `c=1`** | 100.26 / 9.56 ms / 12.13 ms / 8.41 ms / 0.865c | **180.06 / 5.41 ms / 6.60 ms / 3.29 ms / 0.781c** | **+79.6% RPS, -45.6% HTTP p95** | `0 / 600 (0.0%)` |
| **2 CPU, `c=2`** | 104.28 / 18.89 ms / 23.39 ms / 16.49 ms / 1.033c | **193.50 / 10.25 ms / 13.10 ms / 7.43 ms / 1.057c** | **+85.6% RPS, -44.0% HTTP p95** | `0 / 600 (0.0%)` |
| **2 CPU, `c=4`** | 108.33 / 36.45 ms / 49.60 ms / 32.50 ms / 1.095c | **210.66 / 18.88 ms / 26.12 ms / 14.63 ms / 1.175c** | **+94.5% RPS, -47.3% HTTP p95** | `0 / 601 (0.0%)` |
| **2 CPU, `c=8`** | 111.62 / 71.70 ms / 100.61 ms / 60.39 ms / 1.122c | **215.82 / 36.74 ms / 52.72 ms / 26.35 ms / 1.223c** | **+93.4% RPS, -47.6% HTTP p95** | `0 / 600 (0.0%)` |
| **2 CPU, `c=24`** | 106.85 / 231.39 ms / 304.71 ms / 159.44 ms / 1.121c | **212.29 / 112.68 ms / 160.07 ms / 63.53 ms / 1.230c** | **+98.7% RPS, -47.5% HTTP p95** | `0 / 600 (0.0%)` |

Độ tăng throughput toàn cục: **gấp `~2.02×` so với `ML-0` (`89.8 -> 180.7 RPS` ở `c=1`, `89.0 -> 178.2 RPS` ở `c=8`, 1 CPU)** và đỉnh **`215.82 RPS` trên 1 Pod ở 2 CPUs**.

---

## 10. ML Service Scaling Benchmark (`ML-S1`) và Batch Inference Micro-Benchmark (`ML-B1`)

### 10.1. Horizontal Pod Scaling trên k3d (`ML-S1`, `A1` `16/1/16`, `15s warmup + 60s measure`) `[Measured]`

Chạy thực tế trên cluster k3d với `maintai-ml-bench:hgb-opt`, `OMP=OPENBLAS=MKL=1`, `--workers 1`, thay đổi `ml-service-shadow` từ `1` đến `4` Pods (`100.0%` SSE coverage ở cả 4 điểm đo, `0` violations):

| Replicas (ML) | Drain (s) | SSE Cov (%) | `sample->biz` p50/p95/p99 (ms) | `sample->ui` p50/p95/p99 (ms) | Cycle UI p95 (ms) | Spread 50 UI p95 (ms) | ML Srv Prom p50/p95/p99 (ms) | ML Cli Prom p50/p95/p99 (ms) | Phân bố Requests giữa các Pods ML (Skew) | ML CPU Total (cores) | ML RAM Total (MiB mean) |
|---:|---:|---:|---|---|---:|---:|---|---|---|---:|---:|
| **1 Pod** | 1.152 | 100.0% | 494.8 / 647.7 / 700.2 | 566.4 / 726.4 / 779.8 | 802.9 | 685.0 | 48.9 / 209.0 / 243.6 | 184.1 / 453.3 / 490.7 | `3,045` (100%) | 0.489 | 134.8 |
| **2 Pods** | 1.047 | 100.0% | 385.2 / 544.8 / 612.2 | 494.7 / 625.4 / 692.5 | 712.9 | 560.0 | 24.9 / 94.7 / 195.7 | 68.7 / 227.7 / 245.9 | `1,614` (`53.1%`) / `1,428` (`46.9%`) | 0.544 | 252.0 |
| **3 Pods** | **0.925** | 100.0% | 384.3 / 521.2 / 571.0 | 484.6 / **599.6** / 664.9 | **644.2** | **552.0** | 16.0 / **49.9** / 90.9 | 44.3 / **147.5** / 229.5 | `1,478` (`48.6%`) / `961` (`31.6%`) / `603` (`19.8%`) | 0.522 | 372.4 |
| **4 Pods** | 1.054 | 100.0% | 388.7 / 516.0 / 546.5 | 489.7 / 593.5 / 624.7 | 639.2 | 539.0 | 11.4 / 43.4 / 67.6 | 38.7 / 93.3 / 189.9 | `872 / 846 / 719 / 605` (`28.6%..19.9%`) | 0.510 | 510.5 |

**Nhận xét chọn `ML-OPT = 3 Pods` cho Round 2 `[Measured]`:**
- Lên từ `1 -> 2 Pods` cắt `sample_to_ui p95` `-101.0 ms` (`726.4 -> 625.4 ms`) và `ml_cli p95` `-225.6 ms` (`453.3 -> 227.7 ms`).
- Lên từ `2 -> 3 Pods` tiếp tục giảm mạnh `ml_srv p95` gần một nửa (`94.7 -> 49.9 ms`), `ml_cli p95` (`227.7 -> 147.5 ms`), và `cycle_completion_ui p95` (`712.9 -> 644.2 ms`), giữ tổng RAM vừa phải (`~372–389 MiB`).
- Lên **`3 -> 4 Pods` lợi ích giảm dần rõ rệt (diminishing returns)**: `sample_to_ui p95` chỉ giảm thêm `6.1 ms` (`599.6 -> 593.5 ms`) và `cycle_completion_ui p95` giảm `5.0 ms` (`644.2 -> 639.2 ms`), trong khi tốn thêm 1 Pod (+138 MiB RAM). Vì vậy **`3 Pods` với `hgb-opt` (`ML-OPT`)** là điểm tối ưu cân bằng giữa tail latency và tài nguyên để đối chiếu Round 2 (`A2` vs `B2`).

### 10.2. Batch Inference Micro-Benchmark độc lập (`ML-B1` — §6.2.C) `[Measured]`

Chạy bằng [ml_batch_microbenchmark.py](../../scripts/benchmarks/burst_keyed_vs_hybrid/ml_batch_microbenchmark.py) (`300` iterations mỗi mức sau warmup) trên chính artifact `hgb-interactions-m56w5ff4` (đối chiếu độ lệch xác suất `max_abs_diff = 0.0 <= 1e-12`):

| Batch Size | DataFrame Pipeline: Batch Mean / p95 (ms) | DF Per-Sample (ms) | DF Throughput (samples/s) | NumPy Fast-Path: Batch Mean / p95 (ms) | NP Per-Sample (ms) | **NP Throughput (samples/s)** |
|---:|---|---:|---:|---|---:|---:|
| **1** | 5.233 / 5.800 | 5.233 | 191.1 | 2.019 / 2.285 | 2.019 | **495.2** |
| **4** | 5.146 / 5.865 | 1.287 | 777.3 | 1.801 / 2.029 | 0.450 | **2,221.6** |
| **8** | 5.261 / 6.132 | 0.658 | 1,520.7 | 1.822 / 2.039 | 0.228 | **4,390.3** |
| **16** | 5.344 / 6.212 | 0.334 | 2,994.2 | 2.163 / 2.599 | **0.135** | **7,397.4** |

**Đánh giá kiến trúc (`[Analytical estimate]` + `[Code-inspection]`):** Micro-batching ở ML (`batch=16`) đẩy hiệu năng thô lên **`7,397 samples/s`** (`0.135 ms/mẫu` — gấp `~15×` single-sample throughput trên 1 core) vì chi phí Cython/NumPy dispatch cố định được phân bổ đều trên 16 hàng. Tuy nhiên, theo yêu cầu kiểm thử (§3 & §6.2.C), **chúng tôi không gộp HTTP Batch vào Round 2** để tránh pha trộn thêm độ trễ gom lô (`batch collect window`) làm sai lệch đánh giá scheduler `Keyed FIFO` vs `Hybrid`. Nếu tải trong tương lai tăng lên 200–1,000+ RPS, thêm cơ chế **server-side opportunistic micro-batching** (hoặc Processing vectorized batch endpoint) sẽ mang lại bước nhảy hiệu năng lớn hơn bất kỳ thay đổi scheduler nào.

---

## 11. Round 2 Scheduler Benchmark (`A2` vs `B2` trên Identical `ML-OPT`)

Round 2 dùng chính xác **`ML-OPT`**: `ml-service-shadow` ảnh `maintai-ml-bench:hgb-opt`, `3` Pods (`min=max=3`), `--workers 1`, `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1`. Mọi tham số Worker, Outbox Relay, Business Consumer giữ nguyên y hệt Round 1 (`6` worker, `2` outbox, `4` biz, `batch=16/25ms`). Mỗi preset chạy **3 lần độc lập × (30s warmup + 300s measure)**.

### 11.1. Chi tiết từng lần chạy (Round 2) `[Measured]`

| Preset | Run | Cycles / Sent | Drain (s) | SSE Cov (%) | `sample->biz` p50/p95/p99 (ms) | `sample->ui` p50/p95/p99 (ms) | First UI p50 (ms) | Cycle UI p95 (ms) | Spread 50 UI p95 (ms) | ML Srv p95 ts/Prom (ms) | ML Cli p95 Prom (ms) | ML CPU (cores) | PW CPU (cores) | Viol |
|---|---:|---:|---:|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| **A2** | 1 | 301 / 16550 | 0.903 | 89.15% | 387.5 / 536.2 / 597.8 | 494.7 / 621.0 / 679.3 | 93.1 | 693.9 | 587.8 | 53.6 / 68.4 | 183.9 | 0.535 | 1.307 | 0 |
| **A2** | 2 | 301 / 16550 | 1.047 | 89.29% | 390.3 / 533.6 / 586.1 | 499.1 / 616.2 / 671.0 | 98.6 | 675.3 | 574.2 | 58.8 / 77.2 | 196.9 | 0.543 | 1.312 | 0 |
| **A2** | 3 | 301 / 16550 | 1.053 | 89.06% | 383.9 / 525.2 / 579.7 | 490.6 / 603.5 / 663.2 | 94.5 | 672.2 | 563.8 | 50.1 / 62.5 | 173.7 | 0.536 | 1.310 | 0 |
| **B2** | 1 | 301 / 16550 | 1.335 | 89.34% | 382.5 / 529.6 / 583.3 | 498.9 / 611.6 / 663.0 | 95.7 | 669.8 | 560.6 | 51.8 / 64.9 | 142.3 | 0.541 | 1.300 | 0 |
| **B2** | 2 | 300 / 16550 | 1.324 | 89.00% | 385.6 / 531.2 / 576.7 | 500.1 / 615.0 / 658.2 | 95.3 | 676.5 | 569.4 | 48.6 / 58.2 | 114.5 | 0.539 | 1.308 | 0 |
| **B2** | 3 | 301 / 16550 | 1.609 | 89.08% | 388.7 / 534.7 / 580.6 | 504.0 / 617.3 / 671.7 | 96.6 | 674.6 | 564.6 | 43.1 / 48.7 | 94.8 | 0.537 | 1.310 | 0 |

*(Ghi chú Run 2 của B2: có 300 chu kỳ đầy đủ nằm trong cửa sổ 300.0s thay vì 301 chu kỳ như các run khác do pha đồng hồ đầu/cuối chu kỳ thứ 301 rơi sát biên; tổng events gửi `total_sent = 16,550` và drain `100%` giống hệt các run còn lại `[Measured]`.)*

### 11.2. Đối chiếu Round 1 vs Round 2 (`A1 -> A2`, `B1 -> B2`, và `A2` vs `B2`) `[Measured]`

| Metric Aggregate (3 runs) | `A1 -> A2` (Hiệu ứng Tối ưu ML) | `B1 -> B2` (Hiệu ứng Tối ưu ML trên Hybrid) | **`B2` vs `A2` (Keyed vs Hybrid sau tối ưu ML)** |
|---|---|---|---|
| `sample_to_ui` p50 (ms) | `-122.8 ms` (`-19.9%`) | `-128.2 ms` (`-20.4%`) | **`+6.2 ms`** (`501.0 vs 494.8`) — A2 tốt hơn nhẹ |
| `sample_to_ui` p95 (ms) | **`-167.5 ms` (`-21.4%`)** | **`-182.4 ms` (`-22.9%`)** | **`+1.1 ms`** (`614.6 vs 613.6`) — Hòa |
| `sample_to_ui` p99 (ms) | `-169.7 ms` (`-20.2%`) | `-196.4 ms` (`-22.8%`) | `-6.8 ms` (`664.3 vs 671.1`) — nhiễu giữa các run |
| `sample_to_biz` p95 (ms) | `-172.7 ms` (`-24.5%`) | `-187.9 ms` (`-26.1%`) | **`+0.1 ms`** (`531.8 vs 531.7`) — Hòa hoàn toàn |
| `first_prediction_ui` p50 (ms) | `-43.7 ms` (`-31.4%`) | `-55.9 ms` (`-36.8%`) | `+0.5 ms` (`95.9 vs 95.4`) — Hòa |
| `cycle_completion_ui` p95 (ms) | `-173.6 ms` (`-20.3%`) | `-195.5 ms` (`-22.5%`) | `-6.8 ms` (`673.6 vs 680.5`) — nhiễu (~1%) |
| `spread_50_ui` p95 (ms) | `-145.7 ms` (`-20.2%`) | `-164.3 ms` (`-22.5%`) | `-10.4 ms` (`564.9 vs 575.3`) — nhiễu giữ kết nối pod |
| ML server duration p95 (ms) | `-194.4 ms` (`-78.2%`) | `-203.6 ms` (`-81.0%`) | `-6.4 ms` (do ClusterIP keep-alive skew ngẫu nhiên, xem §14) |
| ML Pod CPU (cores) | `-0.449 cores` (`-45.6%`) | `-0.454 cores` (`-45.7%`) | `+0.003 cores` (Hòa) |

**Tại sao `ml_client p95` và `spread_50_ui` lệch nhẹ giữa `A2` (`184.8 ms` / `575.3 ms`) và `B2` (`117.2 ms` / `564.9 ms`)?** Xem bảng phân bố requests theo Pod ở §14: trong `A2` Run 1–2, một Pod ML nhận **`~47–49%`** lưu lượng từ 6 worker giữ HTTP/1.1 keep-alive (ClusterIP TCP pinning), kéo `ml_srv p95` của Pod nóng đó lên `81.7–86.8 ms` so với `39–49 ms` của 2 Pod còn lại. Trong `B2` Run 3 (khi keep-alive rải đều hơn `32.7%/39.8%/27.4%`), `ml_client p95` xuống tới **`94.8 ms`** cho cả A và B! Bản chất khác biệt đó đến từ **TCP load-balancing skew**, không phải thuật toán `Hybrid`.

---

## 12. End-to-End Latency Breakdown (9 mốc Pipeline -> Browser UI SSE) `[Measured]`

Bảng phân rã từng chặng từ giá trị **median của 3 runs** trên mỗi preset (tính theo ms):

| Chặng đo (`latency_ms`) | A0 (`8/1/8`) | A1 (`16/1/16`) | B1 (`16/4/16`) | A2 (`16/1/16`, OPT) | B2 (`16/4/16`, OPT) | Quan sát chính |
|---|---:|---:|---:|---:|---:|---|
| 1. `sample_to_edge_spool` (p50 / p95) | 4.2 / 5.8 | 4.2 / 5.5 | 4.1 / 5.9 | 4.2 / 6.0 | 4.2 / 5.8 | Ổn định ~4–6 ms |
| 2. `edge_to_ingestion_ack` (p50 / p95) | 183.7 / 267.2 | 185.6 / 270.9 | 186.7 / 274.8 | 197.3 / 296.4 | 199.5 / 299.1 | Burst 50 HTTP/TLS requests |
| 3. `sample_to_ingestion_ack` (p50 / p95) | 188.0 / 271.6 | 189.8 / 275.8 | 191.1 / 279.5 | 201.9 / 301.1 | 203.9 / 304.2 | Chiếm ~33–40% UI p50 ở Round 2 |
| 4. `kafka_arrival_spread_per_cycle` (p50 / p95) | 223.0 / 309.0 | 225.0 / 317.0 | 226.0 / 318.0 | 242.0 / 343.0 | 246.0 / 341.0 | Đầu vào tới Processing rải qua ~0.24–0.34 s |
| 5. `kafka_to_hybrid_admit` (p50 / p95) | — | — | 11.9 / 62.7 | — | 22.8 / 140.5 | Chờ poll + Stage A fencing/admission |
| 6. `ml_inference_server` ts (p50 / p95) | 83.3 / 240.7 | 92.3 / 247.6 | 94.1 / 249.2 | 14.1 / 53.6 | 12.4 / 48.6 | **Cắt -78%..-81% p95 ở Round 2!** |
| 7. `ack_to_proc_db` (p50 / p95) | 342.2 / 467.1 | 354.6 / 496.7 | 355.0 / 495.4 | 154.1 / 298.9 | 146.1 / 288.5 | **Cắt ~200 ms nhờ K1 + 3 Pods ML** |
| 8. `proc_db_to_outbox` (p50 / p95) | 49.8 / 119.1 | 46.5 / 113.0 | 53.7 / 123.0 | 69.9 / 147.2 | 75.9 / 147.2 | Poll 10ms + OrderedOutboxRelay TX |
| 9. `outbox_to_business_db` (p50 / p95) | 2.1 / 14.5 | 2.0 / 13.8 | 2.1 / 15.5 | 4.1 / 24.1 | 4.3 / 29.4 | Kafka -> BusinessConsumer TX |
| 10. `business_db_to_browser_sse` (p50 / p95) | 75.8 / 99.3 | 76.3 / 102.6 | 76.5 / 101.7 | 89.7 / 143.7 | 95.1 / 151.4 | PG NOTIFY -> Business API SSE -> Playwright |
| **E2E `sample_to_business_db`** (p50 / p95 / p99) | 529.8 / 683.5 / 734.0 | 534.4 / 700.7 / 766.2 | 551.9 / 714.2 / 765.8 | **387.5 / 533.6 / 586.1** | **385.6 / 531.2 / 580.6** | `100.0%` events mỗi run |
| **E2E `sample_to_browser_sse_ui`** (p50 / p95 / p99) | 611.1 / 760.9 / 813.9 | 619.5 / 780.2 / 845.9 | 635.3 / 792.4 / 843.2 | **494.7 / 616.2 / 671.0** | **500.1 / 615.0 / 663.0** | `~89.2%` SSE window (~13,420 ev/run) |

### Trả lời trực diện câu hỏi về trải nghiệm UI (`[Measured]`)
1. **Prediction đầu tiên trên UI (`first_prediction_browser_ui p50`):** Tối ưu ML rút từ `139.1 ms (A1)` / `151.8 ms (B1)` xuống **`95.4 ms (A2)` / `95.9 ms (B2)`** (`-31%..-37%`), nhưng **Hybrid không làm prediction đầu tiên xuất hiện sớm hơn Keyed FIFO** (`+0.5 ms`, hòa).
2. **Độ dàn trải 50 máy trên UI (`spread_50_machines_browser_ui p95`):** Giảm từ `~721–729 ms` (Round 1) xuống `575.3 ms (A2)` và `564.9 ms (B2)` (Round 2). Với từng chu kỳ không gặp Pod skew bất lợi, cả A2 và B2 đều đạt `~564–575 ms`.
3. **Hoàn tất cả 50 máy một chu kỳ (`cycle_completion_browser_ui p95`):** Rút từ `854.1 ms (A1)` / `869.1 ms (B1)` xuống **`680.5 ms (A2)` / `673.6 ms (B2)` (`-20.3%..-22.5%`)**. Thời gian từ lúc sinh mẫu tới lúc cả 50 máy hiện đầy đủ trên trình duyệt đều nằm gọn dưới **0.68 s ở p95** (lọt thoải mái trong chu kỳ 1.000 s tiếp theo).

---

## 13. Kafka và Processing Worker Analysis

### 13.1. Phân bố Partition và Lag `[Measured]`
- Cả 15 runs trong Round 1 + Round 2 dùng đủ **`12/12` Kafka partitions** của `sensor.readings`.
- `kafka_max_lag` tối đa quan sát trong toàn bộ chu kỳ đo trên cả 4 consumer groups (`local-processing-v1`, `local-business-prediction-v1`, `local-business-live-v1`, `local-business-alert-v1`) = **`0`** (do sampling lag mỗi vài giây luôn bắt kịp khi tốc độ xử lý vượt `50 RPS`).
- `paused_partitions_peak = 0` và `queued_records_peak = 0.0` ở `A1`, `B1`, `A2`, `B2` (chỉ có `A0` ghi nhận `queued_records_peak = 5.0–6.0` records vì `active_keys = 8` làm giới hạn hàng đợi nội bộ chạm nhẹ khi một pod gánh > 8 máy).

### 13.2. Gauge `active_keys_peak`, `inflight_tasks_peak`, `max_per_key_inflight_peak` `[Measured]`
- Tổng cộng trên 6 Pods Worker:
  - `A0` (`max_active=8/pod`, tổng tối đa 48): peak thực tế **`43–44`**, `max_per_key_inflight_peak = 1`.
  - `A1` (`max_active=16/pod`, tổng tối đa 96): peak thực tế **`47–49`**, `max_per_key_inflight_peak = 1`.
  - `A2` (`16/1/16` + ML nhanh hơn `4×`): peak đồng thời chỉ cần **`36–37`**.
  - `B2` (`16/4/16` + ML nhanh hơn `4×`): peak đồng thời chỉ **`28–30`**, và quan trọng nhất: **`max_per_key_inflight_peak = 1` ở mọi run B1 và B2**! Nghĩa là trên workload 50 máy × 1 Hz, không có hai reading nào cùng `machine_id` bị dồn vào ML song song cùng một lúc.

### 13.3. Giải thích chính xác chỉ số `Queue Wait` trong Prometheus (`[Measured]` + `[Code-inspection]`)
- `maintai_processing_queue_wait_seconds` sử dụng bucket histogram Prometheus mặc định, trong đó **bucket thấp nhất là `le="0.05"` (`50 ms`)**, bucket tiếp theo là `le="0.1"` (`100 ms`).
- Khi phép nội suy tuyến tính Prometheus tính p50/p95 trên một histogram mà `> 99%` mẫu rơi dưới `50 ms`, công thức tuyến tính tất yếu cho ra con số sàn **`p50 = 25.0 ms`** (`0.5 × 50 ms`) và **`p95 = 47.5 ms`** (`0.95 × 50 ms`).
- **Số đo chính xác nhất là `mean_ms` (`_sum / _count`):**
  - `A0`: `mean = 6.95–8.71 ms`, `p99 = 200.2–235.5 ms` (**chứng tỏ `max_active_keys=8` có gây queue cục bộ ở ~1–5% tail khi một worker nhận >8 máy!**).
  - `A1`: **`mean = 0.32–0.39 ms`**, `p99 = 49.5 ms` (**nâng `max_active_keys = 16` triệt tiêu hoàn toàn tail queue wait cục bộ trên 6 pods/12 partitions!**).
  - `B1`: `mean = 0.69–0.73 ms` (`p99 = 49.5 ms`).
  - `A2`: `mean = 0.78–0.81 ms` (`p99 = 49.5 ms`).
  - `B2`: `mean = 2.31–2.48 ms` (`p99 = 49.5 ms` — tăng nhẹ từ `~0.8 -> ~2.4 ms` do Stage A admission DB TX chiếm poll/admit slot).

---

## 14. ML Bottleneck Analysis: 3 Tầng Nguyên nhân Thực tế `[Measured]`

1. **Tầng 1 — Python GIL + Threadpool + OpenMP/BLAS (`ML-0` -> `ML-C1`):**
   - Trong `def predict(...)`, FastAPI điều phối request vào 40 worker threads chia sẻ một Python GIL. Khi `OMP/OPENBLAS` không bị khóa `=1`, nhiều thread native cùng thức dậy cho tác vụ rất nhỏ (`1 × N`), tiêu tốn Host CPU tới `23.9%`. Khóa `=1` khôi phục `4.5–10.8%` RPS, nhưng **1 process Python bị GIL khóa trần ở `1.122 cores` và `111.6 RPS`** dù container được cấp 2 CPUs.
2. **Tầng 2 — Single-Row `pd.DataFrame` + `ColumnTransformer` Allocation (`ML-C1` -> `ML-K1`):**
   - Với batch = 1, việc dựng DataFrame, lookup cột, chạy `SensorInteractions` + `ColumnTransformer` ngốn **`~5–6 ms` (gần 70% CPU)** chỉ để tính 1 lần `predict_proba` vốn chỉ tốn **`~2 ms`**. Thay thế bằng `_FastInferencePath` zero-DataFrame thuần NumPy cắt thời gian xử lý server từ **`7.18 ms -> 2.56 ms` p50** (`c=1`) và đẩy throughput lên **`2.02×` (`180.7–183.9 RPS` @ 1 CPU; `215.8 RPS` @ 2 CPUs)**.
3. **Tầng 3 — HTTP/1.1 Keep-Alive Connection Pinning qua Kubernetes `ClusterIP` (`ML-S1` & Round 1/2) `[Measured]`:**
   - Do [worker main.py](../../processing/worker/src/consumer/main.py#L56) dùng `httpx.AsyncClient(trust_env=False)` mặc định HTTP/1.1 keep-alive tới ClusterIP `http://ml-service-shadow:8001`, kết nối TCP sống dai (`keepalive_expiry=5.0s`) giữa 6 Worker Pods với `2–3` ML Pods không tự rebalance giữa từng request:
     - `A0 Run 3`: Pod `94fjg` nhận **`5,646` req (`mean 74.3 ms`)** vs `cv64c` nhận **`9,402` req (`62.5%`, `mean 138.7 ms`, `p95 400.1 ms`)**.
     - `ML-S1 (3 Pods)`: **`1,478` (`48.6%`) / `961` (`31.6%`) / `603` (`19.8%`)**.
     - `A2 Run 2`: **`7,326` (`48.7%`) / `4,185` (`27.8%`) / `3,536` (`23.5%`)** -> khiến Pod nóng đạt `p95 = 86.8 ms` so với Pod mát `45.6 ms`!
   - **Khuyến nghị tiếp theo (`[Analytical estimate]`):** Có thể cân bằng đều hơn qua L7 (Envoy/Linkerd/nginx), hoặc cấu hình `httpx.Limits` / rotation `Connection: close` định kỳ hay `--limit-max-requests` phía HTTP pool mà vẫn kiểm soát chi phí TCP handshake.

---

## 15. DB, Outbox Relay và Business Backend Analysis `[Measured]`

| Thành phần & Chỉ số (3 runs / 5 phút mỗi run) | `A0` (`Keyed 8`) | `A1` (`Keyed 16`) | `B1` (`Hybrid 16/4`) | `A2` (`Keyed 16`, OPT) | `B2` (`Hybrid 16/4`, OPT) |
|---|---:|---:|---:|---:|---:|
| Hybrid Stage A `admission` Prom p50 / p95 / p99 (ms) | — | — | 7.2 / **23.9** / 43.7 | — | 13.5 / **46.8** / 80.4 |
| Hybrid Stage C `persist` Prom p50 / p95 / p99 (ms) | — | — | 33.4 / **95.5** / 199.9 | — | 48.8 / **171.7** / 234.3 |
| `processing_db` `xact_commit` delta (mỗi 5m run) | `150,345–150,596` | `150,138–150,683` | `116,817–117,163` | `146,795–146,997` | `113,171–113,348` |
| `business_db` `xact_commit` delta (mỗi 5m run) | `42,664–42,756` | `42,650–42,862` | `42,729–42,906` | `41,487–41,815` | `41,484–41,502` |
| PG Lock Wait Samples (`> 0 ms`) / Deadlocks | **0 / 0** | **0 / 0** | **0 / 0** | **0 / 0** | **0 / 0** |
| Peak PG Conns / Active (`proc` \| `biz`) | 56/1 \| 40/1 | 63/4 \| 43/4 | 60/1 \| 43/4 | 63/2 \| 43/4 | 58/4 \| 43/4 |
| PG CPU mean (`kubectl top`) | `645–688 m` | `670–701 m` | `754–780 m` | `739–756 m` | `793–853 m` |
| `processing-outbox-relay` CPU (`2` Pods) | `388–405 m` | `392–407 m` | `397–413 m` | `406–419 m` | `407–423 m` |
| `proc_db_to_outbox` latency p50 / p95 (ms) | 49.8 / 119.1 | 46.5 / 113.0 | 53.7 / 123.0 | 69.9 / 147.2 | 75.9 / 147.2 |
| `business-consumer` CPU (`4` Pods) | `617–654 m` | `622–660 m` | `628–668 m` | `673–711 m` | `690–724 m` |
| `business_db_to_browser_sse` p50 / p95 (ms) | 75.8 / 99.3 | 76.3 / 102.6 | 76.5 / 101.7 | 89.7 / 143.7 | 95.1 / 151.4 |

**Diễn giải kỹ thuật quan trọng:**
1. **Tại sao `Stage A/C` tăng từ `B1` (`23.9 / 95.5 ms` p95) lên `B2` (`46.8 / 171.7 ms` p95) và `proc_db_to_outbox` tăng từ `~50 ms` lên `~75 ms` trong Round 2 (`[Measured]` + `[Analytical estimate]`)?**
   - Ở Round 1, ML bị chậm (`~250 ms` p95) đóng vai trò như một **van tiết lưu tự nhiên**, rải đều thời điểm completion của các worker về DB/Outbox/Business trong suốt chu kỳ.
   - Ở Round 2, 3 ML Pods xử lý cực nhanh (`~14 ms` p50), khiến cả 50 readings mỗi giây **dồn sóng đồng bộ** vào PostgreSQL, Outbox Relay (`2` pods), và `PG NOTIFY` -> `Business API` cùng một cửa sổ hẹp — đẩy PG CPU lên `~750–853 m`, `proc_db_to_outbox p95` từ `~113 -> ~147 ms`, và `business_db_to_browser_sse p95` từ `~102 -> ~151 ms` (tuy vẫn an toàn tuyệt đối vì `lock_waits = 0` và `deadlocks = 0`).
2. **Tại sao `xact_commit` trên `processing_db` ở `B1/B2` (`~113k–117k`) thấp hơn `A1/A2` (`~147k–150k`) dù Hybrid có thêm TX `admit()`?** `[Code-inspection]` + `[Analytical estimate]`: ở `A0/A1/A2`, `KeyedProcessingHandler` gọi `self._durable_replay` + `prepare` (mỗi hàm mở 1 read-only `connection()` PG kiểm tra cached terminal -> cộng gộp từ `~7` short TXs + outbox claim per event). Ở `B1/B2`, `_handle_hybrid` gộp admission vào poll thread ticket nên bớt mở `connection` read rời cho các event mới không replayed, mặc dù PG CPU cao hơn `+10–14%` do thực hiện nhiều SQL cập nhật `processing_alert_ledger` + `processing_admission_cursor` + advisory locks.
3. **Caveat lớn nhất cần công khai (`[Code-inspection]`):** Theo [postgres.yaml](../../../MaintAI-gitops/local/platform/postgres.yaml#L47), PostgreSQL 17 trên `k3d` cấu hình `-c synchronous_commit=off`! Khi chạy production RDS/Patroni bật `synchronous_commit=on` (`fsync` commit WAL đồng bộ trên mạng multi-AZ), mỗi giao dịch commit sẽ tăng `+1–4 ms`: chi phí commit thêm của Hybrid (`Stage A` + `Stage C` + cập nhật ledger + `pg_advisory_xact_lock`) sẽ lộ rõ rệt hơn nhiều so với `Keyed FIFO`!

---

## 16. Prediction Ordering Correctness (§9.1) `[Measured]`

Chứng minh qua 3 lớp bằng chứng độc lập:

1. **Tích hợp thật PG17 + Kafka 4.0.0 ([test_hybrid_ordering.py](../../processing/worker/tests/test_hybrid_ordering.py#L655-L823)) — `test_r101_critical_slow_and_r102_normal_fast_end_to_end_ordering`**:
   - Bơm 2 readings liên tiếp của `M-001` là `R101 (seq=101, prob=0.92 -> CRITICAL)` và `R102 (seq=102, prob=0.08 -> NORMAL)`. Dùng `threading.Barrier(2)` + `r102_persisted` ép `R102` hoàn tất ML và commit DB trước `R101`.
   - **Xác minh điểm giữa kỳ (lúc R102 xong nhưng R101 đang chờ):** `maintai_kafka_outbox(topic='inference.results')` chỉ mới chứa `102`; `alert.signals` = **`0` rows**; `processing_alert_ledger` giữ `(101, 'PENDING')`, `(102, 'NO_SIGNAL')`.
   - **Xác minh kết quả cuối cùng sau khi R101 hoàn tất:** `processing_results = 2`, `predictions = 2`, `processing_conflicts = 0`, `last_sequence = 102`, `last_admitted_sequence = 102`, `alert_frontier_seq = 2`, cả 2 readings nằm đúng trong lịch sử.
   - Đẩy lần lượt `inference.results` sai thứ tự (`[102 NORMAL, sau đó 101 CRITICAL]`) vào `Projector` của Business DB: **`ProjectionRepository.live('M-001')['latest_valid_prediction']` và `.detail('M-001')` giữ nguyên `sequence_number == 102`, `prediction == 'NORMAL'`** (KHÔNG bị `R101` đến trễ ghi đè lùi!).
   - Toàn bộ `processing/worker/tests/test_hybrid_ordering.py` + `backend/business-consumer/tests/test_alert_sequence_guard.py` (9 tests với `MAINTAI_M2_INTEGRATION=1`): **9/9 PASSED**.
2. **Toàn bộ `15 × 5 phút` + `4 × 1 phút (S1)` runs trên Live k3d:** `wrong_latest_sequence = 0`, `missing_outcomes = 0`, `proc_conflicts_delta = 0`, `biz_conflicts_delta = 0`, `bad_unavailable_downgrade = 0`.
3. **Live Pod-Kill Recovery trên Kubernetes k3d (`B2`, `15s warmup + 45s measure`)**:
   - **Kill `processing-worker` (`processing-worker-596d9b564d-5lfdl`) giữa burst:** `46` cycles (`3,100` readings gửi bao gồm warmup), pod hồi phục trong **`26.018 s`**, drain hoàn tất trong **`1.988 s`**, SSE coverage **`99.83%` (`2296/2300`)**, `sample_to_ui p95 = 686.0 ms` (vẫn dưới 1s dù rebalance! tail p99 `1015.2 ms`, max cycle rebalance `3,578.7 ms`), **`total_violations = 0`**, `kafka_max_lag = 0`.
   - **Kill `ml-service-shadow` (`ml-service-shadow-c9b58fc87-l8qqj`) giữa burst:** `46` cycles (`3,050` readings), hồi phục **`24.989 s`**, drain **`0.907 s`**, SSE coverage **`100.0%` (`2300/2300`)**, `sample_to_ui p95 = 645.9 ms`, `cycle_completion_ui p95 = 719.1 ms`, `2` pods ML còn lại tự gánh 100% tải nhờ 1-retry của `InferenceClient`, **`total_violations = 0`**.

---

## 17. Alert Lifecycle Correctness (§9.2) `[Measured]`

1. **Trong bài kiểm chứng `R101 CRITICAL (slow)` vs `R102 NORMAL (fast)` ([test_hybrid_ordering.py](../../processing/worker/tests/test_hybrid_ordering.py#L745-L820)):**
   - Khi `R102 NORMAL` xong trước `R101`, `AlertResequencer` **KHÔNG chốt hay phát `NO_SIGNAL` trước khi biết số phận của `R101`**. Ngay khi `R101 CRITICAL` xong sau, `drain_frontier()` khóa frontier tuần tự, đánh dấu `101 -> EMITTED` (sinh đúng `alert.signals` tại `dispatch_seq=1`) và `102 -> RESOLVED_NO_SIGNAL`.
   - Đẩy `alert.signals(101)` vào `AlertHandler` **sau** khi `latest_valid_prediction` đã cập nhật tới `seq=102 NORMAL`: bảng `alerts` ghi nhận **1 alert `CRITICAL` trạng thái `OPEN` với `latest_source_sequence = 101`**, `detail_view['active_alerts']` giữ đúng alert này **và KHÔNG tự động resolve sai bởi `R102 NORMAL` đã chiếu trước đó**.
2. **Trên toàn bộ runs k3d Round 1, Round 2, `S1`, và Recovery:**
   - Mỗi 5-phút run có đúng **`530` alert signals (`proc_alert_delta == biz_alert_delta == 530`)**, sinh/cập nhật `70–79` alert records, với **`stuck_ledger_rows = 0`**, **`pending_outbox_after_drain = 0`**, **`duplicate_active_alerts = 0`**, **`bad_unavailable_downgrade = 0`**, **`wrong_latest_sequence = 0`** ở tất cả `21` lần chạy k3d (`A0/A1/B1 × 3 + S1 × 4 + A2/B2 × 3 + Recovery × 2`).

---

## 18. Resource Utilization và Capacity Planning

### 18.1. Tài nguyên đo thực tế trên Local k3d (`8C/16T`, `15 GiB RAM`) `[Measured]`

| Thành phần (`Replicas`) | Round 1 (`A1` / `B1` CPU & RAM) | Round 2 (`A2` / `B2` CPU & RAM) | Ghi chú |
|---|---|---|---|
| **Host Total CPU (`%` of 16 vCPU)** | `53.5%` / `54.4%` | `51.7%` / `52.1%` | Cả cụm + benchmark client chạy chung 1 máy |
| `ml-service-shadow` (2p R1 / 3p R2) | `0.985c` / `0.993c` cgroup (`261–263 MiB`) | **`0.536c` / `0.539c` cgroup** (`384 MiB`) | Tiết kiệm **~45% CPU** dù chạy 3 Pods |
| `processing-worker` (`6` Pods) | `1.225c` / `1.262c` cgroup (`541–543 MiB`) | `1.310c` / `1.308c` cgroup (`544–546 MiB`) | Mỗi Pod chỉ ~0.22 cores, ~91 MiB |
| `business-consumer` (`4` Pods) | `~652–668 m` (`~312 MiB`) | `~708–724 m` (`~310 MiB`) | — |
| `postgres` (`1` Pod) | `~699–780 m` (`~954–975 MiB`) | **`~752–853 m` (`~973 MiB`)** | B2 ăn nhiều PG CPU hơn A2 ~10–13% |
| `processing-outbox-relay` (`2` Pods) | `~404–413 m` (`~180 MiB`) | `~419–423 m` (`~179 MiB`) | — |
| `ingestion-api` (`2` Pods) | `~368–373 m` (`~159–161 MiB`) | `~373–378 m` (`~165 MiB`) | — |
| `kafka` (`1` Pod) | `~202–205 m` (`~999 MiB`) | `~205–212 m` (`~1,000 MiB`) | — |

### 18.2. Capacity Planning cho Production `[Analytical estimate]` + `[NOT_MEASURED]`

Vì cluster benchmark là local single-node và PG chạy `synchronous_commit=off`, bảng sizing dưới đây là ước tính phân tích cho production (`synchronous_commit=on`, Kafka `RF=3`, Pod có `limits.cpu` rõ ràng):

| Workload mục tiêu | `ml-service` (`hgb-opt`, `--workers 1`, `OMP=1`) | `processing-worker` (`Keyed FIFO`) | Tài nguyên khuyến nghị mỗi Pod ML | **PROVISIONAL** SLO mục tiêu (kết thúc chu kỳ trên UI) |
|---|---|---|---|---|
| **50 máy × 1 Hz = 50 RPS** (Burst đồng bộ) | **3 Pods** (chịu mất 1 Pod vẫn còn 2 Pods giữ tải ~50 RPS ở `p95 < 100 ms`) | 6 Pods, `active_keys=16`, `per_key=1`, `total_inflight=16` | `requests: 500m CPU / 192Mi`, `limits: 1.5–2.0 CPU / 512Mi–1Gi` (tránh limit 1.0 CPU gây throttle khi burst c>1) | `< 1,000 ms` p95 (cần kiểm chứng lại trên RDS `synchronous_commit=on` — **`PROVISIONAL`**) |
| **100–200 máy × 1 Hz = 100–200 RPS** `[Analytical estimate]` | **4–6 Pods** (mỗi Pod đạt `180–215 RPS` bão hòa nhưng giữ utilization mục tiêu `<= 35–40%` tương đương `60–80 RPS/pod` để `http_p95 < 25 ms` dưới burst) | 6–12 Pods (`12` hoặc `24` partitions Kafka; giữ `active_keys=16–24`) | `requests: 500m CPU / 256Mi`, `limits: 2.0 CPU / 512Mi–1Gi` + xem xét L7 load balancing (Linkerd/Envoy) hoặc kết nối ngắn chu kỳ | `< 1,000 ms` p95 (**`[NOT_MEASURED]`**, **`PROVISIONAL`**) |
| **Hot-machine / Backlog replay burst trên ít máy** `[Analytical estimate]` | Tỷ lệ thuận RPS mục tiêu | Bật **`Hybrid`** (`per_key=4`, `active_keys=16`, `total_inflight=16`–`32`), đảm bảo PG Pool & IO chịu được 2 TX + ledger | Tương tự + tăng `storage/WAL IOPS` RDS | Không để 1 máy kẹt backlog làm nghẽn cả partition (**`[NOT_MEASURED]`** end-to-end multi-reading/machine) |

*(Tất cả SLO cho production được gắn nhãn **`PROVISIONAL`** vì chưa benchmark trên RDS `synchronous_commit=on` hay mạng đa AZ thật — **`[NOT_MEASURED]`**.)*

---

## 19. Performance vs Reliability Trade-off `[Measured]` + `[Code-inspection]`

- **`Keyed FIFO` (`active_keys=16, per_key=1, inflight=16`)**:
  - *Ưu điểm:* Chỉ 1 TX ghi/event trên `processing_db`, PG CPU thấp hơn Hybrid (`~740 m vs ~830 m`), thời gian drain nhanh hơn (`~0.9–1.05 s vs ~1.32–1.61 s`), không cần vận hành thêm bảng `processing_admission_cursor` / `processing_alert_ledger` / recovery cursor khi bootstrap history cũ, đơn giản khi debug sự cố.
  - *Nhược điểm:* Không có song song nội tại cho cùng `1 machine_id` -> nếu 1 máy đột ngột gửi burst `20` readings/s trong khi 49 máy khác im lặng, máy đó bị xử lý tuần tự 1-1.
- **`Hybrid Ordering` (`active_keys=16, per_key=4, inflight=16`)**:
  - *Ưu điểm:* Giữ chính xác tuyệt đối thứ tự UI (`latest_valid_prediction` monotonic theo `sequence_number`) và thứ tự alert (`AlertResequencer` barrier), mở song song theo `per_key` khi 1 máy bị backlog. Chống split-brain tốt khi kill pod giữa chừng (nhờ DB fencing + ledger).
  - *Nhược điểm:* Tốn 2 pha DB write/lock (`Stage A Admit` p95 `23.9–46.8 ms` + `Stage C Persist/Resequence` p95 `95.5–171.7 ms`), nhạy cảm hơn nhiều với `fsync` latency nếu PG bật `synchronous_commit=on`, drain lâu hơn, và làm tăng độ phức tạp vận hành mà **hoàn toàn không mang lại bất kỳ cải thiện độ trễ nào** trên workload đều `1 reading/machine/s`.
- **`K1` ML NumPy Fast-Path**:
  - *Trade-off:* Chỉ sử dụng nhanh nếu pipeline đúng chuẩn `SensorInteractions -> ColumnTransformer(OHE/Scaler/passthrough) -> estimator.predict_proba` và **bắt buộc vượt qua kiểm tra `abs(ref - fast) <= 1e-12`** lúc khởi động. Mọi thay đổi pipeline trong tương lai không khớp tự động quay lại `pd.DataFrame` an toàn (`0` rủi ro sai kết quả âm thầm).

---

## 20. Recommendation (Khuyến nghị Dứt khoát)

1. **Scheduler Processing mặc định cho 50 máy × 1 Hz (và burst đồng bộ 50 máy):**
   - **Chọn `Keyed FIFO` (`PROCESSING_HYBRID_ORDERING_ENABLED=false`)**.
   - **Tăng `PROCESSING_MAX_ACTIVE_KEYS` từ `8` lên `16`** (giữ `PROCESSING_MAX_INFLIGHT_PER_KEY=1`, `PROCESSING_MAX_TOTAL_INFLIGHT=16`) để loại bỏ rủi ro key-starvation (`99p queue_wait ~200–235 ms -> 49.5 ms`) khi Kafka chia 12 partitions hơi lệch về một vài Pod worker.
   - **Không bật Hybrid mặc định.** Chỉ bật Hybrid bằng config flip khi triển khai profile chuyên biệt cho **edge replay / high-frequency single-machine burst (> 4 readings/s trên cùng 1 `machine_id`)**.
2. **ML Service Configuration & Code:**
   - **Đưa tối ưu `K1` (`_FastInferencePath` trong [local_baseline.py](../../ml-service/inference/api/local_baseline.py)) vào Serving (`hgb-opt`)** — giúp tăng gấp `2.0×` throughput/core và cắt `-45%` CPU tiêu thụ mà vẫn kiểm chứng toán học `< 1e-12` lúc khởi động.
   - **Khóa cấu hình env: `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, giữ `--workers 1`** (scale ngang bằng Kubernetes Pods thay vì multi-worker nội bộ).
   - **Scale ML tối thiểu `2` Pods (hoặc `3` Pods cho Burst `p95 <= 50 ms` và HA `N-1`)**, đồng thời đặt `limits.cpu >= 1500m–2000m` (không bóp cứng `1.0` CPU như test cũ gây 89–99% throttling khi concurrency thoáng qua vượt 1).
3. **Follow-up tối ưu tiếp theo (nếu cần giảm thêm skew `19.8% / 48.6%`):**
   - Khắc phục HTTP/1.1 keep-alive affinity giữa `processing-worker` và ClusterIP `ml-service` (bằng service mesh L7 / HTTP/2 hoặc rotation connection định kỳ trong `httpx.AsyncClient`), dự kiến hạ tail `ml_client p95` ổn định về **`~95–115 ms`** chỉ với `3` ML Pods.

---

## 21. Giới hạn Đo kiểm và Điều kiện Môi trường (Known Limitations)

1. **Độ phủ cửa sổ Playwright SSE trên các run 5 phút (`~89.0–89.4%`):** `[Code-inspection]` + `[Measured]`
   - Trong Round 1 và Round 2 (`30s warmup + 300s measure = 330s`), Playwright probe gặp lỗi `TypeError: Cannot read properties of undefined (reading 'then')` tại giây `~300` (lúc JWT 300s hết hạn) do bug unwrapping `resolveReady(donePromise)` trong `sse_browser_probe.mjs`, làm các cycle `~270–301` không có mẫu SSE trình duyệt (~`13,350–13,450 / 15,050` readings = **`89.00%–89.39%` coverage**).
   - Ngược lại, **`sample_to_business_db` ở DB phủ 100.0% mọi `15,050` readings và mọi invariants/drain/Prometheus kiểm chứng 100% toàn bộ `16,550` readings**, đồng thời mọi run ngắn (`ML-S1`, `Recovery`) đạt **`99.83%–100.0%`** SSE coverage.
   - Bug unwrap trong [sse_browser_probe.mjs](../../scripts/benchmarks/burst_keyed_vs_hybrid/sse_browser_probe.mjs#L206-L239) **đã được vá xong (`resolveReady({ done: donePromise })`)** cho các lần tái lập sau, và **chúng tôi không bịa hay nội suy `10.8%` SSE còn thiếu** trong số liệu Round 1/2.
2. **PostgreSQL `synchronous_commit=off` trong local GitOps:** làm giảm nhân tạo chi phí `fsync` của DB transaction trong môi trường k3d local. Ảnh hưởng làm Hybrid (`Stage A` + `Stage C`) trông nhẹ hơn thực tế production RDS (`synchronous_commit=on`) -> **củng cố thêm kết luận không chọn Hybrid mặc định**.
3. **K3d Single-Node & Unset Pod `limits.cpu`:** tất cả Pods, DB, Kafka, Ingress và `SensorEngine` chạy chung 1 node host (`Ryzen 7 7435HS`, 16 vCPU, 15 GiB RAM với 7.8 GiB swap đang sử dụng) và cgroup Pods không giới hạn `cpu.max` (`throttled_ratio_pct = 0.0%` trên mọi k3d runs, trái ngược với standalone Docker `1.0` CPU benchmark).
4. **Local-Fixture Scope:** `PROCESSING_PROFILE=local-fixture`, `MAINTAI_LOCAL_ONLY=1`, Kafka `PLAINTEXT RF=1`, model `hgb-interactions-candidate-v1` dùng mock identity fixture (`local-demo`, chưa qua gate release chính thức lên AWS production).
5. **Prometheus Histogram Bucket Floor (`50 ms`) ở `queue_wait`:** bucket nhỏ nhất của `maintai_processing_queue_wait_seconds` là `0.05s`, do đó `p50=25.0 ms` / `p95=47.5 ms` chỉ phản ánh cận nội suy tuyến tính — phải đọc `mean_ms` (`0.32–2.48 ms`) để đánh giá chính xác.
6. **Không benchmark end-to-end trường hợp 1 hot machine gửi `> 4 readings/s`:** ưu thế lý thuyết của Hybrid trên hot-machine chưa chạy benchmark k3d riêng (`[NOT_MEASURED]` trên k3d; mới được chứng minh chức năng & correctness ở §9.1/§9.2).

---

## 22. Lệnh Tái lập Kiểm thử (Reproducible Commands) và Rollback

### 22.1. Chạy Unit & Contract Tests (Simulator + Keyed SDK + ML Fast-Path)
```bash
# 1. Burst Sensor Simulator + Keyed FIFO SDK unit tests
.venv/bin/pytest sensor/tests/ messaging/tests/unit/test_keyed.py -q

# 2. ML Service local_baseline (tạo venv theo requirements-ci.lock nếu hệ thống chưa cài sklearn)
python3 -m venv /tmp/ml-ci-venv
/tmp/ml-ci-venv/bin/pip install -r ml-service/inference/requirements-ci.lock
/tmp/ml-ci-venv/bin/python -m unittest discover -s ml-service/inference/tests -p 'test_local_baseline*.py' -v

# 3. Repository integrity check & SSE script syntax check
.venv/bin/python .github/scripts/check_repository.py
node --check scripts/benchmarks/burst_keyed_vs_hybrid/sse_browser_probe.mjs
```

### 22.2. Chạy Correctness Integration §9.1 & §9.2 trên Real PG17 + Kafka 4.0.0
```bash
# Yêu cầu containers maintai-m2-postgres-1 (port 55432) & maintai-m2-kafka-1 (port 19092) đang chạy
MAINTAI_M2_INTEGRATION=1 \
MAINTAI_TEST_PG_DSN="postgresql://maintai:maintai@127.0.0.1:55432/maintai" \
MAINTAI_TEST_KAFKA_BOOTSTRAP="127.0.0.1:19092" \
.venv/bin/pytest --import-mode=importlib \
  processing/worker/tests/test_hybrid_ordering.py \
  backend/business-consumer/tests/test_alert_sequence_guard.py -v
```

### 22.3. Chạy Standalone ML Capacity Audit & Batch Micro-Benchmark
```bash
# Build benchmark Docker images từ source hiện tại
docker build -f ml-service/inference/Dockerfile -t maintai-ml-bench:hgb-opt .

# Run ML Capacity Audit (ví dụ mức ML-K1 1 CPU)
.venv/bin/python scripts/benchmarks/burst_keyed_vs_hybrid/ml_capacity_audit.py \
  --image maintai-ml-bench:hgb-opt --tag ML-K1-1CPU \
  --model-dir ml-service/artifacts/integration/hgb-interactions-m56w5ff4 \
  --cpus 1.0 --workers 1 \
  --env OMP_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 --env MKL_NUM_THREADS=1 \
  --duration 60 --concurrencies 1,2,4,8,12,24 \
  --out /tmp/ml_k1_1cpu.json

# Run Batch Inference Micro-Benchmark
/tmp/ml-ci-venv/bin/python scripts/benchmarks/burst_keyed_vs_hybrid/ml_batch_microbenchmark.py \
  --model-dir ml-service/artifacts/integration/hgb-interactions-m56w5ff4 \
  --out /tmp/ml_b1_batch_micro.json
```

### 22.4. Chạy k3d End-to-End Burst Benchmark
```bash
# Lưu ý: tạm dừng ArgoCD auto-sync & KEDA trước khi patch tạm cluster benchmark, và luôn khôi phục sau đó!
.venv/bin/python scripts/benchmarks/burst_keyed_vs_hybrid/k3d_burst_benchmark.py \
  --kubeconfig /tmp/k3d-bench-kubeconfig.yaml \
  --presets A2,B2 --repeats 3 --warmup 30 --duration 300 \
  --ml-shadow-replicas 3 --ml-shadow-image maintai-ml-bench:hgb-opt \
  --ml-shadow-env OMP_NUM_THREADS=1,OPENBLAS_NUM_THREADS=1,MKL_NUM_THREADS=1 \
  --ml-shadow-args "--workers=1" \
  --out /tmp/round2_results.json
```

### 22.5. Lệnh Rollback An toàn
- **Rollback runtime configuration ngay lập tức:**
  ```bash
  # Giữ Keyed FIFO (không cần migrate hay reset inbox):
  kubectl -n maintai-local set env deploy/processing-worker \
    PROCESSING_HYBRID_ORDERING_ENABLED=false \
    PROCESSING_MAX_ACTIVE_KEYS=16 \
    PROCESSING_MAX_INFLIGHT_PER_KEY=1 \
    PROCESSING_MAX_TOTAL_INFLIGHT=16
  ```
- **Rollback ML Fast-Path (bất kỳ lý do nào):**
  - Về runtime: dùng image trước tối ưu (`ghcr.io/chiendz11/maintai-ml-service:sha-94248e49bd84b112162244f62b25fd466cbf82f4`).
  - Về source: `git checkout <base-sha> -- ml-service/inference/api/local_baseline.py` hoặc `git revert <commit-sha>`.

---

## 23. Raw Metrics và Test Evidence Index

Toàn bộ dữ liệu thô đã kiểm tra và lưu giữ có cấu trúc trong `MaintAI`:

| Artifact / File | Nội dung chính | Link trực tiếp |
|---|---|---|
| Báo cáo kỹ thuật này | Tổng hợp 23 mục, provenance & kiến nghị | [BURST_KEYED_VS_HYBRID_BENCHMARK.md](BURST_KEYED_VS_HYBRID_BENCHMARK.md) |
| Round 1 Raw JSON | `A0`, `A1`, `B1` × 3 runs × 5 phút (Fixed Baseline ML) | [round1/results.json](../benchmarks/burst-keyed-vs-hybrid/round1/results.json) |
| Round 2 Raw JSON | `A2`, `B2` × 3 runs × 5 phút (Identical `ML-OPT`) | [round2/results.json](../benchmarks/burst-keyed-vs-hybrid/round2/results.json) |
| Live Pod-Kill Recovery JSON | `B2` kill `processing-worker` & `ml-service-shadow` | [recovery/results.json](../benchmarks/burst-keyed-vs-hybrid/recovery/results.json) |
| `ML-0` Baseline Standalone | 1 CPU default env, `c=1..24` (`host cgroup v2`) | [ml0_baseline_1cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml0_baseline_1cpu.json) |
| `ML-C1` Threads (`1` & `2` CPU) | `OMP=OPENBLAS=MKL=1`, phát hiện trần GIL `1.122c` | [ml_c1_threads_1cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_c1_threads_1cpu.json), [ml_c1_threads_2cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_c1_threads_2cpu.json) |
| `ML-C2` Workers=2 (`2` CPU) | `--workers 2`, phát hiện `+45ms` low-concurrency stall | [ml_c2_workers2_2cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_c2_workers2_2cpu.json) |
| `ML-K1` NumPy Fast-Path (`1` & `2` CPU) | Zero-DataFrame fast-path parity `<1e-12` (`2.0×` RPS) | [ml_k1_fastpath_1cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_k1_fastpath_1cpu.json), [ml_k1_fastpath_2cpu.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_k1_fastpath_2cpu.json) |
| `ML-S1` k3d Scaling (`1..4` Pods) | Scale ngang `hgb-opt` + phát hiện TCP keep-alive skew | [ml_s1_k3d_scaling.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_s1_k3d_scaling.json) |
| `ML-B1` Batch Micro-Benchmark | Độc lập batch `1, 4, 8, 16` (`7,397 samp/s` max) | [ml_b1_batch_micro.json](../benchmarks/burst-keyed-vs-hybrid/ml_audit/ml_b1_batch_micro.json) |
| Harness Benchmark & Probes | Scripts tái lập (Playwright SSE + cgroup v2 + k3d) | [k3d_burst_benchmark.py](../../scripts/benchmarks/burst_keyed_vs_hybrid/k3d_burst_benchmark.py), [sse_browser_probe.mjs](../../scripts/benchmarks/burst_keyed_vs_hybrid/sse_browser_probe.mjs), [ml_capacity_audit.py](../../scripts/benchmarks/burst_keyed_vs_hybrid/ml_capacity_audit.py), [ml_batch_microbenchmark.py](../../scripts/benchmarks/burst_keyed_vs_hybrid/ml_batch_microbenchmark.py) |
| Simulator Burst Source & Tests | Synchronized/Burst mode giữ `staggered` mặc định (`140/140 PASS`) | [settings.py](../../sensor/src/simulator/settings.py), [engine.py](../../sensor/src/simulator/scheduling/engine.py), [test_durable_runtime.py](../../sensor/tests/test_durable_runtime.py) |
| Ordering & Alert Tests | §9.1 & §9.2 E2E test trên PG17 + Kafka 4.0.0 (`9/9 PASS`) | [test_hybrid_ordering.py](../../processing/worker/tests/test_hybrid_ordering.py) |
| ML Service Fast-Path & Tests | Verified NumPy Fast-Path + parity tests (`34/34 PASS`) | [local_baseline.py](../../ml-service/inference/api/local_baseline.py), [test_local_baseline.py](../../ml-service/inference/tests/test_local_baseline.py) |
