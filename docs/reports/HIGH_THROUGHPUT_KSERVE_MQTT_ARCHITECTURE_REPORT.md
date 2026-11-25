# Báo Cáo Kiến Trúc High-Throughput: MQTT QoS 1, SQLite Spool v2, Ordered Per-Key Micro-Batching & KServe KPA

**Ngày cập nhật:** 2026-11-25  
**Commits tham chiếu (`main`):** `MaintAI@7f79d8d49041f339fe2fb98170534d1685e783ca` (PR `#64`, `#65`), `MaintAI-gitops@16c70f6` (PR `#118`, `#122`, `#123`, `#127`)  
**Phạm vi tài liệu:** Tổng hợp đầy đủ kiến trúc vận hành thực tế đã hợp nhất trên `main` và kiểm chứng steady-state trên cụm `k3d-maintai-local` (`k3s v1.35.5`, 15 GiB RAM). Không thay đổi HTTP/Kafka wire contracts `v1.3`/`v1.4` hoặc ranh giới DB owners.

---

## 1. Động lực & Quyết định Kiến trúc Cốt lõi

Để tháo các nút cổ chai khi tăng tải **50 máy × 1 Hz** (`SIM_MACHINE_COUNT=50`, `SIM_INTERVAL_MS=1000`), burst và phục hồi sau gián đoạn (**Recovery 30s–30m**) mà **không phá vỡ thứ tự dữ liệu theo máy**, hệ thống áp dụng các quyết định sau:

1. **Giữ nghiêm ngặt `PROC-ORDER-01` (`per_key = 1`, Keyed FIFO):** Mỗi `machine_id` chỉ có tối đa `1` tác vụ đang thực thi tại một thời điểm trong `processing-worker`, commit offset Kafka theo *safe completed prefix* từng partition. **Không** sử dụng cơ chế Hybrid out-of-order hay `AlertResequencer` (nhánh thử nghiệm được lưu trữ tại tag `archive/hybrid-ordering-benchmark`, commit `67a748a`).
2. **Thay HTTP per-reading trên hot path sensor bằng MQTT QoS 1 (giữ HTTP `POST /api/v1/sensor-readings` song song):** `sensor` kết nối bền bỉ qua TCP tới Eclipse Mosquitto nội bộ thay vì mở hàng chục HTTP+JWT calls/giây, đồng thời `ingestion-api` giữ nguyên endpoint HTTP cũ (`2` replicas) để bảo đảm tương thích ngược 100%.
3. **Nâng cấp SQLite Spool v2 (`O(1)` & chống starvation):** Loại bỏ `SELECT COUNT(*), SUM(size)` toàn bảng mỗi tick, tách quarantine khỏi `pending`, và áp quota theo máy kết hợp xoay vòng round-robin.
4. **KServe `InferenceService` + Knative Serving + KPA + Vectorized Batch ML:** Chuyển `ml-service-champion` sang KServe custom predictor container (`minReplicas=2`, `maxReplicas=6`, `UVICORN_WORKERS=1`) hỗ trợ cả `/api/v1/predict/batch` và `/api/v1/predict`, đi kèm zero-pandas NumPy fast path.
5. **Kafka Pipelining & Synchronous Commit Batching:** Pipelined `publish_batch()` với `KAFKA_LINGER_MS=2` + `lz4` trên producer; gộp `commit` đồng bộ theo batch ở `business-consumer` mà vẫn giữ serial bounded-prefetch `ConsumerRunner` (`64/32`).

---

## 2. Sơ Đồ Luồng Dữ Liệu End-to-End Hiện Tại

```mermaid
flowchart LR
  subgraph EDGE["Nguồn mô phỏng & Edge"]
    SUI["Simulator UI (:8080)"] -->|JWT OIDC Control| SEN["sensor (1 pod Recreate)\nSQLite Spool v2 (100k / 128 MiB)"]
    SEN -->|MQTT QoS 1\n.../machines/{id}/telemetry| MQ["Mosquitto 2.1.4 (:1883)\nPVC local-mosquitto-data"]
    SEN -.->|HTTP fallback| ING["ingestion-api (2 pods :8000)\nPOST /api/v1/sensor-readings"]
  end

  subgraph BRIDGE["Ingestion → Kafka"]
    MQ -->|No $share, QoS 1| BR["ingestion-mqtt-bridge (1 pod Recreate)\nBatch 32 / 5ms, PUBACK sau Kafka ACK"]
    BR -->|publish_batch, lz4, linger=2ms| K1[("Kafka 12 partitions\nsensor.readings / sensor.rejected")]
    ING -->|publish| K1
  end

  subgraph PROC["Processing (PROC-ORDER-01)"]
    K1 -->|local-processing-v1| PW["processing-worker (6 pods)\nKeyed FIFO per_key=1, max_batch_per_key=16"]
    PW -->|POST /predict/batch or /predict| MIR["ml-service-mirror (Nginx, 2 pods :8001)"]
    MIR -->|95% + 100% sync| KOU["Kourier Internal Gateway\nKnative Serving v1.21.3"]
    KOU -->|KPA concurrency target=4| MLC["KServe ml-service-champion\n(2..6 pods, UVICORN_WORKERS=1)"]
    MIR -.->|5% async mirror| MLS["ml-service-shadow (1 pod)"]
    PW -->|Tx + Outbox| PDB[("processing_db")]
    PDB --> OR["processing-outbox-relay (2 pods)\nmode=ordered, linger=2ms, lz4"]
    OR --> K2[("Kafka 12 partitions\ninference.results / alert.signals")]
  end

  subgraph BUS["Business & UI"]
    K1 -->|local-business-live-v1| BC["business-consumer (4 pods)\nCommit Batching 16 / 50ms"]
    K2 -->|prediction & alert groups| BC
    BC --> BDB[("business_db")]
    BDB -->|PG NOTIFY| BA["business-api (2 pods :8003)"]
    BA -->|REST + SSE| FE["Business UI (frontend)"]
  end
```

---

## 3. Chi Tiết Nâng Cấp Theo Từng Phân Hệ

### 3.1. `sensor/` — MQTT Transport (QoS 1) & SQLite Spool v2

- **Transport (`sensor/src/simulator/client.py`, `settings.py`):**
  - Tách protocol `DeliveryTransport` với 2 implementation: `BackendClient` (HTTP) và `MqttTransport` (`paho-mqtt` v2).
  - `MqttTransport` duy trì kết nối bền bỉ, cấu hình Last Will `.../controller/status = OFFLINE` và Birth `ONLINE`, publish QoS 1 tới `maintai/v1/site/local/machines/{machine_id}/telemetry`, và chỉ trả `outcome="accepted"` sau khi nhận `PUBACK`.
  - Chọn qua `SIM_TRANSPORT_MODE` (`mqtt` ở overlay local, mặc định `http` để giữ an toàn/tương thích).
- **Spool v2 & Chống Starvation (`sensor/src/simulator/ordering/store.py`, `metrics.py`):**
  - Nâng `schema_version` từ `1 → 2` với auto-migration idempotent trên `/state/state.sqlite3`.
  - Bảng `spool_quarantine` tách riêng bản ghi `expired` và `rejected` (FIFO tự cắt khi vượt `SIM_QUARANTINE_MAX_RECORDS=10000`), giữ bản ghi `auth` trong `spool`.
  - Bảng `spool_stats` lưu bộ đếm **O(1)** (`pending_count`, `pending_bytes`, count theo từng `machine_id`) trong cùng `transaction()`, loại bỏ query `COUNT(*)/SUM(size)` `O(N)`.
  - Quota mặc định mới: `spool_max_records=100_000`, `spool_max_bytes=134_217_728` (128 MiB), giới hạn per-machine `spool_max_records_per_machine` kết hợp `round_robin_offset` mỗi tick để chống starvation cho các máy cuối danh sách (`M-040`..`M-050`).
  - Không silent drop: tăng `maintai_simulator_spool_dropped_total{reason}` + log warning; xuất đầy đủ gauges `maintai_simulator_spool_records{status}`, `maintai_simulator_spool_bytes`, `maintai_simulator_spool_quota_ratio`.

### 3.2. Mosquitto + `ingestion-mqtt-bridge` + Kafka Pipelining

- **Broker (`MaintAI-gitops/local/platform/mosquitto.yaml`, `mosquitto.conf`):**
  - Image `eclipse-mosquitto:2@sha256:fa12172a051310efe5bcb7a6a11b77bd5faacb82cbacd7742aa758bf024fd8e4` (`mosquitto 2.1.4`), non-root `uid=1883`, `persistence true` trên PVC `local-mosquitto-data` (1 GiB `local-path`), `max_inflight_messages 128`, `max_queued_messages 100000`.
  - `PersistentVolumeClaim/local-mosquitto-data` và `Service/mosquitto` đặt cùng `sync-wave: '-30'` với `Deployment/mosquitto` để tránh deadlock `WaitForFirstConsumer` của `local-path` trên Argo CD, được bảo vệ bằng `NetworkPolicy/allow-sensor-ingestion-to-mosquitto`.
- **Single-Pod Bridge (`MaintAI/ingestion-api/src/maintai_ingestion/mqtt_bridge.py`, `MaintAI-gitops/environments/local/apps/ingestion-mqtt-bridge.yaml`):**
  - Chạy `replicas: 1` với `strategy: Recreate`, **cấm** topic filter `$share/...` trong `config.py` và `mqtt_bridge.py` nhằm giữ nguyên thứ tự FIFO từng máy khi đọc từ Mosquitto.
  - Subscribe `maintai/v1/site/+/machines/+/telemetry` (chấp nhận cả `("/telemetry", "/readings")` và ACK ngay các message `/status` không thuộc telemetry), gom batch `INGESTION_MQTT_BATCH_SIZE=32` / `INGESTION_MQTT_BATCH_WAIT_MS=5`, `INGESTION_MQTT_MAX_INFLIGHT=256`.
  - Chỉ gọi `client.ack(mid, qos=1)` sau khi `KafkaPublisher.publish_batch()` xác nhận durability từ Kafka (hoặc quarantine `sensor.rejected` cho `400/422`); giữ nguyên un-ACKed khi gặp `503/504` để Mosquitto giao lại.
- **Kafka Producer Pipelining (`messaging/src/maintai_messaging/producer.py`, `config.py`):**
  - Thêm `publish_batch(events)` đẩy toàn bộ `produce()` trước rồi `poll()` chờ callback trong 1 broker RTT, **không giữ `self._lock`** khi chờ broker ACK.
  - Bổ sung `KAFKA_LINGER_MS=2` (`0..20`) và `KAFKA_COMPRESSION_TYPE=lz4` cho `ingestion-api`, `ingestion-mqtt-bridge`, `processing-worker` và `processing-outbox-relay`.

### 3.3. KServe `InferenceService` + Knative KPA + Vectorized Batch ML

- **Stack Serving (`MaintAI-gitops/environments/local/ml-serving/`):**
  - **KServe `v0.18.1`** + **Knative Serving `v1.21.3`** + **Kourier `v1.21.1`** (kèm Lua health check cho `serving.kserve.io/InferenceService` trong `argocd-cm`).
  - `InferenceService/ml-service-champion`: Custom Predictor Container (`ghcr.io/chiendz11/maintai-ml-service`), `minReplicas: 2`, `maxReplicas: 6`, `scaleMetric: concurrency`, `scaleTarget: 4`, `containerConcurrency: 8` — giữ tối thiểu 2 pod để tránh cold-start và không bật song song HPA.
  - **`UVICORN_WORKERS=1`** trong `ml-service/inference/Dockerfile` (và overlay `ml-serving` + `ml-service-shadow`): chạy 1 process Python/pod giúp tiết kiệm RAM trên máy 15 GiB, tránh sai lệch metric Prometheus trong RAM (`CollectorRegistry`), và nhường scaling ngang cho Knative KPA.
- **Zero-Pandas Vectorized Fast Path & Batch API (`ml-service/inference/api/local_baseline.py`):**
  - Chuẩn hóa ma trận NumPy trực tiếp cho feature allowing-list (không dựng `pandas.DataFrame` mỗi request); kiểm tra tương đương số học lúc khởi động (`max_abs_diff = 2.78e-17` với `logreg-baseline-v0`).
  - Cung cấp `POST /api/v1/predict/batch` (`BatchPredictRequest`, tối đa `64` items) song song với `POST /api/v1/predict`.
- **Traffic Mirroring:** `ml-service-mirror` (`nginxinc/nginx-unprivileged`, `2` replicas) nhận request từ `processing-worker` tại `:8001`, forward tới `kourier-internal.kourier-system.svc.cluster.local:80` và mirror bất đồng bộ `5%` tới `ml-service-shadow` (`1` replica) theo `NetworkPolicy` Zero-Trust.

### 3.4. Ordered Per-Key Micro-Batching & Synchronous Commit Batching

- **Processing Worker (`processing/`, `messaging/`):**
  - `PROCESSING_MAX_BATCH_PER_KEY=16`: khi một `machine_id` có nhiều event liên tiếp đã về sẵn trong hàng đợi partition, worker giữ nguyên strict FIFO `per_key=1`, gom tối đa 16 reading của cùng máy, chạy DQ tuần tự rồi gọi `InferenceClient.predict_batch()` cho các item `VALID` (tự động fallback về `/api/v1/predict` nếu endpoint nhận trả `404`), sau đó commit transaction DB + outbox và báo `OffsetCoordinator` tiến safe completed prefix.
- **Business Consumer (`messaging/src/maintai_messaging/consumer.py`):**
  - Giữ nguyên serial bounded-prefetch `ConsumerRunner` (`64/32`), bổ sung commit batching đồng bộ (`KAFKA_COMMIT_BATCH_SIZE=16`, `KAFKA_COMMIT_MAX_WAIT_MS=50`) giúp giảm số lần `commitSync` lên Kafka mà không làm thay đổi ngữ nghĩa durability sau SQL commit.

---

## 4. Bằng Chứng Kiểm Chứng Thực Tế Trên `k3d-maintai-local` (2026-11-25)

Sau khi triển khai hoàn chỉnh lên `main` và khắc phục 3 vấn đề vận hành cụm (`local-mosquitto-data` sync-wave PR `#122`, khôi phục secret LocalStack sau restart, chờ TCP `postgres:5432` trong `business-db-migration` PR `#123` + đồng bộ `alembic_version` `business_0007_rebuild`), hệ thống được đo thực tế khi bật **50/50 máy × 1 Hz** qua Simulator UI (`T0 = 04:30:48Z` → `T1 = 04:31:00Z`):

| Chặng | Kết quả đo thực tế (`T0 → T1`) | Ghi chú |
| :--- | :--- | :--- |
| **`sensor` (`1` pod)** | `50/50 RUNNING`; `42,609 → 43,188` (**+579** bản ghi, `ACCEPTED = 43,188`) | `PUBACK` ~`0.4–0.6 ms`; SQLite spool `pending=0`, `bytes=0`, `0` drops |
| **`mosquitto` (`1` pod)** | Hoạt động bình thường trên port `1883`, PVC `local-mosquitto-data` `Bound` | Không lỗi kết nối hoặc quyền ghi `/mosquitto/data` |
| **`ingestion-mqtt-bridge` (`1` pod)** | `/health/ready` `200 UP` (`mqtt: true`, `kafka: true`); `accepted`: `1,436 → 2,022` (**+586**) | Không lỗi/drop dữ liệu Kafka; nhận topic `.../telemetry` |
| **Kafka (`kafka-0`)** | Lag nhóm real-time: `local-processing-v1=4`, `local-business-live-v1=5`, `local-business-prediction-v1=2`, `local-business-alert-v1=0` | Nhóm archive `local-m6-...-raw-archive-v1` giữ batch lag `700` |
| **`processing-worker` (`6` pods) & `processing_db`** | `sensor_events`: `1,447 → 2,035`; `processing_results`/`predictions`/`risk_decisions`: `1,447 → 2,036` | `maintai_processing_frontier_stall_seconds = 0.0` trên cả 6 pods |
| **KServe `ml-service-champion` (`2` pods) & `ml-service-shadow`** | Champion xử lý cân bằng `1,060` + `1,077` = `2,137` `SUCCESS`; latency nội bộ ~`0.86 ms/req`; Shadow trả `200 OK` | Test trực tiếp `POST /api/v1/predict` và `/api/v1/predict/batch` qua `ml-service-mirror:8001` đều trả `200 OK` |
| **`processing-outbox-relay` (`2` pods), `business-consumer` (`4` pods) & `business_db`** | `maintai_kafka_outbox`: `1,451 → 2,042` (`pending: 0 → 1`); `business_db` đủ `50/50` máy ở cả 3 bảng `latest_*`, `alerts`: `4 → 6 OPEN` | Không pod crash/restart hoặc lỗi runtime trên toàn bộ 7 chặng |

---

## 5. Giới Hạn Kiểm Chứng & Những Điểm Không Tự Suy Diễn

1. **Steady-state vs Burst/Recovery thực tế trên cụm:** Đo kiểm trên xác nhận tải ổn định `50 msg/s` (`batch_size=1` ở steady-state, KServe ở `minReplicas=2`). Kịch bản nén spool và xả dồn (tạm dừng `ingestion-mqtt-bridge` trong 30 giây – 30 phút để kích hoạt hàng đợi spool lớn, gọi `/api/v1/predict/batch` nhiều phần tử và KPA scale `2 → 6` pods) đã có unit/endpoint tests nhưng cần phiên load-test riêng trên cụm để thu thập p95 recovery time.
2. **Local `k3d` ≠ Production HA:** Cụm local dùng 1 node `k3d`, 1 broker Kafka, 1 replica `mosquitto` + `ingestion-mqtt-bridge` và LocalStack in-memory (cần seed lại secret vào LocalStack qua `seed_localstack_secrets.py` + `localdev.history.seed()` nếu cụm khởi động lại). Production AWS phải sử dụng EKS multi-AZ, MSK/Strimzi replication `3`/`minISR=2`, ECR image digests, và AWS Secrets Manager thật.
3. **Metric instrumentation phụ trên MQTT Bridge:** `maintai_metric_samples_dropped_total{reason="SOURCE_TIME_MISSING"}` tăng cùng nhịp `accepted` trên `ingestion-mqtt-bridge` vì luồng MQTT gọi thẳng `gateway.handle_batch` mà không qua HTTP middleware gán `RequestObservation`; điều này không ảnh hưởng tới dữ liệu Kafka.
