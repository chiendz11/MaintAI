# Báo Cáo Baseline Vận Hành Local 50 Máy & Cấu Hình Cụm (`k3d`)

**Ngày ghi nhận:** 2026-11-22 (cập nhật tối ưu Business SSE & benchmark 50 máy đồng thời; bản đầu `2026-11-21`)  
**Phiên bản nguồn:** `MaintAI@main` (`business-api` SSE optimization sau `3b47520ef87cbed0f8f567696ef43db86206306b`) · `MaintAI-gitops@66be41c5728291701596a4f92a36a228eb25a320`  
**Phạm vi:** Cụm Kubernetes local đơn nút `k3d-maintai-local` chạy trên máy trạm/laptop **16 vCPU / 15 GiB RAM (+ 23 GiB swap)** — tách biệt hoàn toàn với cấu hình production trên AWS (`MaintAI-gitops/environments/production/`).

---

## 1. Phân Biệt Rõ Phạm Vi Baseline

| Mức tải / Profile | Số máy & Tốc độ | Vai trò | Tài liệu chuẩn |
| --- | --- | --- | --- |
| **Smoke Local (`LOAD-D5`)** | `5` máy × `1 reading/s` = `5 readings/s` | Kiểm tra chức năng cơ bản, E2E luồng dữ liệu nhẹ | [02_ASSUMPTIONS_AND_CAPACITY.md §2](../design-basis/02_ASSUMPTIONS_AND_CAPACITY.md) |
| **Local Operational Baseline (`LOAD-L50`)** | **`50` máy (`M-001`..`M-050`) × `1 reading/s` = `50 readings/s`** | **Baseline vận hành & đo kiểm hiệu năng chuẩn cho cụm local 15–16 GiB RAM** | Tài liệu này & [LOCAL_DEMO_GUIDE.md](../operations/LOCAL_DEMO_GUIDE.md) |
| **Design Target (`LOAD-N100` / `CAPACITY-BASE100-01`)** | `100` máy × `1 reading/s` = `100 readings/s` (`scale-ready` cho `X > 100`) | Mục tiêu thiết kế kiến trúc tổng thể; trên laptop 15 GiB gây bão hòa CPU & swap thrashing ở tầng `business-consumer` | [design-basis/README.md](../design-basis/README.md) |

Lý do chọn `50` máy × `1 reading/s` thay vì giữ `100` máy hay tăng `SIM_INTERVAL_MS`: giữ đúng `SIM_INTERVAL_MS=1000` để mọi máy hoạt động đúng nhịp thực tế `1 reading/s` (khớp `freshMs=2000` / `staleMs=5000` trên Business UI và mục tiêu `Interval ≤ 1s`), đồng thời đưa tổng tải từ `100/s` (`200 msg/s` vào `business-consumer`) xuống `50/s` (`100 msg/s` vào `business-consumer`) để vừa khít ngân sách CPU/RAM của máy host 15 GiB.

---

## 2. Cấu Hình Cụm Hiện Tại Cho Baseline 50 Máy

### 2.1. Replicas, Autoscaling, Tài Nguyên & Image Tag Ứng Dụng (`maintai-local`)

Nguồn khai báo: [`MaintAI-gitops/environments/local/apps/`](../../../MaintAI-gitops/environments/local/apps/kustomization.yaml).

| Workload | Cơ chế Scale | Replicas | CPU Req / Lim | RAM Req / Lim | Image / Tag (`maintai-local`) | Ghi chú cấu hình chính |
| --- | --- | ---: | --- | --- | --- | --- |
| `ingestion-api` | HPA CPU 70% (`min=2, max=2`) | `2` | `100m` / — | `128Mi` / `1Gi` | `sha-c2bce008...` | `INGESTION_MAX_INFLIGHT=64`, chia tải qua 2 pods tránh ghim L4 keep-alive |
| `processing-worker` | KEDA lag `20` + CPU 70% (`min=6, max=6`) | `6` | `100m` / — | `128Mi` / `1Gi` | `sha-648f1ef3...` | `keyed` (`PROC-ORDER-01`), `PROCESSING_MAX_ACTIVE_KEYS=8`, `REGISTRY_CACHE=30s`, `KAFKA_POLL=0.01s`, ML mirror |
| `processing-outbox-relay` | Static replica patch | `2` | `100m` / — | `128Mi` / `1Gi` | `sha-3b47520e...` | `ordered`, `OUTBOX_BATCH_SIZE=64`, `OUTBOX_POLL_INTERVAL_MS=10`, `CLAIM_LEASE=120s`, ép dùng unique index |
| `processing-read-api` | Base Deployment | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-c2bce008...` | `PROCESSING_HISTORY_ENABLED=true`, `CLEANER_DRY_RUN=true`, `RETENTION_ENABLED=false` |
| `ml-service` | HPA CPU 70% (`min=2, max=2`) | `2` | `100m` / — | `128Mi` / `1Gi` | `sha-94248e49...` | `logreg-baseline-v0`, `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1` |
| `ml-service-shadow` | HPA CPU 80% (`min=1, max=1`) | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-94248e49...` | `hgb-interactions-candidate-v1`, `OMP/OPENBLAS=1` |
| `ml-service-mirror` | Local manifest (Nginx) | `1` | `25m` / `1000m` | `32Mi` / `128Mi` | `nginx-unprivileged` (pin digest) | Mirror `5%` sang shadow, upstream keepalive `64/32` |
| `business-consumer` | KEDA lag `20` + CPU 70% (`min=4, max=4`) | `4` | `100m` / — | `128Mi` / `1Gi` | `sha-648f1ef3...` | Serial `SDK 0.3.1` (`prefetch<=64`), `KAFKA_POLL_INTERVAL_SECONDS=0.01`, phục vụ `50 live + 50 pred = 100 msg/s` |
| `business-api` | HPA CPU 70% (`min=1, max=4`) | `1–4` *(auto-scale)* | `100m` / — | `128Mi` / `1Gi` | Optimized SSE (`maintai-business-api`) | `SSESettings.coalesce_seconds=0.05` (`BUSINESS_SSE_COALESCE_SECONDS`), `_pump=25ms`, `max_connections=128`, `Subscription.queue maxsize=512`, batch `ProjectionRepository.live_many()` |
| `storage-consumer` | Base Deployment | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-f3466035...` | `KAFKA_POLL=0.01`, `STORAGE_CATALOG_ENABLED=true`, `INTERVAL=1s`, `ARCHIVE_OUTCOMES=false` |
| `sensor` | Static (`Recreate`, không HPA) | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-aaa05bab...` | `SIM_MACHINE_COUNT=50`, `SIM_INTERVAL_MS=1000`, `SIM_INITIAL_STATE=PAUSED`, `SIM_SEND_WORKERS=32`, SQLite `/state/state.sqlite3` |
| `frontend` | HPA CPU 70% (`min=1, max=4`) | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-94248e49...` | `freshMs=2000`, `staleMs=5000`, OIDC PKCE `maintai-business-ui` |
| `simulator-ui` | Base Deployment | `1` | `100m` / — | `128Mi` / `1Gi` | `sha-94248e49...` | `pollIntervalMs=5000`, OIDC PKCE `maintai-simulator-ui` |
| `rag-service` | HPA CPU 75% (`min=1, max=3`) | `1` | `250m` / `2` | `512Mi` / `2Gi` | `maintai-local/rag-service` (pin SHA) | `MODEL_LOCAL_FILES_ONLY=true`, `OMP_NUM_THREADS=2`, Groq `openai/gpt-oss-20b` |
| Platform datastores (`postgres`, `kafka`, `minio`, `keycloak`, `mlflow`) | StatefulSet / Deployment | `1` mỗi thành phần | `postgres=100m`, `kafka/keycloak=250m`, `minio/mlflow=100m` | `postgres/kafka/keycloak: lim 2Gi`; `minio/mlflow: lim 1Gi` | Pin SHA256 digest | Xem mục 2.3 bên dưới |
| Observability (`prometheus`, `grafana`, `loki`, `promtail`, `pushgateway`) | StatefulSet / DaemonSet / Deployment | `1` mỗi thành phần | `25m–50m` | `loki/prom/grafana: lim 512Mi` | Pin SHA256 digest | `loki` giới hạn `512Mi` từng gặp `OOMKilled` khi log dày |

### 2.2. Ngân Sách `ConnectionPool` Tới `postgres-0`

`postgres-0` cấu hình `max_connections = 300`. Với `ConnectionPool` hữu hạn được chuẩn hóa trong [`backend/shared/persistence/pool.py`](../../backend/shared/persistence/pool.py) và [`processing/shared/persistence/pool.py`](../../processing/shared/persistence/pool.py):

| Dịch vụ | Công thức / Tham số Pool | Max conn / Pod | Số Pod (Baseline 50 máy) | Tổng Max Pool | Nguồn mã nguồn |
| --- | --- | ---: | ---: | ---: | --- |
| `processing-worker` | `size = max(12, max_active_keys + 4)` (`8+4=12`) | `12` | `6` | `72` | [`processing/worker/src/consumer/main.py`](../../processing/worker/src/consumer/main.py#L37-L40) |
| `processing-outbox-relay` | `size = 4` | `4` | `2` | `8` | [`processing/outbox-relay/src/relay/main.py`](../../processing/outbox-relay/src/relay/main.py#L75) |
| `business-consumer` | `size = 8` (mặc định `ConnectionPool`) | `8` | `4` | `32` | [`backend/business-consumer/src/runtime.py`](../../backend/business-consumer/src/runtime.py#L18) |
| `business-api` | `size = 12` (`statement_timeout=3000ms`) | `12` | `2` (max `4`) | `24` (max `48`) | [`backend/business-api/src/api/main.py`](../../backend/business-api/src/api/main.py#L29-L37) |
| `processing-read-api` | `source(4) + meta(4) + job(2) + catalog(2)` | `12` | `1` | `12` | [`processing/read-api/HISTORY.md`](../../processing/read-api/HISTORY.md) |
| **Tổng ngân sách tối đa** | Cộng dồn các pool chính (+ `drift`/`mlflow`/admin) | — | **15 pods giữ pool** | **`148` (< `300`)** | Dư biên > `130` kết nối so với trần `300` |

### 2.3. Tham Số Tinh Chỉnh Datastore, Transport & Simulator

* **PostgreSQL 17 (`local/platform/postgres.yaml`):** `max_connections=300`, `synchronous_commit=off` (tối ưu local SSD), `shared_buffers=512MB`, `wal_buffers=16MB`, `max_wal_size=2GB`, `checkpoint_timeout=15min`, `wal_writer_delay=20ms`, `limits.memory=2Gi`.
* **Kafka 4.0.0 KRaft (`local/platform/kafka.yaml` & `topics.sh`):** 1 broker local, 4 topics (`sensor.readings`, `sensor.rejected`, `inference.results`, `alert.signals`), **12 partitions/topic**, `RF=1`, `min.insync.replicas=1`, retention `7 ngày` (`168h`), `max.message.bytes=1 MiB`.
* **Keycloak Local Realm (`local/platform/maintai-realm.json`):** Claim `machine_ids` của `maintai-business-ui`, `maintai-simulator-ui`, `maintai-registry`, `maintai-processing`, `maintai-sensor`, `maintai-ml` đồng bộ danh sách 50 máy `M-001`..`M-050`.
* **Business SSE ([`backend/business-api/src/sse/hub.py`](../../backend/business-api/src/sse/hub.py) & [`backend/shared/persistence/projections.py`](../../backend/shared/persistence/projections.py)):** `SSESettings.coalesce_seconds = 0.05` (`50 ms`, cấu hình qua `BUSINESS_SSE_COALESCE_SECONDS`), `heartbeat_seconds = 15`, vòng `_pump` `25 ms` (`min(50ms, max(10ms, coalesce_seconds / 2))`), `Subscription.queue(maxsize=512)` (giữ nguyên budget bộ nhớ `connection_bytes = 2 MiB`, `total_bytes = 8 MiB`), đọc snapshot theo lô qua `ProjectionRepository.live_many(machine_ids)` (`WHERE m.machine_id = ANY(%s)`) khi nhiều máy cùng dirty trong 1 tick; trang chi tiết máy mở stream lọc riêng `?machine_id=M-XXX`, dashboard Fleet dùng `/api/v1/events/stream`.

---

## 3. Tóm Tắt 7 Nút Cổ Chai Đã Khắc Phục Trước Khi Chốt Baseline

1. **Quét spool SQLite ở `sensor` (`MaintAI` PR `#58`):** `StateStore.due()` trước đây quét toàn bảng khi hàng chờ tăng (`73.45 ms`); thêm index `spool_pending_created` và đổi sang join head theo máy giảm còn **`0.73–1.60 ms`** trên ~10k dòng.
2. **Ghim kết nối HTTP/1.1 L4 từ `sensor` vào 1 pod `ingestion-api` (`MaintAI-gitops` PR `#82`):** Phân tải qua nhiều replica `ingestion-api` giúp latency `202 Accepted` giảm từ `~284 ms` xuống `~13.6–17.4 ms` ở tải `50/s`.
3. **Nhịp poll Kafka & phân bổ `processing-worker` (`MaintAI-gitops` PR `#75`, `#84`):** Đặt `KAFKA_POLL_INTERVAL_SECONDS=0.01`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_REGISTRY_CACHE_SECONDS=30`.
4. **Hở connection PostgreSQL (`MaintAI` PR `#55`, `#56`):** Bổ sung bounded `ConnectionPool` cho `business-api`, `processing-worker`, `processing-outbox-relay` đồng nhất với `business-consumer`.
5. **Bão hòa RAM / Swap thrashing trên laptop 15 GiB RAM (`MaintAI-gitops` PR `#89`, `#96` + giảm tải `100 -> 50 req/s`):** Ở `100 req/s` với `processing-worker=12` và `business-consumer=4`, CPU bão hòa (`id = 1%`), swap giật tới `233 MB/s` (`356 MB/s` trước PR `#89`) và `local-business-live-v1` tụt `10,756`. Sau khi chốt baseline `50 req/s` (`6` workers + `4` business consumers + `2` ingestion + `2` ML + `2` relay), swap write `so` giảm về **`0`** và CPU còn rảnh `27–52%`.
6. **Lỗi query plan ở `maintai_kafka_outbox` (`MaintAI` PR `#59` @ `3b47520ef`):** Khi `maintai_kafka_outbox` (~1.36 triệu dòng) có burst ~40k `PENDING`, PostgreSQL chọn nhầm partial index `maintai_kafka_outbox_pending` (`estimated rows=1`, thực tế quét `2,546` dòng × `135` keys = **`520.9 ms` / `380,052` buffer hits**). Thêm `o.dispatch_seq IS NOT NULL` và đưa lọc `status` ra ngoài `LATERAL (... LIMIT 1)` trong `OrderedOutboxRelay._claim()`, kết hợp `COALESCE(status, '')='LEASED'` trong `OutboxRelay._finish_in_connection()` ép dùng unique index `maintai_outbox_dispatch_order` và Primary Key `maintai_kafka_outbox_pkey`, giảm truy vấn `_claim()` thực tế xuống **`8.35 ms` / `855` buffer hits** (**nhanh hơn 62×, giảm 444× buffer reads**).
7. **Độ trễ gom notification SSE (`coalesce_seconds = 1.0 -> 0.05`), giới hạn hàng đợi Fleet (`maxsize = 32 -> 512`) và đọc gộp `ProjectionRepository.live_many`:** Với `coalesce_seconds = 1.0` trước đây, telemetry (commit DB tại `p50 = 3.98 ms`) kích hoạt timer `1.0s`, còn `prediction` về sau tại `p50 = 51.03 ms` thường bị kẹt chờ hết cửa sổ `1.0s` (`Hop 2 avg = 498.88 ms, p95 = 1,024.67 ms`) và **`33.4–34.9%`** sự kiện ở nhịp `1 Hz` bị chu kỳ $n+1$ ghi đè trước khi kịp flush; đồng thời `Subscription.queue(maxsize=32)` khiến stream Fleet theo dõi 50 máy (`50 × 3 = 150` frames/chu kỳ) dễ chạm ngưỡng `GAP`. Sau khi giảm `coalesce_seconds` xuống `0.05` (`50 ms`, `_pump = 25 ms`), mở rộng `Subscription.queue(maxsize=512)` và gộp truy vấn bằng `ProjectionRepository.live_many(machine_ids)`, chặng `Hop 2` của `prediction.completed` giảm xuống **`avg = 53.42 ms / p50 = 42.40 ms / p95 = 110.83 ms`** (`0` sự kiện `> 500 ms`), tỷ lệ giao sự kiện lên UI đạt **`1,500 / 1,500` (`100.0%`, `0` bị ghi đè)** với **`0` ngắt `GAP`**.

---

## 4. Số Liệu Đo Đạc Thực Tế Trên Cụm Local Tại Baseline 50 Máy

*Thời điểm đo trên cụm `k3d-maintai-local`: `2026-11-21` (sau `T0 = 2026-11-21T02:18:48Z`), cấu hình 50 máy `M-001`..`M-050` ở trạng thái `RUNNING` (`M-051`..`M-100` ở trạng thái `PAUSED`), `spool = 0`, `sha-3b47520ef` trên `processing-outbox-relay`.*

### 4.1. Độ Ổn Định 5 Tầng Pipeline (Snapshot A @ `02:20:29Z` vs Snapshot B @ `02:21:34Z`)

| Tầng Pipeline | Snapshot A (`+65s`) | Snapshot B (`+130s`) | Kết luận |
| --- | --- | --- | --- |
| 1. `sensor` (`1` pod) | `50 RUNNING / 50 PAUSED`, `spool = 0`, `accepted_30s = 1,487` (`49.6/s`) | `50 RUNNING / 50 PAUSED`, `spool = 1`, `accepted_30s = 1,500` (`50.0/s`) | Phát đều đặn đúng `50 readings/s` |
| 2. `local-processing-v1` (`6` workers) | `total_lag = 0` (`max = 0`), `processed_30s = 1,493` (`49.8/s`) | `total_lag = 20` (`max = 4`), `processed_30s = 1,500` (`50.0/s`) | Không tích tụ lag |
| 3. `maintai_kafka_outbox` (`2` relays) | `PENDING = 3`, `LEASED = 1`, `pub_30s = 1,500` (`50.0/s`) | `PENDING = 0`, `LEASED = 0`, `lag_keys = 0`, `pub_30s = 1,507` (`50.2/s`) | Hết sạch hàng chờ outbox |
| 4. `local-business-live-v1` (`4` consumers) | `total_lag = 0` (`max = 0`) | `total_lag = 0` (`max = 0`) | Theo kịp 100% |
| 5. `local-business-prediction-v1` (`4` consumers) | `total_lag = 0` (`max = 0`) | `total_lag = 0` (`max = 0`) | Theo kịp 100% |
| Host & `postgres-0` | `postgres-0`: `970m / 992Mi`; `vmstat`: `id = 33–43%`, `si/so <= 8 MB/s`, `Avail RAM = 2.1 GiB` | `postgres-0`: `778m / 1,001Mi`; `vmstat`: `id = 27–52%`, **`so = 0`**, `si = 1.3–12.6 MB/s`, `Avail RAM = 1.8 GiB` | Không swap thrashing, 0 pipeline `OOMKilled` |

### 4.2. Metric A — Khoảng Cách Tiếp Nhận Ở Ingestion (`Interval = accepted_at(n) - accepted_at(n-1)`)

Đo trên cửa sổ `90s` ổn định (`>= T0 + 30s`) toàn bộ `50` máy (`processing_db.sensor_events`):

| Chỉ số Metric A | Giá trị đo được |
| --- | --- |
| Tổng số khoảng đo / Số máy | **`4,450` khoảng** trên **`50` máy** |
| Số lần nhảy thứ tự (`sequence_number - prev != 1`) | **`0`** |
| Trung bình / Trung vị (`p50`) | **`999.9 ms` / `1,000.1 ms`** |
| `p95` / `p99` | **`1,029.4 ms` / `1,091.8 ms`** |
| `min` / `max` | `772.6 ms` / `1,222.9 ms` |
| Tỷ lệ vượt ngưỡng `> 1,200 ms` / `> 1,500 ms` / `> 2,000 ms` | `3 / 4,450` (`0.07%`) / **`0`** / **`0`** |

### 4.3. Metric B — Độ Trễ Xử Lý & Giao Tới Giao Diện UI Trên Cả 50 Máy Đồng Thời (`Latency = ui_received_at - server_accepted_at`)

Phương pháp đo đồng thời trên toàn bộ **`50/50` máy (`M-001`..`M-050` ở nhịp `1 Hz`, cửa sổ `30s` = `1,500` readings)** trước và sau tối ưu SSE:
* Mở đồng thời `50` luồng SSE HTTPS đi qua Ingress (`https://app.maintai.localhost/api/v1/events/stream?machine_id=M-001..M-050`), `1` luồng tổng Fleet (`?` không lọc `machine_id`, theo dõi đủ 50 máy), và các luồng Direct HTTP trực tiếp tới từng pod `business-api` qua `port-forward` để ghép chính xác `100%` từng `frame_id` (`2,953/2,953` frame trước tối ưu; `5,446/5,446` frame sau tối ưu).
* Nhờ khớp đúng `frame_id` cho từng sự kiện `(machine_id, event_id)`, tổng độ trễ **Metric B** được tách cộng tính chính xác thành **3 chặng (`Hop 1 + Hop 2 + Hop 3 = Total E2E`)**.

1. **Đối chiếu Metric A (`Interval`) trong cùng cửa sổ đo `30s` (`1,500` events / `1,450` khoảng trên `50/50` máy):**

| Phiên bản đo (`30s` window) | Số khoảng / Nhảy thứ tự | `avg` | `p50` | `p95` | `p99` | `min` / `max` | `> 1.5s` |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| Trước tối ưu (`coalesce = 1.0`, `2026-11-21T17:14:55Z..17:15:25Z`) | `1,450` / **`0`** | `999.91 ms` | `1,000.14 ms` | `1,008.04 ms` | `1,021.63 ms` | `932.92` / `1,048.90 ms` | **`0`** |
| Sau tối ưu (`coalesce = 0.05`, `2026-11-21T17:35:22Z..17:35:52Z`) | `1,450` / **`0`** | `1,000.29 ms` | `1,000.16 ms` | `1,011.88 ms` | `1,024.45 ms` | `912.75` / `1,087.11 ms` | **`0`** |

2. **Chặng 1 (`Server Accepted → business_db Commit`, đo qua `maintai_kafka_inbox.processed_at`, `n = 1,500`):**

| Nhóm Consumer (`business_db`) | Giai đoạn | `n` | `avg` | `p50` | `p95` | `p99` | `max` | `> 500 ms` |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `telemetry` (`local-business-live-v1`) | Trước (`1.0s`) | 1,500 | `5.60 ms` | **`3.98 ms`** | `12.66 ms` | `25.47 ms` | `63.95 ms` | `0` |
| `telemetry` (`local-business-live-v1`) | Sau (`0.05s`) | 1,500 | `7.27 ms` | **`4.32 ms`** | `19.50 ms` | `42.45 ms` | `83.42 ms` | `0` |
| `prediction` (`local-business-prediction-v1`) | Trước (`1.0s`) | 1,500 | `60.19 ms` | **`51.03 ms`** | `119.41 ms` | `177.05 ms` | `241.25 ms` | `0` |
| `prediction` (`local-business-prediction-v1`) | Sau (`0.05s`) | 1,500 | `70.06 ms` | **`55.71 ms`** | `164.59 ms` | `226.02 ms` | `375.85 ms` | `0` |

3. **Chặng 2 (`business_db Commit → SSE Sent [Direct Pod]`) — Trọng tâm tối ưu `NotificationHub`:**

| Sự kiện SSE | Giai đoạn | Mẫu khớp (`n / 1,500`) | `avg` | `p50` | `p95` | `p99` | `max` | `> 500 ms` |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `telemetry.received (first)` | Trước (`1.0s`) | `977` | `60.85 ms` | `39.67 ms` | `119.65 ms` | `977.87 ms` | `1,024.07 ms` | `15` |
| `telemetry.received (first)` | **Sau (`0.05s`)** | **`1,500`** | **`51.74 ms`** | `42.09 ms` | `106.08 ms` | **`135.46 ms`** | **`216.22 ms`** | **`0`** |
| `processing.status_changed (COMPLETED)` | Trước (`1.0s`) | `538` | `77.39 ms` | `44.13 ms` | `156.34 ms` | `988.84 ms` | `1,024.22 ms` | `15` |
| `processing.status_changed (COMPLETED)` | **Sau (`0.05s`)** | **`1,500`** | **`54.72 ms`** | `43.18 ms` | `114.66 ms` | **`161.32 ms`** | **`275.95 ms`** | **`0`** |
| `prediction.completed` | Trước (`1.0s`) | `999` | `498.88 ms` | `132.32 ms` | `1,024.67 ms` | `1,051.01 ms` | `1,080.55 ms` | `476 (47.6%)` |
| `prediction.completed` | **Sau (`0.05s`)** | **`1,500`** | **`53.42 ms`** *(-89.3%)* | **`42.40 ms`** *(-68.0%)* | **`110.83 ms`** *(-89.2%)* | **`152.05 ms`** *(-85.5%)* | **`252.12 ms`** | **`0 (0.0%)`** |

4. **Chặng 3 (`SSE Sent → Browser Received [Ingress HTTPS]`):**
   * Trên toàn bộ **`2,953 / 2,953`** frame khớp trước tối ưu và **`5,446 / 5,446`** frame khớp sau tối ưu (đi qua Nginx Ingress với `proxy-buffering: "off"`): `p50 ≈ -0.32 đến -0.91 ms` (lệch jitter port-forward Direct vs loopback Ingress), `p95 <= +1.0 ms`, `p99 <= +3.02 ms`, `max <= +18.41 ms`, **`0` frame `> 500 ms`**. Ingress HTTPS truyền frame gần như tức thời.

5. **Tổng độ trễ Metric B (`Server Accepted → Browser Received` trên 50 luồng Ingress HTTPS đồng thời) & Tỷ lệ giao đủ sự kiện:**

| Chỉ số (`30s` / `1,500` events trên `50` máy) | Trước tối ưu (`coalesce = 1.0`, `maxsize = 32`) | Sau tối ưu (`coalesce = 0.05`, `maxsize = 512`, `live_many`) | Mức cải thiện |
| --- | --- | --- | --- |
| **`telemetry.received (first)` E2E** | `n = 977` *(34.9% bị ghi đè)* · `avg = 91.66 ms`, `p50 = 67.46 ms`, `p95 = 170.99 ms`, `p99 = 1,021.61 ms`, `max = 1,081.82 ms` (`>1s: 15`) | **`n = 1,500 (100%)`** · `avg = 86.97 ms`, `p50 = 83.27 ms`, `p95 = 125.70 ms`, **`p99 = 173.08 ms`**, **`max = 279.90 ms`** (`>500ms: 0, >1s: 0`) | Đủ `100%` readings, `p99` giảm **`83.1%`** |
| **`processing.status_changed (COMPLETED)` E2E** | `n = 538` · `avg = 129.54 ms`, `p50 = 97.32 ms`, `p95 = 224.33 ms`, `p99 = 1,035.79 ms`, `max = 1,084.00 ms` (`>1s: 15`) | **`n = 1,500 (100%)`** · `avg = 123.37 ms`, `p50 = 89.46 ms`, `p95 = 272.25 ms`, **`p99 = 350.71 ms`**, `max = 559.38 ms` (`>500ms: 1, >1s: 0`) | `>1s` về **`0`**, `p99` giảm **`66.1%`** |
| **`prediction.completed` E2E** | `n = 999` *(33.4% bị ghi đè)* · `avg = 553.22 ms`, `p50 = 190.11 ms`, `p95 = 1,077.02 ms`, `p99 = 1,118.01 ms`, `max = 1,161.04 ms` (`>1s: 476 / 47.6%`) | **`n = 1,500 (100%)`** · **`avg = 121.93 ms`**, **`p50 = 88.39 ms`**, **`p95 = 266.13 ms`**, **`p99 = 348.81 ms`**, `max = 559.24 ms` (`>500ms: 1 / 0.07%`, **`>1s: 0`**) | **`avg -78.0%`**, **`p95 -75.3%`**, **`>1s: 47.6% → 0%`** |
| **Tỷ lệ sự kiện bị ghi đè trước khi flush** | Telemetry `523/1,500` (`34.9%`) · Prediction `501/1,500` (`33.4%`) | **Telemetry `0/1,500` (`0%`) · Processing `0/1,500` (`0%`) · Prediction `0/1,500` (`0%`)** | **Giao đủ `100.0%` trên cả 50 máy** |
| **Fleet stream (`50` máy trong 1 kết nối)** | `3,400` frames (`2,946` khớp cửa sổ) | **`6,293` frames (`5,430` khớp cửa sổ, đủ `50/50` máy, sống `30.0s`, `0` `GAP`)** | Tăng **`+84.3%`** frame khớp không rớt kết nối |

*Ghi chú về ngưỡng `coalesce_seconds = 0.05` (`50 ms`, `_pump = 25 ms`):* Do `local-business-live-v1` commit tại `p50 = 4.32 ms` và `local-business-prediction-v1` commit tại `p50 = 55.71 ms`, cửa sổ `50 ms` cho phép ~`68–77%` số chu kỳ gom tự nhiên `telemetry` (`VALID`) + `processing` (`COMPLETED`) + `prediction` vào cùng một lần flush tại **`p50 = 83–89 ms` E2E** mà không giữ reading chờ quá chu kỳ `1,000 ms`; các reading mà `prediction` về trễ hơn `~75 ms` sẽ đẩy ngay `UNVERIFIED/PENDING` trước rồi đẩy `prediction.completed` sau ~`42 ms` kể từ lúc DB commit (`Hop 2 p95 = 110.83 ms`, `max = 252.12 ms`). Trong 1 lượt đo chuyển tiếp khi HPA scale `business-api` từ `1 -> 3` pods giữa cửa sổ (`2026-11-21T17:33:28Z..17:33:58Z`), `Hop 2` của `prediction.completed` vẫn đạt `avg = 51.13 ms, p95 = 131.15 ms, max = 278.44 ms` (`0 > 500ms`) và `0/1,500` bị ghi đè, chỉ có `2/1,500` (`0.13%`) E2E `> 1s` do tail `Hop 1` (`p95 = 311.46 ms`) lúc khởi tạo thêm 2 pods; ở trạng thái ổn định ngay sau đó, `100%` (`1,500/1,500`) sự kiện tới trình duyệt trong `<= 559.38 ms`.

*Lưu ý đối chiếu với lần đo sơ khởi ban đầu (`40s` trên 5 máy `M-001, M-013, M-025, M-038, M-050` với `coalesce_seconds = 1.0`, `413` frames):* Kết quả trước tối ưu trên 5 máy (`prediction.completed avg = 670.0 ms, p50 = 1,026.3 ms`) nhất quán hoàn toàn với kết luận trên cả 50 máy đồng thời rằng `Hop 2` (`coalesce_seconds = 1.0`) là nguyên nhân duy nhất gây đuôi ~`1s`, trong khi `Hop 1` và `Hop 3` đều rất nhanh.

---

## 5. Vận Hành Bộ Nhớ, Đĩa `/data` & Khác Biệt Kiểm Chứng

* **OOM / RAM:** Không có pod pipeline nào bị `OOMKilled` trong suốt quá trình chạy `50 req/s` và `100 req/s`. Chỉ pod log `loki` (`limits.memory = 512Mi`) từng bị `OOMKilled` 3 lần khi mật độ log cao.
* **Ngưỡng đĩa k3s (`DiskPressure`):** k3s trên node đơn kích hoạt `DiskPressure=True` (`node.kubernetes.io/disk-pressure:NoSchedule`) khi phân vùng `/data` vượt `85%` (`<15%` trống), đồng thời kubelet image GC dọn các image nhàn rỗi như `maintai-local/dev-tools`. Đã thực hiện dọn volume/dữ liệu test cũ và image dư thừa giúp `/data` giảm từ **`164 GiB / 192 GiB` (`86%`, `DiskPressure=True`)** xuống **`130–133 GiB / 192 GiB` (`68–70%`, `59–62 GiB` trống, `DiskPressure=False`)**. Khi vận hành local baseline 50 máy, cần giữ `/data` còn trống `> 15–20%` và tạm **Pause** các máy trên Simulator UI khi không theo dõi.
* **Ràng buộc chuyển cấu hình `SIM_MACHINE_COUNT=100 -> 50` trên PVC `sensor` cũ:** Hàm `StateStore.initialize()` tại [`sensor/src/simulator/ordering/store.py`](../../sensor/src/simulator/ordering/store.py#L111-L119) lưu `source_identity` gắn với `machine_count`. PVC `sensor` đã được khởi tạo sạch ở đúng `50` máy (`M-001`..`M-050`, `PAUSED`, `spool = 0`), đồng bộ với danh sách `machine_ids` trong Keycloak realm local.
* **Phạm vi bằng chứng:**
  * *Unit & Contract CI:* `MaintAI` PR `#59` pass `57/57` checks (gồm `kafka-ci` và `kafka-integration`); `ruff check`/`format` (`266` files) và `pytest` cho `backend/business-api` (`5` tests SSE core) cùng suite `processing` + `backend` (`-m 'not integration'`) pass sau tối ưu SSE; `MaintAI-gitops#96`, `#97` pass `CI / required` (`kubectl kustomize` + `kubeconform`).
  * *Đo kiểm cụm sống:* Thực hiện trực tiếp trên `k3d-maintai-local` tại tải `50 req/s` trên đủ `50/50` máy (`M-001`..`M-050` ở trạng thái `RUNNING`, `spool = 0`) cho cả cấu hình trước (`coalesce_seconds=1.0`) và sau tối ưu (`coalesce_seconds=0.05`, `maxsize=512`, `ProjectionRepository.live_many`), sau đó đưa toàn bộ 50 máy về `PAUSED`.
