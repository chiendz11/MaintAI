# Báo Cáo Baseline Vận Hành Local 50 Máy & Cấu Hình Cụm (`k3d`)

**Ngày ghi nhận:** 2026-11-21  
**Phiên bản nguồn:** `MaintAI@3b47520ef87cbed0f8f567696ef43db86206306b` (kèm báo cáo này) · `MaintAI-gitops@66be41c5728291701596a4f92a36a228eb25a320`  
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
| `business-api` | HPA CPU 70% (`min=1, max=4`) | `2` *(live)* | `100m` / — | `128Mi` / `1Gi` | `sha-648f1ef3...` | `SSESettings.coalesce_seconds=1.0`, `max_connections=128`, `Subscription.queue maxsize=32` |
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
* **Business SSE (`backend/business-api/src/sse/hub.py`):** `SSESettings.coalesce_seconds = 1.0`, `heartbeat_seconds = 15`, vòng pump `50 ms`, `Subscription.queue(maxsize=32)`, trang chi tiết máy mở stream lọc riêng `?machine_id=M-XXX`.

---

## 3. Tóm Tắt 6 Nút Cổ Chai Đã Khắc Phục Trước Khi Chốt Baseline

1. **Quét spool SQLite ở `sensor` (`MaintAI` PR `#58`):** `StateStore.due()` trước đây quét toàn bảng khi hàng chờ tăng (`73.45 ms`); thêm index `spool_pending_created` và đổi sang join head theo máy giảm còn **`0.73–1.60 ms`** trên ~10k dòng.
2. **Ghim kết nối HTTP/1.1 L4 từ `sensor` vào 1 pod `ingestion-api` (`MaintAI-gitops` PR `#82`):** Phân tải qua nhiều replica `ingestion-api` giúp latency `202 Accepted` giảm từ `~284 ms` xuống `~13.6–17.4 ms` ở tải `50/s`.
3. **Nhịp poll Kafka & phân bổ `processing-worker` (`MaintAI-gitops` PR `#75`, `#84`):** Đặt `KAFKA_POLL_INTERVAL_SECONDS=0.01`, `PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_REGISTRY_CACHE_SECONDS=30`.
4. **Hở connection PostgreSQL (`MaintAI` PR `#55`, `#56`):** Bổ sung bounded `ConnectionPool` cho `business-api`, `processing-worker`, `processing-outbox-relay` đồng nhất với `business-consumer`.
5. **Bão hòa RAM / Swap thrashing trên laptop 15 GiB RAM (`MaintAI-gitops` PR `#89`, `#96` + giảm tải `100 -> 50 req/s`):** Ở `100 req/s` với `processing-worker=12` và `business-consumer=4`, CPU bão hòa (`id = 1%`), swap giật tới `233 MB/s` (`356 MB/s` trước PR `#89`) và `local-business-live-v1` tụt `10,756`. Sau khi chốt baseline `50 req/s` (`6` workers + `4` business consumers + `2` ingestion + `2` ML + `2` relay), swap write `so` giảm về **`0`** và CPU còn rảnh `27–52%`.
6. **Lỗi query plan ở `maintai_kafka_outbox` (`MaintAI` PR `#59` @ `3b47520ef`):** Khi `maintai_kafka_outbox` (~1.36 triệu dòng) có burst ~40k `PENDING`, PostgreSQL chọn nhầm partial index `maintai_kafka_outbox_pending` (`estimated rows=1`, thực tế quét `2,546` dòng × `135` keys = **`520.9 ms` / `380,052` buffer hits**). Thêm `o.dispatch_seq IS NOT NULL` và đưa lọc `status` ra ngoài `LATERAL (... LIMIT 1)` trong `OrderedOutboxRelay._claim()`, kết hợp `COALESCE(status, '')='LEASED'` trong `OutboxRelay._finish_in_connection()` ép dùng unique index `maintai_outbox_dispatch_order` và Primary Key `maintai_kafka_outbox_pkey`, giảm truy vấn `_claim()` thực tế xuống **`8.35 ms` / `855` buffer hits** (**nhanh hơn 62×, giảm 444× buffer reads**).

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

### 4.3. Metric B — Độ Trễ Xử Lý & Giao Tới Giao Diện UI (`Latency = ui_received_at - server_accepted_at`)

1. **B1 — Luồng SSE xác thực OAuth2 PKCE thực tế (`40s` trên 5 máy `M-001, M-013, M-025, M-038, M-050`, tổng `413` frame mới khớp chính xác `(machine_id, event_id)` với `processing_db.sensor_events`):**

| Sự kiện SSE | Mẫu khớp | `avg` | `p50` | `p95` | `p99` | `min` | `max` | `> 2s` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `telemetry.received` | 138 | `179.9 ms` | **`95.6 ms`** | `1,021.3 ms` | `1,097.5 ms` | `20.6 ms` | `1,234.1 ms` | **`0%`** |
| `processing.status_changed` | 138 | `180.7 ms` | **`96.0 ms`** | `1,021.6 ms` | `1,099.7 ms` | `22.0 ms` | `1,234.3 ms` | **`0%`** |
| `prediction.completed` | 137 | `670.0 ms` | `1,026.3 ms` | `1,120.3 ms` | `1,175.2 ms` | `56.8 ms` | `1,234.2 ms` | **`0%`** |

2. **B2 — Đối chiếu độ trễ ghi vào `business_db` trên toàn bộ `50/50` máy (`db_updated_at - server_accepted_at`):**

| Bảng projection (`business_db`) | Số máy khớp | `avg` | `p50` | `p95` | `p99` | `max` |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `latest_telemetry` | `50/50` | `97.7 ms` | **`93.6 ms`** | `200.5 ms` | `216.2 ms` | `216.4 ms` |
| `latest_processing` | `50/50` | `97.4 ms` | **`93.6 ms`** | `199.8 ms` | `215.4 ms` | `216.0 ms` |
| `latest_valid_prediction` | `50/50` | `146.2 ms` | **`146.5 ms`** | `218.8 ms` | `249.7 ms` | `251.4 ms` |

*Giải thích cơ chế:* Toàn bộ pipeline xử lý từ HTTP `202 Accepted` đến khi ghi xong dự đoán AI vào `business_db` chỉ mất **`93.6–146.5 ms` trung vị (`<= 251.4 ms` max)**. Khi lên SSE, `NotificationHub` có `SSESettings.coalesce_seconds = 1.0` gộp các `PG NOTIFY` của cùng máy trong cửa sổ `1.0s`: `telemetry` kích hoạt lần flush đầu tiên (`~95.6 ms`), còn `prediction` về sau vài chục ms thường chờ thêm 1 nhịp `1.0s` (`p50 = 1,026.3 ms`). Toàn bộ `413/413` frame đều tới UI trong **`<= 1.23 giây`**.

---

## 5. Vận Hành Bộ Nhớ, Đĩa `/data` & Khác Biệt Kiểm Chứng

* **OOM / RAM:** Không có pod pipeline nào bị `OOMKilled` trong suốt quá trình chạy `50 req/s` và `100 req/s`. Chỉ pod log `loki` (`limits.memory = 512Mi`) từng bị `OOMKilled` 3 lần khi mật độ log cao.
* **Ngưỡng đĩa k3s (`DiskPressure`):** k3s trên node đơn kích hoạt `DiskPressure=True` (`node.kubernetes.io/disk-pressure:NoSchedule`) khi phân vùng `/data` vượt `85%` (`<15%` trống), đồng thời kubelet image GC dọn các image nhàn rỗi như `maintai-local/dev-tools`. Trong quá trình chạy dài sau test (`>4h` tích lũy dữ liệu), node đã chạm `DiskPressure=True` tại `2026-11-21T10:21:12+07:00`. Khi vận hành local baseline 50 máy, cần giữ `/data` còn trống `> 15–20%` và tạm **Pause** các máy trên Simulator UI khi không theo dõi.
* **Ràng buộc chuyển cấu hình `SIM_MACHINE_COUNT=100 -> 50` trên PVC `sensor` cũ:** Hàm `StateStore.initialize()` tại [`sensor/src/simulator/ordering/store.py`](../../sensor/src/simulator/ordering/store.py#L111-L119) lưu `source_identity` gắn với `machine_count`; việc đổi `SIM_MACHINE_COUNT` trên PVC `/state/state.sqlite3` đã từng khởi tạo với 100 máy sẽ bị từ chối khi pod khởi động lại, đòi hỏi xóa/tách PVC cũ hoặc thực hiện migration state volume rõ ràng (cùng việc đồng bộ lại Keycloak realm nếu realm đã được bootstrap với 100 máy).
* **Phạm vi bằng chứng:**
  * *Unit & Contract CI:* `MaintAI` PR `#59` pass `57/57` checks (gồm `kafka-ci` và `kafka-integration`); `ruff`, `lock_contracts.py --check`, `pytest` (`88 passed`); `MaintAI-gitops#96`, `#97` pass `CI / required` (`kubectl kustomize` + `kubeconform`).
  * *Đo kiểm cụm sống:* Thực hiện trực tiếp trên `k3d-maintai-local` tại tải `50 req/s` (`50 RUNNING / 50 PAUSED` vào thời điểm đo). Sau khi sửa GitOps sang `SIM_MACHINE_COUNT=50`, chưa restart lại PVC `sensor` cũ để tránh tự ý xóa state volume đang có và do node đang có `DiskPressure=True` cần giải phóng đĩa trước.
