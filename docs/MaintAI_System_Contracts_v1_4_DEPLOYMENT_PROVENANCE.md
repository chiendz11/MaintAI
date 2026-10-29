> **Source update23/10 — SDK0.2.0/bundle1.1.0:** keyed runner/coordinator,
> durable handler/DB fence, ordered relay, migration002 và transport wrappers đã
> có source/tests. Full app callbacks/probes/images/deployment còn cần tích hợp.
> [Implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) và
> readiness §19 ghi bằng chứng, commands và giới hạn; các snapshot cũ đọc theo ngày.

> **Tích hợp MaintAI — 2026-10-19:** Tài liệu thiết kế mục tiêu v1.4, chưa xác nhận code/DB/GitOps đã migrate. Đọc [docs index](README.md) để áp dụng quyết định nhóm 4 người, Simulator UI riêng và thứ tự ưu tiên; [readiness](implementation-readiness.md) ghi các schema/scaffold chưa được cung cấp.

> **v1.4 — Chuẩn kiến trúc Deployment và Versioning/Provenance (19/10/2026).** Đây là bản đầy đủ kế thừa v1.3.1 và bổ sung điều khoản v1.4. Khi có mâu thuẫn, **ưu tiên chương v1.4**. Việc chỉnh sửa Markdown không đồng nghĩa đã sửa repository, GitOps, DB hay các service thực tế.
> **Thứ tự đọc:** `DEPLOYMENT_ARCHITECTURE.md`, `RUNTIME_DATAFLOW.md`, `VERSIONING_AND_PROVENANCE.md`, `IMPLEMENTATION_PLAN.md`. Schemas máy đọc và mẫu Deployments tại `../contracts/`, `../implementation/`.

# MaintAI — Hợp đồng hệ thống v1.4: Deployment và truy vết nguồn gốc (kế thừa đầy đủ v1.3.1)

> **PROC-ORDER-01 — 2026-10-23:** §42 thông qua
> [Processing concurrency/ordering](PROCESSING_CONCURRENCY_AND_ORDERING.md)
> revision1.0.0. Các điều khoản keyed execution, safe offset frontier và ordered
> outbox ở đây ưu tiên mô tả tuần tự/row-only relay kế thừa. Đây là target mới,
> SDK0.2.0 đã có implementation/migration/tests; app/prod integration còn pending.
> Wire versions và deployment boundaries giữ nguyên.

> **OBS-01 / MLFLOW-01 — 2026-10-20:** System Contracts §41 và Guide §49
> thông qua [metrics catalog](OBSERVABILITY_IMPLEMENTATION.md) và
> [MLflow integration](MLFLOW_INTEGRATION_GUIDE.md) revision1.0.0. Các chi tiết
> names/types/labels/buckets, SDK/identity/recovery ưu tiên ví dụ kế thừa mâu thuẫn;
> docs không tự migrate app, schemas, ledger hoặc platform.

> **UI-01 — 2026-10-20:** §40 và [UI Architecture](UI_ARCHITECTURE.md) chốt bốn UI
> chính, hai frontend ứng dụng phát hành độc lập và hai UI công cụ platform.
> Quyết định này bổ sung ranh giới UI/release, không đổi wire/API/DB schemas hoặc
> chứng minh frontend/Control API đã implement. Các mô tả UI kế thừa đọc theo §40.

> **Vai trò:** Tài liệu quy chuẩn (Source of Truth) cho Ingestion API độc lập, Processing Service, Business Backend (API + Consumer), Simulator, Kafka, Storage, ML, RAG, Drift, React, PostgreSQL/pgvector và Observability.
>
> **Cơ sở:** MaintAI – hệ thống dự đoán hỏng máy và hỗ trợ kỹ thuật viên; dùng AI4I 2020, phân loại lỗi nhị phân, Alert không phụ thuộc LLM, có kỹ thuật viên giám sát và truy vết xuyên suốt.
>
> **Phiên bản tài liệu kế thừa:** `1.3.1` (thuộc dòng v1.3), đã được mở rộng lên v1.4 trong các chương 34–39; các event schema có phiên bản riêng; bổ sung UI thời gian thực theo hai luồng, sự kiện kết quả inference và telemetry projection. Đây là đặc tả mục tiêu; implementation/OpenAPI hiện có phải migrate riêng.
>
> **Nền tảng 1.2.0:** Kafka sensor.readings + raw S3 consumer; split Processing/Business DB ownership; Ingestion HTTP 202; alert.signals/outbox/inbox; Drift qua API/S3. Kế thừa toàn bộ DRIFT/ML/RAG invariants 1.1.0 trừ các điểm migration được đánh dấu.


> **Quyết định kế thừa1.3.1 — áp dụng dưới các điều khoản v1.4:** Đọc **§32** cùng §31. Topic chính thức đổi từ `sensor.raw` (tài liệu 1.2 cũ) thành **`sensor.readings`**, bổ sung topic `inference.results` và luồng UI **Live Telemetry + Latest Valid Prediction**. Thuật ngữ SSE “optional” trong các chương kế thừa 1.1/1.2 được thay bằng SSE ưu tiên cho bản demo realtime; REST snapshot vẫn là source of truth để khôi phục. Các mô tả “prediction hiện tại null” của kết quả event invalid vẫn đúng, nhưng **không được xoá `latest_valid_prediction` của lịch sử hiển thị**.

---

## 0. Mục tiêu và nguyên tắc bắt buộc

Tài liệu này nhằm giúp 4 thành viên phát triển độc lập nhưng tích hợp được ngay khi ghép service.

Các nguyên tắc không được phá vỡ:

1. Chỉ sensor event có `validation_status = VALID` mới được đưa vào ML inference.
2. `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE` không bao giờ được chuyển thành `NORMAL`.
3. ML chỉ dự đoán và trả risk/probability; ML Service không tạo alert.
4. Risk Decision Engine của Backend chuyển prediction thành trạng thái/risk level theo `policy_version`.
5. Processing Risk Decision phát Alert Signal; **Business Consumer/API** sở hữu Alert Manager và vòng đời alert; alert path không phụ thuộc RAG/LLM.
6. RAG/LLM chỉ enrich alert bằng Maintenance Brief/Checklist có citation.
7. Nếu RAG không đủ bằng chứng, phải trả `INSUFFICIENT_EVIDENCE`, không tự bịa hướng dẫn.
8. Technician là người quyết định thao tác bảo trì. MaintAI không điều khiển PLC/actuator.
9. Mọi prediction/alert phải truy ngược được về sensor event, `model_version`, `policy_version` và thao tác người dùng.
10. `machine_id` và timestamp do simulator tạo chỉ phục vụ mô phỏng hệ thống, không được dùng như historical ML features của AI4I.
11. Drift Monitoring chạy ngoài critical processing path; Drift Worker lỗi/chậm không được làm gián đoạn Ingestion → Kafka → Processing → Risk → Business Alert.
12. `INVALID_DATA` và `DATA_DRIFT` là hai khái niệm khác nhau: invalid là lỗi ở record-level; drift là thay đổi phân bố ở window-level. Drift không tự động đổi prediction, severity hay risk policy trong MVP.

---

# 1. Contract Map toàn hệ thống — Scale-Ready + Live Telemetry + Latest Valid Prediction (v1.3.1)

**Chuẩn hiện hành:** Ingestion API độc lập với Business Backend. Business Backend chỉ sở hữu Business API, Business Consumer (telemetry, prediction, alert handlers), read projections và alert lifecycle. `Prediction Consumer` chỉ sao chép kết quả để hiển thị; **ML computation và prediction history** thuộc Processing Service. Không ghi đầy đủ raw history vào `business_db`.

```text
Sensor Simulator [RUNNING | PAUSED; local retry/rejected log; scenarios]
    | HTTP POST (new records only if RUNNING)
    v
INGESTION API [separate deployment; transport auth, envelope, Kafka producer]
    | Kafka broker ACK -> HTTP 202; failures -> retry/local buffer
    v
KAFKA sensor.readings [key = machine_id]
    |                       |                         |
    | group processing     | group business-live     | group raw-archive
    v                       v                         v
PROCESSING WORKER      BUSINESS CONSUMER         STORAGE CONSUMER
 DQ -> ML Service       telemetry handler         batch JSONL.gz
    -> risk              (latest snapshot)         -> S3/MinIO raw
    |                    |                         [all accepted raw]
 processing_db          |
    + outbox             |    Kafka inference.results <--- Processing outbox
    |                    +<-- business prediction handler (no ML computation)
    +--> alert.signals ------> business alert handler (dedupe/lifecycle)
                               |
                        business_db (machines, alerts, latest UI projections)
                               | durable projection commit -> NOTIFY / pub-sub
                        BUSINESS API Deployment [REST + SSE]
                               |
                          REACT FRONTEND
                   Live Telemetry (may be UNVERIFIED)
                   Latest Valid Prediction (separate event, age)
                   Latest Processing + Active Alerts
                   Simulator Control (demo only; Business API facade)
                               |
                    POST /internal/simulator/... (authorized)
                               v
                    Sensor Simulator Control API
                    (pause/resume/scenario per machine)

Drift CronJob: S3 curated / Processing read API -> drift_db + S3 drift-reports
Training Job: curated S3 -> MLflow API -> mlflow_db + S3 model artifacts
RAG/LLM: knowledge_db (pgvector) + S3 manuals; invoked off alert critical path
POSTGRESQL: 1 instance, 5 logical DBs: processing, business, drift, mlflow, knowledge
```

**Kafka ownership:** `sensor.readings` được consume bởi 3 nhóm độc lập: `processing-v1`, `raw-archive-v1`, `business-live-v1`. `business-consumer` còn consume `inference.results` và `alert.signals`; các handler là module, không phải ba microservices. Mỗi topic có consumer group riêng theo subscription, không buộc một group ID chung cho cả ba topic. Business Consumer có thể là một process chạy nhiều loops hoặc một process với client subscribed nhiều topics nếu tuân thủ offset/retry policies. `business-api` **không** tự consume Kafka để đáp ứng SSE.

**SSE trigger không đòi ghi raw history vào database**: Business Consumer materialize **latest snapshot** vào business_db để REST/reconnect và phát tín hiệu sau commit qua PG LISTEN/NOTIFY (MVP) hoặc pub/sub broadcast. Không lưu tất cả raw records ở Business. S3 archive độc lập là nguồn raw history dài hạn. Do notify không bền vững, REST bootstrap/reconnect định kỳ/đúng lúc là cơ chế hồi phục.

## 1.1. Ownership và interface

| Runtime / trách nhiệm | Sở hữu | Triển khai / dữ liệu |
|---|---|---|
| Sensor Simulator + control API | `sensor/` | Một control instance demo; per-machine RUNNING/PAUSED, scenario, retry buffer; không là PLC |
| Ingestion API | Ingestion boundary riêng, **không nằm trong Business Backend** | Deployment `ingestion-api`; HTTP → Kafka; không DB, không DQ sâu, không ML |
| Processing Worker | Processing Service | Deployment `processing-worker`; DQ, ML client, risk, `processing_db`, transactional outbox |
| ML Inference Service | ML serving | Deployment `ml-service`; model predict, không tạo risk alert |
| Storage Consumer | Raw archive | Deployment `storage-consumer`; `sensor.readings` → S3; không ghi `business_db` |
| Business Consumer (3 handlers) | Business Backend | Deployment `business-consumer`; telemetry/prediction **projection** và alert handler; không chạy ML |
| Business API | Business Backend | Deployment `business-api`; REST, SSE, RBAC, alerts/feedback, history/brief facade |
| `business_db` | Business Backend | Machine registry, alert lifecycle, read models **chỉ latest telemetry/last valid prediction/processing status**, không full raw history |
| Drift Worker | Drift | CronJob; API/S3 read, `drift_db`; không consume raw trực tiếp theo mặc định |
| ML Training + MLflow Server | Training/Registry | MLflow API, `mlflow_db`, S3; không retrain chỉ vì raw file được upload |
| RAG/LLM | Knowledge | `knowledge_db`+S3 manuals; copilot độc lập alert path |
| React Frontend | UI | Chỉ Business REST/SSE; live telemetry và prediction là 2 dữ liệu có nguồn thời gian khác nhau |

## 1.2. Database, ordering, scaling

Năm logical DB trên một PostgreSQL instance (`processing_db`, `business_db`, `drift_db`, `mlflow_db`, `knowledge_db`). Chỉ Processing ghi full processing event/history; S3/MinIO chứa raw archive; Business ghi alert và **latest read projections**. Không cross-service direct SQL; không cần Redis khi MVP dùng PG NOTIFY, nhưng cần tính đến tải pub/sub và nhiều SSE Pod. Kafka partition key = `machine_id`, `sequence_number` tăng đơn điệu theo máy cùng `stream_epoch` (nếu simulator restart/reset). Các projector phải compare-and-set ordering token để không overwrite kết quả mới bằng event cũ; không sort theo `event_id` hoặc inference completion time.

# 2. Quy ước chung

## 2.1. API version

Tất cả public/internal HTTP API dùng prefix:

```text
/api/v1
```

Thay đổi breaking contract phải tăng major version:

```text
/api/v2
```

Không dùng URL version cho model artifact. Model dùng `model_version` riêng.

---

## 2.2. Content type

```http
Content-Type: application/json
Accept: application/json
```

`/metrics` dùng Prometheus text format.

---

## 2.3. Timestamp

Dùng RFC 3339 / ISO-8601 có timezone.

Khuyến nghị lưu nội bộ ở UTC:

```text
2026-10-12T04:30:15.231Z
```

Frontend có thể convert về timezone người dùng.

Không gửi timestamp không timezone như:

```text
2026-10-12 11:30:15
```

---

## 2.4. ID

Các ID phải là opaque string; consumer không được parse ý nghĩa từ ID.

Khuyến nghị:

```text
event_id       evt_<uuid>
prediction_id  pred_<uuid>
alert_id       alt_<uuid>
brief_id       brf_<uuid>
feedback_id    fb_<uuid>
request_id     req_<uuid>
correlation_id cor_<uuid>
```

`machine_id` có thể do simulator cấu hình:

```text
M-001
M-002
M-003
```

---

## 2.5. Correlation / trace headers

Mọi request giữa service phải truyền:

```http
X-Request-ID: req_...
X-Correlation-ID: cor_...
```

Quy tắc:

- Nếu incoming request chưa có `X-Request-ID`, service tạo mới.
- `X-Correlation-ID` được giữ xuyên suốt một flow SensorEvent → Prediction → Alert → RAG.
- Mỗi hop có thể có `request_id` riêng nhưng cùng `correlation_id`.
- Log bắt buộc có hai field này nếu có context.

---

## 2.6. JSON naming

Dùng `snake_case` thống nhất cho API payload.

Đúng:

```json
{"failure_probability": 0.91}
```

Không dùng lẫn:

```json
{"failureProbability": 0.91}
```

---

## 2.7. Null / missing

- Field bắt buộc bị thiếu → validation error.
- Không dùng `null` để thay cho một state có ý nghĩa.
- Dùng enum rõ ràng như `UNKNOWN`, `INVALID`, `UNAVAILABLE`.

---

# 3. Common Enums

## 3.1. DataValidationStatus

```text
VALID
INVALID
```

Không có `NORMAL` ở tầng validation.

---

## 3.2. MachineOperationalState

```text
NORMAL
WATCH
HIGH
CRITICAL
INVALID
UNKNOWN
```

Ý nghĩa:

- `NORMAL`: prediction hợp lệ và risk policy đánh giá bình thường.
- `WATCH`: cần theo dõi.
- `HIGH`: nguy cơ cao, thường cần alert tùy policy.
- `CRITICAL`: ưu tiên kiểm tra cao nhất.
- `INVALID`: dữ liệu đầu vào không hợp lệ.
- `UNKNOWN`: không thể kết luận do dependency/system failure.

---

## 3.3. MLPredictionLabel

```text
NORMAL
FAILURE
```

Đây là output binary classification, không đồng nghĩa trực tiếp với severity alert.

---

## 3.4. AlertSeverity

```text
WATCH
HIGH
CRITICAL
```

Không dùng `NORMAL` làm severity alert.

---

## 3.5. AlertStatus

```text
OPEN
ACKNOWLEDGED
IN_PROGRESS
ESCALATED
RESOLVED
FALSE_ALERT
```

---

## 3.6. GroundedStatus

```text
GROUNDED
INSUFFICIENT_EVIDENCE
VALIDATION_FAILED
UNAVAILABLE
```

---

## 3.7. ServiceDependencyStatus

```text
AVAILABLE
DEGRADED
UNAVAILABLE
```

## 3.8. DriftStatus

```text
STABLE
WARNING
CRITICAL
INSUFFICIENT_DATA
ERROR
```

Ý nghĩa:

- `STABLE`: không có bằng chứng drift vượt policy threshold.
- `WARNING`: ít nhất một feature/prediction distribution vượt ngưỡng warning nhưng chưa đạt policy critical.
- `CRITICAL`: aggregate policy đánh giá drift nghiêm trọng.
- `INSUFFICIENT_DATA`: current window chưa đủ sample để kết luận.
- `ERROR`: worker không tính được report do baseline/config/DB/runtime failure.

`DriftStatus` là trạng thái monitoring, **không phải** `MachineOperationalState` và không được map trực tiếp sang `AlertSeverity`.

---

# 4. Standard Error Contract

Mọi API lỗi business/system phải trả cùng envelope:

```json
{
  "error": {
    "code": "ML_UNAVAILABLE",
    "message": "ML inference service is unavailable",
    "retryable": true,
    "details": {},
    "request_id": "req_123",
    "correlation_id": "cor_123"
  }
}
```

## 4.1. Error codes chuẩn

| Code | HTTP | Retryable | Ý nghĩa |
|---|---:|---|---|
| `INVALID_REQUEST` | 400 | No | JSON/request không hợp lệ |
| `VALIDATION_FAILED` | 422 | No | HTTP ingress envelope invalid; detailed feature/range invalid xử lý async |
| `EVENT_CONFLICT` | N/A async (409 nếu ingress có durable index) | No | Same event_id/different payload bị Processing quarantine |
| `MACHINE_NOT_FOUND` | 404 GET / async event | No | Machine GET 404; accepted sensor event machine unknown xử lý async |
| `ALERT_NOT_FOUND` | 404 | No | Không có alert |
| `INVALID_ALERT_TRANSITION` | 409 | No | Chuyển state alert không hợp lệ |
| `ML_UNAVAILABLE` | N/A async | Yes | ML unavailable ở Processing Worker; không đổi HTTP 202 đã ACK |
| `ML_TIMEOUT` | N/A async | Yes | Worker retry/mark UNKNOWN; không đổi HTTP 202 đã ACK |
| `RAG_UNAVAILABLE` | 503 | Yes | RAG unavailable; không ảnh hưởng alert |
| `INSUFFICIENT_EVIDENCE` | 200 | No | RAG không đủ bằng chứng; đây là business result |
| `DRIFT_REFERENCE_NOT_FOUND` | N/A batch | No | Không tìm thấy reference profile tương ứng model/dataset version |
| `DRIFT_INSUFFICIENT_DATA` | N/A batch | Yes later | Runtime window chưa đủ sample để kết luận |
| `DRIFT_COMPUTE_ERROR` | N/A batch | Yes | Worker không thể tính drift report |
| `INTERNAL_ERROR` | 500 | Maybe | Lỗi hệ thống không phân loại |

| `KAFKA_UNAVAILABLE` | 503 | Yes | Ingestion không thể xác nhận publish Kafka |
| `EVENT_NOT_MATERIALIZED` | 404 status query | Yes after 202 | Event đã accept nhưng Processing chưa materialize hoặc ID không tồn tại |
| `RATE_LIMITED` | 429 | Yes | Ingestion quá quota, dùng Retry-After khi phù hợp |

Không trả stack trace cho Frontend.

---

# 5. Sensor Simulator → Backend Contract

## 5.1. Endpoint

```http
POST /api/v1/sensor-readings
```

## 5.2. SensorReadingRequest

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "received_at": "2026-10-12T04:30:15.231Z",
  "source": "AI4I_SIMULATOR",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

## 5.3. Field contract

> Các constraint chi tiết của `features` trong bảng này là **contract Data Quality của Processing Worker**, **không phải** điều kiện để Ingestion trả HTTP 202. Ingestion chỉ xác minh envelope tối thiểu và `features` là object.


| Field | Type | Required | Constraint |
|---|---|---|---|
| `event_id` | string | Yes | Unique/idempotency key |
| `machine_id` | string | Yes | Non-empty |
| `received_at` | RFC3339 datetime | Yes | Simulator event time |
| `source` | string | Yes | MVP = `AI4I_SIMULATOR` |
| `features.product_type` | enum string | Yes | `L`, `M`, `H` |
| `features.air_temperature_k` | number | Yes | finite number |
| `features.process_temperature_k` | number | Yes | finite number |
| `features.rotational_speed_rpm` | number | Yes | finite, > 0 |
| `features.torque_nm` | number | Yes | finite; range policy do validator quản lý |
| `features.tool_wear_min` | number | Yes | finite, >= 0 |

### Cấm gửi làm ML features

Các field sau không được dùng làm model input:

```text
machine_failure
TWF
HDF
PWF
OSF
RNF
machine_id
event_id
received_at
```

Ground-truth label có thể tồn tại trong simulator test harness để đánh giá offline/demo evidence, nhưng không gửi vào `/predict`.

---

## 5.4. Ingestion ACK — thay thế response đồng bộ của v1.1

`POST /api/v1/sensor-readings` là **command tiếp nhận bất đồng bộ**. Sau khi envelope hợp lệ, Kafka producer nhận broker ACK theo cấu hình (đề xuất `acks=all`, idempotent producer, timeout và retries có giới hạn), API trả `202`:

```http
HTTP/1.1 202 Accepted
Content-Type: application/json
Location: /api/v1/sensor-events/evt_001/status
```

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "ingestion_status": "ACCEPTED",
  "processing_status": "PENDING",
  "status_url": "/api/v1/sensor-events/evt_001/status",
  "correlation_id": "cor_001"
}
```

`202` **không chứa `prediction_id`, `alert_id`, `machine_state` hoặc `validation_status` kết luận**. Processing chạy sau khi response đã được gửi. `Location` là endpoint truy vấn kết quả sau này, có thể tạm `404 EVENT_NOT_MATERIALIZED` khi consumer chưa tạo bản ghi; UI/poller xử lý 404 này như "chưa materialize", không tự diễn dịch thành mất event. Nếu muốn tránh 404 chuyển tiếp, triển khai ingress-status index/cache durable riêng; **không giả định Kafka đồng bộ cập nhật processing_db**.

### Ingestion guard vs detailed Data Quality

- **Ingestion guard (trước Kafka):** authentication/rate limit, payload size, JSON parse, `event_id`, `machine_id`, timestamp đúng cú pháp, `source`, `features` là JSON object; không reject nhiệt độ/outlier chỉ vì nằm ngoài ML domain.
- **Data Quality (sau Kafka):** feature presence/type, finite, L/M/H, ranges, normalization consistency, machine registry validation. Invalid được persist với `validation_status=INVALID`, `prediction_id=null`, không chạy ML.
- Dữ liệu HTTP malformed JSON → `400 INVALID_REQUEST`; thiếu/invalid envelope → `422 VALIDATION_FAILED`; không xác thực → `401/403`; rate limit → `429`; broker không ACK/timeout → `503/504` retryable. Không trả `422` chỉ vì feature range invalid **sau** khi đã ACK Kafka.
- Machine registry thuộc Business Service. Event có `machine_id` không provision được đánh dấu `INVALID`/`MACHINE_NOT_FOUND` **sau** Kafka qua bản sao registry được đồng bộ có version hoặc Business lookup có timeout/cache; **không tự tạo máy**. `GET /machines/{id}` vẫn 404 nếu máy không tồn tại.

## 5.5. Truy vấn kết quả xử lý sau khi đã tiếp nhận

```http
GET /api/v1/sensor-events/{event_id}/status
```

API read-only do **Processing owner** cung cấp (Business facade có thể proxy). `200` khi record đã materialize:

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "processing_status": "COMPLETED",
  "validation_status": "VALID",
  "machine_state": "HIGH",
  "prediction_id": "pred_001",
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "alert_delivery_status": "PUBLISHED",
  "correlation_id": "cor_001",
  "updated_at": "2026-10-17T01:00:01Z"
}
```

Các giá trị `processing_status`: `PENDING`, `PROCESSING`, `COMPLETED`, `INVALID`, `RETRYING`, `FAILED`. Đây là **processing lifecycle**, không phải `MachineOperationalState`. `FAILED` do dependency exhaustion/poison event không được map thành `NORMAL`. `INVALID` ghi `validation_status=INVALID`, `prediction_id=null`. Chỉ trả `alert_id` khi **Business Service đã tạo xác nhận**, không giả định có sẵn ngay lúc Processing publish signal. Frontend lấy alerts từ Business API hoặc SSE/polling; eventual consistency là hành vi bình thường.

`GET` chưa materialize: `404 EVENT_NOT_MATERIALIZED` (có thể retry sau khi nhận 202); invalid unknown ID trả cùng status nhưng nên dùng timeout phía client để kết luận cần điều tra. Khi lựa chọn status index của Ingestion về sau thì có thể nâng contract này qua một quyết định riêng.

## 5.6. Idempotency / retries / conflict trong kiến trúc bất đồng bộ

- `event_id` là idempotency key bất biến; Simulator **giữ nguyên ID và body khi retry**.
- Kafka dùng **at-least-once** ở cấp ứng dụng; producer retry/consumer rebalance có thể tạo/giao event trùng. Unique `(event_id)` và `payload_hash` trong `processing_db`; replay giống nhau là no-op theo business side-effects; payload khác cùng ID được ghi conflict/quarantine **không tạo prediction/alert**.
- **Không cam kết HTTP 409 `EVENT_CONFLICT` tại Ingestion** khi Ingestion không có durable idempotency index riêng. Cùng ID nhưng body khác có thể vẫn được `202`, sau đó Processing phát hiện và lưu `EVENT_CONFLICT`. Nếu yêu cầu 409 tại ingress thì phải thiết kế durable dedupe/index hoặc service lookup nhất quán; đó là mở rộng contract.
- Storage Consumer lưu được mọi accepted Kafka envelope, kể cả record invalid/conflict; raw archive là append-only về mặt logic. Event trùng trong raw archive được nhận diện theo event_id và Kafka topic/partition/offset, không tự ý làm mất payload phục vụ audit.
- Consumer commit offset **sau khi** DB transaction (kể cả outbox) hoặc batch S3 + metadata/checkpoint hoàn tất bền vững; không để `auto_commit` gây data loss.

---

# 6. Data Quality Validator Contract

Đây là internal contract của **Processing Worker sau khi consume `sensor.readings`**, không expose HTTP ingestion. Các field về feature thiếu/sai kiểu không bị Ingestion từ chối trước Kafka nếu envelope tối thiểu hợp lệ.

## 6.1. Input

```json
{
  "event_id": "evt_001",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

## 6.2. Output

```json
{
  "status": "VALID",
  "validator_version": "sensor-validator-1.0.0",
  "errors": []
}
```

Invalid:

```json
{
  "status": "INVALID",
  "validator_version": "sensor-validator-1.0.0",
  "errors": [
    {
      "field": "features.product_type",
      "code": "UNSUPPORTED_VALUE",
      "message": "product_type must be one of L, M, H"
    }
  ]
}
```

## 6.3. Validation layers

1. JSON/schema validation.
2. Required fields.
3. Type validation.
4. finite/NaN/Inf validation.
5. categorical constraints.
6. domain/range constraints.
7. optional cross-field sanity rules.

Range rules phải config/version hóa, không hard-code rải rác trong controller.

---

# 7. Backend → ML Inference Contract

## 7.1. Endpoint

```http
POST /api/v1/predict
```

ML Service chỉ nhận event đã VALID.

---

## 7.2. PredictionRequest

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

Backend gửi **raw validated features**. Encoding/scaling/preprocessing thuộc ML Service để tránh training-serving skew.

---

## 7.3. PredictionResponse

```json
{
  "prediction_id": "pred_001",
  "event_id": "evt_001",
  "prediction": "FAILURE",
  "failure_probability": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "preprocessor_version": "maintai-preprocessor-1.0.0",
  "model_threshold": 0.63,
  "inference_ms": 18,
  "predicted_at": "2026-10-12T04:30:15.420Z"
}
```

## 7.4. Semantics

### `failure_probability`

```text
0.0 <= failure_probability <= 1.0
```

Đây là score chính Backend dùng cho Risk Decision Engine.

### `prediction`

Binary label của model dựa trên threshold đã chọn trong validation ML:

```text
NORMAL
FAILURE
```

### `model_threshold`

Threshold dùng để sinh `prediction`. Nó **không bắt buộc bằng** các threshold severity của product.

Ví dụ:

```text
model_threshold = 0.63
product CRITICAL threshold = 0.85
```

Điều này hoàn toàn hợp lệ.

---

## 7.5. ML Service error behavior

### Request invalid

HTTP `422`.

### Model chưa load / artifact lỗi

HTTP `503`:

```json
{
  "error": {
    "code": "MODEL_NOT_READY",
    "message": "Model artifact is not ready",
    "retryable": true,
    "details": {}
  }
}
```

### Backend timeout

Khuyến nghị timeout MVP:

```text
2 seconds
```

Backend có thể retry tối đa 1 lần nếu lỗi connection/transient; không retry validation error.

Nếu hết retry:

```text
machine_state = UNKNOWN
prediction = none
alert decision = không giả định NORMAL
```

---

# 8. ML Artifact Contract

Một release ML phải chứa ít nhất:

```text
artifacts/
  model.joblib
  preprocessor.joblib
  metadata.json
```

## 8.1. metadata.json

```json
{
  "model_name": "maintai-failure-classifier",
  "model_version": "1.0.0",
  "preprocessor_version": "1.0.0",
  "dataset": "AI4I-2020",
  "target": "machine_failure",
  "features": [
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min"
  ],
  "threshold": 0.63,
  "metrics": {
    "recall_failure": 0.88,
    "precision_failure": 0.55,
    "f1_failure": 0.68,
    "pr_auc": 0.71
  }
}
```

Các số trên chỉ là ví dụ format, không phải metric mặc định của dự án.

## 8.2. Drift Reference Profile Artifact

Mỗi model release dùng cho drift monitoring phải có reference profile tương ứng với **training split** hoặc tập reference đã được chốt, không lấy test set làm baseline drift.

Layout đề xuất:

```text
artifacts/
  model.joblib
  preprocessor.joblib
  metadata.json
  drift_reference.json
```

Contract tối thiểu của `drift_reference.json`:

```json
{
  "schema_version": "1.0.0",
  "dataset_version": "ai4i-v1",
  "model_version": "maintai-ml-1.0.0",
  "created_at": "2026-10-12T10:00:00Z",
  "sample_count": 7000,
  "features": {
    "torque_nm": {
      "type": "numeric",
      "metric": "PSI",
      "bins": [0, 20, 30, 40, 50, 60, 100],
      "probabilities": [0.01, 0.07, 0.21, 0.38, 0.22, 0.11]
    },
    "product_type": {
      "type": "categorical",
      "metric": "PSI",
      "categories": ["L", "M", "H"],
      "probabilities": [0.60, 0.30, 0.10]
    }
  },
  "prediction_reference": {
    "failure_rate": 0.034
  }
}
```

Invariants:

- `model_version` của reference profile phải khớp model deployment mà report đang đánh giá.
- Feature list phải khớp feature contract của model, không chứa `event_id`, `machine_id`, `received_at`, target hoặc fault labels.
- Bin/category ordering là một phần của contract; Drift Worker không tự ý regenerate bins từ current window.
- Nếu không tìm được profile tương ứng, report phải là `ERROR` với reason `DRIFT_REFERENCE_NOT_FOUND`; không fallback im lặng sang model/version khác.

---

# 9. Risk Decision Engine Contract

Risk Engine là Backend internal component.

## 9.1. Input

```json
{
  "prediction_id": "pred_001",
  "failure_probability": 0.91,
  "prediction": "FAILURE",
  "model_version": "maintai-ml-1.0.0"
}
```

## 9.2. Policy config

Contract schema:

```json
{
  "policy_version": "risk-policy-1.0.0",
  "thresholds": {
    "watch": 0.40,
    "high": 0.65,
    "critical": 0.85
  },
  "create_alert_for": ["HIGH", "CRITICAL"]
}
```

**Lưu ý:** giá trị `0.40/0.65/0.85` chỉ là ví dụ. Nhóm phải chốt bằng evidence từ validation/test; contract chỉ cố định tên field và semantics.

## 9.3. Output

```json
{
  "machine_state": "CRITICAL",
  "severity": "CRITICAL",
  "should_create_alert": true,
  "policy_version": "risk-policy-1.0.0",
  "decision_reason": "failure_probability >= critical threshold"
}
```

## 9.4. Invariants

- Risk Engine không gọi LLM.
- `UNKNOWN` ML state không được map thành `NORMAL`.
- Mọi decision phải lưu `policy_version`.

---

# 10. Alert Manager Contract — Business Service

Business Consumer nhận `alert.signals` phát từ Processing/outbox và là nơi **duy nhất** tạo, dedupe, cập nhật alert trong `business_db`. `CreateAlertCommand` dưới đây là cấu trúc nghiệp vụ logic, được map từ event `alert.signals`, **không** phải API direct DB/HTTP được Processing phép gọi tùy ý.

## 10.1. CreateAlertCommand

```json
{
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "severity": "CRITICAL",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "correlation_id": "cor_001"
}
```

## 10.2. Alert DTO

```json
{
  "alert_id": "alt_001",
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "severity": "CRITICAL",
  "status": "OPEN",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "dedupe_key": "M-001:CRITICAL",
  "created_at": "2026-10-12T04:30:15.500Z",
  "updated_at": "2026-10-12T04:30:15.500Z"
}
```

---

## 10.3. Alert state transitions

Allowed transitions:

```text
OPEN
 ├──> ACKNOWLEDGED
 ├──> ESCALATED
 └──> FALSE_ALERT

ACKNOWLEDGED
 ├──> IN_PROGRESS
 ├──> ESCALATED
 ├──> RESOLVED
 └──> FALSE_ALERT

IN_PROGRESS
 ├──> RESOLVED
 ├──> ESCALATED
 └──> FALSE_ALERT

ESCALATED
 ├──> IN_PROGRESS
 └──> RESOLVED

RESOLVED      = terminal
FALSE_ALERT   = terminal
```

Các transition ngoài sơ đồ trả `409 INVALID_ALERT_TRANSITION`.

---

## 10.4. Dedupe / cooldown contract

Contract config:

```json
{
  "dedupe_window_seconds": 300,
  "dedupe_by": ["machine_id", "severity"],
  "active_statuses": ["OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"]
}
```

Nếu cùng `dedupe_key` trong cửa sổ và alert cũ vẫn active:

- Không tạo alert mới.
- Update `updated_at`, `latest_prediction_id`, `latest_risk_score` nếu schema triển khai có các field này.
- Tăng metric `alerts_deduplicated_total`.

---

# 11. Alert Lifecycle APIs — Backend → Frontend

## 11.1. ACK alert

```http
POST /api/v1/alerts/{alert_id}/ack
```

Request:

```json
{
  "actor_id": "tech-001",
  "note": "Đã tiếp nhận cảnh báo"
}
```

Response:

```json
{
  "alert_id": "alt_001",
  "previous_status": "OPEN",
  "status": "ACKNOWLEDGED",
  "updated_at": "2026-10-12T04:32:00Z"
}
```

---

## 11.2. Mark in progress

```http
POST /api/v1/alerts/{alert_id}/start
```

---

## 11.3. Escalate

```http
POST /api/v1/alerts/{alert_id}/escalate
```

Request:

```json
{
  "actor_id": "tech-001",
  "note": "Cần trưởng ca kiểm tra"
}
```

---

## 11.4. Resolve

```http
POST /api/v1/alerts/{alert_id}/resolve
```

```json
{
  "actor_id": "tech-001",
  "resolution": "Inspected simulated equipment; no further action required",
  "note": "Demo resolution"
}
```

---

## 11.5. Mark false alert

```http
POST /api/v1/alerts/{alert_id}/false-alert
```

```json
{
  "actor_id": "tech-001",
  "reason": "Inspection did not confirm abnormal condition",
  "note": "False positive feedback for evaluation"
}
```

---

# 12. Feedback Contract

## 12.1. Endpoint

```http
POST /api/v1/alerts/{alert_id}/feedback
```

## 12.2. Request

```json
{
  "technician_verdict": "FALSE_ALERT",
  "recommendation_feedback": "PARTIALLY_USEFUL",
  "note": "Checklist useful but alert was not confirmed"
}
```

### technician_verdict

```text
CONFIRMED_ISSUE
FALSE_ALERT
INCONCLUSIVE
```

### recommendation_feedback

```text
USEFUL
PARTIALLY_USEFUL
NOT_USEFUL
NOT_AVAILABLE
```

## 12.3. Response

```json
{
  "feedback_id": "fb_001",
  "alert_id": "alt_001",
  "recorded_at": "2026-10-12T04:40:00Z"
}
```

Feedback không tự động trigger retraining trong MVP.

---

# 13. Backend → RAG/LLM Contract

RAG chạy sau khi alert đã tồn tại hoặc khi technician yêu cầu brief.

## 13.1. Endpoint

```http
POST /api/v1/briefs
```

## 13.2. MaintenanceBriefRequest

```json
{
  "alert": {
    "alert_id": "alt_001",
    "machine_id": "M-001",
    "severity": "CRITICAL",
    "status": "OPEN",
    "risk_score": 0.91,
    "policy_version": "risk-policy-1.0.0"
  },
  "prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91,
    "model_version": "maintai-ml-1.0.0"
  },
  "sensor_context": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  },
  "intent": "TECHNICIAN_CHECKLIST"
}
```

### Intent enum

```text
MAINTENANCE_BRIEF
TECHNICIAN_CHECKLIST
ALERT_EXPLANATION
```

---

## 13.3. MaintenanceBriefResponse – grounded

```json
{
  "brief_id": "brf_001",
  "alert_id": "alt_001",
  "grounded_status": "GROUNDED",
  "summary": "Thiết bị có risk score cao. Kết quả ML không khẳng định chắc chắn nguyên nhân hỏng hóc.",
  "uncertainty": "Risk score phản ánh xác suất theo mô hình, cần kiểm tra thực tế.",
  "checklist": [
    {
      "step": 1,
      "action": "Kiểm tra điều kiện vận hành liên quan đến torque theo manual.",
      "citation_ids": ["cit_001"]
    },
    {
      "step": 2,
      "action": "Kiểm tra tool wear theo hướng dẫn của tài liệu.",
      "citation_ids": ["cit_002"]
    }
  ],
  "citations": [
    {
      "citation_id": "cit_001",
      "document_id": "manual-001",
      "document_version": "2.0",
      "section": "4.2",
      "page": 18,
      "chunk_id": "chunk-123"
    },
    {
      "citation_id": "cit_002",
      "document_id": "manual-001",
      "document_version": "2.0",
      "section": "5.1",
      "page": 21,
      "chunk_id": "chunk-145"
    }
  ],
  "retrieval": {
    "top_k": 5,
    "retrieved_chunk_ids": ["chunk-123", "chunk-145", "chunk-180"]
  },
  "generator": {
    "provider": "configured-provider",
    "model": "configured-model"
  },
  "generated_at": "2026-10-12T04:30:16.400Z"
}
```

---

## 13.4. MaintenanceBriefResponse – insufficient evidence

HTTP `200`:

```json
{
  "brief_id": "brf_002",
  "alert_id": "alt_001",
  "grounded_status": "INSUFFICIENT_EVIDENCE",
  "summary": "Không đủ bằng chứng trong maintenance manual hiện có để tạo hướng dẫn kiểm tra có căn cứ.",
  "uncertainty": "Yêu cầu manual review.",
  "checklist": [],
  "citations": [],
  "retrieval": {
    "top_k": 5,
    "retrieved_chunk_ids": []
  }
}
```

`INSUFFICIENT_EVIDENCE` là kết quả nghiệp vụ hợp lệ, không phải HTTP 5xx.

---

## 13.5. RAG unavailable

Backend vẫn giữ alert bình thường.

Frontend nhận trạng thái:

```json
{
  "copilot_status": "UNAVAILABLE",
  "message": "Maintenance brief is temporarily unavailable. Alert remains active."
}
```

---

# 14. Knowledge Base / pgvector Contract

## 14.1. Document metadata

```json
{
  "document_id": "manual-001",
  "title": "Maintenance Manual - Sample Equipment",
  "version": "2.0",
  "document_type": "MAINTENANCE_MANUAL",
  "language": "en",
  "effective_from": "2026-01-01",
  "source_file": "manual_v2.pdf"
}
```

## 14.2. DocumentChunk

```json
{
  "chunk_id": "chunk-123",
  "document_id": "manual-001",
  "document_version": "2.0",
  "section": "4.2",
  "page": 18,
  "text": "...",
  "safety_notes": [],
  "embedding_model": "configured-embedding-model"
}
```

## 14.3. Citation rule

Mọi actionable checklist item phải có ít nhất một `citation_id` trỏ tới chunk thực sự được retriever trả về.

Citation Validator phải reject citation không tồn tại trong retrieval context.

---

# 15. Business API → Frontend Read APIs

> Các Machine/Alert/Brief DTO hiện hữu giữ semantic. Business API phục vụ Frontend; để lấy sensor history, runtime prediction/status, Business dùng Processing read API/projection, **không đọc trực tiếp `processing_db`**. Cross-service eventual consistency phải được UI chịu được.


Frontend chỉ gọi Backend, không gọi trực tiếp ML/RAG DB trong MVP.

---

## 15.1. Fleet list

```http
GET /api/v1/machines
```

Response:

```json
{
  "items": [
    {
      "machine_id": "M-001",
      "display_name": "Machine 001",
      "state": "CRITICAL",
      "latest_risk_score": 0.91,
      "active_alert_count": 1,
      "last_seen_at": "2026-10-12T04:30:15Z",
      "data_status": "VALID"
    }
  ],
  "total": 1
}
```

---

## 15.2. Machine detail

```http
GET /api/v1/machines/{machine_id}
```

```json
{
  "machine_id": "M-001",
  "display_name": "Machine 001",
  "state": "CRITICAL",
  "data_status": "VALID",
  "last_seen_at": "2026-10-12T04:30:15Z",
  "latest_sensor": {
    "event_id": "evt_001",
    "features": {
      "product_type": "M",
      "air_temperature_k": 298.1,
      "process_temperature_k": 308.6,
      "rotational_speed_rpm": 1551,
      "torque_nm": 42.8,
      "tool_wear_min": 108
    }
  },
  "latest_prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91,
    "model_version": "maintai-ml-1.0.0"
  },
  "active_alerts": ["alt_001"]
}
```

---

## 15.3. Sensor history

```http
GET /api/v1/machines/{machine_id}/sensor-readings?limit=100&before=<timestamp>
```

Response phải sort mới nhất trước hoặc ghi rõ sort contract.

Khuyến nghị:

```text
sort = received_at DESC
```

---

## 15.4. Alert list

```http
GET /api/v1/alerts?status=OPEN&severity=CRITICAL&machine_id=M-001
```

```json
{
  "items": [
    {
      "alert_id": "alt_001",
      "machine_id": "M-001",
      "severity": "CRITICAL",
      "status": "OPEN",
      "risk_score": 0.91,
      "created_at": "2026-10-12T04:30:15Z"
    }
  ],
  "total": 1
}
```

---

## 15.5. Alert detail

```http
GET /api/v1/alerts/{alert_id}
```

```json
{
  "alert": {
    "alert_id": "alt_001",
    "machine_id": "M-001",
    "severity": "CRITICAL",
    "status": "OPEN",
    "risk_score": 0.91,
    "model_version": "maintai-ml-1.0.0",
    "policy_version": "risk-policy-1.0.0",
    "created_at": "2026-10-12T04:30:15Z"
  },
  "sensor_event": {
    "event_id": "evt_001",
    "validation_status": "VALID",
    "features": {}
  },
  "prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91
  },
  "copilot": {
    "status": "AVAILABLE",
    "brief_id": "brf_001",
    "grounded_status": "GROUNDED"
  },
  "history": []
}
```

---

# 16. Realtime Contract – SSE (UI phương án C; REST khôi phục)

Nếu cần realtime dashboard, ưu tiên SSE trước WebSocket vì use case chủ yếu server → UI.

## 16.1. Endpoint

```http
GET /api/v1/events/stream
Accept: text/event-stream
```

## 16.2. Event types

```text
machine.updated
alert.created
alert.updated
brief.ready
service.degraded
```

Ví dụ:

```text
event: alert.created
id: evtstream_001
data: {"alert_id":"alt_001","machine_id":"M-001","severity":"CRITICAL","status":"OPEN"}
```

Nếu nhóm không cần realtime trong MVP, polling REST là chấp nhận được; không để SSE làm blocker.

---

# 17. Database Contract — ownership theo logical DB (v1.2)

**Không dùng SQL cross-service**: không đọc/ghi trực tiếp DB của service khác, không có FK vật lý xuyên database. Mỗi logical DB có migration Alembic/owner/credential riêng; 5 DB cùng chung PostgreSQL instance ở MVP.

| Logical database | Sở hữu | Bảng / dữ liệu tối thiểu |
|---|---|---|
| `processing_db` | Processing | `sensor_events`, `predictions`, `risk_decisions`, `processing_outbox`, `processing_machine_refs` (nếu dùng local registry projection) |
| `business_db` | Business | `machines`, `alerts`, `alert_events`, `feedback`, `rag_briefs`, `business_inbox` |
| `drift_db` | Drift CronJob | `drift_reports`, `drift_checkpoints`, optional run metadata |
| `mlflow_db` | MLflow Server | MLflow-managed run/experiment/model registry tables; **không tự tạo bằng Alembic MaintAI** |
| `knowledge_db` | RAG | `documents`, `document_chunks`, pgvector indexes |

### 17.1. Processing tables

```text
sensor_events(event_id PK, machine_id, event_time, ingested_at,
              source, raw_features_json JSONB, payload_hash,
              processing_status, validation_status, validation_errors_json,
              validator_version, correlation_id, kafka_topic,
              kafka_partition, kafka_offset, created_at, updated_at)
predictions(prediction_id PK, event_id UNIQUE, label, failure_probability,
            model_version, preprocessor_version, model_threshold,
            inference_ms, predicted_at, correlation_id)
risk_decisions(risk_decision_id PK, event_id UNIQUE, prediction_id,
               machine_state, severity_nullable, should_alert,
               policy_version, decided_at)
processing_outbox(outbox_id PK, event_type, aggregate_id, payload_json,
                  status, created_at, published_at_nullable)
```

`machine_id` là external reference tới `business_db.machines`, không phải FK cross-DB. Đảm bảo uniqueness của `event_id`; chỉ một current prediction/risk result trên event. Nếu dùng replica registry (máy đã provision), đồng bộ từ Business theo API hoặc event có version; lỗi lookup không biến machine thành `NORMAL`.

### 17.2. Business tables

```text
machines(machine_id PK, display_name, state, last_seen_at, created_at, updated_at)
alerts(alert_id PK, machine_id FK, source_event_id, source_prediction_id,
       severity, status, risk_score, model_version, policy_version,
       dedupe_key, created_at, updated_at)
alert_events(alert_event_id PK, alert_id FK, event_type, from_status,
             to_status, actor_id, note, timestamp)
feedback(feedback_id PK, alert_id FK, technician_verdict,
         recommendation_feedback, note, created_at)
rag_briefs(brief_id PK, alert_id FK, grounded_status, summary, uncertainty,
           checklist_json, citations_json, retrieved_chunk_ids_json,
           generator_provider, generator_model, generated_at)
business_inbox(message_id PK UNIQUE, topic, partition_no, offset_no,
               consumed_at, source_event_id)
```

`source_prediction_id`, `source_event_id`, citations/document IDs là **opaque external IDs**; không FK sang `processing_db`/`knowledge_db`. Business Consumer transactional inbox + alert dedupe trong `business_db` để xử lý Kafka at-least-once. Business API sở hữu all alert lifecycle mutations và phát REST/SSE updates sau commit.

### 17.3. Drift/Knowledge/MLflow

```text
drift_db.drift_reports(report_id UNIQUE, computed_at, detector_version,
                       reference_dataset_version, model_version,
                       window_start, window_end, sample_count,
                       overall_status, report_json)
drift_db.drift_checkpoints(worker_name PK, last_processed_event_at,
                           last_processed_event_id, last_success_at,
                           last_report_id, updated_at)
knowledge_db.documents(document_id, version, title, type, source_file, ...)
knowledge_db.document_chunks(chunk_id, document_id, document_version,
                             section, page, text, embedding, ...)
```

`mlflow_db` để MLflow Server tự quản lý metadata/schema. S3/MinIO lưu **raw archive**, **curated datasets**, **drift reports**, **model artifacts**, **RAG source documents**. Tránh lưu một S3 object cho mỗi sensor event; Storage Consumer ghi batch có checkpoint theo Kafka offset.

### 17.4. Query và ownership

- Frontend gọi Business API, **Business API gọi Processing read API** khi cần sensor history/prediction snapshot, không JOIN database khác.
- Drift chỉ đọc Processing read API (`VALID` window có pagination/cursor/snapshot semantics) hoặc curated S3 và chỉ ghi `drift_db`.
- Business có thể lưu machine state projection từ `machine.updated` event/snapshot do Processing phát (cần contract riêng nếu chọn); khi chưa có, Business API có thể compose on-read qua Processing API. Không có cross-DB transaction tự động.
- DB migrations được quản lý riêng, expand/contract tương thích event versions; DB scale là scale PostgreSQL **instance**/read replica/partition, không phải scale logical database tự thân.

---

# 18. Traceability Contract

Một `alert_id` phải truy ra được chain thông qua **Business API + Processing read API**, không cần (và không được) SQL JOIN xuyên logical DB:

```text
alert_id
  -> prediction_id
      -> event_id
          -> machine_id
          -> validated features
      -> model_version
      -> preprocessor_version
  -> policy_version
  -> alert_events
  -> feedback
  -> rag_brief
      -> document_id + version + chunk_id + page/section
```

Integration test phải chứng minh được chain này.

Drift report phải truy vết được ít nhất:

```text
report_id
  -> detector_version
  -> model_version
  -> reference_dataset_version
  -> reference profile schema/version
  -> runtime window_start/window_end/sample_count
  -> feature scores + aggregate policy
```

---

# 19. Health Contract

Mỗi service Backend / ML / RAG phải expose:

## 19.1. Liveness

```http
GET /health/live
```

HTTP `200` nếu process sống.

```json
{
  "status": "UP",
  "service": "ml-inference",
  "version": "1.0.0"
}
```

Liveness không nên phụ thuộc DB/LLM provider ngoài nếu điều đó gây restart loop.

---

## 19.2. Readiness

```http
GET /health/ready
```

Ví dụ ML:

```json
{
  "status": "UP",
  "checks": {
    "model_loaded": true
  }
}
```

Ví dụ RAG:

```json
{
  "status": "DEGRADED",
  "checks": {
    "database": true,
    "vector_store": true,
    "llm_provider": false
  }
}
```

---

# 20. Metrics Contract

**OBS-01/1.0.0 — 2026-10-20:** §41 và
[OBSERVABILITY_IMPLEMENTATION.md](OBSERVABILITY_IMPLEMENTATION.md) chốt types,
units, labels/enums, buckets, runtime owners và commit/retry semantics. Catalog
đó bổ sung danh sách tối thiểu ở dưới và thay short aliases/ví dụ mâu thuẫn.
HTTP/Kafka payload versions không đổi; source/GitOps còn cần implement/migrate.

Mỗi long-running application service expose metrics ở internal endpoint;
Drift batch dùng Pushgateway theo §20.4, không tạo endpoint mất theo process.
Static frontends không tự xuất app business metrics. HTTP endpoint:

```http
GET /metrics
```

## 20.1. Backend metrics

```text
maintai_http_requests_total{route,method,status}
maintai_http_request_duration_seconds{route,method}
maintai_sensor_events_total{validation_status}
maintai_invalid_sensor_events_total{reason}
maintai_alerts_created_total{severity}
maintai_alerts_deduplicated_total{severity}
maintai_alert_transitions_total{from_status,to_status}
maintai_dependency_errors_total{dependency,error_type}
```

## 20.2. ML metrics

```text
maintai_ml_inference_requests_total{status}
maintai_ml_inference_duration_seconds
maintai_ml_predictions_total{prediction}
maintai_ml_failure_probability
maintai_ml_model_info{model_version}
```

## 20.3. RAG metrics

```text
maintai_rag_requests_total{status,grounded_status}
maintai_rag_request_duration_seconds
maintai_rag_retrieval_duration_seconds
maintai_rag_retrieved_chunks
maintai_rag_citation_validation_failures_total
```

## 20.4. Drift metrics

Drift Worker là batch/one-shot process. Vì process exit sau mỗi run nên Prometheus không thể scrape trực tiếp một endpoint đã biến mất. **Pattern mặc định cho prototype:** worker push latest gauges vào Prometheus Pushgateway; Prometheus scrape Pushgateway. Nếu nhóm không dùng Pushgateway, phải có một long-lived metrics adapter tương đương đọc latest `drift_report`; không giả định Prometheus scrape được process one-shot.

Metric contract tối thiểu:

```text
maintai_drift_feature_score{feature,metric}
maintai_drift_feature_status{feature,status}
maintai_drift_features_drifted
maintai_drift_overall_status{status}
maintai_drift_window_samples
maintai_drift_reports_total{status}
maintai_drift_worker_last_success_timestamp_seconds
maintai_prediction_failure_ratio
```

`maintai_drift_feature_status{feature,status}` và
`maintai_drift_overall_status{status}` là **one-hot gauges0/1**: đúng một trong
STABLE/WARNING/CRITICAL/INSUFFICIENT_DATA/ERROR có value1. Không dùng mã số0..4
cho cùng metric name; thay quy ước số hóa mâu thuẫn trước đây, giữ renderer thật.
Score không tính được thì absent, không set0. Heartbeat lấy từ checkpoint đã commit;
report status phải được ghép freshness/missing-data guard khi hiển thị.

Không dùng `report_id`, `event_id`, `machine_id`, timestamp riêng lẻ làm Prometheus labels.

## 20.5. Label cardinality rules

Không dùng các giá trị có cardinality cao làm Prometheus label:

```text
event_id
alert_id
machine_id / machine free-text
request_id
user note
```

Các ID này để trong log, không để trong metric label.

---

# 21. Structured Logging Contract

JSON log khuyến nghị:

```json
{
  "timestamp": "2026-10-12T04:30:15.500Z",
  "level": "INFO",
  "service": "maintai-backend",
  "event": "alert_created",
  "request_id": "req_001",
  "correlation_id": "cor_001",
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "alert_id": "alt_001",
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0"
}
```

Không log API key, secret, credential hoặc full prompt chứa thông tin nhạy cảm nếu sau này dùng dữ liệu thật.

Drift Worker log tối thiểu nên có:

```json
{
  "timestamp": "2026-10-12T15:00:00Z",
  "level": "INFO",
  "service": "drift-worker",
  "event": "drift_report_computed",
  "report_id": "drift-20261012-001",
  "model_version": "maintai-ml-1.0.0",
  "reference_dataset_version": "ai4i-v1",
  "window_start": "2026-10-12T13:00:00Z",
  "window_end": "2026-10-12T15:00:00Z",
  "sample_count": 500,
  "overall_status": "CRITICAL"
}
```

---

# 22. Security / Auth Contract — shared deployment

Auth bắt buộc cho shared/demo deployment. External OIDC cấp token; public browser
clients dùng Authorization Code+PKCE; Business/Sensor kiểm tra issuer/audience/
expiry/signature/scope/machine permission. Không thêm Auth Server trong MaintAI.

| Identity | Quyền |
| --- | --- |
| VIEWER | Read fleet/live/history/alerts/brief/drift theo machine scope |
| TECHNICIAN | Read + alert lifecycle/feedback/request brief |
| ADMIN | Business operations, quyền vận hành explicit; không tự tạo API thay policy |
| SIMULATOR_OPERATOR | Control trên Simulator UI -> Sensor, không tự có business mutations |
| Service identities | Audience/scope riêng cho Sensor/Processing/Business/Drift, không shared DB credentials |

Anonymous chỉ local-only profile explicit, không default shared deployment.
SSE dùng authenticated fetch streaming; không token trong query string. Missing/
invalid token401; thiếu quyền403. Policy/model release qua reviewed GitOps PR,
không có admin policy mutation API trong target. Role matrix chi tiết ở §34.

# 23. Fail-safe Contract Matrix

| Tình huống | System state | Có gọi ML? | Có alert mới? | RAG | UI phải hiển thị |
|---|---|---:|---|---|---|
| Sensor valid, ML normal | NORMAL/WATCH theo policy | Yes | Theo policy | Optional | Prediction + state |
| Sensor invalid | INVALID | No | No | No | Invalid + lỗi validation |
| ML timeout | UNKNOWN | Attempted | Không suy đoán Normal | No | ML unavailable / Unknown |
| ML 503 | UNKNOWN | Attempted | Không suy đoán Normal | No | Dependency unavailable |
| High/Critical prediction, RAG OK | HIGH/CRITICAL | Yes | Yes | Yes | Alert + brief/checklist |
| High/Critical prediction, RAG down | HIGH/CRITICAL | Yes | Yes | Unavailable | Alert vẫn hiển thị; brief unavailable |
| RAG insufficient evidence | HIGH/CRITICAL | Yes | Yes | No grounded advice | Alert + manual review message |
| Duplicate event identical | Existing state | No second inference | No duplicate | No duplicate | Existing result |
| Duplicate alert condition | HIGH/CRITICAL | Yes | Dedupe/update | Optional | Một active alert |
| Drift window chưa đủ sample | Monitoring = INSUFFICIENT_DATA | Không ảnh hưởng | Không ảnh hưởng | Không ảnh hưởng | Monitoring báo chưa đủ dữ liệu |
| Drift Worker lỗi/DB read lỗi | Monitoring = ERROR/STALE | Không ảnh hưởng | Không ảnh hưởng | Không ảnh hưởng | Online path vẫn chạy; platform alert worker stale/error |
| Drift = WARNING/CRITICAL | Online state giữ nguyên theo prediction/policy | Có như bình thường | Theo risk policy | Theo alert path | Monitoring hiển thị drift riêng, không đổi machine severity tự động |
| DB write critical failure | DEGRADED/ERROR | Depends stage | Không claim success | No | Explicit system error |

---

# 24. Frontend State Contract

Frontend không tự suy diễn state từ màu hoặc probability. Backend trả state rõ ràng.

## Required UI states

```text
LOADING
NORMAL
WATCH
HIGH
CRITICAL
INVALID
UNKNOWN
ERROR
EMPTY
```

Ví dụ:

- `INVALID`: dữ liệu không hợp lệ.
- `UNKNOWN`: ML/system không thể kết luận.
- `ERROR`: request UI/API lỗi.

`UNKNOWN` tuyệt đối không render bằng style/label của `NORMAL`.

---

# 25. Contract Versioning Rules

## 25.1. API contract semantic version

```text
1.0.0
```

- PATCH: sửa doc/field optional không phá consumer.
- MINOR: thêm endpoint/field optional/enum chỉ khi consumer forward-compatible.
- MAJOR: rename/remove/đổi semantics field, enum breaking, endpoint breaking.

## 25.2. Model version độc lập

```text
maintai-ml-1.0.0
maintai-ml-1.1.0
```

Không đồng nhất model version với API version.

## 25.3. Risk policy version

```text
risk-policy-1.0.0
```

Mỗi alert lưu policy version đã dùng.

## 25.4. Knowledge document version

Citation bắt buộc có:

```text
document_id
document_version
section/page/chunk_id
```

---

# 25A. Drift Monitoring Contract

Drift Monitoring là control thuộc ML/MLOps monitoring, chạy ngoài critical alert path. Contract này định nghĩa dữ liệu vào, baseline, output report, persistence, scheduling và observability.

## 25A.1. Phạm vi

MVP theo dõi:

```text
Data Drift
Prediction Drift
Data Quality Trend (supporting signal)
```

Performance drift chỉ được kết luận khi có delayed ground truth đáng tin cậy. Feedback của technician có thể dùng cho evaluation về sau nhưng Drift Worker **không tự retrain** model trong MVP.

## 25A.2. Current Window Input

Drift Worker chỉ đọc sensor events có:

```text
validation_status = VALID
```

Feature contract:

```text
product_type
air_temperature_k
process_temperature_k
rotational_speed_rpm
torque_nm
tool_wear_min
```

Không đưa vào feature drift:

```text
event_id
machine_id
received_at
Machine failure
TWF/HDF/PWF/OSF/RNF
```

Window policy là config, ví dụ:

```yaml
drift:
  window_size: 500
  minimum_samples: 200
```

Nếu `sample_count < minimum_samples`, worker phải ghi report `INSUFFICIENT_DATA`, không phát kết luận WARNING/CRITICAL.

## 25A.3. Detector / Policy Config

Detector implementation có thể thay đổi miễn giữ report contract. Initial policy đề xuất:

```yaml
drift:
  detector_version: drift-worker-1.0.0
  numeric_metric: PSI
  categorical_metric: PSI
  psi:
    warning: 0.10
    critical: 0.20
  aggregation:
    critical_if_critical_features_gte: 2
    warning_if_warning_features_gte: 1
```

Các threshold trên là project policy ban đầu, phải version hóa/calibrate bằng scenario test; không xem là chân lý thống kê cố định.

## 25A.4. DriftReport

Canonical JSON contract:

```json
{
  "schema_version": "1.0.0",
  "report_id": "drift-20261012-001",
  "computed_at": "2026-10-12T15:00:00Z",
  "detector_version": "drift-worker-1.0.0",
  "reference": {
    "dataset_version": "ai4i-v1",
    "model_version": "maintai-ml-1.0.0"
  },
  "current_window": {
    "start_at": "2026-10-12T13:00:00Z",
    "end_at": "2026-10-12T15:00:00Z",
    "sample_count": 500
  },
  "overall_status": "CRITICAL",
  "summary": {
    "total_features": 6,
    "warning_features": 1,
    "critical_features": 2
  },
  "features": [
    {
      "feature": "torque_nm",
      "feature_type": "numeric",
      "metric": "PSI",
      "score": 0.27,
      "warning_threshold": 0.10,
      "critical_threshold": 0.20,
      "status": "CRITICAL"
    }
  ],
  "prediction_drift": {
    "reference_failure_rate": 0.034,
    "current_failure_rate": 0.112
  },
  "reason": null
}
```

Rules:

- `overall_status` phải thuộc `DriftStatus`.
- Mỗi feature result phải mang metric + score + threshold đã dùng để audit được.
- Report phải gắn đúng `model_version` + `reference_dataset_version`.
- Không có baseline đúng version → `ERROR`, không tự chọn baseline gần nhất.
- Một report không được cập nhật machine state hay alert severity.

## 25A.5. Simulator Drift Scenario Contract

Scenario file machine-readable đề xuất:

```yaml
name: gradual-torque-drift
seed: 42
phases:
  - name: baseline
    events: 300
  - name: gradual-drift
    events: 700
    transforms:
      torque_nm:
        type: linear_shift
        start_delta: 0
        end_delta: 8
      rotational_speed_rpm:
        type: linear_scale
        start_factor: 1.0
        end_factor: 0.93
```

Scenario transforms phải tạo **valid-but-shifted data**; scenario invalid-data nằm ở fault-injection contract khác.

Scenario tối thiểu:

```text
normal
sudden-drift
gradual-drift
categorical-drift
```

## 25A.6. Batch Execution Contract

Một run Drift Worker:

```text
load checkpoint
 -> resolve deployed model version
 -> load matching drift_reference.json
 -> query current VALID window
 -> enforce minimum_samples
 -> compute feature drift
 -> compute prediction drift
 -> aggregate status
 -> persist drift_report
 -> push/update latest gauges to Pushgateway (hoặc metrics adapter tương đương)
 -> update checkpoint
 -> exit
```

Worker phải idempotent theo window/report identity hợp lý để retry không tạo report duplicate vô hạn.

## 25A.7. Scheduling Contract

Local/Compose: worker chạy one-shot bằng command; Pushgateway là long-lived metrics target cho batch job, ví dụ:

```text
docker compose run --rm drift-worker
```

Metric grouping key không chứa `report_id`/`event_id`; dùng key ổn định như `job=maintai-drift-worker` và optional `environment=demo`. Mỗi run cập nhật latest gauges thay vì tạo time series mới theo report.

Kubernetes optional deployment dùng `CronJob`, không phải request-path Deployment. Schedule là environment config; ví dụ demo `*/15 * * * *`.

## 25A.8. Monitoring Alert Contract

Drift alert là **platform/MLOps alert**, không phải maintenance alert.

Prometheus alert rule tối thiểu nên cover:

```text
DataDriftWarning
DataDriftCritical
DriftWorkerStale
DriftWorkerError
```

Alert rule nên dùng persistence (`for:` hoặc nhiều run liên tiếp) để giảm noise.

---

# 26. Contract Tests bắt buộc

## CT-01 — Valid SensorEvent

Given valid envelope → Ingestion Kafka ACK rồi HTTP 202. Processing consume → ML gọi **đúng một lần theo business effect** (có thể retry transport), persist prediction/risk.

## CT-02 — Invalid SensorEvent

Given feature missing/wrong type với envelope hợp lệ → HTTP 202, Processing persist INVALID, ML mock nhận zero call. Envelope thiếu event_id/JSON malformed vẫn trả 422/400 trước Kafka.

## CT-03 — Backend ↔ ML response

ML mock trả đủ:

```text
prediction
failure_probability
model_version
```

Backend parse thành công.

## CT-04 — ML timeout

Processing phải persist explicit `UNKNOWN`/`RETRYING`/`FAILED` async, không `NORMAL`; HTTP 202 đã gửi không bị thay đổi.

## CT-05 — Prediction → risk

Given score và policy fixture → Risk Engine trả đúng level + `policy_version`.

## CT-06 — Alert independent from RAG

RAG mock = 503, high/critical prediction vẫn tạo alert.

## CT-07 — Alert lifecycle

OPEN → ACKNOWLEDGED → IN_PROGRESS → RESOLVED thành công.

Invalid transition phải 409.

## CT-08 — Dedupe

Nhiều high-risk event cùng machine/severity trong window → một active alert.

## CT-09 — Grounded RAG

Checklist actionable item phải reference citation có trong retrieved chunks.

## CT-10 — Insufficient evidence

Không có chunk đủ điều kiện → `INSUFFICIENT_EVIDENCE`, checklist rỗng hoặc manual-review only.

## CT-11 — Frontend DTO

Frontend test fixture phải deserialize đúng Machine/Alert/Brief DTO.

## CT-12 — Traceability

Từ `alert_id` truy ra được event, prediction, model version, policy version và citations.

## CT-13 — Drift reference/version match

Given deployed `model_version=maintai-ml-1.0.0` → worker chỉ load reference profile có cùng model version; mismatch phải fail explicit.

## CT-14 — No-drift scenario

Reference-like runtime window đủ sample → report `STABLE`.

## CT-15 — Drift scenario

Gradual/sudden/categorical drift fixture → expected feature score/status và aggregate `WARNING` hoặc `CRITICAL` theo policy fixture.

## CT-16 — Insufficient drift samples

`sample_count < minimum_samples` → `INSUFFICIENT_DATA`; không emit critical drift conclusion.

## CT-17 — Drift isolation

Drift Worker down/error → Backend/ML/Risk/Alert critical path vẫn pass integration test.

---

# 27. End-to-End Example — Kafka-backed asynchronous

## Step 1 — Sensor → Ingestion HTTP

```http
POST /api/v1/sensor-readings
Content-Type: application/json
```

```json
{
  "event_id":"evt_demo_001",
  "machine_id":"M-003",
  "received_at":"2026-10-17T01:00:00Z",
  "source":"AI4I_SIMULATOR",
  "features":{"product_type":"H","air_temperature_k":300,"process_temperature_k":310,"rotational_speed_rpm":1300,"torque_nm":62,"tool_wear_min":210}
}
```

Ingestion kiểm tra transport/envelope, Kafka publish `sensor.readings` với key `M-003`, broker ACK rồi trả:

```http
HTTP/1.1 202 Accepted
Location: /api/v1/sensor-events/evt_demo_001/status
```

```json
{"event_id":"evt_demo_001","machine_id":"M-003","ingestion_status":"ACCEPTED","processing_status":"PENDING","status_url":"/api/v1/sensor-events/evt_demo_001/status","correlation_id":"cor_demo_001"}
```

## Step 2 — Hai consumer groups độc lập

```text
sensor.readings partition N offset X
  ├─ processing-v1: Data Quality -> ML -> Risk
  └─ raw-archive-v1: JSONL.gz batch -> S3 raw-sensor/
```

Nếu Processing/ML thất bại, raw vẫn archive; nếu S3 thất bại, Processing vẫn xử lý miễn Kafka còn giữ record chưa commit cho Storage group.

## Step 3 — Processing Worker và status API

Data Quality hợp lệ thì ML trả (giá trị chỉ minh họa):

```json
{"prediction_id":"pred_demo_001","event_id":"evt_demo_001","prediction":"FAILURE","failure_probability":0.91,"model_version":"maintai-ml-1.0.0","preprocessor_version":"maintai-preprocessor-1.0.0","model_threshold":0.63,"inference_ms":18,"predicted_at":"2026-10-17T01:00:01Z"}
```

Worker persist `sensor_events`/`predictions`/`risk_decisions` và `processing_outbox` trong transaction. `GET /api/v1/sensor-events/evt_demo_001/status` trả `200` khi materialized; trong khoảng chuyển tiếp trước đó có thể trả `404 EVENT_NOT_MATERIALIZED`. Không gán `alert_id` tại Processing.

## Step 4 — Outbox → Business Consumer → Alert

Outbox relay publish `alert.signals` với `signal_id`, source `event_id`, `prediction_id`, risk/severity, model/policy versions. Business Consumer ghi `business_inbox`, tạo/dedupe alert `alt_demo_001` và audit trong `business_db`. Business API sau đó cho phép React GET/SSE xem alert; không cần RAG để tạo alert.

## Step 5 — RAG, technician và drift

Business API phục vụ `POST /alerts/{id}/brief` (async generation) sau khi alert đã tồn tại, RAG trả brief có grounded citations hoặc `INSUFFICIENT_EVIDENCE`/UNAVAILABLE; technician `ack`/`start`/`resolve` theo state machine cũ. Drift Worker batch dùng Processing VALID runtime window + model-matched drift reference, ghi `drift_db`/S3 và metrics; không tự điều chỉnh alert severity hoặc trigger training.

## Step 6 — Failure và replay assertions

- Kafka không ACK → Ingestion 503/504, Simulator retry cùng `event_id`.
- Feature sai type nhưng envelope đúng → 202, Processing `INVALID`/không gọi ML, S3 vẫn lưu.
- Rebalance/crash dẫn tới message redelivery → processing unique ID + business inbox + storage manifest chống business side effects trùng.
- ML unavailable → Processing `UNKNOWN/FAILED`, không `NORMAL`.
- Kafka retention hết khi consumer chậm → **có nguy cơ mất events chưa consume**, cần monitoring/SLO outage.

---

# 28. Repo / Contract File Layout thực tế và phần còn cần migrate

App monorepo hiện có `backend/`, `frontend/`, `ingestion-api/`, `processing/`,
`storage-consumer/`, `sensor/`, `ml-service/`, `rag-service/`, `drift-worker/`,
`architecture/` và `docs/`. GitOps/hạ tầng ở hai repo riêng, không tạo `platform/`
trong app chỉ vì ví dụ kế thừa có tên đó. Simulator UI riêng có source/client/demo
tại `sensor-simulator-UI/`; Sensor0.3.0 đã implement Control provider và local
browser/HTTP integration. Production IdP/Kafka E2E và rollout chưa verify.

```text
contracts/
├── README.md
├── requirements.in
├── requirements.lock
└── v1.3/
    ├── openapi/   # Sáu provider API, info.version1.3.2
    ├── schemas/   # Shared/runtime/Drift schemas baseline
    ├── events/    # Sensor/results/alert/archive/Drift/SSE baseline
    ├── examples/
    └── validate.py
```

Markdown target là1.4.0; HTTP bundle máy đọc được vẫn1.3.2. Kafka schemas đã freeze
riêng tại [contracts/kafka/](../contracts/kafka/README.md); `contracts/provenance/`
trong bộ bàn giao vẫn chưa được cung cấp. Freeze provider/release schemas và
consumer migration trước khi đổi producer. Xem [repository layout](repository-layout.md)
và [readiness](implementation-readiness.md).

---

# 29. Definition of Done cho Contract v1

Contract v1 được xem là frozen khi:

- [ ] SensorEvent field/type/unit đã chốt.
- [ ] Không có target leakage fields trong ML request.
- [ ] Backend → ML request/response có mock contract test.
- [ ] ML error/timeout semantics đã chốt.
- [ ] Risk policy schema có `policy_version`.
- [ ] Alert states/transitions đã chốt.
- [ ] Dedupe rule có fixture test.
- [ ] Backend → RAG request/response có `grounded_status` + citations.
- [ ] `INSUFFICIENT_EVIDENCE` path có test.
- [ ] Backend → Frontend Machine/Alert DTO đã chốt.
- [ ] `INVALID`/`UNKNOWN` được Frontend hiển thị khác `NORMAL`.
- [ ] `/health/live`, `/health/ready`, `/metrics` tồn tại ở service chính.
- [ ] Log có `request_id` và `correlation_id`.
- [ ] `alert_id` truy vết được tới event/model/policy/RAG citations.
- [ ] Drift reference profile gắn đúng `model_version`/`dataset_version`.
- [ ] Drift Report schema + `DriftStatus` + insufficient-data/error semantics đã chốt.
- [ ] `drift_reports` + `drift_checkpoints` schema có migration/contract test.
- [ ] Drift metrics không dùng high-cardinality labels.
- [ ] Drift Worker failure không làm fail online alert path.
- [ ] Secrets không nằm trong contract payload hoặc source code.
- [ ] Cả4 thành viên cùng review version v1.4 sau khi code/OpenAPI/event schemas và compatibility migration đã thống nhất.

---

# 30. Tóm tắt contract cần freeze đầu tiên

Nếu nhóm chưa thể freeze toàn bộ ngay, phải freeze 5 contract này trước khi code sâu:

```text
1. SensorEvent
   Simulator -> Ingestion -> Kafka sensor.readings -> Processing

2. PredictionRequest / PredictionResponse
   Processing Worker <-> ML

3. RiskDecision + Alert DTO + Alert lifecycle
   Processing Risk + Kafka alert.signals + Business lifecycle + Frontend

4. MaintenanceBriefRequest / Response + Citation
   Business API <-> RAG

5. Health / Metrics / Correlation logging
   All services -> Platform

6. Drift Reference + Drift Report
   ML artifact / Runtime DB -> Drift Worker -> Platform
```

Năm contract đầu tiên vẫn là prerequisite cho critical path. Drift contract có thể freeze song song trong giai đoạn ML/observability, nhưng phải ổn định trước milestone Observable & Resilient.


---


# 31. Kafka/Event Contracts — áp dụng cho kiến trúc v1.3.1

## 31.1. Topic, consumer group và ownership

| Topic | Message key | Producer | Consumer group | Retention / ý nghĩa |
|---|---|---|---|---|
| `sensor.readings` | `machine_id` | Ingestion API | `processing-v1`; `raw-archive-v1` | Append-only log tạm thời; lưu đến khi mọi group bắt kịp, S3 là archive dài hạn |
| `alert.signals` | `machine_id` | Processing outbox relay | `business-alerts-v1` | Alert intent bất đồng bộ, độc lập với RAG |
| `sensor.quarantine` (tùy chọn) | `machine_id`/`event_id` | Processing | operator/recovery tooling | Technical poison/conflicts, không dùng để loại raw khỏi S3 |

Các consumer group **đọc độc lập**: một event trên `sensor.readings` có thể xử lý bởi Group Processing và Group Storage. Nhiều replicas *trong cùng group* chia các partitions, không phải mọi replica đều nhận mọi event. `machine_id` làm key để giữ thứ tự theo từng máy **trong một partition**; ordering không đảm bảo xuyên topic/group hoặc khi producer thay đổi key.

## 31.2. `sensor.readings` envelope

Kafka value (JSON mẫu, bảo toàn giá trị raw của features):

```json
{
  "schema_version": "1.0.0",
  "event_id": "evt_001",
  "machine_id": "M-001",
  "received_at": "2026-10-17T01:00:00Z",
  "ingested_at": "2026-10-17T01:00:00.125Z",
  "source": "AI4I_SIMULATOR",
  "features": {"product_type":"M","air_temperature_k":298.1,"process_temperature_k":308.6,"rotational_speed_rpm":1551,"torque_nm":42.8,"tool_wear_min":108},
  "correlation_id": "cor_001",
  "request_id": "req_001"
}
```

- `received_at` giữ nguyên nghĩa event-time từ payload Simulator v1.1; `ingested_at` là thời gian server nhận. Không dùng hai trường như thể đồng nghĩa.
- Giá trị `features` chưa qua Data Quality; không loại outlier/range invalid ở Ingestion. `schema_version` thuộc **event envelope**, không phải version API hay model.
- Kafka key = `machine_id`; headers có `correlation_id`, `event_id`, `schema_version`; producer settings đề xuất `acks=all`, idempotence, `enable_auto_commit=false` ở consumer. Dùng TLS/SASL khi triển khai ngoài local.
- Archive bảo toàn event payload và metadata (topic/partition/offset), nếu cần lưu **raw HTTP bytes** phải thiết kế trường raw bytes riêng; JSON serialization thông thường không byte-identical với HTTP input.
- Topic `sensor.readings` không compact theo `event_id` (nó dùng machine key); cấu hình retention theo khả năng outage, lag, dung lượng, thời gian archive. Tăng partitions sau này có thể làm thay đổi đường phân phối key; cần test ordering/race.

## 31.3. `alert.signals` event

```json
{
  "schema_version": "1.0.0",
  "signal_id": "sig_001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "machine_id": "M-001",
  "severity": "CRITICAL",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "decided_at": "2026-10-17T01:00:01Z",
  "correlation_id": "cor_001"
}
```

`signal_id` idempotency key cho Business inbox. Business nhận cùng signal nhiều lần không tạo alerts hoặc alert_events trùng. Alert dedupe giữa **các event khác nhau** vẫn dùng `(machine_id, severity, active status, cooldown)` theo hợp đồng §10, không được nhầm với event-idempotency. Business là nơi duy nhất tạo `alert_id`; Processing chỉ gửi intent.

## 31.4. Transaction boundaries và delivery guarantees

1. Ingestion trả `202` sau khi Kafka broker ACK; khi publish timeout/không chắc outcome thì trả lỗi retryable, Simulator retry **same** `event_id`; có thể xảy ra duplicate.
2. Processing consumer: parse/validate → gọi ML nếu VALID → transaction persist sensor/prediction/risk và **outbox signal** → commit DB → commit Kafka offset. Nếu crash trước offset commit, reprocessing phải idempotent. Không cam kết exactly-once xuyên Kafka, PostgreSQL và S3.
3. Outbox relay: publish `alert.signals`, xác nhận broker, rồi mark published; crash có thể phát lại → Business inbox chống trùng.
4. Business consumer: insert inbox marker + create/update alert/audit trong **cùng DB transaction**; commit trước Kafka offset. Máy chưa tồn tại hoặc signal sai contract được quarantine/triage, không silently create new machine.
5. Storage consumer: ghi batch S3 thành công + manifest/checkpoint (idempotent object key gắn partition/start/end offsets), sau đó commit offset. Retry có thể tái ghi batch; bảo đảm recover/dedupe theo manifest; không tuyên bố exactly-once nếu chưa chứng minh.
6. ML transient timeout: backoff/retry hữu hạn, status `RETRYING`; hết budget thì `FAILED/UNKNOWN` có reason và khả năng replay; không đổi thành `NORMAL`.
7. Poison/invalid: event có envelope hợp lệ vẫn được archive; validation invalid phải persist `INVALID` và commit sau ghi. Parse poison/conflict tạo quarantine record với reason rồi commit khi xử lý an toàn; đừng retry vô hạn gây nghẽn partition.

## 31.5. Simulator và outage

Sensor giữ local durable buffer (có giới hạn quota/TTL) và retry exponential backoff + jitter với cùng payload/ID khi mạng, Ingestion hoặc Kafka lỗi. 4xx do malformed/auth/envelope không retry mù; 429/503/504 retry theo policy. Kafka full hoặc outage không thể bảo đảm "sensor luôn gửi thành công". Phải đo `oldest_unacked_event_age`, `buffer_used_bytes`, `retry_count`.

## 31.6. Contract đọc runtime cho Drift và Frontend

Processing owner cung cấp read-only APIs (implementation may proxy via Business):

```http
GET /api/v1/internal/runtime-windows?model_version=maintai-ml-1.0.0&limit=500&before=...
GET /api/v1/sensor-events/{event_id}/status
```

Response window chứa `items[]` với **VALID six features**, event/prediction times, `prediction`/probability khi có, `model_version`, `next_before`, `window_start`, `window_end`, `snapshot_at`. Page ổn định với cursor `(event_time, event_id)` và filter explicit; không dùng truy vấn paginated ngẫu nhiên tạo window không tái lập được. Drift phải xác nhận reference `model_version`/`dataset_version` match; không lấy raw invalid làm input reference-comparison. S3 raw dùng cho audit/data prep, **không coi là runtime VALID window nếu chưa qua chuẩn hóa tương ứng model**.

Drift CronJob ghi `drift_db` và S3 report, công bố Pushgateway, checkpoint riêng; không consume Kafka realtime trong baseline này. Training không tự kích hoạt từ mỗi object mới; raw → quality curation/versioned dataset → approved training trigger → MLflow.

## 31.7. Scaling, SLO và observability

- Scaling: Ingestion `HTTP RPS/p95`, Processing `consumer_lag + event_to_prediction_latency + ML p95`, Storage `lag + S3 batch latency`, Business `alert_signal_lag + SSE connections`. HPA/KEDA chưa bắt buộc ở MVP.
- Phân biệt `ingestion_ack_latency` (HTTP → broker ACK), `event_to_prediction_latency` và `event_to_alert_visible_latency`; không dùng 202 để tuyên bố inference SLA.
- Prometheus: `kafka_consumergroup_lag` (KafkaExporter), `maintai_ingestion_accepted_total`, `maintai_ingestion_publish_failures_total`, `maintai_processing_events_total{status}`, `maintai_storage_batches_total{status}`, `maintai_alert_signals_total{outcome}`, S3 errors, DB pool saturation, DLQ/quarantine count. Không gắn high-cardinality `event_id`/`machine_id` lên metrics.
- Health: Ingestion readiness cần Kafka producer connectivity/config; Processing readiness cần Kafka+processing_db và ML connectivity theo deployment policy; Storage needs Kafka+S3; Business Consumer needs Kafka+business_db. Dependency outage không được đưa vào liveness.
- Cần verify Kafka retention lớn hơn `max expected outage + catch-up time + safety margin`; alert nếu group lag/oldest age tiến gần retention.

## 31.8. Migration từ v1.1 synchronous sang v1.2 async

| Hành vi cũ | Hành vi mới | Việc phải làm |
|---|---|---|
| HTTP sensor POST trả 200 hoàn tất hoặc 422 detailed INVALID | POST trả 202 khi Kafka ACK; detailed INVALID async | Cập nhật simulator, OpenAPI, UI fixtures, consumer tests |
| Backend HTTP trực tiếp gọi ML và tạo alert | Kafka Processing consumer → ML → risk → outbox → Business consumer | Tách codebase entrypoints, ownership bảng và event contracts |
| Backend dùng chung `machines`,`sensor_events`,`alerts` | Business owns machines/alerts, Processing owns events/predictions | Split Alembic/migrations, copy/migrate data, external ID links |
| Drift query SQL runtime DB | Drift qua Processing API hoặc curated S3 | Cập nhật drift data adapter, checkpoint semantics |
| Raw sensor lưu operational DB/export | Kafka Storage Consumer → S3 raw archive | S3 batch & offset tracking, retention alerts |
| API `GET /machines` đọc trực tiếp tất cả từ một DB | Business API compose/maintain projections qua stable API/event | Không SQL cross-DB; add integration tests |

**Machine-readable OpenAPI/JSON Schemas và source code không nằm trong năm file được cung cấp.** Đây là *target contract update*, không phải xác nhận các endpoint, topics, tables hoặc CI hiện tại đã được triển khai. Phải cập nhật riêng các file tại `contracts/`, code, tests và migration trước khi công bố v1.2 runtime compliant.


---

# 32. REALTIME-UI-01 — Phương án C: Live Telemetry + Latest Valid Prediction (v1.3.1)

## 32.1. Tính chất nghiệp vụ và tính chính xác

- **Nguồn yêu cầu:** phương án C trong hai cuộc trao đổi thiết kế UI: **Live Telemetry** cập nhật khi Kafka chấp nhận reading, **Latest Valid Prediction** chỉ cập nhật sau khi inference hoàn thành thành công; không nhấp nháy `PENDING` từng giây.
- `HTTP 202` xác nhận **event được Kafka chấp nhận**, không xác nhận feature hợp lệ/ML thành công. Live telemetry là **received raw, unvalidated**, không được ngầm gắn nhãn `VALID`.
- Latest telemetry và latest prediction **có thể thuộc hai event_id khác nhau**. UI phải chỉ rõ `telemetry.event_id`, `latest_valid_prediction.source_event_id`, timestamps và độ trễ. Không mô tả giá trị prediction cũ như kết luận về reading hiện hành.
- Processing event hiện tại `INVALID/FAILED/UNKNOWN` không được ghi đè/xoá *lịch sử* latest valid prediction; phải cập nhật **processing health/status** riêng và giữ active alerts theo rule cũ. UI vẫn hiển thị latest prediction với nhãn **historical/stale**, không suy ra `NORMAL`.
- Không được bỏ qua đánh giá alert/audit chỉ vì UI chọn không render một `prediction.completed` đến muộn. `machine_state` có source/timestamp riêng; không tự tính từ color/probability.
- Sensor data AI4I có `machine_id` và timestamp mô phỏng; “realtime” ở UI nói về **thời gian hệ thống nhận và render**, không chứng minh IoT measurements từ thiết bị thật.

## 32.2. Kafka topics và các consumer groups bắt buộc

| Topic | Producer | Consumer group / owner | Mục đích |
|---|---|---|---|
| `sensor.readings` | Ingestion API | `processing-v1` / Processing | Data Quality, ML, Risk, persistence |
| `sensor.readings` | Ingestion API | `raw-archive-v1` / Storage Consumer | Batch raw events vào S3 (kể cả invalid features) |
| `sensor.readings` | Ingestion API | `business-live-v1` / Business Consumer telemetry handler | Materialize latest received telemetry và notify UI |
| `inference.results` | Processing outbox relay | `business-prediction-v1` / Business Consumer prediction handler | Materialize latest completed valid prediction, cập nhật processing status |
| `alert.signals` | Processing outbox relay | `business-alert-v1` / Business Consumer | Dedupe/create/update alert lifecycle và audit |

Topic key là `machine_id` để phân vùng theo máy. Không giả định thứ tự xuyên topic/group. `inference.results` phải xuất từ durable outbox trong `processing_db` để tránh trạng thái ghi prediction xong nhưng mất event. Nên chạy handlers của `business-live-v1` và `business-prediction-v1` trong cùng Business Consumer deployment/image (group IDs tách biệt), nhưng không dùng cùng schema tables của Processing trực tiếp. Storage Consumer phải commit offset sau khi object S3/manifest được ghi thành công và có idempotency.

**Tên topic được chốt:** từ 1.3 dùng `sensor.readings` theo thiết kế UI đã cung cấp. `sensor.raw` là tên lịch sử ở bộ 1.2, **không chạy song song như alias tự động**; việc migrate producer/consumers, ACL, offsets, fixtures và dashboard phải được plan rõ. Raw archive vẫn chứa raw bytes/payload từ `sensor.readings`.

## 32.3. Contract events: telemetry/prediction

Event bên dưới minh hoạ fields thêm vào **existing Kafka envelope**; các field `event_id`, `machine_id`, `received_at`, `source`, `features`, `schema_version`, `correlation_id` kế thừa §5/§31. `server_accepted_at` do ingress tạo, khác thời gian simulator đo. `sequence_number` **bắt buộc tăng đơn điệu và không reset theo mỗi machine_id kể cả Pause/Resume/restart**, lưu checkpoint bền vững ở Simulator. `stream_epoch` là nhãn lineage để nhận biết phiên chạy, không dùng thứ tự từ string epoch để so sánh các readings. Nếu không đảm bảo sequence bền vững, phải dùng một ingress durable monotonic ordering token do hệ thống cấp trước khi bật latest-projection CAS.

```json
{
  "schema_version": "1.3.0",
  "event_id": "evt_004",
  "machine_id": "M-001",
  "sequence_number": 4,
  "received_at": "2026-10-17T00:00:03.000Z",
  "server_accepted_at": "2026-10-17T00:00:03.050Z",
  "source": "AI4I_SIMULATOR",
  "features": {"product_type":"M","air_temperature_k":298.1,"process_temperature_k":308.6,"rotational_speed_rpm":1551,"torque_nm":42.8,"tool_wear_min":108},
  "correlation_id": "cor_004"
}
```

`inference.results` dùng một envelope có outcome và lineage; **chỉ `COMPLETED` + `VALID` mới chứa prediction**. Để phân biệt out-of-order và trạng thái thất bại, phải emit cả terminal `INVALID`/`FAILED` (hoặc event `processing.status.changed` tương đương đã version); không publish NORMAL giả khi ML lỗi.

```json
{
  "schema_version": "2.0.0",
  "result_id": "res_002",
  "result_kind": "PREDICTION_COMPLETED",
  "source_event_id": "evt_002",
  "machine_id": "M-001",
  "stream_epoch": "run_20261017_a",
  "sequence_number": 2,
  "processing_status": "COMPLETED",
  "validation_status": "VALID",
  "completed_at": "2026-10-17T00:00:03.000Z",
  "failure_probability": 0.12,
  "prediction": "NORMAL",
  "machine_state": "NORMAL",
  "model_version": "model-v1",
  "policy_version": "risk-policy-v1",
  "correlation_id": "cor_002"
}
```

Với `INVALID`/`FAILED`, các field ML (`failure_probability`, `prediction`, `model_version` nếu không có model chạy) để null theo contract và thêm `validation_errors` hoặc `failure_code` có kiểm soát. `processing_status=COMPLETED` không tự đồng nghĩa `alert_id` đã tồn tại; alert thuộc lifecycle của Business Consumer, được phát riêng.

## 32.4. Backend live read model & REST snapshot

Business sở hữu **read model phục vụ UI**, không sở hữu raw sensor full history:

- `live_machine_telemetry(machine_id, event_id, stream_epoch, sequence_number, features_raw, received_at, server_accepted_at, ingested_at, source)` — bản ghi mới nhất được nhận, **chưa mặc định valid**.
- `live_machine_prediction(machine_id, source_event_id, stream_epoch, sequence_number, prediction_id, failure_probability, prediction_label, machine_state_at_prediction, model_version, policy_version, source_received_at, completed_at)` — prediction **hợp lệ hoàn thành mới nhất theo thứ tự event**; không xóa nếu reading gần nhất invalid/FAILED.
- `live_machine_processing(machine_id, source_event_id, stream_epoch, sequence_number, status, validation_status, failure_code, updated_at)` — trạng thái xử lý của reading gần nhất; trạng thái cần phân biệt với prediction đang trưng bày.

Đây là **read projections / bảng khuyến nghị**, không bắt buộc dùng đúng 3 bảng vật lý; có thể dùng một bảng snapshot trong `business_db` nếu giữ ba trục trạng thái độc lập. Không lưu toàn bộ raw history thêm lần hai; raw history ở `processing_db`/S3. Việc ghi projection cần idempotency key và compare-and-set theo **`sequence_number` tăng đơn điệu xuyên các `stream_epoch` cho cùng máy**; `stream_epoch` chỉ giúp định danh phiên chạy/lineage, không được đem string epoch ra lexicographic compare. Nếu không có durable sequence đủ tin cậy, phải thay bằng ordering token assigned từ ingress durable ledger trước khi claim latest projection chính xác. `received_at` do simulator tạo không phải khóa thứ tự tuyệt đối nếu clock lệch hoặc retries.

**Endpoint:** `GET /api/v1/machines/{machine_id}/live` (Business API, owner `business_db`), trả 200 snapshot dạng:

```json
{
  "machine_id": "M-001",
  "telemetry": {"event_id":"evt_004","sequence_number":4,"server_accepted_at":"2026-10-17T00:00:03.050Z","validation_status":"UNVERIFIED","features_raw":{"rotational_speed_rpm":1551}},
  "latest_valid_prediction": {"source_event_id":"evt_002","sequence_number":2,"completed_at":"2026-10-17T00:00:03.000Z","failure_probability":0.12,"prediction":"NORMAL","machine_state_at_prediction":"NORMAL","model_version":"model-v1"},
  "latest_processing": {"source_event_id":"evt_004","processing_status":"PENDING","updated_at":"2026-10-17T00:00:03.050Z"},
  "as_of":"2026-10-17T00:00:03.060Z"
}
```

`UNVERIFIED` **chỉ là UI/projection receipt status**, không mở rộng `DataValidationStatus` của DQ (vẫn `VALID|INVALID`). Dữ liệu raw phải được escape/sanitize khi render vì có thể sai kiểu. Trường null cần xử lý rõ khi chưa có prediction lịch sử. `as_of` dùng để trình bày snapshot freshness, không bảo đảm mô hình bất kỳ đã chạy xong.

## 32.5. SSE events v1.3

**Endpoint:** `GET /api/v1/events/stream` (Business API facade; auth/tenant/machine filtering áp dụng). Thêm events:

| SSE event name | Phát khi nào | Dữ liệu tối thiểu | Phản ứng UI |
|---|---|---|---|
| `telemetry.received` | Live projector nhận `sensor.readings` | machine_id, event_id, sequence_number, server_accepted_at, features_raw | Cập nhật ô Telemetry nếu event mới hơn |
| `prediction.completed` | Live projector nhận valid `inference.results` | machine_id, source_event_id, sequence_number, completed_at, probability, prediction, model_version, machine_state_at_prediction | Cập nhật Latest Valid Prediction nếu mới hơn |
| `processing.status_changed` | INVALID/FAILED/RETRYING/PROCESSING | machine_id, source_event_id, status, occurred_at, failure_code? | Đổi state xử lý, không xoá prediction cuối hợp lệ |
| `alert.created` / `alert.updated` | Business transaction commit | alert_id, machine_id, severity, status, updated_at | Cập nhật alerts/badge, fetch detail khi cần |
| `brief.ready` / `service.degraded` / `drift.updated` | Có dữ liệu nguồn đã lưu | reference/status/timestamp | Hiển thị trạng thái tương ứng |

**SSE payload ví dụ** (payload là projection event, không phải output ML độc lập):

```text
event: telemetry.received
id: live-M-001-4
retry: 3000
data: {"machine_id":"M-001","event_id":"evt_004","sequence_number":4,"server_accepted_at":"2026-10-17T00:00:03.050Z","features_raw":{"rotational_speed_rpm":1551}}

event: prediction.completed
id: pred-M-001-2
data: {"machine_id":"M-001","source_event_id":"evt_002","sequence_number":2,"completed_at":"2026-10-17T00:00:03.000Z","failure_probability":0.12,"prediction":"NORMAL","model_version":"model-v1"}
```

`id` là hint để reconnect, không hứa durable SSE replay. UI luôn **GET snapshot sau reconnect**, sau đó merge với events; để tránh gap giữa fetch và subscribe, **mở SSE trước, buffer sự kiện trong khi fetch snapshot, áp ordering guard khi hợp nhất** (hoặc cursor-based consistent snapshot khi triển khai). REST snapshot là cơ sở đồng bộ, SSE chỉ tăng độ nhanh.

## 32.6. Quy tắc lựa chọn kết quả, freshness, trạng thái hiển thị

**Chính sách bảo vệ UI:** Với mỗi `machine_id`, chỉ ứng dụng event telemetry/prediction có ordering token mới hơn giá trị hiện tại; không lấy `completed_at` để xác định record sensor nào mới hơn. Khi prediction R1 hoàn thành sau R2, vẫn persist lịch sử & xử lý alert theo policy, nhưng UI không ghi đè R2 bằng R1. Event invalid mới nhất không làm mất latest valid prediction, nhưng gắn nhãn current data invalid.

Hiển thị **hai freshness** khác nhau:

1. Telemetry age: `now - server_accepted_at` (phản ánh thời điểm hệ thống nhận).
2. Prediction **source age / lag**: `now - source_server_accepted_at` (nếu projection mang theo) và chênh `telemetry.sequence_number - prediction.sequence_number` khi cùng sequence space; `completed_at` là thời gian hoàn thành chứ **không** là thời gian measurement.

UX demo có thể dùng **ngưỡng cấu hình**: prediction source age < 2s = FRESH; 2–5s = UPDATING/DELAYED; >5s = STALE. Đây là ngưỡng ví dụ, **không phải model accuracy/SLO đã chứng minh**, phải điều chỉnh theo tốc độ replay và latency thực tế. No prediction → `NOT_AVAILABLE`; processing FAILED → `INFERENCE_UNAVAILABLE` cạnh prediction cũ; telemetry quá hạn → `TELEMETRY_STALE/OFFLINE_CANDIDATE` (không tự khẳng định thiết bị hỏng).

**UI không nhấp nháy PENDING theo từng record**; chỉ cho thấy chế độ Updating/Delayed khi có lag đáng kể. `STALE` không bằng `NORMAL`; dự đoán cũ phải mang nhãn “Latest completed prediction for event ...” và không làm giả hiện trạng máy.

## 32.7. Fan-out SSE khi Backend API có nhiều replicas

**Không được** dùng `business-live-v1` như một Kafka consumer group chia partitions giữa N Backend API Pods rồi giả định **mọi Pod đều nhận mọi event**. Kafka phân event cho một consumer trong group, khiến client SSE ở replica khác bỏ lỡ live updates.

**MVP:** tách `business-consumer` và `business-api`; sau khi commit read projection, dùng PG LISTEN/NOTIFY để fan-out qua mọi Business API Pod. In-process emitter chỉ là biến thể dev khi thực sự cùng process một API replica, không phải topology scale-ready mặc định.

**Scale-ready nhiều replicas:** chạy **một/tập consumer projector (group cố định)** để materialize vào `business_db`; sau khi commit, phát fan-out notification qua pub/sub đến **mọi API replica** (PostgreSQL LISTEN/NOTIFY cho demo traffic thấp hoặc Redis Pub/Sub khi cần). Notifications không thay thế REST snapshot: best-effort và có thể mất khi disconnect. API Pod nhận notification, lọc theo machine/user authorization rồi gửi SSE cho clients đang kết nối. Nếu không muốn thêm pub/sub lúc đầu, phương án correctness khác là một **unique Kafka group ID riêng cho mỗi SSE replica** để mỗi replica nhận toàn bộ events, đổi lại tiêu tốn consumer group resources; không dùng nó như giải pháp scale mặc định.

Business-owned live projector cần kiểm soát tốc độ cập nhật/sampling (ví dụ ≤1 UI update/s/machine), không đánh đồng sampling để render với việc bỏ event trong Processing/Archive. SSE client reconnect/resync phải hoạt động khi mất thông báo hoặc scale/restart.

## 32.8. Bộ acceptance tests realtime mới

- Sensor R1, R2, R3 gửi 1s/machine; telemetry SSE đến trước prediction. Không phát `NORMAL` giả trong thời gian inference pending.
- Artificial ML delay 1.5s/event: telemetry vẫn tăng đều, prediction có timestamp/source_event riêng, hiển thị Delayed/Stale theo ngưỡng.
- R2 hoàn thành trước R1: latest prediction vẫn R2; R1 có history/alert processing theo policy.
- R3 INVALID rồi R4 VALID: R3 không gửi ML; raw S3 chứa cả R3; UI hiển thị status INVALID và giữ prediction cũ có nhãn stale cho tới R4.
- ML down / processing lag: live telemetry vẫn đến; preview prediction cũ không bị coi là mới; alerts đã mở không tự đóng.
- Storage consumer down: live UI/inference vẫn chạy; archive lag tăng và catch-up trước retention; S3 payload recovery idempotent.
- SSE disconnect/reconnect + GET live snapshot: khôi phục đúng state, không trùng hoặc lùi sequence.
- 2 Business API replicas: cùng một telemetry event phải được fan-out đến clients kết nối **ở cả hai replicas**.
- 401/403/stream subscribe filter: client không được xem dữ liệu máy không được phép.
- Scale test/metrics: ingress RPS/p95, Kafka consumer lag (3 nhóm sensor.readings), event-to-telemetry UI p95, event-to-prediction UI p95, dropped notification/resync, SSE connections.

## 32.9. Phạm vi thay đổi so với v1.2

| v1.2 | v1.3 |
|---|---|
| Topic `sensor.raw` (lịch sử) | **`sensor.readings`** (một tên chuẩn thống nhất) |
| Kafka chỉ có Processing + Storage groups từ source readings | Thêm Telemetry live projector group, vẫn giữ Storage/Processing độc lập |
| Không có results broadcast topic riêng | Thêm **`inference.results`** (outbox + versioned envelope) |
| UI chỉ nhận machine/alert updates hoặc polling | UI **C**: live telemetry SSE ngay sau ingress và latest valid prediction SSE sau inference |
| Machine Detail có thể hiểu null prediction theo event mới nhất | Phân biệt `latest_event_result` (có thể null) với **last valid prediction** (lịch sử, gắn age) |
| SSE optional chung chung | SSE ưu tiên demo realtime + multi-replica fan-out/resync contract |

Các cập nhật được diễn giải là **target design**, không có nghĩa code, SQL migrations, OpenAPI/JSON Schema, Kafka topics hoặc UI đã được implement. Breaking change topic cần migration review trước khi triển khai.


---

# 33. ARCH-DECISION-01 — Kiến trúc nghiệp vụ và Simulator Control (bổ sung v1.3.1)

## 33.1. Ranh giới service/deployment không nhập nhằng

- **`ingestion-api`** độc lập Business Backend; không ghi `business_db`, không gọi ML, không đẩy SSE, chỉ guard HTTP và Kafka ACK. Việc đặt chung monorepo không làm nó thành business module.
- **`processing-worker`** tạo validation/inference/risk, persist `processing_db`, phát `inference.results` và `alert.signals` bằng outbox. `ml-service` tính model probability, không tạo alerts.
- **`business-consumer`** (thuộc Business Backend) có `telemetry_handler` cho `sensor.readings`, `prediction_handler` cho `inference.results`, `alert_handler` cho `alert.signals`. Chúng cùng logic ownership, không phải 3 microservices độc lập; mỗi handler idempotent, có offset/checkpoint/inbox strategy.
- **`business-api`** (thuộc cùng Business Backend) chỉ cung cấp REST/SSE, authorization, business actions và Simulator Control facade; không truy cập trực tiếp `processing_db`.
- **`storage-consumer`** archive mọi Kafka-accepted raw sensor envelope hợp giới hạn từ `sensor.readings` vào S3, kể cả dữ liệu feature bị Data Quality đánh INVALID. Kafka raw topic không phải kho lưu trữ dài hạn.

## 33.2. Dữ liệu nào vào Business DB?

| Dữ liệu | `business_db` | Source-of-truth đầy đủ |
|---|---|---|
| Latest raw **snapshot per machine** | Có, 1 row/read model per machine (hoặc cache phù hợp); update có kiểm soát | Accepted Kafka event; raw archive trên S3 |
| Full raw reading history | **Không** | S3/MinIO raw archive |
| VALID/INVALID, prediction/risk history | Không lưu toàn bộ; chỉ latest projection phục vụ UI | `processing_db` |
| Latest valid prediction | Có, snapshot chứa `source_event_id`, seq/epoch, probability, model, policy, timestamps | `processing_db` |
| Latest processing status | Có, snapshot riêng; không xóa prediction hợp lệ lịch sử khi event mới INVALID/FAILED | `processing_db` |
| Machine registry, alert lifecycle, technician feedback | Có, owner nghiệp vụ | `business_db` |

Ghi DB chỉ để phục vụ REST recovery/read model, **không phải điều kiện kỹ thuật để gọi SSE**. Do sử dụng nhiều Business API Pod, sau transaction commit Business Consumer phát notification/pubsub tới toàn bộ SSE replicas. Với MVP dùng PostgreSQL `NOTIFY` sau transaction; payload thông báo ngắn chứa machine_id/change_type, API lấy projection theo DB (có coalescing/rate limiting). Mất notification -> GET snapshot resync. Không khuyến nghị ghi mọi raw event vào Business DB.

## 33.3. HTTP Ingestion: lỗi schema không làm dừng cả Simulator

| Tình huống | Ingestion HTTP | Simulator | Xử lý sau đó |
|---|---|---|---|
| Basic envelope hợp lệ, Kafka ACK | `202` | Tiếp tục phát; ổn định event_id khi retry | Cả 3 groups có thể đọc |
| JSON hỏng | `400` | Đánh dấu bản ghi bị từ chối, lưu local rejected log/quarantine, **tiếp tục** bản ghi sau | Không tự publish vào sensor.readings |
| Thiếu `event_id`, `machine_id`, `features` không phải object | `422` | Ghi rejected + **không retry mù**; tiếp tục phát nếu RUNNING | Không inference, không archive Kafka event đã bị reject |
| Envelope hợp lệ nhưng feature thiếu/sai/range như RPM âm | `202` | Gửi record tiếp theo | Processing INVALID + validation details; Storage S3 vẫn lưu raw |
| Kafka down/ACK timeout | `503/504` | Local durable retry queue, exponential backoff, same event_id/payload; không coi đã accepted | Tiếp nhận lại khi Kafka phục hồi |
| `429` | retry theo `Retry-After` có giới hạn | Buffer, tránh spam | Producer backpressure |
| Auth invalid `401/403` | cấu hình/credential cần can thiệp | Không retry vô hạn; cảnh báo lỗi control plane | Không tạo Kafka event |

**Giới hạn:** Client-side rejected log không thay thế central raw archive. Nếu muốn quản lý malformed HTTP payload tập trung thì cần thiết kế explicit ingress quarantine an toàn riêng (auth/PII/size), **không** giả định Storage Consumer có thể lưu message chưa từng vào Kafka.

## 33.4. Business Consumer → SSE sau khi đọc Kafka

1. `sensor.readings` -> `telemetry_handler` nhận event *accepted nhưng UNVERIFIED*, cập nhật latest telemetry bằng CAS ordering; commit transaction, gửi change signal -> Business API replicas -> `telemetry.received` SSE. Không chờ Processing.
2. Processing xử lý Data Quality; VALID -> ML -> Risk; INVALID/timeout -> status INVALID/FAILED/UNKNOWN, không phát prediction giả. Emit `inference.results` với **discriminated result kind** (xem §33.5) để Business có cập nhật kết quả không prediction.
3. `prediction_handler` nhận result; `PREDICTION_COMPLETED` mới cập nhật `latest_valid_prediction`; `DATA_INVALID` hoặc `INFERENCE_UNAVAILABLE` chỉ cập nhật `latest_processing` và DQ status cho đúng event, không xóa last valid prediction.
4. `alert_handler` nhận `alert.signals`; inbox+dedupe/cooldown trong Business transaction -> alert row/history; notification -> `alert.created/updated`. Alert không tự đóng khi gặp reading NORMAL mới; RAG nằm ngoài alert path.
5. API SSE không phải Kafka consumer cùng group: projector dùng `business_db` + NOTIFY/pubsub để fan-out đến **mọi** API replicas; FE mở SSE trước REST snapshot rồi merge theo `sequence_number` bền vững theo máy cho từng trục telemetry/prediction/status. UI cũ/mới có thể có source_event khác.

## 33.5. Kết quả Processing — đã thay bằng outcome1.4.0

Định dạng union2.0.0/result_kind và ví dụ1.1.0 trước đây đã bị thay thế.
Tất cả producer/consumer nghiệp vụ dùng [schema outcome1.4.0](../contracts/kafka/events/inference-result.schema.json)
và [fixtures hiện hành](../contracts/kafka/examples/manifest.json), theo §35 và
[OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md). Không viết producer từ ví dụ cũ.

`kind` gồm PREDICTION_COMPLETED, DATA_INVALID, INFERENCE_UNAVAILABLE. Kafka key
là machine_id; result_id là identity bất biến; event_id dẫn về sensor event.
COMPLETED cần risk_score cùng model/policy/source provenance; hai outcome lỗi
không có prediction/risk giả. Lỗi inference chỉ terminal sau retry được cấu hình;
PENDING có thể suy từ telemetry/status API, không cần phát mỗi trạng thái trung gian.

## 33.6. Simulator UI riêng, Pause/Resume và scenario (amendment1.3.2)

Control plane: **Simulator UI -> Sensor Control API**, same-origin management
Ingress `/api/v1/simulator/...`. Main Frontend chỉ gọi Business; Business không
còn control facade. Role SIMULATOR_OPERATOR hoặc ADMIN có explicit control scope.
Sensor persist control state/version trước200; idempotency key + expected version
bảo vệ thao tác cũ. Chi tiết payload ở OpenAPI Simulator và Guide §46.

```http
GET  /api/v1/simulator/machines
GET  /api/v1/simulator/machines/{machine_id}/control
POST /api/v1/simulator/machines/{machine_id}/pause
POST /api/v1/simulator/machines/{machine_id}/resume
PUT  /api/v1/simulator/machines/{machine_id}/scenario
```

Mutation body có expected_control_version; scenario thêm scenario name allowlist.
Cùng key khác body409; version mismatch409 CONTROL_VERSION_CONFLICT. Timeout phải
GET reconcile, không tự xác nhận thành công. Sequence/state/spool bền vững qua
restart; một controller Recreate, strict failover/multi-controller cần lease.
Pause dừng new generation, không hủy backlog/retries/alerts; scenario không tự
resume. RUNNING/PAUSED, freshness, risk và alert lifecycle là các trục độc lập.
Sensor điều khiển demo, không phải PLC/actuator. Không đưa token secret vào JS.

## 33.7. Các scenario và bảng acceptance criteria

| Scenario | Expected result |
|---|---|
| CRITICAL alert -> ACK -> Pause -> Maintenance -> RESOLVE -> Recovery scenario -> Resume | Simulator PAUSED không có **new** readings; old in-flight results/alerts vẫn tới; alert chỉ RESOLVED theo technician action; resume có new seq/event_id và model quyết định xác suất |
| `PAUSED` rồi Kafka lag còn backlog | Live telemetry có thể cập nhật từ event cũ trong queue; UI hiển thị control PAUSED + accepted_at/sequence và label backlog, không gọi đó là fresh streaming |
| Timeout giữa Simulator UI và Sensor Control API | Không tự cho nút pause thành công; GET state để đối chiếu và idempotent retry nếu phù hợp |
| Replay invalid envelope R2 giữa R1/R3 | R2 bị reject/log riêng, simulator vẫn phát R3; source sequence có thể gap, không coi gap là hệ thống mất dữ liệu |
| VALID-envelope nhưng invalid feature | `202`, archived raw S3, processing INVALID, last valid prediction giữ nguyên với age/stale |
| Processed results R3 hoàn thành trước R2 | Projection/latest FE giữ R3 nếu ordering mới hơn, R2 vào history/alert vẫn xét |
| Business Consumer restart/multi API Pod | Inbox+offset consistency, no double alerts; all SSE API Pods receive notify; REST resync recovers notification loss |
| Pause controls offline real device? | Không: demo-only control cannot actuate PLC; operator alerts independently managed |

**Definition of Done:** các tests trên có trong contract/integration/E2E suites; S3 never duplicates per logical batch unintentionally; migrations và OpenAPI/JSON Schemas cập nhật ngoài docs.


---

# 34. v1.4 — Phạm vi áp dụng và thứ tự ưu tiên của các điều khoản mới

**Phiên bản thiết kế mục tiêu:** `1.4.0`. Các mục §34–§39 **ưu tiên hơn** lời văn kế thừa từ v1.3.1 nếu có mâu thuẫn; những bất biến không bị thay thế vẫn giữ hiệu lực. Đây là quyết định thiết kế, **không** có nghĩa source/runtime đã được sửa. JSON Schemas tại `../contracts/events/` và `../contracts/provenance/` là đầu vào chuẩn khi triển khai; OpenAPI của repo thật phải đồng bộ riêng.

Giữ nguyên: Ingestion Deployment riêng; DQ ở Processing Worker; Business không archive toàn bộ raw; latest telemetry khác latest VALID prediction; Alert/lifecycle do Business; RAG/Drift độc lập; năm logical DB. **Bắt buộc khai báo `processing-read-api` thành Deployment và internal Kubernetes Service riêng**, cùng codebase và `processing_db` với Processing nhưng dùng role `processing_ro`. Worker chỉ consume Kafka, không phục vụ History HTTP.

# 35. v1.4 — Kafka topics, HTTP status và quyền sở hữu

**Wire freeze của repo:** [Kafka bundle2.0.0](../contracts/kafka/README.md) và
[hướng dẫn tích hợp](KAFKA_DEVELOPMENT_GUIDE.md) là ánh xạ máy đọc được của mục
này: accepted1.3.0, rejected1.4.0, outcome reader/writer1.4.0 `kind`, alert1.0.0.
[OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md) bỏ reader2.0.0; Business giữ
risk_score gốc của COMPLETED, không lấy probability bù risk. Ví dụ inherited
ở §33.5 đã được thay bằng schema hiện hành. Quarantine mặc định HASH_ONLY,
key theo verified source identity.
SDK cung cấp transaction/transport, application callbacks và HTTP DTO migrations
vẫn cần owner triển khai; không suy ra toàn v1.4 đã hoàn tất.

| Topic | Producer | Consumer | Nơi lưu lâu dài | Thay đổi |
|---|---|---|---|---|
| `sensor.readings` | Ingestion sau kiểm tra envelope | Worker; Business Live; Storage Accepted | S3 accepted raw + hot Processing metadata | Giữ nguyên |
| `sensor.rejected` | Ingestion cho lỗi schema có auth/size phù hợp | Storage Rejected | S3 rejected sanitized envelope | **MỚI**; thay local-only log |
| `inference.results` | Processing Outbox Relay | Business Prediction, tùy chọn Storage Outcome | `processing_db`, tùy chọn S3 | Mở rộng tagged union, **không đổi topic** |
| `alert.signals` | Processing Outbox Relay | Business Alert | `business_db` Alerts/lifecycle | Giữ nguyên |

**Tính bền vững của HTTP reject:** Không lưu raw body tùy tiện khi request không xác thực, độc hại, quá lớn hoặc ngoài quarantine policy. Với request đã xác thực, giới hạn kích thước hợp lệ nhưng sai JSON/envelope, tạo event `sensor.rejected` gồm `rejection_id`, request/source identity, thời gian UTC, validation issues, chế độ xử lý payload và body đã sanitize/truncate hoặc hash khi được phép. **Đợi Kafka ACK trước khi trả `400 MALFORMED_JSON` hoặc `422 VALIDATION_FAILED`**. Kafka down => `503/504` có thể retry. Simulator local rejected log chỉ bổ trợ, không thay archive trung tâm. Payload sai schema có thể thiếu `machine_id`, nên không bắt buộc trường này làm Kafka key hoặc query filter.

**Kết quả Data Quality:** Envelope hợp lệ được `202` sau `sensor.readings` ACK kể cả khi feature value INVALID. Worker xử lý và publish `DATA_INVALID`, không gọi ML. Tagged union trong `inference.results` gồm:

- `PREDICTION_COMPLETED`: Có model/threshold/prediction/risk và định danh liên quan.
- `DATA_INVALID`: Có DQ validation errors; không có model/prediction/risk giả.
- `INFERENCE_UNAVAILABLE`: Có typed error và không có prediction thành công giả.

Mỗi outcome giữ `result_id`, `schema_version`, source `event_id`, `sequence_number`, Kafka source position, `dq_ruleset_version` + digest và model/policy version thực tế khi áp dụng. **Không bắt buộc thêm `sensor.quality.results` hay đổi tên `inference.results` trong v1.4.**

**Atomicity:** Transaction Worker phải ghi idempotent event/outcome/risk và tạo outbox cho **mọi outcome kết thúc**, kể cả INVALID/UNAVAILABLE. Chỉ tạo `alert.signals` outbox nếu risk policy yêu cầu. Input offset chỉ commit sau DB transaction bền vững. Relay gửi lại an toàn; Business Consumer ghi inbox và cập nhật projection/Alert trong cùng DB transaction trước khi commit Kafka offsets. Storage commit offsets sau khi S3 object và manifest đã bền vững.

# 36. v1.4 — Hợp đồng Processing Read API và History

**M6-HISTORY-01,29/10/2026, ưu tiên mô tả history cũ:** `processing-read-api` vẫn
là Deployment riêng, cùng Processing owner/DB. Đọc original outcomes bằng
processing_ro; được ghi metadata snapshot/export/catalog bằng pool/role quyền hẹp
riêng. Không cấp quyền sửa source/outcome/risk/inbox/outbox. Snapshot/export runner
thuộc Read API; Worker chỉ realtime; không thêm processing-maintenance. M6 giữ
processed history trong DB, raw ở S3/MinIO; chưa processed archive hoặc cleanup thật.
Business vẫn là public facade duy nhất; Alert/lifecycle/feedback thuộc business_db.

Target M6 giữ public sensor-readings route, raw receipts sort
`(received_at,topic,partition,offset)`; outcome history sort `(processed_at,result_id)`
với processed_at map processing_results.completed_at. Opaque cursor gắn snapshot,
query/scope và clock; không dùng event_id thay receipt locator hoặc completion time
để cập nhật latest. Baseline before/before_event_id không âm thầm đổi semantics.
Predictions/history/chart/export và filter from/to cần OpenAPI/schema/index/UI đồng
bộ trong PR M6 trước enable. Quarantine thiếu machine/time cần scope riêng, không
fabricate fields. Raw chưa archive/catalog lag phải báo thật; Kafka ACK không
chứng minh History đã query được. Đặc tả tại §44 và M6-HISTORY-01.

Role processing_ro vẫn chỉ SELECT; Read API M6 có thêm role metadata riêng,
không gỡ read-only guard của pool đọc. Owner migration cấp quyền; Worker/Relay
giữ quyền cũ. Business/Drift không SQL processing_db. Mỗi DB có Alembic owner;
dùng chung PostgreSQL vẫn chung CPU/I/O. Source M5 chưa có các grants/metadata M6.

# 37. v1.4 — Hợp đồng quản lý phiên bản và provenance

Quy chuẩn chính: `VERSIONING_AND_PROVENANCE.md`. Model release phải có source/curated dataset manifest bất biến, split indices+hash, training Git SHA + image digest, MLflow run ID, registered model version cụ thể, model/preprocessor artifact SHA256, feature schema, threshold evidence, drift reference digest và approved `release_id`. Inference **pin và xác minh** artifact/image identity khi startup. Alias `champion` chỉ là selector, **không** thay được immutable identity dùng cho audit.

Prediction thành công phải liên kết sensor event và DQ rules digest với exact registered model, MLflow run, effective release/image/artifact và threshold. Risk Decision lưu `policy_version` cùng digest. Alert lưu prediction/risk nguồn, các thời điểm source/detected/created và lifecycle append-only. Brief RAG lưu PDF/chunk versions cùng prompt/embedding/chunker. Drift report ghi model/reference digest, detector params, window watermark. Legacy records có thể thiếu provenance; **không backfill Git SHA/hash bằng suy đoán**.

Tham chiếu JSON Schemas: `../contracts/provenance/release_manifest.schema.json`, `../contracts/events/inference.results.v1_4.schema.json`, `../contracts/events/sensor.rejected.schema.json`, `../contracts/provenance/archive_manifest.schema.json`. JSON examples trong `../examples/` có digest **giả dạng hợp lệ cú pháp**, không được deploy. Quy định phê duyệt release, retention và rollback nằm trong tài liệu provenance chuẩn.

# 38. v1.4 — Ranh giới Deployment và nghĩa vụ chịu lỗi

Danh mục Kubernetes Deployments: `frontend`, `simulator-ui` (client/demo đã có, chưa deploy), `sensor-simulator`, `ingestion-api`, `processing-worker`, **`processing-read-api`**, `processing-outbox-relay`, `storage-consumer`, `business-consumer`, `business-api`, `ml-service`, `rag-service`, `mlflow`. `drift-worker` là CronJob; training là Job theo yêu cầu. Một codebase/image có thể chạy nhiều Deployments với commands khác nhau. Manifests thật bắt buộc xác minh entrypoints, secrets theo quyền tối thiểu và image OCI digest thực. Templates ở `../implementation/k8s/` còn placeholders và Drift CronJob đang suspend nên **chưa chạy trực tiếp được**.

Bài kiểm thử sự cố bắt buộc: Kafka producer lỗi; schema rejection khi Kafka không ACK; envelope đúng nhưng DQ INVALID; Worker DB commit rồi Relay down; Storage down; Business replay trùng; SSE fan-out tới hai Business API Pods; prediction stale/out-of-order; RAG down; drift-reference mismatch; tải History tăng khi Worker ổn định; rollback model nhưng Alert cũ giữ provenance. Mọi truy vấn History/release phải kiểm tra quyền.

# 39. v1.4 — Trình tự migration và tiêu chí nghiệm thu

Thực hiện theo thứ tự phụ thuộc: **khảo sát source/migrations → freeze schemas/fixtures tương thích → thêm DB columns/indexes → accepted/rejected producer+consumers → DQ outcome/outbox → S3 archive manifests → Read API Deployment riêng → MLflow lineage → RAG/Drift versions → release manifest/GitOps → E2E/recovery/load tests**. Consumers phải còn đọc được Kafka event schema cũ cho tới khi dữ liệu cũ hết retention hoặc có migration strategy thay thế. **Markdown/JSON Schemas/YAML scaffold không đồng nghĩa đã triển khai hoặc benchmark scale.** Xem `IMPLEMENTATION_PLAN.md` để giao việc từng owner và test assertions.

# 40. UI-01 — Bốn giao diện và ranh giới phát hành

**Quyết định dự án ngày2026-10-20, UI-01 revision1.0.0**, chi tiết ở
[UI_ARCHITECTURE.md](UI_ARCHITECTURE.md). Mục này ưu tiên mô tả gộp UI trong các
chương kế thừa khi có mâu thuẫn về navigation/ownership/release; payload/HTTP/event
vẫn theo các schema đã freeze và migration tương ứng.

| UI chính | Interface | Owner và runtime |
| --- | --- | --- |
| Business | Business REST/SSE | Người1; `frontend/` → image/Deployment frontend riêng |
| Sensor Simulator | Sensor Control API | Người4; simulator-ui planned → image/Deployment riêng; Sensor backend riêng |
| Grafana | Grafana server → Prometheus/exporters, report datasource được duyệt | Platform Người4; dashboards/config version riêng |
| MLflow | MLflow Server tracking/registry UI | ML Người2 + Platform Người4; server/config version riêng |

Portal chỉ là static launcher tùy chọn. `grafana_drift.html` là cùng Grafana UI
với tab mặc định Drift; số file HTML/dashboard không tạo thêm service hoặc DB.
Grafana/MLflow dùng giao diện công cụ, không thêm hai frontend React production.

**Business boundary:** giữ monitoring/drift summary/catalog/list/detail đã có
contract, Alert/History/Copilot/feedback theo scope. Main frontend chỉ gọi Business;
không điều khiển Sensor hoặc truy cập DB/Kafka/ML/RAG trực tiếp. Grafana là nơi
điều tra kỹ thuật sâu, không thay Business lifecycle hoặc chạy lại Drift detector.

**Simulator boundary:** browser gọi Sensor qua management origin/auth. Baseline
có list machines/read control/pause/resume/scenario, role SIMULATOR_OPERATOR/ADMIN,
Idempotency-Key và expected_control_version cho mutation. ACK sau persist;
timeout/unknown outcome phải GET reconcile; Pause không xóa spool/backlog/Alert.
Rate mutation, counters/queue/preview, batch commands và control SSE trong ý tưởng
demo chưa được coi là API có sẵn; cần freeze/implement DTO riêng trước khi enable.

**Platform boundary:** UI browser không chứa DB/S3/Kafka/service credentials.
Grafana drift report datasource cần Drift owner định nghĩa read-only views/grants
hoặc adapter có auth và query budgets. Không cho Grafana dùng Drift writer,
Business SQL drift_db hoặc browser SQL. MLflow Server giữ mlflow_db/artifact
access; thay run/alias không tự promote inference hoặc đổi historical provenance.

**Independent release:** Business và Simulator có build output/image digest,
Deployment/Service/public config và rollout/rollback riêng. Thay một frontend
không cập nhật Pod template của frontend/backend khác nếu API/dependency không
đổi. Chia folder hoặc cùng monorepo không thay điều kiện này. Shared dependencies
và breaking APIs phải kiểm tra các consumers bị ảnh hưởng, dùng compatibility
rollout thay vì giả định luôn deploy riêng được. CI/GitOps giữ check bắt buộc,
chọn affected components, pin đúng component image/config, không tự rollout tất cả.

**Acceptance:** chứng minh release một UI giữ image/config/Pod template của UI còn
lại; Sensor vẫn phát khi browser/Simulator UI đóng hoặc rollout; auth/scopes riêng;
unknown control command reconcile đúng; Grafana/MLflow outage không tự chặn
Business online path. Shared cluster/Ingress/IdP/DB vẫn là shared fault domains.
UI-01 không tăng wire version, không tạo source/runtime/provisioning trong lượt docs.


# 41. OBS-01 và MLFLOW-01 — Hợp đồng instrumentation và tracking

**MLFLOW-IMAGE-01,21/10/2026:** Image source/pins/local dev và publisher MLflow
chuyển sang `ml-service/mlflow/` trong MaintAI; production manifests/config/IRSA
vẫn ở GitOps/hạ tầng. Source domain không gộp training/inference/server images.
GitOps chỉ nhận app-signed MLflow digest từ maintai-publish.yml/main, đúng source
annotations và app actor; legacy GitOps producer không còn hợp lệ cho group này.
Giữ existing workload-release envelope; không đổi wire/API schemas, tự bật service,
publish/deploy hay coi local source checks là server integration evidence. Chi tiết
ở MLFLOW-01 §2.1 và Guide §52, ưu tiên các mô tả owner image kế thừa mâu thuẫn.

**Revision decision1.0.0, ngày2026-10-20.** Bổ sung chuẩn v1.4, không tăng HTTP/
Kafka versions hoặc chứng minh code đã migrate. Các điều khoản ở đây và catalog
được dẫn ưu tiên ví dụ metrics/MLflow kế thừa nếu mâu thuẫn.

1. [OBSERVABILITY_IMPLEMENTATION.md](OBSERVABILITY_IMPLEMENTATION.md) là catalog
   chuẩn: base names, Counter/Gauge/Histogram, labels/enum, units, bucket profiles,
   owners và commit/reset/retry semantics. Không tự thêm aliases/ID labels.
2. Duration histograms dùng base names, library sinh _bucket/_sum/_count;
   probability base maintai_ml_failure_probability, chunk histogram base
   maintai_rag_retrieved_chunks. Không nhầm generated sample count với số chunks.
3. Drift state là one-hot0/1/status. Worker report persistence, required report
   publication và checkpoint/heartbeat commit có retry semantics tại OBS-01 §5.
   Missing/stale/error không trở thành0/STABLE. Drift không đổi risk/retrain.
4. ACK, prediction commit và alert API-readable visibility là phép đo khác nhau.
   Timestamp từ đúng accepted source event; Alert Signal1.0 thiếu accepted time
   thì phải resolve qua owner interface hoặc migrate DTO được review. Chưa có
   source thì skip observation+coverage signal, không bịa latency.
5. [MLFLOW_INTEGRATION_GUIDE.md](MLFLOW_INTEGRATION_GUIDE.md) chốt logical training/
   candidate identities, real run/registry IDs, required params/metrics/evidence,
   immutable artifact refs, SDK usage và release mapping. MLflow tags/aliases
   không phải unique key hoặc immutable content guarantee.
6. Trước create run/model version phải durable intent dưới một coordinated owner.
   Unknown create result không blanket retry/get-or-create; read reconciliation,
   0/multiple matches => unresolved/triage. Lease expiry không fence in-flight
   MLflow request. Chưa đủ evidence/identity/hash thì không dispatch release.
7. Model-result-v1 platform schema và app target provenance schemas là artifacts
   riêng; không thêm registry/training keys vào closed HTTP/Kafka/result payload
   nếu chưa freeze/version provider-consumer migration. Numeric registry version
   khác logical model_version; nối qua verified metadata/release.
8. App owners tạo metrics/training data; GitOps sở hữu scrape/exporters/rules/
   dashboard/MLflow Server deployment; infrastructure sở hữu cloud resources.
   Không tạo thêm React UI Grafana/MLflow, training DB thứ6 hoặc query xuyên owner.

Guide §49, [readiness](implementation-readiness.md) và hai tài liệu mới ghi tests,
bàn giao và gaps. Chỉ hoàn tất khi có source, locked dependencies, fixtures,
network/recovery evidence và deployment checks tương ứng; lượt docs không chạy
các actions đó hoặc tự phê duyệt release/model.

# 42. PROC-ORDER-01 — Concurrency và ordering của Processing/Outbox

**Decision1.0.0,2026-10-23.**
[PROCESSING_CONCURRENCY_AND_ORDERING.md](PROCESSING_CONCURRENCY_AND_ORDERING.md)
là đặc tả chi tiết được ủy quyền cho điều khoản này. Target system vẫn1.4.0;
decision bổ sung behavior, không tự thay closed DTO, frozen SQL hoặc SDK version.

1. Processing đọc `sensor.readings` bằng bounded keyed scheduler: cùng
   `machine_id` tối đa một active task trong assignment hợp lệ; khác máy được
   xử lý đồng thời kể cả cùng partition. Nhả key sau durable terminal outcome/
   duplicate verification, không sau riêng ML response. Sensor scheduler là
   module khác; không tạo scheduler service/deployment.
2. Per-machine FIFO giữ Kafka arrival order; source sequence phải tăng bền vững
   xuyên epoch/restart. Không sort `(epoch,sequence)` khi epoch là opaque label.
   Identity duplicate dùng outcome cũ; conflict/first-seen sequence đi lùi phải
   fail closed/reconcile; sequence gap hợp lệ không chờ vô hạn.
3. Kafka offset chỉ tiến qua safe completed prefix của **từng partition** sau
   transaction inbox/history/risk/outbox. Không commit offset cao chỉ vì future
   đó xong; fetched/queued/completed-behind-hole đều thuộc ledger có giới hạn.
   No auto commit/store; revoke/lost không cho stale generation commit.
4. Stale handler không được mutate owner DB sau chuyển assignment. RAM generation
   không đủ: cần durable transaction fence cho cursor/outcome/risk/outbox và
   unique identity. Remote inference có thể bị gọi lại; không hứa exactly-once ML.
5. Outbox dispatch theo durable `(topic,machine_id,dispatch_seq)`; chỉ dispatch
   head sau predecessor đã broker-ACK và mark bền vững. Head đang retry/leased
   chặn successor cùng key/topic; máy khác tiếp tục. Row-only leases/SKIP LOCKED
   chưa đáp ứng. Permanent failure giữ blocked state, không silent skip.
6. Ordered dispatch không hứa physical Kafka deliveries luôn tăng sequence:
   late duplicate/stale publish vẫn cần inbox/CAS. Không có total order giữa
   results/signals/telemetry; Alert không bị bỏ chỉ vì latest projection mới hơn.
7. Business giữ ba handlers/groups, projections độc lập và alert lifecycle;
   INVALID/UNAVAILABLE giữ historical valid prediction. SSE post-commit, REST
   resync; UI coalescing không bỏ history/outcomes/alerts. Storage giữ runner
   hiện hành; không bật keyed mode đồng loạt cho SDK consumers.
8. Queue/bytes/active-key/uncommitted budgets, ML/DB concurrency, poll/deadline/
   drain và fencing phải chốt trước enable. Kafka partitions giới hạn group
   members, không trực tiếp giới hạn số keyed tasks trong một member.
9. SQL order/cursor/fence cần **migration mới**, compatibility/cutover review;
   frozen Kafka bundle đổi SQL phải có bundle/lock mới. Không chạy legacy relay
   hoặc Worker bỏ qua fence cùng writable stream khi target được bật.
10. Catalog metrics bổ sung tại OBS-01 §11; acceptance gồm offset hole, hot key,
    queue saturation, late completion/rebalance, hai relay cùng key, ACK unknown
    và load/recovery. Sơ đồ/render/checksum không thay runtime evidence.

Guide §53, Plan §K, implementation report và readiness §19 ghi SDK0.2 source/tests
cùng các việc application/prod còn pending.
Worker/Relay/Read API vẫn ba Processing runtimes; không thêm topic/DB/UI cho
quyết định này. Rollout/rollback phải giữ inbox/outbox/history và release lineage.

# 43. Giả định ban đầu100 máy và scale-ready — CAPACITY-BASE100-01

**Quyết định23/10/2026:** giả định vận hành ban đầu100 máy,1 reading/s/máy,
tổng100 readings/s.100 là baseline phân tích/nghiệm thu, không phải giới hạn cứng.
MaintAI có **kiến trúc scale-ready**: tăng capacity từng boundary khiX>100, giữ
contracts, per-machine ordering, durability và quyền truy cập.

VớiX máy cùng rate r, R=X×r; nếu khác rate thìR=Σr_i. Tính Worker replicas/concurrency
bằng mean task time, utilization planning và downstream budgets, rồi đo end-to-end
alert UI p95 theo mục tiêu≤2s của đề (không gồm LLM brief). Không dùng HTTP202,
backend DB-visible metric hay phép cộng p95 stage để thay bằng chứng browser.

Tăng replicas/concurrency phải cân ML/DB/Storage/Relay/Business/SSE và catch-up;
không auto tách service hoặc tăng partition theo từng máy.12 partition giữ nguyên
trước reviewed remapping/order cutover. Các profile scale là cấu hình phân tích,
không thay schema,topic,SQL migrations,SDK defaults hoặc actual deployments.

Chi tiết, phép tính và quy trìnhX>100: [Design basis revision1.1](design-basis/README.md),
[Capacity §11](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
Smoke5 máy vẫn dùng phát triển nhỏ; source default5 không tự đổi trong lượt docs.

# 44. M6-HISTORY-01 — Ba Deployment, Read API sở hữu snapshot/export

**Quyết định29/10/2026, revision1.0.0:**
[M6-HISTORY-01](M6_HISTORY_ARCHITECTURE_DECISION.md) là đặc tả chuẩn M6,
[M6-RAW-01](M6_RAW_ARCHIVE_HANDOFF.md) là contract Storage → History. Mục này ưu tiên
SELECT-only toàn runtime, Worker maintenance, accepted-time history pagination và
processed archive/retention của các chương kế thừa khi mâu thuẫn.

1. Giữ Worker/Read API/Relay; không processing-maintenance. Worker chỉ realtime;
   Read API chạy bounded durable snapshot/export jobs trong cùng deployment.
2. Outcome history sort processed_at/result_id, processed_at map completed_at;
   raw sort received_at/Kafka locator. Latest vẫn source sequence, không timestamp.
3. Pool original processing_ro vẫn read-only; thêm pool metadata quyền hẹp, không
   ghi source/outcomes/risk/inbox/outbox. Owner migrations/grants là prerequisite.
4. M6 không processed S3 archive, không bật source-data cleanup. Cleaner chỉ dry-run.
5. Raw reader dùng immutable catalog generation và coverage từng partition; Kafka
   ACK/raw commit không có nghĩa catalog đã complete. Không bucket scan mỗi query.
6. CSV nhỏ có preflight streaming, CSV/Parquet lớn dùng202 job và retry/lease/fencing.
   Business facade kiểm scope; download kiểm quyền lại; UI vẫn xem history trực tiếp.
7. Local DB-only demo khác full M6 dùng MinIO; AWS S3 không bắt buộc. Không giả raw
   history empty/COMPLETE khi provider chưa sẵn sàng. Không tự thay wire schemas,
   migrations/SQL SDK, implementation status hoặc platform config bởi quyết định docs.

# 45. RISK-POLICY-01 — Source policy thuộc app, cấp qua GitOps

Quyết định30/10/2026: app owner Processing quản lý policy JSON, schema1.0.0 và
catalog ở `processing/config/risk-policies/`. GitOps giữ selector app Git SHA +
policy path/version/raw SHA/canonical SHA; lấy đúng committed source để tạo
ConfigMap read-only tại `/local/config/risk-policy.json`. Infrastructure không
cấp một bản policy chỉnh tay trong private fixtures. Worker vẫn dùng
`PROCESSING_RISK_POLICY_FILE`, đọc lúc khởi động và ghi canonical policy hash
vào outcome; không đổi Kafka wire hoặc RiskPolicy DTO.

Policy `demo-risk-v1` dùng ngưỡng tham khảo §9.2, manifest ghi demo/chưa calibration;
không bảo đảm scenario Critical tạo alert. ConfigMap content hash thay Pod
reference khi đổi policy. Model identity và receipts vẫn là gate riêng. Flow
local được triển khai; production cutover phải review riêng, không sửa schema
production đã freeze. Xem [quyết định đầy đủ](RISK_POLICY_OWNERSHIP.md).
