# HTTP API baseline1.3.2 — migration đến target v1.4

Ngày cập nhật: 2026-10-20. **Machine API description version: `1.3.2`**; OpenAPI
format `3.1.1`. `contracts/v1.3/` là baseline tự chứa còn được CI kiểm tra.
Target docs mới là1.4.0; quarantine/provenance và compatibility migration chưa có
trong bundle này. Các rows phía dưới mô tả baseline1.3.2, không tự override v1.4.

Phụ lục cụ thể hóa những phần chưa đủ chi tiết trong
[System Contracts v1.4](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md)
§34–39 ưu tiên khi migrate; xem [readiness](implementation-readiness.md) về field,
version, history cursor và artifact còn thiếu. Các mục dưới đây giữ chi tiết HTTP kế thừa,
không phải bằng chứng backend/ML/RAG đã được implement hoặc deploy.

Baseline ghi lại tên field/giao tiếp đang có để lập compatibility migration.
OpenAPI ghi `x-maintai-source` để truy về baseline hoặc quyết định
dưới đây. Nếu implementation khác contract, cần sửa implementation hoặc review
thay đổi contract; không sửa schema chỉ để làm CI xanh.

## STATE-01 — Tách HTTP ingress ACK khỏi kết quả nghiệp vụ (BREAKING v1.2)

| Tình huống | HTTP/body tại edge | UI/consumer xử lý |
|---|---|---|
| Sensor envelope hợp lệ + Kafka ACK | **`202 Accepted`**, `ingestion_status=ACCEPTED`, `processing_status=PENDING`, `status_url` | Không hiển thị prediction như đã xử lý; tra cứu trạng thái hoặc nhận SSE sau này |
| Feature type/range invalid nhưng envelope đúng | `202` | Processing ghi `INVALID`, `validation_errors`, không gọi ML; hiển thị khi query status/history |
| Missing `event_id`/`machine_id`, timestamp sai cú pháp, `features` không phải object | `422 VALIDATION_FAILED` | Client sửa payload, không retry mù |
| JSON không parse được | `400 INVALID_REQUEST` | Client sửa payload |
| Kafka unavailable hoặc không xác nhận publish | `503 KAFKA_UNAVAILABLE` / `504 DEPENDENCY_UNAVAILABLE` | Simulator retry có backoff với cùng event_id; không coi event đã nhận |
| Rate limited | `429 RATE_LIMITED` | Retry có giới hạn theo Retry-After |
| Event đã accept nhưng worker chưa materialize status | `GET /sensor-events/{id}/status` tạm `404 EVENT_NOT_MATERIALIZED` | Retry/poll có giới hạn; không suy luận kết quả cuối |
| Processing VALID/COMPLETED | Status GET `200` + prediction/risk | Render latest event state |
| Processing INVALID | Status GET `200`, `processing_status=INVALID` | Prediction/alert null, giữ details |
| ML timeout exhausted | Status GET `200` với `FAILED/UNKNOWN` | Không render NORMAL |
| Existing fleet/alerts empty | `200`, `items=[]` | EMPTY |
| Machine/alert detail không tồn tại | `404` + resource error | Not found |
| Alert action invalid transition | `409 INVALID_ALERT_TRANSITION` | Refresh state |
| RAG unavailable | Alert GET `200`, copilot UNAVAILABLE | Alert lifecycle vẫn hoạt động |
| Drift stored ERROR | Report GET `200` | Monitoring cảnh báo lỗi, không đổi machine state |

Machine `state` vẫn giữ `NORMAL/WATCH/HIGH/CRITICAL/INVALID/UNKNOWN`. `PENDING/PROCESSING/RETRYING/COMPLETED/FAILED` thuộc **event processing status**, không thêm vào `MachineOperationalState`. INVALID/UNKNOWN không tự đóng cảnh báo đang active từ trước. `LOADING/EMPTY/ERROR` chỉ là UI render states.

**Breaking change rõ ràng:** mô tả v1.1 "sensor hợp lệ → HTTP 200 COMPLETED" và "sensor invalid features → HTTP 422" không còn áp dụng cho entrypoint Kafka-backed v1.2. Không thể vừa trả 202 ngay sau ACK vừa biết kết quả ML như luồng synchronous cũ.

## PAGE-01 — Phân trang fleet và alerts

- `limit`: mặc định 50, từ 1 đến 200; `offset`: mặc định 0, không âm.
- Fleet: `machine_id ASC`. Alerts: `created_at DESC, alert_id DESC`.
- `total` đếm các bản ghi khớp filter trước limit/offset. `items=[]` có thể xảy ra
  với offset vượt trang cuối dù total vẫn lớn hơn 0.
- Filter machine_id không tồn tại trên collection alerts trả trang rỗng. Endpoint
  đọc trực tiếp một machine/alert không tồn tại trả 404 tương ứng.

## PAGE-02 — Sensor history và keyset cursor

`GET /api/v1/machines/{machine_id}/sensor-readings` trả `items`, `next_before`,
`next_before_event_id`; mặc định 100, tối đa 500 bản ghi. Order:
`received_at DESC, event_id DESC`. Cặp cursor trỏ tới bản ghi cuối trang; trang
kế dùng phép so sánh tuple `< (before, before_event_id)`.

Hai cursor cùng null khi hết trang. Client nên truyền cả hai để không mất bản
ghi có timestamp trùng nhau. `before` đứng một mình vẫn hợp lệ theo baseline và
có nghĩa timestamp `< before`; `before_event_id` thiếu `before` trả 422.

Record VALID có features đúng allowlist và errors rỗng. Record INVALID giữ raw
features cùng validation issues để hiển thị lỗi; raw features có thể sai kiểu
hoặc thiếu field, tuyệt đối không chuyển lại cho inference như dữ liệu VALID.

## ACTION-01 — Alert lifecycle

| Action | Source statuses | Target |
| --- | --- | --- |
| `ack` | OPEN | ACKNOWLEDGED |
| `start` | ACKNOWLEDGED, ESCALATED | IN_PROGRESS |
| `escalate` | OPEN, ACKNOWLEDGED, IN_PROGRESS | ESCALATED |
| `resolve` | ACKNOWLEDGED, IN_PROGRESS, ESCALATED | RESOLVED |
| `false-alert` | OPEN, ACKNOWLEDGED, IN_PROGRESS | FALSE_ALERT |

Giữ nguyên sơ đồ baseline 10.3. RESOLVED/FALSE_ALERT là terminal. Lặp lại một
action khi source state không còn hợp lệ cũng trả 409, không âm thầm báo success.
Mọi action trả 200 với alert_id, previous_status, status, updated_at sau khi
transaction cập nhật trạng thái và audit history đã commit.

`start`, `ack`, `escalate` nhận actor_id và note tùy chọn; `resolve` thêm resolution
bắt buộc; `false-alert` thêm reason bắt buộc. Auth-enabled mode phải kiểm tra
actor_id khớp principal được xác thực; mismatch trả 403. Body actor_id không cấp quyền.

## ACTION-02 và AUDIT-01 — Feedback và timeline

Feedback thành công trả `201` với DTO ở baseline 12.3. Đây là append-only feedback
cho alert hiện hữu, không tự chuyển lifecycle hoặc trigger training. Không coi
POST feedback là idempotent để retry mù khi không biết outcome đã commit chưa.

Alert detail history order `timestamp ASC, alert_event_id ASC`. Entry có event_type,
from_status, to_status, actor_id, note, timestamp. Entry CREATED có from_status và
actor_id null nếu do hệ thống tạo; thao tác technician phải truy được actor.

## BRIEF-01 — Backend facade đọc brief đầy đủ

Endpoint bổ sung: `GET /api/v1/alerts/{alert_id}/brief`.

Response 200 có `copilot_status`, `message`, `brief`:

- AVAILABLE: brief đầy đủ theo maintenance-brief schema. Đọc grounded_status để
  phân biệt GROUNDED, INSUFFICIENT_EVIDENCE và VALIDATION_FAILED.
- NOT_REQUESTED/PENDING/UNAVAILABLE: brief null, trạng thái và message rõ ràng.
- Alert không tồn tại: 404 ALERT_NOT_FOUND. Có alert nhưng chưa có brief là 200
  NOT_REQUESTED/PENDING, không phải 404.

Endpoint đọc cache/persistence, không gọi RAG đồng bộ. Alert detail giữ tên field
`copilot.status` theo baseline 15.5; endpoint facade dùng `copilot_status` theo
mẫu frontend ở baseline 13.5. Hai field dùng cùng CopilotStatus semantics.

## BRIEF-02 — Technician yêu cầu generation

Endpoint bổ sung: `POST /api/v1/alerts/{alert_id}/brief`, body chứa intent.
Chỉ trả `202 PENDING` sau khi yêu cầu generation đã được lưu bền vững. Backend
thực hiện công việc ngoài ingestion path; 202 không đảm bảo generation sẽ thành công.
Request cùng alert/intent khi còn pending dùng lại pending work. Khi hoàn tất,
GET facade trả AVAILABLE hoặc UNAVAILABLE; UI có thể polling hoặc nhận brief.ready.

Phụ lục không lựa chọn queue/framework triển khai. Backend vẫn gọi RAG qua
`POST /api/v1/briefs`; frontend không gọi endpoint nội bộ này.

## RAG-01 — Grounding và lỗi

GROUNDED cần checklist/citations không rỗng, generator và generated_at. Mỗi step
phải tham chiếu citation tồn tại; citation phải trỏ tới chunk thực sự được retriever
trả về. JSON Schema kiểm tra cấu trúc; validator contract kiểm tra liên kết IDs.

INSUFFICIENT_EVIDENCE và VALIDATION_FAILED là 200 business results với checklist
và citations rỗng. Provider/runtime unavailable trả 503 RAG_UNAVAILABLE; backend
chuyển thành CopilotStatus UNAVAILABLE, không thay severity/lifecycle của alert.

## ML-01 — Biên model threshold

PredictionResponse dùng FAILURE khi failure_probability >= model_threshold,
ngược lại NORMAL; equality thuộc phía FAILURE. Đây là biên cụ thể hóa mục 7.4.
Model threshold khác risk/severity thresholds của backend. Frontend render
prediction/state backend trả, không tự áp model threshold để quyết định severity.

## MON-01 — Monitoring summary

Endpoint bổ sung: `GET /api/v1/monitoring/summary`. Response có status, observed_at,
services backend/ml/rag, và drift summary. Đọc được observation DEGRADED vẫn là
HTTP 200; không đồng nghĩa mọi dependency khỏe.

`drift.worker_status` là FRESH/STALE/NOT_AVAILABLE, tách khỏi DriftStatus.
Tính freshness từ checkpoint last_success_at với stale_after_seconds cấu hình;
900 giây trong example chỉ là minh họa, không phải lịch chạy production đã chốt.
Report có thể được tái sử dụng nên computed_at không phải worker heartbeat.
`latest_report.window_end_at` cho biết độ mới của dữ liệu, tách khỏi worker freshness.

Backend resolve deployed model và worker được cấu hình trước khi tổng hợp; không
trộn checkpoint/model khác. Không có checkpoint thì last_success_at null và
worker_status NOT_AVAILABLE; vẫn có thể có report ERROR từ lượt chạy thất bại.

## MON-02 — Đọc persisted drift reports (amendment1.3.2)

GET `/api/v1/drift-reports` đọc Business catalog metadata từ committed S3 manifests;
GET `/{report_id}` tải exact version + checksum. Không chạy detector hoặc SQL drift_db.
List filter model_version/overall_status, limit default20/max100, keyset opaque
cursor bind filters + publication snapshot; order computed_at DESC, report_id DESC.
Response có catalog_as_of/stale. Catalog stale không giả realtime; detail source
unavailable503; no report404; persisted ERROR/INSUFFICIENT_DATA vẫn200.
Không thêm machine_id filter cho global rolling reports. RECOVERY-01 §4 chốt bridge.

## AUTH-01 — Auth bắt buộc ở shared deployment (amendment1.3.2)

External OIDC cấp token; Business/Sensor kiểm tra issuer/audience/expiry/scope và
machine authorization. Shared/demo deployment không cho anonymous; local-only
profile phải explicit. Roles/scopes đầy đủ ở architecture decisions §5. Main UI
và Simulator UI dùng public clients riêng; không nhúng service credentials.
Missing/invalid token401; permission/machine scope thiếu403. SSE dùng authenticated
fetch streaming, không token query string. Health/metrics là platform observations.

## HTTP-01, HEALTH-01 và SSE-01 — Quy ước transport

- Payload lỗi business/system dùng error envelope baseline 4. Error.code được
  ràng buộc theo response; không tự dùng error code của service khác cho cùng status.
  Các code UNAUTHORIZED, FORBIDDEN, DRIFT_REPORT_NOT_FOUND và
  DEPENDENCY_UNAVAILABLE là bổ sung để cụ thể hóa auth/resource/dependency failures.
- Sensor 422 chỉ phản ánh envelope-level validation trước Kafka; health readiness cũng dùng schema
  riêng thay vì error envelope vì đó là platform observation.
- X-Request-ID/X-Correlation-ID đi trong response; edge tạo IDs khi incoming thiếu,
  backend truyền correlation ID xuyên service và request ID cho từng hop. Body
  error giữ trace IDs tùy chọn để tương thích các ví dụ rút gọn trong baseline.
- `Retry-After` trên 503/504 là tùy chọn, dùng số giây trong profile này. Chỉ retry
  có giới hạn khi retryable=true và operation có semantics cho phép; không retry
  400/401/403/409/422 như lỗi transient. Processing ML client giữ timeout 2 giây, tối đa 1 retry
  cho connection/transient failure theo baseline 7.5.
- Liveness 200 UP phản ánh process. Readiness 200 UP khi các checks bắt buộc đạt; Ingestion cần Kafka connectivity, Processing Worker cần Kafka + processing_db, Storage Consumer cần Kafka + S3, Business Consumer cần Kafka + business_db;
  503 DEGRADED/DOWN khi chưa sẵn sàng. Backend readiness không phụ thuộc RAG;
  baseline brief/alert independence vẫn giữ nguyên.
- SSE cho Live Telemetry + Latest Prediction được bổ sung ở REALTIME-API-01: telemetry.received, prediction.completed, processing.status_changed cùng các event alert/brief/service/drift; schema máy đọc cần cập nhật tương ứng. Last-Event-ID chỉ là reconnect hint; không hứa
  replay không mất event. Sau reconnect, UI fetch lại REST. Lỗi sau khi stream
  đã gửi headers không thể đổi thành HTTP error response mới.

## Giới hạn của bộ tài liệu

Không thêm HTTP API cho drift worker, không thêm login/token issuance, admin
policy mutation hoặc auto-retraining endpoint. Các spec là đầu vào để implement
và viết consumer/provider tests. Việc schemas/examples pass CI không chứng minh
service thật đã thực thi đúng các invariants này.


---

## KAFKA-API-01 — Semantics Ingestion HTTP 202 (mới v1.2)

```http
POST /api/v1/sensor-readings
Content-Type: application/json
X-Correlation-ID: cor_001
```

```http
HTTP/1.1 202 Accepted
Location: https://business.example.invalid/api/v1/sensor-events/evt_fixture_1/status
```

```json
{"event_id": "evt_fixture_1", "machine_id": "M-001", "ingestion_status": "ACCEPTED", "processing_status": "PENDING", "server_accepted_at": "2026-10-18T00:00:00Z", "status_url": "https://business.example.invalid/api/v1/sensor-events/evt_fixture_1/status", "correlation_id": "cor_fixture_1"}
```

Ingestion publish `sensor.readings` keyed `machine_id`, chờ broker ACK (acks=all). Chỉ nhận syntax/transport/envelope guard; feature validation tại Processing. Source of truth cho `machine_state` là kết quả xử lý đã persisted, không phải HTTP 202.

## KAFKA-API-02 — Status polling và query consistency

`GET /api/v1/sensor-events/{event_id}/status` trả trạng thái persisted nếu đã materialize trong `processing_db`. `404 EVENT_NOT_MATERIALIZED` trong thời gian Kafka→consumer chưa hoàn tất **không nhất thiết** đồng nghĩa event không tồn tại; client retry giới hạn/backoff. `COMPLETED` / `INVALID` / `FAILED` là trạng thái kết thúc của lượt xử lý theo policy; prediction có thể null với INVALID/FAILED. Business facade muốn trả alert_id phải query business-owned alert, không suy luận alert tạo thành công chỉ từ `alert.signals` sent.

## KAFKA-API-03 — Không dùng chung credentials/database xuyên service

Business API → Processing read API để lấy sensor history/latest prediction; Drift CronJob → Processing runtime-window read API hoặc S3 curated, không đọc SQL `processing_db`; Business owns `machines`, `alerts`, `feedback`, `rag_briefs`; Processing owns `sensor_events`, `predictions`, `risk_decisions`. `POST /api/v1/briefs` vẫn là internal RAG endpoint; `POST /api/v1/alerts/{id}/brief` vẫn accept asynchronous work như trước, tách khỏi sensor-ingest.

## KAFKA-API-04 — Quy trình đồng bộ lại machine-readable specs

Ghi chú migration v1.2: specs hiện hành tự chứa ở `contracts/v1.3/`, bộ kiến trúc cũ đã gỡ. Runtime vẫn cần migrate: change sensor POST response 200/422, thêm accepted/status DTO, `EVENT_NOT_MATERIALIZED`, Kafka envelope và alert signal schemas, fixtures, consumer/provider tests. Giữ invariant các endpoint lifecycle, pagination, RAG grounding, ML threshold, monitoring và SSE trong tài liệu này trừ khi có quyết định khác.


---

## REALTIME-API-01 — Realtime Frontend contract v1.3 (quyết định mới)

**Nguồn:** hai cuộc trao đổi UI/architecture người dùng cung cấp. Frontend chọn **phương án C** thay vì “chỉ update UI khi ML done” hoặc “nhấp nháy Pending trên mỗi event”. Topic đã chốt trong System Contracts §32 là `sensor.readings` (thay tên `sensor.raw` ở 1.2), `inference.results` và `alert.signals`.

### REST bootstrap của Machine Detail

```http
GET /api/v1/machines/{machine_id}/live
```

200 JSON có `machine_id`, `telemetry`, `latest_valid_prediction`, `latest_processing`, `as_of`; chi tiết shape ở System Contracts §32.4. `telemetry` và `latest_valid_prediction` **không cần cùng source_event_id**; prediction không tồn tại → null, không hiển thị `NORMAL`. `last_valid_prediction` của lần trước **không bị xóa** chỉ vì event hiện hành INVALID/FAILED, nhưng phải gắn label historical/stale. Business API quản lý UI-facing projection trong `business_db`, không query trực tiếp SQL của Processing DB.

```http
GET /api/v1/events/stream?machine_id=M-001
Accept: text/event-stream
```

SSE types mới: `telemetry.received`, `prediction.completed`, `processing.status_changed`; tiếp tục có `alert.created`, `alert.updated`, `brief.ready`, `service.degraded` và có thể `drift.updated`. Chi tiết payload và sequence ở System Contracts §32.5. `GET /api/v1/machines` có thể trả summary fields `last_telemetry_at`, `latest_valid_prediction_at`, `prediction_source_event_id`, `prediction_freshness` làm optional additions; không tự gọi ML từ read endpoint.

### Error and freshness semantics

- `POST /api/v1/sensor-readings` vẫn **202 sau Kafka ACK**, không trả prediction; UI không trông chờ kết quả đồng bộ.
- `telemetry.received` là **raw accepted / unvalidated**; nếu status sau đó INVALID, UI trình bày data-quality issue, không lấy raw làm kết luận “ổn định”.
- Chỉ `prediction.completed` của `COMPLETED + VALID` mới thay đổi Latest Valid Prediction; stale/out-of-order results vẫn được lưu lịch sử nhưng không overwrite read model.
- `processing.status_changed` cho INVALID/FAILED không tự động xóa/resolve alert hoặc xóa last valid prediction.
- Chế độ aging minh hoạ cấu hình `<2s FRESH`, `2–5s DELAYED`, `>5s STALE`; dùng source reading accepted time để tính độ cũ của input dự đoán, không dùng `completed_at` một mình.
- Client SSE nhận event out-of-order phải so ordering token theo máy; replay after reconnect dựa REST GET snapshot, không dựa `Last-Event-ID` như bảo đảm durable.
- Mất fan-out notifications có thể khôi phục bằng REST resync, nhưng để realtime trên nhiều replicas phải sử dụng broadcast layer (pub/sub) hoặc topology được chứng minh phát đến mọi SSE Pod, không dùng single shared Kafka consumer group làm broadcast.

### Snapshot sample khi inference chậm

```json
{
  "machine_id": "M-001",
  "telemetry": {"event_id":"R4","sequence_number":4,"server_accepted_at":"2026-10-17T00:00:03.050Z","features_raw":{"rotational_speed_rpm":1500}},
  "latest_valid_prediction": {"source_event_id":"R2","sequence_number":2,"failure_probability":0.12,"prediction":"NORMAL","completed_at":"2026-10-17T00:00:03.000Z"},
  "latest_processing": {"source_event_id":"R4","processing_status":"PENDING"},
  "as_of":"2026-10-17T00:00:03.060Z"
}
```

**UI:** Live Telemetry hiển thị R4; Latest Valid Prediction hiển thị R2 kèm “based on R2 · older than latest telemetry”, trạng thái updating/delayed theo ngưỡng; không gộp hai event làm một kết luận.

### Contract testing

Tạo OpenAPI schema cho live snapshot + SSE event JSON schemas / examples; contract tests phải cover out-of-order results, multiple API replicas, reconnect, auth filtering và data-invalid preservation. Target machine specs đã được bổ sung ở `contracts/v1.3/`; CI kiểm tra trực tiếp bộ này.


---

## OWNERSHIP-API-01 — Không nhập nhằng Ingestion, Processing và Business

| HTTP endpoint/stream | Owner / deployment | Dữ liệu |
|---|---|---|
| `POST /api/v1/sensor-readings` | **Ingestion API** riêng (không thuộc Business Backend) | Envelopes tối thiểu, `202` chỉ khi Kafka ACK |
| `GET /api/v1/sensor-events/{event_id}/status` | Processing read API nội bộ, Business API facade có thể proxy nếu scope UI cần | VALID/INVALID/FAILED/UNKNOWN; eventual consistency |
| `GET /api/v1/machines/{id}/live` | Business API | Latest UI read models trong `business_db`, không raw history |
| `GET /api/v1/events/stream` | Business API | SSE telemetry, prediction, processing, alerts, brief/drift updates |
| Alert lifecycle/feedback/brief endpoints | Business API | Business DB & RAG facade |
| Simulator control endpoints | Sensor Control API <- Simulator UI riêng | Demo-only control state; không qua Business |

Business Consumer đọc các topics `sensor.readings`, `inference.results`, `alert.signals` với ba **handlers** trong một business codebase, chạy khác Deployment Business API; không phải ba inference services. Processing tạo prediction, Business chỉ materialize UI projection. `business_db` không lưu full raw history; Storage Consumer lưu S3, Processing lưu validation/predictions/risk history. PostgreSQL NOTIFY/pubsub sau projector commit để mọi SSE replica nhận thông báo; snapshot REST khôi phục notification loss. SSE không yêu cầu DB raw recording.

## INGEST-ERROR-02 — HTTP rejection không dừng toàn bộ Simulator

- `400` JSON malformed: record rejected local log/quarantine; Simulator tiếp tục record tiếp theo nếu `RUNNING`.
- `422` envelope missing machine/event IDs hoặc `features` không phải object: reject **record**, không retry vô hạn và không publish Kafka.
- `202` basic envelope hợp lệ, nhưng giá trị feature sai type/range: Data Quality Worker đánh INVALID, không gọi ML; raw Kafka event được Storage Consumer archive S3.
- `503/504` Kafka unavailable or ack timeout: Simulator giữ pending event cùng event_id/payload trong durable retry spool, backoff; chỉ coi accepted khi có broker ACK.
- `429`: bounded retry / `Retry-After`; `401/403`: configuration or auth error requiring operator intervention.
- Không tự khẳng định 409 EVENT_CONFLICT synchronous nếu không có ingress durable dedupe ledger; Processing record conflict/validation outcome.

## REALTIME-API-02 — Processing outcome và projection boundaries

`inference.results` (giữ tên topic v1.3) dùng union có `result_kind`:

| `result_kind` | Prediction fields | Effect trên Business UI |
|---|---|---|
| `PREDICTION_COMPLETED` | Required, VALID, COMPLETED | Update latest_valid_prediction theo source ordering; SSE `prediction.completed` |
| `DATA_INVALID` | null | Update latest_processing + validation issues; SSE `processing.status_changed`, không xóa prediction cũ |
| `INFERENCE_UNAVAILABLE` | null | Update latest_processing FAILED/UNKNOWN; SSE `processing.status_changed`, không hiển thị NORMAL giả |

SSE `telemetry.received` phản ánh Kafka ACK accepted (UNVERIFIED) chứ không xác nhận DQ. `sequence_number` của mỗi máy phải tăng đơn điệu bền vững qua Pause/Resume và restart; `stream_epoch` là lineage metadata (không dùng sort); nếu không thể bảo đảm thì ingress phải cấp durable per-machine ordering token; `event_id` opaque và `completed_at` không dùng để sort nguồn sensor. Business API multiple replicas dùng PG LISTEN/NOTIFY / pubsub; UI REST after SSE reconnect.

## SIM-CONTROL-API-01 — Simulator Pause/Resume/Scenario (demo only)

```http
GET  /api/v1/simulator/machines/{machine_id}/control
POST /api/v1/simulator/machines/{machine_id}/pause
POST /api/v1/simulator/machines/{machine_id}/resume
PUT  /api/v1/simulator/machines/{machine_id}/scenario
```

GET example:

```json
{
  "machine_id": "M-001",
  "stream_state": "PAUSED",
  "scenario": "degradation",
  "updated_at": "2026-10-17T05:10:00Z",
  "source": "SIMULATOR_CONTROL"
}
```

Simulator UI gọi Sensor trực tiếp qua management origin; không Business facade. Mutation `pause/resume` return `200` **sau khi Simulator internal API ACK control state**; repeated command same state is idempotent; `PUT scenario` validates whitelist, changes future data only and doesn't auto-resume. Internal timeout/non-ACK must not be treated as successful control; query GET to reconcile. User role/access must explicitly permit demo control; `VIEWER` cannot send controls.

`PAUSED` means no **new** sensor records for that machine, not Kafka backlog canceled; retry queue for previously generated readings remains explicitly controlled. `simulator.control_changed` optional SSE after confirmed state + pubsub fan-out. Pause/Resume does not update Risk State/AlertStatus, and Simulator is not an industrial actuator. UI must distinguish explicit `PAUSED` from inferred `STALE`; as soon as no fresh data, label `Last Telemetry` not Live.


## COMPLETE-1.3.2 — Quyết định ưu tiên cho HTTP/API

[System Contracts §34](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) và các normative
specs ưu tiên mọi baseline cũ. Six provider descriptions:
`contracts/v1.3/openapi/openapi-{ingestion,processing,business,simulator,ml,rag}.yaml`.
ML/RAG provider payload vẫn1.1.0, description version1.3.2. ProcessingResult/history hiện dùng outcome1.4.0 theo OUTCOME-14-ONLY bên dưới.

- HISTORY-01 chốt range/point/raw/chart/export routes, HMAC cursor + snapshot10m,
  limits, full coverage/error410/503 và private download303.
- RECOVERY-01 chốt durable brief jobs và GET `/brief-jobs/{job_id}`. POST brief202
  phải có job_id/status_url sau commit; GET không tự chạy LLM.
- Drift list dùng opaque cursor + catalog_as_of/stale, thay before/before_report_id
  của MON-02; report detail lấy exact S3 version, không chạy detector hoặc SQL drift_db.
- Simulator control routes do Sensor phục vụ, không thuộc Business. Mutation có
  Idempotency-Key + expected_control_version; state ACK chỉ sau persistent write.
- Schema SSE data dùng envelope schema1.0.0 có type/machine_id/data/occurred_at;
  event name khớp type. Thêm brief.updated/projection.resync. REST resync luôn cần.

Document/API revision không tự đổi behavior của executable Sensor/Drift hiện có.
Bộ OpenAPI kiến trúc cũ đã gỡ; runtime migration và DB schemas vẫn phải implement
theo Guide v1.4 §42–47 cùng chi tiết history/recovery kế thừa. Provider ML/RAG1.1.0 và Drift schema1.0.0 vẫn còn hiệu lực.

Ingestion status_url/Location phải là absolute Business API URL được cấu hình;
Sensor gọi Ingestion internal origin không được resolve relative URL về Ingestion.

## UI-01 — Ranh giới HTTP của bốn UI (2026-10-20)

[UI Architecture](UI_ARCHITECTURE.md) và System Contracts §40 chốt ownership;
lượt docs này không sửa OpenAPI/schema hoặc tăng protocol version.

- Business UI chỉ gọi Business REST/SSE; history/RAG/drift do Business facade
  theo scope. Giữ monitoring summary và report list/detail đã có; Grafana không
  làm mất các endpoint hoặc chuyển browser qua DB/Processing API trực tiếp.
- Simulator UI chỉ gọi Sensor Control trên management origin. Baseline Simulator
  có5 operations: GET `/api/v1/simulator/machines`, GET `/{machine_id}/control`,
  POST `/{machine_id}/pause`, POST `/{machine_id}/resume`, PUT `/{machine_id}/scenario`
  dưới cùng collection path. Mutation có Idempotency-Key/expected_control_version;
  ScenarioCommand thêm scenario. Roles SIMULATOR_OPERATOR/ADMIN và machine scope.
- ControlState hiện không có rate/counters/queue/preview/ACK-detail. Chưa có rate
  mutation, batch control, control SSE hoặc toggle RAG. Không tự thêm properties
  vào DTO đóng; cần PR freeze request/response/error/auth/idempotency/persistence
  và schema version trước khi bật chức năng tương ứng trong UI thật.
- Unknown command outcome dùng GET reconcile, không suy ra PAUSED từ timeout;
 409 đọc state mới trước ý định mới, cùng command retry giữ key/body theo semantics
  đã chốt. Business severity/Alert lifecycle không bị simulator command thay đổi.
- Grafana browser gọi Grafana server; report datasource phải owner-approved,
  read-only/bounded nếu dùng SQL, server giữ credentials. Không phát minh Drift
  HTTP deployment; Business report catalog APIs giữ owner/dataflow hiện có.
- MLflow browser gọi MLflow Server theo quyền platform; server giữ DB/artifact
  access. UI registry không là API bỏ qua approved model release PR.
- Public origins/links/capabilities là runtime config riêng của mỗi frontend;
  schema config immutable hiện có cần revision mới nếu thêm keys. Không dùng
  wildcard CORS, token query string hoặc frontend service credentials làm shortcut.

Tách release không thay API compatibility: UI cũ còn mở phải tiếp tục được provider
hỗ trợ hoặc có kế hoạch migration/refresh rõ. CI selection chưa implement; frontend
image/build độc lập không miễn provider/consumer tests khi shared contract thay đổi.


## OBS-01 / MLFLOW-01 — Metrics và tracking không thay HTTP DTO (2026-10-20)

System Contracts §41 chốt [metrics catalog](OBSERVABILITY_IMPLEMENTATION.md) và
[MLflow workflow](MLFLOW_INTEGRATION_GUIDE.md) revision1.0.0. Internal metrics
exposition, bounded labels/buckets và run/artifact metadata không tự thêm fields
vào Business/Simulator/OpenAPI DTO, Kafka events hoặc strict model-result-v1.

Alert Signal1.0 chưa có accepted timestamp. Histogram event-to-alert cần exact
source-event lookup qua owner interface hoặc event/DTO migration được review;
không dùng sensor received_at/latest telemetry hoặc direct processing_db SQL.
Thiếu source thì metrics coverage unavailable, alert vẫn được xử lý bình thường.

Internal Registry receipt có name/numeric version/run/source/payload hash; các
fields đó vào verified serving metadata/release. Canonical Kafka outcome1.4 có
mlflow_run_id nhưng chưa explicit registry name/version; thêm wire fields cần
provider-consumer fixtures/version migration riêng, không sửa JSON cho khớp ví dụ.
HTTP200/202, run FINISHED, registry READY và release deployment là các trạng thái
khác nhau, không thay semantics bằng một monitoring status chung.

## PROC-ORDER-API-01 — Concurrency không đổi ACK hoặc consistency boundary

Quyết định23/10: [PROC-ORDER-01](PROCESSING_CONCURRENCY_AND_ORDERING.md), Contracts §42.
Keyed Processing/ordered relay là internal behavior; chưa đổi OpenAPI/HTTP status,
wire tagged union, SSE event names hoặc closed DTO trong lượt docs.

HTTP202 vẫn chỉ broker ACK. Status/history có thể chưa thấy outcome; frontend
không lấy committed Kafka offset làm trạng thái inference từng máy. Business
trả các projections có source/age riêng và lifecycle Alert thực tế; result và
signal giữa topics có thể đến khác thời điểm. Duplicate/stale update không được
làm lùi latest, INVALID không xóa valid prediction lịch sử. Không so sánh opaque
epoch hoặc dùng latest-telemetry guard để bỏ Alert Signal cũ.

Machine source-order conflict xử lý nội bộ fail closed/reconcile theo decision;
không tự invent HTTP error/SSE kind/ML status nếu chưa review schema. Queue/lag/
blocked-key metrics ở management endpoint, không tự thêm vào public response.

## INGEST-M1-01 — HTTP adapter và operational profile (2026-10-23)

Ingestion API description tăng MINOR lên1.4.0 theo System §25 vì thêm operational endpoints; các API khác/bundle HTTP directory vẫn baseline1.3.2. Không đổi accepted1.3.0/rejected1.4.0 hoặc canonical Kafka bundle1.0.0. Đây là interface implementation M1, không chứng minh đã deploy.

- SDK ACK có schema_version; HTTP adapter chỉ chọn các field IngestAck hiện có, giữ absolute status_url/Location. Kafka wire không đổi.
- SDK error được ánh xạ về runtime Error: message cố định, retryable, request_id/correlation_id trong error; rejection_id ở error.details. Không echo body/token/exception.
- Auth trước body/gateway: role SENSOR_SERVICE, audience ingestion; source subject và machine scope chỉ từ verified claims. Shared claim names/issuer/JWKS/algorithms là cấu hình bắt buộc, không đoán provider. Profile local-fixture explicit chỉ trên loopback; không phải OIDC production.
- Missing/unsupported Content-Type hoặc compressed body415; hỗ trợ application/json với optional charset. Streaming byte limit mặc định262144, body timeout10s→408; không publish partial body. Admission trước auth (nhưng auth vẫn trước đọc/publish body),8 concurrent requests/process→429/Retry-After1, không phải global RPS quota.
- /health/live không phụ thuộc Kafka; /health/ready kiểm tra config, cached Kafka metadata và auth key state; metadata không chứng minh producer ACL/ACK quorum. /metrics chỉ expose platform metrics, hạn chế network ingress.
- 202 sau accepted ACK; authenticated malformed/envelope-invalid400/422 sau rejected ACK. Feature invalid vẫn202;503/504 retry giữ event ID/payload. Request headers invalid là transport400 trước gateway, không quarantine sensor body.
- Shared OIDC/TLS/ACL và Sensor migration202 cần bằng chứng tích hợp riêng; local tests không đủ nghiệm thu deployment.


## OUTCOME-14-ONLY — 2026-10-28

[Quyết định outcome1.4](OUTCOME_V1_4_MIGRATION.md) ưu tiên các đoạn inherited
về union2.0.0. Kafka và entrypoint ProcessingResult/history chỉ dùng schema
1.4.0, discriminator kind, event_id nguồn và risk_score bắt buộc cho COMPLETED.
Wrapper history giữ source_event_id và so với outcome.event_id. SSE result_kind
là field riêng, không tự đổi cùng Kafka. Fleet lấy risk gốc từ projection;
không có probability fallback hoặc hỗ trợ đọc/backfill legacy.


## M6-HISTORY-01 — Versioned history profile

[M6 architecture](M6_HISTORY_ARCHITECTURE_DECISION.md) ưu tiên PAGE-02/history
clock/SELECT-only/export-writer kế thừa. M6 history/export dùng profile header và
schema riêng, OpenAPI Business/Processing description2.0.0. M5 point/status/live/
Alert DTO không đổi; Kafka bundle/runtime.schema.json immutable giữ nguyên.

Processing completed_at (API processed_at) khác raw received_at; profile/cursor
không chuyển latest ordering từ sequence sang time. Nullable time availability
đi cùng explicit partition coverage; no unknown cutoff fabricated. Small CSV200
hoặc durable202 theo preflight, không tự đổi mode sau khi đã gửi headers.

Internal history cần cả service token và Business-signed delegation<=60s;
actor/scope từ token đã kiểm chữ ký, không browser headers. Source ro pool không
được ghi; metadata rw pool và leased runner trong Read API là phạm vi quyền riêng.
Các operation khác giữ nguyên machine semantics trước thay đổi này.
