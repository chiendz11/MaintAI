# Phụ lục hoàn thiện HTTP API contract

Ngày: 2026-10-15. API description version: `1.1.0`; OpenAPI format: `3.1.1`.

Phụ lục cụ thể hóa những phần chưa đủ chi tiết trong
[System Contracts v1.1 DRIFT](MaintAI_System_Contracts_v1_1_DRIFT.md), không thay
đổi nội dung file gốc. Các mục dưới đây là quyết định bổ sung cho đặc tả mục tiêu,
không phải bằng chứng backend/ML/RAG đã được implement hoặc deploy.

Baseline vẫn là chuẩn cho invariants, tên field đã có và giao tiếp giữa các
component. OpenAPI ghi `x-maintai-source` để truy về baseline hoặc quyết định
dưới đây. Nếu implementation khác contract, cần sửa implementation hoặc review
thay đổi contract; không sửa schema chỉ để làm CI xanh.

## STATE-01 — Phân biệt HTTP, nghiệp vụ và giao diện

| Tình huống | HTTP/body | UI xử lý |
| --- | --- | --- |
| Đọc dữ liệu thành công | `200` + DTO | Render state backend trả |
| Danh sách không có kết quả | `200`, `items=[]` | EMPTY, không phải lỗi server |
| Sensor hợp lệ, ML hoàn tất | `200`, VALID/COMPLETED | Hiển thị prediction và machine state |
| Sensor invalid, có IDs hợp lệ để ghi nhận | `422`, SensorInvalid | INVALID + validation issues; prediction/alert null |
| Payload không có IDs hợp lệ để ghi nhận | `422`, error VALIDATION_FAILED | Lỗi request; không bịa IDs hoặc tạo prediction |
| JSON không parse được | `400`, INVALID_REQUEST | Sửa request, không retry tự động |
| ML unavailable/timeout sau retry | `503/504`, SensorDependencyFailure | UNKNOWN, prediction/alert null; không hiển thị NORMAL |
| Đọc alert khi RAG đang lỗi | `200`, copilot UNAVAILABLE | Alert vẫn hiển thị và xử lý được |
| RAG không đủ bằng chứng | `200`, INSUFFICIENT_EVIDENCE | Hiển thị manual review, không có actionable checklist |
| RAG citation validation thất bại | `200`, VALIDATION_FAILED | Không hiển thị advice chưa được grounded |
| Đọc report drift có overall_status=ERROR | `200` + report đã lưu | Hiển thị lỗi monitoring; không đổi machine state |
| Resource không tồn tại | `404` + error envelope | Hiển thị not found; khác danh sách rỗng |
| Chuyển alert state sai | `409`, INVALID_ALERT_TRANSITION | Refresh alert, cập nhật actions được phép |
| DB/service transient failure | `503`, DEPENDENCY_UNAVAILABLE | Lỗi dependency rõ ràng; chỉ retry có giới hạn |

Machine `state` luôn là enum nghiệp vụ, không null. `latest_risk_score` và
`latest_prediction` của event hiện tại là null khi INVALID/UNKNOWN; không lấy
prediction cũ làm kết luận cho event mới. Lịch sử vẫn giữ prediction cũ. Máy đã
khai báo nhưng chưa có event dùng UNKNOWN, `data_status=null`, `last_seen_at=null`.
Null ở đây biểu thị dữ liệu chưa có, không thay thế trạng thái nghiệp vụ.

INVALID/UNKNOWN không tự xóa các alerts đang active từ trước. `LOADING`, `EMPTY`
và `ERROR` của giao diện là trạng thái request/render, không thêm vào MachineState.

Sensor `machine_id` phải được provision/seed trước; không tự tạo machine khi
ingest nhầm ID. Unknown machine trả MACHINE_NOT_FOUND. Đây là chi tiết bổ sung
cho mục 5. Cùng event_id và canonical payload trả outcome đã lưu mà không tạo
prediction/alert thứ hai; payload khác trả EVENT_CONFLICT. Ingestion không đợi RAG.

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

## MON-02 — Đọc persisted drift reports

Endpoints bổ sung: `GET /api/v1/drift-reports` và `GET /api/v1/drift-reports/{report_id}`.
List mặc định model đang deploy, cho phép model_version/overall_status filter,
limit mặc định 20, tối đa 100; order computed_at DESC, report_id DESC. Phân trang
keyset bằng before/ before_report_id, response next_before/next_before_report_id.
Hai cursor cùng null khi hết trang; before_report_id thiếu before trả 422.

Đọc nguyên report đã lưu, không chạy detector lại, không sửa computed_at hoặc
policy snapshot. Report không tồn tại trả 404 DRIFT_REPORT_NOT_FOUND. Report
ERROR/INSUFFICIENT_DATA tồn tại vẫn trả 200. Reports hiện là global rolling
windows; API không thêm machine_id filter như thể có drift report cho từng máy.

## AUTH-01 — Profile xác thực tùy chọn trong baseline MVP

Baseline 22 chưa chốt auth prototype/issuer/login flow. OpenAPI mô tả hai profile
qua `security: [<scheme>, {}]`: auth-enabled dùng bearerAuth cho người dùng hoặc
X-Service-API-Key cho sensor/backend service; auth-disabled chỉ dành cho prototype.
Không hiểu nhánh anonymous trong tài liệu là quyền bỏ xác thực ở deployment đã bật auth.

Khi bật auth: read dùng VIEWER/TECHNICIAN/ADMIN; mutate alert/feedback/brief dùng
TECHNICIAN/ADMIN. SENSOR_SERVICE/BACKEND_SERVICE là service identities, không phải
user roles mới. `x-maintai-roles` ghi quyền cho từng operation. Thiếu/sai credential
trả 401 UNAUTHORIZED, thiếu quyền trả 403 FORBIDDEN; không retry tự động các lỗi này.
Health/metrics là platform endpoints, không phải frontend business API.

## HTTP-01, HEALTH-01 và SSE-01 — Quy ước transport

- Payload lỗi business/system dùng error envelope baseline 4. Error.code được
  ràng buộc theo response; không tự dùng error code của service khác cho cùng status.
  Các code UNAUTHORIZED, FORBIDDEN, DRIFT_REPORT_NOT_FOUND và
  DEPENDENCY_UNAVAILABLE là bổ sung để cụ thể hóa auth/resource/dependency failures.
- Sensor 422 có hai dạng đã nêu ở STATE-01; health readiness cũng dùng schema
  riêng thay vì error envelope vì đó là platform observation.
- X-Request-ID/X-Correlation-ID đi trong response; edge tạo IDs khi incoming thiếu,
  backend truyền correlation ID xuyên service và request ID cho từng hop. Body
  error giữ trace IDs tùy chọn để tương thích các ví dụ rút gọn trong baseline.
- `Retry-After` trên 503/504 là tùy chọn, dùng số giây trong profile này. Chỉ retry
  có giới hạn khi retryable=true và operation có semantics cho phép; không retry
  400/401/403/409/422 như lỗi transient. ML client giữ timeout 2 giây, tối đa 1 retry
  cho connection/transient failure theo baseline 7.5.
- Liveness 200 UP phản ánh process. Readiness 200 UP khi các checks bắt buộc đạt;
  503 DEGRADED/DOWN khi chưa sẵn sàng. Backend readiness không phụ thuộc RAG;
  baseline brief/alert independence vẫn giữ nguyên.
- SSE tùy chọn có năm loại events trong baseline 16. Các payload bổ sung được
  mô tả ở sse-event.schema.json. Last-Event-ID chỉ là reconnect hint; không hứa
  replay không mất event. Sau reconnect, UI fetch lại REST. Lỗi sau khi stream
  đã gửi headers không thể đổi thành HTTP error response mới.

## Giới hạn của bộ tài liệu

Không thêm HTTP API cho drift worker, không thêm login/token issuance, admin
policy mutation hoặc auto-retraining endpoint. Các spec là đầu vào để implement
và viết consumer/provider tests. Việc schemas/examples pass CI không chứng minh
service thật đã thực thi đúng các invariants này.
