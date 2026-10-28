# Business Backend — một codebase, hai Deployments

`backend/` là **source ownership**, không phải một deployment ôm ingestion,
validation, inference và risk như v1.1. Target là System Contracts v1.4 §34–42;
runtime inventory/OpenAPI baseline1.3.2 có version riêng.

| Runtime folder | Workload | Trách nhiệm |
| --- | --- | --- |
| [`business-api/`](business-api/Dockerfile) | Deployment `business-api` | REST/SSE, RBAC, actions/feedback/Copilot, Processing history facade, enqueue/poll durable jobs, read drift catalog. |
| [`business-consumer/`](business-consumer/Dockerfile) | Deployment `business-consumer` | Ba handlers telemetry/prediction/alert, latest projections/alert lifecycle và bounded durable job loop. |
| `shared/` | Không phải deployment | Alerts, audit, feedback, ordering, inbox, notifications và persistence của Business. |
| `migrations/` | Không phải deployment | Migrations chỉ thuộc `business_db`, dùng chung cho API/consumer. |

Hai runtime có thể dùng cùng image Business với entrypoint khác. Các handlers là
module trong một consumer runtime, không phải telemetry/prediction microservices.
`business_db` giữ machines/alerts/feedback và latest snapshots, không full raw hoặc
toàn bộ processing history. Consumer commit projection/inbox trước notification
đến API replicas; REST snapshot khôi phục khi mất SSE/NOTIFY.

Không query `processing_db`; gọi Processing read API/S3 theo contract. Không chạy
ML/risk tại Business. API hiện có scoped registry lookup; các callbacks và repository
latest projections nằm trong [Business projections](business-consumer/README.md).
Consumer chạy hai role loops telemetry/prediction; Alert/job loops chưa được triển khai.
Kafka loops và transactional inbox dùng
[messaging SDK](../messaging/README.md); cách tích hợp tại [Kafka guide §6](../docs/KAFKA_DEVELOPMENT_GUIDE.md). Xem
[runtime map](../architecture/README.md) và [migration](../docs/repository-layout.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.

## PROC-ORDER-01 — Business giữ projections/lifecycle độc lập

[Decision §8](../docs/PROCESSING_CONCURRENCY_AND_ORDERING.md) áp keyed execution
ở Processing; Business giữ serial role loops/inbox SDK0.3.0. Registry, projection
storage/callbacks, Consumer, live HTTP và status facade đã có. Fleet/MachineDetail
đọc projections; thông tin Alert cần nguồn đọc cùng Business DB. Alert lifecycle
chưa được triển khai, nên runtime chưa cấu hình nguồn này trả503 cho các máy cần đọc.
Telemetry, latest processing và valid prediction có CAS riêng bằng durable source
sequence xuyên epoch; không sort UUID/offset giữa partition. INVALID giữ valid
snapshot lịch sử. Signal đến muộn vẫn cần dedupe/lifecycle/audit, không bị bỏ vì
latest projection mới hơn. Commit DB rồi fan-out SSE, reconnect REST snapshot;
NORMAL mới không tự đóng Alert, giữa topics không có total order.

## API hiện có và cấu hình status facade

- `GET /api/v1/machines`: Fleet theo machine scope, filter `state`, sort `machine_id`
  tăng dần; `limit` mặc định50/tối đa200, `offset` không âm. `total` được đếm sau
  scope/filter và trước phân trang.
- `GET /api/v1/machines/{machine_id}`: MachineDetail kết hợp registry/projections.
- `GET /api/v1/machines/{machine_id}/live`: ba projections độc lập từ Business DB.
- `GET /api/v1/sensor-events/{event_id}/status`: gọi Processing Read API qua HTTP.

Các machine/event IDs trong URL phải được percent-encode, kể cả slash. Resource
`/live` được nối sau machine ID đã encode; một ID kết thúc bằng `/live` vẫn là
registry lookup khi slash thuộc ID được encode.

Fleet/detail/live cho `VIEWER`, `TECHNICIAN`, `ADMIN`; status cho thêm `SENSOR_SERVICE`.
Mọi route vẫn kiểm machine scope. Status facade dùng hai cấu hình đi cùng nhau:
`BUSINESS_PROCESSING_READ_URL` và `BUSINESS_PROCESSING_TOKEN_FILE`. File chứa access
token service riêng, audience và role `BUSINESS_SERVICE` phù hợp Processing; không
được dùng token browser làm credential dịch vụ. Token được đọc lại mỗi request để
hỗ trợ rotation. Khi chưa cấu hình dependency này, status trả503.

Fleet/Detail dùng transaction read-only `REPEATABLE READ` cho projections và nguồn
Alert. `ProjectionRepository(..., alert_reader=...)` nhận nguồn đọc trong cùng
connection Business; nguồn này phải trả đầy đủ danh sách active alert IDs cho từng
machine được yêu cầu. Chưa cấu hình hoặc trả thiếu dữ liệu:503, không thay bằng
`active_alert_count=0` hay `active_alerts=[]`. Trang không có items vẫn trả200 và
`total` đúng. Runtime hiện chưa nối nguồn Alert; tests dùng nguồn fixture riêng.

Outcome1.4.0 giữ nguyên risk đã ghi; SDK từ chối outcome2.0 trước callback.
Không backfill legacy hoặc tính lại risk trong GET. Fleet thiếu risk đã lưu là
lỗi dữ liệu và vẫn trả503; xem [quyết định cutover](../docs/OUTCOME_V1_4_MIGRATION.md). INVALID/FAILED/PENDING lần lượt hiển thị
INVALID/UNKNOWN/UNKNOWN; Detail không trả prediction lịch sử như prediction hiện tại.
`/live` giữ snapshot lịch sử độc lập. Khi Detail có `data_status=VALID`, telemetry
chưa xác minh hoặc INVALID không được gắn nhãn VALID; `latest_sensor` trả null trong
DTO này và raw vẫn nằm ở `/live`.

Facade giới hạn deadline3s, HTTP timeout2s và response64KiB, từ chối redirects và
response không hợp DTO. `404 EVENT_NOT_MATERIALIZED` là eventual consistency và
cho phép poll có giới hạn; timeout trả504. `X-Correlation-ID` xuyên hop, request ID
riêng từng hop. Business không có SQL client/credentials của Processing.

`/health/ready` kiểm Business tables và OIDC keys; `/metrics` xuất HTTP counters/
histogram theo OBS-01 và dependency errors cho Business DB/OIDC. Các lỗi Processing
HTTP thể hiện ở status503/504 của route status; catalog chưa có nhãn dependency
riêng cho Processing HTTP. Health/scrape không tính vào REST latency.

Mỗi runtime có command riêng: `maintai-business-api`, `maintai-business-consumer`.
Migrations/provisioning vẫn là owner operations riêng. Xem [Consumer](business-consumer/README.md)
và [Processing status API](../processing/read-api/README.md) để cấu hình/checks.

## SSE và PostgreSQL fan-out

`GET /api/v1/events/stream` nhận optional machine_id, yêu cầu VIEWER/TECHNICIAN/ADMIN
và machine scope. Browser dùng authenticated fetch; không đưa token vào query.
Stream đóng khi JWT hết hạn; lỗi auth/scope/listener trước handshake dùng Error DTO.

Mỗi API process có một LISTEN connection vào business_db. Consumer gọi pg_notify
trong transaction inbox/projections, nên notification chỉ đến sau commit. Payload
chỉ là version và hash machine ID; API đọc lại live snapshot và fan-out tới clients
được cấp quyền. Không dùng Kafka group làm broadcast hoặc NOTIFY làm history.

| Biến | Mặc định |
| --- | --- |
| BUSINESS_NOTIFY_CHANNEL | maintai_business_live_v1; API và Consumer phải khớp |
| BUSINESS_SSE_MAX_CONNECTIONS | 128 mỗi API process |
| BUSINESS_SSE_CONNECTION_BYTES | 2097152 pending bytes mỗi connection |
| BUSINESS_SSE_TOTAL_BYTES | 8388608 pending bytes mỗi API process |

Coalesce tối đa1update/giây/máy; heartbeat15s. Slow client/vượt budget hoặc listener
mất kết nối sẽ đóng stream để client reconnect và REST resync. Không durable replay
từ Last-Event-ID; notification mất được phục hồi từ snapshot. Listener connection
phải nối thẳng PostgreSQL hoặc session pooling, không transaction-pooling LISTEN.

SSE dùng ba event types theo schema hiện có; INVALID/FAILED giữ valid prediction
lịch sử. Metrics OBS-01 có maintai_sse_connections và maintai_sse_resync_total;
HTTP metrics đo handshake, không toàn thời lượng stream. SQL/role ownership giữ
nguyên; chưa thêm Alert source, full history hoặc deployment manifests.
