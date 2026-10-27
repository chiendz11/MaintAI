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
ở Processing; Business giữ serial role loops/inbox SDK0.2.0. Registry, projection
storage/callbacks, Consumer, live HTTP và status facade đã có; Fleet và lifecycle
chưa hoàn tất. MachineDetail hiện vẫn là registry DTO, chưa kết hợp projections.
Telemetry, latest processing và valid prediction có CAS riêng bằng durable source
sequence xuyên epoch; không sort UUID/offset giữa partition. INVALID giữ valid
snapshot lịch sử. Signal đến muộn vẫn cần dedupe/lifecycle/audit, không bị bỏ vì
latest projection mới hơn. Commit DB rồi fan-out SSE, reconnect REST snapshot;
NORMAL mới không tự đóng Alert, giữa topics không có total order.

## API hiện có và cấu hình status facade

- `GET /api/v1/machines/{machine_id}`: scoped registry lookup.
- `GET /api/v1/machines/{machine_id}/live`: ba projections độc lập từ Business DB.
- `GET /api/v1/sensor-events/{event_id}/status`: gọi Processing Read API qua HTTP.

Các machine/event IDs trong URL phải được percent-encode, kể cả slash. Resource
`/live` được nối sau machine ID đã encode; một ID kết thúc bằng `/live` vẫn là
registry lookup khi slash thuộc ID được encode.

Registry/live cho `VIEWER`, `TECHNICIAN`, `ADMIN`; status cho thêm `SENSOR_SERVICE`.
Mọi route vẫn kiểm machine scope. Status facade dùng hai cấu hình đi cùng nhau:
`BUSINESS_PROCESSING_READ_URL` và `BUSINESS_PROCESSING_TOKEN_FILE`. File chứa access
token service riêng, audience và role `BUSINESS_SERVICE` phù hợp Processing; không
được dùng token browser làm credential dịch vụ. Token được đọc lại mỗi request để
hỗ trợ rotation. Khi chưa cấu hình dependency này, status trả503.

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
