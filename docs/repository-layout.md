# Repository layout — docs target1.4, runtime baseline1.3.2

## Nguồn tài liệu và amendment

Sáu tài liệu v1.4 nhập2026-10-19 thay hai bản Contract/Guide v1.3; ADR versioned1.3.2
được gộp vào docs index và các phụ lục chi tiết. Source manifest giữ upstream SHA,
current SHA và thông tin tài liệu đã retire. Quyết định dự án/nhóm4 người ở docs
index; System Contracts §34–39 và Guide §42–47 là target1.4. Inventory dưới đây vẫn
là baseline runtime1.3.2, chưa tự thay folder/image/entrypoint khi nhập docs.

## Code owner khác deployment

[`architecture/runtime-map.json`](../architecture/runtime-map.json) liệt kê 13
runtime folders. Mỗi folder có `runtime.json` và `Dockerfile`; đây là metadata
skeleton, **không phải Kubernetes manifests hoặc entrypoint đã implement**.

- `ingestion-api/`, `storage-consumer/`, `frontend/`, `rag-service/`: mỗi root là
  một planned Deployment.
- `backend/`: Business owner, có hai runtime `business-api/`, `business-consumer/`.
- `processing/`: Processing owner, có `worker/`, `read-api/`, `outbox-relay/`.
- `ml-service/`: ML owner, có `inference/` Deployment và `training/` WorkflowTask;
  shared preprocessing không phải service thứ ba.
- `sensor/`: target long-running simulator/control Deployment; `drift-worker/`:
  target periodic CronJob. Implementation hiện tại của hai module còn theo v1.1.
- `messaging/`: shared Python SDK, không deployment. `contracts/kafka/` là canonical
  transport bundle; các owner gọi adapters thay vì copy Kafka clients vào từng module.

Business API và Consumer cùng image group/DB owner; Processing worker/read/relay
cũng có thể dùng cùng image với entrypoint khác. Tách runtime không bắt buộc tạo
image riêng. ML train và ML inference vẫn có image group riêng. Cả 13 runtime
folders có Dockerfile riêng: 11 placeholder mới ghi runtime ID/workload/source và
TODO, chưa build/run; Dockerfile thật của sensor/drift được giữ nguyên.

## Modules được di chuyển

| Skeleton cũ | Skeleton hiện tại |
| --- | --- |
| `backend/sensor-ingestion/` | `ingestion-api/src/http_ingestion/` |
| `backend/data-quality/` | `processing/shared/data_quality/` |
| `backend/inference-client/` | `processing/shared/inference_client/` |
| `backend/risk-decision-engine/` | `processing/shared/risk_engine/` |
| `backend/api/`, `rag-client/` | `backend/business-api/src/api/`, `rag_client/` |
| `backend/alert-manager/`, `audit-logger/`, `feedback-manager/`, `persistence/` | `backend/shared/alerts/`, `audit/`, `feedback/`, `persistence/` |
| Các feature folders dưới `frontend/` | `frontend/src/features/`, `src/api/`, `src/shared/` |

Lượt tạo skeleton chỉ di chuyển `.gitkeep` của thư mục rỗng. Code sensor/drift và
modifications tốc độ per-machine được giữ lại; Guide v1.1 đã gỡ ở lượt dọn tài liệu
theo yêu cầu người dùng. Không đổi database,
Kafka broker/topic, Docker runtime hoặc repositories GitOps/infrastructure.

## Source thực thi chưa migrate

- Sensor hiện gửi HTTP synchronous v1.1 và chờ completed DTO; chưa xử lý 202 ACK,
  local durable retry/reject spool, durable per-machine sequence hoặc control API.
  Các folder `control/`, `scheduling/`, `retry_spool/`, `ordering/` là placeholder.
- Drift hiện có direct PostgreSQL data loader; target chỉ đọc Processing API hoặc
  curated S3 và persist riêng `drift_db`. Hai adapters mới là placeholder.
- `backend/`, `processing/`, `storage-consumer/`, ML/RAG/Frontend
  vẫn thiếu application runtime. Kafka producer/consumer, inbox/outbox migration
  và raw S3 adapter nằm tại `messaging/`; owner tích hợp DQ/risk/history/projections,
  auth/probes/SSE và migrations nghiệp vụ. Xem [hướng dẫn nhóm](KAFKA_DEVELOPMENT_GUIDE.md).
- Trainer tham chiếu và deployment blueprints trong hai repo riêng vẫn ở trạng
  thái trước migration, chưa tự chuyển theo việc nhập bộ Markdown mới.
- `contracts/v1.3/` là bundle hiện hành tự chứa sáu OpenAPI/schemas/examples và
  validator được CI gọi. `contracts/kafka/` bổ sung wire target1.4, fixtures/SQL/hash
  lock; không xóa baseline đang là dependency accepted/alert/legacy schemas.
  Sensor giữ frozen wire fixture trong `sensor/tests/fixtures/`; Drift tests dùng
  schema1.0.0 hiện hành trong bundle mới. Không đổi payload để giả vờ source đã migrate.

## Thiết kế kế thừa và phần cần migrate

- Baseline machine union2.0.0/result_kind vẫn được CI kiểm tra; Kafka writer1.4.0
  kind/provenance đã freeze riêng, reader hỗ trợ cả2. Owner rollout consumer-first
  trước cutover; HTTP/provider/release schemas chưa migrate toàn bộ.
- CAS dùng durable per-machine sequence, epoch lineage-only; không compare epoch.
- Simulator UI riêng -> Sensor, Business facade cũ bị supersede. 13 descriptors
  hiện có là planned metadata1.3.2, implementation vẫn skeleton/legacy; Simulator UI chưa có folder/descriptor.
- History DB/S3, archived original outcomes, query/export APIs, durable jobs,
  Drift S3 catalog và recovery đã có normative specs; code/migrations chưa có.
- Legacy sensor ACK200/422 và direct-SQL Drift adapter chưa chứng minh target.
- Companion GitOps/infra code đang target1.3.1+simulator-ui, chưa được sửa/validate
  để bật archive/jobs/amendment1.3.2 hoặc quarantine/provenance1.4. App docs không tự áp dụng cloud changes.

Chi tiết từng module và acceptance ở [readiness ledger](implementation-readiness.md).

## Dọn các dấu vết không còn dùng

Hai placeholder simulator facade/control trong Business/Main UI đã được xóa;
tài liệu/contracts kiến trúc v1.1 đã gỡ sau khi chuyển các dependency còn dùng.
Executable source Sensor/Drift được giữ theo yêu cầu người dùng. Xem
[báo cáo rà soát](legacy-artifact-audit.md). Target metadata13 runtimes đã đồng bộ
1.3.2; Simulator UI riêng được ghi planned-without-source, chưa tạo fake deployment.


## Ingestion M1 (2026-10-23)

`ingestion-api/src/maintai_ingestion/` là package thực thi HTTP gateway, thay vì
chỉ các thư mục `.gitkeep`. Entry point `maintai-ingestion`, runtime/dev locks,
Dockerfile và tests đã thêm; metadata ghi local implementation, chưa deploy.
HTTP Ingestion1.4.0 có health/metrics/error adaptation, các API khác giữ1.3.2.
Không thay Kafka bundle hoặc Sensor legacy client trong M1.
