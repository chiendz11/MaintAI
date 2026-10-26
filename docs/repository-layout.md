# Repository layout — docs target1.4, runtime baseline1.3.2

## Nguồn tài liệu và amendment

Sáu tài liệu v1.4 nhập2026-10-19 thay hai bản Contract/Guide v1.3; ADR versioned1.3.2
được gộp vào docs index và các phụ lục chi tiết. Source manifest giữ upstream SHA,
current SHA và thông tin tài liệu đã retire. Quyết định dự án/nhóm4 người ở docs
index; System Contracts §34–39 và Guide §42–47 là target1.4. Inventory dưới đây vẫn
là baseline runtime1.3.2, chưa tự thay folder/image/entrypoint khi nhập docs.

## Code owner khác deployment

UI-01 ngày2026-10-20 chốt bốn UI chính, chi tiết
[UI Architecture](UI_ARCHITECTURE.md). `frontend/` vẫn là Business UI;
`sensor-simulator-UI/` là source root React/TypeScript cho Simulator frontend độc lập. Không
đổi tên runtime frontend, không gộp frontend vào Sensor backend image. Grafana
và MLflow dùng UI của platform tools. MLflow image source/locks/publisher ở app;
production provisioning/ingress/config và Grafana images vẫn ở GitOps/hạ tầng.
Không thêm hai React roots hoặc portal service bắt buộc trong app.

Lượt implementation20/10 đã thêm sensor-simulator-UI/ app/tests/build/lockfile, demo driver,
live Control client/OIDC/recovery, Dockerfile/runtime descriptor và architecture
index. Provider Sensor0.3.0 đã có HTTP/browser integration; chưa build image hoặc deploy. Hai frontend có thể chung repo
nhưng phải có image/config/Deployment và release riêng; shared library ảnh hưởng
nhiều consumer thì CI/release có phối hợp.

[`architecture/runtime-map.json`](../architecture/runtime-map.json) liệt kê14 app
runtime folders và1 platform descriptor MLflow riêng. Mỗi folder có `runtime.json`
và `Dockerfile`; đây là metadata
inventory, **không phải Kubernetes manifests hoặc bằng chứng mọi entrypoint đã implement**.

- `ingestion-api/`, `storage-consumer/`, `frontend/`, `rag-service/`: mỗi root là
  một planned Deployment.
- `sensor-simulator-UI/`: static React frontend riêng, demo/live client; Sensor0.3.0 provider đã implement; chưa deploy.
- `backend/`: Business owner, có hai runtime `business-api/`, `business-consumer/`.
- `processing/`: Processing owner, có `worker/`, `read-api/`, `outbox-relay/`.
- `ml-service/`: ML owner, có `inference/` Deployment, `training/` WorkflowTask và
  `mlflow/` platform image source/local dev/publisher. Ba image độc lập; production
  MLflow Deployment/Service/config vẫn ở GitOps. Shared preprocessing không là runtime.
- `sensor/`: target long-running simulator/control Deployment; `drift-worker/`:
  target periodic CronJob. Sensor0.3.0 đã migrate ACK202/control; Drift implementation còn theo v1.1.
- `messaging/`: shared Python SDK, không deployment. `contracts/kafka/` là canonical
  transport bundle; các owner gọi adapters thay vì copy Kafka clients vào từng module.

Business API và Consumer cùng image group/DB owner; Processing worker/read/relay
cũng có thể dùng cùng image với entrypoint khác. Tách runtime không bắt buộc tạo
image riêng. ML train và ML inference vẫn có image group riêng. Cả14 runtime
folders có Dockerfile riêng:10 placeholders, Sensor/Drift, Simulator UI và Storage.
Storage image chưa build/run; Dockerfile không chứng minh deployment đã chạy.
MLflow platform Dockerfile bổ sung ở `ml-service/mlflow/`, không cộng vào14 app
runtimes hoặc tạo thêm database/deployment. Upstream base/version pins còn null;
production publication bị chặn cho tới khi owner chốt và kiểm cặp SDK/server.

## Modules được di chuyển

| Skeleton cũ | Skeleton hiện tại |
| --- | --- |
| `backend/sensor-ingestion/` | `ingestion-api/src/maintai_ingestion/` (implementation M1; placeholder `http_ingestion/` đã dọn) |
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

- Sensor0.3.0 có HTTP202 client, SQLite journal/receipts/audit, scoped JWT Control API,
  durable scheduler/sender và probes/metrics. control/scheduling/retry_spool/ordering
  là modules cùng một runtime; không phải service mới. Volume/IdP/Kafka rollout chưa verify.
- Drift hiện có direct PostgreSQL data loader; target chỉ đọc Processing API hoặc
  curated S3 và persist riêng `drift_db`. Hai adapters mới là placeholder.
- Storage0.1.0 có runtime trong `storage-consumer/src/maintai_storage/`, dùng SDK
  accepted/rejected/optional outcomes, probes/metrics và retry. Offline config/tests
  không cần AWS; external integration và bucket/IRSA thực chưa xác nhận.
- `backend/`, `processing/`, ML/RAG/Frontend
  vẫn thiếu application runtime. Kafka producer/consumer, inbox/outbox migration
  và raw S3 adapter nằm tại `messaging/`; owner tích hợp DQ/risk/history/projections,
  auth/probes/SSE và migrations nghiệp vụ. Xem [hướng dẫn nhóm](KAFKA_DEVELOPMENT_GUIDE.md).
- Trainer tham chiếu và deployment blueprints trong hai repo riêng vẫn ở trạng
  thái trước migration, chưa tự chuyển theo việc nhập bộ Markdown mới.
- `contracts/v1.3/` là bundle hiện hành tự chứa sáu OpenAPI/schemas/examples và
  validator được CI gọi. `contracts/kafka/` bổ sung wire target1.4, fixtures/SQL/hash
  lock; không xóa baseline đang là dependency accepted/alert/legacy schemas.
  Sensor tests dùng current HTTP schemas; frozen wire fixture v1.1 đã gỡ. Drift tests dùng
  schema1.0.0 hiện hành trong bundle mới. Không đổi payload để giả vờ source đã migrate.

## Thiết kế kế thừa và phần cần migrate

- Baseline machine union2.0.0/result_kind vẫn được CI kiểm tra; Kafka writer1.4.0
  kind/provenance đã freeze riêng, reader hỗ trợ cả2. Owner rollout consumer-first
  trước cutover; HTTP/provider/release schemas chưa migrate toàn bộ.
- CAS dùng durable per-machine sequence, epoch lineage-only; không compare epoch.
- Simulator UI riêng -> Sensor, Business facade cũ bị supersede.14 descriptors
  cùng baseline1.3.2; Simulator UI client đã implement, phần lớn app còn skeleton/legacy.
- History DB/S3, archived original outcomes, query/export APIs, durable jobs,
  Drift S3 catalog và recovery đã có normative specs; code/migrations chưa có.
- Sensor ACK202/control đã test local; direct-SQL Drift adapter vẫn cần migrate.
- Companion GitOps/infra code đang target1.3.1+simulator-ui, chưa được sửa/validate
  để bật archive/jobs/amendment1.3.2 hoặc quarantine/provenance1.4. App docs không tự áp dụng cloud changes.

Chi tiết từng module và acceptance ở [readiness ledger](implementation-readiness.md).

## Dọn các dấu vết không còn dùng

Hai placeholder simulator facade/control trong Business/Main UI đã được xóa;
tài liệu/contracts kiến trúc v1.1 đã gỡ sau khi chuyển các dependency còn dùng.
Executable source Sensor/Drift được giữ theo yêu cầu người dùng. Xem
[báo cáo rà soát](legacy-artifact-audit.md). Metadata14 runtimes dùng baseline
1.3.2; việc thêm Simulator UI client/descriptor không chứng minh deployment thật.

## PROC-ORDER-01 — module đề xuất bên trong Processing

[Decision1.0.0,23/10](PROCESSING_CONCURRENCY_AND_ORDERING.md) đề xuất keyed runner,
per-machine queues/executor, partition completion coordinator và assignment fence
trong Worker; order cursor/key lease/head-only claim trong Relay. Các modules
đã có trong SDK0.2.0; processing/src có wrappers/factory. Full app callbacks,
health/image còn pending; đọc implementation report thay vì suy từ folder layout.

Worker/Read API/Relay giữ runtime/image-group/DB owner hiện có. Legacy ConsumerRunner
hiện một in-flight/toàn assignment; legacy row-only relay chưa ordered theo key. Thêm
Processing opt-in thay vì tự đổi Storage/Business defaults; migrations mới và
bundle pin review trước enable. Readiness §18 và Plan §K ghi source gaps.

## Ingestion M1 (2026-10-23)

`ingestion-api/src/maintai_ingestion/` là package thực thi HTTP gateway, thay vì
chỉ các thư mục `.gitkeep`. Entry point `maintai-ingestion`, runtime/dev locks,
Dockerfile và tests đã thêm; metadata ghi local implementation, chưa deploy.
HTTP Ingestion1.4.0 có health/metrics/error adaptation, các API khác giữ1.3.2.
M1 dùng gateway/envelope/Kafka publisher của SDK `messaging/`, không nhân bản SDK
vào các module rỗng. Lượt chuẩn hóa OBS-01 ngày2026-10-25 dọn bốn placeholder
`http_ingestion/`, `envelope/`, `kafka_producer/`, `observability/` dưới Ingestion
`src/`; HTTP/auth/guards/metrics thực thi ở package trên. SDK0.2.0/bundle1.1.0
được pin từ lượt cập nhật ordering trước đó; cleanup này không đổi wire/bundle.
Sensor0.3.0 đã có ACK202 client riêng. Local tests và image smoke không thay
nghiệm thu shared OIDC/TLS/ACL, k3d hoặc đo tải; xem [runbook M1](../ingestion-api/README.md).
