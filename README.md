# MaintAI

Application monorepo với **target design1.4: Deployment + Versioning/Provenance**.
Sáu tài liệu v1.4 nằm trong [docs/](docs/README.md), nhập ngày2026-10-19; thay thế
hai tài liệu phiên bản v1.3. Phần lớn source vẫn skeleton/legacy; Simulator UI
client0.1.0 đã có. Machine contracts và
runtime metadata vẫn baseline1.3.2. Kafka có SDK Python và bundle wire riêng;
application runtime chưa tự migrate theo việc cập nhật tài liệu.

## Nhìn folder để biết deployment

**Mỗi thư mục runtime có `Dockerfile` riêng để thể hiện một đơn vị triển khai mục tiêu.**
`runtime.json` bổ sung loại workload, source owner và trạng thái implementation.
`backend/`, `processing/`, `ml-service/` là codebase/ownership containers; `shared/`
và các handler bên trong là module. Image có thể dùng chung cho nhiều deployment
với entrypoint khác. Chi tiết ở [deployment map](architecture/README.md) và
[index máy đọc được](architecture/runtime-map.json).

```text
MaintAI/
├── architecture/                    # 14 app +1 platform descriptor; không phải manifests
├── docs/                            # Target v1.4 + chức năng/readiness/chi tiết kế thừa
├── contracts/                       # v1.3/ là baseline máy đọc được1.3.2
├── messaging/                       # Thư viện Kafka Python dùng chung; không deployment
├── sensor/                          # [Deployment] target simulator + control API
├── ingestion-api/                   # [Deployment] HTTP envelope → Kafka ACK 202
├── processing/                      # OWNER Processing; không là một Deployment
│   ├── worker/                      # [Deployment] DQ → ML client → risk → outbox
│   ├── read-api/                    # [Deployment] internal status/history/window API
│   ├── outbox-relay/                # [Deployment] outbox → Kafka results/signals
│   ├── shared/                      # Modules riêng của Processing
│   └── migrations/                  # processing_db
├── storage-consumer/                # [Deployment] Kafka → S3/MinIO raw archive
├── backend/                         # OWNER Business; không chứa ingestion/DQ/risk
│   ├── business-api/                # [Deployment] REST/SSE/history + jobs facade
│   ├── business-consumer/           # [Deployment] telemetry/prediction/alert handlers
│   ├── shared/                      # Business modules, latest projections/inbox/alerts
│   └── migrations/                  # business_db
├── ml-service/                      # OWNER ML; ba image group riêng
│   ├── inference/                   # [Deployment] model serving /predict
│   ├── training/                    # [WorkflowTask] batch training/evaluation/export
│   ├── mlflow/                      # [Platform Deployment] upstream Server/UI image + local dev/publisher
│   ├── shared/preprocessing/        # Code dùng chung, không phải deployment
│   └── artifacts/                   # Generated bundles ngoài Git
├── rag-service/                     # [Deployment] knowledge/RAG/LLM/citations
├── drift-worker/                    # [CronJob] Processing API/S3 → drift_db/report
├── frontend/                        # [Deployment] skeleton chỉ Business REST/SSE
│   └── src/                        # Live telemetry/latest prediction/realtime
└── sensor-simulator-UI/             # [Deployment] React UI riêng; demo + Control API client
```

Kafka, PostgreSQL (5 logical DBs), S3/MinIO, MLflow Server, Argo và monitoring thuộc
các repo `MaintAI-gitops`/`Maint-infrastructure` về production deployment/resources.
Riêng MLflow image source/locks/local dev/publisher ở `ml-service/mlflow/`; GitOps
chỉ nhận app-signed digest qua PR. `runtime.json` là metadata kiến trúc,
không phải Kubernetes YAML, image đã build hoặc entrypoint hoạt động.

## Luồng mục tiêu

Sensor → Ingestion API → Kafka `sensor.readings`, sau đó có ba consumer groups:
Processing (DQ/ML/risk), Business live projection và Storage raw archive. Processing
outbox phát `inference.results`/`alert.signals`; Business Consumer cập nhật latest
snapshots/alerts, rồi PG NOTIFY/pubsub → Business API replicas → SSE/UI.

UI giữ riêng **Live Telemetry**, **Latest Valid Prediction** và **Latest Processing**,
cùng source event/age và ordering guard. Simulator UI riêng gọi Sensor Control API; pause không xóa backlog hoặc resolve alert.
Business DB chỉ chứa latest projections và business data, không full raw history.

## Đã có gì và còn thiếu gì

- Sensor0.3.0 có ACK202, SQLite/WAL sequence/spool/receipts, scoped JWT Control API,
  scheduler/sender riêng và OBS-01 metrics. Image/deployment chưa verify.
- Drift Worker có detector/CLI/persistence/metrics v1.1; target API/S3 adapters và
  ownership `drift_db` còn skeleton. Xem [sensor](sensor/README.md) và
  [drift worker](drift-worker/README.md) để dùng implementation hiện có.
- [messaging/](messaging/README.md) có Kafka ACK/consumer, owner-local inbox/outbox,
  relay và raw S3 adapter; [Kafka guide](docs/KAFKA_DEVELOPMENT_GUIDE.md) hướng dẫn
  4 người tích hợp. Nghiệp vụ/entrypoint của Ingestion/Processing/Business,
  ML/RAG/Business Frontend và SSE/history vẫn cần implement; Sensor Control đã có.
- [Simulator UI](sensor-simulator-UI/README.md) có React/TypeScript, demo5 máy, live
  Control API client, OIDC, command recovery và checks riêng. Sensor Control API
  đã implement; browser chạy với provider thật + fixture Ingestion; chưa deploy UI.
- Cả14 app runtime folders có Dockerfile:10 placeholders, Sensor/Drift/Simulator UI
  và Storage có source; MLflow thêm platform image descriptor/Dockerfile riêng với
  base/version pins còn unset, chưa build/publish.
- [OpenAPI/schemas/examples](contracts/README.md) hiện tập trung tại
  [v1.3/](contracts/v1.3/README.md): sáu OpenAPI cùng schema/examples tự chứa,
  baseline union2.0.0. [Kafka bundle](contracts/kafka/README.md) bổ sung writer1.4,
  quarantine và reader legacy; HTTP/provider/release migrations chưa hoàn tất. Xem
  [migration notes](docs/repository-layout.md).

CI structure kiểm tra10 source-owner folders và messaging; API validation kiểm tra
đặc tả1.3.2, Kafka SDK có unit/contract và real broker/DB integration jobs,
Sensor tests kiểm ACK202/control/durability/auth; Drift source còn v1.1. Các job component skeleton
vẫn tắt. Simulator UI có job typecheck/format/unit/build/Chromium riêng, được
thêm vào `CI / required`. Xem [.github/README.md](.github/README.md) và [AGENTS.md](AGENTS.md).

Chạy giao diện ngay: `cd sensor-simulator-UI`, `npm ci --ignore-scripts`, `npm run dev`;
mở http://127.0.0.1:5174. Mặc định là demo cục bộ, không gọi Sensor/Kafka thật.


## Tài liệu thiết kế v1.4

[Cơ sở thiết kế: Yêu cầu → Giả định → Kiến trúc → Biện luận](docs/design-basis/README.md)
giải thích kiến trúc từ đề Chủ đề3 với baseline100 máy×1 reading/s=100 readings/s.
Hệ thống scale-ready: khiX>100, tính lại capacity và tối ưu latency từng stage;
5 máy là smoke test nhỏ. Các phép tính chưa là kết quả benchmark.

[System Contracts §34–43](docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md)
và [Guide §42–55](docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md)
bổ sung rejected archive, Read API read-only, MLflow/dataset/model/release lineage
và rollout/rollback. [Docs index](docs/README.md) giữ quyết định nhóm 4 người,
Simulator UI riêng và phân cấp tài liệu. [Readiness](docs/implementation-readiness.md)
ghi các khác biệt cần freeze và artifacts chưa có; không coi docs là runtime evidence.

[Local Kubernetes bằng k3d](docs/LOCAL_DEVELOPMENT_K3D.md) có folder hạ tầng riêng
`../MaintAI-k3d/`: Kafka/DB/MinIO/monitoring và app integration có review gates.
Chưa tạo cluster/build images/E2E; không thay AWS Terraform hoặc production GitOps.

## PROC-ORDER-01 — Processing concurrency và ordered Outbox

SDK0.2.0/bundle1.1.0 có keyed FIFO, bounded cross-machine execution, safe partition
offset coordinator, durable DB fence và head-of-key ordered relay. Processing
transport0.1.0 có wrappers/factory trong processing/src/maintai_processing/.
Cùng máy tuần tự; máy khác có thể chạy đồng thời kể cả cùng partition. Offset501
xong khi500 chưa durable vẫn không cho commit502. Head Outbox retry giữ successor.

Đọc [đặc tả](docs/PROCESSING_CONCURRENCY_AND_ORDERING.md) và
[báo cáo triển khai/cách dùng](docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md).
Full DQ/ML/risk/history callbacks, health server, image/owner DB activation và
rollout còn cần tích hợp. Storage/Business giữ serial default; không thêm runtime.
