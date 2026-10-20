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
├── architecture/                    # Index 14 runtime descriptors; không phải manifests
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
├── ml-service/                      # OWNER ML; hai image group riêng
│   ├── inference/                   # [Deployment] model serving /predict
│   ├── training/                    # [WorkflowTask] batch training/evaluation/export
│   ├── shared/preprocessing/        # Code dùng chung, không phải deployment
│   └── artifacts/                   # Generated bundles ngoài Git
├── rag-service/                     # [Deployment] knowledge/RAG/LLM/citations
├── drift-worker/                    # [CronJob] Processing API/S3 → drift_db/report
├── frontend/                        # [Deployment] skeleton chỉ Business REST/SSE
│   └── src/                        # Live telemetry/latest prediction/realtime
└── sensor-simulator-UI/             # [Deployment] React UI riêng; demo + Control API client
```

Kafka, PostgreSQL (5 logical DBs), S3/MinIO, MLflow Server, Argo và monitoring thuộc
các repo `MaintAI-gitops`/`Maint-infrastructure`. `runtime.json` là metadata kiến trúc,
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
- Cả14 runtime folders có Dockerfile:11 placeholders, Sensor/Drift Dockerfiles
  hiện có và Simulator UI static-server Dockerfile mới (chưa build image).
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

[System Contracts §34–39](docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md)
và [Guide §42–47](docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md)
bổ sung rejected archive, Read API read-only, MLflow/dataset/model/release lineage
và rollout/rollback. [Docs index](docs/README.md) giữ quyết định nhóm 4 người,
Simulator UI riêng và phân cấp tài liệu. [Readiness](docs/implementation-readiness.md)
ghi các khác biệt cần freeze và artifacts chưa có; không coi docs là runtime evidence.
