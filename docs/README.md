# MaintAI docs — thiết kế mục tiêu v1.4

Cập nhật: **2026-10-20**. Sáu tài liệu v1.4 đã được nhập vào repo và hiệu chỉnh
đường dẫn cùng các quyết định dự án đã chốt. Bộ Kafka Python dùng chung đã được
thêm riêng; DB/application/image/deployment chưa tự migrate theo tài liệu.

## Đọc theo thứ tự

| Tài liệu | Vai trò |
| --- | --- |
| [System Contracts v1.4](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) §34–41 | Nguồn chuẩn cho v1.4, UI-01, OBS-01/MLFLOW-01; ưu tiên phần kế thừa |
| [Deployment Architecture](DEPLOYMENT_ARCHITECTURE.md) | Deployments/Services, DB roles, Kafka topics, quarantine và giới hạn scale |
| [Runtime Dataflow](RUNTIME_DATAFLOW.md) | Accepted/rejected, DQ/inference/alerts, history và crash boundaries |
| [Versioning & Provenance](VERSIONING_AND_PROVENANCE.md) | Dataset/split/model/image/policy/manual/release lineage, approvals và rollback |
| [Implementation Guide v1.4](MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) §42–49 | Triển khai Deployment/provenance, UI và instrumentation/tracking vào từng module |
| [Implementation Plan](IMPLEMENTATION_PLAN.md) | Thứ tự PR 00–10, migration và tiêu chí nghiệm thu cho công việc triển khai sau này |
| [Functional Overview](MaintAI_Functional_Overview.md) | Chức năng và trải nghiệm của người dùng |
| [UI-01 — Bốn giao diện](UI_ARCHITECTURE.md) | Quyết định20/10: Business, Simulator, Grafana, MLflow; API/auth, prototype gaps và release độc lập |
| [OBS-01 — Metrics implementation](OBSERVABILITY_IMPLEMENTATION.md) | Catalog chuẩn names/types/labels/units/buckets, commit/retry, Drift one-hot/Pushgateway, queries và bàn giao GitOps |
| [MLFLOW-01 — Training integration](MLFLOW_INTEGRATION_GUIDE.md) | SDK examples, run/model/artifact identity, durable intent/checkpoints, retry/unknown recovery và strict output mapping |
| [Sensor0.3.0 implementation](SENSOR_IMPLEMENTATION_REPORT.md) | Các file/lý do thay đổi, runtime/auth/durability, bằng chứng kiểm tra và giới hạn |
| [Readiness / khoảng cách tích hợp](implementation-readiness.md) | Implementation thực tế, artifacts chưa được cung cấp và điểm phải freeze |
| [Repository layout](repository-layout.md) | Folder/runtime đang có và phần vẫn skeleton |
| [API decisions](api-contract-decisions.md) | Chi tiết HTTP của baseline máy đọc được1.3.2; chờ migration v1.4 |
| [History/storage detail](history-storage-contract.md) | Thiết kế archive/chart/export chi tiết kế thừa; chỉ áp dụng khi không trái v1.4 |
| [Recovery/jobs detail](recovery-jobs-operations.md) | Durable jobs, Drift catalog, replay/rolling; giữ các phần không bị v1.4 thay thế |
| [Machine-readable baseline](../contracts/v1.3/README.md) | Sáu OpenAPI cùng schemas/examples1.3.2 đang được validator/CI kiểm tra |
| [Kafka development guide](KAFKA_DEVELOPMENT_GUIDE.md) | SDK Python, contracts Kafka, inbox/outbox/S3, local/CI và cách tích hợp cho4 người |
| [Artifact audit](legacy-artifact-audit.md) | Tài liệu đã xóa và lý do giữ các tài liệu/code còn cần |

## Thứ tự ưu tiên

1. Yêu cầu người dùng và các quyết định dự án tại mục dưới.
2. System Contracts v1.4 §34–41, UI-01 và các điều khoản Deployment/Dataflow/Provenance tương ứng.
   §41 ủy quyền catalog OBS-01/1.0.0 và workflow MLFLOW-01/1.0.0 làm chi tiết
   chuẩn, thay các short names, mã drift0..4 hoặc get-or-create assumptions cũ.
3. Các chương kế thừa và Guide; đặc tả chi tiết history/recovery/API bổ sung những phần chưa bị thay thế.
4. Source, runtime metadata và machine contracts là bằng chứng trạng thái đang có,
   không tự chứng minh đã thực hiện yêu cầu v1.4.

Khi các tài liệu còn khác nhau về field/version/cursor, ghi lại trong readiness,
freeze schema/migration trước khi đổi producer hoặc consumer. Không tự sửa schema
đang được CI kiểm tra chỉ để đổi nhãn thành v1.4.

## Quyết định dự án còn hiệu lực

- **Nhóm 4 người:** Người 1 làm Backend + Main Frontend + Ingestion + toàn bộ
  Processing; Người 2 làm ML; Người 3 làm RAG; Người 4 làm Platform/MLOps,
  Sensor + Simulator UI, Drift và Storage Consumer. Kafka cluster/topics/ACL do
  Người4 quản lý. Clients/consumer loops/inbox/outbox/S3 dùng chung từ `messaging/`;
  Người1 tích hợp callbacks nghiệp vụ, Người4 cấu hình/vận hành Storage adapter.
- **Main UI → Business REST/SSE. Simulator UI riêng → Sensor Control API.**
  Business không làm simulator-control facade. Câu chữ inherited về panel control
  trong Main UI/Business proxy phải đọc theo quyết định này. Simulator UI có
  source/runtime descriptor trong `sensor-simulator-UI/`; Sensor0.3.0 đã có provider
  với SQLite state/spool và local provider/browser checks. Việc có source hoặc thêm dòng sơ đồ không chứng minh đã deploy.
- **UI-01 — bốn UI chính:** Business và Simulator là hai frontend có image,
  Deployment/Service, public runtime config và vòng phát hành riêng. Grafana/MLflow
  dùng UI công cụ do platform quản lý. Portal chỉ là launcher tùy chọn;
  `grafana_drift.html` là tab entry của cùng Grafana. Không suy ra năm/sáu frontend
  từ số HTML. Chi tiết bắt buộc đọc: [UI_ARCHITECTURE.md](UI_ARCHITECTURE.md),
  System Contracts §40 và Guide §48. Sửa một UI không bắt các UI khác rollout;
  shared dependency hoặc breaking API vẫn cần compatibility/release phối hợp.
- Shared deployment bắt buộc auth/RBAC/machine scope; external OIDC, không thêm
  Auth Service. Local anonymous chỉ khi bật explicit cho localhost/fixtures.
  Browser dùng Authorization Code + PKCE; Sensor/Business/providers kiểm tra token
  và scope theo nhiệm vụ. Không đặt access token trong SSE query string.
- Ingestion ACK `status_url`/`Location` dùng absolute Business API URL được cấu
  hình; Ingestion không sở hữu status endpoint. Ví dụ relative URL kế thừa cần
  được chuẩn hóa khi migrate DTO/fixtures.
- Processing sở hữu DQ/inference/risk và processed history/archive; Storage sở hữu
  raw accepted/rejected archive; Business sở hữu alerts/lifecycle và latest projections.
  Không SQL xuyên DB owner. `processing-read-api` là runtime riêng cùng Processing,
  dùng DB read-only; các thao tác ghi snapshot/export phải do owner writer thực hiện.
- ML training và inference có image riêng. Approved dataset kích hoạt batch
  training; model phát hành qua PR GitOps. Drift không tự retrain/promote hay đổi risk.
- GitOps/hạ tầng nằm ở hai repo riêng; mỗi module owner tự làm tests, Dockerfile,
  probes và cấu hình workload, Người 4 giữ pipeline/môi trường chung.
- **OBS-01 + MLFLOW-01:** App owners tạo metrics/tracking data theo hai hướng dẫn
  mới; Người4 quản lý Prometheus/exporters/Grafana/MLflow Server ở GitOps. Tên,
  labels/types/buckets theo catalog; drift state one-hot0/1. Create-run/version
  có durable intent; unknown result không blind retry hoặc tự chọn latest.
  Chưa migrate source/ledger/GitOps chỉ vì docs đã chuẩn hóa.

## Trạng thái tích hợp

Tài liệu mục tiêu là **1.4.0**; `contracts/v1.3/` và14 runtime descriptors vẫn là
**baseline1.3.2**; Sensor0.3.0 dùng wire1.3.0/control1.3.2, Drift còn implementation1.1. `contracts/kafka/`
bundle1.0.0 bổ sung rejected1.4/result1.4 cùng compatibility reader2.0 và `messaging/`
có transport/durable adapters; không phải toàn bộ implementation v1.4. Chưa có
release manifest/provider DTO migrations hoặc full app/K8s scaffold của bộ bàn
giao. Các lệnh `implementation/scripts/...`
trong Plan là ví dụ cho artifacts chưa được cung cấp, không phải lệnh repo đang chạy.
Chi tiết và khác biệt cần giải quyết ở [readiness](implementation-readiness.md).

UI-01 chỉ chốt thiết kế, không tạo frontend/config/deployment hoặc sửa wire schema.
Business UI vẫn skeleton; Simulator UI đã có [React client/demo](../sensor-simulator-UI/README.md),
live adapter/OIDC/recovery, tests và Dockerfile riêng. Sensor Control API0.3.0 đã implement; browser checks có real provider/fixture Ingestion,
TLS JWKS/OAuth tests. Production IdP/Kafka, image build/deploy chưa verify. Grafana/MLflow HTML1.6 là
prototype, không phải deployment thật. Rate/counters/preview của Simulator demo
chưa có đủ Control DTO/API; Grafana report datasource/grants cần owner freeze.
Các Business monitoring/drift APIs đang có contract vẫn được giữ.

## Provenance và tài liệu đã thay thế

Hai file System Contracts/Guide v1.3 đã được thay bằng v1.4. ADR versioned1.3.2
đã gỡ sau khi giữ các quyết định dự án ở đây và chi tiết còn cần trong các phụ lục.
Functional Overview, API/history/recovery, readiness/layout/audit vẫn còn chức năng
riêng nên được giữ và cập nhật, không phải bản sao thừa.

[source-manifest.json](source-manifest.json) giữ hash nguyên bản của sáu file
Downloads, hash bytes hiện tại và metadata các bản đã retire. File nguồn Downloads
không bị sửa. Không chạy kế hoạch triển khai trong tài liệu chỉ vì nhập docs.
