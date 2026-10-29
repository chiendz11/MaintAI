# MaintAI docs — thiết kế mục tiêu v1.4

Cập nhật: **2026-10-29**. Sáu tài liệu v1.4 đã được nhập vào repo và hiệu chỉnh
đường dẫn cùng các quyết định dự án đã chốt. Bộ Kafka Python dùng chung đã được
thêm riêng; DB/application/image/deployment chưa tự migrate theo tài liệu.

## Đọc theo thứ tự

Để hiểu lý do hình thành kiến trúc, đọc trước bộ
[Yêu cầu → Giả định tải → Kiến trúc → Biện luận](design-basis/README.md).
Bộ này đối chiếu đề Chủ đề3: **giả định ban đầu100 máy×1 reading/s=100 readings/s**;
5 máy là smoke test nhỏ. **Hệ thống scale-ready ở cấp kiến trúc**: khiX>100 sẽ tính
lạiR=X×r, đo bottleneck và tối ưu latency theo budget; không giới hạn cứng100 máy
hoặc tuyên bố throughput đã benchmark. Xem [quy trình scale](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).

| Tài liệu | Vai trò |
| --- | --- |
| [Cơ sở thiết kế](design-basis/README.md) |4 tài liệu dẫn từ FR/quality criteria của đề đến workload, capacity math, architecture decisions và crash/rolling tradeoffs |
| [System Contracts v1.4](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) §34–43 | Nguồn chuẩn cho v1.4, UI-01, OBS-01/MLFLOW-01, PROC-ORDER-01 và baseline100/scale-ready; ưu tiên phần kế thừa |
| [Deployment Architecture](DEPLOYMENT_ARCHITECTURE.md) | Deployments/Services, DB roles, Kafka topics, quarantine và giới hạn scale |
| [Local development k3d](LOCAL_DEVELOPMENT_K3D.md) | Folder hạ tầng local riêng, core platform, app gates và điều kiện còn thiếu trước E2E; chưa tạo cluster |
| [Bộ sơ đồ trực quan](diagrams/README.md) | Use Case, kiến trúc logic, deployment và data flow; SVG/PNG, nguồn DOT và gallery offline có zoom/pan |
| [Runtime Dataflow](RUNTIME_DATAFLOW.md) | Accepted/rejected, DQ/inference/alerts, history và crash boundaries |
| [Versioning & Provenance](VERSIONING_AND_PROVENANCE.md) | Dataset/split/model/image/policy/manual/release lineage, approvals và rollback |
| [Implementation Guide v1.4](MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) §42–56 | Deployment/provenance, UI, instrumentation, Sensor/Storage/MLflow, concurrency, capacity100→X và outcome1.4-only |
| [Kafka/Processing implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) | Bằng chứng23/10 của SDK0.2.0/bundle1.1.0; xem OUTCOME-14-ONLY cho release hiện hành; callbacks/modes/migration/defaults, real broker/DB evidence và app/platform gaps |
| [PROC-ORDER-01 — Processing concurrency/ordering](PROCESSING_CONCURRENCY_AND_ORDERING.md) | Keyed FIFO/executor, safe offset frontier, durable ownership fence, ordered outbox, budgets và migration/acceptance; SDK source đã có, app/prod integration pending |
| [Implementation Plan](IMPLEMENTATION_PLAN.md) | Thứ tự PR 00–10, migration và tiêu chí nghiệm thu cho công việc triển khai sau này |
| [Functional Overview](MaintAI_Functional_Overview.md) | Chức năng và trải nghiệm của người dùng |
| [UI-01 — Bốn giao diện](UI_ARCHITECTURE.md) | Quyết định20/10: Business, Simulator, Grafana, MLflow; API/auth, prototype gaps và release độc lập |
| [OBS-01 — Metrics implementation](OBSERVABILITY_IMPLEMENTATION.md) | Catalog chuẩn names/types/labels/units/buckets, commit/retry, Drift one-hot/Pushgateway, queries và bàn giao GitOps |
| [MLFLOW-01 — Training integration](MLFLOW_INTEGRATION_GUIDE.md) | SDK examples, run/model/artifact identity, durable intent/checkpoints, retry/unknown recovery và strict output mapping |
| [MLflow image/local dev](../ml-service/mlflow/README.md) | App-owned server Dockerfile/locks/Compose/publisher; GitOps-owned deployment/config và signer migration |
| [Sensor0.3.0 implementation](SENSOR_IMPLEMENTATION_REPORT.md) | Các file/lý do thay đổi, runtime/auth/durability, bằng chứng kiểm tra và giới hạn |
| [Storage0.1.0 runtime](../storage-consumer/README.md) | Archive accepted/rejected/optional outcomes, offline config/tests, S3/MinIO settings, retry/probes/metrics và CI |
| [Readiness / khoảng cách tích hợp](implementation-readiness.md) | Implementation thực tế, artifacts chưa được cung cấp và điểm phải freeze |
| [Repository layout](repository-layout.md) | Folder/runtime đang có và phần vẫn skeleton |
| [OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md) | Outcome1.4 duy nhất, risk gốc từ Processing, bỏ reader2.0, SDK0.3/bundle2.0 và runbook cutover |
| [M5 implementation](M5_IMPLEMENTATION_REPORT.md) | Alert lifecycle/evidence/audit/feedback, hot point-read, notification/UI, role grants và kiểm chứng local |
| [M6-HISTORY-01 — Quyết định M6](M6_HISTORY_ARCHITECTURE_DECISION.md) | Ba Deployment Processing; Read API ghi metadata giới hạn, snapshot/export trong Read API; processed history chỉ DB, không cleanup thật |
| [M6-RAW-01 — Storage handoff cho Chiến](M6_RAW_ARCHIVE_HANDOFF.md) | Catalog generation/receipt lookup, coverage từng partition, MinIO local, quyền và acceptance; provider Storage0.2.0 có source |
| [Storage catalog0.2.0 report](STORAGE_CATALOG_IMPLEMENTATION_REPORT.md) | Source/CI, failure-recovery evidence, local fixture harness và giới hạn k3d/M6 |
| [M6 Reader Guide](../storage-consumer/M6_READER_GUIDE.md) | Packaged schemas/examples, delta-chain sync, coverage và cách Người1 tích hợp |
| [API decisions](api-contract-decisions.md) | Chi tiết HTTP của baseline máy đọc được1.3.2; chờ migration v1.4 |
| [History/storage detail](history-storage-contract.md) | Thiết kế archive/chart/export chi tiết kế thừa; chỉ áp dụng khi không trái v1.4 |
| [Recovery/jobs detail](recovery-jobs-operations.md) | Durable jobs, Drift catalog, replay/rolling; giữ các phần không bị v1.4 thay thế |
| [Machine-readable baseline](../contracts/v1.3/README.md) | Sáu OpenAPI cùng schemas/examples1.3.2 đang được validator/CI kiểm tra |
| [Kafka development guide](KAFKA_DEVELOPMENT_GUIDE.md) | SDK Python, contracts Kafka, inbox/outbox/S3, local/CI và cách tích hợp cho4 người |
| [Artifact audit](legacy-artifact-audit.md) | Tài liệu đã xóa và lý do giữ các tài liệu/code còn cần |

## Thứ tự ưu tiên

1. Yêu cầu người dùng và các quyết định dự án tại mục dưới. **M6-HISTORY-01**
   ưu tiên các mô tả cũ về history clocks/cursor, SELECT-only toàn Read API,
   maintenance loop ở Worker, processed archive và retention M6.
2. System Contracts v1.4 §34–43, UI-01 và các điều khoản Deployment/Dataflow/Provenance tương ứng.
   §41 ủy quyền catalog OBS-01/1.0.0 và workflow MLFLOW-01/1.0.0 làm chi tiết
   chuẩn, thay các short names, mã drift0..4 hoặc get-or-create assumptions cũ.
   §42 ủy quyền PROC-ORDER-01/1.0.0 và metrics extension OBS-01 §11; các điều
   khoản này ưu tiên khuyến nghị Processing tuần tự/row-only relay cũ. Không
   nâng trạng thái source/machine contracts chỉ vì target docs đã cập nhật.
3. Các chương kế thừa và Guide; đặc tả chi tiết history/recovery/API bổ sung những phần chưa bị thay thế.
4. Source, runtime metadata và machine contracts là bằng chứng trạng thái đang có,
   không tự chứng minh đã thực hiện yêu cầu v1.4.

Khi các tài liệu còn khác nhau về field/version/cursor, ghi lại trong readiness,
freeze schema/migration trước khi đổi producer hoặc consumer. Không tự sửa schema
đang được CI kiểm tra chỉ để đổi nhãn thành v1.4.

## Quyết định dự án còn hiệu lực

- **M6-HISTORY-01,29/10:** Giữ ba Deployment Processing. Worker chỉ realtime;
  Read API sở hữu history/snapshot/export và runner nền bền vững, dùng pool SELECT
  cho dữ liệu gốc + pool ghi metadata riêng. Kết quả sort processed_at/result_id,
  raw sort received_at/Kafka locator; không đổi latest sequence guard. Chưa archive
  processed outcomes hoặc xóa dữ liệu thật; không thêm processing-maintenance.
  [Decision](M6_HISTORY_ARCHITECTURE_DECISION.md), [handoff Chiến/Người4](M6_RAW_ARCHIVE_HANDOFF.md).
  Demo DB-only khác full M6 với MinIO local; không cần AWS S3. Đây là design freeze,
  không khẳng định History OpenAPI/migration đã implement; Storage publisher0.2.0 có report riêng.

- **OUTCOME-14-ONLY,28/10:** Kafka reader/writer và HTTP/history outcome entrypoint
  chỉ dùng1.4.0. Business giữ risk_score do Processing phát; bỏ compatibility2.0
  và không backfill probability thành risk. SDK0.3.0/bundle2.0.0 giữ SQL001/002.
  [Runbook](OUTCOME_V1_4_MIGRATION.md) phân biệt source migration với dữ liệu cũ.

- **CAPACITY-BASE100-01,23/10:** baseline100 máy×1 reading/s; kiến trúc scale-ready,
  khiX>100 tính/đo lại capacity và tối ưu latency từng stage theo Contracts §43,
  Guide §55 và [Capacity §11](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
  100 máy không là trần, source default5 chưa tự thay đổi.

- **PROC-ORDER-01,23/10:** Processing target dùng bounded keyed FIFO: cùng máy
  tuần tự, khác máy song song, kể cả cùng Kafka partition. Commit safe completed
  prefix theo partition, durable fence chặn stale DB writes. Outbox target thêm
  dispatch order/cursor/head-only claim theo topic/máy; unknown ACK không cho
  successor vượt head. Late duplicates và cross-topic ordering vẫn cần inbox/CAS.
  Source sequence tăng xuyên epoch, không sort opaque epoch. Giữ Deployment/DB/
  topic/bốn UI; SDK0.2.0 đã có code/tests. Guide §53, Plan §K và readiness §19 ghi app/prod gaps.

- **MLFLOW-IMAGE-01,21/10:** Dockerfile/locks/local dev/publisher MLflow ở
  `ml-service/mlflow/` trong app; training/inference/server có image riêng. GitOps
  quản lý production workload/config và nhận exact app-signed digest qua PR.
  Đây là chuyển source producer, không tự bật deployment, cấp IAM hoặc chọn
  server/client version. MLFLOW-01 §2.1 và Guide §52 ghi cutover và prerequisites.

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
- Processing sở hữu DQ/inference/risk và processed history; Storage sở hữu
  raw accepted/rejected archive; Business sở hữu alerts/lifecycle và latest projections.
  Không SQL xuyên DB owner. `processing-read-api` là runtime riêng cùng Processing,
  đọc dữ liệu gốc bằng pool read-only, ghi metadata snapshot/export/catalog bằng
  pool quyền hẹp riêng theo M6-HISTORY-01. Processed archive hoãn sau M6.
- ML training và inference có image riêng. Approved dataset kích hoạt batch
  training; model phát hành qua PR GitOps. Drift không tự retrain/promote hay đổi risk.
- GitOps/hạ tầng nằm ở hai repo riêng; mỗi module owner tự làm tests, Dockerfile,
  probes và cấu hình workload, Người 4 giữ pipeline/môi trường chung.
- **OBS-01 + MLFLOW-01:** App owners tạo metrics/tracking data theo hai hướng dẫn
  mới; Người4 quản lý Prometheus/exporters/Grafana/MLflow deployment ở GitOps.
  MLflow image source/pins/local dev/publisher ở `ml-service/mlflow/`, image riêng
  với training/inference; GitOps chỉ nhận signed digest/config qua PR. Tên,
  labels/types/buckets theo catalog; drift state one-hot0/1. Create-run/version
  có durable intent; unknown result không blind retry hoặc tự chọn latest.
  Chưa migrate source/ledger/GitOps chỉ vì docs đã chuẩn hóa.

## Trạng thái tích hợp

Tài liệu mục tiêu là **1.4.0**; `contracts/v1.3/` và14 runtime descriptors vẫn là
**baseline1.3.2**; có thêm platform descriptor MLflow ở app, không thêm deployment
mới. Sensor0.3.0 dùng wire1.3.0/control1.3.2, Drift còn implementation1.1. `contracts/kafka/`
bundle2.0.0 giữ migration002/rejected1.4 và chỉ đọc/ghi result1.4. HTTP/history
outcome entrypoint cũng dùng schema này. SDK0.3.0 ở `messaging/`
có transport/durable adapters; không phải toàn bộ implementation v1.4. Chưa có
release manifest/provider DTO migrations hoặc full app/K8s scaffold của bộ bàn
giao. Các lệnh `implementation/scripts/...`
trong Plan là ví dụ cho artifacts chưa được cung cấp, không phải lệnh repo đang chạy.
Chi tiết và khác biệt cần giải quyết ở [readiness](implementation-readiness.md).

Storage Consumer0.1.0 đã có owner runtime, locked package, Dockerfile và CI; dùng
microbatch1 của SDK, preflight versioning, retry/rebalance và OBS-01. Chưa có AWS
bucket/IRSA hoặc xác nhận Kafka+S3/container/rollout thật. Có thể validate config
và chạy fixture tests khi chưa có AWS; cấu hình endpoint/bucket/region/credential
qua environment sau. Xem [runbook Storage](../storage-consumer/README.md).

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


Source update23/10: SDK0.2.0/bundle1.1.0 và Processing transport0.1.0 có
keyed/ordered/fenced code, explicit migration/activation và local broker/DB tests.
Các snapshot trước vẫn giữ theo ngày; [implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md)
và readiness §19 là trạng thái hiện tại. App/probes/image/production integration
và config3 release cutover còn pending, không tự enable workload.

## Cập nhật Ingestion M1 — 2026-10-23

Ingestion có HTTP runtime và local tests/image; HTTP Ingestion OpenAPI additive1.4.0,
các API khác giữ baseline1.3.2. Xem [runbook](../ingestion-api/README.md) và
[hướng dẫn và kiểm tra Ingestion](../ingestion-api/README.md). Không thay kết luận
full-system readiness hoặc nghiệm thu shared OIDC/TLS/ACL/deployment.

M5 có implementation local và tests; xem [M5 report](M5_IMPLEMENTATION_REPORT.md) cho source
hiện hành. Các snapshot inventory cũ không chứng minh tất cả provider/deployment đã hoàn tất.
