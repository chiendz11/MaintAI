# Hướng dẫn làm việc trong MaintAI

Áp dụng cho toàn bộ repository. Tuân thủ hướng dẫn hệ thống/công cụ và yêu cầu
hiện tại của người dùng; các tài liệu kỹ thuật dưới đây không tự mở rộng phạm vi
công việc. Yêu cầu chỉ tạo skeleton hoặc chỉ sửa CI/tài liệu phải giữ đúng phạm vi.

## Tài liệu cần đọc

Trước khi triển khai hoặc thay đổi một module:

1. Đọc `docs/README.md`, `architecture/README.md`, `architecture/runtime-map.json`,
   `docs/repository-layout.md`, README/Dockerfile/runtime.json của component.
2. Target hiện tại là `docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md`,
   revision `1.4.0`: đọc mục 0–4, 31–42 và mục liên quan. §34–42 ưu tiên phần kế thừa;
   quyết định dự án tại `docs/README.md` giữ phân công 4 người và Simulator UI riêng.
   Đọc `docs/DEPLOYMENT_ARCHITECTURE.md`, `docs/RUNTIME_DATAFLOW.md`,
   `docs/VERSIONING_AND_PROVENANCE.md`, `docs/implementation-readiness.md`.
3. Đọc các mục tương ứng trong
   `docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md`; Frontend đọc
   §40–41 và phần v1.4 §42–53; đọc §53 cho PROC-ORDER-01 và Plan §K.
   `docs/IMPLEMENTATION_PLAN.md` là kế hoạch, không là runtime evidence.
   Đọc source/tests để phân biệt implementation v1.1, metadata/OpenAPI baseline1.3.2
   với target docs1.4.0. Kafka SDK và các phần Người 1 đến M5 đã có source;
   đọc báo cáo từng mốc để biết phần đã kiểm chứng và phần còn dùng fixtures.
4. Xem JSON Schemas liên quan trong `contracts/v1.3/`; kiểm tra cả provider và consumer
   của interface trước khi sửa payload hoặc semantics.
5. Với HTTP API, đọc `contracts/README.md`, `contracts/v1.3/README.md` (six target OpenAPIs + schemas/examples) và
   `docs/api-contract-decisions.md`. API giữ các DTO baseline1.3.2; outcome/history
   dùng schema Kafka1.4.0 qua local ref theo OUTCOME-14-ONLY. Các bộ schema phải
   đi cùng nhau khi validate/package; không chứng minh mọi API đã lên v1.4. Freeze schemas
   và migration trước khi thay producer/consumer. Sensor tests dùng SensorRequest/IngestAck/Control DTO hiện hành; generated features
   vẫn kiểm allowlist sáu trường. Không phục hồi wire synchronous v1.1.
6. Với Kafka, đọc `docs/KAFKA_DEVELOPMENT_GUIDE.md`, `contracts/kafka/README.md`
   và `messaging/`. SDK0.3.0 + bundle2.0.0 chỉ đọc/ghi outcome1.4.0 (`kind`);
   đọc `docs/OUTCOME_V1_4_MIGRATION.md` (OUTCOME-14-ONLY). Không reader legacy2.0,
   không suy risk từ probability hoặc backfill legacy. Business giữ risk gốc;
   event cũ bị từ chối trước callback/commit. SHA lock và SQL immutable theo
   bundle; bump bundle khi đổi. Không biến callback skeleton thành success giả.
7. Với Alert/audit/feedback/Processing evidence, đọc `docs/M5_IMPLEMENTATION_REPORT.md`.
   Dedupe fixed300s dùng clock sau machine lock, snapshot gốc bất biến, actions
   machine-first rồi Alert lock; không retry POST unknown hoặc dùng latest thay evidence.
8. Processing/Outbox đọc `docs/PROCESSING_CONCURRENCY_AND_ORDERING.md`, Contracts
   §42, Guide §53 và `docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md`. SDK đã
   có keyed runner/coordinator/fenced handler/ordered relay và app DQ/inference-client/
   risk/owner persistence cùng health server/images. History/archive đầy đủ thuộc M6;
   source và local tests không chứng minh deployment hoặc provider production đã sẵn sàng.

Với thay đổi chỉ về tài liệu/CI, đọc phần đặc tả liên quan đến thay đổi; không
cần triển khai module hoặc chạy toàn bộ app tests chỉ vì tài liệu có checklist.

## Xử lý khác biệt

- System Contracts là nguồn chuẩn cho payload, enums, API, DB/artifact và failure
  semantics. Nếu ví dụ trong Guide khác contract, dùng contract. Khi các nguồn v1.4
  còn mâu thuẫn về version/field/cursor, xem readiness và nêu điểm chưa freeze;
  không âm thầm coi baseline1.3.2 đã đáp ứng v1.4.
- Stack/layout ghi là đề xuất trong Guide không bắt buộc. Source owners hiện có:
  `sensor/`, `ingestion-api/`, `processing/`, `storage-consumer/`, `backend/`
  (Business Backend), `frontend/`, `sensor-simulator-UI/`, `ml-service/`, `rag-service/`, `drift-worker/`.
  Simulator UI có client/demo riêng; đọc `sensor-simulator-UI/README.md` để chạy checks
  và phân biệt mock tests với browser/provider HTTP integration và TLS OIDC fixtures.
- Thư mục có `runtime.json` + `Dockerfile` là một planned workload. `backend/`,
  `processing/`, `ml-service/` là codebase containers; `shared/` không phải runtime.
  Dockerfile/image không quyết định số deployment. Metadata runtime không phải
  Kubernetes manifest, entrypoint đã chạy hoặc giấy phép deploy.
- Đặc tả mô tả hệ thống mục tiêu, không chứng minh tính năng đã được triển khai.
  Kiểm tra source và CI; không mô tả skeleton/mock/fixture là service production.
- Nếu thông tin cần thiết chưa có hoặc contract tự mâu thuẫn, nêu rõ điểm thiếu
  và tác động; không tự bịa payload hoặc âm thầm thay đổi contract để code chạy.

## Ranh giới và invariants

- GitOps/hạ tầng được quản lý trong hai repo riêng. Deployment manifests, lịch
  CronJob, Prometheus rules và Grafana dashboards thuộc các repo đó. Compose dành
  riêng cho integration tests có thể nằm trong component tests.
  Local dev có folder riêng `../MaintAI-k3d/`, xem `docs/LOCAL_DEVELOPMENT_K3D.md`;
  không chép Secrets/PLAINTEXT/replication1/image receipts local lên production.
- Quyết định21/10: source đóng gói MLflow Server/UI, dependency/base pins, local
  Compose và publisher ở `ml-service/mlflow/`; không gộp training/inference images.
  MLflow vẫn là platform runtime: production manifests/config/IRSA ở GitOps/hạ tầng.
  Đọc README module và MLFLOW-01; image source chuyển repo không tự bật deployment.
- Ingestion API độc lập: envelope/auth + Kafka ACK, không deep DQ/ML/DB/SSE.
- PROC-ORDER-01: cùng máy FIFO, khác máy song song có giới hạn; chỉ commit safe
  completed prefix từng partition sau durability. Fetched/queued/done-behind-hole
  cùng thuộc memory budget. Rebalance/timeout phải fence SQL lẫn offset.
- Ordered relay dispatch head topic/máy; retry/unknown ACK chặn successor. Late
  duplicates/cross-topic reordering vẫn cần inbox/CAS và alert lifecycle. Keyed
  mode chỉ opt-in Processing; Storage/Business giữ default serial runner.
- Giữ nguyên001 SQL;002 và activation là explicit Processing-owner operations
  sau drain. Initial legacy source inbox cần reviewed cursor bootstrap; không
  reset inbox hoặc bịa source order từ timestamp để enable.
- Processing sở hữu DQ, inference client, risk, processing_db và outbox. Business
  sở hữu business_db, alerts/audit/feedback/latest projections và RAG client.
- Business API và Business Consumer cùng codebase/DB owner nhưng khác Deployment.
  Ba consumer handlers telemetry/prediction/alert không phải ba microservices.
- Business DB chỉ giữ latest telemetry/latest valid prediction/latest processing
  và business data; raw ở S3/MinIO; Processing history hot DB + verified S3 archive.
- Không query DB xuyên owner. Business/Drift dùng Processing read API/curated S3.
- Main Frontend chỉ gọi Business REST/SSE; Simulator UI riêng gọi Sensor Control API. PG NOTIFY/pubsub fan-out tới
  mọi API replica, REST snapshot khôi phục; không dùng Kafka group làm SSE broadcast.
- Chỉ event `VALID` đi vào ML. `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE` không được
  chuyển thành `NORMAL`; xử lý lỗi phải giữ đúng failure semantics trong contract.
- RAG/LLM enrich alert và cần citations; lỗi RAG không chặn luồng tạo alert.
- Sensor/ML features chỉ dùng allowlist sáu features trong contract. Không đưa
  IDs, timestamps, target hoặc fault labels vào model features.
- Latest valid prediction là historical projection có source ID/age riêng; event
  mới INVALID/FAILED không xóa snapshot đó. Sequence theo máy bền vững qua restart;
  không sort opaque event_id/epoch. Pause chỉ dừng tạo event mới, không resolve alert.
- Drift là batch ngoài critical alert path; `DATA_DRIFT` khác `INVALID_DATA`.
  Drift không tự đổi machine state/severity/risk policy, retrain hay promote model.
- Reference drift phải khớp model/dataset version và đến từ training/approved
  reference; không tạo fallback từ runtime window. Test fixture không phải baseline
  production. Report/checkpoint/metrics phải giữ khả năng retry và traceability.

## Thực hiện và kiểm tra

- Làm rõ interface của module bằng contract trước khi thêm implementation; giữ
  nhất quán giữa đặc tả, models, JSON Schemas và consumer/provider.
- Khi đổi interface, áp dụng Contracts mục 25 về versioning và cập nhật các tests
  liên quan; không cập nhật schema để hợp thức hóa một thay đổi trái contract.
- Chạy kiểm tra phù hợp với thay đổi và yêu cầu hiện tại. Với sensor/drift, dùng
  lệnh trong README component và jobs tương ứng ở `.github/workflows/ci.yml`.
- Với skeleton/docs, chỉ kiểm tra cấu trúc, links/metadata và diff. Không thực hiện
  các checklist triển khai trong tài liệu đính kèm nếu người dùng chỉ yêu cầu skeleton.
- Với thay đổi API docs/schemas/examples, chạy `python contracts/v1.3/validate.py` bằng
  environment đã cài `contracts/requirements.lock`; giữ job `api-contracts` trong
  `CI / required`. Validator kiểm tra baseline1.3.2, không chứng minh target
  v1.4.0 đã được implement. ML/RAG wire1.1.0 và Drift schema1.0.0 vẫn đang được dùng;
  không đổi version chỉ để đồng bộ tên thư mục. Không sửa schema để hợp thức hóa lỗi runtime.
- Phân biệt unit/contract tests, mock/fixture integration, DB/container tests và
  production validation. Báo rõ kiểm tra đã chạy, đã skip và giới hạn còn lại.
- Nếu Docker chưa chạy, không coi container tests là đã pass local. Có thể dùng
  kết quả CI của đúng commit làm bằng chứng và ghi rõ nơi thực thi.
- Giữ secrets/connection strings khỏi logs; không commit dataset đầy đủ,
  credentials hoặc output sinh ra ngoài các fixtures đã quản lý bằng Git.

## Kiểm tra source PROC-ORDER-01

Khi sửa keyed/ordered code, chạy Kafka unit + processing/tests, PostgreSQL ordering
và real Kafka integration khi có local services. Commands/opt-in env trong report.
Storage regression cần chạy khi SDK pin/default runner đổi. CI giữ kafka-ci và
kafka-integration trong required; không tuyên bố Docker/K8s/load PASS từ unit tests.
