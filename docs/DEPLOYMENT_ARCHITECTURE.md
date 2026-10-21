> **Tích hợp MaintAI — 2026-10-19:** Tài liệu thiết kế mục tiêu v1.4, chưa xác nhận code/DB/GitOps đã migrate. Đọc [docs index](README.md) để áp dụng quyết định nhóm 4 người, Simulator UI riêng và thứ tự ưu tiên; [readiness](implementation-readiness.md) ghi các schema/scaffold chưa được cung cấp.

# MaintAI v1.4 — Kiến trúc triển khai Kubernetes theo Deployment

**Trạng thái:** Đặc tả kiến trúc mục tiêu (sẵn sàng làm cơ sở triển khai), **không phải bằng chứng cluster đang chạy**. Ngày quyết định: 19/10/2026. Kế thừa MaintAI v1.3.1; cần chủ sở hữu duyệt và merge vào repo source/GitOps thật.

## 1. Phạm vi, thuật ngữ và danh sách workload

**Không đồng nhất** codebase, Kubernetes Deployment, Kubernetes Service, logical database và Kafka consumer group:

- **Codebase** chứa mã nguồn; cùng một image có thể có nhiều startup commands để chạy `processing-worker`, `processing-read-api`, `processing-outbox-relay` trong **ba Deployments khác nhau**.
- **Deployment** quản lý Pods và replicas. **Kubernetes Service** cho địa chỉ mạng ổn định tới Pods, thường cần cho HTTP API nhưng không bắt buộc cho Worker chỉ consume Kafka.
- `business-api` và `business-consumer` là hai Deployments thuộc cùng codebase Business, sở hữu `business_db` và migration chung.
- Ban đầu dùng **một PostgreSQL instance, năm logical DB**; chia logical DB không tự tách CPU, RAM, I/O, connections.
- Ba consumer groups đọc `sensor.readings` để xử lý, lưu trữ và chiếu telemetry; đó không phải ba bản sao vật lý của Kafka topic.

| Workload | Kubernetes resource | Quy mô dựa vào | Quyền truy cập | Ghi chú |
|---|---|---|---|---|
| React Frontend | Deployment + Service | Lưu lượng người dùng | Không có DB credentials | Chỉ gọi Business API |
| Simulator UI riêng | Deployment + Service (planned, chưa có source) | Người vận hành | Sensor Control API | Tách Main UI, Người4 phụ trách |
| Simulator + Control | Deployment + internal Service | Kịch bản demo | State/queue retry riêng | Ban đầu 1 replica, không phải PLC |
| Ingestion API | Deployment + Service | HTTP RPS, ACK p95 | Kafka producer | Auth, envelope validation, accepted/rejected |
| Processing Worker | Deployment | Kafka lag, ML throughput | Ghi `processing_db`, gọi ML | DQ → inference → risk; retry có giới hạn |
| Processing Read API | **Deployment + Service riêng** | History RPS/p95 | `processing_db` read-only, S3 read | History API logic, **không** cần `history_db` |
| Processing Outbox Relay | Deployment | Tuổi/số lượng outbox pending | Quyền outbox có giới hạn + Kafka producer | Publish kết quả bền vững |
| Storage Consumer | Deployment | Archive lag, S3 throughput | Kafka consume, S3 write | Accepted/rejected/outcome (tùy chọn) |
| Business Consumer | Deployment | Projection lag | Kafka + ghi `business_db` | Ba handlers, không infer ML |
| Business API | Deployment + Service | REST RPS/SSE clients | `business_db` với quyền theo nhiệm vụ | Alert, Feedback, SSE, facades |
| ML inference | Deployment + internal Service | Inference QPS/p95 | Model artifacts được pin | Không quyết định Alert |
| RAG API | Deployment + internal Service | Retrieval/LLM QPS | `knowledge_db`, S3 manuals | Lỗi RAG không làm chặn Alert |
| MLflow Server | Deployment + Service | Train/tracking/registry | `mlflow_db` và S3 artifacts | Metadata + registry |
| Drift Worker | CronJob | Theo lịch và data window | `drift_db`, S3 hoặc Read API | Không nằm trên inference critical path |
| Training Pipeline | Job/Workflow theo yêu cầu | Dataset release đã duyệt | Curated S3 + MLflow | Không auto-run do raw upload |
| Prometheus/Grafana/Pushgateway | Workload Platform | Monitoring | TSDB/metrics | Manifest hạ tầng tách khỏi ứng dụng |

## 2. Sơ đồ Deployment — nguồn Mermaid chỉnh sửa được

```mermaid
flowchart LR
  subgraph EXTERNAL[Client và nguồn mô phỏng]
    FE[React UI · Deployment]
    SUI[Simulator UI · Deployment planned]
    SIM[Sensor Simulator · Deployment / Control API]
  end
  subgraph CLUSTER[Kubernetes: namespace MaintAI]
    ING[ingestion-api · Deployment]
    K[(Kafka · topics)]
    PW[processing-worker · Deployment]
    PR[processing-read-api · Deployment]
    OR[processing-outbox-relay · Deployment]
    ST[storage-consumer · Deployment]
    BC[business-consumer · Deployment]
    BA[business-api · Deployment + SSE]
    ML[ml-service · Deployment]
    RG[rag-service · Deployment]
    DW[drift-worker · CronJob]
    TJ[training · Job]
    MF[MLflow · Deployment]
  end
  subgraph DATA[Lớp dữ liệu: 5 DB và Object Storage]
    P[(processing_db)]
    B[(business_db)]
    D[(drift_db)]
    M[(mlflow_db)]
    R[(knowledge_db)]
    S[(S3 / MinIO)]
  end
  SUI -->|Control HTTP có auth| SIM
  SIM -->|HTTP| ING
  ING -->|sensor.readings / sensor.rejected| K
  K -->|Nhóm Processing| PW
  K -->|Nhóm Archive| ST
  K -->|Nhóm Business| BC
  PW -->|HTTP predict| ML
  PW -->|Transaction + Outbox| P
  P --> OR -->|inference.results / alert.signals| K
  ST -->|Lưu trữ bất biến| S
  BC -->|Snapshots và Alerts| B
  B --> BA
  FE -->|REST / SSE| BA
  BA -->|Facade lịch sử| PR
  BA -->|Facade Copilot| RG
  PR -->|Read-only| P
  PR -->|Tra cứu raw lưu trữ| S
  RG --> R
  RG --> S
  DW -->|API lấy window| PR
  DW --> D
  TJ -->|Training run có MLflow tracking| MF
  TJ --> S
  MF --> M
  MF --> S
  ML -->|Tải artifact đã pin khi startup| S
```

Đây là **sơ đồ phụ thuộc logic của các Deployments**. Kafka/PostgreSQL/S3 có thể đặt trong cluster demo hoặc dùng dịch vụ managed/external; không suy ra tính sẵn sàng cao (HA). `processing-read-api` là Deployment riêng; tài liệu nguồn mô tả Compose entrypoint, nhưng app repo hiện vẫn skeleton và chưa xác minh startup command. `processing-worker` không phục vụ HTTP history.

## 3. Chủ sở hữu dữ liệu và phân quyền DB

| Database | Owner | Client và quyền đề xuất | Ranh giới |
|---|---|---|---|
| `processing_db` | Processing | Worker `processing_rw`, Relay quyền outbox, Read API `processing_ro` | Business/Drift không SQL trực tiếp |
| `business_db` | Business | Consumer `business_rw`, API theo scope | Alert, lifecycle, feedback, brief, latest snapshots; **không** full raw history |
| `drift_db` | Drift | Worker `drift_rw` | Checkpoints, drift reports |
| `mlflow_db` | MLflow | Chỉ MLflow Server | Migration do MLflow quản lý |
| `knowledge_db` | RAG | Service `rag_rw` | Documents, chunks, pgvector |

Storage Consumer lưu **raw accepted**, schema-rejected được phép lưu và tùy chọn outcome dài hạn vào S3. `processing_db` có thể giữ sensor payload **trong hot window có giới hạn** để hỗ trợ UI. S3 là kho lưu trữ dài hạn. History Read API không được quét toàn bộ bucket cho mỗi query; phải dùng DB index, manifest chia partition/ngày hoặc query engine. **Lịch sử Alert thuộc Business**, query `business_db`, không chuyển qua Processing Read API.

## 4. Kafka topics, publisher và consumer

| Topic | Publisher | Consumer groups | Ý nghĩa |
|---|---|---|---|
| `sensor.readings` | Ingestion | `processing-v1`, `raw-archive-v1`, `business-live-v1` | Chỉ được xác nhận đúng schema/envelope, **chưa** có DQ result |
| `sensor.rejected` **mới v1.4** | Ingestion | `raw-rejected-archive-v1` | Request sai cấu trúc được quarantine an toàn trong giới hạn auth/size |
| `inference.results` | Processing Outbox Relay | `business-prediction-v1`, `outcome-archive-v1` tùy chọn | Tagged union `PREDICTION_COMPLETED`, `DATA_INVALID`, `INFERENCE_UNAVAILABLE` |
| `alert.signals` | Processing Outbox Relay | `business-alert-v1` | Business dedupe và quản lý vòng đời |

Giữ tên `inference.results` để tương thích v1.3.1 dù topic cũng chứa `DATA_INVALID`; đổi thành `processing.results` sẽ là breaking change và **không thực hiện trong v1.4**. Không bắt buộc topic thứ năm `sensor.quality.results` vì outcome DQ đã nằm trong tagged union. Hệ thống là **at-least-once**; không cam kết exactly-once toàn luồng.

## 5. Luồng schema rejection và các trường hợp crash

1. Ingestion kiểm tra auth, kích thước request và JSON/envelope. **Không archive payload không xác thực hoặc quá lớn** một cách vô điều kiện.
2. Request đã xác thực, nằm trong quarantine size limit nhưng malformed JSON/envelope: tạo `rejection_id` server-side, nhận diện nguồn, `request_id`, thời gian nhận UTC, lỗi schema, payload đã sanitize/giới hạn hoặc hash không đảo ngược. Publish `sensor.rejected` với `acks=all`; **chỉ sau ACK** mới trả `400` (JSON parse) hoặc `422` (envelope). Kafka publish thất bại => `503/504` có thể retry, không nói đã lưu. Dedupe bằng request identity/rejection ID phù hợp.
3. Envelope hợp lệ => publish `sensor.readings`, đợi ACK => `202 ACCEPTED/PENDING`, ngay cả khi features sau đó bị DQ loại.
4. Ingestion/Kafka unavailable => Simulator giữ bounded local retry spool và `event_id` ổn định; UI thể hiện stale. Không hứa lưu vô hạn nếu spool đầy.
5. Processing DQ INVALID => lưu outcome+outbox, không gọi ML, không tự đóng Alert; raw được Storage Consumer archive độc lập, có thể hoàn tất sau.

**Đây là thay đổi contract so với câu chữ cũ chỉ ghi log lỗi schema ở Simulator.** Hệ thống thật cần migration producer/consumer và kiểm thử security/quarantine.

## 6. Tách luồng ghi realtime và luồng đọc lịch sử

**Online write:** Simulator → Ingestion → Kafka → Processing (DQ/ML/Risk) → transaction `processing_db` + outbox → Kafka outcomes/signals → Business Consumer → `business_db` → Business API/SSE. Song song: Storage Consumer → S3; Business telemetry projection nhận raw sớm nhưng đánh dấu `UNVERIFIED`.

**Query read:** React → Business API. Dashboard/Alert đọc `business_db`; trạng thái/lịch sử reading qua `processing-read-api` → indexed `processing_db` và S3 có giới hạn; drift reports qua adapter/API phù hợp. Không bắt Processing Worker thực hiện truy vấn HTTP.

**Tra cứu event cũ:** Index theo `event_id`, `machine_id` (nếu có), nguồn/tenant, schema version, `topic/partition/offset` và `server_accepted_at` UTC. Hot query trả từ DB. Cold query tra partition/ngày/manifest trước rồi đọc object S3 và gắn DQ/prediction bằng event ID. Chưa archive xong phải trả `PENDING_ARCHIVE`, không giả có dữ liệu trên S3. Schema-rejected có thể không có machine ID; phải query theo source/request/time có phân quyền.

## 7. Scale, tính bền vững và giới hạn HA

- Số Worker xử lý thực tế bị giới hạn bởi Kafka partitions, ML/DB throughput và latency downstream. HPA/KEDA chỉ bật sau khi có baseline.
- Read API scale độc lập Worker, nhưng dùng chung `processing_db` nên vẫn có read/write contention. MVP: index, pagination, bounded query, pool/timeout; chỉ cân nhắc read replica sau benchmark. Thêm logical DB thứ sáu cùng instance không cách ly tài nguyên vật lý.
- Kafka offset chỉ commit sau durable side effects: Processing transaction, Business transaction + idempotency inbox, Storage S3 object+manifest. Processing outbox nằm cùng transaction prediction/risk để chống mất event sau commit DB.
- SSE nhiều Business API replicas cần fan-out (`LISTEN/NOTIFY` demo hoặc Redis Pub/Sub) sau projector commit. Reconnect dùng REST snapshot để đồng bộ; không hứa replay toàn bộ SSE.
- Retention Kafka phải đủ bao phủ downtime + recovery + safety margin; cảnh báo khi lag sắp chạm retention.
- Single broker PostgreSQL/MinIO node chỉ phục vụ demo, **không phải production HA**.

## 8. Điều kiện bảo mật và vận hành

- HTTPS/Ingress cho React và Business API; Ingestion chỉ public nếu cần nhận sensor bên ngoài. Processing Read API, ML, RAG, MLflow và Simulator Control là internal có auth/ACL.
- Kubernetes NetworkPolicy, DB roles tối thiểu, Kafka topic ACL, IAM theo S3 bucket/prefix; secret qua Secret/secret manager, không hard-code trong Git.
- `processing_ro`: chỉ SELECT các bảng/views được duyệt. `processing_rw`: quyền ghi cho Worker. Relay: quyền trên outbox và trạng thái gửi có giới hạn. Migration chạy bằng Job riêng trước rollout, không tự migrate khi mỗi Pod startup.
- Image thực tế được pin bằng OCI SHA256 digest trong overlays; rollback bằng approved release manifest và kế hoạch migration tương thích.
- Không cho phép sửa audit/feedback âm thầm; kiểm tra quyền thao tác Alert và đọc sensor/manual.

## 9. Điều kiện hoàn thành và bằng chứng

Chỉ gọi kiến trúc này là **đã triển khai** khi: (a) Deployments/Services/CronJobs tồn tại ở Compose/K8s; (b) Read API tăng replicas độc lập Worker; (c) migrations/grants được áp dụng; (d) contracts accepted/rejected/outcomes được kiểm thử; (e) Kafka/outbox/S3 phục hồi sau crash; (f) release manifest thật nối Git/image/model/dataset/policy; (g) có kết quả load/failure benchmarks.

Manifest kèm bộ bàn giao là **scaffold có placeholder**, không phải bằng chứng cluster đã chạy.
