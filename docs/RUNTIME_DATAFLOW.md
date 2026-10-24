> **Tích hợp MaintAI — 2026-10-19:** Tài liệu thiết kế mục tiêu v1.4, chưa xác nhận code/DB/GitOps đã migrate. Đọc [docs index](README.md) để áp dụng quyết định nhóm 4 người, Simulator UI riêng và thứ tự ưu tiên; [readiness](implementation-readiness.md) ghi các schema/scaffold chưa được cung cấp.

# MaintAI v1.4 — Luồng dữ liệu lúc chạy và các bất biến bắt buộc

Đọc cùng [Kiến trúc Deployment](DEPLOYMENT_ARCHITECTURE.md) để hiểu ranh giới runtime, và [Quy chuẩn Versioning & Provenance](VERSIONING_AND_PROVENANCE.md) để hiểu các định danh bất biến.

## 0. Điểm vào của bốn UI — UI-01, 2026-10-20

| UI | Luồng request/data | Bất biến |
| --- | --- | --- |
| Business | Browser → Business REST/SSE → projections/history/RAG/drift facade | Không gọi Sensor Control, ML/RAG/Kafka/DB trực tiếp |
| Simulator | Browser → Sensor Control → durable state/scheduler/spool → Ingestion | Browser không phát readings hoặc mutate Alert; không qua Business proxy |
| Grafana | Browser → Grafana server → Prometheus/exporters + approved report datasource | Không tính lại detector hoặc sửa risk; metrics không thay report DB |
| MLflow | Browser → MLflow Server → mlflow_db/artifact store | UI tracking/registry không tự promote inference release |

Sensor backend chạy độc lập browser/UI rollout; đóng hoặc deploy lại Simulator UI
không tự Pause máy. Business UI deploy riêng cũng không restart Sensor. Grafana
hoặc MLflow UI lỗi không tự chặn Business query/Alert path; phụ thuộc lúc model
startup/training phải đánh giá riêng. Xem [UI Architecture](UI_ARCHITECTURE.md).

Control mutation dùng Idempotency-Key + expected_control_version; ACK200 sau
persist,409/timeout cần GET reconcile. API hiện chỉ list/read/pause/resume/scenario;
rate/counters/preview ở HTML cần contract bổ sung trước khi nối thật.

Business summary/drift APIs vẫn tồn tại cho main user, trong khi Grafana chuyên
điều tra kỹ thuật. Report detail datasource ở Grafana cần owner-approved interface
hoặc read-only views/grants, không thêm Drift HTTP server chỉ để sao chép mock.
Các server giữ credential riêng; frontend runtime config và Portal chỉ có public
URLs/capabilities. Grafana/MLflow chưa Ready phải báo rõ, không dựng số liệu giả.

## 1. Nhận dữ liệu, archive, inference và cập nhật Frontend

```mermaid
sequenceDiagram
  participant S as Simulator
  participant I as Ingestion
  participant K as Kafka
  participant A as Storage Consumer
  participant W as Processing Worker
  participant M as ML Service
  participant P as processing_db + Outbox
  participant R as Outbox Relay
  participant B as Business Consumer
  participant D as business_db
  participant U as Business API / SSE
  S->>I: POST /sensor-readings (event_id và sequence ổn định)
  I->>K: sensor.readings (acks=all)
  K-->>I: Broker ACK
  I-->>S: 202 ACCEPTED, PENDING
  par Ba consumer groups độc lập
    K->>A: sensor.readings
    A->>A: Lưu S3 batch bất biến + manifest
    A->>K: Commit offset sau archive bền vững
  and
    K->>B: sensor.readings (live group)
    B->>D: CAS latest telemetry + inbox
    D-->>U: Thông báo sau khi commit
  and
    K->>W: sensor.readings
    W->>W: Data Quality
    alt VALID
      W->>M: predict (model release đã pin)
      M-->>W: prediction + model version thực tế
      W->>W: Áp dụng Risk Policy
    else INVALID
      W->>W: Không gọi ML, lưu validation errors
    end
    W->>P: Outcome + Outbox trong cùng transaction
    W->>K: Commit input offset
    P->>R: Lấy outbox còn pending
    R->>K: inference.results và alert.signals nếu có
    K->>B: Outcome / Alert Signal
    B->>D: Ghi prediction/alert + inbox
    D-->>U: Thông báo sau khi commit
  end
```

**Không được nhầm:** HTTP `202` chỉ chứng minh Kafka đã ACK, **không** chứng minh inference hoặc S3 archive đã xong. Archive có thể hoàn tất trước hoặc sau prediction. Kafka không bảo đảm thứ tự tổng giữa các topic; latest projections phải so sánh ordering token ổn định theo máy. React nhận SSE thông qua Business API, không phải HTTP response gửi ngược cho Sensor.

## 2. Trường hợp sai schema ở Ingestion

```mermaid
sequenceDiagram
  participant S as Simulator
  participant I as Ingestion
  participant K as Kafka
  participant A as Storage Consumer
  participant O as S3
  S->>I: Payload sai cấu trúc nhưng có auth, đúng size limit
  I->>I: Tạo rejection_id và sanitize
  I->>K: sensor.rejected (acks=all)
  alt Broker ACK thành công
    K-->>I: ACK
    I-->>S: 400/422 không retry mù; ghi local rejected log
    K->>A: sensor.rejected
    A->>O: Lưu rejected object + checksum
    A->>K: Commit offset
  else Kafka unavailable
    I-->>S: 503/504 có thể retry, trạng thái nhận có thể chưa chắc chắn
  end
```

Payload không xác thực, độc hại hoặc vượt giới hạn có thể bị từ chối mà **không** lưu raw body. Schema rejection khác với Data Quality INVALID: `DATA_INVALID` do Worker đánh giá trên reading đã đi qua `sensor.readings`.

## 3. Luồng truy vấn lịch sử reading và Alert

```mermaid
sequenceDiagram
  actor U as Technician / React
  participant B as Business API
  participant H as Processing Read API
  participant P as processing_db
  participant S as S3 / manifest index
  participant A as business_db
  U->>B: GET /machines/M-001/sensor-readings?status=INVALID
  B->>H: Query có auth và phạm vi máy/nguồn
  H->>P: Truy vấn theo index, join DQ/prediction
  alt Raw vượt hot-retention window
    H->>S: Tra batch qua partition/date manifest
    S-->>H: Raw details hoặc PENDING_ARCHIVE
  end
  H-->>B: Trang dữ liệu + cursor + completeness
  B-->>U: JSON response
  U->>B: GET /alerts?status=RESOLVED
  B->>A: Đọc alerts và alert_events do Business sở hữu
  A-->>B: Rows và lifecycle timeline
  B-->>U: JSON response
```

Không được quét toàn bộ S3 bucket để phục vụ mỗi truy vấn browser. Hot-history cần index phù hợp trên `machine_id`, `server_accepted_at`, `event_id`, DQ status và vị trí Kafka. Có thể giữ JSON features kích thước giới hạn trong DB trong hot window, không nhân bản raw vô hạn. Cold-history có thể dùng (a) batch manifests có partition/date pruning; (b) Athena/Trino/DuckDB/Parquet query engine nếu dung lượng đủ lớn. **Không cần tạo `history_db` chỉ vì đã có Deployment Read API riêng**.

## 4. Vòng đời dữ liệu và trạng thái

| Lớp | Ví dụ trạng thái | Owner | Hệ quả |
|---|---|---|---|
| HTTP/Kafka | `ACCEPTED`, `REJECTED`, `PUBLISH_UNAVAILABLE` | Ingestion | Mới xác nhận khả năng tiếp nhận tại broker |
| Archive | `PENDING_ARCHIVE`, `ARCHIVED`, `FAILED_ARCHIVE` | Storage | Khả năng truy xuất bản sao bền vững từ S3 |
| DQ | `VALID`, `INVALID` | Processing | INVALID không chạy ML, raw vẫn được giữ |
| Processing | `PENDING`, `PROCESSING`, `RETRYING`, `COMPLETED`, `INVALID`, `FAILED` | Processing | Lỗi không được thay bằng NORMAL giả |
| ML | `NORMAL`, `FAILURE` + xác suất | ML | Chỉ là prediction, không phải severity |
| Risk | `NORMAL`, `WATCH`, `HIGH`, `CRITICAL`, `INVALID`, `UNKNOWN` | Processing | Kết quả áp dụng risk policy |
| Alert | `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS`, `ESCALATED`, `RESOLVED`, `FALSE_ALERT` | Business | Trạng thái công việc kỹ thuật viên |
| Simulator | `RUNNING`, `PAUSED` | Simulator | Dừng/tạo readings mới, không tự đóng Alert |

## 5. Chống trùng và bảo toàn thứ tự

- `event_id` phải giữ nguyên qua retry. `rejection_id`/request identity phải cho phép chống trùng quarantine khi thích hợp. Vị trí `topic/partition/offset` lưu riêng với business IDs.
- Transaction Processing lưu event/hash/outcome/risk/outbox; unique constraints ngăn ghi trùng. Commit offset sau commit DB.
- Business Consumer dùng inbox idempotent theo `signal_id`/`result_id` trong cùng transaction cập nhật projection/Alert. Dedupe Alert theo policy + machine + window, không chỉ theo timestamp.
- Latest telemetry, latest processing outcome và latest successful prediction là **ba projections khác nhau**. Dùng sequence tăng đơn điệu theo máy để không bị rollback do out-of-order. Reading INVALID mới không xóa last valid prediction cũ; UI cần nhãn STALE/historical.
- History pagination dùng cặp `(server_accepted_at,event_id)` hoặc cursor contract đã freeze; không dùng thứ tự chuỗi event_id để xác định reading mới nhất.
- Outcome event có `schema_version`, `event_id`, `result_id`, `producer_git_sha`, `dq_ruleset_version`, model/policy version nếu có, thời gian và nguồn gốc reading.

## 6. Ma trận kiểm thử chịu lỗi

| Kịch bản | Dữ liệu bền vững cần có | Hành vi người dùng/vận hành |
|---|---|---|
| Kafka ngắt ở Ingestion | Không trả ACK giả, Sensor retry spool | UI stale; metrics publish error |
| Sai schema hợp lệ cho quarantine | `sensor.rejected` eventual archive | Tra theo source/request/time, không đòi machine_id |
| Feature DQ INVALID | Raw được archive; `DATA_INVALID`; ML call = 0 | Hiển thị lý do; giữ last valid prediction lịch sử |
| Worker crash sau DB commit nhưng trước event publish | Pending outbox được gửi sau phục hồi | Eventual outcome, không Alert trùng |
| Storage crash | Kafka giữ backlog đến retention | Cảnh báo archive lag, Processing vẫn chạy |
| Business Consumer replay | Inbox ngăn trùng Alert/action | Chỉ một Alert logic |
| History API tăng tải | History query có thể chậm, Worker replicas độc lập | Timeout/pagination; lưu ý DB contention |
| RAG down | Alert và technician actions không bị rollback | Copilot `UNAVAILABLE` |
| Model/Drift reference mismatch | Report `ERROR` lưu đúng versions | Không tự đổi machine risk |
| Model rollback | Pin manifest/artifact release cũ | Prediction mới ghi model cũ thực tế; Alert cũ không đổi |

## 7. Những phần chưa hoàn thành ở code thật

Kafka wire, raw manifest microbatch1, topic/ACL và compatibility reader đã được
code riêng trong [SDK/hướng dẫn Kafka](KAFKA_DEVELOPMENT_GUIDE.md). Chưa xác nhận
real broker/S3 integration hoặc deployment. Còn cần hot/cold query, curated archive
membership, migrations/app callbacks, endpoint prediction history nếu UI cần tab
chi tiết và benchmark. SDK không hoàn tất toàn luồng trong sơ đồ này.

## 8. Observability/tracking — OBS-01 và MLFLOW-01

App metrics được đo tại boundary thật: broker ACK, owner DB/outbox commit,
archive verification, ML/RAG completion; Prometheus/Grafana không tạo business
success thay runtime. Retry/inbox duplicate không tăng committed business event
lần nữa. Histograms cross-service dùng đúng accepted timestamp; Alert Signal1.0
hiện thiếu field này, cần exact-event owner lookup hoặc reviewed schema migration,
không lấy latest telemetry/received_at để đo p95 giả.

Training có durable operation intents trước create-run/version, frozen artifacts,
real selected run/registry identity và verified result trước release dispatch.
MLflow create timeout/5xx có thể đã commit, nên không blind retry theo tag search.
Dispatch ACK khác deploy success; metrics/logging outage không được làm rollback
Alert hoặc đổi risk. Chi tiết source/gate/SDK/recovery ở
[OBS-01](OBSERVABILITY_IMPLEMENTATION.md) và [MLFLOW-01](MLFLOW_INTEGRATION_GUIDE.md).


## 9. Sensor provider đã implement

Sensor0.3.0 lưu reading+sequence/cursor vào SQLite spool trước HTTP; sender chỉ
xóa sau ACK202 đúng identity. Lỗi/mất response giữ same event/payload cho retry.
Control state/version/receipt/audit commit trước200, Pause không xóa backlog.
Scheduler và sender ở cùng Sensor Deployment, independent browser. Bằng chứng
HTTP/browser/TLS fixtures và giới hạn deployment ở [readiness §15](implementation-readiness.md#15-sensor030--providerdurable-runtime2010).
