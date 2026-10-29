# MaintAI — Mô tả chức năng hệ thống

> **Target v1.4 + UI-01 — 2026-10-20:** System Contracts §34–40 và [quyết định dự án](README.md)
> ưu tiên phần kế thừa. Giữ Simulator UI riêng và các chi tiết history/jobs/recovery;
> bổ sung schema quarantine và provenance. Machine contracts vẫn baseline1.3.2.

Ngày cập nhật: 2026-10-20. **Target: System Contracts `1.4.0` (Deployment / Kafka /
Realtime UI / History DB–S3 / Versioning & Provenance)**. Các API/UI chi tiết kế thừa
được giữ, cần đối chiếu migration trước khi gọi là đã implement.

Tài liệu mô tả **hệ thống sẽ làm gì, người dùng sử dụng ra sao và kết quả mong
đợi trong từng tình huống**. Đây là phạm vi chức năng mục tiêu, không phải thông
báo rằng mọi tính năng đã được triển khai hoặc chạy trên production.

Theo snapshot gốc 2026-10-15, `sensor/` và `drift-worker/` đã có implementation; `backend/`,
`frontend/`, `ml-service/`, `rag-service/` còn là skeleton. **Các thành phần Kafka/Ingestion/Storage/Business split và Realtime UI C trong bản v1.3 là kiến trúc mục tiêu mới**, chưa được xác nhận đã implement. Các API trong `contracts/` hiện cần đồng bộ lại.

## 1. Mục tiêu, phạm vi và nguồn tham chiếu

MaintAI là hệ thống **hỗ trợ bảo trì dựa trên dự đoán và hỗ trợ kỹ thuật viên**.
Hệ thống tiếp nhận dữ liệu cảm biến, dự đoán khả năng máy gặp lỗi, áp dụng chính
sách rủi ro để tạo cảnh báo và cung cấp hướng dẫn kiểm tra có trích dẫn tài liệu.
Một worker độc lập theo dõi sự thay đổi phân bố dữ liệu để hỗ trợ giám sát ML.

Ví dụ một phiên sử dụng:

1. Simulator (nếu RUNNING) gửi event HTTP tới Ingestion, nhận `202` **chỉ sau Kafka ACK**; UI có thể cập nhật raw live telemetry khi projector nhận `sensor.readings`.
2. Group Processing đọc Kafka, kiểm tra Data Quality, gọi ML nếu hợp lệ và đưa ra risk decision; `inference.results` cập nhật **Latest Valid Prediction** khi hoàn thành.
3. Processing phát `alert.signals`; Group Business tạo/dedupe alert để kỹ thuật viên tiếp nhận.
4. Copilot bổ sung giải thích và checklist từ tài liệu bảo trì.
5. Kỹ thuật viên kiểm tra thực tế, cập nhật cảnh báo và gửi phản hồi.
6. Drift Worker định kỳ kiểm tra dữ liệu vận hành có khác reference hay không.

Tài liệu này diễn giải chức năng, không định nghĩa lại payload. Khi triển khai,
đọc theo thứ tự:

| Nguồn | Nội dung cần tra cứu |
| --- | --- |
| [System Contracts](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) | Invariants, enums, giao tiếp giữa các thành phần, traceability và failure semantics; là chuẩn ưu tiên. |
| [Phụ lục HTTP API](api-contract-decisions.md) | Các chi tiết bổ sung về phân trang, vòng đời cảnh báo, brief, monitoring và trạng thái HTTP/UI. |
| `contracts/README.md` (machine-readable source trong repository, **không được đính kèm bộ Markdown này**) | Đường dẫn API, request/response, lỗi và dữ liệu mẫu cụ thể. |
| [Implementation Guide](MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) | Hướng dẫn tổ chức và triển khai từng module; các phần đề xuất không mặc định là yêu cầu bắt buộc. |
| [UI Architecture — UI-01](UI_ARCHITECTURE.md) | Bốn UI, nhóm người dùng, ownership/auth/API, prototype gaps và phát hành độc lập. |

## 2. Người dùng và các thành phần tham gia

### 2.1. Người dùng

| Vai trò | Nhu cầu và thao tác chính |
| --- | --- |
| Người theo dõi — `VIEWER` | Xem danh sách máy, trạng thái, dữ liệu, cảnh báo và monitoring; không thay đổi nghiệp vụ. |
| Kỹ thuật viên — `TECHNICIAN` | Xem dữ liệu, tiếp nhận và xử lý cảnh báo, yêu cầu Copilot, ghi nhận kết quả kiểm tra và đánh giá khuyến nghị. |
| Quản trị viên — `ADMIN` | Có các quyền nghiệp vụ trên; quyền vận hành phải được cấp explicit; chưa có admin policy/config mutation API. |
| Người điều khiển demo — `SIMULATOR_OPERATOR` | Chỉ điều khiển máy giả lập qua Simulator UI riêng theo scope được cấp; không mặc định có business mutation. |

Auth bắt buộc ở shared/demo deployment; anonymous chỉ local explicit. External
OIDC cung cấp identity; Business/Sensor enforce roles và machine scope. Không
thêm auth service riêng hoặc self-registration API. SIMULATOR_OPERATOR là quyền
cho Simulator UI riêng; TECHNICIAN không mặc định có control permission.

### 2.1.1. Bốn UI theo nhóm người dùng — UI-01

| UI | Người sử dụng chính | Chức năng và ranh giới |
| --- | --- | --- |
| Business | Người quản lý/kỹ thuật viên có business scope | Máy, prediction, history, Alert/Copilot/feedback, summary monitoring; chỉ Business REST/SSE |
| Sensor Simulator | Người demo/kiểm thử có simulator scope | Xem/điều khiển stream theo máy qua Sensor Control API; không thay Alert lifecycle |
| Grafana | DevOps/MLOps | Năm dashboard kỹ thuật tham chiếu: overview, inference, Kafka/Processing, infrastructure, drift; dùng Grafana thật |
| MLflow | ML Engineer/MLOps | Experiments, runs, artifacts, registry và lineage; dùng MLflow UI thật |

Business và Simulator có image/Deployment/release riêng. Grafana/MLflow do platform
vận hành với quyền truy cập riêng, không mặc định cấp qua role ADMIN của Business.
Portal chỉ là trang chọn bốn UI; không là UI nghiệp vụ/runtime thứ năm.
Business vẫn có summary và context drift/model cần cho người dùng; tách Grafana
không xóa các APIs monitoring/drift đã chốt hoặc buộc mọi người dùng phải vào Grafana.
Chi tiết UX/API/release và các chức năng demo còn thiếu contract ở
[UI Architecture](UI_ARCHITECTURE.md).

### 2.2. Các thành phần hệ thống (thiết kế mục tiêu 1.3.2)

| Thành phần / repository | Trách nhiệm | Data ownership |
|---|---|---|
| `sensor/` | AI4I replay, fault injection, local retry buffer + stable event ID | Không có database server |
| `ingestion-api/` — Ingestion API độc lập (tên logical; monorepo tùy chọn) | HTTP guard, auth, correlation, Kafka publish/ACK; **không chạy ML** | Không sở hữu logical DB |
| `processing/` — Processing Worker (logical owner) | Kafka consumer, Data Quality, ML client, risk, outbox | `processing_db` |
| Storage Consumer | Consume `sensor.readings`, batch raw archive vào S3/MinIO | S3 raw objects |
| Business Backend (`business-api` + `business-consumer`, cùng codebase) | Machine registry, alert lifecycle, feedback, brief facade, React REST/SSE | `business_db` |
| `ml-service/` | Offline training + online inference deployment, không sở hữu operational DB | Model artifacts qua MLflow/S3 |
| MLflow Server | Experiments/model registry | `mlflow_db` + S3 artifacts |
| `rag-service/` | Documents, retrieval, LLM, citation validation | `knowledge_db` + S3 source docs |
| `drift-worker/` | Periodic feature/prediction drift; không consume Kafka realtime mặc định | `drift_db` + S3 reports |
| `frontend/` | Business REST/SSE; không gọi thẳng ML/RAG/DB | Không sở hữu DB |
| Kafka | Durable event transport, `sensor.readings`, `inference.results`, `alert.signals` | Retention hữu hạn, không thay S3 history |

**Database layout:** một PostgreSQL instance chứa 5 logical DB: `processing_db`, `business_db`, `drift_db`, `mlflow_db`, `knowledge_db` (pgvector). Không có SQL JOIN giữa DB; Business/Drift đọc dữ liệu Processing qua API hoặc curated S3. GitOps/platform vẫn ở repositories hạ tầng riêng.

## 3. Bốn luồng hoạt động bất đồng bộ

```text
Simulator UI -> Sensor Control API + scheduler -> Ingestion API -> Kafka sensor.readings
  processing-v1 -> Processing Worker -> DQ -> ML -> Risk -> processing_db + outbox
  raw-archive-v1 -> Storage Consumer -> S3 raw archive
  business-live-v1 -> Business Consumer -> latest raw telemetry
Processing outbox -> inference.results / alert.signals -> Business Consumer
  -> business_db -> transactional NOTIFY -> Business API replicas -> Main UI REST/SSE
Main UI history -> Business API -> Processing Read API -> outcomes DB / raw MinIO-S3 catalog
Read API -> snapshot/export metadata + bounded durable export runner
M6: no processed archive, no Worker maintenance loop, no real cleanup
Business durable job loop -> RAG briefs / drift manifest catalog sync
Drift CronJob -> drift_db + S3 report/manifest; no Drift HTTP deployment
Approved dataset -> batch training/evaluation -> GitOps PR -> reviewed model release
One PostgreSQL instance: processing_db, business_db, drift_db, mlflow_db, knowledge_db
```

**Bốn data paths:** (1) `sensor.readings` → Business telemetry snapshot/SSE ngay sau Kafka ACK; (2) `sensor.readings` → Processing DQ/ML/Risk → `inference.results`/`alert.signals` → Business projections/alerts/SSE; (3) `sensor.readings` → Storage Consumer → S3 raw archive; (4) Drift CronJob đọc Processing API/S3 ngoài realtime path. RAG enriches existing alerts on demand. Business chỉ giữ **latest telemetry và latest valid prediction** trong read models, không ghi toàn bộ raw history. `Business Consumer` nằm trong Business Backend codebase nhưng chạy Deployment riêng với Business API; Ingestion API **không** nằm trong Business Backend.

Simulator `PAUSED` chỉ ngừng **phát records mới**, không chặn processing các events đã accepted hay tự RESOLVE alert. Simulator UI riêng gọi Sensor Control API trên management origin; Main UI không có control facade. Raw sensor hiện ở UI phải gắn `UNVERIFIED` cho đến khi Processing trả Data Quality outcome.

## 4. Tạo dữ liệu cảm biến mô phỏng

**Người sử dụng:** developer/người chạy demo. **Module:** `sensor/`.

### 4.1. Replay dữ liệu AI4I

Simulator đọc dataset AI4I, chuyển từng bản ghi thành sensor event và gửi tới
Ingestion API khi máy đang RUNNING. Một event gồm ID riêng, máy được gán, thời gian có timezone, nguồn và
sáu features:

| Feature | Ý nghĩa | Đơn vị/giá trị |
| --- | --- | --- |
| `product_type` | Loại chất lượng sản phẩm | `L`, `M`, `H` |
| `air_temperature_k` | Nhiệt độ không khí | Kelvin |
| `process_temperature_k` | Nhiệt độ quá trình | Kelvin |
| `rotational_speed_rpm` | Tốc độ quay | Vòng/phút |
| `torque_nm` | Mô-men xoắn | Nm |
| `tool_wear_min` | Thời gian mòn dụng cụ | Phút |

CLI hiện cho cấu hình dataset/scenario/seed/tốc độ/giới hạn event và dry-run,
xuất JSONL/log/metrics. HTTP baseline đặc tả danh sách máy, đọc control state,
**Pause/Resume riêng từng máy** và đổi scenario; Control API còn cần implement.
Rate mutation, retry/reject counters và preview reading của HTML demo chưa có DTO
đầy đủ. Không coi mọi tùy chọn CLI là API browser; cần freeze contract trước khi
bật các nút đó theo [UI-01 §4](UI_ARCHITECTURE.md#4-api-simulator-phần-đã-đặc-tả-và-khoảng-cách-với-demo).

Theo behavior hiện có sensor 0.2.0, mỗi vòng gửi luân phiên đủ số máy cấu hình, theo thứ tự cố định
được quyết định bởi seed. Mặc định chu kỳ mỗi máy là 1.000 ms, speed 1: mỗi máy
có mục tiêu 1 bản ghi/giây, nên 5 máy có tổng 5 bản ghi/giây; X máy có tổng X
bản ghi/giây. Các lượt gửi được giãn đều và dùng các dòng CSV kế tiếp. HTTP vẫn
tuần tự nên backend chậm/retry có thể giảm tốc độ thực tế. Giới hạn event và các
phase drift tính trên toàn luồng. Xem `sensor/README.md` về công thức và migration.

`machine_id` và thời gian được tạo để mô phỏng hệ thống. Không dùng chúng làm
features của model hoặc coi replay là lịch sử lỗi thật của từng máy. Ground truth
và các nhãn loại lỗi trong dataset không được gửi vào model input.

### 4.2. Tình huống dữ liệu lệch phân bố

| Scenario | Tình huống muốn mô phỏng |
| --- | --- |
| `normal` | Replay baseline để làm đối chứng. |
| `sudden-drift` | Một số thông số đổi phân bố đột ngột sau giai đoạn baseline. |
| `gradual-drift` | Các thông số dịch chuyển dần theo thời gian replay. |
| `categorical-drift` | Tỷ lệ `L/M/H` thay đổi. |

Các scenario drift phải giữ dữ liệu hợp lệ. Tên scenario không bảo đảm kết luận
drift trên mọi model/reference; Drift Worker quyết định theo dữ liệu, reference
và policy thực tế. Kết quả từ test fixture không phải bằng chứng về model production.

### 4.3. Tình huống lỗi để kiểm tra hệ thống

Simulator hỗ trợ tạo lỗi thiếu field, sai kiểu, ngoài miền hợp lệ, gửi trùng event
và giả lập timeout/disconnect tại client. Mục tiêu là kiểm tra backend từ chối
đúng, retry có giới hạn và không tạo kết quả trùng khi response bị mất.

Ví dụ: tốc độ quay âm là dữ liệu invalid; torque vẫn đúng kiểu và miền hợp lệ
nhưng phân bố cả cửa sổ thay đổi có thể là drift. Hai tình huống cần xử lý riêng.

## 5. Tiếp nhận, kiểm tra chất lượng dữ liệu và theo dõi trạng thái

**HTTP edge:** `POST /api/v1/sensor-readings` tại Ingestion API. **Async result:** `GET /api/v1/sensor-events/{event_id}/status` qua Processing read API/Business facade.

### 5.1. Ingestion (HTTP → Kafka)

Simulator gửi schema1.3.0 gồm `event_id`, `machine_id`, `stream_epoch`,
`sequence_number`, `received_at`, `source`, `features`. Ingestion kiểm tra xác thực, kích thước, JSON/envelope cơ bản, gắn `correlation_id` và publish `sensor.readings` với key `machine_id`. Khi Kafka đã ACK, trả HTTP `202 ACCEPTED` với `status_url` và `processing_status=PENDING`, **không đợi ML**.

Nếu Kafka lỗi, trả 503/504 retryable; Simulator giữ cùng event_id/payload trong local buffer để retry backoff. **400/422 envelope invalid chỉ reject record đó**: sau auth/size guard, Ingestion phải đợi ACK `sensor.rejected` trước khi trả400/422; mặc định HASH_ONLY, không raw body. Local rejected log chỉ bổ trợ. Simulator tiếp tục phát record kế khi RUNNING, không dừng toàn bộ stream. Auth401/403 là lỗi cấu hình cần can thiệp, không retry vô hạn; oversized413 không lưu quarantine tùy tiện. `machine_id` phải được provision; machine lookup lỗi hoặc chưa provision được ghi nhận ở Processing, **không tự tạo máy**.

Kafka transport dùng [shared Python SDK](KAFKA_DEVELOPMENT_GUIDE.md), contracts
ở `contracts/kafka/`; app auth/HTTP/DQ/ML/risk callbacks và Sensor ACK202 migration
vẫn cần implement. Library không tự hoàn tất feature của người dùng.

### 5.2. Data Quality ở Processing Worker

Worker mới thực hiện đủ kiểm tra feature type, required, finite, category L/M/H, range/domain. Nhánh VALID được gửi ML; nhánh INVALID ghi validation issues vào `processing_db`, không gọi ML, không tạo alert từ event ấy. Record invalid vẫn phải được Group Storage lưu raw vào S3; tránh loại bỏ outlier quan trọng để điều tra.

`202` là kết quả ingest; trạng thái VALID/INVALID/UNKNOWN là kết quả xử lý sau đó, lấy qua status API/SSE. Khi ML unavailable, event hiện tại FAILED/UNKNOWN, prediction null, không đổi thành NORMAL và không đóng alert đang active.

### 5.3. Duplicate và conflict

`event_id` ổn định theo sensor reading; Worker dùng unique event_id + payload hash, xử lý re-delivery idempotent. Cùng ID và nội dung giống nhau không tạo prediction/alert thứ hai; cùng ID nhưng payload khác ghi conflict/quarantine. Nếu Ingestion không có durable dedupe index, nó **không thể cam kết trả HTTP 409** cho collision trước Kafka; client có thể vẫn nhận 202 và thấy kết quả xử lý conflict về sau.

## 6. Dự đoán khả năng lỗi và quyết định rủi ro

### 6.1. Suy luận ML online

**Module:** `ml-service/`. **API nội bộ:** `POST /api/v1/predict`.

Processing Worker (Kafka consumer) chỉ gửi raw features đã được xác nhận `VALID`. ML dùng preprocessing
đi cùng model artifact rồi trả:

- `failure_probability`: score xác suất từ 0 đến 1.
- `prediction`: `NORMAL` hoặc `FAILURE`, theo `model_threshold`.
- ID prediction/event, phiên bản model/preprocessor, thời gian và độ trễ suy luận.

Khi probability bằng hoặc lớn hơn model threshold, nhãn là `FAILURE`. Output này
là dự đoán nhị phân, không khẳng định nguyên nhân lỗi cụ thể hay máy chắc chắn hỏng.

Nếu ML chưa sẵn sàng, mất kết nối hoặc timeout sau retry cho phép, event hiện
tại không có prediction và máy là `UNKNOWN`. Backend trả lỗi dependency rõ ràng;
không lấy prediction cũ hoặc score 0 để biểu thị kết quả của event mới.

### 6.2. Quyết định rủi ro tại Processing Worker

Risk Decision Engine thuộc Processing áp dụng policy có version lên probability để quyết định
trạng thái máy và việc cần tạo cảnh báo.

| Trạng thái | Ý nghĩa nghiệp vụ |
| --- | --- |
| `NORMAL` | Có kết luận hợp lệ và risk dưới mức cần theo dõi theo policy. |
| `WATCH` | Cần theo dõi thêm. |
| `HIGH` | Rủi ro cao; tạo alert khi policy yêu cầu. |
| `CRITICAL` | Mức ưu tiên kiểm tra cao nhất; tạo alert khi policy yêu cầu. |
| `INVALID` | Dữ liệu đầu vào hiện tại không hợp lệ. |
| `UNKNOWN` | Chưa thể kết luận, ví dụ do ML không sẵn sàng. |

Model threshold quyết định nhãn ML; risk thresholds quyết định mức rủi ro của
sản phẩm. Hai loại ngưỡng có thể khác nhau. Các số ngưỡng minh họa trong tài liệu
gốc không phải giá trị đã được chứng minh phù hợp cho production.

Frontend hiển thị state backend trả, không tự tính severity từ probability.
Mỗi decision cần lưu `policy_version` để giải thích lại kết quả sau này.

### 6.3. Chuẩn bị model offline

Hệ thống cần pipeline phát triển model tách khỏi API suy luận:

1. Chuẩn bị dataset chỉ với sáu features được cho phép và target `Machine failure`.
2. Cố định train/validation/test split và seed để tái tạo kết quả.
3. Huấn luyện baseline ML, một model candidate khác và so sánh rule-based baseline.
4. Đánh giá recall, precision, F1, PR-AUC, false negative rate và confusion matrix;
   không chỉ dựa vào accuracy.
5. Chọn threshold trên validation, sau đó đánh giá cuối trên test.
6. Xuất model, preprocessor, metadata và drift reference tương ứng phiên bản.

Guide đặt mục tiêu `Recall_failure >= 0.85`, `Precision_failure >= 0.50`. Đây là
mục tiêu cần đo và báo cáo, không phải kết quả hiện đã đạt. Drift reference dùng
training split hoặc approved reference; không lấy test set làm baseline production.

## 7. Theo dõi đội máy và xem chi tiết máy

Đây là phạm vi Business UI. Bố cục ZIP tham chiếu ba menu Tổng quan/Thiết bị/
Cảnh báo; history/chart, detail, brief/feedback và summary có thể ở trang con/tab.
Số menu không thay thế phạm vi chức năng, không xóa các khả năng đã mô tả.

### 7.1. Fleet Dashboard — tình trạng fleet và freshness

**API:** `GET /api/v1/machines` (Business API); live cards/summary lấy từ Business-owned projection, REST snapshot + SSE. Dashboard hiển thị danh sách máy, trạng thái/risk do backend trả (NORMAL/WATCH/HIGH/CRITICAL/INVALID/UNKNOWN), số alert chưa xử lý, thời điểm **nhận telemetry gần nhất**, thời điểm **dữ liệu đầu vào của prediction gần nhất**, độ trễ/dữ liệu cũ và kết nối SSE. Filter trạng thái, tìm `machine_id`, phân trang, click xem Machine Detail. Không suy ra `NORMAL` vì có reading raw mới.

### 7.2. Machine Detail — phương án C (hai thẻ riêng)

**REST bootstrap:** `GET /api/v1/machines/{machine_id}/live`; **SSE:** `GET /api/v1/events/stream?machine_id=...`.

Màn hình có hai khu vực độc lập:

1. **Live Telemetry** — 6 AI4I features; cập nhật khi nhận `telemetry.received` từ topic `sensor.readings`, hiển thị event ID / timestamp / “Live Xs ago”, nhãn **raw received / chưa xác thực Data Quality**. Nếu validation sau đó fail, hiển thị issues, không gắn `VALID` giả.
2. **Latest Valid Prediction** — probability, nhãn NORMAL/FAILURE, risk state tại thời điểm dự đoán, model/policy version, **source_event_id**, completed timestamp và **prediction source age**. Chỉ cập nhật khi inference của một event **mới hơn** hoàn thành hợp lệ; prediction chậm/out-of-order không ghi đè prediction mới, vẫn tồn tại trong history và alert processing.

Nếu event R4 đã nhận nhưng prediction mới nhất vẫn là R2, UI hiển thị R4 trong Telemetry và R2 trong Prediction, kèm nhãn `Updating`, `Delayed` hoặc `Stale` dựa trên ngưỡng cấu hình. **Không hiện PENDING nhấp nháy mỗi giây** và không che giấu prediction cũ là mới. Không có prediction → `Not available`, ML lỗi → `Inference unavailable` cạnh lịch sử, active alerts không bị tự động xoá. Dữ liệu nhận thời gian thực khác với độ mới kết luận ML. Bố cục chính thức tham khảo §19.

### 7.3. Lịch sử sensor, processing outcomes và biểu đồ

Main UI gọi Business API; Business authorize rồi gọi Processing Read API, không
SQL cross-owner. M6 đọc original outcomes từ processing_db, raw từ S3/MinIO qua
catalog đã verify. Read API sở hữu snapshot/export và chỉ ghi metadata quyền hẹp.
Chi tiết chốt tại [M6-HISTORY-01](M6_HISTORY_ARCHITECTURE_DECISION.md).

| Chức năng | Public route `/api/v1` | Cách dùng |
| --- | --- | --- |
| Raw receipt history | GET /machines/{machine_id}/sensor-readings | Giữ duplicates/conflicts với receipt ID và processing link khi có |
| Processing/prediction history | GET /machines/{machine_id}/predictions | Completed/invalid/failed outcomes, model/policy versions, filter + cursor |
| Một kết quả gốc | GET /predictions/{result_id} | Không chạy lại model; giữ lineage/evidence |
| Biểu đồ theo thời gian | GET /machines/{machine_id}/timeseries | min/max/avg/count, units, null gaps, event markers |
| Xuất lịch sử | POST /history-exports; GET /history-exports/{job_id} | Job bền vững, authorized download sau success |

M6 chưa archive processed outcomes hoặc xóa source data. Profile30d/365d hoãn
chờ retention approval; snapshot/cursor có expiry và coverage. Outcome sort theo
processed_at/result_id, raw theo received_at/Kafka locator; empty200 khác
expired410 và unavailable503. Full raw demo dùng MinIO local, không cần AWS S3.
Chart/export có query budgets để không chiếm hết pool dashboard. Alert evidence
business-owned vẫn còn khi hot records đã cleanup. Raw invalid fields được render
an toàn, không biến null thành0% hoặc NORMAL. Chi tiết normative ở
[HISTORY-01](history-storage-contract.md).

## 8. Tạo và xử lý cảnh báo bảo trì

### 8.1. Tự tạo và chống trùng cảnh báo

Business Consumer nhận `alert.signals` từ Processing Outbox, tạo alert khi policy yêu cầu và
lưu liên kết opaque IDs về máy, event, prediction, model version, policy version và trace IDs trong `business_db`.
Alert được tạo trước khi cần kết quả Copilot.

Nếu nhiều event liên tiếp có cùng máy/severity trong dedupe window và alert cũ
còn active, hệ thống giữ một active alert thay vì tạo thêm liên tục. Cửa sổ dedupe
là cấu hình; `300` giây trong baseline là ví dụ cấu hình, không phải cam kết cố định.

`severity` cho biết mức ưu tiên (`WATCH`, `HIGH`, `CRITICAL`); `status` cho biết
tiến độ xử lý (`OPEN`, `ACKNOWLEDGED`, ...). Tiếp nhận một alert không làm severity
tự giảm và không làm máy tự chuyển `NORMAL`.

### 8.2. Danh sách và chi tiết cảnh báo

**APIs:** `GET /api/v1/alerts`, `GET /api/v1/alerts/{alert_id}`.

Danh sách hỗ trợ lọc theo status, severity, machine ID và phân trang. Màn hình
chi tiết tập hợp alert, sensor snapshot, prediction, risk score, model/policy
versions, trạng thái Copilot và timeline xử lý để kỹ thuật viên hiểu bối cảnh.

### 8.3. Các thao tác của kỹ thuật viên

Các action dùng `POST /api/v1/alerts/{alert_id}/{action}`:

| Action | Người dùng muốn làm gì? | Trạng thái nguồn hợp lệ | Trạng thái đích |
| --- | --- | --- | --- |
| `ack` | Xác nhận đã tiếp nhận cảnh báo. | `OPEN` | `ACKNOWLEDGED` |
| `start` | Bắt đầu hoặc tiếp tục xử lý. | `ACKNOWLEDGED`, `ESCALATED` | `IN_PROGRESS` |
| `escalate` | Chuyển cảnh báo lên mức cần hỗ trợ/xử lý tiếp. | `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS` | `ESCALATED` |
| `resolve` | Kết thúc xử lý, ghi nội dung kết quả. | `ACKNOWLEDGED`, `IN_PROGRESS`, `ESCALATED` | `RESOLVED` |
| `false-alert` | Ghi nhận kiểm tra không xác nhận bất thường. | `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS` | `FALSE_ALERT` |

Các thao tác ghi nhận người thực hiện và note theo contract; `resolve` bắt buộc
có `resolution`, `false-alert` bắt buộc có `reason`. Nếu bật auth, actor phải khớp
người đã xác thực và có quyền; tự điền `actor_id` không cấp quyền thực hiện.

`RESOLVED` và `FALSE_ALERT` là trạng thái kết thúc. Contract hiện không có reopen.
Chuyển sai trạng thái, kể cả gửi lại action đã hoàn tất, trả
`409 INVALID_ALERT_TRANSITION`. UI cần tải lại alert để hiển thị các action còn hợp lệ.

Action chỉ được báo thành công sau khi trạng thái và audit history đã được lưu.
Hai kỹ thuật viên cùng thao tác phải được xử lý theo state hiện tại tại backend;
UI không quyết định chuyển state chỉ bằng trạng thái đã đọc từ trước.

## 9. Technician Copilot: giải thích và hướng dẫn có căn cứ

### 9.1. Nội dung hỗ trợ

Copilot hoạt động trên alert đã tồn tại, tự enrich theo luồng backend hoặc khi
kỹ thuật viên yêu cầu. Ba intent được hỗ trợ trong contract:

| Intent | Nội dung |
| --- | --- |
| `MAINTENANCE_BRIEF` | Tóm tắt cảnh báo và bối cảnh kiểm tra. |
| `TECHNICIAN_CHECKLIST` | Các bước kiểm tra có dẫn nguồn. |
| `ALERT_EXPLANATION` | Giải thích cảnh báo trong phạm vi prediction và bằng chứng. |

Brief gồm summary, uncertainty, checklist và citations. Mỗi actionable checklist
item phải có nguồn tới chunk thực sự đã được retrieval trả về. Citation giữ
document ID/version, section/page và chunk ID để người dùng đối chiếu nguồn.

Đây là hỗ trợ quyết định theo bối cảnh alert, chưa phải một giao diện chat tự do.
Hiển thị citation metadata không đồng nghĩa đã có API tải hoặc mở PDF gốc.

### 9.2. Yêu cầu và đọc kết quả qua Business API

POST `/api/v1/alerts/{alert_id}/brief` kiểm tra TECHNICIAN/ADMIN + machine scope,
freeze source evidence và commit durable job trước trả202 job_id/status_url.
Business Consumer job runner claim lease, gọi RAG ngoài alert path, lưu outcome
và audit rồi notify. GET brief/job chỉ đọc kết quả, không tự chạy LLM.

UI phân biệt job QUEUED/RUNNING/SUCCEEDED/FAILED với business outcome GROUNDED,
INSUFFICIENT_EVIDENCE, VALIDATION_FAILED, UNAVAILABLE. Retry/lease/fencing cho phép
process restart; stale runner không ghi đè kết quả mới. Các alert actions vẫn
hoạt động khi RAG lỗi. Chi tiết [RECOVERY-01 §3](recovery-jobs-operations.md).

### 9.3. Các trạng thái cần hiển thị

Có hai lớp trạng thái khác nhau:

| Lớp | Giá trị | Cách hiểu |
| --- | --- | --- |
| `copilot_status` | `NOT_REQUESTED` | Chưa yêu cầu brief; không phải lỗi. |
| `copilot_status` | `PENDING` | Yêu cầu đã tiếp nhận, đang chờ kết quả. |
| `copilot_status` | `AVAILABLE` | Có kết quả; cần đọc tiếp `grounded_status`. |
| `copilot_status` | `UNAVAILABLE` | Không cung cấp được brief lúc này; alert vẫn xử lý được. |
| `grounded_status` | `GROUNDED` | Có checklist và citations đã qua kiểm tra liên kết nguồn. |
| `grounded_status` | `INSUFFICIENT_EVIDENCE` | Thiếu bằng chứng; thông báo cần manual review, checklist/citations rỗng. |
| `grounded_status` | `VALIDATION_FAILED` | Kết quả sinh không đạt kiểm tra grounding; không hiển thị actionable advice. |

`AVAILABLE` không bảo đảm có lời khuyên: brief hoàn tất nhưng thiếu bằng chứng
vẫn có thể là `AVAILABLE / INSUFFICIENT_EVIDENCE`, HTTP `200`. Khi RAG/provider
unavailable, backend biểu diễn `copilot_status=UNAVAILABLE`; alert giữ nguyên.

### 9.4. Chuẩn bị và sử dụng kho tri thức

Phía RAG cần pipeline ingestion tài liệu: parse, giữ metadata trang/mục, chia
chunks, tạo embeddings và lưu PostgreSQL/pgvector. Phiên bản tài liệu mới không
được xóa lịch sử nguồn đã dùng cho brief cũ.

Khi sinh brief, RAG xây query từ bối cảnh, lọc metadata khi phù hợp, truy xuất
chunks, tạo prompt, gọi LLM và kiểm tra citations. Thiếu evidence phải trả trạng
thái rõ ràng; không tự bù hướng dẫn kỹ thuật từ trí nhớ chung của LLM. Chất lượng
retrieval cần được đánh giá bằng gold set và `Recall@k` theo Guide.

Đây là chức năng chuẩn bị dữ liệu nội bộ; contract hiện chưa có màn hình/API
upload, xóa hay quản trị tài liệu dành cho người dùng.

## 10. Ghi nhận kết quả kiểm tra và phản hồi

**API:** `POST /api/v1/alerts/{alert_id}/feedback`.

Kỹ thuật viên có thể ghi nhận hai đánh giá độc lập:

| Nhóm phản hồi | Giá trị | Ý nghĩa |
| --- | --- | --- |
| Kết quả kiểm tra — `technician_verdict` | `CONFIRMED_ISSUE` | Có vấn đề được kiểm tra xác nhận. |
| Kết quả kiểm tra | `FALSE_ALERT` | Không xác nhận được bất thường. |
| Kết quả kiểm tra | `INCONCLUSIVE` | Chưa đủ cơ sở kết luận. |
| Độ hữu ích — `recommendation_feedback` | `USEFUL`, `PARTIALLY_USEFUL`, `NOT_USEFUL`, `NOT_AVAILABLE` | Đánh giá khuyến nghị hữu ích, hữu ích một phần, không hữu ích hoặc không có để đánh giá. |

Feedback có thể kèm note; khi lưu thành công backend trả `201` cùng feedback ID,
alert ID và thời gian ghi nhận. Dữ liệu này phục vụ audit và đánh giá sau này.

Gửi feedback `FALSE_ALERT` không tự chuyển lifecycle alert sang `FALSE_ALERT`.
Muốn đổi trạng thái phải gọi action tương ứng. Feedback cũng không tự điều
chỉnh policy, retrain hoặc promote model. Không retry POST feedback một cách
mù quáng nếu chưa biết lần gửi trước đã được lưu hay chưa.

## 11. Theo dõi drift dữ liệu và prediction

**Module:** `drift-worker/`. **Đối tượng sử dụng kết quả:** người vận hành/ML và
người theo dõi monitoring.

### 11.1. Câu hỏi chức năng cần trả lời

Worker đánh giá: **dữ liệu vận hành gần đây có khác đáng kể so với reference
được dùng cho model đang đánh giá không?** Ví dụ từng record torque vẫn hợp lệ,
nhưng cả cửa sổ có tỷ lệ torque cao hơn nhiều so với reference.

Worker tính drift trên sáu features; có thể đánh giá thêm thay đổi tỷ lệ dự đoán
`FAILURE/NORMAL` khi có đủ predictions đúng model và prediction reference.
Prediction drift là thay đổi phân bố đầu ra, không chứng minh model đã giảm
độ chính xác. Performance drift cần ground truth đáng tin cậy mới kết luận được.

### 11.2. Dữ liệu đầu vào và policy

- Chỉ lấy events `VALID`; không đưa invalid records, IDs, timestamp hoặc target
  vào features để tính drift.
- Reference phải khớp model/dataset version; không tự chọn reference gần nhất
  hoặc tạo reference từ runtime window để che mất drift.
- Implementation hiện lấy tối đa 500 events hợp lệ mới nhất trên toàn bộ tập
  runtime; minimum mặc định là 200. Đây là cửa sổ theo số bản ghi, không phải mặc
  định 15 phút hoặc báo cáo riêng cho từng máy.
- Các rolling windows có thể chồng lấp. Checkpoint ghi tiến độ thành công,
  không biến window thành batch chỉ chứa những event chưa từng được đọc.
- Numeric và categorical features dùng PSI. Policy hiện có warning `0.10`,
  critical `0.20` ở cấp feature; report tổng thể critical khi ít nhất hai features
  critical. Các giá trị này cần version hóa và hiệu chỉnh bằng evidence.

Một feature critical đưa report tổng thể lên warning nếu chưa đủ điều kiện
aggregate critical. Prediction drift có thể góp vào cảnh báo tổng thể warning;
không được hiểu mọi nhãn critical cấp feature đều làm máy thành critical.

### 11.3. Kết quả và xử lý lỗi

| `overall_status` | Ý nghĩa để người vận hành xử lý |
| --- | --- |
| `STABLE` | Chưa có bằng chứng drift vượt policy hiện tại. |
| `WARNING` | Có tín hiệu lệch phân bố cần theo dõi/điều tra. |
| `CRITICAL` | Drift đạt mức nghiêm trọng theo aggregate policy. |
| `INSUFFICIENT_DATA` | Chưa đủ mẫu để kết luận; không báo stable cho trường hợp này. |
| `ERROR` | Không tính được report, ví dụ thiếu reference đúng version. |

Report lưu model/reference/detector versions, cửa sổ, số mẫu, scores,
thresholds và lý do để truy vết. Retry cùng input dùng report đã có thay vì tạo
bản sao vô hạn. Prediction đến muộn hoặc reference/policy thay đổi có thể tạo
report mới theo identity của input.

Worker chạy một lượt rồi thoát: lưu report → công bố metrics → commit checkpoint
→ cập nhật heartbeat. Khi metrics publication là bắt buộc nhưng thất bại,
checkpoint không tiến; lượt sau có thể dùng report đã lưu để thử lại.

Drift chỉ tạo kết quả monitoring. Nó không tự đổi trạng thái máy, severity hoặc
policy; không tự tạo maintenance alert, retrain hoặc promote model. Platform
có thể cảnh báo drift/stale/error bằng Prometheus rules ở repo GitOps riêng.

### 11.4. Xem danh sách và chi tiết báo cáo

Business cung cấp report summary/detail theo contract cho người dùng được phép;
Grafana cung cấp điều tra kỹ thuật PSI/trend/reference/current qua datasource
riêng được duyệt. Hai UI không tạo hai detector hoặc hai nguồn quyết định drift.

Drift CronJob giữ detector/checkpoint trong drift_db và công bố versioned S3
report + committed manifest. Business job runner sync metadata catalog, không
đọc drift_db và không thêm Drift HTTP server cho one-shot worker.

GET `/api/v1/drift-reports` có model/status filter, opaque cursor và catalog_as_of/
stale; GET `/{report_id}` lấy exact S3 version + verify checksum. No reports là
empty200, missing ID404, report ERROR vẫn200, lỗi nguồn503. UI không suy ra STABLE
khi thiếu/cũ dữ liệu; `drift.updated` phát sau catalog commit. Chi tiết RECOVERY-01 §4.

## 12. Giám sát sức khỏe hệ thống và độ mới dữ liệu

### 12.1. Monitoring trên ứng dụng

**API:** `GET /api/v1/monitoring/summary`.

Summary/panel trạng thái trong Business UI cho biết trạng thái backend/ML/RAG, thông báo dependency,
thời điểm quan sát, report drift gần nhất và tình trạng worker. Dependency có
thể là `AVAILABLE`, `DEGRADED` hoặc `UNAVAILABLE`; đọc được observation degraded
vẫn là request HTTP `200` thành công.

Cần phân biệt ba thông tin:

| Thông tin | Câu hỏi được trả lời |
| --- | --- |
| Report `overall_status` | Phân bố dữ liệu được đánh giá là stable, drift hay chưa kết luận được? |
| Worker `FRESH` / `STALE` / `NOT_AVAILABLE` | Worker có hoàn tất thành công gần đây theo checkpoint không? |
| `latest_report.window_end_at` | Dữ liệu dùng trong report mới đến thời điểm nào? |

Worker có thể còn fresh nhưng dữ liệu đầu vào đã cũ. Report được dùng lại có thể
giữ `computed_at` cũ dù worker vừa chạy thành công; không dùng riêng thời gian
tính report làm heartbeat. Ngưỡng stale là cấu hình môi trường.

### 12.2. Health, metrics và logs cho vận hành

Instrumentation theo [OBS-01](OBSERVABILITY_IMPLEMENTATION.md): từng module owner
tạo metrics với names/types/labels/buckets và commit/retry semantics đã chốt;
Người4 nối Prometheus/exporters/Grafana trong GitOps. Training tạo experiments/
metrics/artifacts/Registry evidence theo [MLFLOW-01](MLFLOW_INTEGRATION_GUIDE.md).
MLflow UI không tự có dữ liệu nếu app chưa log; docs không chứng minh deployment.

Ingestion API, Business API, ML và RAG cần `/health/live`, `/health/ready` và `/metrics`; Kafka workers expose metrics/health qua exporter hoặc probe tương ứng. Liveness
cho biết process sống; readiness cho biết khả năng phục vụ. RAG không sẵn sàng
không được tự làm backend mất readiness của luồng cảnh báo.

Metrics phục vụ theo dõi ingestion ACK/failed publish, Kafka consumer lag, S3 archive lag, request/errors/latency, chất lượng dữ liệu, prediction,
cảnh báo, retrieval/citation failures và drift. Drift Worker one-shot công bố metrics
qua Pushgateway để Prometheus đọc khi process đã thoát. Structured logs giữ
trace IDs để điều tra, không ghi secrets.

Prometheus/Grafana cung cấp quan sát kỹ thuật sâu hơn. API monitoring hiện chưa
trả các chỉ số p95/error rate đầy đủ và chưa có URL Grafana trong DTO; việc thêm
dashboard/link trên UI cần cấu hình hoặc bổ sung contract phù hợp. Manifests,
lịch worker, alert rules và Grafana dashboards thuộc repo GitOps/hạ tầng riêng.
Grafana outage không được làm chặn chức năng Alert/History của Business. Link
Grafana/MLflow lấy từ public runtime config đã duyệt, không gắn credentials vào URL.

## 13. Cập nhật giao diện và xử lý trạng thái

### 13.1. Realtime v1.3 — Live Telemetry và Prediction độc lập

Frontend dùng **phương án C**, SSE là kênh đẩy cập nhật nhanh cho UI real-time; REST bootstrap/reconnect là nguồn trạng thái chuẩn. Business Consumer (tách runtime khỏi Business API) consume `sensor.readings` và `inference.results`, cập nhật snapshot; Backend SSE Publisher đẩy `telemetry.received`, `prediction.completed`, `processing.status_changed`, các alert/brief/service events tới React. **Một Backend replica không được giả định nhận được tất cả sự kiện chỉ vì cùng Kafka consumer group với các replica khác**; khi scale cần fan-out/pubsub theo System Contracts §32.7. SSE mất kết nối → reconnect và resync REST, không hứa event replay hoàn chỉnh.

### 13.2. Những trạng thái UI phải phân biệt

Bổ sung v1.3: UI ingest/test harness cần hiển thị `ACCEPTED/PENDING/PROCESSING/RETRYING/COMPLETED/INVALID/FAILED` **là trạng thái xử lý event**, không được render `PENDING` thành trạng thái máy. Từ Business Dashboard, alert và machine snapshots đến muộn là eventual consistency bình thường; sau SSE reconnect fetch lại REST.

| Tình huống | Hành vi giao diện |
| --- | --- |
| Đang tải request | Hiển thị `LOADING`; không kết luận trạng thái máy từ dữ liệu chưa có. |
| Danh sách rỗng | Hiển thị `EMPTY`, không coi là lỗi server. |
| `NORMAL` / `WATCH` / `HIGH` / `CRITICAL` | Hiển thị đúng state từ backend, kèm risk nếu có. |
| Dữ liệu `INVALID` | Chỉ ra lỗi validation; không render như trạng thái bình thường. |
| Máy `UNKNOWN` | Thông báo chưa thể kết luận; không thay bằng score 0 hoặc màu/nhãn normal. |
| Copilot pending/unavailable | Hiển thị trạng thái riêng; giữ phần cảnh báo và các action hoạt động. |
| Brief thiếu evidence/grounding thất bại | Hiển thị thông báo và không đưa actionable checklist. |
| Request lỗi mạng/API | Hiển thị `ERROR` và hướng xử lý phù hợp; không spinner vô hạn. |
| Resource không tồn tại | Hiển thị not found; khác danh sách không có phần tử. |
| Action bị `409` | Thông báo state đã thay đổi/không hợp lệ, tải lại dữ liệu. |
| Không có quyền khi bật auth | Xử lý `401/403`; không gửi lại liên tục như lỗi transient. |

`LOADING`, `EMPTY`, `ERROR` là trạng thái request/render. Chúng không được thêm
vào enum trạng thái máy hoặc dùng để thay thế `INVALID`/`UNKNOWN`.

## 14. Audit và truy vết quyết định

Từ một cảnh báo, hệ thống cần truy ngược được:

```text
Alert → Prediction → Sensor event → Máy + validated features
      → Model/preprocessor versions
      → Risk policy version
      → Timeline thao tác + người thực hiện
      → Feedback
      → Brief → Document version + chunk + page/section
```

Timeline trình bày các sự kiện theo thời gian, gồm tạo cảnh báo và thay đổi
trạng thái, trạng thái trước/sau, actor, note và timestamp. Các mốc thời gian
được lưu có timezone; frontend có thể hiển thị theo timezone người dùng.

Mục tiêu là trả lời được: cảnh báo do dữ liệu nào, model/policy nào, ai đã xử lý,
khuyến nghị lấy từ tài liệu nào và người dùng phản hồi ra sao. Request/correlation
IDs hỗ trợ nối log giữa các service; không đưa các ID riêng lẻ vào metric labels.

## 15. Các kịch bản sử dụng tiêu biểu

| Kịch bản | Chuỗi hành vi mong đợi |
| --- | --- |
| Máy hoạt động bình thường | Sensor valid → ML có prediction → risk policy trả normal → dashboard hiển thị bình thường. |
| Có rủi ro cần xử lý | Sensor valid → ML → risk policy yêu cầu alert → alert xuất hiện → technician ACK/start → kiểm tra → resolve và ghi feedback. |
| RAG không hoạt động | Alert vẫn tạo và xử lý được → Copilot báo unavailable → technician tiếp tục kiểm tra theo quy trình của mình. |
| RAG thiếu bằng chứng | Brief hoàn tất với insufficient evidence → không có checklist giả → người dùng được yêu cầu manual review. |
| Sensor gửi dữ liệu lỗi | Envelope hợp lệ được Kafka ACK 202 → Processing ghi INVALID, không gọi ML → UI status/history hiển thị lỗi; raw vẫn archive S3. |
| ML timeout | HTTP ingest đã 202, nhưng async Processing ghi RETRYING rồi FAILED/UNKNOWN nếu hết retry; UI không suy đoán máy normal. |
| Gửi lại cùng event do mất response | HTTP có thể vẫn 202; Processing/Business consumers idempotent nên không phát sinh prediction/alert thứ hai. |
| Dữ liệu đổi phân bố nhưng vẫn hợp lệ | Online path tiếp tục → worker batch đánh giá drift → monitoring cảnh báo riêng; machine severity vẫn theo risk policy. |
| Worker lâu không thành công | Monitoring hiển thị stale theo checkpoint → người vận hành điều tra; việc tiếp nhận sensor không bị chặn. |

## 16. Ranh giới chức năng hiện tại

Những phần dưới đây chưa nằm trong chức năng đã chốt hoặc cần quyết định riêng
trước khi triển khai:

- Điều khiển máy, PLC/actuator hoặc tự động thực hiện bảo trì.
- Dự đoán chính xác nguyên nhân/loại lỗi hoặc remaining useful life; model hiện
  có nhiệm vụ binary machine failure classification.
- Tự retrain/promote model hoặc tự đổi risk policy khi có drift/feedback.
- Chat Copilot tự do, gọi RAG trực tiếp từ frontend, hoặc đưa hướng dẫn không có evidence.
- Quản lý lịch bảo trì, phụ tùng, đơn công việc, phân công người xử lý hoặc gửi
  email/SMS; các API hiện tại chưa định nghĩa những chức năng này.
- CRUD máy, quản lý người dùng, UI upload tài liệu, API chỉnh policy hoặc API
  xem/tải file nguồn; quyền dự kiến và dữ liệu lưu trữ không tự tạo ra các API đó.
- Báo cáo performance drift khi chưa có ground truth đủ tin cậy.

Auth shared deployment và SSE realtime là target bắt buộc; local fixtures có profile riêng.
Nếu mở rộng phạm vi, cập nhật contract, API docs và chức năng liên quan cùng nhau.

## 17. Bản đồ chức năng để triển khai

| Nhóm chức năng | Owner/source hiện hành | System Contracts |
| --- | --- | --- |
| Replay/fault/scenario/control | sensor0.3.0 có ACK202/durable state/control/auth; UI có demo/live client/provider HTTP tests | 5, 33.6, 34 |
| Ingestion ACK202 | ingestion-api | 5, 31–34 |
| DQ/idempotency/inference client/risk | processing | 6–9, 31–34 |
| Model training/inference artifacts | ml-service/training + inference | 7–8, 34 |
| Alert creation/lifecycle/audit/feedback | backend/business-consumer + business-api | 10–12, 34 |
| Latest projections/REST/SSE | backend + frontend | 15–16, 32–34 |
| History/chart/export/archive/retention | processing owner; Business facade/Main UI | 34 + HISTORY-01 |
| Copilot durable jobs/RAG/citations | backend job runner + rag-service | 13–14, 34 + RECOVERY-01 |
| Drift detector/publication/catalog | drift-worker + Business job runner | 25A, 34 + RECOVERY-01 |
| Auth | Business/Sensor/providers theo OIDC/service scopes | 22, 34 |
| Health/metrics/logs | Mỗi runtime; platform ở repo riêng | 19–21, 34 |

Machine specs đang có: six OpenAPIs/schemas/examples baseline1.3.2 trong `contracts/v1.3/`.
Bundle này tự chứa các schema và ví dụ còn hiệu lực. Bộ OpenAPI/tài liệu kiến trúc
v1.1 đã gỡ; code Sensor/Drift chưa tự migrate theo việc dọn tài liệu.

## 18. Phạm vi kế thừa1.3.2 và các kịch bản vận hành

### 18.1. Storage và retraining

Storage Consumer ghi raw receipt batches có topic/partition/offset manifests;
Processed archive được hoãn sau M6; M6 giữ original outcomes trong processing_db. S3 raw là evidence dài hạn, không tự động có nhãn failure mới. Data Preparation kiểm tra quality, nguồn ground truth và version hóa curated dataset (DVC hoặc manifest phù hợp) trước khi Training Pipeline dùng; không auto-retrain mỗi khi một batch được upload. MLflow Server sở hữu metadata DB và S3 artifacts.

### 18.2. Những lỗi phải được hiển thị / giám sát

| Lỗi | Hành vi expected |
|---|---|
| Kafka offline | Ingestion 503/504, local sensor buffer + retry, không trả 202 giả |
| Processing tạm dừng | Ingestion vẫn 202 nếu Kafka khỏe; Processing lag tăng; Storage vẫn archive |
| Storage Consumer/S3 offline | Processing vẫn inference; Storage lag tăng; cảnh báo trước retention expiry |
| Feature INVALID | 202 ingestion → status INVALID; ML call = 0; raw vẫn lưu |
| ML downtime | Event RETRYING/FAILED/UNKNOWN; không tác động tới raw archive |
| Alert consumer restart | Same signal có thể replay nhưng không tạo alert/alert_event trùng |
| Drift CronJob lỗi | Báo monitoring stale/error; online ingestion/inference/alert không phụ thuộc |
| RAG lỗi | Alert vẫn đọc/ACK/resolve được; brief báo UNAVAILABLE |

### 18.3. Giới hạn đánh giá

AI4I 2020 không cung cấp telemetry time-series thực của mỗi máy; simulator gán machine/time để kiểm thử kiến trúc. MaintAI có kiến trúc scale-ready với baseline100 máy×1 reading/s. KhiX>100, tính lại capacity và tối ưu latency từng stage; benchmark đo năng lực của cấu hình triển khai, không đặt trần100 máy cho kiến trúc.

### 18.4. Đồng bộ triển khai

Machine-readable baseline1.3.2 có ở `contracts/v1.3/`; Compose/GitOps và
application code không tự được triển khai từ Markdown. Mỗi implementation PR cần
provider/consumer validation và migrations theo System Contracts1.4, gồm compatibility với baseline.


---

## 19. Đặc tả Frontend UI v1.3 — theo phương án C được người dùng chọn

### 19.1. Information Architecture / Điều hướng

**React Web App** với sidebar cố định trên desktop, menu thu gọn trên mobile. Các trang chính: **Fleet Dashboard**, **Machine Detail**, **Alerts**, **Alert Detail + Technician Copilot**, **Monitoring / Drift**, và phần **System Health** (có thể tích hợp trong Monitoring). Header có trạng thái SSE (`Connected`, `Reconnecting`, `Offline`), last sync timestamp, role/user. `VIEWER` xem; `TECHNICIAN` thao tác alert/feedback/brief; `ADMIN` theo quyền contract, không tự thêm trang quản trị cấu hình ngoài scope.

```text
+--------------------- MaintAI Header: Connection / Last synced ------------------+
| Sidebar             | Main content                                          |
| Fleet Dashboard     | Machine M-001 / Device health                        |
| Machines            | +-------------------------+ +------------------------+ |
| Alerts              | | LIVE TELEMETRY          | | LATEST VALID PREDICTION| |
| Monitoring / Drift  | | Air Temp: 298.1 K       | | NORMAL · 12%           | |
|                     | | RPM: 1,500              | | Based on event R2      | |
|                     | | Received R4 · 0.1s ago  | | Model v1 · age 3s      | |
|                     | | Raw / verifying         | | Updating / delayed     | |
|                     | +-------------------------+ +------------------------+ |
|                     | Timeline: sensor readings + predictions + alerts    |
|                     | Recent alerts / maintenance actions                 |
+---------------------+----------------------------------------------------------+
```

### 19.2. Màn hình Fleet Dashboard

- Summary cards: tổng máy, NORMAL/WATCH/HIGH/CRITICAL, INVALID/UNKNOWN, active alerts, số máy có stale prediction; không lấy current raw telemetry để tự kết luận máy NORMAL.
- Table: machine ID, current backend operational state, last telemetry timestamp, prediction age/source event, latest failure probability (kèm stale indicator), open alerts, action “View”. Sort/filter/search/pagination.
- SSE `telemetry.received` chỉ đổi last telemetry / online freshness cho đúng máy; `prediction.completed` mới đổi probability/latest prediction nếu ordering token mới hơn; `alert.*` đổi alert counts.
- Empty/loading/API unavailable/connection reconnect có trạng thái riêng, không có spinner vĩnh viễn.

### 19.3. Màn hình Machine Detail — màn quan trọng nhất của realtime UX

- Header: machine ID, machine metadata từ Business API, latest **processed operational state** và active alert badges.
- **Card Live Telemetry**: product type, air/process temperature (Kelvin, đổi °C nếu UI hỗ trợ và ghi đơn vị), RPM, torque, tool wear; label `Latest received reading`, `event_id`, `server_accepted_at`, age tính phía client, DQ state `UNVERIFIED/VALID/INVALID` ở lớp UI; dữ liệu invalid hiển thị lỗi nhưng không gửi inference.
- **Card Latest Valid Prediction**: classification, failure probability, model + risk policy version, source_event_id, source reading timestamp/age, inference completed_at, freshness tag. Nếu reading R4 đến mà prediction R2 chưa được thay, giữ R2 nhưng hiển thị `Based on older data`.
- **Status strip**: ingestion/processing lag, last processing result (PENDING/RETRYING/INVALID/FAILED), SSE connection status, latest arrival vs latest prediction source; tránh làm người dùng hiểu `PENDING` liên tục là lỗi.
- **Timeline**: sensor arrival, DQ result, prediction/risk, alert markers theo event lineage; nếu prediction cũ đến trễ không được vẽ như reading mới. Tabs: `Readings`, `Predictions`, `Alerts` (có thể gộp timeline MVP).
- CTA: `View active alerts`, `Open alert details`; tác vụ acknowledge/resolve chỉ ở Alert Detail.

### 19.4. Màn hình Alerts / Alert Detail + Copilot

Alerts list: tabs All/Open/Acknowledged/In Progress/Escalated/Resolved/False Alert; severity, machine ID, detected time, risk/prediction source event, assigned technician nếu scope có. Alert Detail có lifecycle timeline, source event ID, probability/model/policy, sensor snapshot đúng event gây alert (không dùng telemetry đang live thay thế), actions theo chuyển trạng thái hợp lệ (ACK, START, ESCALATE, RESOLVE, FALSE_ALERT), notes/feedback, audit. Panel **Technician Copilot** hiển thị brief, checklist, citations/manual pages, `PENDING/AVAILABLE/UNAVAILABLE/INSUFFICIENT_EVIDENCE/VALIDATION_FAILED`; RAG lỗi không chặn xem và xử lý alert.

### 19.5. Monitoring / Drift & System Health

Monitoring tiles: Kafka lag Processing / Telemetry projector / Storage / Prediction projector, ingestion ACK p95, ML inference p95, SSE connected clients, telemetry freshness, prediction source age distribution, S3 archive lag; machine-readable metrics đến từ backend monitoring facade hoặc Grafana link, **không** cần đưa Prometheus trực tiếp ra browser. Drift panel hiển thị model/reference version, report computed_at, sample_count, STABLE/WARNING/CRITICAL/INSUFFICIENT_DATA/ERROR và stale worker checkpoint. `DATA_DRIFT` **không** thay trực tiếp state máy hay cảnh báo bảo trì.

### 19.6. UX states và acceptance scenarios

| Kịch bản | Live Telemetry | Latest Prediction | Thông điệp phải có |
|---|---|---|---|
| Normal R1 realtime | R1 · received now | R1 (sau inference) | FRESH |
| ML chậm R4 tới, R2 mới xong | R4 · 0.1s ago | R2 · 12% | “Based on R2”, Updating/Delayed theo ngưỡng |
| R1 đến muộn sau R2 prediction | Giữ telemetry R2 hoặc newer | Giữ prediction R2 | Không rollback UI; R1 lưu history |
| Current reading INVALID | Hiện raw + issues | Prediction hợp lệ trước (nếu có) | Current data INVALID, prediction historical |
| ML Service unavailable | Telemetry vẫn chạy | Last valid nếu có, kèm age | Prediction unavailable/delayed; không map NORMAL |
| SSE disconnected | Giữ snapshot cuối, connection Reconnecting | Giữ snapshot có timestamp | REST resync sau reconnect |
| Chưa có prediction | Có thể có telemetry | Empty / Not available | Không hiện 0% hay NORMAL giả |
| Alert RAG lỗi | Telemetry/prediction không đổi | Không đổi | Alert và actions vẫn hoạt động |

Ngưỡng UX mẫu (cấu hình): source age `<2s` FRESH, `2–5s` DELAYED, `>5s` STALE. Không coi đây là SLO production; phối hợp replay rate và benchmark. Telemetry cập nhật khi Kafka accept, không chờ Processing, nhưng không tuyên bố một raw record đã qua validation.

### 19.7. API/Data ownership cho FE

`GET /api/v1/machines`, `GET /api/v1/machines/{id}/live`, `GET /api/v1/machines/{id}/sensor-readings`, `GET /api/v1/alerts`, `GET /api/v1/alerts/{id}`, `GET /api/v1/alerts/{id}/brief`, `GET /api/v1/drift-reports`, `GET /api/v1/monitoring/summary`, `GET /api/v1/events/stream` từ **Business API/facade**. Browser không truy cập Kafka, Processing DB, ML/RAG hay Drift DB trực tiếp. Frontend có thể kết nối SSE một lần, filter machine tùy screen, trong khi provider enforce permissions. Khi nhiều API replicas cần fan-out đúng đến tất cả SSE connections.


---

## 20. Simulator UI riêng và các trục trạng thái (1.3.2)

**Trang/mục Simulator Control** (demo): danh sách máy, scenario, trạng thái `RUNNING/PAUSED`, nút `Pause`/`Resume`, `Select Scenario`, thời điểm command đã xác nhận; chỉ người có quyền demo control mới dùng được. Simulator UI riêng gọi Sensor Control API trên same-origin management route; Main UI chỉ gọi Business và không điều khiển PLC thật.

**Luồng vận hành demo:** Normal → Degradation → Risk HIGH/CRITICAL → Alert OPEN → Technician ACK → **Pause** M-001 để mô phỏng dừng telemetry trong lúc bảo trì → Alert IN_PROGRESS → Resolve bằng technician action → chọn Recovery scenario → Resume → readings mới → ML quyết định risk. Pause **không** tự động ACK/RESOLVE; NORMAL reading mới **không** tự RESOLVE alert. AI4I 2020 không có chuỗi bảo trì thực, scenario recovery là mô phỏng có chủ đích, không là dữ liệu ground truth.

**Hai giao diện có trách nhiệm riêng**; Main Machine Detail không có nút điều khiển Simulator:

| Card/panel | Nội dung và nguồn | Khi PAUSED/lag/invalid |
|---|---|---|
| Simulator UI riêng | `RUNNING/PAUSED`, scenario, last command acknowledged; Sensor Control API từ Simulator UI riêng | `PAUSED` dù in-flight readings còn đến; phân biệt `STALE` do không thấy telemetry |
| Live/Last Telemetry | `sensor.readings` → Business telemetry projection → SSE; 6 fields, unit, event_id/sequence/accepted_at | `Last Telemetry` khi no fresh data, `UNVERIFIED` tới lúc có validation; không giả là máy còn hoạt động |
| Latest Valid Prediction | Processing → `inference.results` → Business prediction projection; model, probability, source event, source age | Giữ kết quả trước nhưng `Historical/Delayed/Stale`, không ghi đè bằng invalid/old result |
| Active Alerts/Technician actions | `alert.signals` → Business lifecycle | Alert OPEN kể cả Prediction NORMAL hoặc simulator PAUSED; chỉ technician action thay state |

**Lỗi sensor:** Request đã auth, đúng size policy nhưng JSON/envelope sai → publish
`sensor.rejected`, đợi broker ACK rồi trả400/422; local rejected log bổ trợ và
Simulator tiếp tục record sau. Publish không được xác nhận →503/504 retryable.
Không archive raw vô điều kiện cho request unauthenticated/quá lớn. Feature INVALID
nhưng envelope hợp lệ →202 sau accepted-topic ACK, Processing INVALID, ML call=0.

**Business database ownership:** Không lưu mỗi raw event vào `business_db`; chỉ latest snapshot per machine (telemetry, last valid prediction, latest processing), alerts/feedback/brief. Raw history đi S3/MinIO; inference history M6 qua Processing Read API đọc processing_db, processed archive hoãn. SSE do projector event/notification kích hoạt chứ không cần full raw DB. Nếu nhiều Business API Pods: Business Consumer commit read model rồi broadcast notification bằng PG LISTEN/NOTIFY (MVP) hoặc Redis pub/sub; REST snapshot khôi phục khi miss.

**Simulator Control state API:** `GET /api/v1/simulator/machines/{id}/control`, `POST .../pause`, `POST .../resume`, `PUT .../scenario`; auth demo-only. `PAUSED` là command-state có source, không suy từ thiếu event; `STREAMING` không xác nhận máy công nghiệp hoạt động; FE tránh nhập nhằng connection state, risk và alert lifecycle.

**Functional acceptance:** Pause M-001 không làm ngừng M-002; cùng lúc backlog vẫn xử lý; resume phát new event_id/sequence tăng đơn điệu, không reset qua restart; control timeout không giả success; UI hiển thị `Last Telemetry` và prediction historical; alert vẫn active cho đến khi technician resolve.


## 21. Chức năng bổ sung đã chốt — amendment1.3.2

- Machine History có raw readings, terminal outcomes, point lookup và chart
  min/max/avg/count, source IDs/model versions, null gaps và explicit coverage.
- M6: outcomes giữ DB, raw qua S3/MinIO catalog; chưa processed archive hoặc cleanup
  source data. Snapshot/export nằm trong Read API, quyền ghi chỉ metadata riêng.
  Profile30d/365d chưa bật; không tái tính model để thay lịch sử.
- Export bất đồng bộ có job status, authorization, artifact expiry và download.
- Copilot jobs bền vững, retry/lease/fencing; failure không cản xử lý alert.
- Drift metadata catalog đồng bộ từ S3 report manifests, UI có catalog freshness;
  không đọc drift_db hoặc suy ra STABLE từ missing report.
- Simulator UI riêng gọi Sensor, không phải một tab điều khiển qua Business.
- Recovery bao gồm shadow projection rebuild/reconciliation, rolling schema và
  capacity acceptance; đây là chức năng/đặc tả cần implement, chưa phải code chạy.

Xem [normative decisions](README.md),
[history contract](history-storage-contract.md), [recovery/jobs](recovery-jobs-operations.md)
và [readiness](implementation-readiness.md).

## 22. Chức năng bổ sung v1.4 — Deployment và provenance

- **Quarantine trung tâm:** Lưu request sai schema được phép giữ qua `sensor.rejected`
  và S3 sanitized archive. Tra cứu có quyền theo source/request/time; không đòi
  machine_id khi payload thiếu trường này. Kafka ACK và trạng thái archive là hai việc riêng.
- **History độc lập:** Main UI → Business → Processing Read API; SELECT original data, ghi metadata quyền hẹp; lịch sử
  alert/actions/feedback vẫn do Business phục vụ. Cold lookup dùng index/partition/
  manifest có giới hạn. Filters/cursor v1.4 phải freeze cùng OpenAPI trước khi code.
- **Truy vết prediction/alert:** Liên kết reading/Kafka locator, DQ rules digest,
  dataset/split, MLflow run, model/preprocessor, release/image và risk policy thực tế.
  Kết quả cũ giữ nguyên lineage khi rollback; không bịa hash để backfill dữ liệu cũ.
- **Truy vết RAG/Drift:** Brief giữ đúng manual/chunk/prompt/embedding version và
  citations; report giữ model/reference/detector/window identity. Reindex không phá citation cũ.
- **Phát hành kiểm soát:** Approved dataset → train/evaluate/register → review
  release manifest → PR GitOps pin digest → rollout/readiness hoặc rollback. Raw
  upload và drift signal không tự approve/retrain/promote.

Đây là chức năng mục tiêu. Artifact/schema/code còn thiếu được ghi trong readiness;
không tự thêm endpoint UI hoặc coi diagnostics/provenance đã có implementation.

## 23. Nhiều máy hoạt động đồng thời — PROC-ORDER-01

Thiết kế23/10 tại [Processing concurrency/ordering](PROCESSING_CONCURRENCY_AND_ORDERING.md)
cho các máy xử lý độc lập hơn: inference máy A chậm không buộc mọi máy khác
chờ cùng handler. Readings của **một máy** vẫn lần lượt DQ/ML/risk và lưu outcome;
khác máy được chạy đồng thời trong budget. Không cần một partition/service mỗi máy.

Với người dùng, máy B có thể cập nhật prediction trước A; đó là hành vi đúng.
Màn hình vẫn tách số đo mới nhất, trạng thái xử lý mới nhất và prediction hợp lệ
mới nhất theo từng máy. INVALID giữ prediction lịch sử có nhãn/tuổi; Alert mở
vẫn cần lifecycle xử lý, không tự biến mất vì input mới NORMAL. Prediction risk
và Alert có thể tới khác thời điểm vì khác topic/handler.

Với vận hành, queue đầy hoặc ML/DB chậm gây backlog/stale có thể quan sát; hệ
thống không bỏ record hoặc xác nhận thành công giả để UI trông realtime. Offset
coordinator/ordered relay nằm trong các runtimes Processing hiện có, không thêm
UI hoặc chức năng điều khiển mới. Đây là target cần code/test theo Plan §K;
SDK transport đã có code/tests; capacity baseline100/s và mở rộngR=X×r vẫn cần đo app thực.

## 24. Quy mô ban đầu100 máy và khả năng mở rộng

Giả định vận hành100 máy, mỗi máy1 reading/s; khi mọi máy RUNNING có100 readings/s.
Main UI quan sát fleet và chi tiết theo quyền; Simulator UI điều khiển luồng demo.
5 máy là smoke test/default source hiện có, không thay baseline100 máy đã chốt.

Hệ thống scale-ready: khiX>100, vẫn giữ flow/ownership nhưng tínhR=X×r và tối ưu
source, ML, DB, Relay/Storage, Business/SSE theo stage latency/lag và tài nguyên.
100 không là maximum machine count. Mục tiêu alert hiển thị p95≤2s (không gồm LLM)
cần đo tới browser theo workload thực; nhiều máy hơn không bắt UI vẽ mọi reading
trung gian hoặc tạo thêm một kết nối SSE cho mỗi máy. [Cơ sở và công thức](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
