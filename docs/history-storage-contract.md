# HISTORY-01 — Lịch sử DB/S3, archive và retention

> **M6 override29/10/2026:** [M6-HISTORY-01](M6_HISTORY_ARCHITECTURE_DECISION.md) ưu tiên phần history/snapshot/export bên dưới: Read API ghi metadata qua pool riêng, runner export trong Read API; processed archive và source cleaner hoãn sau M6. Các thuật toán archive/retention bên dưới là thiết kế tương lai, không là task M6. Brief/recovery ngoài phạm vi này giữ nguyên.

Chi tiết thiết kế kế thừa1.3.2, rà soát khi nhập target1.4 ngày2026-10-19.
Processing sở hữu logical processing history; Business chỉ facade. Giữ thuật toán
archive/coverage/export còn cần, áp dụng khi không trái System Contracts v1.4 §36
và [quyết định dự án](README.md). Cursor, retention và writer cho snapshot/export
phải đối chiếu các khác biệt trong [readiness](implementation-readiness.md).

## 1. Dữ liệu và retention M6

| Dữ liệu | Nơi giữ | Quyết định M6 |
| --- | --- | --- |
| Latest UI + alerts/audit/feedback | business_db | Giữ ownership hiện có; không full raw history |
| Terminal processing outcomes | processing_db | Giữ original outcomes; không archive S3 hoặc cleanup thật |
| Raw accepted/rejected receipts | S3/MinIO raw | Storage giữ object/manifest + catalog/coverage riêng |
| Snapshot/catalog index/export jobs | processing_db history_meta | Read API ghi metadata quyền hẹp, pool riêng |
| History query snapshot | Metadata + immutable membership/refs | TTL10 phút; không giữ SQL transaction dài |
| Export artifact | Private exports S3/MinIO | TTL tải24h; URL ký<=5 phút, kiểm quyền lại |

Profile30d hot/365d raw/processed là ý tưởng retention tương lai chưa được bật.
M6 giữ retention_enabled=false, cleaner_dry_run=true; chưa có processed archive
thì không được xóa outcomes vì đủ tuổi. Hết snapshot/job TTL không xóa source
history, evidence hoặc artifact model/policy còn được tham chiếu. Demo DB-only
không cần object store; full raw history/export job dùng MinIO local, không cần AWS.

## 2. Logical processing record

History record bất biến gồm `result_id`, `source_event_id`, `machine_id`, seq/epoch,
source received/accepted times, occurred/completed times, result kind/status,
validation issues hoặc failure code, completed prediction/probability/threshold,
model/preprocessor/dataset/policy versions, risk snapshot và raw reference.
Features đầy đủ hoặc reference phải cho phép lấy đúng nguồn gốc; raw reference
gồm object version/checksum và event locator, không chỉ một URL có thể bị đổi.
History giữ original outcomes, không tính lại bằng model mới để thay lịch sử.
Raw ref có thể null/PENDING_ARCHIVE khi chưa có object/catalog; không bịa URL/version.
Invalid/failed outcomes cũng được giữ trong DB; ML fields null đúng terminal schema.
Processed archive là phần sau M6, không phải trạng thái đã có.

Business lưu một `alert_evidence_snapshot` bất biến khi tạo alert: source event,
result/prediction IDs, six input features nếu VALID, probability, risk, versions,
timestamps. Đây là bằng chứng nghiệp vụ có giới hạn theo alert, không phải full
raw history. Alert còn mở/hold không mất evidence khi hot rows bị cleanup.

## 3. Writer snapshot/export đã chọn cho M6

Read API sở hữu snapshot/export và metadata catalog mirror; không chuyển việc này
vào Processing Worker hoặc thêm processing-maintenance. Read API dùng processing_ro
cho original data với read-only guard, và credential giới hạn trên history_meta
cho snapshots/members/jobs/catalog entries. Không có quyền ghi source/outcome/risk/
inbox/outbox, không DDL hoặc cross-owner SQL. Owner migration/grants do Người1.

Export runner bền vững chạy cùng runtime Read API với concurrency/pool/quota riêng;
POST chỉ trả202 sau job commit. Hai API replicas claim bằng lease/fencing; crash
phục hồi từ DB, không từ request-local tasks. Full rules ở M6-HISTORY-01 §4–5.

## 4. Raw catalog và phần archive hoãn

Storage Consumer giữ raw object/manifest protocol hiện có, bổ sung immutable
catalog generations/segments + per-partition coverage theo M6-RAW-01. Read API
sync metadata và query index, không scan bucket trong mỗi HTTP request. Catalog
lag riêng với raw commit; published offset/timestamp không tự chứng minh COMPLETE.

M6 chưa upload original processed outcomes xuống S3, chưa có processed cold lookup
hoặc VERIFIED/CLEANED batch state. Nếu làm sau M6 phải có quyết định version mới,
writer/membership/checksum/lease/pins/recovery và retention approval trước cleanup.
Không dùng optional outcome wire archive của Storage làm bản thay thế DB history.

## 5. API history public và internal

Public owner Business; internal owner Processing. Public routes gọi internal
tương ứng với service identity + authorized machine scope, không forward token
browser làm service credential và không SQL cross-owner.

| Method / public `/api/v1` | Internal `/api/v1/internal` | Kết quả |
| --- | --- | --- |
| GET /machines/{machine_id}/predictions | /machines/{machine_id}/predictions | HistoryPage |
| GET /predictions/{result_id} | /predictions/{result_id} | HistoryRecord |
| GET /machines/{machine_id}/sensor-readings | /machines/{machine_id}/sensor-readings | SensorHistoryPage, gồm raw features + processing link khi có |
| GET /machines/{machine_id}/timeseries | /machines/{machine_id}/timeseries | TimeSeriesPage |
| POST /history-exports | /history-exports | CSV nhỏ HTTP streaming hoặc202 ExportJob sau durable commit |
| GET /history-exports/{job_id} | /history-exports/{job_id} | ExportJob, không tự chạy job |
| GET /history-exports/{job_id}/download | /history-exports/{job_id}/download | 303 private signed URL sau authorization |

History query: `from`, `to` RFC3339 UTC (inclusive/exclusive), default last 1h,
`limit` default100/max500, `cursor`, optional result_kind/model_version. Sorting
descending `(processed_at,result_id)`, processed_at map completed_at hiện có; ổn định khi timestamp trùng.
`prediction` history trả tất cả terminal outcomes; tên endpoint không ngầm lọc
chỉ VALID. `result_kind` cho phép UI lọc completed predictions khi cần.
Sensor history là receipt history, có receipt_id ổn định từ topic/partition/offset;
sort descending `(received_at,topic,partition,offset)`, giữ duplicate/conflict deliveries
để audit, khác canonical processing history sort theo result_id.
Sensor history lấy raw archive/catalog theo receipt; không biến raw
chưa Processing thành VALID. Raw và processing snapshots có lineage riêng.

First page tạo `snapshot_id`, `as_of`, catalog_generation và bounded immutable
DB membership. Response có `items`, `next_cursor`, `snapshot_id`, `as_of`,
`expires_at`, `coverage` (COMPLETE), `tiers_used` (HOT cho outcomes; ARCHIVE cho raw), available_from/available_through.
Snapshot pin committed rows/catalog generation/raw versions; source không cleanup trong M6.
DB membership được materialize tối đa100000 IDs/snapshot; budget lớn hơn trả
422 QUERY_TOO_LARGE kèm gợi ý giảm range hoặc tạo history export. Range tương tác
tối đa31 ngày; export tối đa365 ngày trong retention. Không giữ SQL transaction
mở suốt 10 phút để giữ snapshot. Late results sau snapshot chỉ xuất hiện khi refresh.

Cursor opaque, ký HMAC, ràng buộc snapshot/query hash/subject/machine/sort/last key,
không nhận bucket/key tùy ý từ client. Mỗi page vẫn kiểm tra quyền. Cursor sai
400 INVALID_CURSOR; hết hạn410 SNAPSHOT_EXPIRED. Không dùng offset pagination.
Request vượt dữ liệu đã expire trả410 HISTORY_EXPIRED với available_from;
range đảo/field sai422; resource thật sự không tồn tại404.

Raw snapshot công bố catalog generation + safe offset coverage từng partition.
Không suy time cutoff từ max(received_at); available_through nullable nếu chưa có
bằng chứng theo clock query. Strict request vượt scope đã verify trả503 với reason
PENDING_ARCHIVE/CATALOG_LAG/GAP, không200 thiếu dữ liệu. Late receipts xuất hiện
khi refresh. Xem M6-RAW-01 về frontier/log gaps và source-time completeness.

Nếu một tầng cần đọc không sẵn sàng, trả503 HISTORY_UNAVAILABLE, không trả200
danh sách thiếu như đủ dữ liệu. Empty200 chỉ khi coverage COMPLETE và không có
record. Point lookup outcome đọc processing_db trong M6, raw details đọc catalog/ref; chưa materialize
processing status trả404 EVENT_NOT_MATERIALIZED, khác expired/not found.

## 6. Chart aggregation

Timeseries query thêm `metric`, `resolution` (auto/1s/10s/1m/5m/1h/1d),
`max_points` default1000/max2000. Metrics allowlist: năm numeric sensor features
và failure_probability/risk_score; product_type không được tính numeric average.
Raw invalid numeric bị loại khỏi numeric aggregate nhưng trả invalid_count;
không clip/impute thành valid. Prediction probability chỉ lấy completed valid.

Buckets UTC `[start,end)` trả start/end/min/max/avg/count/invalid_count; bucket
không sample trả null values, không nội suy. Nếu explicit resolution vượt
max_points ->422; auto chọn resolution đủ budget. Response giữ unit, source
metric, snapshot/as_of, coverage và markers cursor riêng để không cắt mất alerts.
UI overlay alert/evidence đúng source time, phân biệt chart bucket và event gốc.
Không dùng chart aggregates làm input ML/Drift. Raw object query vượt thời gian
interactive trả503 QUERY_TIMEOUT hoặc yêu cầu export, không chạy vô hạn trong API.

## 7. Export và retention recovery

Export job: QUEUED -> RUNNING -> SUCCEEDED / FAILED / EXPIRED. Idempotency-Key
ràng buộc actor + canonical query, cùng key khác query ->409. Frozen query scope,
raw catalog generation khi dùng raw, snapshot và authorized machines được lưu khi accept.
Nếu cần hơn100000 DB IDs, export runner của Read API tạo snapshot theo chunk với durable
membership và pins; nó có pool riêng, không chiếm pool dashboard. Không bỏ giới
hạn bằng cách chạy query khổng lồ trong HTTP handler.

Download trước success ->409 EXPORT_NOT_READY; sau artifact TTL ->410;
không có quyền ->403; job không tồn tại404. Quyền phải được kiểm tra lại lúc
download, không chỉ lúc tạo. Export không resolve alert hoặc kích hoạt training.

M6 không chạy source-data cleanup; cleaner chỉ dry-run trong Read API codebase.
Không có processed archive VERIFIED thì không xóa outcomes. Raw object/catalog hỏng
phải báo lỗi/UNKNOWN, dừng công bố COMPLETE; giữ DB history và evidence. Dọn export
artifact riêng chỉ theo TTL/pins/policy, không dùng rule đó cho raw hoặc outcomes.

## 8. Acceptance cần có khi implement

- DB outcome pages và raw catalog pages có timestamp trùng: không duplicate/missing.
- Late terminal commit không lọt snapshot cũ; refresh mới thấy kết quả đó.
- Export upload/crash/lease/retry: một canonical job output, không partial success.
- SQL tests chứng minh metadata writer không sửa outcome/inbox/outbox; Worker không export.
- Archive unavailable: explicit503; expired history410; empty complete200.
- Invalid/failed giữ null ML; evidence alert/source data không bị cleanup M6.
- Query/exports re-check role/machine scope; cursor không dùng sang user khác.
- Chart spike còn trong max; gaps null; không đổi prediction/alert từ aggregates.

Export Parquet dùng record schema1.0.0 (nested outcome, ZSTD); CSV UTF-8 có header,
flatten scalar outcome fields, features/errors/risk nested thành JSON trong quoted
columns. Export manifest giữ query/snapshot/schema và checksum các parts. Buckets
versioning phải bật để ObjectRef có version_id thật; không dùng giả version/ETag.

## 9. PROC-ORDER-01 — history khi outcomes hoàn thành song song

Theo [quyết định concurrency/ordering](PROCESSING_CONCURRENCY_AND_ORDERING.md),
Processing có thể hoàn tất máy B trước A dù A có Kafka offset thấp hơn. Lưu
source sequence/locator và completed_at riêng, không dùng completion order làm
source order hoặc thay history pagination cursor đã freeze. History vẫn giữ
mọi logical outcome; unique identity ngăn duplicate receipt tạo bản ghi logic mới.

Offset coordinator chỉ advance safe completed prefix sau DB durability;
raw catalog/coverage vẫn cần verified membership riêng; processed archive hoãn.
Frontier chưa tiến không
chứng minh các outcomes sau hole chưa có trong DB. Raw Storage giữ từng receipt
và runner/microbatch hiện hành; không đổi batching hoặc bỏ archive theo UI
coalescing. Ordered outbox không thay snapshot/catalog/coverage rules của M6-HISTORY-01.
