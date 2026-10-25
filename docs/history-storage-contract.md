# HISTORY-01 — Lịch sử DB/S3, archive và retention

Chi tiết thiết kế kế thừa1.3.2, rà soát khi nhập target1.4 ngày2026-10-19.
Processing sở hữu logical processing history; Business chỉ facade. Giữ thuật toán
archive/coverage/export còn cần, áp dụng khi không trái System Contracts v1.4 §36
và [quyết định dự án](README.md). Cursor, retention và writer cho snapshot/export
phải đối chiếu các khác biệt trong [readiness](implementation-readiness.md).

## 1. Ba tầng dữ liệu và profile khởi đầu

| Dữ liệu | Nơi giữ | Profile thiết kế, cấu hình được |
| --- | --- | --- |
| Latest UI + alerts/audit/feedback | business_db | Latest theo máy; business retention riêng, không bị hot-history cleanup |
| Processing terminal outcomes | processing_db | Hot 30 ngày theo thời điểm hoàn tất; không xóa pending/retrying |
| Raw accepted envelopes | S3 raw | 365 ngày, không coi raw là kết quả inference gốc |
| Terminal processing archive | S3 processed | 365 ngày, daily export; lag target <=24h |
| History query snapshot | Processing temporary state/artifact | TTL 10 phút, pin dữ liệu được tham chiếu |
| Export artifact | Private S3 exports | TTL 24h; download authorization mỗi lần, URL ký tối đa 5 phút |

Đây là profile baseline1.3.2 để tham khảo; v1.4 chưa phê duyệt thời hạn retention
cụ thể theo yêu cầu dữ liệu/ngân sách. Không coi các con số là policy production
đã chốt hoặc số liệu dung lượng/độ trễ đã đo.
DB có thể giữ lâu hơn 30 ngày khi archive lỗi hoặc còn pin; tuyệt đối không xóa
chỉ vì đủ tuổi. S3 lifecycle chỉ được expire batch sau hạn và khi không còn pin
hoặc evidence hold. Không xóa artifact model/preprocessor/policy đang được history
tham chiếu mà chưa có chính sách giữ version tương ứng.

## 2. Logical processing record

History record bất biến gồm `result_id`, `source_event_id`, `machine_id`, seq/epoch,
source received/accepted times, occurred/completed times, result kind/status,
validation issues hoặc failure code, completed prediction/probability/threshold,
model/preprocessor/dataset/policy versions, risk snapshot và raw reference.
Features đầy đủ hoặc reference phải cho phép lấy đúng nguồn gốc; raw reference
gồm object version/checksum và event locator, không chỉ một URL có thể bị đổi.
Archive giữ original outcomes, không tính lại bằng model mới để thay lịch sử.
Invalid/failed outcomes cũng được archive, ML fields null như terminal schema.

Business lưu một `alert_evidence_snapshot` bất biến khi tạo alert: source event,
result/prediction IDs, six input features nếu VALID, probability, risk, versions,
timestamps. Đây là bằng chứng nghiệp vụ có giới hạn theo alert, không phải full
raw history. Alert còn mở/hold không mất evidence khi hot rows bị cleanup.

## 3. Archive writer đã chọn

MVP chọn **Processing-owned batch export từ processing_db**, không thêm topic
prediction archive và không giả định `inference.results` chứa mọi cột history.
Storage Consumer giữ nhiệm vụ raw archive. Processing Worker có maintenance loop
riêng, cùng DB owner, không giữ consumer polling thread trong lúc export S3.

1. Sau terminal commit, maintenance chọn unassigned immutable records theo batch
   có giới hạn, gán duy nhất archive_batch_id và immutable membership trong DB.
2. Claim batch bằng lease/fencing; đọc tập thành viên đã đóng. Result đến muộn
   vào batch tiếp theo; không dùng timestamp watermark để bỏ qua late commits.
3. Ghi Parquet part files theo schema_version/event_date; target <=128 MiB/file.
   Immutable keys gồm batch_id + part number, không ghi một object mỗi record.
4. Đọc/kiểm tra object version, SHA-256, row count, membership digest và ID coverage.
5. Publish immutable committed manifest sau khi mọi part đã có và kiểm tra đủ.
6. Ghi VERIFIED + manifest reference trong DB với lease fencing token. Nếu mất
   ACK sau publish, retry tìm manifest/batch ID rồi reconcile, không tạo batch mới.
7. Cleaner chỉ xử lý thành viên VERIFIED đủ tuổi và không pin/hold. Xóa theo
   manifest membership; chỉ drop partition khi partition đã đóng, mọi row đủ
   điều kiện và không còn writer/transaction có thể thêm row vào partition ấy.

States: PLANNED -> UPLOADING -> VERIFIED -> CLEANED; lỗi retry giữ nguyên batch ID.
Lease hết hạn được reclaim với fencing token tăng; stale runner không được cập
nhật VERIFIED/CLEANED. Upload parts nhưng chưa manifest chỉ là orphan chưa đọc.
Orphan cleanup sau grace period phải kiểm tra batch/lease và manifest references.

## 4. Layout và manifest

```text
raw-sensor/schema=1.3.0/topic=.../partition=.../offsets=...jsonl.gz
processed/schema=1.0.0/event_date=YYYY-MM-DD/batch=<id>/part-000.parquet
processed-manifests/committed/YYYY-MM-DD/<batch_id>.json
history-exports/<job_id>/<artifact_id>.parquet
```

Manifest schema 1.0.0 có `batch_id`, `created_at`, `record_schema_version`,
`row_count`, `membership_sha256`, `min/max_source_server_accepted_at`,
`min/max_occurred_at`, immutable `members_index` reference và `objects[]`.
Mỗi reference có bucket/key/version_id/sha256/size_bytes; ETag không thay SHA-256.
Members index liệt kê result IDs và event locators; phục vụ coverage, delete và
point lookup. Catalog generation là tập manifest đã commit bất biến, không chỉ
`latest.json`. Catalog publishing dùng compare-and-set generation; readers có thể
pin generation cũ trong TTL. S3 paths/publication dates không thay event time.

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
| POST /history-exports | /history-exports | 202 ExportJob sau durable commit |
| GET /history-exports/{job_id} | /history-exports/{job_id} | ExportJob, không tự chạy job |
| GET /history-exports/{job_id}/download | /history-exports/{job_id}/download | 303 private signed URL sau authorization |

History query: `from`, `to` RFC3339 UTC (inclusive/exclusive), default last 1h,
`limit` default100/max500, `cursor`, optional result_kind/model_version. Sorting
descending `(source_server_accepted_at,result_id)`, ổn định kể cả cùng timestamp.
`prediction` history trả tất cả terminal outcomes; tên endpoint không ngầm lọc
chỉ VALID. `result_kind` cho phép UI lọc completed predictions khi cần.
Sensor history là receipt history, có receipt_id ổn định từ topic/partition/offset;
sort descending `(server_accepted_at,receipt_id)`, giữ duplicate/conflict deliveries
để audit, khác canonical processing history sort theo result_id.
Sensor history lấy accepted raw archive và hot raw references; không biến raw
chưa Processing thành VALID. Raw và processing snapshots có lineage riêng.

First page tạo `snapshot_id`, `as_of`, catalog_generation và bounded immutable
hot membership. Response có `items`, `next_cursor`, `snapshot_id`, `as_of`,
`expires_at`, `coverage` (COMPLETE), `tiers_used` (HOT/ARCHIVE), available_from/available_through.
Snapshot pin hot rows và manifests; chuyển tầng trong TTL không làm mất bản ghi.
Hot membership được materialize tối đa100000 IDs/snapshot; budget lớn hơn trả
422 QUERY_TOO_LARGE kèm gợi ý giảm range hoặc tạo history export. Range tương tác
tối đa31 ngày; export tối đa365 ngày trong retention. Không giữ SQL transaction
mở suốt 10 phút để giữ snapshot. Late results sau snapshot chỉ xuất hiện khi refresh.

Cursor opaque, ký HMAC, ràng buộc snapshot/query hash/subject/machine/sort/last key,
không nhận bucket/key tùy ý từ client. Mỗi page vẫn kiểm tra quyền. Cursor sai
400 INVALID_CURSOR; hết hạn410 SNAPSHOT_EXPIRED. Không dùng offset pagination.
Request vượt dữ liệu đã expire trả410 HISTORY_EXPIRED với available_from;
range đảo/field sai422; resource thật sự không tồn tại404.

Raw history snapshot chỉ cover đến durable raw archive/hot watermark; unarchived
accepted events vẫn có thể chỉ xuất hiện trong live snapshot. available_through
phải phản ánh cutoff, strict request vượt coverage503 thay vì200 thiếu dữ liệu.

Nếu một tầng cần đọc không sẵn sàng, trả503 HISTORY_UNAVAILABLE, không trả200
danh sách thiếu như đủ dữ liệu. Empty200 chỉ khi coverage COMPLETE và không có
record. Point lookup ưu tiên hot, sau đó members index/archive; chưa materialize
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
Không dùng chart aggregates làm input ML/Drift. Long cold query vượt thời gian
interactive trả503 QUERY_TIMEOUT hoặc yêu cầu export, không chạy vô hạn trong API.

## 7. Export và retention recovery

Export job: QUEUED -> RUNNING -> SUCCEEDED / FAILED / EXPIRED. Idempotency-Key
ràng buộc actor + canonical query, cùng key khác query ->409. Frozen query scope,
manifest generation, snapshot và authorized machines được lưu khi accept.
Nếu cần hơn100000 hot IDs, worker tạo snapshot/export theo chunk với durable
membership và pins; nó có pool riêng, không chiếm pool dashboard. Không bỏ giới
hạn bằng cách chạy query khổng lồ trong HTTP handler.

Download trước success ->409 EXPORT_NOT_READY; sau artifact TTL ->410;
không có quyền ->403; job không tồn tại404. Quyền phải được kiểm tra lại lúc
download, không chỉ lúc tạo. Export không resolve alert hoặc kích hoạt training.

DB cleanup và S3 expiry là hai bước riêng; backup DB không phải archive query.
Nếu cleaner crash, membership + status cho phép tiếp tục. Nếu manifest bị thiếu
hoặc checksum sai sau verification, quarantine batch, ngừng cleanup liên quan,
phát metric/incident và phục hồi từ DB/backup trước khi đánh dấu COMPLETE.

## 8. Acceptance cần có khi implement

- Crossing hot/archive boundary: không duplicate/missing, stable cursor và pin.
- Late terminal commit vào batch tiếp theo, không bị timestamp watermark bỏ qua.
- Upload crash/lease expiry/retry: một logical batch, không premature deletion.
- Archive unavailable: explicit503; expired history410; empty complete200.
- Invalid/failed outcomes giữ null ML; evidence alert còn truy cập sau hot cleanup.
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

Offset coordinator chỉ advance safe completed prefix sau DB durability; processed
archive/retention vẫn cần verified membership riêng. Frontier chưa tiến không
chứng minh các outcomes sau hole chưa có trong DB. Raw Storage giữ từng receipt
và runner/microbatch hiện hành; không đổi batching hoặc bỏ archive theo UI
coalescing. Ordered outbox không thay snapshot/manifest/watermark rules của HISTORY-01.
