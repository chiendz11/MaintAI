# Tham chiếu runtime M6 History

Read API đọc outcomes trong `processing_db` và raw receipts từ catalog S3/MinIO đã
kiểm version/checksum. Business là facade duy nhất của frontend. Worker và Relay
không chạy export loop; không có history database hoặc Deployment bổ sung.

## API và quyền

Các route nội bộ bắt đầu bằng `/api/v1/internal/`:

| Route | Dữ liệu |
| --- | --- |
| `GET machines/{machine_id}/predictions` | Các terminal outcomes, kể cả INVALID/FAILED |
| `GET machines/{machine_id}/sensor-readings` | Raw receipts, giữ duplicate event khác Kafka locator |
| `GET machines/{machine_id}/timeseries` | Buckets min/max/avg/count/invalid_count và prediction markers |
| `POST history-exports` | CSV nhỏ hoặc durable CSV/Parquet job |
| `GET history-exports/{job_id}` | Trạng thái job đã lưu |
| `GET history-exports/{job_id}/download` | Proxy tệp đã kiểm version/checksum |

Các route M6 yêu cầu `X-MaintAI-History-Profile: M6-HISTORY-01/1.0.0`, service JWT và
JWT ủy quyền do Business ký. Browser JWT không được chuyển tới Processing. JWT
ủy quyền dùng RS256, audience `processing-history`, lifetime tối đa60s, leeway5s;
`azp` phải bằng subject của service JWT. Actor/machine scope được ràng buộc vào
snapshot, cursor và job. Service token vẫn cần role `BUSINESS_SERVICE`.

M5 status và `GET predictions/{result_id}` giữ DTO hiện có, không yêu cầu profile
M6 hoặc thêm `processed_at` vào HistoryRecord1.0.0.

## Clocks, snapshot và giới hạn

Processing dùng `processing_results.completed_at` làm `processed_at`, thứ tự
giảm dần `(completed_at,result_id)`. Raw dùng `(received_at,topic,partition,offset)`
giảm dần. Không dùng các clocks này để thay ordering của latest/SSE.

Range là `[from,to)` theo UTC, mặc định1h; tương tác tối đa31 ngày. Trang mặc định
100, tối đa500. Snapshot tương tác hết hạn sau10 phút, tối đa100000 bản ghi/64MiB.
Membership được materialize trước khi trả trang đầu, rồi publish atomically vào
metadata; không giữ source transaction trong suốt thời gian phân trang. Late
commits chỉ xuất hiện khi tạo snapshot mới. Cursor ký có phiên bản khóa, ràng buộc
actor/scope/query/collection/snapshot; hết hạn trả410.

Source pool mặc định4 connections, metadata pool4, job pool2 và catalog pool2;
thời gian chờ pool tối đa2s. Source statement timeout3s, metadata10s. Runtime kiểm
quyền trước khi bật M6, không tự migrate hoặc provision roles.

Raw catalog phải có generation hợp lệ, không quá120s, đủ12 partitions và không
có gap/lag/unknown. `available_from`/`available_through` có thể null; response giữ
offset coverage để không tạo watermark từ max received_at. Thiếu catalog/object,
sai checksum/version hoặc coverage không khả dụng trả503. Reader không scan bucket
trong HTTP request. Đồng bộ một generation bị giới hạn1000000 entries/512MiB.

## Cấu hình bổ sung

Giữ các biến `PROCESSING_READ_*` trong [README](README.md), rồi cấu hình:

| Biến | Ý nghĩa |
| --- | --- |
| `PROCESSING_HISTORY_ENABLED` | `true` bật M6; mặc định `false` |
| `PROCESSING_HISTORY_METADATA_DATABASE_URL` | Cùng Processing DB, credential khác source reader |
| `PROCESSING_HISTORY_META_SCHEMA` | Mặc định `history_meta` |
| `PROCESSING_HISTORY_DELEGATION_ISSUER` | Issuer tin cậy cho JWT Business ký |
| `PROCESSING_HISTORY_DELEGATION_JWKS_URL` | JWKS chứa public keys của Business |
| `PROCESSING_HISTORY_CURSOR_KEY_FILE` | JSON chứa `active` và map `keys`; secret base64 tối thiểu32 bytes/key |
| `PROCESSING_HISTORY_RAW_BUCKET` | Bucket raw/catalog chỉ đọc |
| `PROCESSING_HISTORY_EXPORT_BUCKET` | Bucket artifact riêng, có quyền ghi prefix `history-exports/` |
| `PROCESSING_HISTORY_S3_ENDPOINT` | Endpoint S3-compatible; bỏ trống khi dùng AWS endpoint chuẩn |
| `AWS_REGION` | Bắt buộc khi cấu hình object store |
| `PROCESSING_SCHEMA_DIRECTORY` | Override vị trí hai schema M6; mặc định dùng dữ liệu đã đóng gói trong wheel |
| `PROCESSING_RETENTION_ENABLED` | Chỉ chấp nhận `false` |
| `PROCESSING_CLEANER_DRY_RUN` | Chỉ chấp nhận `true` |

Object store là tùy chọn; không có store vẫn đọc Processing history/chart và CSV
nhỏ. Raw và artifact jobs báo unavailable. Hai bucket phải bật versioning. Credentials
S3 lấy từ credential chain của boto3; không đặt secrets vào source hoặc logs.

Sau khi provision các roles riêng, owner chạy migration và grant metadata rõ ràng:

```bash
maintai-processing-migrate upgrade
maintai-processing-migrate grant-history-metadata --metadata-role processing_history_meta
```

Lệnh dùng `PROCESSING_DATABASE_URL` của owner. Source reader cần SELECT bốn bảng
nguồn, không quyền ghi/DDL. Metadata role chỉ CRUD các bảng `history_meta`, không
SELECT/write nguồn, inbox hoặc outbox. Không dùng owner credential cho runtime.

## Export và retention

POST yêu cầu `Idempotency-Key`; cùng actor/key nhưng payload khác trả409. CSV AUTO
được preflight trước headers, tối đa10000 rows/10MiB/10s; vượt budget chuyển job.
PARQUET luôn trả202 sau khi intent và input hold đã commit. Không tự retry POST
không rõ kết quả; gửi lại chính key/payload cũ để đối chiếu.

Jobs dùng SQL lease30s, heartbeat5s và fencing token. Input membership thuộc hold
của job, giữ qua snapshot TTL/restart/retry. Deadline30 phút, tối đa3 attempts;
artifact tối đa512MiB và TTL24h. Upload dùng key bất biến/conditional write,
reconcile exact version/checksum trước SUCCEEDED. Download proxy xác thực lại
actor/scope; chưa sẵn sàng409, artifact hết hạn410. Không chuyển credentials tới
redirect URL. Không có object lifecycle/delete được bật từ runtime này.

CSV giữ scalar columns và `record_json`, `features_json`, `outcome_json`, `risk_json`;
text có nguy cơ spreadsheet formula được prefix dấu nháy đơn. Parquet dùng ZSTD,
scalar identity/time columns dạng UTF-8; `outcome`, `features_raw`, `record` dùng
Arrow JSON logical type. Cần JSON-decode các trường này để lấy cấu trúc gốc. Cách
đóng gói giữ được INVALID features khác kiểu, NUL và sequence lớn, không ép sang
Arrow integer64 hoặc tự tạo prediction/risk cho outcome thiếu ML.

Metadata expiry chỉ giải phóng membership hết hạn khi không còn job hold; giữ
descriptor để key CSV cũ vẫn trả410. Không xóa source outcomes/raw/model/policy.
Dry-run source candidates và pins:

```bash
maintai-processing-read-api --retention-dry-run --before 2026-10-01T00:00:00Z --limit 1000
```

Source deletion luôn bị chặn bởi `PROCESSED_ARCHIVE_DISABLED`. Kết quả dry-run
không là giấy phép bật cleaner hoặc bằng chứng đã kiểm toàn bộ Alert backlog.

## Kiểm chứng local

Compose test riêng nằm tại [tests/compose.yaml](tests/compose.yaml). PostgreSQL/Kafka/
MinIO chỉ bind loopback; MinIO test được build từ source release khóa checksum.
Đây không phải image hoặc cấu hình shared platform.

```bash
docker compose -p maintai-m6 -f processing/read-api/tests/compose.yaml up -d --build --wait
MAINTAI_M2_INTEGRATION=1 MAINTAI_M6_INTEGRATION=1 \
MAINTAI_M2_DATABASE_URL="$M6_TEST_DATABASE_URL" \
  .venv/bin/python -m pytest processing/read-api/tests
docker compose -p maintai-m6 -f processing/read-api/tests/compose.yaml down --volumes
```

`M6_TEST_DATABASE_URL` phải trỏ đúng PostgreSQL loopback của project test; không dùng
production URL. Compose mặc định PostgreSQL36432, Kafka40092, MinIO39000/39001.
Phải kiểm tra quyền khởi động/dừng dịch vụ trước khi chạy các lệnh này.

OBS-01 xuất `maintai_history_queries_total{source,outcome}` và
`maintai_history_query_duration_seconds{source}` sau response; labels không chứa
machine/event/query/cursor. Source deployments và TLS/OIDC/IAM cần kiểm chứng riêng trên môi trường shared;
local harness sử dụng Storage consumer/audit/publisher thật.

## Catalog provider native

Reader dùng `maintai-storage-consumer==0.2.1`, `CatalogStore` và `CatalogReader`.
Provider schema trong package Storage tách khỏi DTO HTTP/export. Native ObjectRef
không có size_bytes; không tự thêm giá trị để phù hợp với DTO export.

Chạy owner migration `processing_0004_catalog` trước khi bật reader và cấp lại
metadata grants bằng helper hiện có. Migration bổ sung native_catalog_heads,
native_catalog_imports, native_catalog_visited và native_catalog_receipts trong
history_meta; không đổi0003 hoặc ghi source/outcome/inbox/outbox. Các snapshot/job
đã materialize vẫn giữ membership và TTL/holds hiện hành. Index fixture cũ không
được gán cluster/topic identity giả; reader native xây index riêng từ provider.

Sync duyệt delta parent_ref về committed ancestor/root, tối đa100generation và
100segment mỗi lượt, kiểm thời gian10s giữa các object. Checkpoint được commit ở
ranh giới segment; mỗi segment tối đa100entries/2MiB theo provider. BUILDING chưa
được đọc như full collection. Pending tiếp tục sau1s; idle60s, lỗi retry10s.
Lease30s được gia hạn ở mỗi transaction checkpoint; completion kiểm owner/fence.
S3 reads không giữ SQL transaction/lock. Receipt bất biến được lưu một lần; mỗi
active generation pin import sequence để index mới không thay đổi view cũ.

Cluster/topic/start-offset thay đổi, receipt xung đột hoặc chain không tới ancestor
làm raw history unavailable; không tự reset nguồn. COMPLETE chỉ là offset prefix,
không là event-time watermark; không yêu cầu số Kafka offset liên tiếp. Dữ liệu
snapshot cũ đã materialize không bị thay đổi bởi một generation mới.
