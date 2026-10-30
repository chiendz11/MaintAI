# Processing Read API

HTTP runtime nội bộ đọc trạng thái event đã được Worker commit trong `processing_db`.
Business gọi API bằng service identity riêng; không truy vấn SQL xuyên owner.

## API và dữ liệu

`GET /api/v1/internal/sensor-events/{event_id}/status` trả DTO `ProcessingStatus`
trong [HTTP baseline](../../contracts/v1.3/schemas/runtime.schema.json). Chỉ role
`BUSINESS_SERVICE` và machine scope tương ứng được đọc kết quả. Event ID trong URL
phải được percent-encode.

- Kết quả durable giữ đúng `COMPLETED`, `INVALID` hoặc `FAILED` và mã lỗi inference.
- Chưa có kết quả durable: `404 EVENT_NOT_MATERIALIZED`. Transaction Worker chưa
  commit không tạo trạng thái `PROCESSING` giả ở API.
- DB/JWKS không truy cập được: `503 DEPENDENCY_UNAVAILABLE`; không trả connection
  string hoặc token trong lỗi.
- `X-Correlation-ID` được giữ xuyên hop; `X-Request-ID` nhận diện request.

Repository nguồn chỉ thực hiện SELECT. Kết nối nguồn bật
`default_transaction_read_only=on`; DB role vẫn phải được cấp SELECT riêng bằng
owner migration/grant operations. Runtime không tạo schema hoặc tự migrate.

## Cấu hình

| Biến | Nội dung |
| --- | --- |
| `PROCESSING_READ_DATABASE_URL` | PostgreSQL URL dùng reader role của Processing |
| `PROCESSING_READ_OIDC_ISSUER` | Issuer JWT được tin cậy |
| `PROCESSING_READ_OIDC_AUDIENCE` | Audience internal Processing đã cấu hình ở issuer |
| `PROCESSING_READ_OIDC_JWKS_URL` | Địa chỉ JWKS, ký RS256 |
| `PROCESSING_READ_ROLES_CLAIM` | Tên claim chứa danh sách roles |
| `PROCESSING_READ_MACHINES_CLAIM` | Tên claim chứa danh sách machine IDs được phép |
| `PROCESSING_READ_PROFILE` | `shared` mặc định; `local-test` yêu cầu DB/OIDC/bind loopback |
| `PROCESSING_READ_HOST` | IP bind, mặc định `0.0.0.0` |
| `PROCESSING_READ_PORT` | Cổng HTTP, mặc định `8004` |

Shared profile yêu cầu HTTPS cho issuer/JWKS. Token thiếu, hết hạn hoặc sai audience
trả401; role/machine không đủ quyền trả403. Không có quyền wildcard ngầm định.
Không cần Kafka hoặc ML credentials để khởi động runtime này.

## Chạy và kiểm tra

Sau khi cài Processing từ môi trường khóa dependencies và khai báo cấu hình:

```bash
maintai-processing-read-api
```

`/health/live` kiểm tra process; `/health/ready` kiểm tra SELECT trên hai bảng
`sensor_events`, `processing_results` và trạng thái khóa OIDC. Probes không chứng
minh issuer production hoặc ACL đã được nghiệm thu.

`/metrics` xuất HTTP request counts/durations và DB/OIDC dependency errors theo
OBS-01. Labels dùng router templates, không chứa event/machine IDs; health/scrape
không được tính vào REST SLO. Lỗi collector không thay đổi kết quả API.

Build context của [Dockerfile](Dockerfile) là root repository. Image chạy non-root,
cài package và SDK từ wheels, nhận SIGTERM qua entrypoint riêng.

```bash
docker build -f processing/read-api/Dockerfile -t maintai-processing-read-api:local .
.venv/bin/python -m pytest processing/read-api/tests
```

Tests HTTP dùng JWT ký thật với issuer/JWKS fixture. Các tests repository tại
`processing/worker/tests/test_status_repository.py` dùng fixture PostgreSQL của
Processing, bật theo hướng dẫn tests trong component.

## Giới hạn

Status và hot point-read giữ DTO M5. M6 bổ sung lịch sử Processing/raw, snapshot,
chart và export theo profile riêng; xem [tham chiếu M6](HISTORY.md).
Archive kết quả Processing và xóa dữ liệu nguồn không thuộc M6.
Business status facade và service credentials được cấu hình riêng. Installed
HTTP/probes/role permissions/SIGTERM được kiểm trong test hai DB của Business;
shared OIDC/TLS, provider production và deployment vẫn cần nghiệm thu riêng.
Image source hoặc tests fixture không chứng minh đã deploy toàn hệ thống.


## M5 — Hot evidence point-read

`GET /api/v1/internal/predictions/{result_id}` trả HistoryRecord1.0.0 chứa outcome
1.4.0 gốc, features_raw, source_server_accepted_at, risk_snapshot và raw_ref=null.
Ba terminal outcomes đều đọc được; INVALID có thể giữ escaped NUL trong raw bytes.
Chỉ BUSINESS_SERVICE và machine scope phù hợp được đọc. Không tồn tại trả404
PREDICTION_NOT_FOUND; sai checksum/schema/source/model/risk trả503 đã sanitize.

Repository join owner-local sensor_events, processing_results, predictions và
risk_decisions. So thời gian accepted theo source_server_accepted_at, không dùng
received_at của sensor. Schema và refs outcome được nạp từ SDK wheel đã đóng gói.
Không cần checkout để validate và không quét S3 hoặc giả archive coverage.

Processing reader role cần SELECT cả bốn bảng này; chạy owner grant helper hiện
có, không cấp write hoặc credential DB cho Business. M5 không bật cleaner. Trước
khi có cleanup nguồn ở giai đoạn sau phải bảo vệ evidence của signal còn backlog
và có archive lookup. M6 chỉ giải phóng metadata hết hạn, không xóa outcomes.

## Thiết kế M6, chưa bật trong runtime

[M6-HISTORY-01](../../docs/M6_HISTORY_ARCHITECTURE_DECISION.md) dùng source SELECT-only
pool hiện có và metadata writer pool riêng trong Read API. Export runner nằm ở
Read API, không Worker. M6 giữ outcomes DB, processed archive/source deletion
hoãn; câu chuẩn bị cleanup ở các mục cũ áp dụng cho pha retention sau M6.
OpenAPI2.0.0/profile mới không chứng minh runtime hiện tại phục vụ history/export.
