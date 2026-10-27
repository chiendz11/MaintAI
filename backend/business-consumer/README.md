# Business latest projections

Các modules hiện có materialize latest telemetry, latest processing và latest
valid prediction trong `business_db`. Runtime `maintai-business-consumer` chạy hai
serial SDK loops `business-live-v1` và `business-prediction-v1` trong cùng process.
Các callbacks nằm trong skeleton `src/handlers/telemetry` và `src/handlers/prediction`;
không khởi động group Alert hoặc tạo handler ACK giả.

## Storage và transaction

Migration `business_0002_projections` kế thừa registry, áp đúng SDK migration001
đã kiểm checksum. Chạy migration bằng owner operation trước runtime; không sửa
revision registry hoặc kích hoạt Processing ordering migration002 trong Business.

- Ba bảng latest chỉ giữ một snapshot mỗi machine/trục. Snapshot lưu dạng bytes
  JSON để bảo toàn raw features, kể cả chuỗi hoặc key chứa NUL.
- `business_source_inbox` giữ source IDs, sequence, epoch, hashes và validation receipt;
  không lưu raw
  history. Guard này phát hiện source/terminal conflict sau khi latest đã tiến lên.
- `maintai_kafka_inbox` chống xử lý trùng trong từng group.
- `business_projection_conflicts` giữ coordinates, hash và mã lỗi tối thiểu.

`ProjectionHandler(connection_factory, consumer_group)` dùng SDK
`TransactionalHandler`: inbox, source guard và projection cùng transaction.
Conflict rollback effects và inbox; evidence được lưu qua transaction riêng,
sau đó lỗi tiếp tục truyền ra. Không trả durable receipt hoặc skip event lỗi.
Không xóa inbox/source guard để bỏ qua conflict; reconciliation cần giữ được
quan hệ source identity và bằng chứng đã xử lý.

## Ordering và dữ liệu

Callbacks khóa registry row của machine trước khi cập nhật. So thứ tự bằng
`sequence_number` NUMERIC dương, hữu hạn, nguyên; không sort epoch hoặc event ID.
Máy chưa provision bị từ chối và không tự tạo registry.

Telemetry và prediction có cursor độc lập. Outcome terminal đến trước telemetry
không bị ghi đè thành PENDING. INVALID/FAILED không xóa prediction hợp lệ lịch sử.
Source cùng sequence nhưng khác event/epoch hoặc terminal có nội dung mâu thuẫn
là conflict, kể cả khi các bảng latest đã chuyển sang event khác.

Native1.4 và legacy2.0 được decode bằng SDK. Metadata giữ `risk_score` native;
legacy thiếu trường này được lưu null, không lấy probability thay thế và không
tự tính lại risk. REST Fleet và state summary cần mapping tương thích riêng.

`ProjectionRepository.live(machine_id)` đọc ba trục trong một SQL statement và
trả `LiveSnapshot`; máy chưa có dữ liệu có các trục null. Repository không query
Processing DB hoặc chạy ML/risk.

## Quyền và tests

`grant_business_permissions` chỉ cấp quyền trong schema owner đã chọn cho hai
roles có sẵn. Reader chỉ SELECT registry/projections; Consumer có quyền inbox,
source guard, projections và một số cột registry. Provisioning roles/databases
là thao tác riêng; helper không tạo credentials.

Tests PostgreSQL tại `tests/test_projections.py` bật bằng
`MAINTAI_M3_INTEGRATION=1` và `MAINTAI_M3_DATABASE_URL` trỏ DB test loopback cô lập.
Tests tự tạo và dọn schema riêng; không dùng DB production. Chúng kiểm ordering,
duplicates, late conflict, concurrency, raw preservation, DTO schema và grants.

## Runtime và observability

`BUSINESS_DATABASE_URL` là connection của Consumer role. Kafka dùng cấu hình SDK
trong [hướng dẫn Kafka](../../docs/KAFKA_DEVELOPMENT_GUIDE.md). Shared profile dùng
SASL/TLS và canonical groups; `KAFKA_GROUP_PREFIX` chỉ dành cho local test.
`BUSINESS_CONSUMER_PROFILE=local-test` yêu cầu DB/bind loopback và Kafka local mode.
Management HTTP mặc định `0.0.0.0:8083`, đổi bằng `BUSINESS_CONSUMER_HOST` và
`BUSINESS_CONSUMER_PORT`.

`/health/live` kiểm process; `/health/ready` dùng probe DB/Kafka có tuổi tối đa10s
và từ chối khi draining. `/metrics` xuất duplicate receipts sau commit và histogram
accepted-to-telemetry-visible theo OBS-01. Lỗi collector không thay đổi receipt.
Shutdown SIGTERM đặt stop cho cả hai loops, chờ transaction đang chạy theo SDK và
dừng management server. Runtime không tự tạo topics hoặc migrate DB.

Tests Kafka dùng groups UUID riêng, bootstrap từ một boundary đầy đủ trước khi
Consumer chạy; không reset group đang hoạt động. Test installed flow bật thêm
`MAINTAI_M3_CONTAINER_TEST=1`, dùng ba app images, hai DB/roles riêng, JWT/JWKS fixture,
gateway ACK, Worker callback/Relay và HTTP status/live. Worker ML/provenance/policy
là fixtures; đây không phải nghiệm thu provider production hoặc outer Ingestion HTTP.
Các container app/DB/roles test được dọn sau mỗi case; volumes broker giữ riêng.

Fleet/MachineDetail readers nằm trong `shared/persistence/projections.py`; chúng
không thay đổi các snapshots mà Consumer ghi. Nguồn đọc Alert chưa được nối vào
runtime; Fleet/Detail báo503 khi cần nguồn này, không tạo dữ liệu rỗng thay thế.
Tests `test_machine_reads.py` kiểm SQL scope/state/pagination, DTO và snapshot nhất
quán bằng PostgreSQL thật cùng nguồn Alert fixture. Alert lifecycle/jobs và
notifications/SSE chưa được triển khai. Full history/archive/export thuộc
Processing interfaces khác với status API.
