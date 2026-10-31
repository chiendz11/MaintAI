# M6 — History, charts và export

Runtime thực hiện profile `M6-HISTORY-01/1.0.0` theo
[quyết định kiến trúc](M6_HISTORY_ARCHITECTURE_DECISION.md) và
[protocol raw catalog](M6_RAW_ARCHIVE_HANDOFF.md). M5 status/point-read, DTO Alert/live,
Kafka SDK0.3.1 giữ wire outcome1.4.0 và bundle2.0.0. HistoryRecord2.0.0 chỉ dùng cho collection/
export M6; không bổ sung processed_at vào evidence HistoryRecord1.0.0.

## Thành phần

| Thành phần | Source | Hành vi |
| --- | --- | --- |
| Processing metadata | `processing/migrations/versions/0003_history.py`, `0004_native_catalog.py`, `processing/shared/history_meta/` | Snapshot membership, export intent/lease, catalog generation/index trong owner DB |
| Read API | `processing/read-api/src/api/history_*.py` | Source SELECT-only, metadata credential riêng; range/raw/chart, CSV nhỏ, durable jobs và metadata expiry |
| Business facade | `backend/business-api/src/api/history*.py`, `backend/business-api/src/processing_client/` | Auth/scope, delegation RS256, bounded HTTP, export proxy; không SQL xuyên owner |
| Business markers | `backend/migrations/versions/0004_history_markers.py`, `backend/shared/persistence/history_markers.py` | Freeze composite chart/Alert markers, cursor ký dùng chung giữa API replicas |
| Main UI | `frontend/src/features/machines/detail/HistoryPanel.tsx`, `frontend/src/api/history-*.ts` | History/raw filters, snapshot pages, SVG charts, CSV/job/download và idempotency reconciliation |

History Processing sort `(completed_at,result_id)`; raw sort
`(received_at,topic,partition,offset)`, giữ receipts trùng event khác locator.
Snapshot freeze có giới hạn, không giữ source transaction10 phút. Cursor ràng buộc
actor/scope/query/profile/snapshot. Late commits chỉ xuất hiện khi refresh.

Catalog sync dùng helper `maintai-storage-consumer==0.2.1` và schema provider native.
Generation là delta; index tích lũy receipts qua exact parent refs, có checkpoint
segment và visited set bền vững. Không copy toàn bộ index mỗi generation hoặc công
bố BUILDING là collection đầy đủ. Duplicate cùng bytes được giữ một logical receipt;
cùng locator khác payload/refs bị từ chối. Source cluster/topic identity và start
prefix không được tự reset. Native ObjectRef không có size_bytes; DTO export giữ
size_bytes và public HTTP profile hiện hành.

Lease/fencing bảo vệ checkpoint và activation. Observation trước network gắn với
head fence; phản hồi S3 cũ không được block generation mới. Source index dùng import
sequence để giữ view của generation đã pin. Coverage yêu cầu đúng12 partition IDs,
không gap/lag/unknown và head published_at còn trong120s. COMPLETE chỉ là offset
prefix đã công bố, không là temporal watermark. Kafka offset không cần liên tiếp.

CSV được preflight trước headers; Parquet luôn qua durable job. SQL lease/fencing,
heartbeat và conditional immutable uploads cho phép runner khác tiếp tục sau lease
hết hạn. Job-owned hold giữ input độc lập snapshot TTL. Metadata expiry dùng row
locks khi giải phóng membership; reader revalidate/giữ lock khi đọc để tránh200
rỗng do expiry race. Idempotency descriptor giữ lại để CSV hết hạn vẫn410.

Parquet dùng UTF-8 identity/time columns và JSON logical type cho outcome/features/
record; dữ liệu heterogeneous INVALID, NUL và số nguyên lớn không bị ép sang int64.
Xem physical format, limits và biến môi trường trong
[tham chiếu Read API](../processing/read-api/HISTORY.md),
[Business README](../backend/README.md) và [Frontend README](../frontend/README.md).

## Kiểm chứng và giới hạn bằng chứng

Các tests có thể chạy độc lập bằng PostgreSQL/Kafka/MinIO trong Compose M6. Những
suite cùng dùng broker test phải chạy tuần tự để không trộn receipts giữa fixture
publishers. Local source/unit tests và browser harness kiểm chứng:

- Source/meta SQL roles riêng, owner hoặc extra source/inbox/outbox/column grants bị từ chối.
- JWT service/delegation, cursor signatures, actor/scope/query mismatch và hết TTL.
- Membership với timestamp trùng/late commit, đủ terminal outcomes và omitted filters khi export lại snapshot.
- Native delta receipts, exact versions/checksums, duplicate/conflict, chuỗi hơn100generation, restart/checkpoint và đủ đúng partition IDs.
- CSV injection protection, job input holds, lease reclaim sau lỗi giả lập ngay sau S3 ACK và artifact reconciliation.
- Composite markers qua hai repository instances, nullable source clock, browser PKCE → HTTP Ingestion → Kafka → Worker/Relay → Business → history/raw/chart/CSV/Parquet download.
- JSON/BigInt decoding trên browser, mobile layout, source timestamps và giữ nguyên luồng live M4/Alert M5.

Browser harness chạy Storage consumer, read-only Kafka audit và catalog publisher
thật từ package0.2.1, cùng HTTP/Kafka/PostgreSQL/MinIO test thật. Helper tại
`processing/tests/storage_provider.py` chỉ quản lý runtime test, chờ coverage và
dọn bucket riêng; không tự tạo raw receipts hoặc catalog. Unit/object tests vẫn có
fixtures metadata có chủ đích để kiểm các trường hợp lỗi và restart.

Issuer, ML/provenance và registry/policy trong browser vẫn là fixtures. Nghiệm thu
local Storage không chứng minh ML production, shared OIDC/TLS/IAM, k3d hoặc rollout
toàn hệ thống đã hoàn thành. Các image Processing được build lại có wheel Storage
nội bộ; installed Read API test dùng hai SQL roles thật.

Baseline kiểm chứng gồm main commit `575c8b4` và implementation M6 trong working tree;
commit SDK patch không đổi wire/schema Kafka hoặc SQL001/002.

CI bật M6 trong Processing integration và Main UI; ba job Processing checks,
Processing integration và Main UI build wheel Storage nội bộ trước khi cài lock; GitHub CI chỉ có
kết quả sau khi commit được đẩy lên. Không suy trạng thái remote từ local tests.

## Kết quả local tại checkpoint tích hợp Storage với SDK0.3.1

| Nhóm kiểm tra | Kết quả |
| --- | --- |
| Processing/Business: unit, contract, DB/Kafka/MinIO integration và container | 546 passed trong lượt đầy đủ, không skip |
| Kafka SDK/transport unit | 86 passed; có overlap với Processing tests |
| Kafka/PostgreSQL integration | 25 passed |
| Storage unit | 78 passed,2integration deselected trong lượt unit riêng |
| Ingestion toàn bộ tests | 149 passed;1deprecation warning của Starlette TestClient |
| Frontend unit | 30 passed |
| Browser M4/M5/M6 | 12 passed |
| HTTP contracts | 6 OpenAPIs, 104 positive/negative cases đạt |
| Kafka bundle2.0.0 | 30 immutable inputs giữ nguyên |
| Cấu trúc/tài liệu | 47 document hashes, 664 links/anchors, 14 app + 1 platform descriptors đạt |
| Packaging/images | Sáu image Processing/Business/Ingestion build lại với SDK0.3.1 và reader Storage0.2.1; installed Read API M6/CSV chạy với hai SQL roles thật; wheels Processing/Storage/SDK import và validate catalog ngoài checkout |

Các nhóm có thể chứa checks trùng phạm vi; không cộng thành số chức năng độc lập.
Regression chart có100001 alert cũ ngoài range: range ngắn vẫn đọc được. SQL lọc
ứng viên theo prediction markers/range trước budget, với indexes machine/time và
machine/prediction. Đây là regression chức năng, không là capacity benchmark.

## Retention và vận hành

M6 giữ outcomes trong Processing DB. Dry-run chỉ tính candidates/pins và lý do
chặn; không xóa outcome/raw/model/policy hoặc bật processed archive. Quyền tải
artifact hết sau24h; không có thao tác xóa object hoặc bucket lifecycle được bật.
Dry-run không là bằng chứng đã xác minh toàn bộ Business Alert/signal backlog.

Catalog generations, snapshot headers và idempotency descriptors được giữ bảo
toàn traceability; storage compaction/retention dài hạn cần policy riêng. Chưa có
bằng chứng sizing/HA/soak/load100; các budgets là giới hạn của profile hiện tại.
Keys/rotation, TLS/OIDC, IAM và rollout shared được cấu hình ngoài app repository.

## Đóng gói và bootstrap trên môi trường CI sạch

Processing source distribution chứa hai schema M6 lấy từ nguồn chuẩn trong
repository. Metadata của archive tham chiếu bản schema bên trong archive; checkout
và đường dẫn cài đặt schema của wheel giữ nguyên. Kiểm tra đóng gói gồm wheel trực
tiếp, wheel dựng từ sdist khi không còn canonical checkout bên cạnh, và tạo lại
sdist từ source release đã giải nén.

Harness Storage tạo canonical test topics bằng SDK và chờ đầy đủ metadata/leader
trước khi tạo bucket hoặc khởi động runtime. Không reset offsets/groups hoặc nới
readiness. Job Processing khai báo `KAFKA_TEST_BOOTSTRAP_SERVERS` khớp cổng Compose
39092; cổng local mặc định của Compose M6 vẫn40092 khi không override.

Bản sửa trên baseline `8cf8de3` được kiểm chứng local bằng hai môi trường trống
độc lập: browser chạy trước mọi suite khác; project sau đó được tạo lại cho
Processing. Biến môi trường được đọc từ job tương ứng trong workflow.

| Kiểm tra bản sửa | Kết quả local |
| --- | --- |
| Build cô lập theo `python -m build processing` | sdist và wheel đạt trên Python3.12 |
| Đóng gói/installed smoke | 3 trường hợp đạt; schema bytes khớp canonical |
| Processing/Business với env CI và broker/DB mới | 549 passed, không skip |
| Browser trên broker/DB mới với env frontend-ci | 12 passed |
| Kafka unit/transport | 86 passed, có overlap với Processing tests |
| Môi trường chỉ cài dependencies SDK | Installed contract validator và Worker/Relay `--help` đạt ngoài checkout |
| Images Processing | Worker, Relay và Read API build được |

HTTP contracts, immutable Kafka inputs, lint/format và dependency checks đạt.
Python3.11 và GitHub Actions của commit sửa cần được xác nhận khi commit được push;
kết quả local không thay thế trạng thái remote. Source runtime/schema nghiệp vụ,
wire protocol, migration và deployment manifests không đổi trong bản sửa này.
