# M6 — History, charts và export

Runtime thực hiện profile `M6-HISTORY-01/1.0.0` theo
[quyết định kiến trúc](M6_HISTORY_ARCHITECTURE_DECISION.md) và
[protocol raw catalog](M6_RAW_ARCHIVE_HANDOFF.md). M5 status/point-read, DTO Alert/live,
Kafka SDK0.3.0 và bundle2.0.0 giữ nguyên. HistoryRecord2.0.0 chỉ dùng cho collection/
export M6; không bổ sung processed_at vào evidence HistoryRecord1.0.0.

## Thành phần

| Thành phần | Source | Hành vi |
| --- | --- | --- |
| Processing metadata | `processing/migrations/versions/0003_history.py`, `processing/shared/history_meta/` | Snapshot membership, export intent/lease, catalog generation/index trong owner DB |
| Read API | `processing/read-api/src/api/history_*.py` | Source SELECT-only, metadata credential riêng; range/raw/chart, CSV nhỏ, durable jobs và metadata expiry |
| Business facade | `backend/business-api/src/api/history*.py`, `backend/business-api/src/processing_client/` | Auth/scope, delegation RS256, bounded HTTP, export proxy; không SQL xuyên owner |
| Business markers | `backend/migrations/versions/0004_history_markers.py`, `backend/shared/persistence/history_markers.py` | Freeze composite chart/Alert markers, cursor ký dùng chung giữa API replicas |
| Main UI | `frontend/src/features/machines/detail/HistoryPanel.tsx`, `frontend/src/api/history-*.ts` | History/raw filters, snapshot pages, SVG charts, CSV/job/download và idempotency reconciliation |

History Processing sort `(completed_at,result_id)`; raw sort
`(received_at,topic,partition,offset)`, giữ receipts trùng event khác locator.
Snapshot freeze có giới hạn, không giữ source transaction10 phút. Cursor ràng buộc
actor/scope/query/profile/snapshot. Late commits chỉ xuất hiện khi refresh.

Catalog sync kiểm exact versions/checksums, generation/frontier không lùi và
preservation của receipts đã publish. Generation mới bỏ receipt hoặc thay payload/
refs bị từ chối; active generation cũ được giữ. Coverage yêu cầu đủ đúng12 partition
IDs, không gap/lag/unknown. Không suy temporal watermark từ max source timestamp.

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
- Raw receipt versions/checksums, duplicate event khác locator, generation mất/thay receipt và đủ đúng partition IDs.
- CSV injection protection, job input holds, lease reclaim sau lỗi giả lập ngay sau S3 ACK và artifact reconciliation.
- Composite markers qua hai repository instances, nullable source clock, browser PKCE → HTTP Ingestion → Kafka → Worker/Relay → Business → history/raw/chart/CSV/Parquet download.
- JSON/BigInt decoding trên browser, mobile layout, source timestamps và giữ nguyên luồng live M4/Alert M5.

Issuer, ML/provenance, registry/policy và catalog publisher trong browser harness
là fixtures rõ ràng; HTTP, broker, SQL và S3-compatible storage là dịch vụ test
thật. `frontend/tests/provider/history_archive.py` không là implementation Storage
publisher. Full provider integration cần catalog publisher/reconciliation theo
handoff cùng credentials/grants thực tế. Các image được build/import-check local
không chứng minh đã chạy M6 trên shared Kubernetes hoặc production.

CI bật M6 trong hai jobs Processing integration và Main UI; GitHub CI chỉ có
kết quả sau khi commit được đẩy lên. Không suy trạng thái remote từ local tests.

## Kết quả kiểm tra local

| Nhóm kiểm tra | Kết quả |
| --- | --- |
| Processing/Business: unit, contract, DB/Kafka/MinIO integration và container | 511 passed, không skip |
| Frontend unit | 30 passed |
| Browser M4/M5/M6 | 12 passed |
| HTTP contracts | 6 OpenAPIs, 104 positive/negative cases đạt |
| Kafka bundle2.0.0 | 30 immutable inputs giữ nguyên |
| Cấu trúc/tài liệu | 41 document hashes, 612 links/anchors, 14 app + 1 platform descriptors đạt |
| Images | Worker, Relay, Read API, Business API/Consumer và Main UI build được; Read API M6/CSV chạy với hai SQL roles thật; Parquet writer/managed reader trong image giữ JSON integer lớn |

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
