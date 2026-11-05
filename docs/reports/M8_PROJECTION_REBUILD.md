# M8 — Business projection rebuild

Thiết kế triển khai RECOVERY-01 §5. Phạm vi gồm ba latest projections của
Business; không replay Alert, audit, feedback, briefs, DQ hoặc inference.

## Storage và chuyển đổi

Migration 0007 bổ sung generation metadata, singleton active pointer, ba bảng
projection theo generation, identity ledger shadow, jobs, expected membership
và journal. Các bảng legacy giữ nguyên; tính năng mặc định tắt. Bootstrap chỉ
chạy explicit sau khi drain writers cũ và triển khai binaries hỗ trợ generation.
Một SQL snapshot chọn active pointer chung cho cả ba trục.

Generation BUILDING/ACTIVE/FAILED; pointer xác định generation phục vụ hiện tại.
Lịch sử activated_at/deactivated_at được giữ; không tự xóa generation cũ.
Sequence dùng NUMERIC nguyên chính xác. Live inbox/identity ledger không reset.

## Ranh giới capture và activation

Live giữ shared advisory transaction gate trước machine lock. Start/activation
giữ exclusive gate, đợi live transactions trước đó commit/rollback. Các bộ đếm
journal/applied commit cùng transaction phát hiện record chưa replay bị mất,
không đòi serial IDs liền nhau. Journal ID
chỉ dùng làm cursor khi đã capture committed frontier dưới gate; không suy
completeness từ sequence được cấp phát hoặc MAX Kafka offset.

Start chụp toàn bộ registry IDs và nhóm Consumer được duyệt. Expected membership
đọc từ durable SDK inbox theo keyset; receipts của job journal được loại khỏi
baseline. Không bật inbox cleaner khi đang rebuild. Live projection và journal
commit cùng transaction. Deadline/budget hết thì job FAILED, capture dừng;
active projection tiếp tục phục vụ. Một job đang build tại một thời điểm.

Runner riêng trong Business Consumer, lease 90s, heartbeat 15s, fenced writes;
không giữ SQL transaction khi đọc S3/HTTP. Batch wire tối đa 32 records/8MiB, tối đa
hai source requests đồng thời. Deadline và journal byte budget do operator cấp.
Activation khóa tối đa 3s, không network dưới lock; expected active generation,
registry scope và verified membership phải khớp. Swap pointer và lưu
`projection.resync` cùng transaction, chỉ visible sau commit.

## Nguồn và quyền

Raw frame dùng Storage SDK 0.2.1 và exact Kafka locator trong inbox, pin cluster,
catalog/object versions và SHA. Outcome gốc dùng Processing point API hiện có.
Không dựng raw frame từ history DTO. Catalog lag chờ trong deadline; sai SHA,
identity hoặc published coverage thiếu record thì fail, không substitute.

API chỉ đọc generation. Consumer ghi active projections/journal; owner rebuild
credential quản lý shadow/jobs/pointer, không có quyền sửa Alert/audit/feedback.
CLI không là HTTP mutation API; credential owner là quyền vận hành. Input giữ
operator label, scope/budgets và refs, không lưu token. Bootstrap là owner command
bổ sung cho sáu thao tác start/status/resume/verify/activate/abort.

## Giới hạn evidence

Baseline local: 372 tests owner không integration và 17 browser tests M1–M7 pass.
Browser dùng PostgreSQL/Kafka/MinIO thật, ML/RAG/OIDC/publisher fixtures. Đây không
là nghiệm thu providers shared hoặc deployment toàn hệ thống.

## Cấu hình và thao tác owner

Hai runtime dùng `BUSINESS_PROJECTION_GENERATIONS_ENABLED=true`; mặc định `false`.
Consumer chỉ khởi động runner khi `BUSINESS_REBUILD_ENABLED=true`, đồng thời có:

| Biến | Ý nghĩa |
| --- | --- |
| BUSINESS_REBUILD_DATABASE_URL | Credential rebuild riêng, cùng Business DB |
| BUSINESS_REBUILD_RAW_BUCKET | Bucket raw/catalog đã duyệt, bật versioning |
| BUSINESS_REBUILD_S3_REGION | Region S3 |
| BUSINESS_REBUILD_S3_ENDPOINT | Tùy chọn; shared yêu cầu HTTPS |
| BUSINESS_PROCESSING_READ_URL | Origin Processing Read API |
| BUSINESS_PROCESSING_TOKEN_FILE | Service token có quyền đọc original outcomes và machine scope |

SDK S3 dùng credential chain hiện có, không ghi token vào manifest/job/status.
`grant_business_permissions(..., rebuild_role=...)` cấp quyền riêng: API không
được đổi pointer, Consumer chỉ cập nhật capture/projections; rebuild role đọc
inbox/source metadata và ghi các bảng recovery, không ghi business effects.
Platform owner chạy grants/rollout; runtime không tự tạo role hoặc migrate.

Trình tự kích hoạt lần đầu:

1. Chạy migration owner `maintai-business-migrate upgrade` và grants mới.
2. Triển khai binaries có hỗ trợ generation; drain mọi projection writer cũ.
   Điều phối reader/writer rollout để API không phục vụ bảng legacy đã stale.
3. Với rebuild owner credential, chạy
   `maintai-business-rebuild bootstrap --writers-drained`.
4. Bật generation cho mọi API/Consumer, kiểm readiness rồi tiếp tục consumption.
   Bật runner với credential riêng khi cần rebuild. Không tắt generation flag
   để quay về legacy tables sau khi đã có live writes vào generation.

Start yêu cầu operator label được duyệt, source Kafka cluster identity trong
catalog và đúng group IDs kể cả prefix của deployment. Không dùng broker URL
thay cluster identity. Deadline phải có timezone; journal budget là số byte wire
được phép giữ. Không có giá trị production mặc định cho hai budget này.

```sh
maintai-business-rebuild start \
  --expected-active "$ACTIVE_GENERATION" --operator "$OPERATOR_LABEL" \
  --source-cluster "$APPROVED_SOURCE_CLUSTER" \
  --telemetry-group "$BUSINESS_LIVE_GROUP" \
  --prediction-group "$BUSINESS_PREDICTION_GROUP" \
  --deadline "$APPROVED_DEADLINE_UTC" \
  --journal-budget-bytes "$APPROVED_JOURNAL_BUDGET"
maintai-business-rebuild status "$JOB_ID"
maintai-business-rebuild verify "$JOB_ID"
maintai-business-rebuild activate "$JOB_ID" --expected-active "$ACTIVE_GENERATION"
```

Runner tự scan membership, đọc nguồn và catch-up; không tự activate. `verify`
fence shadow writer, chụp membership/journal đã commit và mở MVCC snapshot chỉ đọc trong gate ngắn;
so sánh ledger theo từng trang và quét checksum ngoài job/generation lock. Live
capture tiếp tục trong lúc quét. Lỗi kết nối/deadlock/serialization được retry
trong deadline, không gắn nhãn nguồn hỏng. Activation chỉ apply tail đã
commit, không gọi provider. Tail quá lớn hoặc timeout: transaction rollback,
generation hiện tại giữ nguyên; dùng `resume` để runner catch-up/verify lại,
giữ nguyên deadline và input. `abort` dừng capture. Job FAILED/ABORTED cần start
mới; không reset input/deadline. Không blind activate generation cũ đã stale.

Job status chỉ xuất metadata/bounded error code, không xuất credential hay
provider exception body. Metrics Consumer dùng status enum hữu hạn:
`maintai_projection_rebuild_jobs`, `maintai_projection_rebuild_oldest_age_seconds`,
`maintai_projection_rebuild_pending_members`, `maintai_projection_rebuild_journal_bytes`.

## Kiểm chứng local

Tests nằm trong `backend/business-consumer/tests/test_projection_*`,
`test_rebuild_*`, `frontend/tests/unit/projection-resync.test.ts` và
`frontend/tests/browser/rebuild-live.spec.ts`. DB tests dùng schema riêng;
Storage tests tạo bucket versioned riêng và dọn cả object versions.

Browser test dùng Storage Consumer/catalog publisher và Processing HTTP thật
trong pipeline local; ML/RAG/OIDC/provenance dùng fixtures có nhãn. Test kiểm
resync qua hai API replicas và số Alert events không tăng do rebuild. Unit UI
kiểm snapshot mới có sequence thấp hơn và phục hồi sau mất notification/reconnect.
Đây không là bằng chứng providers shared, tải BASE100 hoặc production recovery.

Opt-in: `MAINTAI_M3_INTEGRATION=1`, `MAINTAI_M8_INTEGRATION=1` và DSN test tại
`MAINTAI_M3_DATABASE_URL`. Browser dùng thêm các cờ M4/M5/M6; M7 độc lập. Không
khởi động shared services hoặc chạy owner operations production từ các test này.

Kết quả local ngày 04/11/2026 sau review:

| Kiểm tra | Kết quả |
| --- | --- |
| Backend unit/DB/MinIO/SSE/installed containers, bật M2/M3/M4/M7/M8 và container flags | 244 passed, không skip |
| Processing owner regression, `-m 'not integration'` | 241 passed |
| Frontend unit | 36 passed |
| Browser M1–M8, Storage/Processing thật, ML/RAG/OIDC fixtures | 18 passed |
| Browser rebuild khi `MAINTAI_M7_INTEGRATION=0` | 1 passed, hai API replicas |
| Ruff/format Processing + Backend; frontend check/build; pip check | Passed |
| HTTP contracts validator | 6 OpenAPIs, 55 operations, 104 positive/negative cases passed |
| Kafka immutable bundle lock | 30 inputs unchanged |
| Business API/Consumer images, installed CLI/help/imports, sdist → wheel | Passed |

Review phát hiện và regression tests đã kiểm chứng: REST refresh khi mất resync
nhưng stream còn sống; ledger verification không giữ live capture lock; DB
transient không kết thúc job như corruption. Tests còn kiểm journal record chưa
replay bị mất, pointer giữ nguyên khi lỗi và input/deadline bất biến. Không có
thay đổi HTTP DTO/Kafka schemas hoặc source thuộc Storage/ML/RAG/Drift.

CI bật thêm duy nhất cờ M8 ở hai job Processing integration và Main UI hiện có.
Kết quả trên là thực thi local, không phải GitHub Actions run của commit đã push.

Gate phối hợp còn lại: provider auth/provenance/release receipts được duyệt;
scope Storage/Processing thực tế; replay window/safety margin; rollout/grants;
pipeline/rebalance/recovery và đo browser alert p95 theo CAPACITY-BASE100-01.
Không bật cleaner cho inbox, journal, generations hoặc nguồn processed/raw.
