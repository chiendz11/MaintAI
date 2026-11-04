# M7 — Copilot jobs và Monitoring

Business API, Business Consumer và Main UI cung cấp Maintenance Brief theo yêu cầu,
tra cứu trạng thái job, catalog Drift và Monitoring. Payload giữ nguyên canonical
HTTP schemas; không thay Kafka SDK hoặc outcome contract.

## Luồng xử lý

TECHNICIAN/ADMIN gửi `POST /api/v1/alerts/{alert_id}/brief` với `Idempotency-Key`
và `intent`. Business khóa machine trước Alert, kiểm scope, đọc và kiểm SHA evidence
gốc M5, rồi lưu immutable input, SHA input, job, idempotency binding và facade trong
một transaction. HTTP202 chỉ được trả sau commit.

Cùng actor/key/request trả cùng receipt; key khác body trả409. Các yêu cầu active
cùng Alert/intent/evidence dùng lại job. Job mới quyết định facade hiện tại; kết quả
hoàn thành trễ của job cũ không ghi đè facade đó. Không dùng latest telemetry thay
original evidence, không chạy lại inference để tạo context.

Business Consumer chạy hai workers brief riêng với Kafka loops. Mỗi claim dùng
`FOR UPDATE SKIP LOCKED`, lease90s, heartbeat15s và fencing token. Network ở ngoài
transaction. Tối đa3attempts, deadline5phút; backoff có jitter. Timeout/429/5xx có
thể retry; auth/schema/citation failure kết thúc attempt theo failure semantics.
Worker crash hết attempts dẫn đến FAILED; deadline hết dẫn đến EXPIRED.

Provider có thể được gọi lại khi mất ACK hoặc crash; chỉ kết quả được commit với
lease/fence hợp lệ mới có hiệu lực tại Business. Không bảo đảm LLM execution exactly-once.
Valid non-grounded response vẫn là job SUCCEEDED. `CopilotResult` dùng AVAILABLE
kèm grounded_status; `BriefJob` dùng enum riêng cho INSUFFICIENT_EVIDENCE/VALIDATION_FAILED.

## API

| API | Hành vi |
| --- | --- |
| POST `/api/v1/alerts/{alert_id}/brief` | TECHNICIAN/ADMIN,202 sau lưu job; không chờ LLM |
| GET `/api/v1/alerts/{alert_id}/brief` | Facade đã lưu; không kích hoạt generation |
| GET `/api/v1/brief-jobs/{job_id}` | Trạng thái bền vững; kiểm machine scope hiện tại |
| GET `/api/v1/monitoring/summary` | Service observations và Drift freshness |
| GET `/api/v1/drift-reports` | Filter model/status, limit20/max100, signed snapshot cursor |
| GET `/api/v1/drift-reports/{report_id}` | Đọc exact S3 version, kiểm SHA/size/schema/identity |

GET yêu cầu các vai trò Business được contract cho phép. Brief reads kiểm quyền
trên Alert/machine tại thời điểm đọc. Drift là report toàn cục theo model, không
bịa machine filter. Opaque Alert IDs dùng raw fixed suffix để không nhầm resource
`/brief` với slash được encode trong ID.

## Drift catalog và Monitoring

Consumer đọc committed manifests dưới
`drift-report-manifests/published/YYYY-MM-DD/<report_id>.json`, partition theo
publication date. Không đọc drift_db hoặc chạy detector.

Recent scans hôm nay/hôm qua chạy mỗi60s. Một loop riêng tiến hành full reconciliation
hằng ngày. S3 continuation được checkpoint theo logical scan trong business_db,
có CAS để các replicas không ghi đè tiến độ nhau; không dùng last lexicographic key
làm checkpoint vĩnh viễn. Mỗi lượt xử lý một page25keys mỗi prefix. Manifest bị giới hạn256KiB,
report2MiB; object vượt giới hạn không được coi là report khả dụng. Khi restart,
scan tiếp tục từ continuation đã lưu; object lỗi được ghi nhận riêng và không chặn
các publications phía sau. Conflict cùng report_id giữ bản verified đã có.

Danh sách giữ membership theo snapshot10phút và cursor gắn actor/scope/query.
Late publications không lọt vào trang sau của snapshot cũ. Consumer xóa metadata
snapshot đã hết hạn theo batch tối đa10000members/lượt; không xóa reports, briefs,
Alert evidence hoặc dữ liệu Processing/Storage. Khi chưa có scan thành công,
list báo503; cache có lỗi đồng bộ được trả với stale=true. Unknown report404,
object thiếu/sai version/SHA/size503; persisted ERROR/INSUFFICIENT_DATA vẫn200.

Service health observations được cache15s, làm mới bởi một background thread/API
process; mỗi probe có total deadline2s và body cap64KiB. Request summary không
chờ provider network. Readiness của Business không phụ thuộc RAG hoặc S3 khỏe.
Các trạng thái service dùng AVAILABLE/DEGRADED/UNAVAILABLE, khác health endpoint UP.

`BUSINESS_MONITORING_MODEL_VERSION` chọn model rõ ràng cho latest report; không
đoán deployed model từ report mới nhất. Chưa có heartbeat bridge được chốt cho
Business nên runtime trả worker_status=NOT_AVAILABLE,last_success_at=null.
Không lấy computed_at/publication_at làm heartbeat. Window end, worker freshness
và catalog staleness được trình bày riêng.

## Cấu hình và migrations

Áp dụng owner migrations0005/0006 bằng `maintai-business-migrate upgrade`, sau đó
chạy owner grants. Không migrate trong startup. API được ghi jobs/idempotency/
facade và list snapshots; Consumer ghi kết quả/audit/catalog và chỉ xóa metadata
snapshot hết hạn. Không cấp quyền SQL xuyên database owner.

Mặc định `BUSINESS_M7_ENABLED=false`. Khi bật, API và Consumer cần cấu hình đồng nhất:

| Biến | Nội dung |
| --- | --- |
| `BUSINESS_M7_ENABLED` | `true` để bật jobs/catalog |
| `BUSINESS_RAG_URL` | HTTPS origin của RAG; loopback HTTP chỉ ở local-test |
| `BUSINESS_RAG_TOKEN_FILE` | File service token; client đọc lại khi gọi, không browser token |
| `BUSINESS_DRIFT_CURSOR_KEY_FILE` | File khóa HMAC tối thiểu32bytes; giống nhau giữa API replicas |
| `BUSINESS_DRIFT_BUCKET` | Bucket được phép đọc report/manifests |
| `BUSINESS_DRIFT_S3_REGION` | Region S3 rõ ràng |
| `BUSINESS_DRIFT_S3_ENDPOINT` | Tùy chọn endpoint MinIO/S3; tuân thủ TLS/local profile |
| `BUSINESS_MONITORING_MODEL_VERSION` | Tùy chọn binding model đã xác minh |
| `BUSINESS_ML_URL` | Tùy chọn HTTPS origin cho readiness observation ML |

S3 dùng credential chain tiêu chuẩn của boto3; GET exact object versions và LIST
prefix chỉ trong bucket đã cấu hình. Không nhúng keys vào frontend/runtime metadata.
Chưa có giao thức rotation cursor keys; đổi khóa làm các cursor cũ không còn hợp lệ.

Metrics Consumer bổ sung `maintai_brief_job_attempts_total{status}`,
`maintai_brief_queue_oldest_age_seconds` và `maintai_drift_catalog_scan_age_seconds`.
Không dùng job/Alert/machine IDs hoặc prompt làm labels. Queue age bằng0 khi không
còn active job; catalog scan age bằng-1 trước successful observation.

## Main UI

Copilot ở Alert Detail cho phép chọn intent, yêu cầu generation theo quyền, xem
checklist/citations và các trạng thái non-grounded/unavailable. POST mất phản hồi
được đối soát qua GET; retry do người dùng yêu cầu giữ nguyên idempotency key và
intent, không tự gửi POST lặp. Poll active job có giới hạn; SSE yêu cầu đọc lại REST.
Citations hiển thị document/version/chunk/page/section; chưa có contract tải PDF
nên không dựng URL tùy ý hoặc render provider HTML.

Trang Monitoring đọc Business REST/SSE, hiển thị service observations, report list/
detail/filters và catalog freshness. `brief.ready`, `brief.updated`, `drift.updated`
được lưu và PG NOTIFY trong transaction; mỗi API replica LISTEN để fan-out. Mất
notification được phục hồi bằng REST snapshot.

## Kiểm chứng và giới hạn

Tests dùng PostgreSQL/Kafka/MinIO thật; RAG HTTP, issuer, ML/policy/model provenance
và committed Drift publisher là fixtures có nhãn. Không gọi Groq hoặc tải embedding
model cho nghiệm thu implementation. Backend package chứa canonical schema bundle;
sdist được build lại thành wheel ngoài checkout để kiểm tính độc lập source release.

Provider RAG hiện có pipeline/citations nhưng service auth, optional-null serialization
và provenance prompt/index/embedding còn cần nghiệm thu tích hợp riêng. Drift publisher
và heartbeat bridge thật chưa được chứng minh bởi fixtures. Manifests/credentials/
rollout shared và toàn hệ thống không được suy từ kết quả local.

### Bằng chứng local ngày04/11/2026

| Kiểm tra | Kết quả |
| --- | --- |
| Processing/Business unit, SQL, Kafka và container với đầy đủ opt-in flags | 582 tests đạt, không skip |
| Client RAG, bao gồm7ca HTTP status/body bound bổ sung sau regression | 13 tests đạt |
| Frontend unit | 33 tests đạt |
| Browser M4–M7 trên project Kafka/DB/MinIO tạo mới | 17 tests đạt |
| Copilot và Monitoring mobile sau chỉnh table scroll | 2 tests đạt |
| Backend sdist → wheel ngoài checkout | Đạt; canonical schema bytes khớp |
| Business API/Consumer và Main UI images | Build đạt; installed imports/schema và Nginx smoke đạt |
| HTTP contracts, Kafka immutable bundle, repository links/runtime metadata | Đạt |
| Ruff, TypeScript, Prettier, frontend production build | Đạt |

Local dùng Compose test với PostgreSQL36432/Kafka40092/MinIO39000; workflow CI
cấu hình PostgreSQL35432/Kafka39092. Các kết quả trên là kiểm chứng local, không
đại diện một GitHub Actions run hoặc shared rollout. Build frontend còn cảnh báo
bundle lớn hơn500KiB; không có build/type-check error.
