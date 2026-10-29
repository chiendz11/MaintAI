# M5 — Alert lifecycle, audit, feedback và giao diện cảnh báo

## 1. Phạm vi và baseline

M4 được squash merge qua PR12 vào main ở `30db0f6`. Nhánh M5 là
`m5-alert-lifecycle-audit-feedback`, worktree riêng để không sửa các bản PDF đang thay đổi ở
checkout gốc. Phạm vi triển khai theo M5-PLAN.md do người dùng cung cấp.

Không thêm Deployment, logical DB, queue hoặc dependency. Business Consumer có
ba serial Kafka loops; Business API/Main UI và Processing Read API giữ runtime
hiện có. Kafka chỉ dùng outcome1.4.0, SDK0.3.0/bundle2.0.0. SQL001/002 và public
DTO giữ nguyên. Migration mới là owner operation `business_0003_alerts`.

## 2. Luồng hoạt động

```text
HTTP Ingestion → sensor.readings → Processing Worker → processing_db + outbox
                                                           │
                                                      Outbox Relay
                                                           │
                                                     alert.signals
                                                           │
                 Processing Read API ← evidence HTTP ← Business Alert Consumer
                                                           │
                                           business_db: Alert/evidence/audit/link
                                                           │
                                         immutable notification + PostgreSQL NOTIFY
                                                           │
                                             mọi Business API replica → SSE
                                                           │
                                         UI invalidates REST list/detail/badges
```

Worker/Relay trong browser fixture thực sự consume/publish Kafka. Model, policy,
registry lookup và issuer là fixtures; endpoint Ingestion là FastAPI HTTP thật,
không gọi trực tiếp SDK gateway thay cho HTTP. Hai owner DB và application roles
riêng được tạo/dọn trong harness test. Đây là bằng chứng local, không là deployment
production hoặc benchmark100 máy.

## 3. Evidence chính xác và bất biến

Processing Read API thêm `GET /api/v1/internal/predictions/{result_id}`. Repository
join bốn bảng của Processing, kiểm checksum outcome, source locator/identity,
đầy đủ model lineage, prediction/risk và trả HistoryRecord. VALID, INVALID và FAILED đều có hot
point-read; raw_features_wire giữ escaped NUL. Timestamp evidence lấy từ accepted
time, không lấy clock sensor. Schema/ref được load từ SDK đã đóng gói.

Business lấy status của event gốc để tìm result_id, rồi lấy đúng HistoryRecord.
Credential, hai requests và validation có tổng wall budget3s, response tối đa256KiB,
không redirects hoặc browser token. Một bounded worker ngăn queue HTTP vô hạn.
Sai evidence ghi diagnostic sau rollback; dependency unavailable không trả receipt.

Inbox duplicate được SDK kiểm trước callback, vì vậy không fetch evidence lại.
Signal đến trước prediction projection hoặc đến muộn hơn latest sequence vẫn
được xử lý. Business snapshot lưu bytes cùng checksum; đọc Alert sau này không
cần Processing còn online. Không suy risk từ probability hoặc latest telemetry.

## 4. Lưu trữ và concurrency

Migration0003 thêm:

| Bảng | Vai trò |
| --- | --- |
| alerts | Identity/risk/model/policy lúc tạo, lifecycle hiện tại và latest metadata nội bộ |
| alert_evidence_snapshots | HistoryRecord nguồn bất biến và checksum |
| alert_signal_links | Unique signal identity/hash, Alert đích, CREATED/DEDUPLICATED |
| alert_events | Append-only timeline; toàn bộ reason/resolution/note được giữ trong details |
| feedback | Append-only verdict/recommendation feedback và actor từ token |
| alert_notifications | Snapshot SSE đúng transaction, gồm cả dedupe update |

Consumer, API actions và Projector dùng cùng machine-first lock. Actions sau đó
khóa Alert và đọc lại status. Dedupe dùng decision_time từ clock_timestamp sau khi
khóa máy, cùng severity và bốn trạng thái active, cửa sổ cố định
`created_at <= decision_time < created_at + 300s`. Đúng300s tạo Alert mới; không
kéo dài cửa sổ theo updated_at. Nếu có nhiều ứng viên chọn newest created_at/ID.

State/audit/link/evidence/notification được ghi trong owner transaction. API chỉ
trả200/201 sau commit; signal receipt chỉ trả sau durability. Action lặp hoặc
transition trái phép trả409. Terminal không tự mở lại; reading NORMAL không tự
resolve Alert. Gộp signal không sửa original evidence hoặc tạo audit lifecycle giả.

Reader chỉ SELECT. Consumer có INSERT Alert/evidence/audit/link/notification và
UPDATE latest metadata; không có UPDATE lifecycle. API có UPDATE(status,updated_at)
trên alerts, UPDATE(updated_at) trên machines để lấy lock, INSERT audit/feedback/
notification. API không provision máy. Application roles không sửa/xóa evidence,
audit hoặc feedback. Helper chỉ grant cho role tồn tại, không tự tạo credential.

## 5. REST, realtime và UI

List Alert áp scope trước total/pagination, limit50/max200, order created_at DESC,
alert_id DESC; hỗ trợ status/severity/machine. Detail đọc original snapshot cùng
timeline timestamp ASC/ID ASC. Copilot rõ UNAVAILABLE tới M7.

Actions ack/start/escalate/resolve/false-alert kiểm TECHNICIAN/ADMIN và actor_id
khớp principal. Resolve cần resolution, false-alert cần reason. Feedback trả201,
actor lấy từ token, không tự thêm lifecycle event hoặc trigger training.

API đã nối Alert reader thật cho Fleet/Detail trong cùng snapshot transaction.
Không có Alert được trả count0/list rỗng từ DB thật. Resolve/false-alert bỏ ID khỏi
active list, không xóa Alert và không đổi operational state của máy.

Một listener/API process nghe channel M4 và `BUSINESS_ALERT_NOTIFY_CHANNEL`
(mặc định maintai_business_alert_v1). Hint chỉ gồm version/machine hash/generated
notification ID. Alert events không coalesce; hint queue tối đa128, giữ byte budgets
M4. SQL read có timeout. Overflow/mất listener/lỗi snapshot đóng stream để REST resync.

UI có `/alerts` và `/alerts/{alert_id}`, filters, pagination, evidence, timeline,
forms và feedback. VIEWER chỉ đọc. Role hints có claim mapping, backend quyết định
quyền. Request đang chạy khóa nút; không optimistic success. 409 tải lại detail.
Mất phản hồi POST không tự retry: action đối soát REST/timeline; feedback báo
unknown vì AlertDetail không xác nhận feedback. Logout/đổi trang hủy requests.

SSE Alert chỉ invalidation, không xếp state theo timestamp hoặc opaque ID. REST
được refresh khi notification/reconnect/tab resume và mỗi30s visible; response cũ
bị hủy/bỏ. Màn hình máy dùng stream M4 đã multiplex Alert, màn hình khác có một
stream chung. Demo riêng có lifecycle/feedback và nhãn dữ liệu mô phỏng.

## 6. Vận hành, CI và kiểm chứng

Consumer shared cần BUSINESS_PROCESSING_READ_URL HTTPS và
BUSINESS_PROCESSING_TOKEN_FILE chứa credential BUSINESS_SERVICE theo machine scope.
API/Consumer phải dùng cùng Alert channel và khác channel M4. Chạy migration/grants
trước rollout; probes kiểm bảng hiện hành. Không bật cleaner Processing trong M5.
Trước cleanup M6 cần bảo vệ evidence cho backlog signals và archive lookup.

OBS-01 created/dedup/transitions/visibility chỉ tăng sau commit. Duplicate inbox
khác business dedupe. Source time thiếu/sai/skew bỏ latency sample và ghi dropped
sample; đây là DB visibility proxy, không phải browser latency hoặc p95 đã đo.

CI chỉ sửa hai jobs Người1: Processing integration bật flags M5, Main UI cài
Ingestion và bật browser M5. Required gate và jobs owner khác giữ nguyên. Kết quả
GitHub Actions phải được kiểm tra trên đúng commit đã push của nhánh M5.

Các log local nằm ngoài Git ở `/tmp/maintai-m5-*.log`. Kết quả và lệnh kiểm tra
được ghi ở mục 8. Các kết quả này áp dụng cho working tree M5; chúng không thay
thế GitHub Actions của commit được phát hành sau này.

## 7. Phần tiếp theo

M6: history/chart/export/archive/retention và notification cleanup. M7: RAG,
brief jobs/citations/Copilot và Business drift screens. M8: provider thật,
rebuild/recovery và load acceptance. Không có các phần đó trong M5 chỉ vì
prototype hoặc schema đã mô tả chúng. Review bởi thành viên khác và CI của commit
được phát hành vẫn là bước bàn giao riêng; không tự deploy từ worktree này.


## 8. Kiểm chứng và cách tái lập

Các kiểm tra dùng Kafka và PostgreSQL thật trong Docker Compose project riêng
`maintai-m5-local`. Browser chạy qua HTTP Ingestion thật, Worker/Relay và Business
Consumer thật; ML, policy, registry lookup và OIDC issuer là fixtures. Hai API
replicas và hai owner databases được tách trong harness. Không có kết quả tải
100 máy, provider production, AWS hoặc k3d deployment trong lần triển khai này.

| Bộ kiểm tra | Kết quả |
| --- | --- |
| Python unit Processing + Business | 276 passed |
| Python integration Processing + Business, gồm Docker images | 141 passed, không skip |
| Ingestion unit + contract | 142 passed |
| Ingestion HTTP/Kafka + image integration | 7 passed |
| Frontend unit | 27 passed |
| Browser Chromium, live pipeline và demo/mobile | 10 passed |
| HTTP contracts | 6 OpenAPIs, 55 operations, 656 local refs, 103 examples/default checks và 64 ca positive/negative đạt |
| Python lint/format; frontend type/format/build; pip check | Đạt |
| Metadata | 38 document hashes, 580 links/anchors, 14 app + 1 platform runtime descriptors đạt |
| Kafka bundle lock | Bundle 2.0.0; 30 immutable inputs giữ nguyên |
| Đóng gói | Build Business/Processing wheels; import 11 modules ngoài checkout; migration M5 và HistoryRecord refs có trong package |
| Container | Build Worker/Relay/Read API/Business API/Consumer/Ingestion/UI; installed-image và UI smoke đạt |
| Cleanup browser | Không còn databases/roles dùng riêng cho fixture |

Regression mới bảo vệ cả nâng cấp từ M4 đã có dữ liệu, mốc dedupe 299.999/300/301
giây, signal cũ đến sau latest prediction, severity khác nhau, escalation và
FALSE_ALERT, rollback trước commit, phân quyền DB, hai API replicas và mất phản
hồi POST sau server commit. Hai ca model lineage bị thiếu/raw JSON hỏng đã được
chạy RED trước khi sửa, rồi chạy GREEN cùng suite cuối.

Chạy từ root repo bằng environment đã cài đúng các requirements lock của module:

```bash
python -m pytest --import-mode=importlib \
  processing/worker/tests processing/outbox-relay/tests processing/read-api/tests \
  processing/tests backend/business-api/tests backend/business-consumer/tests \
  -m 'not integration'
python -m pytest ingestion-api/tests/unit ingestion-api/tests/contract
ruff check backend processing
ruff format --check backend processing
ruff check frontend/tests/provider --config backend/pyproject.toml
ruff format --check frontend/tests/provider --config backend/pyproject.toml
python -m pip check
python messaging/scripts/lock_contracts.py --check
python .github/scripts/check_repository.py
```

Integration dùng các flags M2/M3/M4/M5 trong `processing-integration` ở CI và
loopback DSN/bootstrap của Compose test. Chạy `-m integration` với cùng danh sách
paths ở trên. Image names và lệnh build nằm trong job đó; không dùng production
credentials. Ingestion có flags riêng `MAINTAI_INGESTION_INTEGRATION=1` và
`MAINTAI_INGESTION_CONTAINER_TEST=1`. Chạy các suite dùng chung Kafka lần lượt.

Frontend chạy `npm run check`, `npm test`, `npm run build`, `npm run test:e2e`
trong `frontend/`. Browser live cần `MAINTAI_M4_INTEGRATION=1`,
`MAINTAI_M5_INTEGRATION=1`, local DB/Kafka và `MAINTAI_TEST_PYTHON` trỏ tới Python
environment đã cài Business, Processing, Ingestion và SDK. Sau đó chạy
`frontend/tests/provider/verify_cleanup.py` với cùng test DSN.

HTTP contract validator dùng environment riêng cài `contracts/requirements.lock`:
`python -B contracts/v1.3/validate.py`. Không gộp lock này vào environment ứng dụng
chỉ để chạy validator. Metadata/link checks và contract tests chỉ xác nhận phần
chúng kiểm, không là bằng chứng toàn bộ target architecture đã chạy production.

## 9. Danh sách đầy đủ các file

Có **32 file mới và 47 file sửa** trong working tree M5, tính cả source, tests,
CI và tài liệu. Không xóa file của M4 và không sửa PDF đang thay đổi ở checkout gốc.
Bảng này được đối chiếu với `git diff --name-only` và untracked files ở thời điểm
bàn giao; build outputs/test logs không được đưa vào Git.

### Backend, database và nghiệp vụ

| File | Thay đổi | Mục đích |
| --- | --- | --- |
| [backend/README.md](../backend/README.md) | Sửa | Hướng dẫn API, ba consumer loops, evidence HTTP, migration/grants, cấu hình và giới hạn M6/M7. |
| [backend/business-api/runtime.json](../backend/business-api/runtime.json) | Sửa | Khai báo phạm vi runtime Business API đã có Alert actions, feedback, source evidence và SSE. |
| [backend/business-api/src/api/alerts.py](../backend/business-api/src/api/alerts.py) | Mới | REST list/detail, ACK/START/ESCALATE/RESOLVE/FALSE_ALERT và feedback; kiểm body, role, actor và scope. |
| [backend/business-api/src/api/app.py](../backend/business-api/src/api/app.py) | Sửa | Cài Alert routes, metrics và readiness cho các bảng M5; giữ exception envelope và runtime API hiện tại. |
| [backend/business-api/src/api/main.py](../backend/business-api/src/api/main.py) | Sửa | Khởi tạo AlertRepository thật; nối active reader vào Fleet/Detail và repository notification vào SSE. |
| [backend/business-api/src/api/observability.py](../backend/business-api/src/api/observability.py) | Sửa | Đăng ký AlertMetrics trong registry metrics của Business API. |
| [backend/business-api/src/sse/hub.py](../backend/business-api/src/sse/hub.py) | Sửa | Một listener nghe hai channels; phát snapshot Alert bất biến, queue tối đa 128, không gộp mất mutations; đóng stream khi không thể resync an toàn. |
| [backend/business-api/tests/test_alert_api.py](../backend/business-api/tests/test_alert_api.py) | Mới | Kiểm REST với PostgreSQL và JWT: lifecycle, feedback, Fleet count, actor giả, scope, token hết hạn và pagination. |
| [backend/business-api/tests/test_alert_sse_budget.py](../backend/business-api/tests/test_alert_sse_budget.py) | Mới | Kiểm disconnect giữa lúc đọc notification và overflow; listener pump không bị chết, connection budget được giải phóng. |
| [backend/business-api/tests/test_evidence_client.py](../backend/business-api/tests/test_evidence_client.py) | Mới | Kiểm credential service, status → đúng result_id, tổng deadline và từ chối evidence thuộc nguồn khác. |
| [backend/business-consumer/README.md](../backend/business-consumer/README.md) | Sửa | Mô tả handler Alert thứ ba, nguồn evidence, credential, retry, retention và observability. |
| [backend/business-consumer/runtime.json](../backend/business-consumer/runtime.json) | Sửa | Thêm alert.signals/business-alert-v1 và mô tả runtime ba loops trong cùng Deployment. |
| [backend/business-consumer/src/config.py](../backend/business-consumer/src/config.py) | Sửa | Đọc, kiểm tra URL/token file/channel Alert; shared mode yêu cầu credential riêng và cấu hình an toàn. |
| [backend/business-consumer/src/handlers/alert/__init__.py](../backend/business-consumer/src/handlers/alert/__init__.py) | Mới | Factory nối handler Alert với cấu hình hiện tại; thiếu provider phải fail closed. |
| [backend/business-consumer/src/main.py](../backend/business-consumer/src/main.py) | Sửa | Khởi tạo handler telemetry/prediction/Alert và EvidenceClient, dùng chung runtime Consumer. |
| [backend/business-consumer/src/observability.py](../backend/business-consumer/src/observability.py) | Sửa | Thêm metrics Alert và tách inbox duplicate khỏi business dedupe. |
| [backend/business-consumer/src/runtime.py](../backend/business-consumer/src/runtime.py) | Sửa | Quản lý vòng đời evidence client, handler thứ ba và health checks của topic/database. |
| [backend/business-consumer/tests/test_alert_lifecycle.py](../backend/business-consumer/tests/test_alert_lifecycle.py) | Mới | Kiểm DB transaction, replay, dedupe mốc 300 giây, snapshot bất biến, signals/actions đồng thời, source trễ, severity, escalation và terminal. |
| [backend/business-consumer/tests/test_alert_migration.py](../backend/business-consumer/tests/test_alert_migration.py) | Mới | Nâng schema M4 đã có máy/projections/inbox lên M5 hai lần; dữ liệu cũ giữ nguyên, bảng Alert mới rỗng và revision đúng. |
| [backend/business-consumer/tests/test_alert_permissions_metrics.py](../backend/business-consumer/tests/test_alert_permissions_metrics.py) | Mới | Kiểm grants tối thiểu, cấm sửa evidence/audit, metrics sau commit và rollback không còn receipt/notification. |
| [backend/business-consumer/tests/test_alert_sse.py](../backend/business-consumer/tests/test_alert_sse.py) | Mới | Hai API replicas nhận đủ các snapshot theo lifecycle; scope được giữ, snapshot cũ không bị thay và malformed hint bị từ chối. |
| [backend/business-consumer/tests/test_installed_flow.py](../backend/business-consumer/tests/test_installed_flow.py) | Sửa | Kiểm các image cài đặt thật, credential giữa owners, quyền tối thiểu, Alert/Fleet và ACK qua HTTP. |
| [backend/business-consumer/tests/test_kafka_runtime.py](../backend/business-consumer/tests/test_kafka_runtime.py) | Sửa | Chuẩn bị topic/group Alert trong Kafka test; hỗ trợ Worker Consumer thật cho browser harness. |
| [backend/business-consumer/tests/test_runtime.py](../backend/business-consumer/tests/test_runtime.py) | Sửa | Cập nhật kỳ vọng ba consumer loops và kiểm runtime shutdown/health. |
| [backend/migrations/versions/0003_alerts.py](../backend/migrations/versions/0003_alerts.py) | Mới | Migration cộng thêm sáu bảng Alert/evidence/link/audit/feedback/notification và indexes; không sửa migrations cũ, không seed hoặc tạo production role. |
| [backend/pyproject.toml](../backend/pyproject.toml) | Sửa | Đóng gói alerts/audit/feedback và Alert handler trong Business wheel; không thêm dependency. |
| [backend/shared/alerts/__init__.py](../backend/shared/alerts/__init__.py) | Mới | Khai báo package nghiệp vụ Alert để dùng chung và đóng gói trong wheel. |
| [backend/shared/alerts/domain.py](../backend/shared/alerts/domain.py) | Mới | Quy tắc lifecycle và đối chiếu signal với evidence: source/result/model/policy/risk, severity, thời gian quyết định và feature allowlist. |
| [backend/shared/alerts/evidence.py](../backend/shared/alerts/evidence.py) | Mới | Đọc status rồi point-read qua HTTP với service token; tổng budget 3 giây, response hữu hạn, không redirect và không tạo queue HTTP vô hạn. |
| [backend/shared/alerts/handler.py](../backend/shared/alerts/handler.py) | Mới | Dùng inbox/transaction SDK; duplicate không fetch lại evidence, effects commit nguyên tử và conflict diagnostic được lưu sau rollback. |
| [backend/shared/alerts/metrics.py](../backend/shared/alerts/metrics.py) | Mới | Counters created/deduplicated/transitions và histogram accepted-to-visible; loại mẫu latency không hợp lệ. |
| [backend/shared/alerts/notifications.py](../backend/shared/alerts/notifications.py) | Mới | Ghi snapshot SSE và pg_notify cùng transaction; hint chỉ chứa version/hash/generated ID, parser kiểm kiểu và giới hạn kích thước. |
| [backend/shared/alerts/repository.py](../backend/shared/alerts/repository.py) | Mới | SQL tạo/gộp Alert, list/detail, locks, actions, feedback, active reader và đọc immutable notification. |
| [backend/shared/audit/__init__.py](../backend/shared/audit/__init__.py) | Mới | Append lifecycle audit, actor, note và toàn bộ reason/resolution vào dữ liệu nội bộ. |
| [backend/shared/feedback/__init__.py](../backend/shared/feedback/__init__.py) | Mới | Append feedback cùng actor và timestamp; không đổi lifecycle hoặc kích hoạt training. |
| [backend/shared/persistence/permissions.py](../backend/shared/persistence/permissions.py) | Sửa | Cấp quyền tách reader, Consumer và API; API chỉ sửa lifecycle, Consumer chỉ sửa latest metadata; lịch sử append-only. |

### Frontend và browser harness

| File | Thay đổi | Mục đích |
| --- | --- | --- |
| [frontend/README.md](../frontend/README.md) | Sửa | Hướng dẫn Alert routes, phân quyền, POST unknown outcome, SSE/REST resync và browser pipeline HTTP thật. |
| [frontend/playwright.config.ts](../frontend/playwright.config.ts) | Sửa | Cho phép chọn interpreter fixture bằng MAINTAI_TEST_PYTHON; giữ isolation và graceful shutdown. |
| [frontend/runtime.json](../frontend/runtime.json) | Sửa | Ghi phạm vi UI cảnh báo đã triển khai; history/Copilot/monitoring còn thuộc pha sau. |
| [frontend/scripts/generate-validators.mjs](../frontend/scripts/generate-validators.mjs) | Sửa | Sinh validators Alert DTO từ schemas đã freeze thay vì tự nới payload. |
| [frontend/src/api/client.ts](../frontend/src/api/client.ts) | Sửa | Client list/detail/actions/feedback, Alert SSE, hủy request và xử lý mutation mất phản hồi; không tự retry POST. |
| [frontend/src/api/codec.ts](../frontend/src/api/codec.ts) | Sửa | Parse Alert DTO và union SSE; giữ BigInt và order guards của telemetry/prediction/processing. |
| [frontend/src/api/demo-alerts.ts](../frontend/src/api/demo-alerts.ts) | Mới | Dữ liệu/demo lifecycle/feedback cho màn hình Alert mà không phụ thuộc backend live. |
| [frontend/src/api/demo.ts](../frontend/src/api/demo.ts) | Sửa | Nối DemoApi với mô phỏng Alert riêng; live vẫn không fallback sang dữ liệu demo. |
| [frontend/src/app/App.tsx](../frontend/src/app/App.tsx) | Sửa | Bật menu/routes Alert, truyền role hints/subject và chọn một SSE stream phù hợp màn hình. |
| [frontend/src/app/config.ts](../frontend/src/app/config.ts) | Sửa | Thêm mapping claim roles cấu hình được cho OIDC UI hints. |
| [frontend/src/app/session.ts](../frontend/src/app/session.ts) | Sửa | Đọc subject/roles để quyết định hiển thị form; backend vẫn kiểm quyền thực. |
| [frontend/src/features/alerts/AlertDetailPage.tsx](../frontend/src/features/alerts/AlertDetailPage.tsx) | Mới | Evidence nguồn, prediction/risk/model/policy, timeline, actions theo trạng thái, form resolution/reason và feedback; Viewer chỉ đọc. |
| [frontend/src/features/alerts/AlertsPage.tsx](../frontend/src/features/alerts/AlertsPage.tsx) | Mới | Danh sách Alert có lọc status/severity/machine, phân trang, loading/empty/error và đường mở chi tiết. |
| [frontend/src/features/alerts/useRefresh.ts](../frontend/src/features/alerts/useRefresh.ts) | Mới | Refresh khi có invalidation, tab visible trở lại và mỗi 30 giây; cleanup listeners khi unmount. |
| [frontend/src/features/fleet/FleetPage.tsx](../frontend/src/features/fleet/FleetPage.tsx) | Sửa | Refresh active Alert badges sau invalidation từ SSE hoặc mutation. |
| [frontend/src/features/machines/detail/MachinePage.tsx](../frontend/src/features/machines/detail/MachinePage.tsx) | Sửa | Refresh metadata/active Alert của máy và bỏ response cũ sau invalidation; giữ ba trục realtime độc lập. |
| [frontend/src/realtime/alerts.ts](../frontend/src/realtime/alerts.ts) | Mới | Event invalidation, danh sách actions hợp lệ, thông báo kết quả chưa xác định và reconnect stream Alert. |
| [frontend/src/styles.css](../frontend/src/styles.css) | Sửa | Bố cục Alert list/detail/forms/timeline, bảng cuộn và responsive mobile. |
| [frontend/src/types/alerts.ts](../frontend/src/types/alerts.ts) | Mới | Các TypeScript types/interfaces cho Alert DTO, actions, feedback và client. |
| [frontend/src/types/validators.d.ts](../frontend/src/types/validators.d.ts) | Sửa | Type declarations cho validators Alert được sinh. |
| [frontend/tests/browser/alerts.spec.ts](../frontend/tests/browser/alerts.spec.ts) | Mới | Browser live: tạo/xử lý/feedback, Viewer chỉ đọc và mất phản hồi ACK/feedback sau server commit mà không POST lặp. |
| [frontend/tests/browser/live.spec.ts](../frontend/tests/browser/live.spec.ts) | Sửa | Regression M4 trên pipeline có Alert source thật; INVALID/unavailable không tự tạo/đóng Alert. |
| [frontend/tests/browser/ui.spec.ts](../frontend/tests/browser/ui.spec.ts) | Sửa | Thêm kiểm thao tác Alert bằng bàn phím trên mobile; giữ CSP, deep links và ba panel realtime. |
| [frontend/tests/provider/http_ingestion.py](../frontend/tests/provider/http_ingestion.py) | Mới | Chạy ứng dụng Ingestion HTTP thật và signed service credential fixture; thu ACK Kafka để truy vết test. |
| [frontend/tests/provider/server.py](../frontend/tests/provider/server.py) | Sửa | Harness hai Business API replicas, Consumer, Processing Read API, Ingestion HTTP và ML/policy/OIDC fixtures, tách owner DB/roles. |
| [frontend/tests/provider/verify_cleanup.py](../frontend/tests/provider/verify_cleanup.py) | Sửa | Kiểm sau browser không còn disposable owner databases/roles, gồm API và Processing reader. |
| [frontend/tests/provider/worker.py](../frontend/tests/provider/worker.py) | Mới | Worker SDK Consumer đọc Kafka thật và Relay phát Kafka thật trong harness; chỉ ML/registry/policy dùng fixtures. |
| [frontend/tests/unit/alerts.test.ts](../frontend/tests/unit/alerts.test.ts) | Mới | Kiểm Alert SSE không phụ thuộc sequence, không retry mutation POST và terminal lifecycle trong demo. |

### Processing evidence

| File | Thay đổi | Mục đích |
| --- | --- | --- |
| [processing/read-api/README.md](../processing/read-api/README.md) | Sửa | Mô tả point-read, checksum/provenance, auth, grants bốn bảng và giới hạn hot DB/retention. |
| [processing/read-api/runtime.json](../processing/read-api/runtime.json) | Sửa | Khai báo hot point-read phục vụ evidence đã có; history/archive/export chưa được tuyên bố triển khai. |
| [processing/read-api/src/api/app.py](../processing/read-api/src/api/app.py) | Sửa | Endpoint nội bộ lấy HistoryRecord theo result_id; BUSINESS_SERVICE/machine scope, 404/503 theo contract. |
| [processing/shared/persistence/evidence.py](../processing/shared/persistence/evidence.py) | Mới | Kiểm checksum, identity, Kafka locator, accepted time, đủ model lineage và risk; giữ raw JSON/escaped NUL, validate HistoryRecord từ SDK bundle. |
| [processing/shared/persistence/status.py](../processing/shared/persistence/status.py) | Sửa | Query owner-local join sensor_events/results/predictions/risk_decisions để đọc đúng result_id. |
| [processing/worker/tests/test_evidence_repository.py](../processing/worker/tests/test_evidence_repository.py) | Mới | Kiểm ba terminal outcomes, checksum/risk/source/model bị hỏng, raw JSON lỗi, escaped NUL và sensor clock khác accepted time. |

### CI, contracts và tài liệu

| File | Thay đổi | Mục đích |
| --- | --- | --- |
| [.github/workflows/ci.yml](../.github/workflows/ci.yml) | Sửa | Bật M5 trong integration/browser của Người 1; cài Ingestion cho harness HTTP thật. Giữ nguyên required gate và các jobs owner khác. |
| [AGENTS.md](../AGENTS.md) | Sửa | Hướng dẫn coding agent đọc báo cáo M5 và giữ invariants evidence, machine-first lock, POST không rõ kết quả; cập nhật trạng thái source. |
| [README.md](../README.md) | Sửa | Cập nhật phạm vi M1–M5 đã có code và phân biệt các Dockerfile ML/RAG còn là placeholder. |
| [contracts/v1.3/openapi/openapi-processing.yaml](../contracts/v1.3/openapi/openapi-processing.yaml) | Sửa | Cập nhật mô tả hot point-read đã được triển khai M5; không đổi wire DTO/schema/version. |
| [docs/M5_IMPLEMENTATION_REPORT.md](../docs/M5_IMPLEMENTATION_REPORT.md) | Mới | Tài liệu bàn giao này: flow, semantics, quyền, vận hành, kiểm chứng và danh sách đầy đủ các file M5. |
| [docs/README.md](../docs/README.md) | Sửa | Thêm mục tra cứu báo cáo M5 trong index tài liệu. |
| [docs/implementation-readiness.md](../docs/implementation-readiness.md) | Sửa | Ghi đúng phạm vi M5 có source/tests, fixtures đã dùng và các phần M6–M8 còn lại. |
| [docs/repository-layout.md](../docs/repository-layout.md) | Sửa | Bổ sung bản đồ đường dẫn Alert backend, UI, migration và point-read. |
| [docs/source-manifest.json](../docs/source-manifest.json) | Sửa | Cập nhật hash tài liệu và quyết định M5-ALERT-01; không thay bundle Kafka đã freeze. |


## 10. Trạng thái bàn giao

- M4 đã merge qua PR #12; main commit `30db0f6` có GitHub Actions thành công.
- M5 nằm ở nhánh `m5-alert-lifecycle-audit-feedback`, worktree `../MaintAI-m5/`.
  Việc push nhánh không đồng nghĩa đã merge main hoặc deploy; PR/review/CI của
  commit phát hành được kiểm tra riêng.
- Checkout `../MaintAI/` vẫn giữ nguyên các thay đổi PDF/tài liệu của người dùng.
- Tài nguyên test thuộc project riêng được dừng sau khi kiểm chứng; không thay
  đổi cluster hoặc dữ liệu ứng dụng của nhóm.
- Commit đề xuất: `feat(alerts): add durable lifecycle, evidence, audit and feedback`.
- PR nên mô tả Alert/evidence/lifecycle/feedback, bảng kiểm local ở mục 8 và giới
  hạn fixtures; chạy CI cho đúng commit, đồng thời cần review từ thành viên khác.
