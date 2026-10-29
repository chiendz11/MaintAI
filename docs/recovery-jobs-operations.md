> **Source update23/10 — SDK0.2.0/bundle1.1.0:** keyed runner/coordinator,
> durable handler/DB fence, ordered relay, migration002 và transport wrappers đã
> có source/tests. Full app callbacks/probes/images/deployment còn cần tích hợp.
> [Implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) và
> readiness §19 ghi bằng chứng, commands và giới hạn; các snapshot cũ đọc theo ngày.

# RECOVERY-01 — Jobs bền vững, phục hồi và vận hành

Chi tiết kế thừa1.3.2, rà soát khi nhập target1.4 ngày2026-10-19. Giữ các quyết định
durable jobs, Drift publication/catalog và recovery chưa bị v1.4 thay thế; không
override version/payload mới. Đây là thiết kế, không phải runbook đã được chạy.
Xem [thứ tự ưu tiên và quyết định dự án](README.md).

## 1. Transaction và crash boundaries

| Runtime | Durable boundary trước ACK/offset | Crash/retry |
| --- | --- | --- |
| Ingestion | Kafka broker ACK trước HTTP202 | ACK bị mất -> Sensor gửi lại cùng ID/payload |
| Processing | Canonical event/result/risk + outbox trong một DB transaction | Trước commit replay có thể gọi ML lại; sau commit dedupe, không lặp business effects |
| Relay | Publish ACK rồi mark published | Publish lại được, result/signal ID giữ nguyên |
| Business | Inbox + projection/alert/audit transaction trước offset | Dedupe side effects, CAS ordering; không commit offset vượt event chưa hoàn tất |
| Raw Storage | Object + manifest bền vững trước offset | Deterministic batch identity, reconcile uncertain upload |
| M6 snapshot/export — Read API | Snapshot/job metadata commit trước response/202 | Lease/fencing/retry; original outcomes read-only; không chạy trong Worker |
| Processed archive | Hoãn sau M6 | M6 không bật cleanup dữ liệu gốc; thiết kế cũ không tự kích hoạt |

Kafka/PostgreSQL/S3/external ML không có transaction exactly-once chung. In-flight
record hoàn tất ngoài thứ tự chỉ được commit contiguous completed offset frontier
theo từng partition. Poison/conflict cần durable quarantine rồi mới advance.
Không dùng string event_id, string epoch hoặc completion time làm source ordering.

## 2. Durable jobs chung

**M6-HISTORY-01,29/10:** Export runner thuộc runtime Read API, cùng Deployment,
pool/budgets riêng. Worker chỉ realtime; không thêm processing-maintenance.
Read API metadata credential chỉ ghi history_meta, không ghi source/outcome/outbox.
Giữ [decision](M6_HISTORY_ARCHITECTURE_DECISION.md) và
[raw handoff](M6_RAW_ARCHIVE_HANDOFF.md) khi áp dụng các quy tắc recovery dưới đây.
Raw catalog có thể lag sau commit; reconciliation phải phục hồi từ durable raw
manifests, không chỉ từ Kafka chưa commit. DB-only demo không chứng minh full M6.

Logical jobs tables thuộc owner, gồm job_id/type, actor/scope, idempotency key +
payload hash, status, immutable input refs, attempts/max_attempts, next_attempt_at,
lease_owner/lease_until/fencing_token, deadline, last_error, output refs, timestamps.
Unique actor+job_type+key; khác hash409. API chỉ trả202 sau transaction commit.

Runner claim bằng `FOR UPDATE SKIP LOCKED` trong transaction ngắn; commit lease
trước network call. Completion dùng CAS job_id+fencing_token, lease heartbeat;
expired lease được reclaim. Không giữ DB lock khi gọi RAG/S3. Retry exponential
backoff+jitter, budget3 attempts/deadline5m cho brief; export30m, archive60m là
profile cấu hình được. Retryable network/429/5xx; validation/auth/input errors
terminal. Stale worker không ghi đè output của runner mới.

Process restart không mất QUEUED/RUNNING jobs. Provider call có thể bị lặp nếu
ACK mất; dùng job_id làm provider idempotency key khi provider hỗ trợ, chỉ commit
một output canonical ở owner. Không hứa LLM execution exactly-once.

## 3. Copilot executor đã chọn

Business API POST `/api/v1/alerts/{alert_id}/brief`: authorize TECHNICIAN/ADMIN,
validate intent, freeze alert evidence/citation context refs, insert brief job và
pending facade trong cùng business transaction; return202 job_id/status_url.
Existing active job cùng alert+intent+evidence version trả cùng202; key khác body409.

Business Consumer chạy bounded job loop riêng với Kafka loops. Runner lấy context
gốc qua Processing Read API nếu snapshot evidence cần bổ sung; không thay bằng
live telemetry. Gọi internal RAG `/api/v1/briefs`, persist result/job state và audit
transaction, NOTIFY sau commit. API GET brief/job chỉ đọc, không chạy lại LLM.

Job technical status SUCCEEDED không đồng nghĩa GROUNDED: business outcome vẫn là
GROUNDED / INSUFFICIENT_EVIDENCE / VALIDATION_FAILED. Exhausted transport/provider
failure -> FAILED, copilot_status UNAVAILABLE. Không evidence không sinh checklist
giả; lỗi brief không cản ACK/RESOLVE hoặc alert creation.
GET `/api/v1/brief-jobs/{job_id}` trả job state; GET alert brief trả facade hiện hữu.
`brief.ready` sau persisted business outcome; failure notify `brief.updated` để UI
refresh; payload chứa job/alert ID và timestamp, không để FE suy đoán từ HTTP202.

## 4. Drift read bridge đã chọn: S3 manifest + Business catalog

Không thêm Drift HTTP service và không cho Business truy cập drift_db.
Drift CronJob persist report + pending-publication ledger trong drift_db; publish
batch metrics rồi mới advance detector checkpoint theo invariant hiện có. Upload
immutable report object rồi committed manifest được retry độc lập bằng ledger. Publication retry có
ledger trạng thái riêng; failed upload không được gắn report available trên UI.
Report_id deterministic cho logical run/window/model/reference; detector retry
không tạo nhiều bản canonical. Checkpoint detector không bỏ qua pending publication.

Manifest chứa schema_version1.0.0, report_id, computed_at, model_version,
dataset_version, overall_status và report_ref(bucket/key/version_id/sha256),
publication_at. Key `drift-report-manifests/published/YYYY-MM-DD/<report_id>.json`,
partition theo **publication** date, không theo window date. Immutable conditional
publish; same report ID different checksum -> conflict/quarantine.

Business job loop sync catalog mỗi60s: enumerate hôm nay+hôm qua, upsert metadata
idempotently theo report_id + checksum. Daily full prefix reconciliation khôi
phục khoảng downtime dài; list continuation token chỉ trong một scan, không dùng
last lexicographic key làm checkpoint vĩnh viễn. Sau crash repeat scan an toàn.
Catalog lưu last_scan_at/complete_scan_at/error và report object refs, không
clone toàn bộ detector state hoặc checkpoint. Không tự kết luận STABLE từ thiếu report.

Business GET `/api/v1/drift-reports` đọc metadata cursor; GET `/{report_id}` lấy
đúng version S3, verify checksum rồi trả existing DriftReport schema1.0.0.
No reports ->200 empty; unknown ID404; source/corruption503 DRIFT_REPORT_UNAVAILABLE.
List default20/max100, filter model_version/overall_status, keyset computed_at
DESC/report_id DESC trong publication snapshot. Opaque cursor bind query/scope.
List response có catalog_as_of/stale; catalog sync lỗi vẫn có thể trả cached list
với stale=true; không được mô tả là danh sách đầy đủ realtime. Detail chỉ200 khi
report object đã đọc/verify; overall_status ERROR vẫn là một report hợp lệ.
`drift.updated` chỉ sau catalog commit; update trễ không chạy lại detector.

## 5. Projection rebuild và reconciliation

Business projection_generation có ACTIVE/BUILDING/FAILED và immutable generation ID.
Rebuild latest telemetry/prediction/processing bằng original records/results trong
archive + hot source qua owner APIs, không chạy lại inference. Ghi shadow tables,
CAS theo machine sequence; giữ nguồn event và versions. Live consumer ghi active
generation và durable catch-up journal trong thời gian rebuild; cutover khóa ngắn,
apply journal đến captured frontier, so sánh counts/sample IDs rồi atomic swap.
Nếu journal retention/gap không đủ -> abort, giữ active generation cũ.

Rebuild mode **không gọi Alert Manager**, không tái tạo alert/audit/feedback từ
raw predictions. Alert lifecycle là business data, phục hồi bằng DB backup/audit
và inbox-aware replay được vận hành explicit. Không dùng xóa inbox rồi replay
production như một cách rebuild UI. Sau swap notify resync, SSE clients GET snapshot.

Inbox/event identity ledger retention >= max(Kafka retention, approved replay
window)+safety margin. Replay vượt ledger window chỉ chạy chế độ maintenance với
archive identity reconciliation và side effects bị khóa; không hứa dedupe vô hạn.
Same event_id different canonical source body -> conflict quarantine, không ghi đè history.
Canonical source body gồm schema_version/event_id/machine_id/epoch/sequence/received_at/
source/features; loại ingress server_accepted_at, request/correlation IDs. Retry có
receipt mới nhưng cùng canonical source giữ original logical result/source time.
Raw archive vẫn giữ mọi Kafka receipt với topic/partition/offset, không mất delivery evidence.

## 6. Backpressure, isolation và scaling

- HTTP rate limit có429/Retry-After; Sensor bounded durable spool có quota/TTL,
  metrics và explicit overflow policy; không ACK giả khi Kafka unavailable.
- Kafka loops không bị S3/LLM jobs block; bounded queues/concurrency, heartbeat và
  max.poll budget tương thích công việc. Pause partition có kiểm soát khi pool đầy.
- ML request timeout2s/tối đa1 retry giữ baseline, nhưng E2E2s là mục tiêu riêng;
  lỗi ML không được dùng để suy ra NORMAL.
- Pool DB dashboard, history, maintenance và RAG riêng/bounded. Mặc định một
  archive job và hai brief jobs đồng thời mỗi runner; phải tính tổng khi tăng replicas.
- API scale bằng RPS/p95/connections; Worker bằng lag/age/ML throughput; Relay bằng
  pending age; Storage bằng lag/archive age; không chỉ CPU.
- Partition assignment giới hạn số consumer members; target PROC-ORDER-01 cho
  nhiều keyed tasks trong một member, khác máy cùng partition chạy đồng thời.
  SDK hiện vẫn một in-flight/runner.12 partitions không là benchmark số máy.
  Hot key/offset hole vẫn gây partition backpressure; đổi partition count cần
  drain/fence/cutover để giữ per-machine source order.
- Autoscaling không làm tăng năng lực DB/ML vô hạn; admission limit phải xét toàn
  cluster. UI coalescing không thay thế broker retention hoặc processing capacity.

## 7. Rolling update, schema và shutdown

Consumers accept old/new versions trước, sau đó producer cutover union2.0.0;
unknown version quarantine/alert, không đoán shape. Chỉ bỏ old reader khi backlog
và replay window hết; không downcast failure result thành NORMAL prediction.
DB migrations expand -> deploy compatible readers/writers -> backfill -> contract
sau khi old pods kết thúc. Git rollback không tự undo database hoặc side effects.

HTTP readiness trước nhận traffic, drain SSE và in-flight requests; FE reconnect
REST resync. Consumer shutdown stop admission, finish budgeted in-flight commits,
commit contiguous offsets, release job leases hoặc để lease expire; unfinished
work replay. Rebalance revoke làm tương tự; không commit công việc chưa durable.
Sensor một controller với persistent sequence/state/spool, Recreate; strict
failover/multiple replicas cần lease/fencing, không chỉ số replicas=1.

## 8. Observability và acceptance

Catalog và exact update/reset semantics theo [OBS-01](OBSERVABILITY_IMPLEMENTATION.md),
ưu tiên shorthand metric names kế thừa; MLflow dùng workflow riêng ở §9 bên dưới.

Trace correlation xuyên HTTP/Kafka/job/archive bằng event_id/result_id/job_id và
correlation_id trong logs/traces; IDs không làm Prometheus labels. Metrics:
ACK/prediction/visible-alert latency, lag/oldest age, pool saturation, retry,
quarantine, lease expiry, archive verified/cleanup lag, history coverage/errors,
brief queue age, drift catalog scan age, SSE reconnect/resync.

Required future scenarios: crash ở mỗi transaction boundary, publish ACK loss,
rebalance lúc inference, out-of-order results, multi-API fan-out, S3 outage vượt
archive cadence, stale lease completion, projection rebuild đồng thời live input,
RAG timeout, catalog downtime+full reconcile và rolling schema2 consumer-first.
Đây là acceptance specification, chưa phải các tests đã viết hoặc đã pass.


## 9. MLflow operation recovery — MLFLOW-01

Chi tiết ở [MLFLOW_INTEGRATION_GUIDE.md](MLFLOW_INTEGRATION_GUIDE.md) §6–8.
Training orchestration ledger do platform quản lý, khác Business Copilot/job DB;
không thêm training SQL tables vào mlflow_db hoặc DB ứng dụng khác.

- training_id/run_key, owner-generation, lease/heartbeat, write-ahead create
  intent và remote IDs phải checkpoint bền vững theo phase trước side effect kế.
- Create-run/model-version timeout,5xx hoặc crash có thể đã commit remote. Không
  tự tạo lại theo search0; search-before-create không có unique guarantee.
- Reconciliation duyệt pages, đối chiếu run/source/artifact hashes;0/multiple
  matches giữ UNKNOWN/triage. Lease expiry không hủy in-flight request cũ;
  không cho writer mới create khi intent trước unresolved.
- Re-log known-run metrics/artifacts giữ timestamp/key/path/bytes; verify partial
  writes, không tự serialize/train lại khi chỉ publication bị lỗi.
- Model-result đã commit thì retry cùng immutable result/change identity;
  RELEASE_DISPATCHED/GitHub204 không chứng minh PR/merge/deploy thành công.

Snapshot GitOps helper hiện có lease/model-result checkpoint nhưng chưa đủ
step-level run/version recovery và fencing/heartbeat của chuẩn này. Đây là gap
implementation, không tạo ledger adapter chỉ bằng ví dụ Python trong docs.

## 10. PROC-ORDER-01 — recovery cho keyed tasks và ordered outbox

Theo [đặc tả chuẩn](PROCESSING_CONCURRENCY_AND_ORDERING.md) §4–7; SDK0.2.0 đã có
keyed/ordered/fenced implementations và tests; xem implementation report/readiness §19.

- Mỗi partition có receipt ledger và safe completed prefix. Một lower-offset
  task chưa durable giữ frontier dù các máy khác đã commit DB; crash replay
  các receipts phía sau và dedupe theo logical identity/hash.
- Revoke dừng admission partition, drain hữu hạn và commit prefix khi còn được
  phép; lost assignment không commit. In-memory generation chỉ chặn offset,
  durable owner generation fence mới chặn late SQL mutation. Không đợi HTTP
  vô hạn trong rebalance callback hoặc giữ transaction lock suốt inference.
- Timeout không chứng minh HTTP/producer đã hủy. Late completion không nhả key
  lần hai, không tạo result mới; unknown DB commit đọc lại durable outcome.
- Head relay đang backoff/ACK unknown chặn successor cùng topic/key. New lease
  owner reconcile/retry cùng message bytes; không mark sent vì request đã enqueue.
  DB fencing không hủy old network publish, downstream vẫn inbox/CAS.
- Stop/drain row-only legacy relay và unfenced Worker trước enable capability
  mới; không chạy mixed writers vượt key cursor. Rollback không reset cursor/
  dispatch_seq/inbox/outbox, cần compatible binary hoặc reviewed maintenance.
- Giới hạn queued/fetched/in-flight/done-behind-hole bằng records và bytes.
  Nếu pause một partition vì hot key, các máy cùng partition có thể chờ; theo
  dõi queue wait/frontier stall và downstream saturation, không drop để giảm lag.

Future recovery drill phải có500 pending/501 completed, stale handler sau revoke,
ML timeout còn chạy, hai relay tranh một key, head retry/ACK loss, graceful drain
hết budget, source-order conflict và backlog catch-up. Chưa thực thi các drills
production này đầy đủ; source tests/evidence cụ thể ở implementation report.
