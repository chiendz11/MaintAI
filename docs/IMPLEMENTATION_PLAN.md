> **Source update23/10 — SDK0.2.0/bundle1.1.0:** keyed runner/coordinator,
> durable handler/DB fence, ordered relay, migration002 và transport wrappers đã
> có source/tests. Full app callbacks/probes/images/deployment còn cần tích hợp.
> [Implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) và
> readiness §19 ghi bằng chứng, commands và giới hạn; các snapshot cũ đọc theo ngày.

> **Tích hợp MaintAI — 2026-10-19:** Tài liệu thiết kế mục tiêu v1.4, chưa xác nhận code/DB/GitOps đã migrate. Đọc [docs index](README.md) để áp dụng quyết định nhóm 4 người, Simulator UI riêng và thứ tự ưu tiên; [readiness](implementation-readiness.md) ghi các schema/scaffold chưa được cung cấp.

# MaintAI v1.4 — Kế hoạch tích hợp và nghiệm thu cho con người và AI coding tool

**Phạm vi:** Kế hoạch implementation tương lai trên source thực tế. Hai file Contract/Guide v1.4 đã được nhập vào `docs/`; mã tham chiếu, schemas và scaffold trong `implementation/`, `examples/` hoặc `Outputs/` của bộ bàn giao chưa được cung cấp. Chưa thực hiện các PR source/migration/GitOps hoặc offline tests được nêu dưới đây. Đọc docs index để áp dụng phân công4 người và Simulator UI riêng.

## A. Khảo sát bắt buộc TRƯỚC KHI sửa mã nguồn

1. Đọc cây thư mục source, `pyproject.toml`, dependencies/lockfiles, import paths, startup entrypoints, ORM/Alembic, migrations, Kafka DTOs, Compose/K8s, contracts, fixtures, tests.
2. Tạo `CURRENT_STATE.md`: phân loại `ĐÃ CÓ`, `CÓ MỘT PHẦN`, `CHƯA CÓ`, `KHÁC CONTRACT`, kèm file-path và bằng chứng. Không tạo lại bảng `sensor_events`, `predictions`, `risk_decisions`, `alerts` nếu có thể mở rộng bảng hiện tại.
3. Xác minh owner và auth/ports thực tế của Ingestion, Processing, Business, Storage, Training/MLflow, RAG, Drift, Frontend, GitOps.
4. Mở feature branch; chạy bộ test v1.3.1 để có baseline. Không đổi producer breaking khi chưa có consumer migration plan.
5. Review/freeze v1.4 JSON Schemas và release manifest trước khi chia PR theo module.
6. Đọc [OBS-01](OBSERVABILITY_IMPLEMENTATION.md) và
   [MLFLOW-01](MLFLOW_INTEGRATION_GUIDE.md): metric catalog, timestamp coverage,
   run/registry identities, strict schema mapping và durable retry/unknown policy.

## B. Thứ tự PR và ranh giới commit đề xuất

| PR | Chủ trì | Công việc trên mã nguồn thật | Contract/test phải đạt | Điều kiện hoàn thành |
|---|---|---|---|---|
| 00 — Khảo sát | Integration | Inventory source, DB, K8s, các schema đang tồn tại | Chưa thay đổi behavior | `CURRENT_STATE.md` được review |
| 01 — Contracts | Processing, Business, MLOps | `sensor.rejected`, tagged `inference.results`, release manifests | Fixtures producer cũ + consumer mới, compatibility | Có migration notes |
| 02 — Data identity | Sensor, Processing | Stable ID/sequence qua restart, Kafka topic/partition/offset, DQ hash | Retry/crash/ordering tests | Không inference/Alert trùng |
| 03 — Persistence | Processing, Business | Alembic mở rộng provenance/index, quyền `processing_ro`, outbox/inbox | Upgrade DB đã có dữ liệu cũ | Không mất dữ liệu, xử lý nullable legacy |
| 04 — Quarantine | Ingestion, Storage | Serializer sanitize, ACK-first, topic/group archive riêng | 400/422 sau ACK; 503/504 khi Kafka down; size/auth policy | Rejected audit được và có giới hạn |
| 05 — Outbox/Storage | Processing, Storage | Publish terminal outcomes; tùy chọn archive outcomes | Commit offset sau durable side effects, replay test | S3 manifests xác minh được |
| 06 — History | Processing Read API, FE | Deployment riêng, DB read-only, filters/cursor, truy vấn cold S3 | Page ổn định khi timestamp trùng; INVALID hiển thị | Không quét cả bucket mỗi query |
| 07 — MLflow lineage | ML, MLOps | Dataset manifest/DVC, split SHA, MLflow run tags, registered version, artifact pin | Digest đúng; mismatch startup fail | Tái lập được run/release |
| 08 — Policy/RAG/Drift | Processing, RAG, Drift | Policy SHA; document/prompt/reference version và citations | Alert provenance, grounding, drift mismatch | Truy vết đủ |
| 09 — GitOps | MLOps | Image digest thực, manifest approvals, K8s overlays, readiness/ACL | Rollout/rollback, secret scan | Không còn placeholder; báo effective release |
| 10 — E2E và tải | Cả nhóm | Failure scenarios, lag/latency benchmark, DB contention | E2E acceptance matrix | Có log, metrics, giới hạn đã biết |

**Lưu ý:** Đây là thứ tự phụ thuộc, không bắt tất cả làm nối tiếp. Sau PR 01 chốt fixtures, các nhánh ML/RAG/FE/Processing có thể làm song song. Tránh refactor toàn hệ thống cùng một PR.

UI-01 thêm workstream tại §I dưới đây, phối hợp các PR00–10 theo dependency;
không yêu cầu hoàn tất toàn bộ data platform trước khi phát triển frontend bằng
contract fixtures rõ ràng. Fixture/mock phải có nhãn, không được báo đã nối live API.

## C. Schema và migration cụ thể (phải điều chỉnh theo DB thật)

### C.1. `processing_db`

- **Sensor event:** Cho phép legacy rows thiếu provenance trong migration đầu; new writes cần `event_id`, `machine_id`, `source_server_accepted_at`, `source_schema_version`, `source_topic`, `source_partition`, `source_offset`, `sequence_number`, `dq_status`, `dq_ruleset_version`, `dq_ruleset_sha256`. Schema-rejected nằm trong quarantine archive, không bắt buộc đưa vào bảng này.
- **Prediction:** `prediction_id`, `event_id`, `registered_model_version`, `mlflow_run_id`, `model_artifact_sha256`, `preprocessor_sha256`, `release_id`, `inference_image_digest`, `feature_schema_version`, probability, threshold, `completed_at`.
- **Risk:** `risk_decision_id`, `prediction_id`, `policy_version`, `policy_sha256`, risk level và decision timestamp.
- **Outbox:** `outbox_id`, topic, `result_id`/`signal_id` cố định, schema version, immutable serialized event, status, attempts, last_error, timestamps. Tạo unique indexes/idempotency constraints.
- **Hot History:** Index `(machine_id, source_server_accepted_at DESC, event_id DESC)`, cân nhắc partial/index theo DQ status dựa trên query thật. Không lưu JSON raw không giới hạn.
- **Credentials:** `processing_ro` chỉ SELECT views/tables được duyệt; Worker và Relay có quyền ghi đúng nhiệm vụ. Read API không có quyền ghi DB.

### C.2. `business_db`

- **Latest projections:** telemetry, latest VALID prediction và latest processing status theo machine, cập nhật bằng guard `sequence_number` chống out-of-order.
- **Alerts:** Liên kết source event/prediction/risk, chính xác model/policy/version, source time, decision time và created time. `alert_events` append-only lưu actor, trước/sau state, note, request ID/time.
- **Business inbox:** Unique `(topic, event_id/result_id/signal_id)` và ghi atomically cùng projection/Alert. Alert `RESOLVED`/`FALSE_ALERT` vẫn phải truy vấn được.
- **Brief:** Lưu document/chunk/citation IDs, document version, prompt version, grounding status, generation identity; không copy toàn bộ tài liệu bảo trì vào Business DB.

### C.3. `drift_db`, `knowledge_db`, `mlflow_db`

- Drift report: `model_release_id`, reference/model digest, detector/rule version, input-window watermark/checkpoint, trạng thái và timestamp; không SQL sang DB của owner khác.
- Knowledge chunks: PDF SHA, document version, page/section, chunker/embedding/ingestion versions. Citation lịch sử phải còn tra cứu được sau re-ingestion.
- MLflow metadata schema do **MLflow Server** sở hữu, không viết Alembic migration thủ công cho `mlflow_db`.

**Quy trình migration an toàn:** `ADD nullable columns + reader tương thích → backfill khi CÓ BẰNG CHỨNG → new writer bắt buộc ghi trường mới → đo tỷ lệ hoàn chỉnh → NOT NULL nếu hợp lệ`. Không tự bịa hash cho legacy rows. Dùng `provenance_incomplete` khi không thể khôi phục lineage.

## D. Điểm tích hợp mã tham chiếu vào source thực tế

```text
Sensor Simulator    -> event_id/sequence ổn định + bounded retry spool
Ingestion API       -> producer accepted/rejected (acks=all)
Processing Worker   -> DQ rules identity → model release → risk policy SHA
processing_db       -> durable records và transactional outbox
Outbox Relay        -> Kafka inference.results + alert.signals
Business Consumer   -> inbox idempotent, latest projections, Alert lifecycle
Storage Consumer    -> deterministic S3 keys + checksum + batch manifest
Processing Read API -> indexed hot history + partition-pruned cold S3 lookup
ML Training Job     -> MLflow run_id, dataset/model artifact hashes
GitOps              -> OCI digest pin + immutable release manifest
```

`implementation/python/maintai_provenance` chỉ kiểm tra JSON release/outcome ở môi trường **offline**, không tạo tác dụng phụ mạng/DB/Kafka. Tích hợp logic tương đương vào các module có sẵn, **không** tạo thêm Provenance Microservice. Versioning/provenance là hợp đồng xuyên các service.

## E. API và Kafka consumers cần hoàn thiện

Giữ Public Business API v1.3.1. `GET /api/v1/machines/{machine_id}/sensor-readings` dùng hai cursor `before`, `before_event_id` và thứ tự giảm ổn định. Các filters đề xuất `quality_status`, `from`, `to`, `limit` phải cập nhật OpenAPI/index cùng lúc (chưa mặc định là API đã có). Nếu muốn tab Prediction History đầy đủ, đề xuất trước endpoint `GET /api/v1/machines/{machine_id}/predictions` và review contract.

Schema-rejected không có machine_id phải có query riêng được phân quyền theo request/source/time. `inference.results` **giữ nguyên tên topic**; không tự tạo dual producers hoặc rename khi chưa có migration plan. Live telemetry từ `sensor.readings` vẫn tới Business trước ML. Outcome archive ở Storage là tùy chọn, không bắt buộc topic mới.

## F. Lệnh kiểm tra cho bộ bàn giao (không thay cho E2E hệ thống)

```bash
python implementation/scripts/validate_contracts.py
PYTHONPATH=implementation/python pytest -q implementation/tests
python implementation/scripts/build_k8s.py
python implementation/scripts/build_contracts.py
```

Các bài này kiểm tra JSON Schemas/invariants/mẫu K8s, **không chứng minh Kafka/S3/DB/service mạng chạy thật**. Khi tích hợp repo thực tế phải test Alembic trên DB **đã có dữ liệu**, contracts giữa provider/consumer, Compose E2E, Kafka/S3 kill/recovery, K8s rollout, model digest từ registry/MLflow, tái lập training run và load test. Đính kèm logs, commands, metrics, CI links trong PR review.

## G. Kịch bản E2E và assertions quan sát được

1. **Reading VALID R100:** Kafka ACK → HTTP 202; UI Live R100 có thể đến trước ML; Worker phát `PREDICTION_COMPLETED`; model/policy digests khớp release; S3 raw eventual archive.
2. **RPM âm R101:** Envelope đúng vẫn nhận 202; raw archive; Worker phát `DATA_INVALID`, ghi DQ rule; **ML calls = 0**; UI giữ last valid prediction cũ (có nhãn), History hiện nguyên nhân.
3. **Envelope sai nhưng đã auth:** Publish `sensor.rejected` ACK trước `422`; archive S3; không vào `sensor.readings`/ML. Kafka rejected topic lỗi ⇒ `503`, không trả `422` giả bền vững.
4. **ACK không chắc chắn:** Simulator retry **cùng event_id**; Worker/Business idempotent; không có hai Alerts.
5. **DB commit, relay ngừng:** Outbox vẫn `PENDING`; restart relay phát lại; dù Kafka redelivery vẫn một Alert logic.
6. **Storage Consumer ngừng:** Worker vẫn inference; metric lag tăng; restart archive S3 rồi commit offset, không mất trước retention.
7. **Hai Business API Pods SSE:** Notification fan-out đúng; inbox chống trùng; reconnect dùng REST ordered snapshot.
8. **100 queries History đồng thời:** Read API scale độc lập Worker; đo DB CPU/pool và Worker latency; không nói shared PostgreSQL tự cô lập.
9. **Model v3 → rollback v2:** Prediction mới ghi đúng registered v2 + digest; Alerts v3 cũ giữ nguyên lineage.
10. **Manual v1 → v2:** Brief cũ vẫn cite được chunk v1; nếu không, ghi integrity violation, không đổi citation sang v2.
11. **Drift reference mismatch:** Report `ERROR` cùng model/reference IDs; không thay machine risk/Alert.
12. **Pause M-001:** M-002 vẫn gửi; in-flight reading M-001 vẫn xử lý; Alert OPEN chỉ do technician action mới được RESOLVE.

## H. Definition of Done và giới hạn tích hợp

**Chỉ được đánh dấu `DOCS + REFERENCE CODE VERIFIED`** khi Markdown/JSON/YAML parse, tests offline đạt và sơ đồ render được. **Không được đánh dấu `PROJECT IMPLEMENTED`** khi PR source chưa merge, image digests chưa build, migrations chưa thực thi, services chưa deploy và chưa có integration/E2E/load/recovery evidence.

## I. UI-01 — Workstream bốn UI và phát hành độc lập

Quyết định20/10/2026 tại [UI Architecture](UI_ARCHITECTURE.md), System Contracts §40
và Guide §48. Bảng này là **kế hoạch các PR implementation sau**, không phải các
việc đã thực thi trong lượt sửa docs.

| PR/work item | Owner | Đầu ra | Điều kiện nghiệm thu |
| --- | --- | --- | --- |
| UI-P0 — Capability/contract freeze | Người1 + Người4, phối hợp ML | Map prototype → API/DTO/auth, public origin/config, dependency graph; xác định field còn thiếu | Không tự thêm rate/counters/preview vào ControlState; schema/version review cho capability mới |
| UI-P1 — Business frontend | Người1 | React trong frontend/, REST/SSE, child pages cho máy/history/alerts/briefs/summary | Freshness/order/auth đúng, giữ lifecycle và drift/monitoring APIs |
| UI-P2 — Sensor Control provider | Người4 | List/get/pause/resume/scenario, scoped auth, persistent version/idempotency, backend entrypoint | ACK sau persist; timeout/conflict reconcile; browser đóng không dừng generation |
| UI-P3 — Simulator frontend | Người4 | Đã có React/demo/live client/OIDC/recovery, Dockerfile/config/build/runtime descriptor và local checks | Còn Sensor/IdP live integration, container build và rollout evidence; unsupported live capabilities vẫn ẩn/unavailable |
| UI-P4 — Grafana | Người4 + Người2 review | Năm dashboard kỹ thuật, Prometheus/exporters và Drift report datasource contract | Không fake STABLE, không browser/Business SQL; least privilege/query budgets |
| UI-P5 — MLflow UI integration | Người2 + Người4 | MLflow Server UI, platform access, run/model/artifact/release links | Không clone React UI, alias/run không tự promote model |
| UI-P6 — Per-component release | Người4 + frontend owners | Affected-component CI, images/config digests riêng, GitOps scoped updates/rollback | Giữ CI / required; đổi một UI không làm rollout UI/backend khác nếu contract không đổi |

Simulator UI có thể bắt đầu bằng baseline5 operations; rate/control SSE/batch
commands/counters là follow-up contract riêng, không là field đã có trong OpenAPI.
Grafana SQL report view/role hoặc adapter phải do Drift owner freeze trước
provisioning; không lấy browser credentials hoặc grant writer cho tiện demo.

Evidence cần lưu cho UI-P6: component/source/image/config/GitOps identities,
Pod template before/after, API compatibility result, auth/control-state tests và
rollback logs. Dockerfile và screenshot prototype không thay thế evidence này.
Portal vẫn tùy chọn, không thêm PR bắt buộc tạo service thứ năm.

## J. OBS-01 / MLFLOW-01 — PR implementation sau khi chuẩn hóa docs

Hai hướng dẫn revision1.0.0 đã chốt thiết kế; các đầu việc dưới **chưa implement**
bởi lượt docs. Không lẫn label metadata/artifact schemas với runtime evidence.

| Work item | Owner | Đầu ra cần có | Gate |
| --- | --- | --- | --- |
| OBS-P0 — Inventory/fixture | Mỗi owner, Người4 review | Metric families/type/labels/units/buckets/reset/commit points và series budget; source aliases thật nếu có | Không tên trùng/label ID/cardinality vượt budget; histogram fixture đúng |
| OBS-P1 — App flow | Người1 | Ingestion/Processing/Business collectors, duplicate/commit semantics, exact-event accepted-time source | No metrics on rollback; missing source không fake p95; alert path không bị monitoring chặn |
| OBS-P2 — ML/RAG | Người2/3 | Inference/model info, grounding/citation/retrieval metrics, no-data/error paths | Không dùng FAILURE prediction hoặc insufficient evidence như server error |
| OBS-P3 — Sensor/Storage/Drift | Người4 | Spool/archive metrics, one-hot states, durable report aggregate/required publication/heartbeat | Retry không double count; no-data khác STABLE; strict writer/retention evidence |
| OBS-P4 — GitOps handoff | Người4 + module owners | Scrape target labels/ACL, honor_labels, exporter naming, full5 dashboards, absent/stale/coverage rules | Real scrape→query→panel; trước enable xem thiếu accepted time/metrics coverage |
| MLFLOW-P0 — Compatibility | Người2+4 | Exact client/server patches + locks/image digests, namespace/ACL, protocol/evaluation key mapping | Pinned-pair SDK/artifact tests, không placeholder hoặc auto-create namespace khi lỗi |
| MLFLOW-P1 — Durable coordination | Người2+4 | Per-run operation intent/checkpoint, owner-generation/heartbeat, frozen artifact refs, unknown triage | Crash sau create/lease expiry/concurrency không blind-create hoặc dispatch thiếu evidence |
| MLFLOW-P2 — SDK/training | Người2 | Real selected run, params/metrics/artifacts, Registry receipt/verification, finalized serving metadata | Read-back bytes/VersionIds/hashes; gate/threshold từ frozen split, no fake metrics |
| MLFLOW-P3 — Release/provenance | Người1+2+4 | Strict result/release/HTTP/Kafka field mapping, fixtures, immutable result reuse | No unversioned extra keys; dispatch ACK khác deploy; no auto-promote |

Các work items gắn vào PR05–10 của kế hoạch hiện hành theo dependencies; không
bắt buộc tạo thêm runtimes. Source/provider/ledger/GitOps implementation, SDK
network tests và deployment phải có yêu cầu thực thi riêng; docs không chạy chúng.


## Sensor0.3.0 — cập nhật tiến độ20/10

UI-P2 đã có source/provider/auth/persistence/scheduler/spool và local tests.
Các phần Sensor của OBS handoff đã có instrumentation. Production IdP/PVC/Kafka,
image/GitOps rollout và affected-component release validation còn mở; không đánh
hoàn tất platform/E2E chỉ vì provider tests qua. Xem [readiness §15](implementation-readiness.md#15-sensor030--providerdurable-runtime2010)
và [báo cáo file thay đổi](SENSOR_IMPLEMENTATION_REPORT.md).

## K. PROC-ORDER-01 — workstream keyed Processing và ordered relay

Quyết định23/10/2026, revision1.0.0 tại
[đặc tả chi tiết](PROCESSING_CONCURRENCY_AND_ORDERING.md). Workstream tương lai
bổ sung PR02/03/05/10, không là bằng chứng các PR đó đã được triển khai.

| Thứ tự | Owner | Deliverable | Gate trước bước kế |
| --- | --- | --- | --- |
| ORDER-P0 — Freeze | Người1 + Người4, Người2 review | Source-order/duplicate/conflict rules, receipt ledger, bounded queues, dispatch/cursor/fence design | Fixtures500–504, source sequence xuyên epoch, timeout/revoke matrix được review |
| ORDER-P1 — Persistence | Người1 | Migration mới: per-machine cursor, durable partition ownership fence, per-topic dispatch_seq/key lease | Upgrade/backfill có evidence, unique constraints; frozen001 không bị sửa; bundle pin mới nếu SQL bundle đổi |
| ORDER-P2 — Keyed runner | Người1 | Opt-in poll owner/FIFO/fair scheduler/executor/completion coordinator | Cùng máy tuần tự, khác máy song song; safe prefix, memory bounds, unknown DB reconcile |
| ORDER-P3 — Ordered relay | Người1 | Head-of-key claim/retry/renew/fenced finish, immutable payload reuse | Hai relay/head backoff/ACK unknown/late duplicate; successor không vượt head |
| ORDER-P4 — Business/OBS | Người1 + Người4 | Independent CAS, alert inbox/lifecycle, metrics OBS-01 §11 và GitOps dashboard handoff | INVALID giữ valid snapshot; historical signal vẫn qua lifecycle; post-commit SSE/resync |
| ORDER-P5 — Cutover/tải | Người1+2+4 | Compatibility gate, drain old writers/relays, bounded canary, load/recovery report | Rebalance/kill/rollback, ML/DB/pool/retention budgets, p95/lag/catch-up/skew được đo |

Storage/Business runner không tự migrate sang keyed mode. Giữ12 partitions và
3 consumer replicas là **profile đo thử**, không default production đủ tải.
Không bật old/new writers không tương thích fence/cursor trên cùng stream.
Runtime tests phải chứng minh generation fence chặn SQL cũ, không chỉ offset.
Late duplicate vật lý không được làm UI lùi hoặc tạo Alert trùng; giữa topics
không có total order. Source sequence/reset sai phải reconcile, không bịa epoch.

Lượt docs chỉ cập nhật spec, sơ đồ, links và hashes. SDK/owner callbacks/migrations/
Kafka bundle/GitOps/runtime tests chưa được sửa hoặc chạy bởi workstream docs.


### K.1. Source progress23/10

ORDER-P1/P2/P3 đã có SDK0.2.0 + migration002/source tests; P0 decision và local
fixtures đã có. Processing transport wrappers/factory có code, **chưa phải full
app**. P4 có optional metrics collectors nhưng Business app/probes/Grafana còn
pending; P5 config3 profile mới NOT_ACTIVATED, chưa rollout/load/legacy-data
bootstrap. Source report/readiness §19 ghi exact evidence. Không mark toàn bộ
workstream DONE hoặc bật workload từ unit/integration PASS.

# L. Baseline100 và capacity/latency choX>100

Giả định ban đầu100 máy×1 reading/s. Kiến trúc scale-ready: sau vertical slice/
failure correctness, xác nhận smoke5→50→baseline100/s; sau đó chọnX>100 theo
capacity measured, không đặt mốc1.000 máy làm yêu cầu ban đầu.

Mỗi mứcX cần offered/accepted/processed unique rate, stage latency/oldest age,
ML/DB/Relay/Storage/Business/SSE resource budgets, alert browser p95≤2s và recovery
headroom. Tính n/c/pool rồi tối ưu bottleneck trước khi tăng tài nguyên; giữ inbox,
FIFO/fences/offset prefix và ordered outbox. Đầu ra là capacity report của đúng
source/model/image/config, không chỉ một sơ đồ/replica count. [Plan chi tiết](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
