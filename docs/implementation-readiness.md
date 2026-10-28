# Implementation readiness — target v1.4 + UI-01, machine baseline1.3.2

Tài liệu v1.4 nhập ngày2026-10-19. Inventory app kế thừa snapshot2026-10-18;
lượt nhập docs không đổi source/runtime. Sau đó đã thêm Kafka SDK Python,
contracts và integration harness; kết quả riêng ở §11 và cập nhật §18–19.
SDK ordering đã có real Kafka/PG tests trong harness riêng; S3/full app E2E,
benchmark, Docker image/deployment và môi trường k3d mới chưa được xác nhận.

## 0. Phân biệt bốn lớp version

- Sáu Markdown mới: target1.4.0, đã nhập và hiệu chỉnh đường dẫn/phân công dự án.
- `contracts/v1.3/`: sáu OpenAPI/schemas/examples baseline1.3.2; Ingestion M1 có
  HTTP revision1.4.0. Chưa migrate đầy đủ mọi API sang target v1.4.
- Runtime map:14 app +1 platform descriptor MLflow, baseline1.3.2; Simulator UI client/descriptor,
  không bump wire version hoặc khẳng định đã deploy.
- Sensor0.3.0: wire1.3.0/Control1.3.2; Drift executable1.1; Simulator UI client0.1.0;
  Ingestion có M1 local; Storage runtime0.1.0 có source/tests/Dockerfile; các application runtimes khác vẫn skeleton.
- Kafka library0.3.0 + canonical bundle2.0.0: outcome reader/writer chỉ1.4.0,
  keyed/ordered/fenced adapters và additive Processing migration002; xem §18–19.

`IMPLEMENTATION_PLAN.md` mô tả các bước implementation tương lai. Những yêu cầu
tạo branch, CURRENT_STATE, migration, test/build/deploy trong đó không phải tác vụ
được thực thi bởi lượt nhập tài liệu này.

## 1. Thiết kế baseline1.3.2 — đối chiếu v1.4 trước khi implementation

| Chủ đề | Quyết định / nguồn |
| --- | --- |
| Terminal result union | OUTCOME-14-ONLY: Kafka/HTTP/history dùng outcome1.4.0; bỏ reader2.0, cutover dữ liệu riêng |
| Simulator UI | Frontend riêng -> Sensor; Main UI -> Business; không Business control facade |
| History | Processing Read API thống nhất hot DB + S3; HISTORY-01 |
| Prediction archive | Processing-owned DB batch export; verified membership trước cleanup |
| Chart | Numeric allowlist, min/max/avg/count, bounded points, lineage markers |
| Brief executor | Durable business jobs + leased runner trong Consumer |
| Drift UI bridge | Versioned S3 report manifests -> Business metadata catalog |
| Replay/rebuild | Shadow projection generations, live catch-up, không replay alert side effects |
| Auth | External OIDC + role/machine checks; explicit local-only anonymous |
| Scaling | Metrics/capacity budgets; consumer partitions và shared DB là giới hạn |

Các mục trên là **specification complete**, không có nghĩa implementation complete.
OpenAPI/schema examples chỉ chứng minh hợp đồng nhất quán về cấu trúc khi được
validate; các invariant transaction/cursor/authorization cần runtime tests riêng.

## 2. Trạng thái implementation thật

| Component | Đang có trong app | Cần implement |
| --- | --- | --- |
| Sensor | 0.3.0: ACK202, SQLite state/sequence/spool,5 Control APIs, JWT/scopes, metrics | Persistent volume/recovery vận hành, production IdP/Kafka và image/rollout evidence |
| Drift | Legacy CLI/detector/persistence/metrics | API/S3 input, owner migration, committed report publication |
| Ingestion | M1 HTTP/auth/guards/lifecycle/probes/metrics; Sensor đã có client202; SDK pin0.3.0 | Shared OIDC/TLS/ACL, tích hợp liên service và GitOps deployment acceptance |
| Processing | SDK durable runner/inbox/outbox/relay/SQL; app callbacks skeleton | DQ/ML/risk/history, owner migrations/read API và archive/export/snapshot jobs |
| Business | SDK3 role loops/inbox transaction adapter; app callbacks skeleton | Projections/alert SQL, REST/SSE/RBAC/lifecycle, jobs/catalog |
| Storage | Runtime0.1.0: SDK consumers, immutable microbatch/manifest, retry/supervision, probes/OBS-01, CLI/config, locked package/image/CI | AWS bucket/region/IRSA/ACL thật; Kafka+S3/container integration, rollout và capacity/cost validation |
| ML | Skeleton hai images | Model loader/serving, trainer/evaluator, versioned bundles |
| RAG | Skeleton | Retrieval, evidence checks, provider integration, citations |
| Main UI | Skeleton | Snapshot/SSE reducer, histories/charts, alerts/Copilot/drift UX |
| Simulator UI | React demo/live client, actor, provider HTTP browser tests, descriptor riêng | Production IdP/browser exchange và image/rollout evidence |
| GitOps/Infrastructure | Hai repo code được soạn riêng | Đồng bộ amendment, cấu hình thật, validate và triển khai khi được yêu cầu |

## 3. Thứ tự implementation và deliverables

1. **Contract freeze:** schemas/examples/providers inventory, permissions, migrations
   và feature flags; không trộn sensor v1.1 response với edge202 mới.
2. **Vertical slice:** one VALID + INVALID reading -> Kafka -> persisted outcome
   -> latest projection -> REST/SSE; event-time/source ordering đúng.
3. **Business correctness:** alert inbox/lifecycle/audit/feedback, OIDC và scoped
   authorization; RAG unavailable không chặn alert.
4. **History/archive:** point/range APIs, DB snapshots, Parquet+manifest batch,
   coverage verification, hot cleanup pins, query/export UI.
5. **Jobs/Drift/control:** durable briefs/export, drift catalog bridge, Sensor
   controller fencing/state persistence và Simulator UI riêng.
6. **Recovery/rollout:** consumer crash/rebalance, projection rebuild, schema
   migration, graceful termination, retention sizing và operational runbooks.
7. **Capacity evidence:** kiến trúc scale-ready, baseline100 máy×1 reading/s;
   dùng workload/số đo dưới đây xác nhận năng lực triển khai và tối ưu khiX>100.

## 4. Capacity/SLO worksheet

Mỗi run ghi source commit, image digest, model/dataset/policy/schema versions,
instance/DB sizes, partition count, duration, máy/rate, users/SSE và history mix.

| Workload | Profile cần ghi | Điều kiện đánh giá |
| --- | --- | --- |
| Ingest + inference | X machines, r records/s/machine, inference concurrency | Arrival rate X*r <= sustained throughput với headroom đã đo |
| UI | U users, connections, subscriptions và update cadence | REST/SSE latency, không sai machine scope, pool không bão hòa |
| History | Hot/cold range, chart point budget, concurrent queries/exports | Complete coverage, cursor stable, dashboard không bị chiếm pool |
| Backlog recovery | Outage duration và catch-up throughput | Catch-up > arrival, retention đủ outage+catch-up+margin |
| Rolling/failure | Pod/ML/Kafka/S3/DB interruptions | Không ACK giả, không duplicate business effects, recovery có giới hạn |

Nếu backlog N records, arrival λ và recovery throughput μ > λ, catch-up time
ước tính N/(μ-λ); nếu μ<=λ thì backlog không tự hết. 12 partitions không cho
biết số máy tối đa; hot-key skew, ML latency và DB I/O có thể giới hạn trước.

Mục tiêu thiết kế visible alert p95<=2s cho healthy online profile; HTTP202 p95,
prediction p95 và UI visibility đo riêng. ML timeout/retry có thể vượt2s và phải
được báo bằng failure/degraded status, không dùng HTTP202 để tuyên bố đạt SLO.
Interactive hot query target p95<=500ms; cold query budget10s, export async30m;
đây là targets để kiểm chứng, không phải measured promises.

## 5. Availability khác scalability

Một RDS instance chứa năm logical DB vẫn chung fault/capacity domain. Profile
infra hiện có single-AZ RDS/một NAT phục vụ chi phí demo, chưa phải HA production.
Ba Kafka brokers cần đúng topology/ISR/disk/retention; replica count không thay
thế việc kiểm chứng mất node/AZ. Sensor một controller có chủ đích và cần lease
để failover strict. Chọn RPO/RTO và backup/restore drill theo môi trường trước
khi tuyên bố production readiness; không tự suy ra RPO0 từ outbox.

## 6. Exit criteria và bằng chứng

- Contract lint/ref/examples pass cho đúng revision; không coi validation là app test.
- Migrations và data ownership review, không cross-owner SQL.
- Future integration/E2E/failure tests cover HISTORY-01 và RECOVERY-01.
- Capacity report có số đo, bottleneck, giới hạn và resource profile tái lập được.
- Rollout/rollback/restore có kết quả thực thi, không chỉ có checklist.
- Workload chưa implement tiếp tục disabled; không gắn nhãn production-ready.

Không tự chạy các checklist test/deploy này chỉ vì đọc tài liệu; thực hiện khi
phạm vi implementation/validation được yêu cầu và báo đúng loại bằng chứng.


## 7. Validation sau dọn tài liệu/contracts — 2026-10-18

- `.venv/bin/python contracts/v1.3/validate.py`: PASS sáu OpenAPI, 52 operations,
  651 local references, 96 HTTP example/default checks, 53 positive/negative cases
  và SSE envelope checks. Bundle hiện hành tự chứa, CI đã chuyển sang validator này.
- Sensor: 88 tests PASS; Drift: 43 unit tests PASS. Sensor dùng frozen wire fixture
  riêng, Drift dùng report/reference schemas1.0.0 trong bundle hiện hành. Các tests
  này kiểm tra implementation đang có, không phải E2E của kiến trúc Kafka mục tiêu.
- Executable source Sensor/Drift được giữ; chỉ thay đường dẫn schema trong tests.
  Không thêm runtime source, DB migration hoặc thay implementation status.
- Không chạy container/DB integration, benchmark, Docker build, Terraform/Kubernetes
  validation hoặc deployment. Chưa có evidence để chuyển implementation target
  ở §2 sang trạng thái hoàn tất.

## 8. Rà soát kế thừa và metadata — 2026-10-18

Ba tài liệu chính đã được sửa trực tiếp ở các chương đang dùng, không chỉ thêm
amendment. Hai placeholder simulator facade/control và bộ tài liệu/contracts
kiến trúc v1.1 đã xóa. Các schema/provider examples còn hiệu lực được chuyển sang
bundle hiện hành; metadata runtime target1.3.2 giữ nguyên trạng thái implementation.
Báo cáo dependency/giữ-xóa:
[legacy-artifact-audit.md](legacy-artifact-audit.md).

## 9. Khoảng cách cần giải quyết trước khi code v1.4 — 2026-10-19

| Chủ đề | Đã nhận / đang có | Việc cần freeze hoặc triển khai |
| --- | --- | --- |
| Quarantine | `contracts/kafka/` có rejected1.4 HASH_ONLY; SDK auth identity/size/envelope boundary, ACK trước400/422; Storage0.1.0 có rejected runtime/probes; GitOps topic/ACL | Ingestion HTTP auth/runtime, real broker/S3 integration; không lưu unauthorized/oversized body |
| Outcome wire | Canonical reader/writer1.4.0/kind; bỏ reader2.0.0 theo OUTCOME-14-ONLY; source/DQ/model/risk lineage schema+fixtures | Owner callbacks/provider DTOs, consumer-first rollout; chưa có cutover/deployment; baseline vẫn nguyên vẹn |
| History cursor | System §36 ghi received_at/event_id; Plan §C/§E và Dataflow dùng accepted-time ordering; baseline có opaque snapshot cursor | Chốt canonical timestamp/cursor, stable pagination, API filters/indexes và khả năng tương thích |
| Read API read-only | v1.4 yêu cầu role `processing_ro`; thiết kế snapshot/export cũ có durable write state | Giữ Read API SELECT-only; bố trí owner writer/maintenance cho snapshot/export trước khi implement |
| Hot/cold/archive | Chi tiết HISTORY-01 giữ original outcomes và coverage; v1.4 cho phép optional outcome archive qua Storage | Chọn writer/membership/manifest contract tương thích; không làm mất bằng chứng gốc hoặc cleanup chưa verify |
| Retention | HISTORY-01 có profile30d/365d; provenance v1.4 chưa chốt thời hạn cụ thể | Các con số cũ là tham khảo, owner phê duyệt theo dữ liệu/ngân sách trước khi bật cleanup |
| Release/provenance | Mới có Markdown yêu cầu dataset/split/model/image/policy/document digests | Bổ sung schemas/examples/manifests, MLflow metadata, provider DTOs, migration và CI gates bằng bằng chứng thật |
| Simulator UI | React demo/live client0.1.0, OIDC/recovery/build/descriptor riêng | Provider/browser HTTP đã test; production IdP, container build và rollout chưa verify |
| Deployment inventory | App có14 descriptors baseline1.3.2, gồm Simulator UI; bảng v1.4 còn liệt kê MLflow platform | Phân biệt app/platform; không coi source/descriptor là deployment |

Các đường dẫn `contracts/events/`, `contracts/provenance/`, root `examples/`,
`implementation/python/`, `implementation/scripts/`, `implementation/tests/`
và `implementation/k8s/` trong sáu tài liệu là artifacts của bộ bàn giao tham chiếu,
**chưa được cung cấp trong lượt nhập sáu Markdown này và chưa tồn tại ở app repo**.
Không tạo empty schemas, fake digests hoặc scripts giả để làm các đường dẫn đó có vẻ hoạt động.
Lệnh kiểm tra hiện có vẫn là `python contracts/v1.3/validate.py`; PASS của nó chỉ
chứng minh baseline1.3.2. Kafka có validator riêng tại §11; provenance/provider
artifacts khác vẫn chưa có, không được suy ra đã validate toàn v1.4.

App là một monorepo: các source owners Processing/Business/ML/RAG/Sensor/Drift
không mặc nhiên là các Git repositories riêng. Provenance phải dùng Git SHA thật
của app/training source tương ứng, GitOps và infrastructure, không tạo SHA giả cho
mỗi thư mục. `NOT_AVAILABLE`/provenance_incomplete chỉ dùng theo schema đã review.

## 10. Validation lượt nhập tài liệu — 2026-10-19

- Local Markdown links và code fences: đã kiểm tra, không có link file hỏng hoặc fence chưa đóng.
- Baseline validator PASS: sáu OpenAPI, 52 operations, 651 local refs,
  96 HTTP example/default checks và53 positive/negative cases. Đây là baseline1.3.2.
- App source/tests, payload schemas/examples, Dockerfiles, CI workflow và runtime
  version/status giữ nguyên so với snapshot trước import. Chỉ sửa externalDocs
  của OpenAPI và một design-document link trong runtime map để tránh trỏ file đã xóa.
- Source manifest giữ upstream/current SHA256 và lịch sử retire. Không chạy app
  tests, container/DB integration, các scripts/scaffold tham chiếu chưa có, build hoặc deploy.

## 11. Kafka SDK dùng chung — 2026-10-19

- `messaging/` có real Kafka client plumbing, gateway, durable receipt runner,
  PostgreSQL inbox/outbox/leased relay và immutable raw S3 adapter. Callback nghiệp
  vụ, auth middleware và runtime health/entrypoints vẫn thuộc owner, chưa implement.
- Canonical bundle1.0.0:4 topics/7 role profiles,15 positive/negative wire cases,
  SQL và28 hash-locked inputs. Legacy schema giữ nguyên; dependency được đóng gói
  trong wheel/sdist, không cần checkout app khi dùng wheel.
- Python3.12:39 unit tests PASS;6 real Kafka/PostgreSQL integration cases SKIP vì
  chưa bật môi trường Docker. Raw S3 dùng fake object store, không gọi AWS/MinIO.
- Unit checks gồm broker ACK uncertainty, commit/handler failure, polling/rebalance,
  receipt binding, schema/key/hash conflict và archive partial-write/replay. Chưa
  có benchmark, real TLS/ACL/broker outage hoặc S3 permission/failure evidence.
- Build sdist -> wheel và validate installed wheel ngoài repo PASS. CI bổ sung
  Kafka SDK + isolated Kafka/PostgreSQL integration vào `CI / required`; workflow
  chưa được chạy trên GitHub trong lượt này.
- GitOps code thêm rejected topic/ACL/exporter, protocol/manifest pin và source
  SHA gates. Workloads vẫn disabled; không run platform render/test/build/deploy.

Hướng dẫn chính: [KAFKA_DEVELOPMENT_GUIDE.md](KAFKA_DEVELOPMENT_GUIDE.md).

## 12. UI-01 — Readiness bốn UI, 2026-10-20

[UI Architecture](UI_ARCHITECTURE.md) chốt Business/Simulator/Grafana/MLflow và
release độc lập. Đây là cập nhật thiết kế; UI prototype1.6 không tăng API/model/
Kafka versions. Sau lượt docs đã triển khai Simulator frontend riêng; phần
evidence frontend ở §13; provider upgrade ở §15. Không suy ra đã deploy.

| Phần việc | Đang có | Còn phải implement/verify |
| --- | --- | --- |
| Business frontend | frontend/ skeleton + API/SSE specification | React, auth, reducer/resync/history/lifecycle/Copilot, image/config release riêng |
| Simulator frontend | sensor-simulator-UI/ React/demo/live client, config/descriptor/Dockerfile, local build/tests và CI job | Production IdP integration, actual container build/digest và rollout |
| Sensor Control provider | 5 frozen APIs đã implement: JWT/auth scope, SQLite receipts/version, scheduler/spool | Production IdP/Kafka, volume failover và deployment validation |
| Demo-only capabilities | HTML có rate và các counters/preview mock | DTO/endpoint/capability/version review trước enable; chưa có live evidence |
| Grafana | Thiết kế metrics/Pushgateway; ZIP mô phỏng5 dashboards | Provisioning thật, datasource permissions, report/histogram contract và query budgets |
| MLflow UI | Platform design/blueprint; HTML mô phỏng | Server/config/access/lineage links thật; model release gates vẫn bắt buộc |
| Independent release | Decision và ma trận checks/deployment đã ghi trong docs | CI affected-component selection, per-image/config GitOps, before/after template/rollback evidence |
| Portal | HTML launcher | Optional, không là runtime/API/DB mới hoặc điều kiện E2E bắt buộc |

Giữ Business monitoring/drift contracts và data ownership hiện có. Grafana report
datasource SELECT drift_db chỉ là đề xuất prototype, chưa có approved views/grants
trong app; cần Drift owner freeze SQL view/role hoặc adapter trước provisioning.
Không tự nới schema ControlState/public runtime config chỉ để UI giống mock.

Các acceptance items UI-01 §8 chỉ hoàn tất khi có runtime evidence tương ứng.
Browser tests của frontend không thay kiểm thử E2E Sensor/IdP/Kafka, container
build hoặc rollout/rollback. Lượt docs trước chỉ kiểm tra tài liệu; lượt frontend
tiếp theo có source và checks riêng ở §13.

## 13. Simulator UI0.1.0 — implementation20/10

- Source: [sensor-simulator-UI/README.md](../sensor-simulator-UI/README.md); React/TypeScript/Vite
  app độc lập Business/Sensor, lockfile, public runtime config và Dockerfile riêng.
- Demo5 máy có control/scenario, preview sáu features/RPM/reading feed; demo state
  trong tab, không gửi reading/control tới external API.
- Live adapter chỉ gọi5 frozen operations, schema guards, OIDC Code+PKCE (tokens
  ở memory), polling/backoff/freshness, version merge và session command recovery.
- Command200 mới ACK;409 reconcile; network/timeout/5xx giữ unknown; GET không
  chứng minh command đã hoàn tất; explicit retry giữ key/body.401/403 không auto-retry.
- Local production build/typecheck/format,18 unit/contract/recovery tests và8
  Chromium scenarios đã chạy. Browser API/IdP là fixtures/login gate, không phải
  kết nối provider thật. OIDC có browser fixture kiểm tra code + S256 PKCE redirect,
  chưa test token exchange với IdP thật. Local dùng Node26.7.0; CI khai báo Node24.
  CI job mới nối required gate nhưng chưa chạy trên GitHub
  ở lượt này. Desktop1440/mobile390 kiểm tra không page overflow hoặc page errors.
- Sensor0.3.0 tiếp theo đã thêm provider, TLS JWKS/OAuth fixtures và browser HTTP checks.
  Production IdP/browser exchange, Docker image build/output digest và rollout chưa verify.
  Rate/counters/preview/SSE/batch capabilities chưa có live contract; UI không bịa.


## 14. OBS-01 / MLFLOW-01 — chuẩn hóa thiết kế20/10

Đã có [OBSERVABILITY_IMPLEMENTATION.md](OBSERVABILITY_IMPLEMENTATION.md) và
[MLFLOW_INTEGRATION_GUIDE.md](MLFLOW_INTEGRATION_GUIDE.md), System Contracts §41,
Guide §49. Hai quyết định revision1.0.0 chốt names/types/labels/units/buckets,
one-hot drift, commit/retry semantics, SDK reference, registry identity, durable
operation intents và strict schema mapping. Các short aliases đã gỡ khỏi phần
hướng dẫn active; mapping cũ→mới giữ trong OBS-01 để migration có chủ đích.

| Phần việc | Evidence hiện có | Còn phải implement/verify |
| --- | --- | --- |
| App metrics | Normative catalog + Python reference; phần lớn runtimes vẫn skeleton | Owner collectors/endpoints/callbacks, locks, type/label/bucket fixtures, error/retry/restart and replica tests |
| Drift metrics | Source render one-hot, PUT snapshot và checkpoint/heartbeat baseline | New data adapters, strict single writer/lease; durable cumulative report counts khi thêm retention; real scrape/panel evidence |
| Alert visibility timing | Canonical accepted/outcome chứa server accepted timestamp | Alert Signal1.0 chưa có accepted time; exact-event owner lookup hoặc versioned migration; không fabricate p95 từ received_at |
| GitOps monitoring | ServiceMonitor/exporter/rules/dashboard2 panels, observability root disabled | Target labels/honor_labels verification, metric coverage + absent/stale guards, full5 dashboards, datasource/report access |
| MLflow compatibility | App ml-service/mlflow có image packaging/locks/publisher/local Compose; server/client pair/base digest chưa pin | Chốt pair/base digest, trainer locks; real container/DB/S3/auth/artifact compatibility và IAM app publisher |
| MLflow operation recovery | training_id/owner/lease/model-result checkpoint trong helper hiện có | Step-level run/registry intents/IDs, generation fencing/heartbeat, immutable evidence refs, unknown-side-effect triage và concurrency/crash tests |
| Registry provenance/wire | Metadata/release requirements; outcome1.4 có mlflow_run_id | Numeric registry fields chưa explicit trong canonical outcome; strict model-result/release/HTTP mapping và fixtures trước schema changes |
| App SDK implementation | Reference snippets create/log/register/search trong Markdown | Trainer implementation, read-back artifact/hash verification, real server/DB/S3 tests; không claim snippets đã được chạy |

Lượt này chỉ sửa docs và source-manifest: kiểm tra local links/heading anchors,
fences, Python AST, catalog names/types và hashes; giữ upstream/retired metadata.
Không chạy training/SDK network/Prometheus/Grafana/DB/Docker/deploy, không thay
app source, HTTP/Kafka schemas, runtime descriptors hoặc hai repo platform.
Schema target còn thiếu tiếp tục được ghi rõ, không tạo fake digests hoặc empty
schemas để đánh dấu hoàn tất.


## 15. Sensor0.3.0 — provider/durable runtime20/10

Đã implement trong app: ACK202 client (không đợi inference), SensorRequest1.3.0,
SQLite/WAL transaction sequence+payload, cursor/scenario step, persistent control
state/version/receipts/audit, process lock, bounded scheduler/sender, spool quota/
TTL/rejected/auth states và maintenance. Auth kiểm JWT signature/issuer/audience/
expiry/iat/role/scope/machine permissions, JWKS cache; outbound service token file
hoặc OAuth client-credentials. Bốn scenario UI khớp YAML; không ép risk/Alert.
Probes và OBS-01 metrics nằm trong Sensor runtime; UI đã hiện last_controlled_by.

Bằng chứng local được ghi chi tiết ở [Sensor implementation report](SENSOR_IMPLEMENTATION_REPORT.md).
Có tests restart thật qua CLI/HTTP, lost-ACK retry, durable sequence/receipt,
parallel CAS, quota/TTL/auth, TLS JWKS/OAuth RSA fixture; browser integration gọi
provider Python thật (Ingestion fixture202/local auth). Unit/contract/schema,
Drift regression, build/lock audit và checks UI được phân biệt trong report.

Chưa có Docker daemon nên chưa build/run image; chưa có output image digest,
GitHub CI của changes chưa commit, Kubernetes/PVC/rolling/failover/load evidence,
production IdP provisioning/browser authorization-code exchange hoặc Kafka E2E.
Một persistent local/block volume, một controller/Recreate; không claim HA.
Rate/counters/queue/preview live/SSE/batch controls chưa được thêm vào wire;
đó là capability extension cần review, không phải số mock để hiển thị live.

## 16. Storage Consumer0.1.0 — owner runtime21/10

Đã thêm `maintai-storage serve`/`verify-config`, accepted/rejected consumer groups
và optional outcome wire archive. SDK giữ immutable microbatch1/manifest1.0.0,
bytes/hash/VersionId, offset commit và rebalance fencing. Runtime thêm bounded
retry/backoff/jitter, shutdown/supervision, bucket versioning/Kafka preflight,
internal probes và OBS-01 registry. Không thêm DB, cross-owner SQL, training trigger,
history/query API hoặc đổi frozen Kafka schema. Cấu hình AWS bucket/region/IRSA
và custom endpoint để môi trường cung cấp sau; offline verify không cần AWS.

Bằng chứng local Python3.12: **54 Storage tests pass, 1 real Kafka/MinIO test skip**;
**39 messaging unit tests pass**. Storage tests dùng in-memory broker/S3 fixtures,
actual botocore Stubber và real CLI subprocess trỏ closed loopback endpoints;
kiểm partial upload, exhausted retry, restart replay, commit failure/conflict,
lost assignment, group isolation, probes/metrics, no-secret logs và SIGTERM.
Wheel/sdist SDK + Storage build được; wheel install/contract verify ngoài checkout,
lint/format và Actions syntax được kiểm riêng.

CI Python3.11/3.12 và Docker build/offline smoke đã được cấu hình, không phải
remote execution evidence. Docker daemon chưa chạy, chưa có AWS/MinIO thực,
IAM/SASL/TLS production hoặc versioned bucket integration. Image build/output
digest, registry publish, rollout/termination, retention/catch-up và object cost/
throughput **chưa được xác nhận**. Microbatch1 cần2 PUT/record chưa tính retries;
đổi batch membership/manifest cần contract review và benchmark, không chỉ tăng
batch size bằng env. Xem [Storage runbook](../storage-consumer/README.md).

## 17. MLFLOW-IMAGE-01 — nguồn image và signer cutover21/10

Đã chuyển Dockerfile MLflow từ GitOps sang `ml-service/mlflow/`: runtime
PostgreSQL/S3 adapter lock, base/server-version lock, local Compose/README,
platform descriptor, app publisher và GitHub App dispatch. App giữ14 application
descriptors +1 platform descriptor; MLflow Deployment/DB/UI không tăng thêm.
MLflow server base/version/compatibility còn null/false, không tự lựa patch.

GitOps bỏ Dockerfile/base-pin và publication group cũ; mapping/verification chuyển
sang exact app maintai-publish.yml/main signer, GitHub OIDC issuer, app actor và
source SHA/group/repository annotations. Existing handoff schema1.0.0 được copy
byte-identical và hash-lock; không đổi transport/HTTP/Kafka contracts hoặc bật
production. Desired state giữ enabled=false/image=null.

**Bằng chứng app local:** 21 offline release tests pass với Docker/AWS/Cosign/GitHub
mock; kiểm source/pins/schema, build-smoke-before-push, reused/unsigned images,
source mismatch và token scope. Lint/format, workflow syntax, docs/metadata/hashes
và source-data consistency được kiểm riêng. GitOps bổ sung4 ownership/signer tests
cho CI tương lai nhưng **chưa chạy platform tests/build/render** trong lượt
code-first này. Không có container/SDK-server/PostgreSQL/S3/IdP/ECR integration.

**Còn trước activation:** chọn base/server/client pair và real digests; kiểm runtime
compatibility/auth; cấu hình app publisher ECR/IAM/GitHub App và receiver actor.
Infrastructure IAM hiện còn grant mlflow ECR cho GitOps publisher, cần chuyển
sang app role ở repo hạ tầng trong lượt riêng. Local Compose dùng PostgreSQL17 +
filesystem artifacts, không chứng minh S3/MinIO production. Chưa publish, dispatch,
apply/deploy, commit hoặc push bởi lượt chuyển source này. Xem
[MLflow runbook](../ml-service/mlflow/README.md).

## 18. PROC-ORDER-01 — thiết kế và source23/10

Decision1.0.0 đã có source SDK0.2.0/bundle1.1.0; inventory/OpenAPI baseline1.3.2
và target docs1.4.0 có version riêng. Bản đầu mục18 chỉ là design snapshot; bảng
này đã cập nhật sau yêu cầu triển khai source, migrations và tests/CI.

| Hạng mục | Source hiện tại | Còn trước production |
| --- | --- | --- |
| Keyed Processing | SDK keyed FIFO/fair executor, Processing wrappers/factory | Real DQ/ML/risk/history callbacks, image/health và sizing |
| Offset coordinator | Safe prefix, offset gaps, receipt/byte budgets, bounded drain | Load/real rolling/failover evidence |
| Durable fence | SQL assignment generation + source cursor + transactional guard | Apply/review owner migrations trên DB thật |
| Ordered relay | Per-topic/key dispatch order/head-only claim/retry/ACK cursor | Production DB activation/monitoring/rollout |
| Migration |002 additive,001 giữ nguyên, explicit activation/legacy gates | Legacy inbox bootstrap riêng; bản này từ chối guessed backfill |
| Metrics | Optional collectors + unit/DB tests | Owner health/metrics HTTP server + GitOps dashboards |
| Business/UI | Invariants đã chốt; default runner serial giữ nguyên | Full callbacks/SQL/API/UI còn skeleton |
| GitOps | SDK/bundle pin + config3 schema/profile NOT_ACTIVATED | Config3 receiver/publisher cutover, real callback/image/source SHA review |

Chi tiết từng file/API/commands/defaults ở
[implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md). Worker/
Relay metadata vẫn skeleton/null/placeholder; không thêm runtime/DB/UI.

## 19. PROC-ORDER-01 — validation source và giới hạn23/10

Local tests dùng Python3.12, Kafka4.0.0/JRE21 và PostgreSQL17.6 tạm trên loopback;
DB/schema biệt lập. Code producer/consumer/fenced transaction/ordered relay được
kiểm thật với broker/DB, không gọi AWS hoặc model service production.

- Unit/Processing wrapper + PostgreSQL/Kafka integration:87 PASS (63 unit/wrapper,
  24 DB/broker integration;3 real Kafka roundtrips). Bao gồm source-order/offset holes, DB stale writer,
  migration/legacy gates, rollback, two-relay race, ACK loss và broker roundtrip.
- Storage regression:54 PASS,1 opt-in Kafka+MinIO SKIP; SDK pin đổi nhưng serial
  runner/raw microbatch behavior giữ nguyên.
- SDK/Processing/Storage sdist/wheel build và installed-package checks được chạy
  riêng; final source hashes/evidence đồng bộ trước kết thúc.
- CI Kafka jobs thêm coverage mới/JUnit, giữ required gate. Chưa trigger remote
  Actions, build Docker, deploy EKS/AWS hoặc benchmark baseline100 readings/s/profile mở rộngR=X×r.

Đây không phải full app E2E/rollout readiness: real callbacks/probes/images,
owner DB provisioning/activation, config3 release receiver và capacity/retention
vẫn cần owner integration. Legacy source inbox không có cursor evidence không
được tự reset/backfill để vượt gate.

## 20. Local Kubernetes profile —23/10

Folder [MaintAI-k3d](../../MaintAI-k3d/README.md) đã được authoring với core platform,
image/source locks, owner DB/S3 bootstrap, monitoring, gated app manifests và CLI
riêng context. Offline tool/schema/hash checks có thể chạy; chưa build platform
images, tạo k3d, validate K8s server hoặc chạy full app E2E. Native MLflow còn chờ
app image compatibility lock; IdP/TLS/DNS, actual app providers/domain callbacks
và migrations chưa được tạo thay bằng mocks. Training/Drift job renderer chưa có
trong local0.1. Xem [local integration](LOCAL_DEVELOPMENT_K3D.md) và bảng readiness
ở folder local. Đây là nền tảng để nhóm tích hợp, không là trạng thái pipeline Ready.

## 21. Baseline100 máy; scale-ready về kiến trúc

Người dùng chốt100 máy,1 reading/s/máy làm giả định khởi đầu. MaintAI scale-ready
qua các boundary và ordered/durable transport đã thiết kế;100 không là trần.
Capacity thực và target alert UI p95≤2s vẫn cần run baseline100/s. KhiX>100,
tínhR=X×r rồi tối ưu/scale đúng stage bằng latency/lag/pool/IOPS/CPU/browser evidence;
không suy throughput từ partition count, replica config hoặc SDK tests.

[Công thức và quy trình](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
Đây là amendment giả định, không thay historical87 transport tests, wire versions,
SQL001/002, model thresholds hoặc trạng thái app/image/deployment còn pending.

## 22. Ingestion M1 local — 2026-10-23

Ingestion có FastAPI runtime với SDK gateway/publisher, JWT/JWKS verifier, bounded
raw-body/admission, ACK-first accepted/rejected, response adapter, health/metrics
và graceful shutdown. HTTP Ingestion OpenAPI tăng MINOR1.4.0; các OpenAPI khác
vẫn1.3.2, Kafka bundle/wire không đổi. Xem [runbook](../ingestion-api/README.md).

Unit/HTTP contracts, real Kafka localhost29092 và installed image smoke dùng JWT
chữ ký với JWKS fixture local được kiểm tra riêng. Không suy shared OIDC provider,
TLS/ACL, S3 archive, Business/ML integration hoặc full-system v1.4 từ các tests này.
GitHub CI/deployment chưa được chạy trong lượt local này. Kế hoạch M1 và kết quả
kiểm tra cụ thể nằm trong docs/superpowers/plans/ và báo cáo phiên làm việc.


## 23. Outcome1.4-only — source migration ngày28/10

[OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md) thay mọi chỉ dẫn compatibility2.0
trong snapshot cũ. SDK0.3.0/bundle2.0.0 bỏ reader legacy, Business giữ risk gốc,
HTTP/history ProcessingResult dùng schema Kafka1.4. SQL001/002 không thay đổi;
không có backfill risk, không rewrite Kafka, không reset offset hoặc migration
dữ liệu trong lần sửa này. Negative fixtures giữ legacy chỉ để kiểm chứng từ chối.

Nhánh source này dựa trên M3: đã có Projector và Fleet reader, không còn chỉ là
callback skeleton. Tuy nhiên nguồn Alert runtime vẫn chưa nối, nên Fleet/Detail
có thể trả503 dù risk hợp lệ. Phần đó không được sửa bởi outcome migration.
Kiểm tra unit/contract và build wheel khác với tests DB/Kafka/container; chỉ
được ghi full pipeline PASS khi có bằng chứng thực thi phù hợp.
