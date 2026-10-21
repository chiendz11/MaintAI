# Implementation readiness — target v1.4, machine baseline1.3.2

Tài liệu v1.4 nhập ngày2026-10-19. Inventory app kế thừa snapshot2026-10-18;
lượt nhập docs không đổi source/runtime. Sau đó đã thêm Kafka SDK Python,
contracts và integration harness; kết quả riêng ở §11. Chưa có xác nhận real
broker/DB/S3 integration, benchmark, Docker image build hoặc deployment của SDK.

## 0. Phân biệt bốn lớp version

- Sáu Markdown mới: target1.4.0, đã nhập và hiệu chỉnh đường dẫn/phân công dự án.
- `contracts/v1.3/`: sáu OpenAPI/schemas/examples baseline1.3.2, chưa migrate v1.4.
- Runtime map và13 descriptors: planned baseline1.3.2; không bump metadata chỉ vì nhập docs.
- Sensor/Drift executable source: implementation1.1; app runtimes khác vẫn skeleton.
- Kafka library0.1.0 + canonical bundle1.0.0: wire target1.4, legacy result reader2.0.

`IMPLEMENTATION_PLAN.md` mô tả các bước implementation tương lai. Những yêu cầu
tạo branch, CURRENT_STATE, migration, test/build/deploy trong đó không phải tác vụ
được thực thi bởi lượt nhập tài liệu này.

## 1. Thiết kế baseline1.3.2 — đối chiếu v1.4 trước khi implementation

| Chủ đề | Quyết định / nguồn |
| --- | --- |
| Terminal result union | Baseline2.0.0; Kafka writer1.4.0/reader1.4+legacy2.0, consumer-first cutover |
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
| Sensor | Legacy replay/fault/scenario và per-machine cadence | ACK202, durable sequence/spool/control API, token validation |
| Drift | Legacy CLI/detector/persistence/metrics | API/S3 input, owner migration, committed report publication |
| Ingestion | SDK gateway/envelope/publisher/ACK; app runtime skeleton | HTTP wrapper, auth, dependency lifecycle/probes và application tests |
| Processing | SDK durable runner/inbox/outbox/relay/SQL; app callbacks skeleton | DQ/ML/risk/history, owner migrations/read API và archive/export/snapshot jobs |
| Business | SDK3 role loops/inbox transaction adapter; app callbacks skeleton | Projections/alert SQL, REST/SSE/RBAC/lifecycle, jobs/catalog |
| Storage | SDK raw S3 immutable object/manifest consumer | Owner entrypoint/probes/IRSA, real S3 integration và capacity/cost validation |
| ML | Skeleton hai images | Model loader/serving, trainer/evaluator, versioned bundles |
| RAG | Skeleton | Retrieval, evidence checks, provider integration, citations |
| Main UI | Skeleton | Snapshot/SSE reducer, histories/charts, alerts/Copilot/drift UX |
| Simulator UI | Chưa có source/runtime folder | UI/control client và deployment descriptor riêng |
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
7. **Capacity evidence:** workload profiles và số đo dưới đây trước khi kết luận
   scale-ready trong triển khai thực tế.

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
| Quarantine | `contracts/kafka/` có rejected1.4 HASH_ONLY; SDK auth identity/size/envelope boundary, ACK trước400/422, Storage rejected group; GitOps topic/ACL | HTTP auth middleware, owner entrypoint/probes, real broker/S3 integration; không lưu unauthorized/oversized body |
| Outcome wire | Canonical writer1.4.0/kind; compatibility reader2.0.0/result_kind, no writer1.1.0; source/DQ/model/risk lineage schema+fixtures | Owner callbacks/provider DTOs, consumer-first rollout; chưa có cutover/deployment; baseline vẫn nguyên vẹn |
| History cursor | System §36 ghi received_at/event_id; Plan §C/§E và Dataflow dùng accepted-time ordering; baseline có opaque snapshot cursor | Chốt canonical timestamp/cursor, stable pagination, API filters/indexes và khả năng tương thích |
| Read API read-only | v1.4 yêu cầu role `processing_ro`; thiết kế snapshot/export cũ có durable write state | Giữ Read API SELECT-only; bố trí owner writer/maintenance cho snapshot/export trước khi implement |
| Hot/cold/archive | Chi tiết HISTORY-01 giữ original outcomes và coverage; v1.4 cho phép optional outcome archive qua Storage | Chọn writer/membership/manifest contract tương thích; không làm mất bằng chứng gốc hoặc cleanup chưa verify |
| Retention | HISTORY-01 có profile30d/365d; provenance v1.4 chưa chốt thời hạn cụ thể | Các con số cũ là tham khảo, owner phê duyệt theo dữ liệu/ngân sách trước khi bật cleanup |
| Release/provenance | Mới có Markdown yêu cầu dataset/split/model/image/policy/document digests | Bổ sung schemas/examples/manifests, MLflow metadata, provider DTOs, migration và CI gates bằng bằng chứng thật |
| Simulator UI | Quyết định người dùng: UI riêng gọi Sensor, Người4 sở hữu | Áp dụng docs index thay inherited Business facade; chưa có source/runtime descriptor |
| Deployment inventory | App có13 descriptors baseline1.3.2; bảng v1.4 còn liệt kê MLflow platform | Phân biệt app/Simulator UI/platform; không coi việc nhập sơ đồ là tạo workloads |

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
