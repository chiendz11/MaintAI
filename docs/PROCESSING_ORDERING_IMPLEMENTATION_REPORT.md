> Cập nhật28/10: phiên bản hiện hành là SDK0.3.0/bundle2.0.0 theo
> [OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md). Báo cáo bên dưới giữ nguyên
> bằng chứng ngày23/10 của SDK0.2.0; không phải kết quả kiểm tra của release mới.

# PROC-ORDER-01 — source Kafka, migrations và cách tích hợp

**Ngày2026-10-23.** SDK `maintai-messaging==0.2.0`, Kafka bundle `1.1.0`,
Processing transport `0.1.0`. Đọc cùng [đặc tả](PROCESSING_CONCURRENCY_AND_ORDERING.md),
System Contracts §42, Guide §53 và [readiness](implementation-readiness.md).

Đã có code transport/durability, tests và CI. **Full Processing application chưa
hoàn tất**: DQ/ML/risk/history callbacks thuộc Người1, model thuộc Người2;
Worker/Relay Dockerfiles còn placeholder, health server và deployment chưa có
bằng chứng. Không dùng fixture INVALID trong tests làm thuật toán DQ production.

## 1. Hiểu thay đổi bằng một ví dụ

```text
Partition P0: offset500 A/11;501 B/21;502 A/12;503 B/22

A/11 chậm; B/21 được chạy và lưu DB trước.
A/12 chờ A/11 durable. B/22 chờ B/21 durable.
Offset500 chưa xong ⇒ chưa commit502 dù501 đã durable.
500/501 đều durable ⇒ được commit502; coordinator tiếp tục theo prefix.
```

Kafka poll/assignment/commit ở một owner thread. Các machine tasks dùng bounded
daemon executor; cùng máy một task, khác máy đồng thời. Hoàn thành theo máy khác
offset order không làm mất record khi crash: chưa ACK offset thì replay, inbox
dùng kết quả đã durable. Không hứa một lần gọi ML duy nhất qua mọi crash/rebalance.

## 2. Files source và lý do thay đổi

| File | Ý nghĩa |
| --- | --- |
| [SDK keyed.py](../messaging/src/maintai_messaging/keyed.py) | Keyed FIFO/fair dispatch, daemon executor, per-partition receipt ledger/safe prefix, pause/resume hysteresis, deadlines/drain |
| [SDK ordering.py](../messaging/src/maintai_messaging/ordering.py) | Explicit migration/activation, durable assignment token/fence, two-phase handler/source cursor, bound ordered outbox và head-only relay |
| [SDK metrics.py](../messaging/src/maintai_messaging/metrics.py) | Optional queue/frontier/stale completion và DB-wide blocked-head collectors; metric failure không đổi business result |
| [SDK config.py](../messaging/src/maintai_messaging/config.py) | Validated KeyedSettings, finite deadlines, bounded records/bytes/keys |
| [SDK postgres.py](../messaging/src/maintai_messaging/postgres.py) | Cho ordered relay finish row và key cursor trong cùng transaction; legacy default giữ nguyên |
| [SDK CLI](../messaging/src/maintai_messaging/cli.py), [service](../messaging/src/maintai_messaging/service.py) | Explicit mode flags/env, Processing-only runner selection, owner migration/activation commands |
| [Migration002](../contracts/kafka/migrations/002_processing_ordering.sql) | Capability gate, durable assignments, source cursor, per-topic key cursor và dispatch order; không sửa001 |
| [Archived bundle1.0 manifest](../contracts/kafka/releases/1.0.0.json) | Giữ nguồn gốc/hash của release trước; manifest mới1.1 thêm002 |
| [Processing package](../processing/pyproject.toml) | Transport wrappers0.1.0, pin SDK0.2.0[postgres] |
| [worker.py](../processing/src/maintai_processing/worker.py), [outbox_relay.py](../processing/src/maintai_processing/outbox_relay.py) | Entry modules thật, gọi SDK CLI đúng role; không tự migrate DB |
| [handlers.py](../processing/src/maintai_processing/handlers.py) | Load reviewed prepare/persist callbacks, owner DSN và đúng Kafka group/prefix; thiếu callback thì fail closed |
| [Tests unit](../messaging/tests/unit/test_keyed.py), [DB/broker](../messaging/tests/integration/test_processing_ordering.py), [wrappers](../processing/tests/test_kafka_entrypoints.py) | Ordering/holes/drain, SQL rollback/fencing, relay races/retry/ACK loss, real Kafka roundtrip và no-fake callback boundary |
| [CI](../.github/workflows/ci.yml) | Kafka jobs chạy tests mới, build SDK/Processing packages, wheel smoke và JUnit integration artifact; giữ required gate |

Storage pin lên SDK0.2.0, raw bundle metadata1.1.0; runner/microbatch/wire archive
giữ nguyên. Business vẫn default serial handlers; app SQL/REST/SSE chưa implement.
Không thêm topic, UI, DB owner hoặc Deployment cho scheduler/coordinator.

## 3. Callback API để Người1 tích hợp

Factory mặc định có sẵn: `maintai_processing.handlers:make_handler`. Owner phải
cung cấp hai hàm trong package thật của mình:

```python
def prepare(event, record, context):
    context.check()
    # Chạy DQ; chỉ VALID gọi ML. Bound timeout/retry và kiểm context khi kết thúc.
    # Trả dữ liệu đã chuẩn bị thật, không giữ DB transaction/lock trong lúc HTTP.
    raise NotImplementedError("Owner must implement real DQ/ML preparation")

def persist(connection, event, record, outbox, prepared):
    # Ghi DQ/history/prediction/risk vào connection hiện tại.
    # outbox.enqueue(connection, "inference.results", terminal_payload)
    # Chỉ completed risk phù hợp mới enqueue alert.signals nếu policy yêu cầu.
    # Không tự commit/rollback connection hoặc gọi HTTP tại đây.
    raise NotImplementedError("Owner must implement history/risk persistence")
```

Đây là signature/điểm tích hợp, **không là callback implementation**. Hãy import
real owner functions, không copy comments thành một callback return None.
SDK giữ fence, source cursor và inbox trong cùng final transaction, kiểm đúng
một terminal result và consistency Signal với completed risk. Callback sai/thiếu
result ⇒ rollback cả effect/history/inbox/outbox. Duplicate hash khớp bỏ prepare;
cùng ID khác body hoặc first-seen sequence đi lùi ⇒ fail closed/reconcile.

`sequence_number` tăng bền vững xuyên epoch; gap hợp lệ không bị chặn vì thiếu
n+1. Source FIFO giữ broker arrival; producer/source order và partition cutover
vẫn phải đúng. Context/DB token phải đúng group/topic/partition của reading.

## 4. Cách chạy khi đã có local DB/broker và callbacks

Các lệnh dưới là runbook cho môi trường riêng được cấu hình, không tự được chạy
trên production khi đọc file. DSN/credentials lấy từ environment/secret store.

```bash
pip install -r messaging/requirements.lock
pip install --no-deps -e ./messaging -e ./processing

maintai-kafka migrate-owner-db --owner processing --processing-ordering
# Chỉ sau khi drain old writers/relays và kiểm dữ liệu cũ:
maintai-kafka activate-processing-ordering --legacy-writers-drained

# Cấu hình PROCESSING_HANDLER_FACTORY=maintai_processing.handlers:make_handler,
# PROCESSING_PREPARE_CALLBACK và PROCESSING_PERSIST_CALLBACK bằng real module:function.
python -m maintai_processing.worker --processing-mode keyed
python -m maintai_processing.outbox_relay --mode ordered
```

SDK CLI tương đương: `consume --role processing --processing-mode keyed
--handler-factory ...` và `relay --mode ordered`. Env mode là
`PROCESSING_EXECUTION_MODE=keyed`, `OUTBOX_EXECUTION_MODE=ordered`.
Nếu không chọn mode, SDK giữ serial/legacy defaults. Keyed mode từ chối role
Business/Storage hoặc handler không có durable acquire/release/handle protocol.

## 5. Migration/activation và phục hồi

- 001 giữ nguyên bytes.002 là explicit Processing-owner migration, capability
  mặc định tắt; Business DB không activate mode này. Startup không tạo schema.
- Initial activation chỉ cho baseline sạch đã drain: từ chối legacy pending
  outbox chưa có order, hoặc legacy source inbox cần reviewed cursor bootstrap.
  **Bản này không tự backfill DB cũ**. Không xóa inbox/guess timestamps để vượt gate;
  cần owner migration riêng có source evidence nếu DB đã có historical processing.
- Khi active, trigger chặn legacy inbox/outbox writers chưa khai báo capability.
  Old relay không claim successor bằng row-only SKIP LOCKED được nữa.
- Owner mới nhận Kafka assignment sẽ tăng durable generation. Final transaction
  giữ short row lock/check generation; stale owner rollback. HTTP prepare không
  giữ SQL lock, remote inference cũ vẫn có thể hoàn thành nhưng không ghi effect mới.
- DB commit unknown ⇒ runner không ACK; restart đọc durable inbox/outcome rồi
  dedupe. Revoke/lost fence ngay và replay; không chờ HTTP trong callback rebalance.
- SIGTERM dừng admission, drain active tasks theo budget, commit safe prefix;
  hết deadline ⇒ fence/replay. Daemon threads không giữ process sống vô hạn;
  callbacks vẫn phải dùng bounded I/O và context. Không auto skip/auto DLQ poison.
- Relay claim một head ngay trước từng bounded publish, key lease vượt delivery
  timeout. ACK và key cursor advance cùng transaction. ACK loss retry cùng bytes;
  late duplicate vẫn có thể đến, không total order xuyên topic. Không auto undo
  migration/cursor/inbox khi rollback binary.

## 6. Config thực sự được hỗ trợ

| Environment | Default/ý nghĩa |
| --- | --- |
| PROCESSING_MAX_ACTIVE_KEYS |8 máy active/replica, mỗi máy một task |
| PROCESSING_MAX_PENDING_RECORDS |512 fetched/queued/in-flight/done-awaiting-commit receipts |
| PROCESSING_MAX_PENDING_BYTES |16777216 accounting bytes; payload/key/headers + bounded overhead, không process RSS |
| PROCESSING_MAX_PENDING_PER_KEY |64 receipts/key; full key có thể pause cả partition |
| PROCESSING_MAX_UNCOMMITTED_PER_PARTITION |128 receipts/partition, gồm completed-behind-hole |
| PROCESSING_RESUME_LOW_WATERMARK |0.5 hysteresis |
| PROCESSING_HANDLER_TIMEOUT_SECONDS |15 seconds, finite và <=120 |
| PROCESSING_DRAIN_TIMEOUT_SECONDS |30 seconds, finite và <=120 |
| OUTBOX_CLAIM_LEASE_SECONDS |120, phải lớn hơn Kafka delivery timeout |
| OUTBOX_BATCH_SIZE |10 heads/run_once, claim từng head ngay trước publish |
| OUTBOX_POLL_INTERVAL_MS |250 milliseconds |

Byte budget phải có reserve cho ít nhất hai max receipts; key/headers bị bound
trong reserve. Kafka client/network buffers/process overhead không là gauge này.
Các defaults là điểm bắt đầu cho dev; baseline thiết kế hiện là100 máy×1 reading/s.
Kiến trúc scale-ready choX>100 qua tính/đo lại replica, concurrency và pool budgets;
transport defaults không là capacity benchmark của app.
ML/DB pool budgets phải tính qua replicas; prepare callback tự bound I/O theo
context/dependency policy, không tạo thread/HTTP vô hạn bên trong callback.

## 7. Metrics, CI và bằng chứng

Owner tạo `ProcessingMetrics(registry)` rồi truyền `metrics=` cho runner;
`OutboxOrderMetrics(connection_factory,registry)` cho relay. Registry được owner
phục vụ ở management endpoint; wrappers chưa có HTTP health server tự động.
Collector errors không rollback business success. Outbox gauges là DB-wide,
dùng max giữa replicas; IDs ở logs/traces, không Prometheus labels. Xem OBS-01 §11.

Local validation23/10 dùng Python3.12, Kafka4.0.0/JRE21 và PostgreSQL17.6 tạm,
chỉ bind loopback, DB/schema test riêng. Đây là real broker/DB tests; HTTP ML,
application DQ/risk và S3 vẫn là callbacks/fixtures tương ứng.

```bash
python -m pytest -c messaging/pyproject.toml messaging/tests/unit processing/tests
MAINTAI_POSTGRES_INTEGRATION=1 python -m pytest messaging/tests/integration/test_processing_ordering.py
MAINTAI_KAFKA_INTEGRATION=1 python -m pytest messaging/tests/integration
python -m pytest -c storage-consumer/pyproject.toml storage-consumer/tests
python messaging/scripts/lock_contracts.py --check
```

Integration cần `KAFKA_TEST_DATABASE_URL` loopback; real broker còn cần
`KAFKA_TEST_BOOTSTRAP_SERVERS`. Tests tự tạo/drop schema; không dùng production DSN.
CI Python3.11/3.12 giữ Kafka jobs trong required, Docker Compose Kafka4/PG17 cho
integration. Local chạy trực tiếp temporary binaries vì Docker daemon chưa chạy.
JUnit/metrics assertions kiểm offset holes, same-key order, stale SQL, activation,
legacy gates, callback rollback, two-relay races, ACK loss và wire roundtrip.

Kafka/Processing: **87 PASS** (63 unit/wrapper,24 integration DB/broker). Trong
đó có3 real Kafka roundtrips; SQL race/fencing dùng PostgreSQL17.6 thật, ML/DQ
callbacks là fixture. Storage regression: **54 PASS,1 opt-in Kafka+MinIO SKIP**.
GitOps: **3 config contract tests PASS**, Kafka source pin verification PASS.
SDK/Processing/Storage đã build sdist/wheel; installed-package smoke chạy ngoài
checkout. Remote Actions chưa được chạy bởi lượt này.

## 8. GitOps/hạ tầng và giới hạn còn lại

GitOps thêm config3.0.0 cho Worker/Relay và profile `config/processing-ordering.json`
ở trạng thái NOT_ACTIVATED. Schema2.0.0 và active desired-state config giữ nguyên;
receiver/publisher vẫn dùng2.0, cần reviewed cutover trước khi phát hành config3.
SDK0.2/bundle1.1 pin được đồng bộ, source SHA/image vẫn chưa có release đã duyệt.
Source/README/AGENTS ở ba repo phân biệt current target với platform baseline1.3.1.

Chưa build Docker image, deploy K8s/AWS, chạy full app E2E, benchmark baseline100 readings/s hoặc profile mở rộngR=X×r,
hoặc rollout/failover production. Terraform resources không đổi. Full callbacks,
history queries, probes, image và release evidence vẫn là việc của application/
platform owners theo Plan §K. Không thêm service/DB/UI hoặc tự promote model.
