# Hướng dẫn Kafka cho nhóm MaintAI

Cập nhật: 2026-10-28. Python dùng chung, package `maintai-messaging==0.3.0`,
Kafka contract bundle2.0.0. Đây là code transport và durable delivery để các
thành viên phát triển nghiệp vụ; không phải xác nhận toàn hệ thống đã chạy.

**PROC-ORDER-01:** §12 + [implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md)
mô tả keyed Processing/ordered relay đã có source SDK0.2.0, explicit modes và
migration002. Serial/legacy defaults giữ nguyên; full app callbacks vẫn cần owner.

## 1. Đã có gì, mỗi người còn làm gì?

| Thành viên | Phạm vi đã chốt | Cách dùng Kafka |
| --- | --- | --- |
| 1 — Ứng dụng | Ingestion, toàn bộ Processing, Backend/Business và Main Frontend | Dùng gateway, consumer loop, inbox/outbox/relay; viết DQ/ML client/risk, SQL history/projection/alert và REST/SSE |
| 2 — ML | Training, preprocessing/evaluation, inference và model bundle/reference | Cung cấp HTTP response và metadata thật cho Processing; không cần Kafka client/credential |
| 3 — RAG | Ingestion tài liệu, retrieval, sinh brief/checklist, citations | Phục vụ Business qua HTTP; không subscribe prediction topic để tự tạo alert |
| 4 — Platform | GitOps/hạ tầng, Kafka, CI/CD/MLOps, Sensor/Simulator UI, Drift và Storage | Quản lý broker/topics/ACL; dùng Storage consumer có sẵn; Sensor gọi HTTP, Drift dùng Processing API/S3 |

Phân công Kafka cũ được thay bằng bảng này. Bộ SDK chung xử lý transport; Người1
không phải tự viết lại producer/commit offset và Người4 không phải viết lại
consumer archive. Mỗi owner vẫn chịu trách nhiệm hành vi nghiệp vụ và lỗi của
service mình, Người4 chịu trách nhiệm vận hành cluster.

Code nằm ở [messaging/](../messaging/README.md), contracts chuẩn ở
[contracts/kafka/](../contracts/kafka/README.md). Đây là thư viện dùng chung được
đóng gói vào image của các service, không thêm Kafka microservice hoặc DB chung.

Đã cung cấp: broker ACK có timeout, kiểm tra schema/key, offset thủ công sau
durable receipt, polling trong khi handler chạy, inbox chống xử lý trùng, outbox
transaction + relay lease/fencing/retry, raw S3 immutable object/manifest, CLI,
local integration Compose và các jobs CI. Chưa cung cấp DQ/risk algorithms,
ML/RAG servers, application SQL/projections/API/auth middleware, health endpoints
hoặc một E2E app hoàn chỉnh. Các Dockerfile/runtime descriptor app cũ vẫn cần
owner tích hợp SDK và entrypoint; không bật workload chỉ vì có thư viện này.

## 2. Luồng và điểm xác nhận thành công

```text
Sensor --HTTP--> IngestionGateway --broker ACK--> HTTP202
                    | accepted              | envelope rejected
                    v                       v
             sensor.readings         sensor.rejected
              |    |    |                   |
              |    |    +--Business live    +--Storage rejected
              |    +--Storage accepted            |
              |          +------------------------+--> S3 object + manifest
              v
          Processing -> DQ -> ML HTTP -> Risk
              -> processing_db: inbox + history + outbox, COMMIT
              -> Kafka offset commit
          Relay -> inference.results / alert.signals -> ACK -> mark PUBLISHED
              -> Business: inbox + projection/alert/audit, COMMIT
              -> Kafka offset commit -> Business API/SSE -> Main UI
```

- HTTP202 nghĩa là Kafka đã ACK accepted event, chưa có nghĩa ML đã xử lý xong.
- Feature values không hợp lệ vẫn có thể qua envelope Ingestion; Processing làm
  deep DQ. Chỉ event VALID được gọi ML. UNKNOWN/lỗi ML không trở thành NORMAL.
- Rejection chỉ trả400/422 sau ACK của quarantine. Hash-only không lưu request
  body;401/403/413 không bị ép vào quarantine. Auth phải chạy trước gateway.
- Consumer chỉ commit offset `record.offset + 1` sau receipt của đúng record.
  Transaction lỗi/handler lỗi/mất assignment không được xác nhận thành công.
- Relay ACK rồi crash trước mark có thể publish lại. Inbox/idempotency là bắt
  buộc; không có cam kết exactly-once xuyên Kafka, PostgreSQL, S3 và ML HTTP.
- Ba Business groups chia công việc khác nhau; Kafka không đảm bảo thứ tự giữa
  các topic. PG NOTIFY/SSE fan-out do Business triển khai sau DB commit.

## 3. Topics, groups và versions

| Topic | Writer | Groups |
| --- | --- | --- |
| `sensor.readings`1.3.0 | Ingestion | `processing-v1`, `raw-archive-v1`, `business-live-v1` |
| `sensor.rejected`1.4.0 | Ingestion | `raw-rejected-archive-v1` |
| `inference.results`1.4.0 | Processing outbox relay | `business-prediction-v1`; optional `outcome-archive-v1` |
| `alert.signals`1.0.0 | Processing outbox relay | `business-alert-v1` |

Nguồn chính xác là [topology.json](../contracts/kafka/topology.json). Production:
12 partitions/topic,3 replicas, min ISR2, retention7 ngày, max broker message1MiB;
SDK default payload256KiB. Local chỉ1 broker, replica/minISR1. Không tự giảm số
partition, không bật auto-create topic. Số máy tối đa còn phụ thuộc ML/DB/lag.

Reader và writer outcome chỉ nhận1.4.0 `kind` theo
[OUTCOME-14-ONLY](OUTCOME_V1_4_MIGRATION.md). `event.source_event_id` ánh xạ
`payload.event_id`; payload/wire/hash giữ nguyên. COMPLETED cần risk_score
gốc từ Processing; Business không suy ra từ probability. SDK không còn
`event.legacy` hoặc đọc result_kind2.0.0. Consumer nghiệp vụ gặp event cũ sẽ
dừng trước callback/commit; Storage vẫn lưu nguyên bytes làm bằng chứng.
HTTP/history outcome entrypoint dùng cùng schema Kafka, các DTO và provider
ML/RAG1.1.0, Drift1.0.0 không tự đổi version. Cutover dữ liệu có trong runbook.

## 4. Cài môi trường và chạy broker development

Từ root MaintAI, Python3.11 hoặc3.12:

```bash
python3.12 -m venv .venv-messaging
.venv-messaging/bin/python -m pip install -r messaging/requirements-dev.lock
.venv-messaging/bin/python -m pip install --no-deps -e ./messaging
source .venv-messaging/bin/activate
set -a
source messaging/.env.example
set +a
maintai-kafka validate-contracts
python messaging/scripts/lock_contracts.py --check
```

`.env.example` chỉ chứa giá trị development; production nạp secret từ hệ thống
quản lý secrets, không source file chứa credential production vào terminal/log.
Nếu Docker daemon đã được bật, có thể chạy môi trường biệt lập:

```bash
docker compose -f messaging/tests/integration/compose.yaml up -d --wait
maintai-kafka create-local-topics
```

Kafka chỉ bind127.0.0.1:19092, PostgreSQL127.0.0.1:15432. Hai named volumes giữ
dữ liệu qua restart. Không chạy `down -v` trên dữ liệu cần giữ. Đổi port bằng
`MAINTAI_KAFKA_PORT`/`MAINTAI_KAFKA_DB_PORT` thì sửa matching client/test URL.
`KAFKA_LOCAL_MODE=true` mới cho PLAINTEXT và group prefix. Đặt prefix riêng để
test độc lập; các replica cùng một service phải dùng cùng group, không thêm podID.

Production dùng SASL_SSL/SCRAM-SHA-512 + CA verification; bắt buộc credential,
`PRODUCER_GIT_SHA` thật cho publisher. Library từ chối auto commit/store, acks khác
all hoặc tắt idempotence. Local bootstrap CLI bị chặn khi local mode tắt.

## 5. Người1: nối Ingestion và Processing

### Ingestion

Tạo một publisher/gateway trong lifecycle process, đóng publisher khi shutdown.
HTTP wrapper lấy raw body có giới hạn kích thước, xác thực JWT/credential và
machine scope trước khi tạo `SourceIdentity`; không lấy allowed machines từ body.

```python
from maintai_messaging.config import KafkaSettings
from maintai_messaging.ingestion import IngestionGateway, SourceIdentity
from maintai_messaging.producer import KafkaPublisher

settings = KafkaSettings.from_env()
publisher = KafkaPublisher(settings, "ingestion")
gateway = IngestionGateway(publisher, "https://business.example.com")

# verified_subject và allowed_ids đến từ middleware đã kiểm chứng token/scope.
identity = SourceIdentity(verified_subject, frozenset(allowed_ids))
response = gateway.handle(
    raw_body, identity=identity,
    request_id=request_id, correlation_id=correlation_id,
)
# HTTP framework chuyển response.status/body/headers thành HTTP response.
```

Lời gọi publish là blocking có timeout; trong ASGI chạy trong threadpool hoặc
sync endpoint, không chặn event loop. Request/correlation ID tối đa256 ký tự;
giữ requestID khi retry rejection và eventID/payload khi retry reading.503/504
là retryable, timeout không chứng minh event chưa tới Kafka. Publisher hiện serialize
lời gọi để giữ ACK chính xác, cần đo throughput trước khi tăng tải.

### Inbox, history và outbox trong cùng transaction

Migration có chủ đích, không gọi mỗi lần worker khởi động:

```bash
export PROCESSING_DATABASE_URL='postgresql://OWNER@HOST/processing_db'
maintai-kafka migrate-owner-db --owner processing
export BUSINESS_DATABASE_URL='postgresql://OWNER@HOST/business_db'
maintai-kafka migrate-owner-db --owner business
```

Hai lệnh minh họa URL, cần credential riêng từ secret. CLI chấp nhận fallback
`DATABASE_URL` theo workload GitOps; không dùng cùng DB development cho hai owner
trong triển khai thật. Migration chỉ thêm `maintai_kafka_inbox/outbox`, không thay
migrations history/alerts của owner; production nên đưa SQL này vào migration
tool hiện dùng và review quyền tối thiểu. Read API không có quyền ghi inbox/outbox.

Factory contract: `module:function(settings, role, registry)` trả durable handler.
Mẫu tích hợp sau cần owner cung cấp `persist_result` và `build_outcome`; đó là
nghiệp vụ app, không có mặc định trả success giả:

```python
import os
from maintai_messaging.postgres import TransactionalHandler, connect

def make_handler(settings, role, registry):
    profile = registry.topology["profiles"][role]
    group = settings.group_prefix + profile["group_id"]
    dsn = os.environ["PROCESSING_DATABASE_URL"]

    def apply(connection, event, record, outbox):
        outcome = build_outcome(event, record)  # DQ -> ML nếu VALID -> risk
        persist_result(connection, event, outcome)
        outbox.enqueue(connection, "inference.results", outcome)
        # Nếu risk policy quyết định phát alert signal, enqueue cùng connection.

    return TransactionalHandler(
        lambda: connect(dsn), group, apply, contracts=registry,
    )
```

```bash
maintai-kafka consume --role processing --handler-factory owner_handlers:make_handler
maintai-kafka relay
```

`owner_handlers` là tên module owner tự viết/cài vào image; hai command là hai
entrypoint worker/relay của cùng Processing image. Mọi ghi history/outbox dùng
**cùng connection**, callback không tự commit, không publish Kafka trực tiếp.
Callback hiện chạy trong transaction nên ML/retry phải có timeout thấp hơn DB
budget; có thể prepare DQ/ML trước transaction trong adapter riêng rồi persist
bằng TransactionalHandler. Cần recheck dedupe/CAS khi persist, không ACK trước.
Default statement timeout15s, lock5s, idle transaction60s không thay cho timeout ML.

`OutcomeFactory(source_event, record, dq_ruleset_version=..., dq_ruleset_sha256=...,
producer_git_sha=...)` tạo payload từ record thực:

- `.completed(Prediction(..., ModelIdentity(...)), RiskDecision(...))`;
- `.invalid(validation_errors)`;
- `.unavailable(failure_code, attempts)`;
- `.alert_signal(completed_outcome)` trả signal hoặc None cho NORMAL.

Owner cung cấp prediction ID, model/dataset/split/release/image hashes và policy
thật. Factory không train model hoặc quyết định risk thay app. Không bịa model
lineage cho INVALID/ML_UNAVAILABLE. Gọi alert_signal khi policy/cooldown yêu cầu
phát tín hiệu; Business vẫn sở hữu alert lifecycle/deduplication.

## 6. Người1: nối Business và Main UI

```bash
maintai-kafka consume --role business --handler-factory business_handlers:make_handler
```

Factory được gọi3 lần với role `business-live`, `business-prediction`,
`business-alert`; chọn callback tương ứng và **Business DSN riêng**. Mỗi handler
dùng factory mở connection riêng hoặc pool thread-safe; không share một connection
giữa3 loops. Inbox group phải đúng group của role, gồm prefix local nếu có.

| Handler | Callback cần làm trong transaction |
| --- | --- |
| live | Latest telemetry chưa kiểm chứng, source identity và tuổi dữ liệu |
| prediction | Latest processing + latest valid prediction, model/policy provenance |
| alert | Tạo/cập nhật alert theo policy, dedupe/cooldown, audit và durable job RAG |

Không ghi đè latest valid prediction bằng INVALID/ML_UNAVAILABLE. Dùng CAS/source
ordering bền vững của app, không sort opaque eventID/epoch và không giả định topic
đến cùng thứ tự. **Legacy SDK relay** có nhiều leases nhưng chưa cam kết publish
theo source sequence; Kafka key giữ partition affinity, không thay ordering guard
của app. **Target PROC-ORDER-01** bổ sung head-of-key ordered dispatch theo từng
topic/máy (§12), đã có OrderedOutboxRelay0.2.0 ở mode opt-in. Late duplicate và thứ tự xuyên topic vẫn
cần inbox/CAS; không bỏ Alert Signal chỉ vì UI đã có reading mới hơn.
SDK chỉ giải quyết durable delivery/idempotent identity; unique
alert constraints, sequence ordering và lifecycle thuộc callback của app.

Ghi `NOTIFY` bằng cùng transaction PostgreSQL hoặc outbox fan-out riêng; API chỉ
phát SSE sau commit. Không dùng một Kafka group để broadcast trực tiếp tới mọi
API replica. Main UI dùng REST snapshot/SSE và status endpoint Business; history
gọi Processing Read API, không đọc processing_db hoặc Kafka trực tiếp.

## 7. Người2 và Người3: ML/RAG

ML cung cấp inference HTTP cùng model/preprocessor versions, hashes, approved
dataset/split, MLflow run, releaseID và inference image digest từ bundle/release
thực. Person1 ánh xạ vào `ModelIdentity`, không điền hằng số fixture. Việc thêm
lineage vào ML HTTP baseline cần PR contract riêng; wire Kafka có field không
nghĩa ML server hiện đã trả field đó. Training vẫn là image/job riêng, triggered
bởi approved dataset manifest; không train trên từng Sensor event.

RAG nhận context được Business cho phép và trả brief/citations qua HTTP. Không
cần join Kafka group để biết có alert mới; Business durable job gọi RAG. RAG lỗi
không rollback alert đã tạo và không làm dừng prediction consumer. Các credentials
Kafka của Ingestion/Processing/Business không chia cho ML/RAG/frontend.

## 8. Người4: Storage, Sensor/Drift và platform

### Raw Storage consumer đã có implementation

Owner runtime0.1.0 hiện ở [storage-consumer/](../storage-consumer/README.md), CLI
`maintai-storage serve`: thêm versioning preflight, independent group loops,
bounded retry, graceful shutdown, management probes và OBS-01. Khi chạy runtime
này **không chạy đồng thời** SDK CLI bên dưới với cùng groups. `maintai-storage
verify-config` chạy offline khi chưa có AWS. Bucket/region/endpoint và credential
chain cấu hình sau qua environment; không tạo bucket hoặc topic ở startup.
Đọc README module cho lockfiles, Docker build context và tests; external
Kafka+S3 integration/capacity chưa được xác nhận bởi việc thêm source.

```bash
export S3_RAW_BUCKET='YOUR_VERSIONED_RAW_BUCKET'
export S3_RAW_PREFIX='raw-sensor'
maintai-kafka consume --role storage
# Chỉ khi cần thêm bản lưu wire outcome, bật rõ ràng:
maintai-kafka consume --role storage --archive-outcomes
```

Dùng một trong hai command, không chạy đồng thời thành hai cấu hình cạnh tranh.
Mặc định2 groups accepted/rejected trong một process. Optional group archive
outcome chỉ giữ **wire**; không thay Processing curated/history export, membership
verification hay hot cleanup. Storage không SQL vào Processing/Business DB.

S3 adapter ghi deterministic key theo topic/partition/offset, gzip JSONL chứa
exact value/key/headers dạng base64, rồi manifest có VersionId + SHA-256. Replay
cùng coordinate phải cùng bytes; conflict dừng, không overwrite. Ghi object xong
manifest lỗi thì chưa commit offset; lần sau verify object và hoàn tất manifest.
Bucket production phải versioned, IAM chỉ prefix cần thiết, dùng IRSA/credential
chain. Custom endpoint phải TLS; unversioned/HTTP chỉ explicit local mode.

Mỗi microbatch hiện có1 receipt, tức2 PUT/record. Cách này dễ phục hồi/deduplicate
nhưng object rate/cost cao ở tải lớn; batch nhiều records cần membership/lease
ổn định và benchmark trước khi thay. S3 thật/MinIO không có trong local Compose;
cần endpoint riêng được cấu hình, không tạo bucket AWS tự động.

### Platform

Repo sibling `MaintAI-gitops` giữ Strimzi/Kafka,4 topics, ACL theo service,
exporter groups và contract pin `config/kafka-contracts.json`. Ingestion được write
accepted/rejected; worker chỉ read accepted; relay write result/signal; Business
read3 topics; Storage read accepted/rejected/optional outcomes. Không cấp wildcard
admin hay tự tạo topics bằng startup code. Production giữ Kafka4.0.0/Strimzi0.47.0
của blueprint; thay versions cần review compatibility riêng.

Shared cluster phải bảo vệ group ACL, retention/disk, ISR, consumer lag và pod
shutdown budgets. ProducerGitSHA lấy từ source SHA của image đã build, không dùng
Git SHA của repo GitOps. Runtime config schemas2.0.0 đã freeze không bị sửa ngầm.
Protocols Kafka đã chốt không có nghĩa DB migrations/entrypoints/workloads khác
đã review; tất cả workload enable flags vẫn false trong blueprint.

Sensor vẫn gửi HTTP Ingestion; simulator UI riêng gọi Sensor Control API. Sensor
source hiện còn wire v1.1 nên phải migrate ACK202/envelope/sequence/spool theo
contract trước E2E; SDK không tự làm migration đó. Drift đọc Processing API hoặc
curated S3 ngoài online path, không consume Kafka để tự retrain/promote.

## 9. Lỗi, rolling update và replay

| Tình huống | Hành vi SDK | Owner cần làm |
| --- | --- | --- |
| Publish timeout/ACK không rõ | 503/504 hoặc relay pending; không success giả | Retry cùng logical ID/payload, giữ spool/outbox |
| Worker/Business crash trước DB commit | Rollback, chưa commit Kafka | Giữ callback transactional |
| Crash sau DB commit trước offset | Redelivery, inbox bỏ duplicate | Không xóa inbox sớm hơn replay horizon |
| Relay crash sau ACK trước mark | Có thể publish lại | Inbox + unique business constraints |
| Mất assignment khi handler đang chạy | Không commit offset bằng assignment cũ | Cho phép retry, external side effects cần idempotency |
| Schema/key/identity conflict | Fail closed, không bỏ qua record | Alert/log metadata an toàn, sửa producer/adapter; replay có review |
| S3 object có nhưng manifest chưa xong | Chưa receipt/commit; verify rồi retry | Giữ object versions và quyền đọc/ghi cần thiết |
| Một Business group lỗi | Service dừng các sibling loops | Supervisor restart, phân biệt lag theo group |

**Implementation hiện tại:** consumer loop xử lý1 in-flight record/runner trên
toàn assignment và tiếp tục poll trong khi callback chạy; pause các partitions
của runner đó. Đây vẫn là baseline cho Business/Storage. **Target Processing
23/10:** keyed concurrency + offset coordinator theo §12, không chỉ tăng
`max_workers`. Partition count giới hạn group members, không bằng số keyed tasks.
Handler không được chờ vô hạn; shutdown/rebalance cần bounded drain và safe
completed-prefix commit, deployment grace phải đủ network/DB timeout.

Không có tự động skip, auto DLQ hoặc retry poison vô hạn trong process. Process
dừng để giữ bằng chứng/offset; restart có backoff và cảnh báo lag do platform cấu
hình. Chưa có application metrics/probes tích hợp trong SDK; owners gắn health
server và metrics trước enablement. Không coi fixture/mock test là benchmark.

Reset consumer offsets hoặc đổi group sẽ replay; đừng xóa topic/outbox/inbox để
"chữa" dữ liệu. Rebuild Business nên dùng shadow projection generation và suppress
alert/RAG side effects; retention inbox/outbox theo cửa sổ replay/backup đã review.
Library không tự dọn dữ liệu hoặc chạy recovery commands trên production.

## 10. Kiểm tra và versioning khi phát triển

```bash
ruff check messaging
ruff format --check messaging
maintai-kafka validate-contracts
python messaging/scripts/lock_contracts.py --check
python -m pytest messaging/tests/unit
python -m build messaging
```

Real integration chỉ bật khi có broker/DB development biệt lập:

```bash
export MAINTAI_KAFKA_INTEGRATION=1
export KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092
export KAFKA_TEST_DATABASE_URL=postgresql://maintai_dev:maintai_dev@127.0.0.1:15432/maintai_messaging_dev
python -m pytest messaging/tests/integration
```

Integration dùng unique group prefix và PostgreSQL schema tạm của test, chỉ chấp
nhận loopback endpoints; không chạy trên cluster/DB dùng chung. CI có jobs riêng
`kafka-ci`/`kafka-integration` thuộc `CI / required`. Chạy test mà không bật biến
opt-in sẽ skip6 real integration cases; skip không phải pass broker/DB.

Thay contract thì thêm fixtures, review reader/writer rồi bump bundle và
`lock_contracts.py --bundle-version ...`. Package Python có version riêng trong
pyproject. GitOps pin SHA-256 manifest contract, image bằng digest/sourceSHA;
không dùng `latest` hoặc giả digest để qua gate. Không bump ML/model version chỉ
vì sửa transport. Deploy reader hỗ trợ cả2 lineage trước khi bật writer1.4.

## 11. Tài liệu chính thức

- [Confluent Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html): callbacks/poll, commit offset kế tiếp, assign/revoke/lost.
- [Kafka4.0 Docker](https://kafka.apache.org/40/getting-started/docker/): môi trường KRaft development.
- [Strimzi0.47 deployment](https://strimzi.io/docs/operators/0.47.0/deploying.html): broker versions, topics và KafkaUser ACL.
- [S3 PutObject](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html): conditional write, checksum và object VersionId.

## 12. PROC-ORDER-01 — target keyed Processing và ordered relay

**Decision1.0.0, source SDK0.2.0 ngày23/10.** Nguồn chuẩn:
[đặc tả concurrency/ordering](PROCESSING_CONCURRENCY_AND_ORDERING.md),
System Contracts §42, Guide §53 và implementation report. CLI hỗ trợ
`consume --role processing --processing-mode keyed`, `relay --mode ordered`;
mode/env opt-in, default serial/legacy. KeyedSettings đọc PROCESSING_MAX_ACTIVE_KEYS
và các budgets trong report, yêu cầu fenced two-phase handler. App DQ/ML/risk/
history callbacks và production deployment chưa được làm thay owner.

| Phần | Người triển khai | Yêu cầu tích hợp |
| --- | --- | --- |
| Processing poll/scheduler | Người1 | Bounded keyed FIFO, một task/máy; khác máy song song trong cùng partition; fair scheduling |
| Offset coordinator | Người1 | Receipt ledger/current assignment; commit safe completed prefix theo partition, không max completed offset |
| DB ownership fence | Người1 | Generation bền vững kiểm atomically với inbox/cursor/history/risk/outbox; RAM callback fence chưa đủ |
| Ordered relay | Người1 | Head-of-key `(topic,machine_id)` + durable dispatch order/key lease; head backoff chặn successor |
| ML budget | Người2 + Người1 | Pinned model, bounded calls/timeouts; late HTTP response không tạo outcome lần hai |
| Kafka/platform | Người4 | Partitions/ACL, retention, poll/grace/pool budgets, dashboard/alerts và load/rebalance evidence |
| Business/UI | Người1 | Inbox, durable-sequence CAS riêng từng projection, signal lifecycle không bị latest-CAS bỏ qua |

Processing handler trả durable receipt sau terminal transaction hoặc verified
exact duplicate, không sau riêng HTTP ML. Coordinator tính cả fetched buffer,
queued work và completed-behind-hole; offset501 xong khi500 chưa durable không
cho commit502. Revoke/lost invalidate generation; stale completion không commit
offset hoặc DB mutation. Per-key FIFO chỉ giữ broker arrival, producer source
order vẫn cần được bảo đảm. Sequence tăng xuyên epoch, không sort opaque epoch.

Legacy relay `ORDER BY created_at ... SKIP LOCKED` chưa có key-order guarantee.
OrderedOutboxRelay đã thêm head-only claim/key lease/cursor bằng migration002;
không sửa SQL bundle1.0.0 tại chỗ. Claim không được bỏ qua predecessor chỉ vì
chưa due/đang leased. Unknown ACK retry cùng ID/key/bytes, không release successor
sớm. Late physical duplicates vẫn có thể xảy ra, không có cross-topic total order.

Giữ `ConsumerRunner` baseline cho Storage/Business; thêm keyed path opt-in cho
Processing. Cutover phải drain/block old workers/relays bỏ qua fences mới. Tests
và work items tại Plan §K. SDK/bundle đã được bump; source/test evidence,
initial legacy-DB bootstrap limit và GitOps config3 pending ở implementation report.

## Capacity baseline100 và mở rộng theoX

Giả định ban đầu100 máy×1 reading/s=100/s. Kiến trúc scale-ready giữ12 partition
và các group độc lập; không có1 consumer/partition cho mỗi máy. TăngX>100 cần
đo/tính riêng source, Processing, Relay, Business và Storage, không chỉ tăng Worker.
[Công thức và latency plan](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100)
ghi rõ serial runner/cycle, keyed active slots và downstream pools. Không đổi
wire bundle, Kafka topology hoặc migration SQL chỉ vì giả định100 máy.


## M6-RAW-01 — Raw catalog, không thay Kafka wire

Theo [M6 decision](M6_HISTORY_ARCHITECTURE_DECISION.md), Chiến/Người4 bổ sung
catalog/coverage vào Storage theo [handoff](M6_RAW_ARCHIVE_HANDOFF.md); không đổi
raw-manifest1.0.0, SDK0.3.0/bundle2.0.0 hoặc SQL001/002. Raw commit boundary giữ
object+manifest; catalog lag và reconciliation từ durable manifests phải công bố
riêng. M6 không bật outcome archive, processed cold history hoặc source cleanup.
Read API tự sở hữu snapshot/export metadata và bounded runner; Worker chỉ realtime,
không thêm processing-maintenance hoặc Kafka command topic cho export. Full demo
raw dùng MinIO local, không cần AWS; fixture không chứng minh provider đã tích hợp.
