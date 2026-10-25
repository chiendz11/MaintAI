# MaintAI Storage Consumer 0.1.0

Một process/image/Deployment archive Kafka vào S3/MinIO. **Chưa cần AWS S3 để
cài package, validate config hoặc chạy unit tests.** `serve` cần Kafka và bucket
đã cấu hình; không có chế độ giả vờ archive thành công.

## 1. Phạm vi và phiên bản

| Subscription | Group | Mặc định |
| --- | --- | --- |
| `sensor.readings` | `raw-archive-v1` | Bật, lưu cả reading feature-invalid |
| `sensor.rejected` | `raw-rejected-archive-v1` | Bật, giữ quarantine do Ingestion đã cho phép |
| `inference.results` | `outcome-archive-v1` | Chỉ bật bằng `STORAGE_ARCHIVE_OUTCOMES=true` |

Dùng `maintai-messaging[s3]==0.2.0` và [Kafka bundle1.1.0](../contracts/kafka/README.md):
accepted1.3.0, rejected1.4.0, optional outcome writer1.4/legacy2.0 giữ nguyên bytes.
Package0.1.0, raw frame/manifest1.0.0, target docs1.4.0 là các version độc lập.
Không đổi wire schemas để triển khai runtime.

Storage không chạy DQ/ML/risk, tạo Alert, SQL vào DB owner khác hoặc tự kích hoạt
training. Processing vẫn sở hữu processed history/curated export/verification/hot
cleanup. Raw archive không thay History API hoặc approved dataset pipeline. Đọc
[Kafka Guide §8](../docs/KAFKA_DEVELOPMENT_GUIDE.md),
[Contract §35](../docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md),
[Guide §44](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md).

## 2. Code và durability

```text
src/maintai_storage/
  settings.py  Offline validation; S3 client qua standard credential chain
  runtime.py   Preflight/probes, consumer loops, supervision và shutdown
  consumer.py  Quan sát SDK poll/rebalance/commit; không tự quyết offset
  archive.py   Retry hữu hạn quanh SDK S3RecordArchive
  state.py     Health/pending state trong RAM, không phải durable checkpoint
  metrics.py   Registry OBS-01
  health.py    Management HTTP nội bộ
  cli.py       verify-config / serve
```

```text
Kafka receipt -> immutable JSONL.gz -> immutable manifest -> durable receipt
              -> SDK synchronous Kafka commit(offset + 1)
```

Manifest freeze `record_count=1`, nên giữ **microbatch một Kafka record**: một
in-flight/group, hai hoặc ba groups cùng process. Polling/rebalance tiếp tục trong
lúc S3 handler chạy. Tăng replicas bị giới hạn bởi partitions và throughput S3.

Frame giữ exact value/key/headers bằng base64 cùng topic/partition/offset và source
timestamp; không parse/loại feature-invalid. HASH_ONLY giữ hash/metadata có sẵn,
không dựng lại body đã bị Ingestion bỏ. Conditional PUT `If-None-Match: *`, SHA-256
và exact object VersionId theo SDK. Key đã có phải cùng bytes; khác bytes dừng.

```text
raw-sensor/topic=sensor.readings/partition=000000/offset=00000000000000000007.jsonl.gz
raw-sensor/topic=sensor.readings/partition=000000/offset=00000000000000000007.manifest.json
raw-sensor/topic=sensor.rejected/partition=.../offset=....jsonl.gz
```

Object xong, manifest lỗi: chưa commit offset; retry/restart verify object cũ rồi
hoàn thành manifest. Manifest xong, Kafka commit lỗi: replay verify cả hai, không
tạo object version mới. Checkpoint bền vững là **Kafka offsets + S3 manifests**,
không thêm SQLite/file offsets. Không đổi bucket/prefix/group sau khi commit nếu
chưa có migration/backfill. Bucket versioning không thay Object Lock/WORM hoặc
quyền bảo vệ dữ liệu khỏi quản trị viên xóa.

## 3. Cài và kiểm tra khi chưa có S3

Từ repo root, Python3.11/3.12:

```bash
python3.12 -m venv storage-consumer/.venv
source storage-consumer/.venv/bin/activate
python -m pip install -r storage-consumer/requirements-dev.lock setuptools==84.0.0 wheel==0.48.0
python -m pip install --no-deps --no-build-isolation -e ./messaging -e ./storage-consumer
KAFKA_LOCAL_MODE=true KAFKA_SECURITY_PROTOCOL=PLAINTEXT S3_RAW_BUCKET=maintai-local \
  maintai-storage verify-config
python -m pytest storage-consumer/tests
ruff check storage-consumer
ruff format --check storage-consumer
```

`verify-config` không tạo boto3/Kafka client, lấy credentials, mở kết nối hoặc tạo
bucket/topic. Nó kiểm cấu hình/contracts/fixtures, trả `network_checked=false`.
`maintai-local` chỉ là tên fixture chưa được tạo. Đây không phải connectivity test.
SDK chưa phát hành PyPI, phải cài `messaging/` hoặc wheel tương ứng cùng Storage.
Lockfiles giữ dependencies bên thứ ba; `.in` là inputs cho pip-tools.

## 4. Cấu hình AWS sau này hoặc MinIO local

Copy [.env.example](.env.example) sang `storage-consumer/.env` đã được Git ignore.
CLI chỉ đọc environment, **không tự load dotenv**:

```bash
set -a
source storage-consumer/.env
set +a
maintai-storage verify-config
maintai-storage serve
```

| Setting | Ý nghĩa/default |
| --- | --- |
| `S3_RAW_BUCKET` | Bắt buộc: bucket name, không `s3://...` hoặc presigned URL |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | Bucket region, AWS_REGION ưu tiên |
| `S3_ENDPOINT_URL` | Trống cho AWS; custom S3/MinIO endpoint nếu cần |
| `S3_RAW_PREFIX` | `raw-sensor`, giữ ổn định xuyên restart/rollout |
| `S3_ADDRESSING_STYLE` | `auto`; MinIO thường `path` |
| `S3_ALLOW_UNVERSIONED` | `false`; chỉ được bật trong explicit Kafka local mode |
| `KAFKA_*` | Bootstrap, SASL_SSL/SCRAM-SHA-512, CA và credentials theo SDK |
| `STORAGE_ARCHIVE_OUTCOMES` | `false`, cần topic/group ACL trước khi bật |
| `STORAGE_HTTP_HOST` / `STORAGE_HTTP_PORT` | `0.0.0.0` / `8008`, management nội bộ |
| `STORAGE_RETRY_ATTEMPTS` | `5`, giới hạn1..10 attempts/receipt |
| `STORAGE_RETRY_INITIAL_SECONDS` / `STORAGE_RETRY_MAX_SECONDS` | `1` / `15`, exponential backoff + jitter |
| `S3_CONNECT_TIMEOUT_SECONDS` / `S3_READ_TIMEOUT_SECONDS` | `3` / `10`, SDK total_max_attempts=1 để không nhân retry hai tầng |
| `STORAGE_PROBE_INTERVAL_SECONDS` / `STORAGE_PROBE_TIMEOUT_SECONDS` | `15` / `3`, background cadence / Kafka metadata timeout |
| `STORAGE_READINESS_MAX_AGE_SECONDS` / `STORAGE_LIVENESS_MAX_AGE_SECONDS` | `60` / `90`, validation giữ budget phù hợp dependency probes |

**AWS thường không cần endpoint URL**: cần bucket name + region + credentials/quyền
phù hợp. Dùng profile khi dev hoặc IRSA/standard credential chain khi deploy; không
hard-code access key. Custom endpoint phải TLS ngoài explicit local mode. Các
`AWS_ENDPOINT_URL*` override bị bỏ qua để không bypass endpoint validation.
TLS verification bật; private CA dùng `AWS_CA_BUNDLE`, không có `verify=false`.

Runtime IAM: `s3:GetBucketVersioning` trên bucket; `s3:PutObject`, `s3:GetObject`
trên đúng prefix; thêm KMS grants nếu SSE-KMS. Kafka cần Describe/Read topics và
Read groups tương ứng. Không wildcard admin. Runtime không create/delete bucket/
topic hoặc bật versioning. Platform owner cấu hình versioning, encryption,
retention, IAM, network và Kafka ACL trong GitOps/hạ tầng.

Preflight dùng GetBucketVersioning + metadata trước khi mở consumers; chưa chứng
minh quyền PUT mọi prefix/group ACL hoặc throughput. Các lỗi write/consume thật
vẫn phải giữ offset. Tài liệu API tham khảo:
[Boto3 configuration](https://docs.aws.amazon.com/botocore/latest/reference/config.html),
[S3 versioning API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html).

## 5. Failure, probes và shutdown

| Tình huống | Hành vi |
| --- | --- |
| Chưa có Kafka/S3/versioning | Management chạy, readiness503, chưa consume/write |
| Timeout/connection/429/5xx hoặc conditional409 | Retry cùng record, bounded backoff, Kafka tiếp tục poll |
| AccessDenied, missing bucket khi write, conflict/config lỗi | Không blind retry, dừng sibling groups, exit1, chưa commit |
| Hết retry budget | Exit1, broker giữ record để redeliver khi service restart |
| Kafka commit thất bại | Không xử lý record tiếp, replay verify archive đã có |
| Rebalance/lost assignment | SDK chặn stale offset commit; writer có thể hoàn tất object an toàn |
| SIGTERM/SIGINT | Readiness xuống, ngừng nhận mới, ngắt backoff, chờ in-flight attempt; chỉ durable record có thể commit |

`GET /health/ready` dùng dependency probes còn hạn + loop health.
`GET /health/live` dùng local supervision/poll heartbeat, không fail chỉ vì Kafka/
S3 unavailable. HTTP probes chỉ đọc cache; không gọi network. Shutdown đóng consumers
trước S3 client. Termination grace cần đủ cho network calls/close; default90s là
điểm bắt đầu để đo, chưa phải rolling-update evidence.

Kafka retention phải đủ outage + catch-up + margin. Service không khôi phục được
raw đã hết retention nhưng chưa archive; `earliest` không bảo đảm chống retention
loss. Platform cần cảnh báo lag/retention gaps và đo catch-up. Storage không có
local spool và không hứa nhận dữ liệu vô hạn khi broker đầy.

## 6. Metrics và logs

`GET /metrics`, registry một process, theo [OBS-01](../docs/OBSERVABILITY_IMPLEMENTATION.md):

- `maintai_storage_batches_total{status="COMMITTED|RETRY|FAILED"}`: COMMITTED sau
  archive durable **trước Kafka ACK**, replay có thể tăng lại; không unique count.
- `maintai_storage_write_duration_seconds{outcome="SUCCESS|ERROR"}`: IO buckets
  đúng catalog; một archive attempt có thể gồm nhiều S3 requests.
- `maintai_storage_oldest_uncommitted_age_seconds`: tuổi từ local admission tới
  offset ACK của oldest owned batch; clear khi commit/revoke/close. Không broker-wide
  lag hoặc thời gian từ Sensor. Idle0 không chứng minh broker backlog0.
- `maintai_dependency_errors_total{dependency,error_type}`: lỗi quan sát tại
  runtime boundary, không cộng với provider/server counters thành unique failures.

Các groups aggregate chung, giữ label signature; không thêm archive_kind/machine/
event/partition labels. JSON logs có role/topic/partition/offset/attempt và error
type, không payload, tokens, credential/config dump hoặc provider exception message.
Collector lỗi được log, không đổi durability/offset. Management không có browser
ingress; NetworkPolicy/scrape grants thuộc GitOps.

## 7. Image, CI và integration thật

Build từ **repo root** để đóng gói SDK/contracts:

```bash
docker build --file storage-consumer/Dockerfile --tag maintai-storage:dev .
docker run --rm --network none --read-only --tmpfs /tmp \
  -e KAFKA_LOCAL_MODE=true -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT \
  -e S3_RAW_BUCKET=maintai-local maintai-storage:dev verify-config
```

Python base pin OCI digest, UID10001, không cần writable app filesystem/checkpoint
volume. Dockerfile-specific ignore chỉ gửi source/lock/contracts cần thiết, không
dataset/.env/.git. Không nhúng credentials hoặc release digest vào image.

CI `storage-ci`: Python3.11/3.12 lint/tests, wheel/sdist, cài wheel vào env mới và
verify ngoài checkout. `storage-container`: build image, smoke offline/non-root/
read-only. Cả hai thuộc `CI / required`; không publish registry/deploy/secrets.
Thêm workflow chưa chứng minh container hoặc remote checks đã chạy.

Tests thông thường dùng in-memory Kafka/S3 fixtures + actual botocore Stubber,
không xác nhận AWS/MinIO/SASL/TLS production. Integration thật mặc định skip:

```bash
MAINTAI_STORAGE_INTEGRATION=1 \
  STORAGE_TEST_KAFKA_BOOTSTRAP=127.0.0.1:19092 \
  STORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  python -m pytest storage-consumer/tests/integration -v
```

Harness chỉ nhận loopback dành riêng cho test. Optional credentials:
`STORAGE_TEST_S3_ACCESS_KEY` / `STORAGE_TEST_S3_SECRET_KEY`, mặc định `minioadmin`.
Test tạo bucket versioned ngẫu nhiên + group prefix riêng, kiểm wire/hash/VersionId
và offsets, xóa fixture bucket ở finally; không xóa topics. SDK bootstrap có thể
tạo4 topics với12 partitions nên phải dùng broker test riêng. Không trỏ vào AWS,
production/shared bucket. Runtime không có quyền create/delete của test harness.
