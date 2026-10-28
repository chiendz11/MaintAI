# MaintAI Ingestion API — M1

HTTP edge độc lập: authenticated raw request → SDK gateway → Kafka ACK → HTTP.
Implementation local có unit/HTTP contract tests, real Kafka integration và image
smoke; **chưa nghiệm thu shared OIDC/TLS/ACL hoặc deploy Kubernetes**. Processing,
Business và Processing có vòng triển khai/nghiệm thu riêng.

## Ranh giới và phiên bản

- `POST /api/v1/sensor-readings`: auth/envelope/size/admission; không DQ sâu,
  ML, DB, RAG, SSE hoặc gọi Business HTTP.
- `sensor.readings` wire1.3.0 keyed `machine_id`; `sensor.rejected` wire1.4.0 keyed
  verified source subject, HASH_ONLY. SDK0.3.0 dùng canonical Kafka bundle2.0.0;
  lượt chuẩn hóa metrics này không đổi wire/bundle.
- HTTP Ingestion OpenAPI1.4.0 là additive MINOR cho M1. Các HTTP APIs khác vẫn
  baseline1.3.2, không suy full target1.4 đã được triển khai.
- HTTP ACK bỏ `schema_version` nội bộ SDK. Error được map sang HTTP Error hiện có;
  `rejection_id` ở `error.details`. Xem [quyết định HTTP](../docs/api-contract-decisions.md#ingest-m1-01--http-adapter-và-operational-profile-2026-10-23).

## Cài đặt development (Python3.12)

Từ root repo; `.venv` và `.venv-contracts` tách riêng vì lockfiles khác nhau.
Nếu chưa có `.venv`, tạo bằng `python3.12 -m venv .venv` trước.
SDK nội bộ được build từ checkout, không lấy từ public registry.

```bash
.venv/bin/python -m pip install build==1.6.1 setuptools==84.0.0 wheel==0.48.0
.venv/bin/python -m build messaging --wheel --outdir /tmp/maintai-m1-wheels
.venv/bin/python -m pip install --no-deps /tmp/maintai-m1-wheels/maintai_messaging-0.3.0-py3-none-any.whl
.venv/bin/python -m pip install --find-links /tmp/maintai-m1-wheels -r ingestion-api/requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e ./ingestion-api
```

`requirements.in`/`pyproject.toml` định nghĩa dependencies trực tiếp; `.lock` pin
closure. Để regenerate bằng `uv` đã cài, build SDK wheel như trên rồi:

```bash
uv pip compile ingestion-api/requirements.in --python .venv/bin/python --find-links /tmp/maintai-m1-wheels --no-emit-find-links --no-header -o ingestion-api/requirements.lock
uv pip compile ingestion-api/requirements-dev.in --python .venv/bin/python --find-links /tmp/maintai-m1-wheels --no-emit-find-links --no-header -o ingestion-api/requirements-dev.lock
```

Kiểm tra diff versions, `pip check`, app và SDK tests sau regenerate; không sửa
Messaging/Contracts locks để đồng bộ tùy tiện. Runtime image cài offline từ
wheelhouse; SDK wheel được build từ source của repo và có bundled contracts.
SDK hiện pin0.3.0 để dùng cùng source Kafka concurrency/ordering; Ingestion vẫn
chỉ dùng gateway/publisher, không bật keyed Processing hoặc ordered relay.

## Chạy local với fixture identity

Profile này chỉ dành loopback; không xác thực người gọi bằng token. Nó được bật
explicit, giả lập một nguồn được phép M-001, không phải OIDC production.
Không bind0.0.0.0, public tunnel hoặc đưa profile này lên shared deployment.

```bash
MAINTAI_KAFKA_PORT=29092 docker compose -p maintai-m1 -f messaging/tests/integration/compose.yaml up -d --wait kafka
set -a
source ingestion-api/.env.example
set +a
.venv/bin/maintai-kafka create-local-topics
.venv/bin/maintai-ingestion
```

Shell khác gửi request đúng envelope mới:

```bash
curl -i -H 'Content-Type: application/json' --data-binary @contracts/kafka/examples/sensor-request.json http://127.0.0.1:8000/api/v1/sensor-readings
```

202 chỉ chứng minh broker ACK. `status_url` trỏ Business origin được cấu hình;
M1 không gọi URL đó và Business có thể chưa chạy. Không mong thấy prediction/Alert
trong ACK. Sensor0.3.0 đã có client ACK202 và durable spool; xem
[Sensor README](../sensor/README.md). Source/tests local không chứng minh đã nghiệm
thu Sensor→Ingestion với OIDC/TLS/ACL trên shared deployment.

## Cấu hình

App đọc environment explicit; không tự nạp `.env`. `.env.example` chỉ là fixture.

| Biến | Ý nghĩa/default |
| --- | --- |
| `INGESTION_ENV` | `shared` default; explicit `local`/`test` cho fixtures |
| `INGESTION_HOST`, `INGESTION_PORT` | CLI default127.0.0.1:8000; shared image đặt host theo deployment |
| `BUSINESS_PUBLIC_ORIGIN` | Bắt buộc absolute origin, không path/query/userinfo; shared phải HTTPS |
| `AUTH_MODE` | `oidc` default; explicit loopback-only `local-fixture` |
| `LOCAL_SOURCE_SUBJECT`, `LOCAL_ALLOWED_MACHINE_IDS` | Fixture-only, explicit subject/machines; không wildcard |
| `OIDC_ISSUER`, `OIDC_JWKS_URL` | Trusted provider URLs bắt buộc trong OIDC; HTTPS ngoài local/test-loopback |
| `OIDC_AUDIENCE` | `ingestion` |
| `OIDC_ALGORITHMS` | Allowlist defaultRS256; asymmetric algorithms, không tin token `alg` để chọn policy |
| `OIDC_ROLES_CLAIM`, `OIDC_MACHINES_CLAIM` | Bắt buộc explicit tên claim; array strings, role SENSOR_SERVICE, machines không wildcard |
| `OIDC_JWKS_TIMEOUT_SECONDS`, `OIDC_JWKS_CACHE_SECONDS` | Default2/300; JWKS size bound128KiB, tối đa128 keys; refresh có cooldown |
| `INGESTION_MAX_INFLIGHT` | Default8/process, admission bao gồm JWT/JWKS work; trả429 khi đầy, không phải global RPS quota |
| `INGESTION_BODY_TIMEOUT_SECONDS` | Default10;408 nếu stream chưa đủ, không publish partial body |
| `INGESTION_PROBE_TIMEOUT_SECONDS`, `INGESTION_PROBE_INTERVAL_SECONDS` | Default1/5; check cũ hơn10s not-ready |
| `KAFKA_*`, `PRODUCER_GIT_SHA` | Dùng SDK validation; shared SASL_SSL/SCRAM/CA và Git SHA thật |

Không có database settings/credentials trong runtime này. Không đặt token/secret
trong file tracked, command history hoặc logs. Shared claim mapping phải xác nhận
với Platform; fixture mapping không tự chứng minh provider thực tế tương thích.

## HTTP behavior và retry

| Tình huống | Kết quả |
| --- | --- |
| Envelope hợp lệ, Kafka ACK | 202 ACCEPTED/PENDING, absolute Location |
| Envelope hợp lệ nhưng feature sai | 202 sau ACK; Processing quyết định INVALID sau |
| Malformed JSON hoặc envelope sai có auth/size hợp lệ | HASH_ONLY rejected ACK rồi400/422 |
| Kafka unavailable / ACK uncertain | 503/504 retryable; giữ event ID và payload, không suy chưa tới broker |
| Missing/invalid JWT / role hoặc machine không được phép | 401/403; không publish |
| Oversize / sai media hoặc compressed body / chậm body | 413/415/408; không publish partial body |
| Admission đầy | 429, Retry-After1; bounded retry |
| JWKS unavailable, không valid cache | 503 fail-closed; không bỏ signature verification |

Retry rejection giữ request ID cùng raw bytes để deterministic rejection ID;
reading retry giữ event ID/sequence/features. Kafka idempotent producer không
cam kết exactly-once xuyên client retries/processes. Auth401/403, malformed400 và
validation422 không retry mù. Wrapper không tự retry publish sau uncertainty.

## Health, metrics, lifecycle

- `/health/live`: process sống; không phụ thuộc Kafka để tránh restart loop.
- `/health/ready`: config/Kafka metadata/auth key state. Check stale, missing topics,
  unavailable dependency hoặc shutdown drain→503. Metadata không chứng minh produce
  ACL, ISR quorum, archive S3 hay Business endpoint.
- `/metrics`: catalog [OBS-01 §3.1–3.2](../docs/OBSERVABILITY_IMPLEMENTATION.md),
  một registry/process, Content-Type từ Prometheus client. Platform giới hạn
  network access. Không machine/event IDs, tokens, bodies hoặc raw URLs làm labels.
- Một publisher/process, blocking SDK/JWKS/probes qua worker threads. Khi request
  bị cancel, đợi worker kết thúc trước release admission slot; khi shutdown drain
  rồi flush. Client disconnect/timeout sau enqueue vẫn có thể đã tới Kafka.
- SIGTERM graceful timeout tính từ body/publish budget; logs không echo exceptions,
  credential/body. Access log server tắt; app chỉ log trace IDs/route/status/duration.

| Metric | Semantics / labels |
| --- | --- |
| `maintai_http_requests_total` | REST kết thúc; `route,method,status`, route template hoặc `__unmatched__` |
| `maintai_http_request_duration_seconds` | HTTP histogram; `route,method`; bỏ health/scrape |
| `maintai_ingestion_accepted_total` | Sau broker ACK `sensor.readings`, trước202; retry có thể tăng lại |
| `maintai_ingestion_rejected_total` | Một terminal rejection/request; `reason=MALFORMED_JSON/VALIDATION_FAILED/AUTH/SIZE/OTHER` |
| `maintai_ingestion_publish_failures_total` | Publish lỗi; `error_type` hữu hạn theo OBS-01 |
| `maintai_ingestion_ack_duration_seconds` | Admission trước auth/body → ACK hoặc publish error; `outcome=ACCEPTED/REJECTED/ERROR` |
| `maintai_dependency_errors_total` | Lỗi call Kafka/JWKS thật; `dependency,error_type`; không đếm invalid JWT local thành provider lỗi |
| `maintai_metric_samples_dropped_total` | Bỏ latency thiếu thời điểm/âm/không hữu hạn; `reason` theo OBS-01 |

Hai histogram dùng HTTP buckets `0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2,5,10`
seconds, thêm `+Inf`. Auth/size/admission reject không publish thì không có ACK
sample. Quarantine publish lỗi trả503/504 không được đếm thành completed rejection.
Quarantine ACK thành công vẫn đếm rejection nếu client hủy request trước khi nhận
response; middleware không đếm lại rejection đã ghi ở ACK.
ACK bất định dùng `UNAVAILABLE`; chỉ dùng `TIMEOUT` khi exception type chứng minh.
Instrumentation lỗi ghi log an toàn, không đổi ACK/HTTP; scrape lỗi phải hiện thiếu
dữ liệu, không giả counter0. Các tên cũ `maintai_ingestion_http_*`,
`maintai_ingestion_publish_total`, `maintai_ingestion_publish_duration_seconds` và
inflight gauge đã gỡ; Người4 cập nhật queries/rules/dashboard ở repo Platform.

Source HTTP/auth/guards/metrics ở `src/maintai_ingestion/`; gateway/envelope và
Kafka transport dùng SDK `messaging/`. Bốn placeholder `http_ingestion`, `envelope`,
`kafka_producer`, `observability` đã dọn, không tạo implementation trùng SDK.

## Kiểm tra

Unit và HTTP contracts dùng real gateway + controlled publisher; JWT tests verify
chữ ký bằng keys sinh trong test. TestClient có thể bị sandbox chặn cross-thread
wakeup; cùng tests đã chạy ngoài sandbox. Pinned Starlette hiện cảnh báo deprecation
HTTPX fallback; đó là cảnh báo test transport, không phải test bị skip.

```bash
PYTHONPATH=ingestion-api/src:messaging/src .venv/bin/python -B -m pytest -p no:cacheprovider ingestion-api/tests/unit ingestion-api/tests/contract
.venv/bin/ruff check ingestion-api
.venv/bin/ruff format --check ingestion-api
.venv-contracts/bin/python contracts/v1.3/validate.py
.venv/bin/python messaging/scripts/lock_contracts.py --check
```

Real Kafka tests yêu cầu broker riêng và không tự skip dependency failure khi enabled:

```bash
MAINTAI_INGESTION_INTEGRATION=1 KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:29092 PYTHONPATH=ingestion-api/src:messaging/src .venv/bin/python -B -m pytest ingestion-api/tests/integration/test_http_kafka.py
```

```bash
docker build -f ingestion-api/Dockerfile -t maintai-ingestion:m1-local .
MAINTAI_INGESTION_CONTAINER_TEST=1 MAINTAI_INGESTION_IMAGE=maintai-ingestion:m1-local KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:29092 PYTHONPATH=ingestion-api/src:messaging/src .venv/bin/python -B -m pytest ingestion-api/tests/integration/test_container.py
```

Trên broker mới, chạy Kafka tests/bootstrap topics trước container smoke.
Container smoke dùng Linux host networking, JWKS fixture local + JWT signature,
real broker, non-root10001, installed packages/contract resources và SIGTERM.
Không publish image, không gọi provider production. Broker không bị xóa dữ liệu;
để dừng project test riêng dùng `docker compose -p maintai-m1 -f messaging/tests/integration/compose.yaml stop kafka`.
Không `down -v` trên dữ liệu cần giữ.

CI có `ingestion-ci` và `ingestion-integration` trong `CI / required`; việc thêm
workflow chưa là bằng chứng GitHub Actions đã chạy đúng commit. K8s manifests,
production secrets/ACL, Prometheus rules và Grafana dashboards thuộc repo Platform.
