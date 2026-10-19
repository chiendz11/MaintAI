> **Tích hợp MaintAI — 2026-10-19:** Tài liệu thiết kế mục tiêu v1.4, chưa xác nhận code/DB/GitOps đã migrate. Đọc [docs index](README.md) để áp dụng quyết định nhóm 4 người, Simulator UI riêng và thứ tự ưu tiên; [readiness](implementation-readiness.md) ghi các schema/scaffold chưa được cung cấp.

> **v1.4 — Chuẩn Deployment và Versioning/Provenance (19/10/2026).** Đây là bản đầy đủ kế thừa v1.3.1, bổ sung yêu cầu v1.4 có thứ tự ưu tiên cao hơn khi mâu thuẫn. Chỉnh sửa Markdown **không** đồng nghĩa đã thay source, GitOps, DB hoặc runtime.
> **Đọc trước khi triển khai:** `DEPLOYMENT_ARCHITECTURE.md`, `RUNTIME_DATAFLOW.md`, `VERSIONING_AND_PROVENANCE.md`, `IMPLEMENTATION_PLAN.md`; JSON Schemas và K8s scaffold đặt trong `../contracts/` và `../implementation/`.

# MaintAI — Hướng dẫn triển khai và phát triển chi tiết từng module — v1.4

> **Mục đích:** Sổ tay triển khai cho nhóm 4 người theo phân công tại docs index xây dựng MaintAI — hệ thống dự đoán hỏng máy và hỗ trợ kỹ thuật viên.
>
> **Nguồn thiết kế:** `MaintAI_BRD_KienTruc_KeHoach_5_Nguoi.docx` và contract hệ thống `MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md`.
>
> **Nguyên tắc:** Tài liệu này **không thay đổi BRD**. Các mục ghi **[BRD bắt buộc]** là invariant phải giữ. Các mục ghi **[Đề xuất triển khai]** là cách tổ chức/code khuyến nghị để nhóm làm nhanh, dễ tích hợp và dễ kiểm thử.
>
> **Lịch sử v1.2:** giữ Drift/ML/RAG rules; chuyển sang Kafka event-driven, HTTP Ingestion trả 202, Processing/Business split, Storage Consumer→S3 raw, 5 logical DBs. Các phần cũ được kế thừa chỉ khi không mâu thuẫn với v1.2; machine-readable contracts/runtime cần migrate riêng.

> **Update v1.3 (quyết định mới):** UI “Live Telemetry + Latest Valid Prediction”, `sensor.readings` (đổi từ tên 1.2 `sensor.raw`), `inference.results` và `alert.signals`; thêm live projection, SSE fan-out đa replicas và thứ tự dữ liệu. **§40 là nguồn hướng dẫn thực thi Frontend cập nhật**, các ví dụ đồng bộ/polling tùy chọn cũ chỉ là hướng lịch sử khi không mâu thuẫn.

---

# 1. Kiến trúc cần triển khai — Scale-Ready / Event Driven / UI C (v1.3.1)

**Ingestion API độc lập** (không đặt trong Business Backend), Processing Worker tạo prediction/risk và ghi `processing_db`, Business Backend nhận Kafka events thông qua **Business Consumer** (các handlers telemetry/prediction/alert) để cập nhật **latest projections và alerts**, còn Business API chỉ phục vụ REST/SSE/technician actions/control facade. Không tạo riêng Telemetry/Prediction microservices. Deploy độc lập `ingestion-api`, `processing-worker`, `storage-consumer`, `business-consumer`, `business-api`, `ml-service`, `rag-service`; Drift CronJob, Training batch, MLflow server riêng.

```text
Sensor Simulator [RUNNING | PAUSED; local retry/rejected log; scenarios]
    | HTTP POST (new records only if RUNNING)
    v
INGESTION API [separate deployment; transport auth, envelope, Kafka producer]
    | Kafka broker ACK -> HTTP 202; failures -> retry/local buffer
    v
KAFKA sensor.readings [key = machine_id]
    |                       |                         |
    | group processing     | group business-live     | group raw-archive
    v                       v                         v
PROCESSING WORKER      BUSINESS CONSUMER         STORAGE CONSUMER
 DQ -> ML Service       telemetry handler         batch JSONL.gz
    -> risk              (latest snapshot)         -> S3/MinIO raw
    |                    |                         [all accepted raw]
 processing_db          |
    + outbox             |    Kafka inference.results <--- Processing outbox
    |                    +<-- business prediction handler (no ML computation)
    +--> alert.signals ------> business alert handler (dedupe/lifecycle)
                               |
                        business_db (machines, alerts, latest UI projections)
                               | durable projection commit -> NOTIFY / pub-sub
                        BUSINESS API Deployment [REST + SSE]
                               |
                          REACT FRONTEND
                   Live Telemetry (may be UNVERIFIED)
                   Latest Valid Prediction (separate event, age)
                   Latest Processing + Active Alerts
                   Simulator Control (demo only; Business API facade)
                               |
                    POST /internal/simulator/... (authorized)
                               v
                    Sensor Simulator Control API
                    (pause/resume/scenario per machine)

Drift CronJob: S3 curated / Processing read API -> drift_db + S3 drift-reports
Training Job: curated S3 -> MLflow API -> mlflow_db + S3 model artifacts
RAG/LLM: knowledge_db (pgvector) + S3 manuals; invoked off alert critical path
POSTGRESQL: 1 instance, 5 logical DBs: processing, business, drift, mlflow, knowledge
```

## 1.1. Online/realtime boundaries

- Ingestion HTTP ACK sau Kafka ACK; retry khi Kafka lỗi, không validate feature deep ở ingress.
- Group business-live nhận `sensor.readings` sớm hơn ML để cập nhật telemetry **unverified**; Group Processing chạy DQ/ML/Risk và outbox `inference.results`/`alert.signals`.
- Business Consumer nhận prediction outcome, chỉ sao latest valid prediction + last processing status, alert consumer thực hiện lifecycle; Business API phát SSE từ business-db projections + notification fan-out.
- Group Storage batch raw accepted events vào S3, không ghi Business DB. Drift, RAG không chặn alert.
- SSE không cần raw events được ghi vào DB. Latest snapshots bền vững chỉ để REST reload/reconnect; PG LISTEN/NOTIFY hỗ trợ nhiều API replicas, notification loss khôi phục qua REST.

## 1.2. Source/runtime/data ownership

| Runtime / trách nhiệm | Sở hữu | Triển khai / dữ liệu |
|---|---|---|
| Sensor Simulator + control API | `sensor/` | Một control instance demo; per-machine RUNNING/PAUSED, scenario, retry buffer; không là PLC |
| Ingestion API | Ingestion boundary riêng, **không nằm trong Business Backend** | Deployment `ingestion-api`; HTTP → Kafka; không DB, không DQ sâu, không ML |
| Processing Worker | Processing Service | Deployment `processing-worker`; DQ, ML client, risk, `processing_db`, transactional outbox |
| ML Inference Service | ML serving | Deployment `ml-service`; model predict, không tạo risk alert |
| Storage Consumer | Raw archive | Deployment `storage-consumer`; `sensor.readings` → S3; không ghi `business_db` |
| Business Consumer (3 handlers) | Business Backend | Deployment `business-consumer`; telemetry/prediction **projection** và alert handler; không chạy ML |
| Business API | Business Backend | Deployment `business-api`; REST, SSE, RBAC, alerts/feedback, simulator control facade |
| `business_db` | Business Backend | Machine registry, alert lifecycle, read models **chỉ latest telemetry/last valid prediction/processing status**, không full raw history |
| Drift Worker | Drift | CronJob; API/S3 read, `drift_db`; không consume raw trực tiếp theo mặc định |
| ML Training + MLflow Server | Training/Registry | MLflow API, `mlflow_db`, S3; không retrain chỉ vì raw file được upload |
| RAG/LLM | Knowledge | `knowledge_db`+S3 manuals; copilot độc lập alert path |
| React Frontend | UI | Chỉ Business REST/SSE; live telemetry và prediction là 2 dữ liệu có nguồn thời gian khác nhau |

# 2. Stack kỹ thuật đề xuất

Đây là lựa chọn triển khai đề xuất, không phải yêu cầu bắt buộc của BRD.

| Khối | Stack đề xuất |
|---|---|
| Sensor Simulator | Python + `httpx`/`requests` + pandas |
| Backend codebase (nhiều entrypoints) | FastAPI + Pydantic + SQLAlchemy + Alembic + Kafka Python client |
| ML Training | pandas + scikit-learn + imbalanced-learn nếu cần + joblib |
| ML Serving | FastAPI + joblib/scikit-learn + Prometheus client |
| Drift Monitoring | Python batch worker + pandas/numpy/scipy hoặc Evidently tùy chọn + SQLAlchemy/psycopg |
| RAG/LLM | FastAPI + pgvector + embedding model/API + LangChain tùy chọn |
| Frontend | React + TypeScript + Vite + TanStack Query hoặc fetch wrapper |
| Database | PostgreSQL (5 logical DBs), pgvector chỉ knowledge_db, riêng MLflow metadata |
| Event broker | Apache Kafka; 3 topics `sensor.readings`/`inference.results`/`alert.signals`, consumer groups độc lập |
| Object storage | S3/MinIO raw archive, curated datasets, model artifacts, drift reports, RAG documents |
| Observability | Prometheus + Grafana + structured JSON logs; Pushgateway cho Drift Worker one-shot |
| Local orchestration | Docker Compose |
| Cloud-native extension | Kubernetes/k3d nếu môn học/demo cần |
| CI | GitHub Actions |

Khuyến nghị pin cùng một Python version mà toàn bộ dependency hỗ trợ ổn định; không để mỗi máy dùng một version khác nhau.

---

# 3. Codebase + Deployment layout đề xuất (giữ tên repo thực tế nếu có)

Với các folders hiện có `sensor/`, `ml-service/`, `rag-service/`, `frontend/`, `drift-worker/`, phần dưới là **ownership boundary logic**. Được quyền tổ chức trong monorepo, nhưng container image và startup command tách rõ. Không mặc định phải refactor toàn repo trước khi chạy unit tests.

```text
maintai/
├── sensor/                        # simulator generator, retry spool, internal control API
│   ├── control/                   # pause/resume/scenario per machine
│   └── scenarios/
├── ingestion-api/                 # independent HTTP -> Kafka producer; no business DB
│   └── src/
├── processing/                    # processing owner; may share repo with ingestion but not DB
│   ├── src/data_quality/
│   ├── src/inference_client/
│   ├── src/risk_engine/
│   ├── src/outbox/
│   └── migrations/                # processing_db only
├── business/                      # one codebase, TWO Deployments
│   ├── src/consumers/             # telemetry_handler, prediction_handler, alert_handler
│   ├── src/api/                   # REST, SSE and simulator facade
│   ├── src/projections/           # latest snapshots only, ordering guards
│   ├── src/alerts/                # lifecycle, feedback, audit
│   └── migrations/                # business_db only
├── storage-consumer/              # Kafka -> S3 raw JSONL.gz batch
├── ml-service/                   # training + inference
├── rag-service/
├── drift-worker/                 # CronJob
├── frontend/
├── contracts/                    # machine-readable OpenAPI/event JSON Schemas (separate updates)
├── docs/
└── docker-compose.yml           # GitOps/platform repo may manage K8s separately
```

**Entrypoints/runtimes:** `ingestion-api` (HTTP), `processing-worker` (Kafka), `processing-outbox-relay` (process/task), `storage-consumer` (Kafka), `business-consumer` (Kafka modules), `business-api` (REST/SSE), `ml-service`, `rag-service`, `drift-worker` (CronJob), `sensor` (simulator + optional internal control HTTP). `Business Consumer` handles different topics via handlers — do not split to `telemetry-service` and `prediction-service` unless real scaling/testing requires it.

## 3.1. Shared conventions

- Config/credentials via environment/secret manager, not hard-coded; no SQL access across ownership areas.
- Every Kafka event: immutable `event_id`/`result_id`, correlation_id, schema version, machine_id key and stable ordering token.
- Sensor per-machine `sequence_number` must **survive pause/resume and restart**; do not sort opaque event_id or textual epoch. If an old simulator cannot provide this, introduce an ingress-assigned durable `machine_ingest_seq` before relying on latest-projection ordering.
- Producer broker ACK ≠ ML/DQ processed; Consumer commit offsets only after durable side effects; use DB outbox/inbox and S3 deterministic batches.
- Observability: HTTP readiness, Kafka lag (per group), SSE subscriber count/error/reconnect, DB pool, ML latency, drift freshness, archive age.
- Keep UI enums separated (stream RUNNING/PAUSED vs telemetry freshness vs machine risk vs alert lifecycle); log event-level errors and do not silently normalise UNKNOWN/INVALID.

# 4. Local boot / execution topology

Boot dependencies theo tầng, nhưng đừng dựa vào `depends_on` thay readiness:

```text
1. PostgreSQL (+ pgvector tại knowledge_db) và MinIO
2. Kafka broker + create sensor.readings, alert.signals topics
3. Chạy migrations processing_db, business_db, drift_db, knowledge_db;
   MLflow Server tự quản lý migration/schema của mlflow_db
4. MLflow Server, ML Inference Service, RAG Service
5. Processing Read API, Business API, Outbox Relay
6. Ingestion API, Processing Consumer, Storage Consumer, Business Consumer
7. Frontend, Prometheus/Pushgateway/Grafana
8. Sensor Simulator, rồi Drift Worker one-shot khi đủ VALID sample
```

Local ports ví dụ: Ingestion `8000`, Business API `8003`, Processing Read `8004`, ML `8001`, RAG `8002`, Kafka `9092` (container và host advertised listeners phải phân biệt), MinIO API `9000`, UI `9001`, PostgreSQL `5432`, MLflow `5000`, Prometheus `9090`, Grafana `3000`. **Không coi đây là network/security config production.**

`docker compose up -d` chỉ boot long-running containers; Drift CronJob có thể chạy `docker compose run --rm drift-worker`. Đường HTTP ingress không gọi ML ngay. Test chức năng phải chờ event materialized/asynchronous alert, không assume trả 200 + completed.

---

# 5. Module A — Sensor Simulator

**Owner chính:** Người4; phối hợp Người1 về Ingestion/status interface.

## 5.1. Mục tiêu

**[BRD bắt buộc]** Simulator phải:

- đọc AI4I;
- replay từng record thành sensor event;
- gán `machine_id`, `event_id`, `received_at` phục vụ mô phỏng;
- điều chỉnh tốc độ replay;
- hỗ trợ deterministic seed;
- fault injection: missing field, invalid type/range, duplicate, disconnect/timeout;
- không dùng `machine_id`/timestamp giả lập làm ML feature lịch sử.

## 5.2. Cấu trúc code

```text
sensor-simulator/
├── src/
│   └── simulator/
│       ├── config.py
│       ├── dataset.py
│       ├── event_factory.py
│       ├── fault_injection.py
│       ├── client.py
│       ├── runner.py
│       └── __main__.py
├── data/
│   └── ai4i2020.csv
├── tests/
├── Dockerfile
└── pyproject.toml
```

## 5.3. Các class/chức năng nên có

### `DatasetReader`

Trách nhiệm:

```text
AI4I CSV -> canonical record
```

Chỉ lấy 6 nhóm feature hợp lệ:

```text
product_type
air_temperature_k
process_temperature_k
rotational_speed_rpm
torque_nm
tool_wear_min
```

Không đưa vào request inference:

```text
Machine failure
TWF
HDF
PWF
OSF
RNF
```

`Machine failure` có thể được giữ riêng trong simulator/test harness để so sánh ground truth, nhưng không gửi như ML feature.

### `EventFactory`

Input:

```text
AI4I row
machine assignment rule
clock
```

Output:

```json
{
  "event_id": "evt-000123",
  "machine_id": "M-03",
  "received_at": "2026-10-12T09:00:01+07:00",
  "product_type": "M",
  "air_temperature_k": 298.1,
  "process_temperature_k": 308.6,
  "rotational_speed_rpm": 1551,
  "torque_nm": 42.8,
  "tool_wear_min": 108
}
```

### `FaultInjector`

Nên hỗ trợ cấu hình:

```text
none
missing_field
invalid_type
out_of_range
duplicate_event
network_timeout
```

Không sửa dataset gốc; chỉ biến đổi event tại runtime.

### `BackendClient` (HTTP Ingestion; async ACK)

Gọi:

```http
POST /api/v1/sensor-readings
```

Cần log `202 ACCEPTED` và **không hiểu nó là inference completed**. Sensor cần local buffer/response retry (same event_id), áp dụng backoff+jitter cho 429/503/504, không retry 400/422/auth errors.

Cần log:

```text
event_id
machine_id
status_code
latency_ms
error
```

## 5.4. Thứ tự dev

```text
B1. Load AI4I thành DataFrame.
B2. Chuẩn hóa tên 6 feature.
B3. Build SensorEvent đúng contract.
B4. Gửi 1 event thủ công sang mock Backend.
B5. Thêm loop replay.
B6. Thêm speed/interval config.
B7. Thêm deterministic seed.
B8. Thêm fault injection.
B9. Thêm metrics/log.
B10. Containerize.
B11. Thêm `ScenarioLoader` cho drift scenario.
B12. Thêm valid-data transforms: gradual shift, sudden shift, categorical mix shift.
```

### Drift Scenario Loader

**[Đề xuất triển khai]** Tách drift scenario khỏi `FaultInjector`, vì drift data vẫn phải **VALID** theo schema/range.

```text
FaultInjector
  -> missing/type/range/duplicate/disconnect
  -> mục tiêu test Data Quality / resilience

DriftScenarioLoader
  -> valid distribution transforms
  -> mục tiêu test Drift Worker
```

Cấu trúc:

```text
sensor-simulator/scenarios/
├── normal.yaml
├── sudden-drift.yaml
├── gradual-drift.yaml
└── categorical-drift.yaml
```

Ví dụ `gradual-drift.yaml`:

```yaml
name: gradual-torque-drift
seed: 42
phases:
  - name: baseline
    events: 300
  - name: gradual-drift
    events: 700
    transforms:
      torque_nm:
        type: linear_shift
        start_delta: 0
        end_delta: 8
      rotational_speed_rpm:
        type: linear_scale
        start_factor: 1.0
        end_factor: 0.93
```

Rules:

- transformed record vẫn phải pass Data Quality Validator;
- không sửa/gửi `Machine failure` như feature;
- scenario có deterministic seed;
- normal scenario dùng làm negative-control cho drift test.

## 5.5. Config gợi ý

```env
BACKEND_BASE_URL=http://backend:8000
SIM_INTERVAL_MS=1000
SIM_SPEED=1.0
SIM_SEED=42
SIM_MACHINE_COUNT=5
SIM_FAULT_MODE=none
SIM_SCENARIO=normal
SIM_SCENARIO_PATH=/app/scenarios
```

## 5.6. Test bắt buộc

- Một valid row tạo đúng payload.
- Feature forbidden không xuất hiện trong request.
- Cùng seed tạo cùng mapping máy/kịch bản.
- Missing field được Backend reject.
- Duplicate `event_id` tạo hành vi idempotent theo contract.
- Backend down không làm simulator crash toàn process nếu policy là retry/continue.
- `normal.yaml` tạo distribution gần reference trong tolerance đã chốt.
- `sudden-drift.yaml`, `gradual-drift.yaml`, `categorical-drift.yaml` vẫn tạo payload VALID nhưng có distribution shift theo fixture.

## 5.7. Definition of Done

```text
[ ] Replay được AI4I
[ ] Có speed control
[ ] Có deterministic seed
[ ] Có ít nhất 4 fault injection scenario
[ ] Không leak label vào ML feature
[ ] Chạy được bằng Docker
[ ] Có log đủ event_id + status
[ ] Có normal/sudden/gradual/categorical drift scenarios
[ ] Drift scenario data vẫn pass validation
```

---

# 6. Module B — Processing Service và Business Backend tách ownership

**Không còn một `backend/` deployment ôm tất cả.** Được phép một monorepo nhưng cần 3 runtime boundaries: `ingestion-api` (edge producer), `processing-worker` (DQ/ML/Risk) và Business Backend gồm `business-consumer` + `business-api` (cùng business codebase). Giữ DRIFT/ML/RAG và các thuật toán cũ ở các mục phía dưới.

## 6.1. Entrypoints triển khai

```text
ingestion/main.py                  # POST /api/v1/sensor-readings -> Kafka
processing/worker_main.py          # consume sensor.readings -> DQ -> ML -> Risk
processing/outbox_relay_main.py    # processing_db outbox -> inference.results/alert.signals
processing/read_api_main.py        # query event statuses/historical reports, limited internal API
business/consumer_main.py          # telemetry, prediction, alert Kafka handlers
business/api_main.py               # REST/SSE, machine/alert actions, simulator facade
sensor/control_api.py              # internal per-machine pause/resume/scenario (demo only)
```

Business Consumer và Business API dùng `business_db` credentials và chung business-owned migrations; Ingestion không có database credential. Processing Worker truy cập độc quyền `processing_db`; Kafka Producer có thể embedded vào HTTP runtime. `Backend` legacy dưới đây ám chỉ Processing cho DQ/ML/Risk và Business cho alerts; không copy shared repository/DB giữa hai service.

## 6.2. Data paths

```text
sensor HTTP -> ingress -> Kafka sensor.readings -> (3 independent groups)
 -> Processing: DQ -> ML Service -> Risk -> processing_db + outbox
    -> Kafka inference.results -> Business prediction handler -> latest projection
    -> Kafka alert.signals -> Business alert handler -> alert lifecycle
 -> Business telemetry handler -> latest telemetry snapshot -> SSE notification
 -> Storage Consumer -> S3 raw archive
Business API <- latest snapshots / alerts business_db -> REST/SSE React
```

Raw errors violating **minimum envelope** get 400/422 and go to **Simulator local rejected log**; stream continues. Deep feature errors get 202, then status INVALID at Processing, full accepted payload archived by Storage; ML not called. Kafka unavailable -> 503/504, stable retry queue. No Kafka ACK => no 202. Existing events never canceled on Pause.

## 6.3. Business DB is NOT telemetry archive

Business Consumer handles 3 topics, and only holds per-machine latest telemetry, latest valid prediction and latest processing status (plus machine/alert/brief data). Use `INSERT ... ON CONFLICT ... WHERE (epoch, sequence) newer` or transaction-level CAS, idempotency inbox/offsets. Business API sends SSE after projector commit via PG LISTEN/NOTIFY or broadcast bus; it does not consume Kafka itself. Multi-replica API must all receive notifications; REST resync after SSE disconnect.

## 6.4. Processing read API

Processing owns event history and DQ/errors; allow Business facade, Drift CronJob query stable Processing API or S3 curated. Do not cross-service SQL JOIN. `POST sensor` is NOT synchronous inference endpoint; status API may be eventually consistent until event materialized. Outbox relay must publish both result and alert intents durably.

# 7. Ingestion API — standalone HTTP gateway

## 7.1. Transport contract

`POST /api/v1/sensor-readings`: authentication, rate/size, parse JSON + basic envelope (`event_id`, `machine_id`, `features` object, source/time syntax), stable `correlation_id`, Kafka producer with `machine_id` key and broker ACK. `202` contains accepted ID/status URL, does not return prediction. Deep Data Quality checks belong to Processing Worker only.

## 7.2. Error semantics và sensor behavior

| Case | HTTP | Sensor action |
|---|---|---|
| Kafka ACK | 202 | Continue regular stream |
| Malformed JSON | 400 | Log rejected record and continue next |
| Missing/bad envelope IDs/object | 422 | Log rejected and continue; no blind retry |
| Invalid features under otherwise valid envelope | 202 | Processing INVALID, raw S3 still stores |
| Kafka unavailable/timeout | 503/504 | Retry buffered event same ID with exponential backoff |
| Rate limit | 429 | Respect Retry-After and bounded retry |
| Auth failure | 401/403 | Flag operator/config issue; no infinite retry |

Invalid envelope not stored in Kafka raw archive. Optional trusted ingress quarantine for malformed events requires its own constraints/security and is NOT part of MVP. Sensor per-machine Pause stops **new** production but keeps durable retries/in-flight accepted processing semantics separately.

## 7.3. Suggested pseudocode

```python
async def ingest(http_request):
    payload = parse_json_or_400(http_request)
    envelope = validate_envelope_or_422(payload)  # not deep feature DQ
    msg = add_ingested_at_correlation(envelope)
    await publish_with_broker_ack('sensor.readings', key=msg['machine_id'], value=msg)
    return accepted_202(event_id=msg['event_id'], status_url=f"/api/v1/sensor-events/{msg['event_id']}/status")
```

Handle Kafka exceptions explicitly with 503/504. Do not promise HTTP 409 conflict without an ingress durable ledger; Processing detects conflicting IDs and quarantines. Producer idempotence != exactly-once end-to-end.

## 7.4. Tests

Ingestion Kafka ACK/error/timeout; feature-invalid-but-envelope-valid; no ML or business DB call; sensor local reject continues next; per-machine sequence ordering; simulator pause is not server/network error.

# 8. Backend submodule B2 — Data Quality Validator

## 8.1. Trách nhiệm

**[BRD bắt buộc]** Chỉ `VALID` mới được gọi ML.

Validation nên tách thành 4 layer:

```text
1. Schema presence
2. Data type
3. Domain/category
4. Range/semantic rules
```

Ví dụ:

```text
product_type in {L,M,H}
rotational_speed_rpm > 0
tool_wear_min >= 0
```

Range cụ thể phải được nhóm chốt từ dataset/data contract; không tự thêm rule gây loại dữ liệu hợp lệ mà không có bằng chứng.

## 8.2. Output nội bộ

```python
ValidationResult(
    status="VALID" | "INVALID",
    errors=[...],
    normalized_features={...}
)
```

`UNKNOWN` nên dành cho tình trạng không thể đánh giá do system/dependency failure, không dùng thay cho invalid input thông thường.

## 8.3. Không làm

- Không silently impute lỗi nghiêm trọng để rồi coi là Normal.
- Không dùng label dataset để validate prediction.
- Không biến validation layer thành model.

## 8.4. Metrics

```text
maintai_invalid_sensor_events_total{reason=...}
maintai_sensor_events_total{status=...}
```

## 8.5. Test

Mỗi rule phải có positive/negative case.

---

# 9. Backend submodule B3 — Inference Client

## 9.1. Trách nhiệm

Processing Worker gọi ML qua client duy nhất:

```http
POST http://ml-inference:8001/api/v1/predict
```

Client chịu trách nhiệm:

- serialize request đúng contract;
- set timeout;
- propagate correlation id;
- giới hạn retry;
- parse/validate response;
- metric latency/error;
- convert dependency error thành explicit internal state.

## 9.2. Không làm

- Không chứa model preprocessing.
- Không tự map score thành severity.
- Không swallow timeout rồi trả `NORMAL`.

## 9.3. Timeout/retry

**[Đề xuất]** Với prototype local, dùng timeout ngắn, ví dụ 1–2 giây; retry tối đa 1 lần cho lỗi network phù hợp. Không retry vô hạn.

Pseudo-flow:

```python
try:
    response = await ml_client.predict(features)
except MLTimeout:
    mark_machine_unknown(reason="ML_UNAVAILABLE")
```

## 9.4. Test

- ML 200 hợp lệ.
- ML response sai schema.
- ML 5xx.
- timeout.
- connection refused.

---

# 10. Backend submodule B4 — Risk Decision Engine

## 10.1. Trách nhiệm

Input:

```text
prediction label
failure_probability/risk_score
model_version
policy config
```

Output:

```text
machine_state
alert_severity
should_alert
policy_version
reason
```

Ví dụ **minh họa**:

```yaml
policy_version: risk-policy-1.0.0
thresholds:
  watch: 0.40
  high: 0.65
  critical: 0.85
alert_on:
  - HIGH
  - CRITICAL
```

Các giá trị trên không phải BRD mandate. Nhóm phải chọn dựa trên validation/false-alarm trade-off.

## 10.2. Lý do tách khỏi ML

```text
ML: P(Failure)=0.82
Risk policy: HIGH
Alert policy: create alert
```

Có thể thay policy mà không retrain model.

## 10.3. Versioning

Mỗi decision phải lưu:

```text
model_version
policy_version
```

## 10.4. Test boundary

Test đúng tại và ngay quanh threshold:

```text
0.6499
0.6500
0.6501
```

Đây là nơi dễ sinh bug off-by-one/comparison.

---

# 11. Business Service — Consumer + API, không tính Prediction

## 11.1. Business Consumer (một Deployment, ba handlers)

- `telemetry_handler`: consumer group `business-live-v1` on `sensor.readings`, materialize **latest raw telemetry snapshot per machine**, no deep Data Quality; `UNVERIFIED` until processing outcome. Không lưu full sensor events/history.
- `prediction_handler`: consumer group `business-prediction-v1` on `inference.results`, materialize latest valid prediction only for result_kind `PREDICTION_COMPLETED`; maintain latest processing status for `DATA_INVALID` / `INFERENCE_UNAVAILABLE`. Do **not** compute prediction/risk.
- `alert_handler`: consumer group `business-alert-v1` on `alert.signals`, inbox + alert dedupe/cooldown + audit/lifecycle. Alert remains OPEN even when later result NORMAL, until technician action.

Each handler commits offset **after** durable DB effects, with idempotency and ordering guard; emit NOTIFY to Business API Pods after transaction commit. One process can multiplex handlers, but this is not three microservices. Tách group IDs để retry/lag các topics độc lập.

## 11.2. Business API (Deployment khác, cùng codebase Business)

REST Machine/Fleet/Alerts/Feedback/Copilot/Monitoring/`/machines/{id}/live` and SSE `/events/stream`; reads `business_db` projections, never reads `processing_db` directly. Simulator Control facade authenticates demo action, forwards pause/resume/scenario to internal Simulator Control API; must not turn pause into physical actuator action. SSE fan-out when API replicas >1 via PG LISTEN/NOTIFY (MVP) or pub/sub; resync snapshot after disconnect. Snapshot DB write is **not** a prerequisite to sending SSE, but supports recovery.

## 11.3. Alert lifecycle

Keep states `OPEN -> ACKNOWLEDGED -> IN_PROGRESS -> RESOLVED` (and allowed ESCALATED/FALSE_ALERT transitions according to System Contracts §10). `PAUSED` Simulator state neither ACKs nor resolves; new low-risk reading also does not resolve. RAG enrichment is optional and non-blocking; technician actions audited.

## 11.4. Tests

Out-of-order prediction R1/R2; invalid features preserve last valid prediction; duplicate raw accepted event does not spam latest projection; two API Pods both SSE notify; alerts deduplicated; stop/restart consumer replay; pause/resume does not alter alert states.

# 12. Backend submodule B6 — Audit Logger

## 12.1. Mục tiêu

Cho phép trace:

```text
SensorEvent
   -> Validation
   -> Prediction
   -> model_version
   -> RiskDecision
   -> policy_version
   -> Alert
   -> Technician Action
   -> RAG Brief
   -> document/version/citation
```

## 12.2. Cách làm

Không cần xây một audit platform phức tạp. MVP có thể dùng:

- DB record có khóa liên kết;
- `alert_events` append-only;
- structured logs;
- `correlation_id` xuyên flow.

## 12.3. Log structure tối thiểu

```json
{
  "timestamp": "...",
  "level": "INFO",
  "service": "backend",
  "event": "alert_created",
  "request_id": "req_...",
  "correlation_id": "cor_...",
  "machine_id": "M-03",
  "event_id": "evt_...",
  "prediction_id": "pred_...",
  "alert_id": "alt_...",
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0"
}
```

Không log secret hoặc raw API key.

---

# 13. Backend submodule B7 — Feedback Manager

## 13.1. Trách nhiệm

Lưu phản hồi thật của technician:

```text
alert verdict
recommendation feedback
note
actor
timestamp
```

Ví dụ:

```json
{
  "technician_verdict": "FALSE_ALERT",
  "recommendation_feedback": "PARTIALLY_USEFUL",
  "note": "Inspected machine; no abnormal condition found"
}
```

## 13.2. Không làm

**[BRD/MVP]** Không tự động retrain model ngay từ feedback.

Feedback là evidence cho evaluation và future retraining pipeline.

---

# 14. Backend submodule B8 — RAG Client

## 14.1. Trách nhiệm

Khi alert đã được tạo, Backend build context và gọi:

```http
POST /api/v1/briefs
```

Context gồm:

```text
alert_id
machine_id
severity
prediction/risk score
sensor context
machine metadata
correlation_id
```

## 14.2. Failure handling

RAG timeout/down:

```text
alert.status      = vẫn giữ nguyên
copilot_status    = UNAVAILABLE
UI                = ALERT + COPILOT UNAVAILABLE
```

Không rollback alert.

## 14.3. Async hay sync?

BRD cho phép post-alert enrichment. Với MVP, có hai cách:

**Cách đơn giản:** Business Consumer tạo alert, commit `business_db`, sau đó Business API/worker gọi RAG ở background task hoặc job. Không chạy RAG trên Ingestion/Processing critical path.

**Cách event-driven:** publish brief work đến một job queue/DB outbox sau alert; Kafka đã có trong nền tảng nhưng không bắt buộc dùng Kafka cho RAG generation ở MVP.

---

# 15. Module C — ML Training Pipeline

**Owner chính:** Người 2.

ML có hai sản phẩm riêng:

```text
A. Offline training/evaluation
B. Online inference service
```

Không đặt notebook training bên trong runtime `/predict`.

## 15.1. Cấu trúc thư mục

```text
ml-inference/
├── training/
│   ├── data.py
│   ├── split.py
│   ├── features.py
│   ├── train_baseline.py
│   ├── train_candidate.py
│   ├── evaluate.py
│   ├── threshold.py
│   └── export_artifact.py
├── service/
│   ├── main.py
│   ├── schemas.py
│   ├── model_loader.py
│   ├── predictor.py
│   ├── health.py
│   └── metrics.py
├── artifacts/
│   └── maintai-ml-1.0.0/
│       ├── model.joblib
│       ├── preprocessor.joblib
│       └── metadata.json
├── notebooks/
│   └── exploration.ipynb
├── tests/
├── Dockerfile
└── pyproject.toml
```

## 15.2. Dataset preparation

**[BRD bắt buộc]** Input model chỉ dùng:

```text
Product quality type
Air temperature
Process temperature
Rotational speed
Torque
Tool wear
```

Target:

```text
Machine failure
```

Không dùng failure-type labels làm input.

## 15.3. Split

Freeze cùng protocol cho mọi model:

```text
train
validation
test
```

Không nhìn test để chọn hyperparameter/threshold.

Nếu random split được sử dụng, lưu seed và indices để reproducible.

## 15.4. Preprocessing

Đưa toàn preprocessing vào một pipeline artifact:

```text
category encoding
numeric processing
model
```

Mục tiêu:

```textđâu
training preprocessing == serving preprocessing
```

Tránh viết preprocessing một kiểu ở notebook và một kiểu khác trong FastAPI.

## 15.5. Baseline + model 2

**[BRD bắt buộc]** Có ít nhất:

```text
baseline: Logistic Regression hoặc Decision Tree
candidate: một model khác
rule-based baseline: một rule cảnh báo đơn giản
```

Model candidate có thể là Random Forest/XGBoost/LightGBM tùy dependency và khả năng giải thích.

## 15.6. Class imbalance

Không tối ưu Accuracy đơn thuần.

Báo cáo:

```text
Recall_failure
Precision_failure
F1
PR-AUC
False Negative Rate
Confusion Matrix
```

Có thể thử:

- class weight;
- threshold tuning;
- sampling trên train בלבד nếu thật sự cần.

Mọi xử lý imbalance phải chỉ fit trên training data.

## 15.7. Threshold selection

Threshold chọn trên validation set, không phải test.

Quy trình:

```text
train model
 -> predict probability validation
 -> sweep threshold
 -> chọn trade-off recall/precision/false alarm
 -> freeze threshold evidence
 -> evaluate final trên test
```

Mục tiêu BRD:

```text
Recall_failure >= 0.85
Precision_failure >= 0.50
```

Nếu dataset/model không đạt, báo trung thực; không thay đổi test để làm đẹp số.

## 15.8. Artifact export

`metadata.json` nên chứa:

```json
{
  "model_name": "maintai-failure-classifier",
  "model_version": "1.0.0",
  "dataset": "AI4I-2020",
  "feature_schema_version": "1.0.0",
  "training_code_version": "git-sha-or-tag",
  "model_threshold": 0.63,
  "metrics": {
    "recall_failure": 0.88,
    "precision_failure": 0.56
  }
}
```

Các số là ví dụ minh họa.

Ngoài model/preprocessor metadata, export thêm reference profile phục vụ Drift Monitoring:

```text
artifacts/<model_version>/
├── model.joblib
├── preprocessor.joblib
├── metadata.json
└── drift_reference.json
```

`drift_reference.json` phải được build từ **training/reference split đã chốt**, không từ test set. Nó nên giữ histogram/bin/category probabilities ổn định để Drift Worker so sánh current window mà không regenerate baseline từ runtime data.

Ví dụ:

```json
{
  "dataset_version": "ai4i-v1",
  "model_version": "maintai-ml-1.0.0",
  "sample_count": 7000,
  "features": {
    "torque_nm": {
      "type": "numeric",
      "metric": "PSI",
      "bins": [0, 20, 30, 40, 50, 60, 100],
      "probabilities": [0.01, 0.07, 0.21, 0.38, 0.22, 0.11]
    },
    "product_type": {
      "type": "categorical",
      "metric": "PSI",
      "categories": ["L", "M", "H"],
      "probabilities": [0.60, 0.30, 0.10]
    }
  },
  "prediction_reference": {
    "failure_rate": 0.034
  }
}
```

## 15.9. Test ML offline

- feature leak test;
- reproducible split test;
- preprocessing schema test;
- artifact load test;
- threshold logic test;
- metric report generated;
- drift reference profile reproducible và khớp feature/model version;
- forbidden fields/target không xuất hiện trong drift reference features.

---

# 16. Module D — ML Inference Service

**Owner chính:** Người 2.

## 16.1. Runtime responsibility

```text
PredictionRequest
   -> schema validation
   -> preprocessing artifact
   -> model artifact
   -> predict_proba
   -> PredictionResponse
```

Không tạo alert.

## 16.2. API

```text
POST /api/v1/predict
GET  /health/live
GET  /health/ready
GET  /metrics
```

## 16.3. Startup

Service startup phải:

```text
1. đọc MODEL_DIR / MODEL_VERSION
2. load metadata
3. load preprocessor
4. load model
5. validate artifact compatibility
6. set readiness=true
```

Nếu artifact lỗi:

```text
liveness = alive
readiness = false
/predict = service unavailable
```

## 16.4. `/predict`

Input đúng 6 feature contract.

Response tối thiểu:

```json
{
  "prediction": "FAILURE",
  "failure_probability": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "inference_ms": 18
}
```

## 16.5. Metrics

```text
ml_inference_requests_total{status=...}
ml_inference_duration_seconds
ml_prediction_total{label=...}
ml_model_info{version=...}  # careful: static low-cardinality label
```

Không dùng `machine_id` làm Prometheus label vì cardinality có thể tăng mạnh.

## 16.6. Test

- startup artifact OK/broken;
- valid predict;
- schema invalid;
- deterministic same input result;
- contract response;
- latency smoke test.

---

# 17. Module E — RAG Knowledge Ingestion

**Owner chính:** Người 3.

RAG cũng có hai pipeline:

```text
A. Offline knowledge ingestion
B. Online brief generation
```

## 17.1. Cấu trúc thư mục

```text
rag-service/
├── ingestion/
│   ├── parser.py
│   ├── chunker.py
│   ├── metadata.py
│   ├── embedder.py
│   └── ingest.py
├── service/
│   ├── main.py
│   ├── schemas.py
│   ├── query_builder.py
│   ├── metadata_filter.py
│   ├── retriever.py
│   ├── prompt_builder.py
│   ├── generator.py
│   ├── citation_validator.py
│   └── guardrails.py
├── evaluation/
│   ├── retrieval_cases.jsonl
│   └── evaluate_retrieval.py
├── tests/
├── Dockerfile
└── pyproject.toml
```

## 17.2. Document model

Mỗi manual phải có:

```text
document_id
version
title
effective metadata
source/path
```

Mỗi chunk phải có:

```text
chunk_id
document_id
document_version
section
page
text
safety_notes nếu có
embedding
```

## 17.3. Chunking

Không cắt tài liệu theo số ký tự mù quáng nếu làm mất section/citation.

Ưu tiên:

```text
heading/section
 -> paragraph groups
 -> size limit + small overlap nếu cần
```

Mỗi chunk phải biết nguồn để citation trả về được.

## 17.4. Ingestion flow

```text
manual file
   -> parse text
   -> normalize
   -> detect section/page metadata
   -> chunk
   -> embedding
   -> insert documents/document_chunks
   -> verify count
```

## 17.5. Re-ingestion/versioning

Không overwrite chunk v1 bằng v2 mà mất lịch sử.

```text
document_id = manual-A
version     = 1.0
version     = 2.0
```

Metadata Filter chọn version phù hợp/current theo product rule.

---

# 18. Module F — Online RAG/LLM Service

## 18.1. Pipeline

```text
MaintenanceBriefRequest
    |
    v
Query Builder
    |
    v
Metadata Filter
    |
    v
Vector Retriever
    |
    +--- no evidence ---> INSUFFICIENT_EVIDENCE
    |
    v
Prompt Builder
    |
    v
LLM Generator
    |
    v
Citation Validator
    |
    +--- failed grounding ---> downgrade/reject action claims
    |
    v
MaintenanceBriefResponse
```

## 18.2. Query Builder

Không nhất thiết dùng LLM để build query ở MVP. Có thể deterministic:

```text
severity + abnormal sensor summary + maintenance intent
```

Mục tiêu là query reproducible và dễ test.

## 18.3. Metadata Filter

Filter trước vector search khi có metadata đủ tin cậy:

```text
document version
machine/document type
language
section category
```

Không lọc quá chặt dẫn tới zero result một cách vô tình.

## 18.4. Retriever

Return:

```text
chunk_id
text
score
document_id
version
section
page
```

Log top-k IDs và score để evaluation/troubleshooting.

## 18.5. Prompt Builder

Prompt phải nói rõ:

- đây là decision-support;
- không khẳng định root cause chắc chắn chỉ từ score;
- chỉ dùng retrieved evidence cho hành động kỹ thuật;
- action/checklist phải có citation;
- thiếu evidence thì nói thiếu evidence;
- không tự tạo thông số sửa chữa ngoài nguồn.

## 18.6. Citation Validator

Validator phải kiểm tra citation output có trỏ vào chunk/document thực sự được retrieve.

MVP có thể dùng deterministic validation:

```text
citation.document_id in retrieved_documents
citation.chunk_id in retrieved_chunks
citation.section/page matches stored metadata
```

Không cần một LLM khác chỉ để kiểm tra citation nếu logic deterministic đủ.

## 18.7. Insufficient evidence

Khi retrieval không đủ:

```json
{
  "grounded_status": "INSUFFICIENT_EVIDENCE",
  "brief": "Không đủ căn cứ từ tài liệu hiện có để đưa ra checklist chi tiết.",
  "checklist": [],
  "citations": []
}
```

Không fallback bằng cách yêu cầu LLM trả lời từ trí nhớ chung.

## 18.8. Metrics

```text
rag_requests_total{status=...}
rag_request_duration_seconds
rag_retrieval_duration_seconds
rag_retrieved_chunks_count
rag_insufficient_evidence_total
```

## 18.9. Evaluation

BRD yêu cầu retrieval quality bằng `Recall@k`.

Tạo gold set nhỏ:

```text
query/context
expected document/chunk
```

Sau đó tính:

```text
Recall@1
Recall@3
Recall@5
```

Grounding/citation test cần kiểm tra action claims có nguồn.

---

# 19. Module G — Database-per-Service + S3 Object Storage

**Quyết định:** 1 PostgreSQL instance chứa 5 logical databases. **`business_db` không chứa raw sensor history**, chỉ read projections snapshot phục vụ UI và business tables. Không có cross-DB foreign keys/SQL JOIN, mỗi service có credential và Alembic riêng. `mlflow_db` do MLflow server quản lý schema. `knowledge_db` enable extension pgvector.

| DB / object storage | Owner | Data |
|---|---|---|
| `processing_db` | Processing | sensor_events, predictions, risk_decisions, processing_outbox |
| `business_db` | Business | machines, alerts, alert_events, feedback, rag_briefs, business_inbox, **latest_telemetry/latest_valid_prediction/latest_processing (read models)** |
| `drift_db` | Drift CronJob | drift_reports, drift_checkpoints |
| `mlflow_db` | MLflow Server | experiments, runs, model registry metadata |
| `knowledge_db` | RAG | documents, document_chunks, embeddings |
| S3 `raw-sensor/` | Storage Consumer | immutable raw accepted sensor payloads, batch+manifest |
| S3 `curated-datasets/` | Data/Training | versioned curated datasets, lineage |
| S3 `mlflow-artifacts/` | MLflow | model files, preprocessor, reference profiles |
| S3 `drift-reports/` | Drift | report objects/exports |
| S3 `rag-documents/` | RAG | PDF/manual sources |

### 19.1. Migrations và quyền

- DB migrations Processing chỉ sửa Processing tables; Business chỉ Business; Drift chỉ Drift; RAG chỉ Knowledge. MLflow sử dụng migration của MLflow.
- DB roles `processing_rw`, `business_rw`, `drift_rw`, `rag_rw`; mỗi role chỉ grants DB của mình; `ingestion_api` không có PostgreSQL credentials.
- PostgreSQL logical DB không scale độc lập về tài nguyên khi cùng instance; scale instance, pool connections, tối ưu queries/partition trước khi cân nhắc tách sang PostgreSQL instances.

### 19.2. Raw Archive đảm bảo bền vững

- Storage consumer gom batch theo partition, ghi `JSONL.gz`/Parquet. Để giữ raw evidence, JSONL.gz nên ghi Kafka accepted envelope + topic/partition/offset, không xóa invalid records.
- Deterministic object keys ví dụ `raw-sensor/topic=sensor.readings/partition=0/year=2026/month=10/day=17/offset-00001-01000.jsonl.gz` cùng manifest/checksum; S3 write thành công rồi mới commit offset.
- At-least-once có thể tái ghi và trùng logical events; dedupe theo manifest/event_id khi curated data. Retention Kafka không được ngắn hơn worst-case storage consumer outage + recover + safety margin.
- ML Training đọc curated dataset đã kiểm tra và version hóa, không tự retrain trên mọi raw object xuất hiện.

---

# 20. Module H — React Frontend

> **Quyết định v1.3:** Frontend triển khai **phương án C: Live Telemetry + Latest Valid Prediction**. Các ví dụ 20.1–20.9 bên dưới được kế thừa để hiểu cấu trúc module, nhưng **§40 có quyền ưu tiên** nếu ví dụ 1.2 nói SSE “optional”, polling-only hoặc trộn latest reading với latest ML output. Màn hình chuẩn ở Functional Overview §19.


**Owner chính:** Người1 (Main Frontend).

Frontend không tự tính risk hoặc alert. Nó render state từ Backend.

## 20.1. Cấu trúc thư mục

```text
frontend/
├── src/
│   ├── api/
│   │   ├── client.ts
│   │   ├── machines.ts
│   │   └── alerts.ts
│   ├── pages/
│   │   ├── FleetDashboard.tsx
│   │   ├── MachineDetail.tsx
│   │   ├── AlertDetail.tsx
│   │   └── MonitoringView.tsx
│   ├── components/
│   │   ├── MachineStatusBadge.tsx
│   │   ├── RiskScore.tsx
│   │   ├── SensorTable.tsx
│   │   ├── AlertTimeline.tsx
│   │   ├── MaintenanceBrief.tsx
│   │   └── CitationList.tsx
│   ├── types/
│   ├── hooks/
│   └── app/
├── tests/
└── Dockerfile
```

## 20.2. Fleet Dashboard

Hiển thị tối thiểu:

```text
machine
latest operational state
risk/severity
last seen
unresolved alerts
service status summary
```

UI phải phân biệt rõ:

```text
NORMAL
WATCH
HIGH
CRITICAL
INVALID
UNKNOWN
```

`UNKNOWN` không được tô/ghi giống `NORMAL`.

## 20.3. Machine Detail

Hiển thị:

```text
latest telemetry
validation status
latest prediction
failure probability/risk score
model version
recent event/prediction timeline
related alerts
```

## 20.4. Alert Detail

Đây là màn hình nghiệp vụ quan trọng nhất:

```text
alert id
machine
severity/status
prediction + risk score
sensor snapshot
model_version
policy_version
alert lifecycle timeline
Maintenance Brief
Checklist
Citations
Copilot state
```

Actions:

```text
ACK
false alert
resolve
escalate nếu endpoint có
note/feedback
accept/reject recommendation
```

## 20.5. Monitoring View

Không cần thay Grafana. Có thể hiển thị summary và link Grafana:

```text
Backend healthy?
ML healthy?
RAG healthy?
request error rate
p95 latency
latest metrics snapshot
```

## 20.6. API state management

Không hard-code demo data sau khi contract đã có.

Nên tách:

```text
API DTO
 -> adapter nếu cần
 -> UI model
```

## 20.7. Realtime

MVP có thể dùng polling 2–5 giây.

Nếu dùng SSE/WebSocket:

- event realtime chỉ để cập nhật nhanh;
- khi reconnect phải fetch latest state từ REST;
- không xem socket event là source of truth duy nhất.

## 20.8. Error states

Frontend phải render explicit:

```text
DATA ERROR
ML UNAVAILABLE
COPILOT UNAVAILABLE
INSUFFICIENT EVIDENCE
NETWORK ERROR
```

Không thay bằng spinner vô hạn.

## 20.9. Test

- component state badges;
- alert lifecycle actions;
- API error handling;
- `UNKNOWN != NORMAL`;
- RAG unavailable alert vẫn hiển thị;
- refresh/reconnect recover được state.

---

# 21. Module I — Observability

**Owner chính:** Người4.

## 21.1. Mục tiêu

Phải trả lời được:

```text
Service có sống không?
Request có lỗi không?
Inference chậm không?
Alert có tăng bất thường không?
Invalid data bao nhiêu?
RAG có retrieve được không?
Runtime data có drift khỏi training reference không?
Drift Worker lần cuối chạy thành công khi nào?
Event nào dẫn tới alert nào?
```

## 21.2. Health

Mọi HTTP service:

```text
GET /health/live
GET /health/ready
```

Ví dụ:

```text
Backend ready:
  database reachable
  config loaded

ML ready:
  model artifact loaded

RAG ready:
  database/pgvector reachable
  embedding/LLM dependency state phù hợp policy
```

Không đưa dependency bên ngoài vào liveness khiến container restart loop không cần thiết.

## 21.3. Prometheus

### Backend

```text
http_requests_total
http_request_duration_seconds
sensor_events_total
invalid_sensor_events_total
alerts_total
alert_lifecycle_actions_total
ml_client_errors_total
rag_client_errors_total
```

### ML

```text
ml_inference_requests_total
ml_inference_duration_seconds
ml_predictions_total
```

### RAG

```text
rag_requests_total
rag_request_duration_seconds
rag_retrieval_duration_seconds
rag_insufficient_evidence_total
```

### Drift Worker

```text
maintai_drift_feature_score{feature,metric}
maintai_drift_feature_status{feature,status}
maintai_drift_features_drifted
maintai_drift_overall_status{status}
maintai_drift_window_samples
maintai_drift_reports_total{status}
maintai_drift_worker_last_success_timestamp_seconds
maintai_prediction_failure_ratio
```

Không gắn `report_id`, `event_id`, `machine_id` vào label.

## 21.4. Label cardinality

Không dùng:

```text
event_id
request_id
correlation_id
machine_id với số lượng rất lớn
```

làm Prometheus label.

Những ID này thuộc logs/traces/DB, không thuộc metrics labels.

## 21.5. Grafana dashboards

### Dashboard 1 — System Overview

```text
request rate
error rate
p50/p95 latency
service health
```

### Dashboard 2 — ML/Alert

```text
prediction distribution
failure prediction rate
alert rate by severity
invalid sensor rate
ML inference p95
```

### Dashboard 3 — RAG

```text
RAG latency
retrieval latency
insufficient evidence rate
error rate
```

### Dashboard 4 — Drift Monitoring

```text
overall drift status
last successful worker run
model/reference version
drifted features / total features
PSI/selected score by feature
prediction failure ratio reference vs current
window sample count
worker error/stale state
```

Prometheus alert rules tối thiểu:

```text
DataDriftWarning
DataDriftCritical
DriftWorkerStale
DriftWorkerError
```

Nên dùng `for:` hoặc yêu cầu nhiều run liên tiếp để tránh noise từ một window ngắn.

---

# 21A. Module I2 — Drift Monitoring Worker

**Owner Drift module/runtime/scheduling/observability:** Người4 — MLOps/Integration.

**Phối hợp model/reference và thuật toán:** Người2 — ML. **Phối hợp Processing API:** Người1.
`drift_db` do Người4 quản lý migrations; Main UI Monitoring do Người1 triển khai.

> [v1.2] Drift Worker **không** connect trực tiếp `processing_db` và **chỉ** lưu state/report vào `drift_db`; DataAccessAdapter dùng Processing read API hoặc curated S3. Các ví dụ "runtime PostgreSQL" kế thừa phải được hiểu theo adapter này.

## 21A.1. Mục tiêu và ranh giới

Drift Worker trả lời câu hỏi:

> Distribution runtime hiện tại có còn giống reference distribution của model đang deploy hay không?

Nó **không** trả lời record hiện tại có hợp lệ không (Data Quality Validator), không dự đoán machine failure (ML Service), và không quyết định maintenance alert (Risk Engine/Alert Manager).

```text
Data Quality Validator = record-level validity
ML Inference           = record-level failure probability
Risk Engine            = product severity/policy decision
Drift Worker           = window-level distribution monitoring
```

## 21A.2. Loại drift trong MVP

```text
Data Drift        = bắt buộc nên có
Prediction Drift  = nên có
Performance Drift = extension khi có delayed ground truth đáng tin cậy
```

Không tự động retrain model từ drift/feedback trong MVP.

## 21A.3. Cấu trúc code

```text
drift-worker/
├── app/
│   ├── main.py
│   ├── config.py
│   ├── detectors/
│   │   ├── numeric.py
│   │   ├── categorical.py
│   │   ├── prediction.py
│   │   └── aggregate.py
│   ├── services/
│   │   ├── drift_service.py
│   │   ├── baseline_loader.py
│   │   └── report_service.py
│   ├── repositories/
│   │   ├── sensor_repository.py
│   │   ├── prediction_repository.py
│   │   ├── drift_repository.py
│   │   └── checkpoint_repository.py
│   ├── schemas/
│   │   └── drift_report.py
│   ├── metrics.py
│   └── logging.py
├── tests/
├── Dockerfile
└── README.md
```

## 21A.4. Reference profile

Load `drift_reference.json` có cùng `model_version` với model deployment. Nếu mismatch/missing:

```text
overall_status = ERROR
reason = DRIFT_REFERENCE_NOT_FOUND
```

Không fallback sang model/version gần nhất.

## 21A.5. Runtime window

Data access qua Processing read API/curated S3, **không direct SQL** `processing_db`; contract cursor/snapshot và model_version phải ổn định. Chỉ lấy các event:

```text
validation_status = VALID
```

Config gợi ý:

```yaml
drift:
  minimum_samples: 200
  window_size: 500
```

Nếu không đủ sample:

```text
INSUFFICIENT_DATA
```

không được báo WARNING/CRITICAL.

## 21A.6. Feature monitor

AI4I features:

```text
product_type
air_temperature_k
process_temperature_k
rotational_speed_rpm
torque_nm
tool_wear_min
```

Không monitor như ML feature:

```text
event_id
machine_id
received_at
target/fault labels
```

## 21A.7. Detector

MVP đơn giản:

```text
Numeric     -> PSI primary, KS optional diagnostic
Categorical -> PSI hoặc Jensen-Shannon
Prediction  -> failure-rate/distribution shift
```

Ví dụ policy:

```yaml
psi:
  warning: 0.10
  critical: 0.20
aggregation:
  critical_if_critical_features_gte: 2
  warning_if_warning_features_gte: 1
```

Các threshold này là initial project policy; lưu cùng report/config version để audit được.

## 21A.8. Execution flow

```text
START
 -> load checkpoint
 -> resolve deployed model_version
 -> load matching reference profile
 -> fetch current VALID window via Processing read API/curated S3
 -> minimum sample check
 -> compute feature drift
 -> compute prediction drift
 -> aggregate DriftStatus
 -> persist drift_report
 -> push latest gauges to Pushgateway
 -> update checkpoint
 -> EXIT
```

Worker nên one-shot, không cần REST server chạy 24/7.

### Batch metrics publication

Do worker exit sau khi chạy, Prometheus **không scrape trực tiếp worker**. Pattern đề xuất:

```text
Drift Worker --push gauges--> Pushgateway <--scrape-- Prometheus -> Grafana/Alert Rules
```

Nếu không muốn dùng Pushgateway, thay bằng một metrics adapter long-lived đọc latest `drift_reports`; không để design ở trạng thái “one-shot process nhưng vẫn expose /metrics để Prometheus scrape”.

Grouping key phải ổn định, ví dụ `job=maintai-drift-worker`; không dùng `report_id`, `event_id`, `machine_id` làm grouping key/label.

## 21A.9. Persistence

Tối thiểu thêm hai bảng:

```text
drift_reports
drift_checkpoints
```

`drift_reports.report_json` giữ feature-level details để tránh tạo quá nhiều bảng ở MVP.

## 21A.10. Drift Report output

```json
{
  "report_id": "drift-20261012-001",
  "computed_at": "2026-10-12T15:00:00Z",
  "detector_version": "drift-worker-1.0.0",
  "reference": {
    "dataset_version": "ai4i-v1",
    "model_version": "maintai-ml-1.0.0"
  },
  "current_window": {
    "start_at": "2026-10-12T13:00:00Z",
    "end_at": "2026-10-12T15:00:00Z",
    "sample_count": 500
  },
  "overall_status": "CRITICAL",
  "summary": {
    "total_features": 6,
    "warning_features": 1,
    "critical_features": 2
  },
  "features": [
    {
      "feature": "torque_nm",
      "metric": "PSI",
      "score": 0.27,
      "status": "CRITICAL"
    }
  ],
  "prediction_drift": {
    "reference_failure_rate": 0.034,
    "current_failure_rate": 0.112
  }
}
```

## 21A.11. Failure handling

| Failure | Worker result | Online inference/alert |
|---|---|---|
| Reference missing/mismatch | ERROR | Không ảnh hưởng |
| Insufficient runtime samples | INSUFFICIENT_DATA | Không ảnh hưởng |
| Processing read API/S3 input failure | ERROR + metric/log | Không ảnh hưởng |
| Persist report failure | ERROR + retry policy | Không ảnh hưởng |
| Worker không chạy đúng lịch | DriftWorkerStale platform alert | Không ảnh hưởng |
| Drift CRITICAL | Monitoring alert | Không tự đổi machine/alert severity |

## 21A.12. Test bắt buộc

```text
no drift             -> STABLE
sudden drift         -> drift detected
gradual drift        -> WARNING/CRITICAL theo fixture
categorical drift    -> detected
insufficient samples -> INSUFFICIENT_DATA
reference mismatch   -> ERROR
worker DB failure    -> online path vẫn hoạt động
worker retry         -> không tạo uncontrolled duplicate reports
```

## 21A.13. Definition of Done

```text
[ ] Training artifact có drift_reference.json
[ ] Current window chỉ dùng VALID runtime events
[ ] Numeric + categorical drift chạy được
[ ] Prediction drift signal có report
[ ] Report lưu PostgreSQL
[ ] Checkpoint lưu được
[ ] Prometheus drift metrics có dữ liệu
[ ] Grafana drift dashboard có panel chính
[ ] Warning/Critical/Stale alert rule tồn tại
[ ] Normal + sudden + gradual + categorical scenarios pass
[ ] Drift Worker lỗi không phá online alert path
```

---

# 22. Module J — Docker Compose / Kubernetes Scale-Ready

## 22.1. Local services tối thiểu

```text
postgres, minio, kafka, topic-init, mlflow, ml-service, rag-service,
ingestion-api, processing-worker, processing-read-api, outbox-relay,
storage-consumer, business-consumer, business-api, frontend,
prometheus, pushgateway, grafana; drift-worker one-shot/profile
```

Có thể dùng cùng Docker image `maintai-backend` cho các entrypoints Python với startup commands khác nhau. Trong Kubernetes **tách Deployment** theo Ingestion API, Processing Worker, Storage Consumer, Business API, Business Consumer, ML, RAG; Drift là `CronJob`. Chưa cần HPA/KEDA ở MVP. Kafka single broker + MinIO single instance dành cho demo không phải HA production.

## 22.2. Configuration (.env.example không chứa secrets)

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SENSOR_RAW_TOPIC=sensor.readings
KAFKA_ALERT_SIGNALS_TOPIC=alert.signals
KAFKA_PROCESSING_GROUP=processing-v1
KAFKA_STORAGE_GROUP=raw-archive-v1
KAFKA_BUSINESS_GROUP=business-alerts-v1
KAFKA_PRODUCER_ACKS=all
KAFKA_CONSUMER_AUTO_COMMIT=false
PROCESSING_DATABASE_URL=postgresql+psycopg://processing_rw:<secret>@postgres:5432/processing_db
BUSINESS_DATABASE_URL=postgresql+psycopg://business_rw:<secret>@postgres:5432/business_db
DRIFT_DATABASE_URL=postgresql+psycopg://drift_rw:<secret>@postgres:5432/drift_db
KNOWLEDGE_DATABASE_URL=postgresql+psycopg://rag_rw:<secret>@postgres:5432/knowledge_db
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_BACKEND_STORE_URI=postgresql+psycopg://mlflow_rw:<secret>@postgres:5432/mlflow_db
S3_ENDPOINT_URL=http://minio:9000
S3_RAW_BUCKET=maintai-data
S3_RAW_PREFIX=raw-sensor/
PROCESSING_READ_BASE_URL=http://processing-read-api:8004
BUSINESS_API_BASE_URL=http://business-api:8003
ML_BASE_URL=http://ml-service:8001
RAG_BASE_URL=http://rag-service:8002
PUSHGATEWAY_URL=http://pushgateway:9091
```

Values `<secret>` là placeholders, không phải working `.env`. Secret thật qua Docker/K8s secrets, TLS/SASL/ACL Kafka phù hợp môi trường. Tách Kafka advertised listeners container vs host.

## 22.3. Readiness / startup

Không chỉ tin `depends_on`: từng process retry/bailout rõ ràng. Ingestion ready khi Kafka producer route có thể publish; Processing cần Kafka/group/DB và ML dependency theo policy; Storage cần Kafka+S3; Business Consumer cần Kafka+business_db; Business API cần business_db. Không đưa Kafka/S3/ML liveness check kiểu restart-loop.

## 22.4. Scale

- Ingestion replicas theo HTTP RPS/p95 và producer errors.
- Processing replicas active tối đa theo partitions và ML/DB capacity; consumer lag không thể hết nếu downstream là bottleneck.
- Storage replicas theo partitions/S3 write throughput và object count.
- Business Consumer theo partitions, Business API theo REST/SSE. Một PostgreSQL instance vẫn là tài nguyên chung.
- Observe backpressure, Kafka disk/retention, oldest consumer lag trước khi bật HPA/KEDA.

## 22.5. One-shot Drift CronJob

```bash
docker compose run --rm drift-worker
```

Drift Worker chỉ có `DRIFT_DATABASE_URL`, `PROCESSING_READ_BASE_URL`, `S3_ENDPOINT_URL`, reference profile và Pushgateway config; **không** có processing_db write credential. Kubernetes CronJob optional, lịch theo config.

---

# 23. Module K — CI/CD và integration pipeline

**Owner chính:** Người4; mỗi owner chịu trách nhiệm test của module mình.

## 23.1. CI tối thiểu

```text
Pull Request
   |
   +--> lint/format
   +--> unit tests
   +--> contract tests
   +--> build container
   +--> basic security/dependency scan
   +--> integration smoke test
```

Khi main/tag:

```text
build images
 -> tag version/git sha
 -> run full integration/E2E
 -> publish artifact/image nếu có registry
```

## 23.2. Contract tests phải chạy sớm

Không chờ service thật hoàn thiện.

Ví dụ:

```text
Backend test dùng mock ML response
ML test validate request/response JSON schema
Frontend test dùng fixture đúng DTO
RAG test dùng alert context fixture
Drift Worker test dùng drift_reference + runtime-window fixtures
```

## 23.3. Migrations trong CI

Integration job:

```text
start empty PostgreSQL
 -> alembic upgrade head
 -> seed minimum data
 -> start services
 -> run test
```

Drift CI riêng hoặc stage trong full integration:

```text
lint drift-worker
 -> unit test detectors
 -> validate drift-report contract
 -> build image
 -> integration test PostgreSQL
 -> replay baseline + drift fixture
 -> run worker
 -> assert expected DriftStatus
```

---

# 24. Test strategy toàn hệ thống

## 24.1. Unit tests

| Module | Test chính |
|---|---|
| Simulator | event factory, fault injection |
| Backend | validator, risk thresholds, state transitions, dedupe |
| ML | preprocessing, artifact loading, prediction contract |
| RAG | chunking, metadata filter, citation validator |
| Drift Worker | numeric/categorical detector, aggregation, minimum-sample, checkpoint |
| Frontend | states/actions/rendering |

## 24.2. Contract tests

```text
Simulator -> Backend SensorEvent
Backend -> ML PredictionRequest/Response
Backend -> RAG MaintenanceBriefRequest/Response
Backend -> Frontend DTO
All services -> Health/Metrics
ML artifact -> Drift Reference
Runtime DB -> Drift Worker -> Drift Report
```

## 24.3. Integration tests

```text
Simulator -> Backend -> ML -> Prediction -> DB
Prediction -> Risk -> Alert -> DB
Alert -> RAG -> citation
Frontend -> Backend lifecycle actions
Simulator drift scenario -> DB -> Drift Worker -> drift_report -> metrics
```

## 24.4. Failure/resilience tests

Bắt buộc có:

```text
malformed sensor event
missing field
ML timeout
ML response invalid
RAG down
RAG no evidence
duplicate high-risk event
DB reconnect/error scenario phù hợp scope
frontend refresh/reconnect
drift reference missing/mismatch
drift insufficient samples
drift worker DB failure/stale run
```

## 24.5. E2E

Kịch bản:

```text
1. Stack healthy.
2. Simulator gửi normal event.
3. Dashboard update, không alert.
4. Simulator gửi failure-like valid event.
5. ML score cao.
6. Risk Engine -> HIGH/CRITICAL.
7. Alert hiển thị ngay.
8. RAG brief xuất hiện sau.
9. Technician ACK.
10. Technician feedback.
11. Resolve/escalate.
12. Trace được toàn chain.
13. Replay normal reference-like window, chạy Drift Worker -> STABLE.
14. Replay gradual/sudden drift window, chạy Drift Worker -> WARNING/CRITICAL theo policy.
15. Grafana/metrics phản ánh drift; maintenance alert path vẫn độc lập.
```

---

# 25. Failure handling matrix khi code (legacy cases; v1.2 transport overrides)

> Các dòng Backend/HTTP trong bảng cũ mô tả **outcome nghiệp vụ bất đồng bộ** sau khi Kafka ACK, không phải response trực tiếp từ `POST sensor-readings`. Xem §37 và System Contracts §31 để test event-driven.

| Failure | Backend behavior | UI | Có gọi tiếp? |
|---|---|---|---|
| Invalid sensor | persist validation error | INVALID/DATA ERROR | Không gọi ML |
| ML timeout | persist dependency failure, state UNKNOWN | ML UNAVAILABLE | Không tạo Normal giả |
| ML invalid response | UNKNOWN + log/metric | ML ERROR | Không quyết định alert từ response lỗi |
| High risk + RAG down | tạo alert bình thường | ALERT + COPILOT UNAVAILABLE | Alert path hoàn tất |
| Retrieval no evidence | lưu brief insufficient | INSUFFICIENT EVIDENCE | Không bịa checklist |
| Duplicate high-risk | update/dedupe alert | một incident/alert hợp lý | Không spam alert |
| Frontend reconnect | REST fetch latest state | state recoverable | Không phụ thuộc socket history |

---

# 26. Sequence triển khai thực tế cho nhóm 4 người

Phân công này áp dụng lựa chọn hiện tại của nhóm; workstreams không phải mỗi
workstream là một thành viên hoặc một Deployment.

## Người 1 — Business, Main Frontend, Processing và Ingestion

Sở hữu `backend/`, `frontend/`, `ingestion-api/`, toàn bộ `processing/` gồm
Worker/Read API/Outbox Relay/shared/migrations. Triển khai contracts, DQ/ML client/
risk, transactional outbox/inbox, projections/alerts/audit/feedback, RBAC, REST/SSE,
history/archive/export, brief jobs và drift catalog. Dùng fixtures khi ML/RAG
chưa sẵn sàng; không chờ provider để làm module và kiểm thử consumer.

## Người 2 — ML

Sở hữu `ml-service/`: EDA, feature allowlist, reproducible split/preprocessing,
baseline/candidate, metrics và threshold bằng validation set, training/inference
hai image, artifact/metadata export, MLflow run lineage, startup verification.
Xuất drift reference đúng model/dataset version; phối hợp Người4 kiểm chứng
detector/scenarios, không tạo reference từ runtime hoặc test set.

## Người 3 — RAG/LLM

Sở hữu `rag-service/` và migrations `knowledge_db`: manuals/parsing/chunking/
embedding/retrieval, gold cases/Recall@k, prompt/LLM/citations, insufficient-evidence
và validation-failed outcomes, document/index/prompt lineage. Tích hợp Business
brief jobs với Người1; RAG down không chặn alert.

## Người 4 — Platform/MLOps, Sensor, Simulator UI, Drift và Storage

Sở hữu hai repo GitOps/hạ tầng cùng `sensor/`, Simulator UI riêng,
`drift-worker/`, `storage-consumer/`. Cấu hình Kafka/topics/ACL/retention,
PostgreSQL/S3/MLflow/OIDC, training orchestration, CI/CD/monitoring và releases.
Triển khai Sensor ACK202/durable sequence/spool/control; raw accepted/rejected
archive; Drift API/S3 adapters/report publication/checkpoint. Người2 cung cấp
model/reference và training code; Người4 orchestration, không tự approve model.

Mỗi owner tự cung cấp tests, Dockerfile, probes, cấu hình và quyền tối thiểu cho
module mình. Người4 giữ pipeline/môi trường chung. Processed archive thuộc
Processing/Người1; raw archive thuộc Storage/Người4. Kafka clients ứng dụng do
Người1 viết; Kafka cluster và Storage Consumer do Người4 phụ trách.

# 27. Integration checkpoints

## M1 — Contract Ready

Điều kiện:

```text
[ ] SensorEvent schema frozen v1
[ ] PredictionRequest/Response frozen v1
[ ] RiskDecision/Alert DTO frozen v1
[ ] MaintenanceBrief contract frozen v1
[ ] Frontend Machine/Alert DTO frozen v1
[ ] mock fixtures committed
[ ] contract tests chạy CI
[ ] drift-reference / drift-report schemas draft/frozen đủ để ML + MLOps phát triển song song
```

## M2 — ML Connected

```text
valid SensorEvent
 -> Backend validator
 -> real ML /predict
 -> prediction persisted
```

Invalid event không gọi ML.

## M3 — Alert Ready

```text
prediction
 -> policy
 -> alert
 -> UI
```

ACK/resolve hoạt động. Tắt RAG vẫn có alert.

## M4 — Copilot Ready

```text
alert
 -> RAG retrieve
 -> LLM
 -> citation
 -> Alert Detail
```

Có no-evidence path.

## M5 — Observable & Resilient

```text
health
metrics
logs
failure tests
secret hygiene
drift worker + drift report + drift metrics/dashboard
E2E
```

## M6 — Demo Ready

Acceptance checklist pass + tagged release + evidence bundle.

---

# 28. Suggested developer commands

Nên chuẩn hóa bằng `Makefile` hoặc scripts để mọi người chạy giống nhau.

Ví dụ:

```text
make bootstrap
make db-up
make migrate
make backend
make ml
make rag
make frontend
make simulator
make drift
make drift-test
make up
make down
make unit
make contract-test
make integration-test
make e2e
```

Không bắt buộc dùng `make`; mục tiêu là tránh mỗi thành viên có một runbook khác nhau.

---

# 29. Seed/demo data

Nên có một thư mục deterministic:

```text
tests/fixtures/
├── sensor_valid_normal.json
├── sensor_valid_high_risk.json
├── sensor_invalid_missing.json
├── sensor_invalid_type.json
├── ml_prediction_failure.json
├── rag_grounded_response.json
├── rag_insufficient_response.json
├── drift_reference.json
├── drift_window_stable.jsonl
├── drift_window_warning.jsonl
└── drift_window_critical.jsonl
```

Các fixture này là contract assets, không phải dữ liệu UI hard-code.

Demo scenario nên dùng seed cố định để tránh việc model ngẫu nhiên không tạo alert đúng lúc demo.

---

# 30. Performance / latency — đo tách 202 ACK và end-to-end async

BRD gốc nêu mục tiêu p95 ≤ 2 giây từ **valid input tới alert hiển thị** (không tính LLM). Sau khi chuyển Kafka, đây phải được hiểu là **end-to-end event-to-visible latency** cần benchmark, không phải thời gian response của HTTP POST; mục tiêu không tự động đạt chỉ vì có queue.

```text
T0 Sensor gửi POST
T1 Ingestion nhận HTTP
T2 Kafka broker ACK (HTTP 202 Accepted)
T3 Processing bắt đầu consume
T4 ML trả prediction
T5 Risk decision + processing_db/outbox committed
T6 Business Consumer tạo/dedupe alert trong business_db
T7 Business API/React quan sát alert (REST/SSE)
```

Đo ít nhất:

- `ingestion_ack_latency = T2 - T1` (ingest nhanh là mục tiêu của Kafka).
- `event_to_prediction_latency = T4 - T0` (gồm queue waiting).
- `event_to_alert_visible_latency = T7 - T0` (business critical end-to-end).
- `archive_lag = S3 durable commit time - Kafka ACK time` (raw archival path).

RAG latency riêng sau khi alert committed; Drift batch không nằm trong p95 này. Load test phải tăng RPS/replicas và đối chiếu Kafka consumer lag, inference p95, Postgres I/O/pool, S3 throughput; report throughput không đủ để chứng minh chất lượng vận hành.

---

# 31. Security/config cho MVP

- `.env` trong `.gitignore`.
- Commit `.env.example`, không commit secret thật.
- API key chỉ truyền qua environment/secret store phù hợp môi trường.
- Không log authorization header.
- Nếu làm auth prototype: role tối thiểu `technician`, `admin`.
- Technician được thao tác lifecycle/feedback; admin mới được config/admin operations.
- Không có endpoint điều khiển PLC/actuator.
- CORS chỉ mở origin frontend cần thiết khi demo.

---

# 32. Những anti-pattern cần tránh

## 32.1. Alert Manager gọi model trực tiếp

Sai:

```text
Alert Manager -> ML
```

Đúng:

```text
Inference Client -> ML -> Risk Engine -> Alert Manager
```

## 32.2. Model quyết định CRITICAL

Không nên để ML trả business severity nếu BRD muốn tách policy.

ML trả:

```text
prediction
failure_probability
model_version
```

Risk Engine mới map severity.

## 32.3. Chờ LLM rồi mới tạo alert

Sai vì phá alert reliability và latency.

## 32.4. Invalid data = Normal

Đây là lỗi fail-safe nghiêm trọng.

## 32.5. Frontend tự tính risk

Frontend chỉ render Backend DTO.

## 32.6. Dùng test set để chọn threshold

Làm mất tính trung thực evaluation.

## 32.7. RAG không giữ source metadata

Nếu chunk không biết document/version/section/page thì gần như không thể citation đúng.

## 32.8. Prometheus label chứa ID duy nhất

Dễ cardinality explosion.

## 32.9. Mỗi người sửa contract riêng

Breaking integration. Contract change phải review bởi provider + consumer.

## 32.10. Nhét drift detection vào Data Quality Validator

Sai vì validator là record-level, drift là window/distribution-level và cần reference baseline.

## 32.11. Tính drift trên mỗi inference request

Làm tăng latency critical path và sai ranh giới kiến trúc. Dùng periodic batch worker.

## 32.12. Drift CRITICAL tự đổi machine thành CRITICAL

Sai semantics. Machine state vẫn do ML prediction + Risk Policy quyết định; drift là MLOps monitoring signal.

## 32.13. Dùng test set làm drift reference

Làm lẫn evaluation với runtime monitoring. Reference profile phải đến từ training/reference data đã chốt.

## 32.14. Baseline không gắn model_version

Dễ so runtime model mới với distribution của model cũ. Reference profile phải version-match.

---

# 33. Definition of Done theo module

## Sensor Simulator

```text
[ ] AI4I replay
[ ] feature leak prevention
[ ] speed/seed
[ ] fault injection
[ ] Backend integration
[ ] normal/sudden/gradual/categorical drift scenario configs
```

## Backend

```text
[ ] DB migrations
[ ] validation
[ ] ML client
[ ] risk decision
[ ] alert lifecycle
[ ] dedupe
[ ] audit
[ ] feedback
[ ] RAG fallback
[ ] OpenAPI
[ ] unit/contract tests
```

## ML

```text
[ ] baseline + model 2
[ ] fixed split protocol
[ ] imbalance handling evidence
[ ] threshold validation
[ ] final metrics
[ ] saved artifact
[ ] /predict
[ ] model_version
[ ] health/metrics
[ ] drift_reference.json version-match model artifact
```

## Drift Worker

```text
[ ] reference profile loader
[ ] VALID runtime window query
[ ] minimum sample rule
[ ] numeric drift
[ ] categorical drift
[ ] prediction drift signal
[ ] aggregate DriftStatus
[ ] drift_reports persistence
[ ] drift_checkpoints persistence
[ ] Prometheus metrics
[ ] worker stale/error rules
[ ] scenario tests
[ ] online path isolation test
```

## RAG

```text
[ ] versioned manual ingestion
[ ] pgvector retrieval
[ ] Recall@k
[ ] prompt guardrails
[ ] citation validation
[ ] insufficient evidence
[ ] /brief
[ ] health/metrics
```

## Frontend

```text
[ ] Fleet Dashboard
[ ] Machine Detail
[ ] Alert Detail
[ ] Monitoring View
[ ] Model Health / drift state visible hoặc link Grafana
[ ] lifecycle actions
[ ] feedback
[ ] RAG citation rendering
[ ] invalid/unknown/unavailable states
```

## Platform

```text
[ ] Dockerized stack
[ ] CI
[ ] DB migration in clean env
[ ] Prometheus
[ ] Grafana + Drift Dashboard
[ ] Drift alert rules
[ ] Drift Worker one-shot Compose + optional CronJob
[ ] structured logs
[ ] contract/integration/E2E/failure tests
[ ] release procedure
```

---

# 34. Checklist trước khi merge một feature

Mỗi PR tự hỏi:

```text
1. Có phá API/schema không?
2. Contract test đã update chưa?
3. Error state có explicit không?
4. Có vô tình map Unknown -> Normal không?
5. Có log correlation_id không?
6. Có metric cần thiết chưa?
7. Có secret bị hard-code không?
8. Unit test có cover branch mới không?
9. Consumer/provider còn tương thích không?
10. Nếu đổi model feature/version: drift_reference đã regenerate/version-match chưa?
11. Nếu đổi drift detector/report: drift contract + scenario tests đã update chưa?
12. Có thay đổi BRD behavior không? Nếu có, phải review/decision log.
```

---

# 35. Kịch bản dev end-to-end nên đạt sớm nhất

> [v1.2] **Không dùng luồng cũ Simulator→Backend validate→ML→Alert synchronous ở bên dưới làm E2E acceptance test.** Luồng E2E bắt buộc mới ở §37: 202 Kafka ACK → Processing → Business alert và Storage S3 song song.

Đừng đợi đến tuần 8 mới có end-to-end. Bản đầu có thể rất đơn giản (Kafka local 1 broker đủ để test chức năng, không phải HA):

```text
Simulator -> HTTP Ingestion -> Kafka sensor.readings (202 after ACK)
  -> Processing Worker -> mock/real ML -> Risk
  -> processing_db + outbox -> Kafka alert.signals
  -> Business Consumer -> business_db -> Frontend list
Kafka sensor.readings -> Storage Consumer -> S3 raw archive
```

Sau đó mới thêm:

```text
RAG
feedback
dedupe
monitoring
auth
resilience hardening
```

Thứ tự ưu tiên:

```text
P0 Contract + critical path
P1 Alert lifecycle + UI
P2 RAG grounded assistance
P3 Observability/failure handling + Drift Monitoring
P4 UX/security/polish
P5 Optional extension
```

---

# 36. Traceability về requirement

| Requirement | Module triển khai chính |
|---|---|
| FR-01 | Simulator + Sensor Ingestion + Data Quality Validator |
| FR-02 | ML Training + ML Inference Service |
| FR-03 | Inference Client + Risk Decision Engine + Alert Manager |
| FR-04 | Backend read APIs + React views |
| FR-05 | Alert Manager + Alert Detail/Feedback UI |
| FR-06 | RAG ingestion + retrieval + LLM + citation validator |
| FR-07 | Feedback Manager + Frontend actions |
| FR-08 | Cross-service fail-safe + UI explicit states |
| FR-09 | DB relations + audit + logs + versions + correlation id |
| FR-10 | Backend auth/config + platform secret hygiene + no actuator |
| Data Drift risk/control | ML reference profile + Drift Worker + Prometheus/Grafana + drift scenarios |

Lưu ý: dòng Data Drift là control cho rủi ro/monitoring trong BRD, không tự tạo thêm một Functional Requirement mới.

---

# 37. Luồng end-to-end bắt buộc để test và demo (v1.2)

```text
1. Simulator lấy row AI4I; tạo event_id cố định, machine_id đã provision,
   received_at có timezone; giữ body/id trong local buffer khi retry.
2. HTTP POST Ingestion; guard chỉ validate envelope, rate/auth; send Kafka.
3. Kafka ACK -> HTTP 202 PENDING; request kết thúc ngay, ML chưa bắt đầu.
4. Processing consumer group consume sensor.readings độc lập với Storage group.
5. Storage Consumer gom batch original accepted event lên S3/MinIO raw/.
6. Processing parse chi tiết + Data Quality:
   - INVALID -> persist issues/INVALID; no ML, no alert, commit offset.
   - VALID -> persist VALID, call ML inference API.
7. ML trả prediction + probability + model/preprocessor version.
8. Risk Engine áp policy_version; persist prediction, risk, processing status,
   optional outbox alert signal trong cùng transaction.
9. Outbox relay publish alert.signals; idempotent/replay safe.
10. Business Consumer insert inbox marker + create/dedupe alert trong business_db;
    commit DB trước Kafka offset.
11. Business API expose alert/fleet UI, live SSE telemetry/prediction/alert notifications; frontend REST bootstrap + reconnect resync.
12. Technician ACK/start/resolve, append alert_events + feedback.
13. Backend facade có thể gọi RAG async sau alert để lấy grounded brief,
    failure không rollback alert.
14. Drift CronJob định kỳ lấy VALID window qua Processing API/S3 curated,
    load đúng reference model_version, lưu drift_db + S3 report + Pushgateway.
15. Training dùng curated/versioned dataset từ S3, MLflow API metadata
    vào mlflow_db và artifact vào S3, không tự retrain mỗi raw upload.
```

### 37.1. Tests bắt buộc theo thứ tự khởi đầu

| Scenario | Assertion |
|---|---|
| Normal event | 202 after ACK -> eventual VALID/prediction; S3 raw contains event |
| Feature INVALID | 202 -> INVALID/no inference; raw still archived |
| Bad JSON/envelope | 400/422 before Kafka publish |
| Kafka producer unavailable | 503/504; sensor retries with same event_id |
| Processing consumer stopped | Kafka lag grows; Storage still archives; catch-up when resumed |
| Storage/S3 stopped | Processing unaffected; group lag tracked, recovery before retention |
| Duplicate/rebalance | No duplicate prediction/risk/alert business effects |
| DB committed, outbox relay crash | Signal eventually delivered after recovery; inbox handles duplicates |
| Business Consumer paused | No incorrect alert claim at ingest; alert appears after resume |
| ML timeout | terminal UNKNOWN/FAILED after retry; no false NORMAL |
| RAG failed | Existing alert unaffected; no fabricated checklist |
| Drift reference mismatch | ERROR report in drift_db; no change to machine state |
| Load test | RPS, 202 ACK p95, e2e prediction p95, alert visibility p95, lag and S3 throughput measured |

### 37.2. Chấp nhận scale-ready ≠ đã scale thành công

Thiết kế đủ boundary để tăng Pod riêng, nhưng không khẳng định đã benchmark. Demo Compose một broker không chịu lỗi broker; PostgreSQL một instance vẫn có SPOF; MinIO local một node chưa có replication. SLOs cần được đo rồi mới khóa các ngưỡng cụ thể.

---

# 38. Kết luận triển khai

MaintAI nên được dev như **một hệ thống có contract rõ**, không phải 5 bài tập riêng ghép vào cuối kỳ.

Ba boundary quan trọng nhất phải được giữ:

```text
ML prediction != business risk decision
Risk decision != alert lifecycle
Alert != RAG/LLM recommendation
```

Tương ứng:

```text
ML Service
    -> dự đoán

Backend Risk/Alert
    -> quyết định và quản lý workflow

RAG/LLM
    -> cung cấp evidence-based assistance
```

Nếu nhóm freeze contract sớm, mock dependency đúng và tích hợp theo milestone, 5 người có thể phát triển song song mà vẫn giữ được một demo end-to-end ổn định, có failure handling, traceability và evidence cho từng requirement.


---


# 39. Playbook triển khai chuyển từ v1.1 sang v1.2

## 39.1. Đầu việc theo thứ tự tránh block nhau

1. **Freeze event schemas và async HTTP contracts** (`sensor.readings`, `alert.signals`, status API, Basic Validator). CI kiểm tra DTO/schema/examples theo version, backward-compatible tests.
2. **Provision Kafka/S3 và 5 DB**, tập migrations vào từng owner, chuyển `machines/alerts/feedback/rag_briefs` từ monolithic DB sang `business_db`, sensor/prediction/risk vào `processing_db`, drift sang `drift_db`. Giữ external ID links, không tạo FK cross DB.
3. **Ingestion HTTP producer** trả 202 đúng thời điểm broker ACK; simulator local retry stable ID.
4. **Processing Consumer** + ML client + Risk Engine + DB transaction/outbox; expose read API statuses/window.
5. **Business Consumer/API** + inbox/alert lifecycle; thay HTTP route synchronous alert creation bằng `alert.signals` handler.
6. **Storage Consumer** batch→S3 with deterministic key, checksum/manifest, offset commit; audit invalid/raw duplicates.
7. **Drift data adapter** chuyển PostgreSQL direct query → Processing read API/curated S3; vẫn giữ detector/report/Pushgateway.
8. **Frontend** hỗ trợ 202 PENDING, polling/status eventual consistency, SSE reconciliation, alert action unchanged.
9. **Training/MLflow/RAG** giữ pipeline và semantics cũ, cấu hình DB/S3 owners.
10. **Compose integration tests, failure injection, load tests**, sau đó mới mô tả các thành phần là implemented.

## 39.2. Chia việc theo 4 thành viên hiện có

| Thành viên | Phần chính | Đầu ra |
| --- | --- | --- |
| Người1 | Backend/Main Frontend/Ingestion/toàn bộ Processing | Business/processing DB, Kafka app clients, APIs/SSE/UI/history/alerts/jobs |
| Người2 | ML | Training/inference, dataset/split/model/reference lineage và đánh giá |
| Người3 | RAG/LLM | Knowledge ingestion/retrieval, citations, prompt/document versions |
| Người4 | Platform + Sensor/Simulator UI + Drift + Storage | Kafka/S3/MLflow/GitOps, simulator, quarantine/raw archive, drift publication |

Chi tiết tại §26 và docs index. Đây là phân công, không chứng minh source đã triển khai.

## 39.3. Consumer crash consistency pseudocode

```python
# Processing: durable side effects trước offset commit
for record in kafka.poll_batch():
    event = decode(record.value)
    outcome = validate_and_predict(event)       # retry policy bounded
    with processing_db.transaction() as tx:
        tx.insert_sensor_if_new(event, outcome)
        tx.insert_prediction_risk_if_new(outcome)
        if outcome.should_alert:
            tx.insert_outbox_if_new(alert_signal(outcome))
    kafka.commit_offset(record.partition, record.offset + 1)

# Business: inbox/idempotent alert state trong cùng DB transaction
for signal in kafka.read('alert.signals'):
    with business_db.transaction() as tx:
        if tx.insert_inbox_once(signal.signal_id):
            tx.create_or_update_alert_by_dedupe_policy(signal)
    kafka.commit_offset(signal.partition, signal.offset + 1)
```

Đây là pseudocode, không chạy như copy-paste. Processing cần phân loại transient/non-transient failures và đảm bảo trạng thái reprocessing không tái tính/hậu quả sai. Outbox relay và Storage cũng phải chứng minh retry safe. Nếu Redis/cache cho ingress status thì phải cân nhắc durability & consistency, không coi volatile cache là source of truth.

## 39.4. S3 archive layout và batch rules

```text
s3://maintai-data/
  raw-sensor/topic=sensor.readings/partition=0/year=2026/month=10/day=17/
    offset-000000-000999.jsonl.gz
    offset-001000-001999.jsonl.gz
  curated-datasets/dataset_version=.../
  drift-reports/model_version=.../
  mlflow-artifacts/
  rag-documents/document_id=.../version=.../
```

Mỗi batch có `offset_start`, `offset_end`, `record_count`, checksum, schema version, object path; path deterministic theo partition+offset ranges. Sau S3 successful write **và** manifest durably committed mới Kafka offset commit. Quyết định dung lượng batch theo measured object count/latency, không cứng hóa số records một cách không có benchmark. Retention policy cho raw objects và test restore/replay.

## 39.5. Metrics và alerting tối thiểu cho event-driven scale-ready

```text
Ingestion: accepted_events_total, kafka_publish_failures_total, ack_latency_p95
Processing: consumer_lag, event_to_prediction_latency, invalid_rate,
            ml_client_timeout_total, DB pool saturation, outbox_pending_age
Storage: consumer_lag/oldest_age, objects_written_total, S3 write failures,
         raw_archive_latency, uncommitted batch age
Business: alert_signal_consumer_lag, inbox_duplicate_total,
          alert_dedupe_total, event_to_alert_visible_latency
Drift: drift_overall_status, last_success_timestamp, model/reference version
Kafka: broker health, disk usage, retention_remaining_relative_to_lag
```

Prometheus metric implementation phải dùng histograms/counters phù hợp, `_p95` ở trên mô tả dashboard objective không mặc định là metric counter. Không gắn raw event/machine/request IDs vào metric labels. Dùng Prometheus dashboards và threshold theo baseline load-test, không bịa SLO production từ AI4I 10k records.

## 39.6. Những gì **chưa được tạo** chỉ từ việc cập nhật Markdown

- Chưa sửa runtime Kafka producers/consumers, Compose manifests, Kubernetes/GitOps repositories, Alembic revisions, SQL schema hoặc database thật.
- Chưa sửa các machine-readable OpenAPI/JSON schemas và automated tests trong folder `contracts/`.
- Chưa thực thi benchmark; không gọi trạng thái demo là production ready/highly available.
- Các nguồn docx BRD hoặc file ngoài năm tài liệu đính kèm không được đánh giá là đã update. Breaking change 202 vs synchronous cần review với BRD/owner trước khi freeze production contract.


---

# 40. Frontend / Realtime UI Development Guide v1.3 — thực thi phương án C

## 40.1. Chốt yêu cầu từ hai cuộc trò chuyện

- **Không đợi inference mới cập nhật sensor tiles.** HTTP Ingestion publish Kafka `sensor.readings`; telemetry projector consume để cập nhật `Live Telemetry` (được chấp nhận nhưng chưa qua full Data Quality).
- **Không hiện PENDING trên mỗi sensor record.** Một status strip nhẹ hiển thị Updating/Delayed/Stale theo latency, chứ không nhấp nháy spinners từng giây.
- **Chỉ valid completed prediction** được hiển thị trong `Latest Valid Prediction`. Reading R4 và prediction R2 cùng tồn tại với event ID/timestamp khác nhau. Không che dấu độ cũ.
- **Không drop raw archive / ML processing / alert** chỉ vì UI không render một event/prediction cũ. UI ordering guard không phải processing skip policy.
- **SSE fan-out** phải đến mọi Backend API replica; Kafka consumer group đơn không phải broadcast. Khi mất SSE thì resync bằng REST, không giả định Last-Event-ID bảo đảm replay.

## 40.2. FE layout và routes cụ thể

```text
┌───────────────────────────────────────────────────────────────────────────────┐
│ MaintAI | [SSE: Connected] [Last sync: 00:00:03]                  [User/Role]│
├────────────────┬──────────────────────────────────────────────────────────────┤
│ Fleet          │ Machine M-001                      Processed State: WATCH    │
│ Machines       │                                                              │
│ Alerts         │ ┌──────────────────────────┐ ┌──────────────────────────┐ │
│ Monitoring     │ │ LIVE TELEMETRY           │ │ LATEST VALID PREDICTION │ │
│                │ │ air_temperature: 298.1 K │ │ Label: NORMAL           │ │
│                │ │ rotational_speed: 1500   │ │ Failure prob: 12%        │ │
│                │ │ tool_wear: 108 min       │ │ Model: v1               │ │
│                │ │ R4 · received 0.1s ago   │ │ Based on R2 · 3s old     │ │
│                │ │ Raw / not yet validated  │ │ UPDATING / DELAYED       │ │
│                │ └──────────────────────────┘ └──────────────────────────┘ │
│                │ [latest processing status + temporal gap banner]            │
│                │ [Sensor/Prediction timeline] [Active alerts + Copilot]      │
└────────────────┴──────────────────────────────────────────────────────────────┘
```

| Route/page | Content | REST source | SSE update |
|---|---|---|---|
| `/machines` | Fleet summary cards, table, search/filter, stale badges | `/api/v1/machines` | telemetry, prediction, alert |
| `/machines/:machineId` | Hai cards độc lập, status strip, timeline, alerts | `/api/v1/machines/{id}/live`, `/api/v1/machines/{id}/sensor-readings` | telemetry.received, prediction.completed, processing.status_changed |
| `/alerts` | Severity/status filters, latest active alerts | `/api/v1/alerts` | alert.created/updated |
| `/alerts/:alertId` | Alert audit + actions + Copilot citations/brief | `/api/v1/alerts/{id}`, `/api/v1/alerts/{id}/brief` | alert.updated, brief.ready |
| `/monitoring` | Drift model/reference, SLO/lag/service health | `/api/v1/monitoring/summary`, `/api/v1/drift-reports` | drift.updated/service.degraded hoặc refresh |

UX: responsive 2 cards ngang desktop, xếp dọc mobile; status bằng **text + icon + colour**, không chỉ màu; timestamp dùng time zone người dùng; mọi biểu đồ phải ghi đơn vị Kelvin/°C, rpm, Nm, phút; loading, empty, auth error, stale, unavailable có states rõ ràng.

## 40.3. FE state types và tách nguồn dữ liệu

```ts
type ProcessingStatus = 'PENDING' | 'PROCESSING' | 'RETRYING' |
  'COMPLETED' | 'INVALID' | 'FAILED';
type Freshness = 'FRESH' | 'DELAYED' | 'STALE' | 'NOT_AVAILABLE';

type LiveTelemetry = {
  event_id: string;
  sequence_number?: number;
  server_accepted_at: string;
  features_raw: Record<string, unknown>; // raw may be INVALID
  validation_status?: 'UNVERIFIED' | 'VALID' | 'INVALID'; // UI projection only
};
type LatestValidPrediction = {
  source_event_id: string;
  sequence_number?: number;
  completed_at: string;
  source_server_accepted_at?: string;
  failure_probability: number;
  prediction: 'NORMAL' | 'FAILURE';
  machine_state_at_prediction?: 'NORMAL' | 'WATCH' | 'HIGH' | 'CRITICAL';
  model_version: string;
  policy_version?: string;
};
type LiveMachineState = {
  machine_id: string;
  telemetry: LiveTelemetry | null;
  latest_valid_prediction: LatestValidPrediction | null;
  latest_processing: {
    source_event_id: string;
    processing_status: ProcessingStatus;
    updated_at?: string;
    failure_code?: string;
  } | null;
  as_of: string;
};
```

**Contract rule:** Bất kỳ invalid/failed reading nào không được biến `latest_valid_prediction` thành một prediction mới. Máy có thể `INVALID/UNKNOWN` cho latest processed reading **đồng thời** có historical last valid prediction. UI phân biệt hai trục.

## 40.4. React state reducer — tránh stale overwrites

```ts
function mergeTelemetry(state: LiveMachineState, event: LiveTelemetry) {
  if (!isNewerReading(event, state.telemetry)) return state;
  return { ...state, telemetry: event };
}
function mergePrediction(state: LiveMachineState, event: LatestValidPrediction) {
  if (!isNewerSourceReading(event, state.latest_valid_prediction)) return state;
  return { ...state, latest_valid_prediction: event };
}
```

`isNewerReading` phải dùng một **ordering token ổn định theo máy và source**: `sequence_number` durable monotonic qua restart; khi không có sequence hợp lệ, **chưa được** tự chọn timestamp hoàn thành inference. Có thể thiết kế ingress sequencing ledger/partition ordering sau khi xác định assumptions và tests. Dùng `event_id` làm idempotency key (opaque), **không sort lexicographic event_id**; timestamp từ simulator có thể bị lệch. Deduplicate SSE by event_id/result_id. SSE updates thuộc nhiều Kafka topics **không có total order chung**; reducer merge theo field-source, không dùng một timestamp chung đè cả snapshot.

## 40.5. SSE hook + REST bootstrap/recovery

Gợi ý modules:

```text
frontend/src/
├── app/                           # layout + routes
├── features/
│   ├── fleet/
│   ├── machines/
│   │   ├── MachineDetailPage.tsx
│   │   ├── LiveTelemetryCard.tsx
│   │   ├── LatestPredictionCard.tsx
│   │   ├── ProcessingHealthStrip.tsx
│   │   └── SensorPredictionTimeline.tsx
│   ├── alerts/
│   ├── copilot/
│   └── monitoring/
├── hooks/useMachineLive.ts
├── api/maintaiApi.ts
├── realtime/sseClient.ts
├── realtime/reducers.ts
├── realtime/freshness.ts
└── types/liveMachine.ts
```

Flow chính: REST GET current live snapshot để bootstrap; mở SSE (khuyến nghị **subscribe trước, buffer trong khi fetch, rồi merge theo ordering guard** để không mất event ở giữa) → handle named events → update component-local/query cache state → reconnect with exponential backoff → fetch snapshot REST và merge. SSE connection status là `CONNECTED/RECONNECTING/OFFLINE`, không phải MachineOperationalState. Browser auth/cookies/token theo auth profile, không stream dữ liệu của máy không được quyền. `Last-Event-ID` là hint; không dựa vào replay bền vững.

Frontend **không** gọi trực tiếp Kafka, ML Service, RAG Service, processing_db, S3 object storage, hay PostgreSQL. Dữ liệu UI đến từ Business facade REST+SSE.

## 40.6. UX freshness và temporal mismatches

```text
Example timeline:
12:00:00.050 Telemetry R1 -> UI
12:00:00.350 Prediction R1 -> UI
12:00:01.050 Telemetry R2 -> UI
12:00:01.400 Prediction R2 -> UI
12:00:02.050 Telemetry R3 -> UI
12:00:03.050 Telemetry R4 -> UI
12:00:03.600 Prediction R3 -> UI
```

At 12:00:03.050 show R4 telemetry and R2 prediction, label `Based on R2`; không gộp R4 cùng 12% của R2 thành kết luận R4. Tại 12:00:03.600 nếu R3 mới hơn R2 (theo sequence) thì update R3; nếu R1 xử lý xong sau R3, **không** revert về R1. Always keep processing/audit result even when not shown as current.

Policy UI demo (config, not hard-coded prod SLO): age of **source reading** `< 2s` FRESH, `2–5s` DELAYED, `>5s` STALE; hiển thị `NO PREDICTION` nếu chưa từng có kết quả valid. Độ cũ của telemetry tính bằng backend accepted time; UI không dùng `completed_at` để đánh giá reading mới hay cũ. Update rate có thể throttle ≤ 1Hz/machine chỉ ở SSE/React, **không** drop history/archive/ML side effects.

## 40.7. Backend responsibilities để hỗ trợ FE chính xác

- Ingestion API (separate from Business Backend): publish `sensor.readings`, bảo toàn event_id/sequence, ACK 202. Không chạy inference để phục vụ UI.
- Business telemetry handler (inside Business Consumer; separate API Deployment): read `sensor.readings`, materialize latest raw receipt trong business_db, phát notification tới SSE layer **không** đợi Group Processing.
- Processing Worker: Data Quality → ML (chỉ VALID) → Risk → persist processing_db/outbox. Emit `inference.results` ngay khi có result/status terminal, cũng emit `alert.signals` khi policy yêu cầu.
- Business prediction handler (inside Business Consumer; not ML Service): consume `inference.results`, materialize latest valid prediction/status riêng; không xóa `latest_valid_prediction` khi INVALID/FAILED.
- Business Alert Consumer: consume `alert.signals`, inbox/dedupe, cập nhật alert/audit, notify SSE. UI source snapshot & alert source owned by Business.
- Business API + SSE: frontend gateway giữ connection user, REST live endpoint, authorization, projection reads; **multi-replica fan-out** bằng broadcast pub/sub (Redis hoặc PG LISTEN/NOTIFY demo) sau projector commit. Với 1 replica demo có thể dùng emitter nội bộ; không được dùng shared Kafka group giữa SSE replicas để quảng bá events rồi kỳ vọng mỗi Pod nhận đầy đủ.
- Storage Consumer archive raw `sensor.readings` cho S3 độc lập, Drift CronJob đọc S3/Processing API như trước.

## 40.8. Các kịch bản test ưu tiên trước demo

1. Replay R1-R5 mỗi 1 giây; SSE telemetry không phụ thuộc ML (trễ inference). UI không chớp pending mỗi lần.
2. ML latency 1.5 giây: R4 live telemetry hiển thị với R2 prediction cũ; đúng status/age.
3. R2 prediction hoàn thành trước R1: reducer giữ R2 và event R1 vẫn log/history.
4. R3 invalid: storage S3 có raw, Processing không gọi ML, UI show data-quality issue, giữ historical latest valid prediction.
5. ML 503: frontend shows telemetry, old prediction with STALE/unavailable (not fresh NORMAL), alerts still retained.
6. Kafka outage: Ingestion fails 503, simulator retry same `event_id`; no false telemetry.received event.
7. SSE disconnect: UI connection badge, last snapshot timestamp, reconnect+GET latest; no duplicate/downgrade state.
8. Multi-backend API replicas: clients connected each replica both get the same filtered events after projector emit; REST remains consistent.
9. Unauthorized machine request/stream: 401/403/filtered; no data leakage.
10. Consumer archive/processing failures separately; one path continues, backlog visible.

## 40.9. Definition of Done FE cho phương án C

- [ ] Fleet / Machine / Alerts / Copilot / Monitoring screens có mock fixtures chuẩn và responsive layout.
- [ ] LiveTelemetryCard độc lập LatestPredictionCard; song song rendering và timestamps khác nhau.
- [ ] Latest valid prediction giữ được history nhưng có stale labels; không tự coi như inference của latest raw reading.
- [ ] Unit tests ordering guard / duplicate SSE / stale handling / invalid / failed / no prediction.
- [ ] REST bootstrap + SSE connect/reconnect + resync tests.
- [ ] API contract/JSON schema fixtures có `sensor.readings`, `inference.results`, live snapshot, SSE event types mới.
- [ ] Tích hợp alert lifecycle và RAG không chặn dữ liệu telemetry.
- [ ] Load tests hiển thị latency end-to-end (ingest ACK vs event-to-telemetry/prediction visible), Kafka lag từng group, SSE fan-out nhiều replicas.


---

# 41. Simulator UI riêng + Business Projection Implementation Plan (1.3.2)

## 41.1. Sensor Simulator Control — per machine

Sensor Control API thuộc `sensor/src/simulator/control/`; ordering/scheduling/
retry_spool là các module đã có placeholder, không phải service riêng. State,
control_version, per-machine sequence và pending payload phải durable trước ACK;
không còn đề xuất persist tùy chọn nếu cần đúng qua restart.

Pause dừng tạo record mới của máy được chọn; spool đã tạo vẫn được retry, Kafka
backlog vẫn được xử lý. Resume dùng event ID mới và tiếp tục sequence tăng qua
restart; scenario đổi future generation, không auto-resume. Một controller với
Recreate; strict failover/multi-controller cần shared lease/fencing.

## 41.2. Sensor Control API cho Simulator UI riêng (amendment1.3.2)

```http
GET  /api/v1/simulator/machines/{machine_id}/control
POST /api/v1/simulator/machines/{machine_id}/pause
POST /api/v1/simulator/machines/{machine_id}/resume
PUT  /api/v1/simulator/machines/{machine_id}/scenario
```

Simulator UI calls Sensor Control API on its own management origin; Sensor authenticates/authorizes demo control. Target return 200 after control ACK, or error/unknown-outcome plus GET reconcile; repeated Pause/Resume idempotent. Example GET:

```json
{
  "machine_id": "M-001",
  "stream_state": "PAUSED",
  "scenario": "normal",
  "control_version": 2,
  "updated_at": "2026-10-18T00:00:00Z",
  "last_controlled_by": "fixture-operator",
  "source": "SIMULATOR_CONTROL"
}
```

Physical machine state not inferred from this; UI distinguishes `PAUSED` vs `STALE` vs `OFFLINE`. Alert resolution always separate Business action.

## 41.3. Projector model / SQL responsibilities

```text
business_db.machines                         # business owned
business_db.alerts, alert_events, feedback  # business owned
business_db.live_machine_telemetry          # one latest row per machine
business_db.live_machine_prediction         # one latest valid prediction row per machine
business_db.live_machine_processing         # one latest processing outcome row per machine
business_db.business_inbox                  # dedupe committed side effects
```

These are **logical concepts**; can combine them into fewer physical tables per actual repository schema, but no unbounded raw history in Business. Comparison uses **durable per-machine monotonic `sequence_number` across Pause/Resume/restarts**, with `stream_epoch` stored only for lineage; if sequence cannot persist, add ingress-assigned monotonic `machine_ingest_seq`/durable ordering first. Never compare arbitrary textual epoch IDs. Do not rely on `event_id` string order. `received_at` may be simulator-generated and not reliable across clocks.

`inference.results`: `PREDICTION_COMPLETED|DATA_INVALID|INFERENCE_UNAVAILABLE`; only first updates last valid prediction, all terminal cases can update latest processing. Consumer transactions update projection/inbox and use PG NOTIFY via transactional SQL so notify is delivered post-commit; Business API listens and fetches latest read model then SSE to all connected authorized clients. DB snapshot is for recovery not required by SSE protocol.

## 41.4. React UX và integration acceptance

Main Machine Detail giữ Live/Last Telemetry, Latest Valid Prediction, Latest
Processing và Active Alerts. Không có simulator buttons hoặc sensor credential.
Simulator UI riêng hiển thị RUNNING/PAUSED/scenario/control_version, last command
ACK và retry/reject counters từ Sensor Control API. Không giả định Simulator UI
có quyền đọc Business telemetry/prediction/alerts hoặc rằng Main UI có control API.

E2E tương lai: pause M-001 nhưng M-002 vẫn phát; backlog cũ vẫn hoàn tất; malformed
envelope bị reject riêng; feature-invalid được raw archive nhưng không inference;
resume tiếp tục sequence; response control mất thì GET reconcile; source ordering
không rollback UI; multi-API notify + reconnect REST; alert không tự resolve.

## 41.5. Migration / implementation order

1. Freeze changed JSON schemas (`sensor.readings` ordering/epoch, discriminated `inference.results` result_kind union, simulator control DTO, SSE events).
2. Separate Ingestion ownership/deployment from Business API; give Processing `processing_db`; keep Business Consumer + Business API two Deployments same codebase.
3. Business Consumer telemetry snapshot from raw; predictions/outcomes from Processing outbox; alerts from signals.
4. PG NOTIFY fan-out + Business REST live bootstrap/SSE + React reducer ordering and stale indicators.
5. Simulator Control API, per-machine pause, recovery scenario and separate Simulator UI; rejected log, durable retry queue.
6. Storage Consumer S3 batch archive, observability lag, failure/replay/load tests, full scenario demo.

**Note:** This document updates **target design** only. Existing code, OpenAPI machine-readable specs, Alembic migrations, CI/GitOps must be updated separately and tested before describing the architecture as implemented.

---

# 42. v1.4 — Hướng dẫn triển khai theo Deployment và các điều khoản ưu tiên

Các mô tả module trong v1.3.1 phía trên vẫn được sử dụng, **trừ khi** bị ghi đè bởi các điều khoản mới. Trước khi code cần đọc `DEPLOYMENT_ARCHITECTURE.md`, `RUNTIME_DATAFLOW.md`, `VERSIONING_AND_PROVENANCE.md` và System Contracts v1.4. Đây là **tài liệu đầu vào bắt buộc**, không phải phần tham khảo tùy chọn. Phải khảo sát repo thực, Alembic migrations, Compose, OpenAPI, Kafka schemas và GitOps trước khi sửa code; nội dung Markdown không chứng minh source đã được cập nhật.

## 42.1. Danh sách Deployments/runtimes cần triển khai đúng

Bổ sung rõ `processing-read-api` là **Kubernetes Deployment độc lập** với `processing-worker`, được expose bằng internal ClusterIP Service và dùng PostgreSQL credential `processing_ro` (read-only). `processing-outbox-relay` là runtime riêng có thể khởi động lại và tăng replica tùy backlog, nhưng phải bảo đảm không publish trùng side effects. `business-api` và `business-consumer` giữ hai Deployments cùng Business codebase. **Không tạo `history_db` thứ sáu** và không bắt buộc History Service riêng trong MVP. Simulator Control chỉ dành demo; training là Job chạy theo yêu cầu, drift là CronJob. Kafka/Postgres/MinIO chạy in-cluster demo hoặc bên ngoài; không mặc định có HA.

## 42.2. Truy vết từ repository đến image và Deployment

Lưu bảng mapping có version trong GitOps: repo/source Git SHA, OCI image digest, startup command/args, cổng, authentication và secrets, owner DB/role, Kafka groups/topic ACL, điều kiện scale, probes và retention. Cùng một image có thể chạy `processing/worker_main.py`, `processing/read_api_main.py`, `processing/outbox_relay_main.py` với startup commands khác nhau; **không copy DQ/ML/Risk vào Read API**. Xác minh Python import paths và cổng từ source thật, không dán mù lệnh minh họa cũ. K8s scaffold ở `../implementation/k8s/base`.

# 43. v1.4 — Triển khai Versioning & Provenance xuyên suốt

## 43.1. Curated dataset và train/validation/test split có thể tái lập

Pin SHA256 của AI4I source file. Khi Data Preparation tạo curated dataset mới, sinh `manifest.json` bất biến ghi source URI/hash, Git SHA của transform code, transform config/version, feature schema digest, từng shard/row count, split indices + SHA256, random seed, label source và các quy tắc loại trừ/chống trùng. DVC với remote S3 phù hợp khi có nhiều experiments, nhưng manifest S3 + checksums vẫn là phương án MVP được chấp nhận. **Không tuyên bố đã dùng DVC khi repo chưa thực sự tích hợp.** Không tune bằng held-out test, không coi Simulator machine/time là bảo trì thực tế.

**Quy trình code đề xuất:**

```text
Download/Verify AI4I Source
  -> ghi source checksum, license, schema
  -> Clean + Feature Allowlist
  -> tạo curated immutable shards và manifest
  -> sinh split_indices.json, lưu seed/split SHA
  -> CI xác minh hash manifest và tính lặp lại
  -> cấp dataset_version cho Training Job
```

## 43.2. ML Training → MLflow → Model release

Training Job ghi MLflow run với `run_id` thực cùng `dataset_version`, manifest SHA, split SHA, training Git SHA, training OCI image digest, feature schema version, lockfile SHA, seed/params, validation/test metrics, threshold-selection evidence và drift reference SHA. Đăng ký model version có exact model/preprocessor artifact digest. Sinh approved release manifest từ giá trị **thật** của registry/MLflow/GitOps, theo `../contracts/provenance/release_manifest.schema.json`.

Alias `candidate`/`champion` nếu có chỉ là selector: tại bước Approve phải resolve thành model version bất biến, rồi GitOps pin version/digest cụ thể. Startup ML Service từ chối readiness nếu checksum model/feature/preprocessor/reference lệch release. Endpoint `/health/ready` hoặc diagnostic nội bộ được phân quyền có thể công bố effective release/model ID, không chứa secrets.

**Bước nghiệm thu:** Train hai lần cùng seed/split/code phải cho lineage và input digests nhất quán; model run ID có thật trong MLflow; artifact hash tính được từ byte thật; cố ý đổi artifact phải làm startup fail, không fallback sang model khác.

## 43.3. Danh tính runtime: DQ → Prediction → Risk → Alert

Processing Worker tải DQ ruleset hash và risk policy hash đã pin. Khi nhận reading, lưu source `topic/partition/offset`, `event_id`, `machine_id`, sequence, DQ status/reasons, DQ ruleset version/digest.

Nếu VALID: gửi ML request, nhận registered model version/artifact thực tế, probability và threshold; lưu prediction/risk cùng causal IDs, timestamps, `release_id`, policy SHA và image digest. Nếu INVALID: **không gọi ML**, lưu terminal invalid outcome và lý do. Nếu ML unavailable: lưu lỗi có kiểu, không sinh NORMAL giả. Trong cùng DB transaction, tạo outbox cho `inference.results` đối với **mọi terminal outcome**. Chỉ tạo `alert.signals` nếu Risk Decision yêu cầu. Business Consumer sở hữu Alert và lifecycle, không tự chạy inference.

**Chống trùng:** Unique event/result IDs; Kafka offset commit sau DB commit; Outbox Relay retry an toàn; Business inbox atomically ghi dấu processed message cùng Alert/projection. Prediction cũ xử lý muộn vẫn lưu history nhưng không ghi đè latest prediction mới hơn.

## 43.4. Provenance của RAG và Drift

RAG ghi source PDF SHA, `document_id/version`, chunk ID, page/section, ingestion pipeline/chunker/embedding model versions, index revision, prompt ID/digest và LLM identity vào Brief. Các citations của Brief cũ vẫn trỏ được đúng phiên bản manual cũ sau reindex. `INSUFFICIENT_EVIDENCE`/`VALIDATION_FAILED` không được tạo checklist thiếu căn cứ.

Drift report lưu reference profile digest gắn với model release cụ thể, detector/policy version, valid-only window watermark, checkpoint và status. Không khớp model/reference ⇒ `ERROR`, không âm thầm regenerate từ current data. Drift không tự retrain/promote hay thay machine risk.

# 44. v1.4 — Kafka Outcomes và hợp đồng Storage/History

## 44.1. Luồng schema rejection (ghi đè mô tả local-only log cũ)

Với request đã auth, trong size limit nhưng sai JSON/envelope: Ingestion sanitize/hash body theo chính sách bảo mật, tạo `rejection_id` và `request_id`, publish Kafka topic `sensor.rejected`, **đợi broker ACK trước khi trả HTTP 400/422**. Nếu publish không xác nhận, trả 503/504 retryable; không giả dữ liệu đã lưu. Không lưu payload không xác thực/quá lớn một cách tùy tiện. Simulator local rejected log có thể giữ để debug. Storage Consumer archive an toàn vào S3 `rejected/`.

Feature INVALID nhưng envelope đúng vẫn được `202` sau `sensor.readings` ACK, đi S3 `accepted/`, Worker phát `DATA_INVALID` và không gọi ML. Như vậy *schema rejected* và *DQ rejected* là hai trạng thái khác nhau, dù cả hai đều có raw evidence khi lưu trữ được phép.

## 44.2. Cấu trúc outcome event

**Không tạo topic chất lượng thứ năm bắt buộc** và không đổi `inference.results` thành `processing.results`. Dùng `schema_version=1.4.0` và discriminator `kind`; mang source event/Kafka locator và DQ ruleset SHA.

- `PREDICTION_COMPLETED`: Bắt buộc model identity, probability, threshold, prediction và risk decision.
- `DATA_INVALID`: Bắt buộc validation errors, **cấm** có model/prediction/risk giả.
- `INFERENCE_UNAVAILABLE`: Bắt buộc typed error, **cấm** fake success.

JSON Schema của repo: [inference-result.schema.json](../contracts/kafka/events/inference-result.schema.json);
fixtures ở [Kafka examples](../contracts/kafka/examples/manifest.json). Python SDK và
owner integration ở [Kafka guide](KAFKA_DEVELOPMENT_GUIDE.md). Transactional outbox
tạo bản ghi cho cả outcome không có Alert. Reader hỗ trợ1.4.0 `kind` và legacy2.0.0
`result_kind`, writer chỉ1.4.0; rollout reader trước writer, không bịa provenance legacy.

## 44.3. S3 archive manifest và History Query

Storage Consumer tạo object key xác định từ `topic/partition/offset` và checksum. Batch manifest phải có `record_count`, offset range, schema version, object path và hash. Chỉ commit Kafka offset sau khi object và manifest đã được ghi bền vững. Có thể archive outcome bằng consumer group riêng.

`processing-read-api` đọc indexed hot history từ `processing_db`; với cold S3 phải tra object bằng partition/date/manifest trước, không quét toàn bucket mỗi UI request. Business API là facade duy nhất cho Frontend. Alert History vẫn ở `business_db`, không truy vấn từ Processing. Read API dùng role read-only; Business không cross-DB SQL. Filter INVALID cần versioned query contracts/cursor. Khi schema-rejected thiếu machine ID, lọc theo source/request/time sau auth.

# 45. v1.4 — Quy trình CI/CD, phát hành và rollback

Đưa JSON Schemas, fixtures, GitOps overlay release manifests và Alembic revisions vào Git. Pipeline khuyến nghị:

```text
PR: lint + unit + schema compatibility
  -> build image + pin OCI SHA256 digest
  -> verify dataset/model/artifact digests
  -> producer/consumer integration tests
  -> approved immutable release manifest
  -> cập nhật GitOps revision
  -> rollout + readiness + effective release check
  -> E2E, outage/backlog, load benchmarks
  -> promote thủ công hoặc rollback release được duyệt trước đó
```

**Không đổi MLflow alias mutable để âm thầm chuyển model của release đang chạy.** Rollback là triển khai lại manifest có image/model/policy digests cũ; DB migrations phải theo expand/contract để reader/consumer cũ còn tương thích. Prediction/Alert lịch sử **không được viết lại** bằng model mới.

# 46. v1.4 — Phân công kỹ thuật theo workstream

| Owner | Công việc và đầu ra bắt buộc |
|---|---|
| Processing/Ingestion (Người1) | Producer accepted/rejected, DQ digest, transactional outbox, `processing_ro` Read API, hot indexes, Kafka offset lineage |
| ML (Người2) | Curated/DVC manifest, split SHA, MLflow run metadata, model/preprocessor artifacts và digests, reference version và startup verification |
| RAG (Người3) | SHA manual, document/chunk/prompt/embedding versions, citations, thiếu bằng chứng, contract fixtures |
| Business/React (Người1) | Inbox/projections, realtime UI, auth, Alert audit, History UI; Simulator UI/Control do Người4 |
| MLOps/Integration (Người4) | Sensor/Simulator UI/Storage/Drift; release approval/GitOps pin digest, S3 manifests, Kafka ACL, per-Deployment secrets, Compose/K8s, Drift CronJob, monitoring, E2E/recovery/load evidence |

Bắt đầu bằng source inventory và freeze contract v1.4. Sau khi fixtures được review, các owner có thể triển khai song song. Xem `IMPLEMENTATION_PLAN.md` để biết thứ tự 11 milestones/PR từ 00–10, tiêu chí pass/fail và migration safety.

# 47. v1.4 — Definition of Done (không tự đạt khi viết tài liệu)

- [ ] Đủ workloads theo Deployment Architecture và Simulator UI riêng; entrypoints lấy từ source thật, không có placeholder image digest. MLflow là platform workload.
- [ ] Read API scale độc lập Worker, dùng role `processing_ro`; không quét S3 toàn bộ cho mỗi request.
- [ ] JSON Schemas/Alembic/source thật thống nhất về `sensor.rejected` và tagged `inference.results`.
- [ ] CI truy vết MLflow run + dataset SHA → model artifact SHA → release/deployment → prediction/risk/alert.
- [ ] Migrations của từng owner và consumers tương thích dữ liệu DB/Kafka cũ đã được kiểm thử.
- [ ] Crash tests Kafka ACK/offset/outbox, S3 restore, Business SSE fan-out và RAG failure đạt.
- [ ] Rollback về model digest cũ được duyệt; Alert/RAG citation lịch sử vẫn tra cứu được.
- [ ] Có source/GitOps PR, deploy logs, load metrics và danh sách lỗi tồn đọng; **không tuyên bố production/HA khi chưa có bằng chứng**.
