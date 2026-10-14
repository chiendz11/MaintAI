# MaintAI — Hướng dẫn triển khai và phát triển chi tiết từng module

> **Mục đích:** Developer handbook cho nhóm 5 người triển khai MaintAI – Predictive Maintenance & Technician Copilot.
>
> **Nguồn thiết kế:** `MaintAI_BRD_KienTruc_KeHoach_5_Nguoi.docx` và contract hệ thống `MaintAI_System_Contracts.md`.
>
> **Nguyên tắc:** Tài liệu này **không thay đổi BRD**. Các mục ghi **[BRD bắt buộc]** là invariant phải giữ. Các mục ghi **[Đề xuất triển khai]** là cách tổ chức/code khuyến nghị để nhóm làm nhanh, dễ tích hợp và dễ kiểm thử.
>
> **Update:** bổ sung Drift Monitoring (`drift-worker/`), drift reference/report contract, simulator drift scenarios, PostgreSQL persistence/checkpoint, Prometheus alert rules, Grafana dashboard, Compose one-shot worker, optional Kubernetes CronJob và drift test strategy.

---

# 1. Kiến trúc cần triển khai

MaintAI giữ **5 ownership area theo nhóm 5 người**, nhưng sau khi bổ sung Drift Monitoring có thêm một deployable batch component `drift-worker/` do **Người 2 (ML) + Người 5 (MLOps/Integration)** đồng sở hữu. Drift Worker không làm thay đổi critical path của BRD.

```text
AI4I Dataset
    |
    v
Sensor Simulator
    |
    | POST /api/v1/sensor-readings
    v
+----------------------------------------------------+
| MAINTAI BACKEND                                    |
|                                                    |
| Sensor Ingestion                                   |
|       |                                            |
| Data Quality Validator                             |
|       | VALID                                      |
| Inference Client --------------------------------+ |
+--------------------------------------------------|-+
                                                   |
                                                   v
                                    +----------------------------+
                                    | ML INFERENCE SERVICE       |
                                    | preprocessing -> model     |
                                    | -> prediction/risk score   |
                                    +-------------+--------------+
                                                  |
                                                  v
+----------------------------------------------------+
| MAINTAI BACKEND                                    |
|                                                    |
| Risk Decision Engine                               |
|       |                                            |
| Alert Manager                                      |
|       |---- Audit Logger                           |
|       |---- Feedback Manager                       |
|       +---- RAG Client -------------------------+  |
+-----------------------------------------------|-+--+
                                                |
                                                v
                                 +-----------------------------+
                                 | RAG / LLM SERVICE           |
                                 | query -> retrieve -> prompt |
                                 | -> LLM -> citation validate |
                                 +---------------+-------------+
                                                 |
                                                 v
                                      PostgreSQL + pgvector

React Frontend <------ REST / optional SSE ------ MaintAI Backend

                 MONITORING / BATCH PATH

ML artifact (drift_reference.json) ----+
                                      |
PostgreSQL runtime sensor/prediction --+--> Drift Worker
                                             |
                                             +--> drift_reports/checkpoint
                                             +--> Pushgateway (batch metrics)
                                                       |
                                                       v
                                                   Prometheus
                                                       |
                                                       v
                                                     Grafana

All online services ---- /health + /metrics + logs ----> Prometheus/Grafana
```

## 1.1. Critical path và support path

**[BRD bắt buộc] Critical alert path:**

```text
Sensor -> Validation -> ML -> Risk Decision -> Alert -> UI
```

RAG/LLM **không được nằm trên đường này**.

**Support/enrichment path:**

```text
Alert -> RAG/LLM -> Maintenance Brief/Checklist -> UI
```

Nếu LLM chết, alert vẫn phải được tạo và hiển thị.

**Monitoring/batch path:**

```text
Training reference profile
        +
Runtime VALID sensor/prediction window
        -> Drift Worker
        -> Drift Report
        -> PostgreSQL + Prometheus/Grafana
```

Drift Worker **không** nằm trên critical alert path. `DATA_DRIFT` cũng không đồng nghĩa `INVALID_DATA`: Data Quality Validator quyết định một record có hợp lệ hay không; Drift Worker đánh giá distribution của một window runtime so với reference distribution.

---

# 2. Stack kỹ thuật đề xuất

Đây là lựa chọn triển khai đề xuất, không phải yêu cầu bắt buộc của BRD.

| Khối | Stack đề xuất |
|---|---|
| Sensor Simulator | Python + `httpx`/`requests` + pandas |
| Backend Core | FastAPI + Pydantic + SQLAlchemy + Alembic |
| ML Training | pandas + scikit-learn + imbalanced-learn nếu cần + joblib |
| ML Serving | FastAPI + joblib/scikit-learn + Prometheus client |
| Drift Monitoring | Python batch worker + pandas/numpy/scipy hoặc Evidently tùy chọn + SQLAlchemy/psycopg |
| RAG/LLM | FastAPI + pgvector + embedding model/API + LangChain tùy chọn |
| Frontend | React + TypeScript + Vite + TanStack Query hoặc fetch wrapper |
| Database | PostgreSQL + pgvector |
| Observability | Prometheus + Grafana + structured JSON logs; Pushgateway cho Drift Worker one-shot |
| Local orchestration | Docker Compose |
| Cloud-native extension | Kubernetes/k3d nếu môn học/demo cần |
| CI | GitHub Actions |

Khuyến nghị pin cùng một Python version mà toàn bộ dependency hỗ trợ ổn định; không để mỗi máy dùng một version khác nhau.

---

# 3. Cấu trúc repository đề xuất

```text
maintai/
├── backend/
├── ml-inference/
├── rag-service/
├── frontend/
├── sensor-simulator/
│   └── scenarios/
│       ├── normal.yaml
│       ├── sudden-drift.yaml
│       ├── gradual-drift.yaml
│       └── categorical-drift.yaml
├── drift-worker/
│   ├── app/
│   │   ├── detectors/
│   │   ├── services/
│   │   ├── repositories/
│   │   ├── schemas/
│   │   ├── metrics.py
│   │   └── main.py
│   ├── tests/
│   └── Dockerfile
├── platform/
│   ├── compose/
│   ├── prometheus/
│   │   └── rules/
│   │       └── drift-alerts.yml
│   ├── grafana/
│   │   └── dashboards/
│   │       └── drift-monitoring.json
│   └── k8s/                 # optional, gồm Drift CronJob
├── contracts/
│   ├── README.md
│   ├── sensor-event.schema.json
│   ├── prediction.schema.json
│   ├── alert.schema.json
│   ├── rag-brief.schema.json
│   ├── drift-reference.schema.json
│   └── drift-report.schema.json
├── tests/
│   ├── contract/
│   ├── integration/
│   ├── e2e/
│   ├── failure/
│   └── drift/
├── docs/
│   ├── MaintAI_System_Contracts.md
│   ├── architecture/
│   └── runbooks/
├── .env.example
├── docker-compose.yml
├── Makefile
└── README.md
```

## 3.1. Quy tắc chung

1. Không hard-code URL service trong code.
2. Không commit API key/password vào Git.
3. Mọi service phải có `/health/live`, `/health/ready`, `/metrics` nếu phù hợp contract.
4. Mọi request xuyên service truyền `X-Request-ID` và `X-Correlation-ID`.
5. API payload dùng `snake_case`.
6. Timestamp dùng ISO-8601 có timezone; DB nên lưu UTC.
7. Không được map `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE` thành `NORMAL`.
8. `failure_probability` của ML không đồng nghĩa trực tiếp với `AlertSeverity`.
9. `model_version`, `policy_version`, `document_version`, `detector_version` phải version độc lập.
10. Drift Reference phải gắn đúng `dataset_version` + `model_version`; không silently dùng baseline của model khác.
11. Drift Worker failure không được làm fail Sensor → Validation → ML → Risk → Alert.
12. Drift status không tự động sửa prediction, machine state hoặc alert severity trong MVP.
13. Contract breaking change phải được thống nhất trước khi merge.

---

# 4. Thứ tự boot local toàn hệ thống

Nên khởi động theo thứ tự:

```text
1. PostgreSQL + pgvector
2. ML Inference Service
3. RAG Service
4. MaintAI Backend
5. React Frontend
6. Prometheus + Grafana
7. Sensor Simulator
8. Drift Worker chạy one-shot khi cần / theo scheduler sau khi đã có runtime samples
```

**[Đề xuất port local]**

```text
Frontend      : 5173
Backend       : 8000
ML Service    : 8001
RAG Service   : 8002
PostgreSQL    : 5432
Prometheus    : 9090
Grafana       : 3000
```

Các port này chỉ là convention local; consumer phải đọc từ environment variable.

---

# 5. Module A — Sensor Simulator

**Owner chính:** Người 1 phối hợp Người 5.

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

### `BackendClient`

Gọi:

```http
POST /api/v1/sensor-readings
```

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

# 6. Module B — MaintAI Backend Core

**Owner chính:** Người 1.

Backend là orchestration/business layer. Không nhét model inference hoặc RAG logic nội bộ vào Backend.

## 6.1. Cấu trúc code đề xuất

```text
backend/
├── app/
│   ├── main.py
│   ├── api/
│   │   ├── sensor_routes.py
│   │   ├── machine_routes.py
│   │   ├── alert_routes.py
│   │   └── health_routes.py
│   ├── schemas/
│   │   ├── sensor.py
│   │   ├── prediction.py
│   │   ├── alert.py
│   │   ├── feedback.py
│   │   └── common.py
│   ├── services/
│   │   ├── sensor_ingestion.py
│   │   ├── data_quality.py
│   │   ├── inference_client.py
│   │   ├── risk_engine.py
│   │   ├── alert_manager.py
│   │   ├── audit_service.py
│   │   ├── feedback_service.py
│   │   └── rag_client.py
│   ├── repositories/
│   │   ├── machine_repo.py
│   │   ├── sensor_repo.py
│   │   ├── prediction_repo.py
│   │   ├── alert_repo.py
│   │   └── feedback_repo.py
│   ├── db/
│   │   ├── models.py
│   │   └── session.py
│   ├── observability/
│   │   ├── logging.py
│   │   └── metrics.py
│   └── config.py
├── alembic/
├── tests/
│   ├── unit/
│   ├── contract/
│   └── integration/
├── Dockerfile
└── pyproject.toml
```

## 6.2. Pipeline xử lý `POST /sensor-readings`

Đây là endpoint quan trọng nhất.

```text
HTTP Request
    |
    v
Sensor Ingestion
    |
    v
Data Quality Validator
    |
    +--- INVALID ---> persist validation error ---> return explicit INVALID
    |
    v VALID
persist sensor event
    |
    v
Inference Client -> ML /predict
    |
    +--- timeout/error ---> persist dependency failure ---> UNKNOWN
    |
    v
persist prediction
    |
    v
Risk Decision Engine
    |
    v
update machine latest state
    |
    v
Alert Manager
    |
    +--- no alert ---> response
    |
    +--- create/update alert ---> response to UI path
                                  |
                                  +--> trigger RAG post-alert
```

Backend cần trả phản hồi nhanh cho critical path; không chờ LLM hoàn thành.

---

# 7. Backend submodule B1 — Sensor Ingestion

## 7.1. Trách nhiệm

- Nhận `POST /api/v1/sensor-readings`.
- Parse schema transport-level.
- Tạo/propagate `request_id`, `correlation_id`.
- Kiểm tra idempotency theo `event_id`.
- Gọi Data Quality Validator.
- Không quyết định Failure.

## 7.2. Nên code thế nào

Route mỏng:

```python
@router.post("/sensor-readings")
async def ingest_sensor_event(payload: SensorReadingRequest, ...):
    return await ingestion_service.process(payload)
```

Business flow đặt ở service, không đặt trong route.

## 7.3. Idempotency

Khi nhận lại cùng `event_id`:

- cùng canonical payload -> trả lại result hiện có hoặc `already_processed`;
- cùng `event_id` nhưng payload khác -> conflict/error;
- không chạy lại ML/alert một cách vô thức.

## 7.4. Test

- JSON hợp lệ.
- JSON thiếu field.
- duplicate event.
- request có/không có correlation header.

---

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

Backend gọi ML qua client duy nhất:

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

# 11. Backend submodule B5 — Alert Manager

## 11.1. Trách nhiệm

- tạo alert;
- dedupe/cooldown;
- cập nhật alert hiện tại khi event mới cùng incident;
- ACK;
- mark in progress nếu dùng;
- false alert;
- escalate;
- resolve;
- ghi `alert_event` history;
- không đợi RAG.

## 11.2. State machine đề xuất

```text
OPEN
  |
  +--> ACKNOWLEDGED
  |       |
  |       +--> IN_PROGRESS
  |       |       |
  |       |       +--> RESOLVED
  |       |
  |       +--> ESCALATED
  |
  +--> FALSE_ALERT
```

Không cho transition bất hợp lệ, ví dụ `RESOLVED -> ACKNOWLEDGED` trừ khi product requirement sau này bổ sung reopen.

## 11.3. Dedupe

Một máy có thể gửi nhiều high-risk event liên tiếp. Dedupe theo ít nhất:

```text
machine_id
severity/risk band
unresolved status
time window/cooldown
```

Có thể dùng:

```text
dedupe_key = machine_id + ":failure-risk"
```

Nếu đang có alert OPEN/HIGH cho cùng incident, update `last_seen_at`, `latest_prediction_id`, `occurrence_count` thay vì tạo alert mới.

## 11.4. Endpoint lifecycle

```text
POST /api/v1/alerts/{id}/ack
POST /api/v1/alerts/{id}/feedback
POST /api/v1/alerts/{id}/resolve
```

Nếu cần `escalate`, nên expose endpoint riêng theo contract version.

## 11.5. Test

- create alert.
- duplicate event không spam.
- ACK hợp lệ.
- transition sai bị reject.
- RAG service down nhưng alert vẫn tạo.

---

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

**Cách đơn giản:** Backend tạo alert, commit DB, sau đó gọi RAG ở background task.

**Cách production-like hơn:** publish job/message sang queue. Chỉ làm khi scope cho phép; không cần thêm queue nếu làm tăng độ phức tạp không cần thiết.

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

```text
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

# 19. Module G — PostgreSQL + pgvector

**Owner:** Người 1 sở hữu transactional tables; Người 3 sở hữu knowledge tables; Người 5 hỗ trợ migration/deployment.

## 19.1. Bảng transactional tối thiểu

```text
machines
sensor_events
predictions
alerts
alert_events
feedback
rag_briefs
```

## 19.2. Bảng knowledge

```text
documents
document_chunks
```

## 19.3. Quan hệ

```text
machines 1---N sensor_events
sensor_events 1---0..1 predictions
predictions 1---0..N alerts
alerts 1---N alert_events
alerts 1---0..N feedback
alerts 1---0..1/latest rag_brief

documents 1---N document_chunks
rag_briefs N---N citations/document_chunks   # có thể lưu JSON ở MVP
```

## 19.4. Index đề xuất

```text
sensor_events(machine_id, received_at DESC)
predictions(event_id)
alerts(machine_id, status, created_at DESC)
alert_events(alert_id, timestamp)
document_chunks(document_id, version)
vector index trên embedding khi dữ liệu đủ lớn
```

## 19.5. Migration

Không tạo table bằng `create_all()` tùy hứng trong production/demo stack.

Dùng Alembic:

```text
revision
upgrade
downgrade
```

Migration phải chạy được trên DB rỗng trong CI.

---

# 20. Module H — React Frontend

**Owner chính:** Người 4.

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

**Owner chính:** Người 5.

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

**Owner thuật toán:** Người 2 — ML.  
**Owner runtime/scheduling/observability:** Người 5 — MLOps/Integration.  
**Phối hợp DB:** Người 1.  
**Phối hợp UI Monitoring:** Người 4.

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

Query chỉ các event:

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
 -> query current VALID window
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
| PostgreSQL read failure | ERROR + metric/log | Không ảnh hưởng |
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

# 22. Module J — Docker / Docker Compose

**Owner chính:** Người 5.

## 22.1. Docker image rule

Mỗi deployable có image riêng:

```text
maintai-backend
maintai-ml-inference
maintai-rag-service
maintai-frontend
maintai-sensor-simulator
maintai-drift-worker
```

Không build toàn bộ Python modules thành một container duy nhất nếu mục tiêu là giữ service boundary trong BRD.

## 22.2. Compose service graph

```text
postgres
  ^      ^
  |      |
backend  rag
  ^       ^
  |       |
ml       embedding/llm external or local
  ^
  |
simulator

frontend -> backend
drift-worker -> postgres + model drift_reference artifact
pushgateway <- drift-worker (batch gauges)
prometheus -> backend/ml/rag + pushgateway
grafana -> prometheus (+ optional PostgreSQL datasource for report table)
```

Compose thêm một `pushgateway` service (port mặc định 9091) khi bật profile monitoring. Drift Worker push **latest batch gauges** vào đây; Prometheus scrape Pushgateway như một target bình thường.

## 22.3. Startup dependency

`depends_on` không thay thế readiness.

Service consumer vẫn phải chịu được dependency chưa sẵn sàng.

### Drift Worker trong Compose

Drift Worker không cần chạy như HTTP daemon. Dùng one-shot/profile:

```yaml
drift-worker:
  build:
    context: ../../drift-worker
  environment:
    DATABASE_URL: ${DATABASE_URL}
    DRIFT_CONFIG_PATH: /app/config/drift.yaml
    MODEL_ARTIFACT_DIR: /artifacts/current
    PUSHGATEWAY_URL: http://pushgateway:9091
  depends_on:
    postgres:
      condition: service_healthy
  profiles:
    - monitoring
```

Chạy local:

```bash
docker compose run --rm drift-worker
```

## 22.4. Environment variables

Ví dụ `.env.example`:

```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=maintai
POSTGRES_USER=maintai
POSTGRES_PASSWORD=change-me

BACKEND_PORT=8000
ML_BASE_URL=http://ml-inference:8001
RAG_BASE_URL=http://rag-service:8002

MODEL_DIR=/app/artifacts/current
RISK_POLICY_VERSION=risk-policy-1.0.0

LLM_PROVIDER=...
LLM_API_KEY=
EMBEDDING_PROVIDER=...

PROMETHEUS_ENABLED=true
LOG_LEVEL=INFO

DRIFT_ENABLED=true
DRIFT_CONFIG_PATH=/app/config/drift.yaml
DRIFT_WINDOW_SIZE=500
DRIFT_MINIMUM_SAMPLES=200
DRIFT_PSI_WARNING=0.10
DRIFT_PSI_CRITICAL=0.20
DRIFT_SCHEDULE=*/15 * * * *
PUSHGATEWAY_URL=http://pushgateway:9091
```

Secret thật không commit.

## 22.5. Kubernetes Drift CronJob (optional)

Khi deploy k3d/Kubernetes, dùng `CronJob` thay vì Deployment cho Drift Worker:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: maintai-drift-worker
spec:
  schedule: "*/15 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: drift-worker
              image: maintai/drift-worker:1.0.0
```

Schedule chỉ là config demo. Worker phải chịu được retry và idempotency. Nếu dùng Prometheus batch metrics, cluster cần Pushgateway (hoặc metrics adapter tương đương) sẵn sàng cho CronJob push kết quả trước khi exit.

---

# 23. Module K — CI/CD và integration pipeline

**Owner chính:** Người 5; mỗi owner chịu trách nhiệm test của module mình.

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

# 25. Failure handling matrix khi code

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

# 26. Sequence triển khai thực tế cho từng người

## Người 1 — Backend Core Owner

```text
1. Pydantic schemas/contracts
2. PostgreSQL models
3. Alembic migration
4. Sensor Ingestion
5. Data Quality Validator
6. Persist sensor event
7. Mock Inference Client
8. Risk Decision Engine
9. Alert Manager + state machine
10. Read APIs machines/alerts
11. Real ML integration
12. Audit/correlation
13. Feedback APIs
14. RAG Client
15. Dedupe/cooldown
16. Auth prototype nếu scope cho phép
```

Không chờ ML service xong mới viết Backend; dùng mock contract trước.

## Người 2 — ML Owner

```text
1. EDA
2. Freeze feature list
3. Reproducible split
4. Preprocessing pipeline
5. Baseline
6. Candidate model
7. Class imbalance experiments
8. Validation threshold selection
9. Final test report
10. Export artifact + metadata
11. Build /predict
12. Health/metrics
13. Contract tests
14. Integrate Backend
15. Export drift_reference.json
16. Implement/validate drift detector logic cùng Người 5
17. Drift scenario calibration + evidence
18. Model/version/drift-reference evidence
```

## Người 3 — RAG/LLM Owner

```text
1. Chốt manual metadata schema
2. Parse manual
3. Chunk preserving source
4. Create pgvector schema
5. Embedding ingestion
6. Build retrieval
7. Gold retrieval cases
8. Recall@k evaluation
9. Build /brief contract
10. Prompt/guardrail
11. LLM generation
12. Citation validator
13. Insufficient-evidence path
14. Health/metrics
15. Integrate Backend
```

## Người 4 — Frontend Owner

```text
1. Types từ Backend contract
2. App shell/routing
3. Fleet Dashboard với mock DTO
4. Machine Detail
5. Alert Detail
6. Lifecycle actions
7. RAG brief/citations
8. Error states
9. Monitoring View
10. Thêm Model Health / Drift status panel
11. Connect real Backend
12. Polling/SSE nếu cần
13. UX/error/reconnect tests
```

## Người 5 — MLOps/Integration Owner

```text
1. Repo conventions
2. Dockerfiles skeleton
3. Compose PostgreSQL
4. Compose Backend/ML/RAG/Frontend
5. CI lint/unit/build
6. Contract test harness
7. Prometheus config
8. Grafana provisioning
9. Structured logging convention
10. Integration tests
11. Failure injection suite
12. Secret/config hygiene
13. Load/p95 tests
14. Drift Worker container + one-shot execution
15. Drift report persistence/checkpoint migration phối hợp Người 1
16. Prometheus drift metrics + alert rules
17. Grafana Drift Dashboard
18. Optional Kubernetes CronJob
19. Drift E2E scenarios phối hợp Người 2
20. Full E2E CI
21. Release/demo orchestration
```

Người 5 không chờ tuần cuối.

---

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

# 30. Performance / latency

BRD đặt mục tiêu p95 ≤ 2 giây từ **valid input đến alert hiển thị**, không tính LLM brief.

Vì vậy đo riêng:

```text
T0 Backend receive
T1 validation done
T2 ML request start
T3 ML response
T4 risk decision
T5 alert committed
T6 frontend observed alert
```

Critical latency:

```text
T6 - T0
```

RAG latency là metric riêng:

```text
T_rag_done - T_alert_created
```

Không để gọi LLM nằm trong transaction tạo alert.

Drift computation cũng nằm ngoài p95 critical path. Không chạy PSI/KS trên mỗi `POST /sensor-readings`; drift là batch/window-level monitoring.

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

Đừng đợi đến tuần 8 mới có end-to-end. Bản đầu có thể rất đơn giản:

```text
Simulator
  -> Backend validate
  -> mock/real ML
  -> Risk Engine
  -> Alert DB
  -> Frontend list
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

# 37. Một request hoàn chỉnh phải đi như thế nào

```text
1. Simulator đọc row 123 AI4I.
2. Gán event_id=evt-123, machine_id=M-03, received_at.
3. POST Backend.
4. Backend tạo correlation_id.
5. Validator trả VALID.
6. Backend persist sensor_event.
7. Inference Client POST ML /predict.
8. ML load preprocessor/model artifact đã sẵn sàng.
9. ML trả FAILURE + probability + model_version.
10. Backend persist prediction.
11. Risk Engine áp policy_version và trả HIGH/CRITICAL.
12. Alert Manager tìm dedupe key.
13. Nếu chưa có incident tương ứng -> create alert.
14. Commit alert trước khi RAG.
15. Frontend có thể nhìn thấy alert.
16. Backend/RAG worker gửi alert context sang RAG.
17. Retriever lấy top-k manual chunks.
18. Nếu đủ evidence -> LLM tạo brief/checklist.
19. Citation Validator kiểm tra nguồn.
20. Lưu rag_brief.
21. Frontend Alert Detail hiển thị brief/citation.
22. Technician ACK.
23. Backend append alert_event.
24. Technician feedback/resolve.
25. Audit chain vẫn truy ngược được về event/model/policy/document.
```

Đây là luồng chuẩn mà integration test và demo nên bám theo.

---

## 37.1. Nhánh Drift Monitoring chạy định kỳ

Nhánh này **không nằm trong request ở trên**:

```text
A. ML training export drift_reference.json.
B. Online path tích lũy VALID sensor_events + predictions trong PostgreSQL.
C. Scheduler chạy drift-worker.
D. Worker resolve deployed model_version và load đúng reference profile.
E. Query latest window đủ sample.
F. Compute data drift + prediction drift.
G. Persist drift_report + checkpoint.
H. Update drift metrics.
I. Grafana/Prometheus hiển thị hoặc alert MLOps nếu WARNING/CRITICAL/STALE.
J. Backend/ML/Risk/Alert vẫn vận hành độc lập nếu Drift Worker lỗi.
```

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
