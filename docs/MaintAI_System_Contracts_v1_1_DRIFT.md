# MaintAI System Contracts

> **Vai trò của tài liệu:** Source of Truth cho giao tiếp giữa Sensor Simulator, MaintAI Backend, ML Inference Service, RAG/LLM Service, Drift Worker, React Frontend, PostgreSQL/pgvector và lớp Observability/Platform.
>
> **Baseline:** MaintAI – Predictive Maintenance & Technician Copilot, AI4I 2020, Binary Machine Failure Classification, alert độc lập với LLM, human oversight, traceability end-to-end.
>
> **Contract version:** `1.1.0`
>
> **Thay đổi 1.1.0:** bổ sung Drift Monitoring contract: reference profile, runtime window, Drift Report, persistence/checkpoint, drift metrics/alerts, simulator drift scenarios và failure semantics.

---

## 0. Mục tiêu và nguyên tắc bắt buộc

Tài liệu này nhằm giúp 5 thành viên phát triển độc lập nhưng tích hợp được ngay khi ghép service.

Các nguyên tắc không được phá vỡ:

1. Chỉ sensor event có `validation_status = VALID` mới được đưa vào ML inference.
2. `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE` không bao giờ được chuyển thành `NORMAL`.
3. ML chỉ dự đoán và trả risk/probability; ML Service không tạo alert.
4. Risk Decision Engine của Backend chuyển prediction thành trạng thái/risk level theo `policy_version`.
5. Alert Manager tạo và quản lý vòng đời alert; alert path không phụ thuộc RAG/LLM.
6. RAG/LLM chỉ enrich alert bằng Maintenance Brief/Checklist có citation.
7. Nếu RAG không đủ bằng chứng, phải trả `INSUFFICIENT_EVIDENCE`, không tự bịa hướng dẫn.
8. Technician là người quyết định thao tác bảo trì. MaintAI không điều khiển PLC/actuator.
9. Mọi prediction/alert phải truy ngược được về sensor event, `model_version`, `policy_version` và thao tác người dùng.
10. `machine_id` và timestamp do simulator tạo chỉ phục vụ mô phỏng hệ thống, không được dùng như historical ML features của AI4I.
11. Drift Monitoring chạy ngoài critical request path; Drift Worker lỗi hoặc chậm không được làm gián đoạn Sensor → Validation → ML → Risk → Alert.
12. `INVALID_DATA` và `DATA_DRIFT` là hai khái niệm khác nhau: invalid là lỗi ở record-level; drift là thay đổi phân bố ở window-level. Drift không tự động đổi prediction, severity hay risk policy trong MVP.

---

# 1. Contract Map toàn hệ thống

```text
AI4I Dataset
    |
    v
Sensor Simulator
    |
    | POST /api/v1/sensor-readings
    v
MaintAI Backend
    |
    |---- Data Quality Validator
    |
    | POST /api/v1/predict
    v
ML Inference Service
    |
    | PredictionResponse
    v
MaintAI Backend
    |
    |---- Risk Decision Engine
    |---- Alert Manager
    |---- Audit / Feedback
    |
    | POST /api/v1/briefs
    v
RAG / LLM Service ----> PostgreSQL + pgvector
    |
    | MaintenanceBriefResponse
    v
MaintAI Backend
    |
    | REST + optional SSE
    v
React Frontend

Runtime sensor_events + predictions
    |
    | periodic read
    v
Drift Worker ----> drift_reports / drift_checkpoints
    |
    | push latest batch gauges
    v
Prometheus Pushgateway ----> Prometheus / Alert Rules / Grafana

All online services
    |---- /health/live
    |---- /health/ready
    |---- /metrics
    |---- structured logs
    v
Prometheus / Grafana / Logs
```

## 1.1. Ownership

| Interface | Provider | Consumer | Owner chính |
|---|---|---|---|
| Simulator → Backend | Backend contract | Sensor Simulator | Người 1 + Người 5 |
| Backend → ML | ML Service | Backend | Người 2 + Người 1 |
| Backend → RAG | RAG Service | Backend | Người 3 + Người 1 |
| Backend → Frontend | Backend API | React | Người 1 + Người 4 |
| All services → Observability | Metrics/log contract | Platform | Người 5 |
| ML artifact → Drift Worker | Drift reference profile | Drift Worker | Người 2 + Người 5 |
| PostgreSQL runtime data → Drift Worker | Valid sensor/prediction window | Drift Worker | Người 1 + Người 2 + Người 5 |
| Drift Worker → Observability | Drift metrics/report status | Prometheus/Grafana | Người 5 |
| Sensor Simulator → Drift tests | Drift scenario config | Drift Worker/E2E tests | Người 1 + Người 2 + Người 5 |
| DB schema / migrations | Backend / RAG / Drift | Các service tương ứng | Người 1 + Người 3 + Người 5 |

---

# 2. Quy ước chung

## 2.1. API version

Tất cả public/internal HTTP API dùng prefix:

```text
/api/v1
```

Thay đổi breaking contract phải tăng major version:

```text
/api/v2
```

Không dùng URL version cho model artifact. Model dùng `model_version` riêng.

---

## 2.2. Content type

```http
Content-Type: application/json
Accept: application/json
```

`/metrics` dùng Prometheus text format.

---

## 2.3. Timestamp

Dùng RFC 3339 / ISO-8601 có timezone.

Khuyến nghị lưu nội bộ ở UTC:

```text
2026-10-12T04:30:15.231Z
```

Frontend có thể convert về timezone người dùng.

Không gửi timestamp không timezone như:

```text
2026-10-12 11:30:15
```

---

## 2.4. ID

Các ID phải là opaque string; consumer không được parse ý nghĩa từ ID.

Khuyến nghị:

```text
event_id       evt_<uuid>
prediction_id  pred_<uuid>
alert_id       alt_<uuid>
brief_id       brf_<uuid>
feedback_id    fb_<uuid>
request_id     req_<uuid>
correlation_id cor_<uuid>
```

`machine_id` có thể do simulator cấu hình:

```text
M-001
M-002
M-003
```

---

## 2.5. Correlation / trace headers

Mọi request giữa service phải truyền:

```http
X-Request-ID: req_...
X-Correlation-ID: cor_...
```

Quy tắc:

- Nếu incoming request chưa có `X-Request-ID`, service tạo mới.
- `X-Correlation-ID` được giữ xuyên suốt một flow SensorEvent → Prediction → Alert → RAG.
- Mỗi hop có thể có `request_id` riêng nhưng cùng `correlation_id`.
- Log bắt buộc có hai field này nếu có context.

---

## 2.6. JSON naming

Dùng `snake_case` thống nhất cho API payload.

Đúng:

```json
{"failure_probability": 0.91}
```

Không dùng lẫn:

```json
{"failureProbability": 0.91}
```

---

## 2.7. Null / missing

- Field bắt buộc bị thiếu → validation error.
- Không dùng `null` để thay cho một state có ý nghĩa.
- Dùng enum rõ ràng như `UNKNOWN`, `INVALID`, `UNAVAILABLE`.

---

# 3. Common Enums

## 3.1. DataValidationStatus

```text
VALID
INVALID
```

Không có `NORMAL` ở tầng validation.

---

## 3.2. MachineOperationalState

```text
NORMAL
WATCH
HIGH
CRITICAL
INVALID
UNKNOWN
```

Ý nghĩa:

- `NORMAL`: prediction hợp lệ và risk policy đánh giá bình thường.
- `WATCH`: cần theo dõi.
- `HIGH`: nguy cơ cao, thường cần alert tùy policy.
- `CRITICAL`: ưu tiên kiểm tra cao nhất.
- `INVALID`: dữ liệu đầu vào không hợp lệ.
- `UNKNOWN`: không thể kết luận do dependency/system failure.

---

## 3.3. MLPredictionLabel

```text
NORMAL
FAILURE
```

Đây là output binary classification, không đồng nghĩa trực tiếp với severity alert.

---

## 3.4. AlertSeverity

```text
WATCH
HIGH
CRITICAL
```

Không dùng `NORMAL` làm severity alert.

---

## 3.5. AlertStatus

```text
OPEN
ACKNOWLEDGED
IN_PROGRESS
ESCALATED
RESOLVED
FALSE_ALERT
```

---

## 3.6. GroundedStatus

```text
GROUNDED
INSUFFICIENT_EVIDENCE
VALIDATION_FAILED
UNAVAILABLE
```

---

## 3.7. ServiceDependencyStatus

```text
AVAILABLE
DEGRADED
UNAVAILABLE
```

## 3.8. DriftStatus

```text
STABLE
WARNING
CRITICAL
INSUFFICIENT_DATA
ERROR
```

Ý nghĩa:

- `STABLE`: không có bằng chứng drift vượt policy threshold.
- `WARNING`: ít nhất một feature/prediction distribution vượt ngưỡng warning nhưng chưa đạt policy critical.
- `CRITICAL`: aggregate policy đánh giá drift nghiêm trọng.
- `INSUFFICIENT_DATA`: current window chưa đủ sample để kết luận.
- `ERROR`: worker không tính được report do baseline/config/DB/runtime failure.

`DriftStatus` là trạng thái monitoring, **không phải** `MachineOperationalState` và không được map trực tiếp sang `AlertSeverity`.

---

# 4. Standard Error Contract

Mọi API lỗi business/system phải trả cùng envelope:

```json
{
  "error": {
    "code": "ML_UNAVAILABLE",
    "message": "ML inference service is unavailable",
    "retryable": true,
    "details": {},
    "request_id": "req_123",
    "correlation_id": "cor_123"
  }
}
```

## 4.1. Error codes chuẩn

| Code | HTTP | Retryable | Ý nghĩa |
|---|---:|---|---|
| `INVALID_REQUEST` | 400 | No | JSON/request không hợp lệ |
| `VALIDATION_FAILED` | 422 | No | Sensor data vi phạm schema/range |
| `EVENT_CONFLICT` | 409 | No | Cùng `event_id` nhưng payload khác |
| `MACHINE_NOT_FOUND` | 404 | No | Không có machine |
| `ALERT_NOT_FOUND` | 404 | No | Không có alert |
| `INVALID_ALERT_TRANSITION` | 409 | No | Chuyển state alert không hợp lệ |
| `ML_UNAVAILABLE` | 503 | Yes | ML service không sẵn sàng |
| `ML_TIMEOUT` | 504 | Yes | ML inference timeout |
| `RAG_UNAVAILABLE` | 503 | Yes | RAG unavailable; không ảnh hưởng alert |
| `INSUFFICIENT_EVIDENCE` | 200 | No | RAG không đủ bằng chứng; đây là business result |
| `DRIFT_REFERENCE_NOT_FOUND` | N/A batch | No | Không tìm thấy reference profile tương ứng model/dataset version |
| `DRIFT_INSUFFICIENT_DATA` | N/A batch | Yes later | Runtime window chưa đủ sample để kết luận |
| `DRIFT_COMPUTE_ERROR` | N/A batch | Yes | Worker không thể tính drift report |
| `INTERNAL_ERROR` | 500 | Maybe | Lỗi hệ thống không phân loại |

Không trả stack trace cho Frontend.

---

# 5. Sensor Simulator → Backend Contract

## 5.1. Endpoint

```http
POST /api/v1/sensor-readings
```

## 5.2. SensorReadingRequest

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "received_at": "2026-10-12T04:30:15.231Z",
  "source": "AI4I_SIMULATOR",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

## 5.3. Field contract

| Field | Type | Required | Constraint |
|---|---|---|---|
| `event_id` | string | Yes | Unique/idempotency key |
| `machine_id` | string | Yes | Non-empty |
| `received_at` | RFC3339 datetime | Yes | Simulator event time |
| `source` | string | Yes | MVP = `AI4I_SIMULATOR` |
| `features.product_type` | enum string | Yes | `L`, `M`, `H` |
| `features.air_temperature_k` | number | Yes | finite number |
| `features.process_temperature_k` | number | Yes | finite number |
| `features.rotational_speed_rpm` | number | Yes | finite, > 0 |
| `features.torque_nm` | number | Yes | finite; range policy do validator quản lý |
| `features.tool_wear_min` | number | Yes | finite, >= 0 |

### Cấm gửi làm ML features

Các field sau không được dùng làm model input:

```text
machine_failure
TWF
HDF
PWF
OSF
RNF
machine_id
event_id
received_at
```

Ground-truth label có thể tồn tại trong simulator test harness để đánh giá offline/demo evidence, nhưng không gửi vào `/predict`.

---

## 5.4. Backend response – valid event processed

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "validation_status": "VALID",
  "processing_status": "COMPLETED",
  "machine_state": "HIGH",
  "prediction_id": "pred_001",
  "alert_id": "alt_001",
  "correlation_id": "cor_001"
}
```

`alert_id` có thể là `null` nếu policy không yêu cầu tạo alert.

---

## 5.5. Backend response – invalid event

HTTP `422`:

```json
{
  "event_id": "evt_002",
  "machine_id": "M-001",
  "validation_status": "INVALID",
  "machine_state": "INVALID",
  "errors": [
    {
      "field": "features.rotational_speed_rpm",
      "code": "INVALID_TYPE",
      "message": "rotational_speed_rpm must be numeric"
    }
  ],
  "prediction_id": null,
  "alert_id": null,
  "correlation_id": "cor_002"
}
```

**Invariant:** `validation_status = INVALID` ⇒ không có ML call.

---

## 5.6. Idempotency / duplicate event

`event_id` là idempotency key.

### Cùng event_id + cùng canonical payload

Backend trả record hiện có, không tạo prediction/alert thứ hai.

### Cùng event_id + payload khác

HTTP `409 EVENT_CONFLICT`.

---

# 6. Data Quality Validator Contract

Đây là internal contract trong Backend, không nhất thiết expose HTTP.

## 6.1. Input

```json
{
  "event_id": "evt_001",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

## 6.2. Output

```json
{
  "status": "VALID",
  "validator_version": "sensor-validator-1.0.0",
  "errors": []
}
```

Invalid:

```json
{
  "status": "INVALID",
  "validator_version": "sensor-validator-1.0.0",
  "errors": [
    {
      "field": "features.product_type",
      "code": "UNSUPPORTED_VALUE",
      "message": "product_type must be one of L, M, H"
    }
  ]
}
```

## 6.3. Validation layers

1. JSON/schema validation.
2. Required fields.
3. Type validation.
4. finite/NaN/Inf validation.
5. categorical constraints.
6. domain/range constraints.
7. optional cross-field sanity rules.

Range rules phải config/version hóa, không hard-code rải rác trong controller.

---

# 7. Backend → ML Inference Contract

## 7.1. Endpoint

```http
POST /api/v1/predict
```

ML Service chỉ nhận event đã VALID.

---

## 7.2. PredictionRequest

```json
{
  "event_id": "evt_001",
  "machine_id": "M-001",
  "features": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  }
}
```

Backend gửi **raw validated features**. Encoding/scaling/preprocessing thuộc ML Service để tránh training-serving skew.

---

## 7.3. PredictionResponse

```json
{
  "prediction_id": "pred_001",
  "event_id": "evt_001",
  "prediction": "FAILURE",
  "failure_probability": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "preprocessor_version": "maintai-preprocessor-1.0.0",
  "model_threshold": 0.63,
  "inference_ms": 18,
  "predicted_at": "2026-10-12T04:30:15.420Z"
}
```

## 7.4. Semantics

### `failure_probability`

```text
0.0 <= failure_probability <= 1.0
```

Đây là score chính Backend dùng cho Risk Decision Engine.

### `prediction`

Binary label của model dựa trên threshold đã chọn trong validation ML:

```text
NORMAL
FAILURE
```

### `model_threshold`

Threshold dùng để sinh `prediction`. Nó **không bắt buộc bằng** các threshold severity của product.

Ví dụ:

```text
model_threshold = 0.63
product CRITICAL threshold = 0.85
```

Điều này hoàn toàn hợp lệ.

---

## 7.5. ML Service error behavior

### Request invalid

HTTP `422`.

### Model chưa load / artifact lỗi

HTTP `503`:

```json
{
  "error": {
    "code": "MODEL_NOT_READY",
    "message": "Model artifact is not ready",
    "retryable": true,
    "details": {}
  }
}
```

### Backend timeout

Khuyến nghị timeout MVP:

```text
2 seconds
```

Backend có thể retry tối đa 1 lần nếu lỗi connection/transient; không retry validation error.

Nếu hết retry:

```text
machine_state = UNKNOWN
prediction = none
alert decision = không giả định NORMAL
```

---

# 8. ML Artifact Contract

Một release ML phải chứa ít nhất:

```text
artifacts/
  model.joblib
  preprocessor.joblib
  metadata.json
```

## 8.1. metadata.json

```json
{
  "model_name": "maintai-failure-classifier",
  "model_version": "1.0.0",
  "preprocessor_version": "1.0.0",
  "dataset": "AI4I-2020",
  "target": "machine_failure",
  "features": [
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min"
  ],
  "threshold": 0.63,
  "metrics": {
    "recall_failure": 0.88,
    "precision_failure": 0.55,
    "f1_failure": 0.68,
    "pr_auc": 0.71
  }
}
```

Các số trên chỉ là ví dụ format, không phải metric mặc định của dự án.

## 8.2. Drift Reference Profile Artifact

Mỗi model release dùng cho drift monitoring phải có reference profile tương ứng với **training split** hoặc tập reference đã được chốt, không lấy test set làm baseline drift.

Layout đề xuất:

```text
artifacts/
  model.joblib
  preprocessor.joblib
  metadata.json
  drift_reference.json
```

Contract tối thiểu của `drift_reference.json`:

```json
{
  "schema_version": "1.0.0",
  "dataset_version": "ai4i-v1",
  "model_version": "maintai-ml-1.0.0",
  "created_at": "2026-10-12T10:00:00Z",
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

Invariants:

- `model_version` của reference profile phải khớp model deployment mà report đang đánh giá.
- Feature list phải khớp feature contract của model, không chứa `event_id`, `machine_id`, `received_at`, target hoặc fault labels.
- Bin/category ordering là một phần của contract; Drift Worker không tự ý regenerate bins từ current window.
- Nếu không tìm được profile tương ứng, report phải là `ERROR` với reason `DRIFT_REFERENCE_NOT_FOUND`; không fallback im lặng sang model/version khác.

---

# 9. Risk Decision Engine Contract

Risk Engine là Backend internal component.

## 9.1. Input

```json
{
  "prediction_id": "pred_001",
  "failure_probability": 0.91,
  "prediction": "FAILURE",
  "model_version": "maintai-ml-1.0.0"
}
```

## 9.2. Policy config

Contract schema:

```json
{
  "policy_version": "risk-policy-1.0.0",
  "thresholds": {
    "watch": 0.40,
    "high": 0.65,
    "critical": 0.85
  },
  "create_alert_for": ["HIGH", "CRITICAL"]
}
```

**Lưu ý:** giá trị `0.40/0.65/0.85` chỉ là ví dụ. Nhóm phải chốt bằng evidence từ validation/test; contract chỉ cố định tên field và semantics.

## 9.3. Output

```json
{
  "machine_state": "CRITICAL",
  "severity": "CRITICAL",
  "should_create_alert": true,
  "policy_version": "risk-policy-1.0.0",
  "decision_reason": "failure_probability >= critical threshold"
}
```

## 9.4. Invariants

- Risk Engine không gọi LLM.
- `UNKNOWN` ML state không được map thành `NORMAL`.
- Mọi decision phải lưu `policy_version`.

---

# 10. Alert Manager Contract

## 10.1. CreateAlertCommand

```json
{
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "severity": "CRITICAL",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "correlation_id": "cor_001"
}
```

## 10.2. Alert DTO

```json
{
  "alert_id": "alt_001",
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "severity": "CRITICAL",
  "status": "OPEN",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0",
  "dedupe_key": "M-001:CRITICAL",
  "created_at": "2026-10-12T04:30:15.500Z",
  "updated_at": "2026-10-12T04:30:15.500Z"
}
```

---

## 10.3. Alert state transitions

Allowed transitions:

```text
OPEN
 ├──> ACKNOWLEDGED
 ├──> ESCALATED
 └──> FALSE_ALERT

ACKNOWLEDGED
 ├──> IN_PROGRESS
 ├──> ESCALATED
 ├──> RESOLVED
 └──> FALSE_ALERT

IN_PROGRESS
 ├──> RESOLVED
 ├──> ESCALATED
 └──> FALSE_ALERT

ESCALATED
 ├──> IN_PROGRESS
 └──> RESOLVED

RESOLVED      = terminal
FALSE_ALERT   = terminal
```

Các transition ngoài sơ đồ trả `409 INVALID_ALERT_TRANSITION`.

---

## 10.4. Dedupe / cooldown contract

Contract config:

```json
{
  "dedupe_window_seconds": 300,
  "dedupe_by": ["machine_id", "severity"],
  "active_statuses": ["OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"]
}
```

Nếu cùng `dedupe_key` trong cửa sổ và alert cũ vẫn active:

- Không tạo alert mới.
- Update `updated_at`, `latest_prediction_id`, `latest_risk_score` nếu schema triển khai có các field này.
- Tăng metric `alerts_deduplicated_total`.

---

# 11. Alert Lifecycle APIs — Backend → Frontend

## 11.1. ACK alert

```http
POST /api/v1/alerts/{alert_id}/ack
```

Request:

```json
{
  "actor_id": "tech-001",
  "note": "Đã tiếp nhận cảnh báo"
}
```

Response:

```json
{
  "alert_id": "alt_001",
  "previous_status": "OPEN",
  "status": "ACKNOWLEDGED",
  "updated_at": "2026-10-12T04:32:00Z"
}
```

---

## 11.2. Mark in progress

```http
POST /api/v1/alerts/{alert_id}/start
```

---

## 11.3. Escalate

```http
POST /api/v1/alerts/{alert_id}/escalate
```

Request:

```json
{
  "actor_id": "tech-001",
  "note": "Cần trưởng ca kiểm tra"
}
```

---

## 11.4. Resolve

```http
POST /api/v1/alerts/{alert_id}/resolve
```

```json
{
  "actor_id": "tech-001",
  "resolution": "Inspected simulated equipment; no further action required",
  "note": "Demo resolution"
}
```

---

## 11.5. Mark false alert

```http
POST /api/v1/alerts/{alert_id}/false-alert
```

```json
{
  "actor_id": "tech-001",
  "reason": "Inspection did not confirm abnormal condition",
  "note": "False positive feedback for evaluation"
}
```

---

# 12. Feedback Contract

## 12.1. Endpoint

```http
POST /api/v1/alerts/{alert_id}/feedback
```

## 12.2. Request

```json
{
  "technician_verdict": "FALSE_ALERT",
  "recommendation_feedback": "PARTIALLY_USEFUL",
  "note": "Checklist useful but alert was not confirmed"
}
```

### technician_verdict

```text
CONFIRMED_ISSUE
FALSE_ALERT
INCONCLUSIVE
```

### recommendation_feedback

```text
USEFUL
PARTIALLY_USEFUL
NOT_USEFUL
NOT_AVAILABLE
```

## 12.3. Response

```json
{
  "feedback_id": "fb_001",
  "alert_id": "alt_001",
  "recorded_at": "2026-10-12T04:40:00Z"
}
```

Feedback không tự động trigger retraining trong MVP.

---

# 13. Backend → RAG/LLM Contract

RAG chạy sau khi alert đã tồn tại hoặc khi technician yêu cầu brief.

## 13.1. Endpoint

```http
POST /api/v1/briefs
```

## 13.2. MaintenanceBriefRequest

```json
{
  "alert": {
    "alert_id": "alt_001",
    "machine_id": "M-001",
    "severity": "CRITICAL",
    "status": "OPEN",
    "risk_score": 0.91,
    "policy_version": "risk-policy-1.0.0"
  },
  "prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91,
    "model_version": "maintai-ml-1.0.0"
  },
  "sensor_context": {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108
  },
  "intent": "TECHNICIAN_CHECKLIST"
}
```

### Intent enum

```text
MAINTENANCE_BRIEF
TECHNICIAN_CHECKLIST
ALERT_EXPLANATION
```

---

## 13.3. MaintenanceBriefResponse – grounded

```json
{
  "brief_id": "brf_001",
  "alert_id": "alt_001",
  "grounded_status": "GROUNDED",
  "summary": "Thiết bị có risk score cao. Kết quả ML không khẳng định chắc chắn nguyên nhân hỏng hóc.",
  "uncertainty": "Risk score phản ánh xác suất theo mô hình, cần kiểm tra thực tế.",
  "checklist": [
    {
      "step": 1,
      "action": "Kiểm tra điều kiện vận hành liên quan đến torque theo manual.",
      "citation_ids": ["cit_001"]
    },
    {
      "step": 2,
      "action": "Kiểm tra tool wear theo hướng dẫn của tài liệu.",
      "citation_ids": ["cit_002"]
    }
  ],
  "citations": [
    {
      "citation_id": "cit_001",
      "document_id": "manual-001",
      "document_version": "2.0",
      "section": "4.2",
      "page": 18,
      "chunk_id": "chunk-123"
    },
    {
      "citation_id": "cit_002",
      "document_id": "manual-001",
      "document_version": "2.0",
      "section": "5.1",
      "page": 21,
      "chunk_id": "chunk-145"
    }
  ],
  "retrieval": {
    "top_k": 5,
    "retrieved_chunk_ids": ["chunk-123", "chunk-145", "chunk-180"]
  },
  "generator": {
    "provider": "configured-provider",
    "model": "configured-model"
  },
  "generated_at": "2026-10-12T04:30:16.400Z"
}
```

---

## 13.4. MaintenanceBriefResponse – insufficient evidence

HTTP `200`:

```json
{
  "brief_id": "brf_002",
  "alert_id": "alt_001",
  "grounded_status": "INSUFFICIENT_EVIDENCE",
  "summary": "Không đủ bằng chứng trong maintenance manual hiện có để tạo hướng dẫn kiểm tra có căn cứ.",
  "uncertainty": "Yêu cầu manual review.",
  "checklist": [],
  "citations": [],
  "retrieval": {
    "top_k": 5,
    "retrieved_chunk_ids": []
  }
}
```

`INSUFFICIENT_EVIDENCE` là kết quả nghiệp vụ hợp lệ, không phải HTTP 5xx.

---

## 13.5. RAG unavailable

Backend vẫn giữ alert bình thường.

Frontend nhận trạng thái:

```json
{
  "copilot_status": "UNAVAILABLE",
  "message": "Maintenance brief is temporarily unavailable. Alert remains active."
}
```

---

# 14. Knowledge Base / pgvector Contract

## 14.1. Document metadata

```json
{
  "document_id": "manual-001",
  "title": "Maintenance Manual - Sample Equipment",
  "version": "2.0",
  "document_type": "MAINTENANCE_MANUAL",
  "language": "en",
  "effective_from": "2026-01-01",
  "source_file": "manual_v2.pdf"
}
```

## 14.2. DocumentChunk

```json
{
  "chunk_id": "chunk-123",
  "document_id": "manual-001",
  "document_version": "2.0",
  "section": "4.2",
  "page": 18,
  "text": "...",
  "safety_notes": [],
  "embedding_model": "configured-embedding-model"
}
```

## 14.3. Citation rule

Mọi actionable checklist item phải có ít nhất một `citation_id` trỏ tới chunk thực sự được retriever trả về.

Citation Validator phải reject citation không tồn tại trong retrieval context.

---

# 15. Backend → Frontend Read APIs

Frontend chỉ gọi Backend, không gọi trực tiếp ML/RAG DB trong MVP.

---

## 15.1. Fleet list

```http
GET /api/v1/machines
```

Response:

```json
{
  "items": [
    {
      "machine_id": "M-001",
      "display_name": "Machine 001",
      "state": "CRITICAL",
      "latest_risk_score": 0.91,
      "active_alert_count": 1,
      "last_seen_at": "2026-10-12T04:30:15Z",
      "data_status": "VALID"
    }
  ],
  "total": 1
}
```

---

## 15.2. Machine detail

```http
GET /api/v1/machines/{machine_id}
```

```json
{
  "machine_id": "M-001",
  "display_name": "Machine 001",
  "state": "CRITICAL",
  "data_status": "VALID",
  "last_seen_at": "2026-10-12T04:30:15Z",
  "latest_sensor": {
    "event_id": "evt_001",
    "features": {
      "product_type": "M",
      "air_temperature_k": 298.1,
      "process_temperature_k": 308.6,
      "rotational_speed_rpm": 1551,
      "torque_nm": 42.8,
      "tool_wear_min": 108
    }
  },
  "latest_prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91,
    "model_version": "maintai-ml-1.0.0"
  },
  "active_alerts": ["alt_001"]
}
```

---

## 15.3. Sensor history

```http
GET /api/v1/machines/{machine_id}/sensor-readings?limit=100&before=<timestamp>
```

Response phải sort mới nhất trước hoặc ghi rõ sort contract.

Khuyến nghị:

```text
sort = received_at DESC
```

---

## 15.4. Alert list

```http
GET /api/v1/alerts?status=OPEN&severity=CRITICAL&machine_id=M-001
```

```json
{
  "items": [
    {
      "alert_id": "alt_001",
      "machine_id": "M-001",
      "severity": "CRITICAL",
      "status": "OPEN",
      "risk_score": 0.91,
      "created_at": "2026-10-12T04:30:15Z"
    }
  ],
  "total": 1
}
```

---

## 15.5. Alert detail

```http
GET /api/v1/alerts/{alert_id}
```

```json
{
  "alert": {
    "alert_id": "alt_001",
    "machine_id": "M-001",
    "severity": "CRITICAL",
    "status": "OPEN",
    "risk_score": 0.91,
    "model_version": "maintai-ml-1.0.0",
    "policy_version": "risk-policy-1.0.0",
    "created_at": "2026-10-12T04:30:15Z"
  },
  "sensor_event": {
    "event_id": "evt_001",
    "validation_status": "VALID",
    "features": {}
  },
  "prediction": {
    "prediction_id": "pred_001",
    "prediction": "FAILURE",
    "failure_probability": 0.91
  },
  "copilot": {
    "status": "AVAILABLE",
    "brief_id": "brf_001",
    "grounded_status": "GROUNDED"
  },
  "history": []
}
```

---

# 16. Optional Realtime Contract – SSE

Nếu cần realtime dashboard, ưu tiên SSE trước WebSocket vì use case chủ yếu server → UI.

## 16.1. Endpoint

```http
GET /api/v1/events/stream
Accept: text/event-stream
```

## 16.2. Event types

```text
machine.updated
alert.created
alert.updated
brief.ready
service.degraded
```

Ví dụ:

```text
event: alert.created
id: evtstream_001
data: {"alert_id":"alt_001","machine_id":"M-001","severity":"CRITICAL","status":"OPEN"}
```

Nếu nhóm không cần realtime trong MVP, polling REST là chấp nhận được; không để SSE làm blocker.

---

# 17. Database Contract tối thiểu

Tên bảng có thể thay đổi trong implementation, nhưng semantic fields phải tương đương.

## 17.1. `machines`

```text
machine_id PK
display_name
state
last_seen_at
created_at
updated_at
```

## 17.2. `sensor_events`

```text
event_id PK
machine_id FK
received_at
source
features_json
validation_status
validation_errors_json
validator_version
correlation_id
created_at
```

## 17.3. `predictions`

```text
prediction_id PK
event_id FK UNIQUE
label
failure_probability
model_version
preprocessor_version
model_threshold
inference_ms
predicted_at
correlation_id
```

## 17.4. `alerts`

```text
alert_id PK
machine_id FK
prediction_id FK
severity
status
risk_score
policy_version
dedupe_key
created_at
updated_at
```

## 17.5. `alert_events`

```text
alert_event_id PK
alert_id FK
event_type
from_status
to_status
actor_id
note
timestamp
```

## 17.6. `feedback`

```text
feedback_id PK
alert_id FK
technician_verdict
recommendation_feedback
note
created_at
```

## 17.7. `documents`

```text
document_id PK
version
title
document_type
language
effective_from
source_file
created_at
```

Khuyến nghị unique:

```text
(document_id, version)
```

## 17.8. `document_chunks`

```text
chunk_id PK
document_id
document_version
section
page
text
safety_notes_json
embedding vector
embedding_model
```

## 17.9. `rag_briefs`

```text
brief_id PK
alert_id FK
grounded_status
summary
uncertainty
checklist_json
citations_json
retrieved_chunk_ids_json
generator_provider
generator_model
generated_at
```

## 17.10. `drift_reports`

```text
id PK
report_id UNIQUE
computed_at
detector_version
reference_dataset_version
model_version
window_start
window_end
sample_count
overall_status
drifted_feature_count
warning_feature_count
critical_feature_count
report_json JSONB
created_at
```

`report_json` là source-of-truth chi tiết cho feature scores, thresholds, reference/current summaries và prediction drift; không cần tạo một bảng con cho từng feature ở MVP.

## 17.11. `drift_checkpoints`

```text
worker_name PK
last_processed_event_at
last_processed_event_id
last_success_at
last_report_id
updated_at
```

Checkpoint chỉ phục vụ scheduling/idempotency của Drift Worker; không được dùng như ML feature.

---

# 18. Traceability Contract

Một `alert_id` phải truy ra được chain:

```text
alert_id
  -> prediction_id
      -> event_id
          -> machine_id
          -> validated features
      -> model_version
      -> preprocessor_version
  -> policy_version
  -> alert_events
  -> feedback
  -> rag_brief
      -> document_id + version + chunk_id + page/section
```

Integration test phải chứng minh được chain này.

Drift report phải truy vết được ít nhất:

```text
report_id
  -> detector_version
  -> model_version
  -> reference_dataset_version
  -> reference profile schema/version
  -> runtime window_start/window_end/sample_count
  -> feature scores + aggregate policy
```

---

# 19. Health Contract

Mỗi service Backend / ML / RAG phải expose:

## 19.1. Liveness

```http
GET /health/live
```

HTTP `200` nếu process sống.

```json
{
  "status": "UP",
  "service": "ml-inference",
  "version": "1.0.0"
}
```

Liveness không nên phụ thuộc DB/LLM provider ngoài nếu điều đó gây restart loop.

---

## 19.2. Readiness

```http
GET /health/ready
```

Ví dụ ML:

```json
{
  "status": "UP",
  "checks": {
    "model_loaded": true
  }
}
```

Ví dụ RAG:

```json
{
  "status": "DEGRADED",
  "checks": {
    "database": true,
    "vector_store": true,
    "llm_provider": false
  }
}
```

---

# 20. Metrics Contract

Mỗi service expose:

```http
GET /metrics
```

## 20.1. Backend metrics

```text
maintai_http_requests_total{route,method,status}
maintai_http_request_duration_seconds{route,method}
maintai_sensor_events_total{validation_status}
maintai_invalid_sensor_events_total{reason}
maintai_alerts_created_total{severity}
maintai_alerts_deduplicated_total{severity}
maintai_alert_transitions_total{from_status,to_status}
maintai_dependency_errors_total{dependency,error_type}
```

## 20.2. ML metrics

```text
maintai_ml_inference_requests_total{status}
maintai_ml_inference_duration_seconds
maintai_ml_predictions_total{prediction}
maintai_ml_failure_probability_bucket
maintai_ml_model_info{model_version}
```

## 20.3. RAG metrics

```text
maintai_rag_requests_total{status,grounded_status}
maintai_rag_request_duration_seconds
maintai_rag_retrieval_duration_seconds
maintai_rag_retrieved_chunks_count
maintai_rag_citation_validation_failures_total
```

## 20.4. Drift metrics

Drift Worker là batch/one-shot process. Vì process exit sau mỗi run nên Prometheus không thể scrape trực tiếp một endpoint đã biến mất. **Pattern mặc định cho prototype:** worker push latest gauges vào Prometheus Pushgateway; Prometheus scrape Pushgateway. Nếu nhóm không dùng Pushgateway, phải có một long-lived metrics adapter tương đương đọc latest `drift_report`; không giả định Prometheus scrape được process one-shot.

Metric contract tối thiểu:

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

Quy ước gauge `maintai_drift_feature_status` nếu cần số hóa:

```text
STABLE=0
WARNING=1
CRITICAL=2
INSUFFICIENT_DATA=3
ERROR=4
```

Không dùng `report_id`, `event_id`, `machine_id`, timestamp riêng lẻ làm Prometheus labels.

## 20.5. Label cardinality rules

Không dùng các giá trị có cardinality cao làm Prometheus label:

```text
event_id
alert_id
machine free-text
request_id
user note
```

Các ID này để trong log, không để trong metric label.

---

# 21. Structured Logging Contract

JSON log khuyến nghị:

```json
{
  "timestamp": "2026-10-12T04:30:15.500Z",
  "level": "INFO",
  "service": "maintai-backend",
  "event": "alert_created",
  "request_id": "req_001",
  "correlation_id": "cor_001",
  "machine_id": "M-001",
  "event_id": "evt_001",
  "prediction_id": "pred_001",
  "alert_id": "alt_001",
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0"
}
```

Không log API key, secret, credential hoặc full prompt chứa thông tin nhạy cảm nếu sau này dùng dữ liệu thật.

Drift Worker log tối thiểu nên có:

```json
{
  "timestamp": "2026-10-12T15:00:00Z",
  "level": "INFO",
  "service": "drift-worker",
  "event": "drift_report_computed",
  "report_id": "drift-20261012-001",
  "model_version": "maintai-ml-1.0.0",
  "reference_dataset_version": "ai4i-v1",
  "window_start": "2026-10-12T13:00:00Z",
  "window_end": "2026-10-12T15:00:00Z",
  "sample_count": 500,
  "overall_status": "CRITICAL"
}
```

---

# 22. Security / Auth Contract cho MVP

Nếu triển khai auth prototype:

## Roles

```text
TECHNICIAN
ADMIN
VIEWER
```

## Quyền tối thiểu

| Action | VIEWER | TECHNICIAN | ADMIN |
|---|---:|---:|---:|
| Xem fleet/machine | Yes | Yes | Yes |
| Xem alert | Yes | Yes | Yes |
| ACK/start/resolve alert | No | Yes | Yes |
| Gửi feedback | No | Yes | Yes |
| Thay risk policy | No | No | Yes |
| Quản trị service/config | No | No | Yes |

Service-to-service credential/API key phải qua env/secret store, không commit vào Git.

---

# 23. Fail-safe Contract Matrix

| Tình huống | System state | Có gọi ML? | Có alert mới? | RAG | UI phải hiển thị |
|---|---|---:|---|---|---|
| Sensor valid, ML normal | NORMAL/WATCH theo policy | Yes | Theo policy | Optional | Prediction + state |
| Sensor invalid | INVALID | No | No | No | Invalid + lỗi validation |
| ML timeout | UNKNOWN | Attempted | Không suy đoán Normal | No | ML unavailable / Unknown |
| ML 503 | UNKNOWN | Attempted | Không suy đoán Normal | No | Dependency unavailable |
| High/Critical prediction, RAG OK | HIGH/CRITICAL | Yes | Yes | Yes | Alert + brief/checklist |
| High/Critical prediction, RAG down | HIGH/CRITICAL | Yes | Yes | Unavailable | Alert vẫn hiển thị; brief unavailable |
| RAG insufficient evidence | HIGH/CRITICAL | Yes | Yes | No grounded advice | Alert + manual review message |
| Duplicate event identical | Existing state | No second inference | No duplicate | No duplicate | Existing result |
| Duplicate alert condition | HIGH/CRITICAL | Yes | Dedupe/update | Optional | Một active alert |
| Drift window chưa đủ sample | Monitoring = INSUFFICIENT_DATA | Không ảnh hưởng | Không ảnh hưởng | Không ảnh hưởng | Monitoring báo chưa đủ dữ liệu |
| Drift Worker lỗi/DB read lỗi | Monitoring = ERROR/STALE | Không ảnh hưởng | Không ảnh hưởng | Không ảnh hưởng | Online path vẫn chạy; platform alert worker stale/error |
| Drift = WARNING/CRITICAL | Online state giữ nguyên theo prediction/policy | Có như bình thường | Theo risk policy | Theo alert path | Monitoring hiển thị drift riêng, không đổi machine severity tự động |
| DB write critical failure | DEGRADED/ERROR | Depends stage | Không claim success | No | Explicit system error |

---

# 24. Frontend State Contract

Frontend không tự suy diễn state từ màu hoặc probability. Backend trả state rõ ràng.

## Required UI states

```text
LOADING
NORMAL
WATCH
HIGH
CRITICAL
INVALID
UNKNOWN
ERROR
EMPTY
```

Ví dụ:

- `INVALID`: dữ liệu không hợp lệ.
- `UNKNOWN`: ML/system không thể kết luận.
- `ERROR`: request UI/API lỗi.

`UNKNOWN` tuyệt đối không render bằng style/label của `NORMAL`.

---

# 25. Contract Versioning Rules

## 25.1. API contract semantic version

```text
1.0.0
```

- PATCH: sửa doc/field optional không phá consumer.
- MINOR: thêm endpoint/field optional/enum chỉ khi consumer forward-compatible.
- MAJOR: rename/remove/đổi semantics field, enum breaking, endpoint breaking.

## 25.2. Model version độc lập

```text
maintai-ml-1.0.0
maintai-ml-1.1.0
```

Không đồng nhất model version với API version.

## 25.3. Risk policy version

```text
risk-policy-1.0.0
```

Mỗi alert lưu policy version đã dùng.

## 25.4. Knowledge document version

Citation bắt buộc có:

```text
document_id
document_version
section/page/chunk_id
```

---

# 25A. Drift Monitoring Contract

Drift Monitoring là control thuộc ML/MLOps monitoring, chạy ngoài critical alert path. Contract này định nghĩa dữ liệu vào, baseline, output report, persistence, scheduling và observability.

## 25A.1. Phạm vi

MVP theo dõi:

```text
Data Drift
Prediction Drift
Data Quality Trend (supporting signal)
```

Performance drift chỉ được kết luận khi có delayed ground truth đáng tin cậy. Feedback của technician có thể dùng cho evaluation về sau nhưng Drift Worker **không tự retrain** model trong MVP.

## 25A.2. Current Window Input

Drift Worker chỉ đọc sensor events có:

```text
validation_status = VALID
```

Feature contract:

```text
product_type
air_temperature_k
process_temperature_k
rotational_speed_rpm
torque_nm
tool_wear_min
```

Không đưa vào feature drift:

```text
event_id
machine_id
received_at
Machine failure
TWF/HDF/PWF/OSF/RNF
```

Window policy là config, ví dụ:

```yaml
drift:
  window_size: 500
  minimum_samples: 200
```

Nếu `sample_count < minimum_samples`, worker phải ghi report `INSUFFICIENT_DATA`, không phát kết luận WARNING/CRITICAL.

## 25A.3. Detector / Policy Config

Detector implementation có thể thay đổi miễn giữ report contract. Initial policy đề xuất:

```yaml
drift:
  detector_version: drift-worker-1.0.0
  numeric_metric: PSI
  categorical_metric: PSI
  psi:
    warning: 0.10
    critical: 0.20
  aggregation:
    critical_if_critical_features_gte: 2
    warning_if_warning_features_gte: 1
```

Các threshold trên là project policy ban đầu, phải version hóa/calibrate bằng scenario test; không xem là chân lý thống kê cố định.

## 25A.4. DriftReport

Canonical JSON contract:

```json
{
  "schema_version": "1.0.0",
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
      "feature_type": "numeric",
      "metric": "PSI",
      "score": 0.27,
      "warning_threshold": 0.10,
      "critical_threshold": 0.20,
      "status": "CRITICAL"
    }
  ],
  "prediction_drift": {
    "reference_failure_rate": 0.034,
    "current_failure_rate": 0.112
  },
  "reason": null
}
```

Rules:

- `overall_status` phải thuộc `DriftStatus`.
- Mỗi feature result phải mang metric + score + threshold đã dùng để audit được.
- Report phải gắn đúng `model_version` + `reference_dataset_version`.
- Không có baseline đúng version → `ERROR`, không tự chọn baseline gần nhất.
- Một report không được cập nhật machine state hay alert severity.

## 25A.5. Simulator Drift Scenario Contract

Scenario file machine-readable đề xuất:

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

Scenario transforms phải tạo **valid-but-shifted data**; scenario invalid-data nằm ở fault-injection contract khác.

Scenario tối thiểu:

```text
normal
sudden-drift
gradual-drift
categorical-drift
```

## 25A.6. Batch Execution Contract

Một run Drift Worker:

```text
load checkpoint
 -> resolve deployed model version
 -> load matching drift_reference.json
 -> query current VALID window
 -> enforce minimum_samples
 -> compute feature drift
 -> compute prediction drift
 -> aggregate status
 -> persist drift_report
 -> push/update latest gauges to Pushgateway (hoặc metrics adapter tương đương)
 -> update checkpoint
 -> exit
```

Worker phải idempotent theo window/report identity hợp lý để retry không tạo report duplicate vô hạn.

## 25A.7. Scheduling Contract

Local/Compose: worker chạy one-shot bằng command; Pushgateway là long-lived metrics target cho batch job, ví dụ:

```text
docker compose run --rm drift-worker
```

Metric grouping key không chứa `report_id`/`event_id`; dùng key ổn định như `job=maintai-drift-worker` và optional `environment=demo`. Mỗi run cập nhật latest gauges thay vì tạo time series mới theo report.

Kubernetes optional deployment dùng `CronJob`, không phải request-path Deployment. Schedule là environment config; ví dụ demo `*/15 * * * *`.

## 25A.8. Monitoring Alert Contract

Drift alert là **platform/MLOps alert**, không phải maintenance alert.

Prometheus alert rule tối thiểu nên cover:

```text
DataDriftWarning
DataDriftCritical
DriftWorkerStale
DriftWorkerError
```

Alert rule nên dùng persistence (`for:` hoặc nhiều run liên tiếp) để giảm noise.

---

# 26. Contract Tests bắt buộc

## CT-01 — Valid SensorEvent

Given valid payload → Backend gọi ML đúng một lần.

## CT-02 — Invalid SensorEvent

Given missing/wrong type → Backend trả INVALID và ML mock phải nhận zero call.

## CT-03 — Backend ↔ ML response

ML mock trả đủ:

```text
prediction
failure_probability
model_version
```

Backend parse thành công.

## CT-04 — ML timeout

Backend phải tạo explicit `UNKNOWN`/error state, không `NORMAL`.

## CT-05 — Prediction → risk

Given score và policy fixture → Risk Engine trả đúng level + `policy_version`.

## CT-06 — Alert independent from RAG

RAG mock = 503, high/critical prediction vẫn tạo alert.

## CT-07 — Alert lifecycle

OPEN → ACKNOWLEDGED → IN_PROGRESS → RESOLVED thành công.

Invalid transition phải 409.

## CT-08 — Dedupe

Nhiều high-risk event cùng machine/severity trong window → một active alert.

## CT-09 — Grounded RAG

Checklist actionable item phải reference citation có trong retrieved chunks.

## CT-10 — Insufficient evidence

Không có chunk đủ điều kiện → `INSUFFICIENT_EVIDENCE`, checklist rỗng hoặc manual-review only.

## CT-11 — Frontend DTO

Frontend test fixture phải deserialize đúng Machine/Alert/Brief DTO.

## CT-12 — Traceability

Từ `alert_id` truy ra được event, prediction, model version, policy version và citations.

## CT-13 — Drift reference/version match

Given deployed `model_version=maintai-ml-1.0.0` → worker chỉ load reference profile có cùng model version; mismatch phải fail explicit.

## CT-14 — No-drift scenario

Reference-like runtime window đủ sample → report `STABLE`.

## CT-15 — Drift scenario

Gradual/sudden/categorical drift fixture → expected feature score/status và aggregate `WARNING` hoặc `CRITICAL` theo policy fixture.

## CT-16 — Insufficient drift samples

`sample_count < minimum_samples` → `INSUFFICIENT_DATA`; không emit critical drift conclusion.

## CT-17 — Drift isolation

Drift Worker down/error → Backend/ML/Risk/Alert critical path vẫn pass integration test.

---

# 27. End-to-End Example

## Step 1 — Simulator

```http
POST /api/v1/sensor-readings
```

```json
{
  "event_id": "evt_demo_001",
  "machine_id": "M-003",
  "received_at": "2026-10-12T04:30:15Z",
  "source": "AI4I_SIMULATOR",
  "features": {
    "product_type": "H",
    "air_temperature_k": 300.0,
    "process_temperature_k": 310.0,
    "rotational_speed_rpm": 1300,
    "torque_nm": 62.0,
    "tool_wear_min": 210
  }
}
```

## Step 2 — Validator

```json
{
  "status": "VALID",
  "validator_version": "sensor-validator-1.0.0",
  "errors": []
}
```

## Step 3 — ML

```json
{
  "prediction_id": "pred_demo_001",
  "event_id": "evt_demo_001",
  "prediction": "FAILURE",
  "failure_probability": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "preprocessor_version": "maintai-preprocessor-1.0.0",
  "model_threshold": 0.63,
  "inference_ms": 18,
  "predicted_at": "2026-10-12T04:30:15.200Z"
}
```

## Step 4 — Risk Decision

```json
{
  "machine_state": "CRITICAL",
  "severity": "CRITICAL",
  "should_create_alert": true,
  "policy_version": "risk-policy-1.0.0",
  "decision_reason": "failure_probability >= critical threshold"
}
```

## Step 5 — Alert

```json
{
  "alert_id": "alt_demo_001",
  "machine_id": "M-003",
  "prediction_id": "pred_demo_001",
  "severity": "CRITICAL",
  "status": "OPEN",
  "risk_score": 0.91,
  "model_version": "maintai-ml-1.0.0",
  "policy_version": "risk-policy-1.0.0"
}
```

Từ thời điểm này alert đã hợp lệ và phải hiển thị được, kể cả RAG chết.

## Step 6 — RAG

```json
{
  "brief_id": "brf_demo_001",
  "alert_id": "alt_demo_001",
  "grounded_status": "GROUNDED",
  "summary": "Risk score cao; cần kiểm tra thực tế trước khi kết luận nguyên nhân.",
  "checklist": [
    {
      "step": 1,
      "action": "Kiểm tra hạng mục theo maintenance manual.",
      "citation_ids": ["cit_001"]
    }
  ],
  "citations": [
    {
      "citation_id": "cit_001",
      "document_id": "manual-001",
      "document_version": "2.0",
      "section": "4.2",
      "page": 18,
      "chunk_id": "chunk-123"
    }
  ]
}
```

## Step 7 — Technician

```text
OPEN
 -> ACKNOWLEDGED
 -> IN_PROGRESS
 -> RESOLVED
```

Mỗi action sinh `alert_event` và giữ cùng trace chain.

## Step 8 — Drift Monitoring (periodic, ngoài request path)

Sau khi runtime đã tích lũy đủ sample:

```text
Drift Worker
 -> load drift_reference.json của maintai-ml-1.0.0
 -> đọc latest VALID sensor_events/predictions
 -> tính feature/prediction drift
 -> persist drift_report
 -> update Prometheus metrics
```

Ví dụ kết quả:

```json
{
  "report_id": "drift_demo_001",
  "model_version": "maintai-ml-1.0.0",
  "sample_count": 500,
  "overall_status": "WARNING"
}
```

Kết quả này không sửa `alt_demo_001` và không đổi machine severity.

---

# 28. Repo / Contract File Layout đề xuất

```text
maintai/
├── backend/
├── ml-inference/
├── rag-service/
├── frontend/
├── sensor-simulator/
├── drift-worker/
├── contracts/
├── platform/
└── docs/
    └── contracts/
        └── MaintAI_System_Contracts.md
```

Sau khi API ổn định hơn có thể tách machine-readable contracts:

```text
contracts/
├── openapi-backend.yaml
├── openapi-ml.yaml
├── openapi-rag.yaml
├── schemas/
│   ├── sensor-event.schema.json
│   ├── prediction.schema.json
│   ├── alert.schema.json
│   ├── maintenance-brief.schema.json
│   ├── drift-reference.schema.json
│   └── drift-report.schema.json
└── examples/
```

Nhưng **file Markdown hiện tại là contract baseline duy nhất** cho giai đoạn đầu.

---

# 29. Definition of Done cho Contract v1

Contract v1 được xem là frozen khi:

- [ ] SensorEvent field/type/unit đã chốt.
- [ ] Không có target leakage fields trong ML request.
- [ ] Backend → ML request/response có mock contract test.
- [ ] ML error/timeout semantics đã chốt.
- [ ] Risk policy schema có `policy_version`.
- [ ] Alert states/transitions đã chốt.
- [ ] Dedupe rule có fixture test.
- [ ] Backend → RAG request/response có `grounded_status` + citations.
- [ ] `INSUFFICIENT_EVIDENCE` path có test.
- [ ] Backend → Frontend Machine/Alert DTO đã chốt.
- [ ] `INVALID`/`UNKNOWN` được Frontend hiển thị khác `NORMAL`.
- [ ] `/health/live`, `/health/ready`, `/metrics` tồn tại ở service chính.
- [ ] Log có `request_id` và `correlation_id`.
- [ ] `alert_id` truy vết được tới event/model/policy/RAG citations.
- [ ] Drift reference profile gắn đúng `model_version`/`dataset_version`.
- [ ] Drift Report schema + `DriftStatus` + insufficient-data/error semantics đã chốt.
- [ ] `drift_reports` + `drift_checkpoints` schema có migration/contract test.
- [ ] Drift metrics không dùng high-cardinality labels.
- [ ] Drift Worker failure không làm fail online alert path.
- [ ] Secrets không nằm trong contract payload hoặc source code.
- [ ] Người 1–5 cùng review và tag contract version `1.1.0`.

---

# 30. Tóm tắt contract cần freeze đầu tiên

Nếu nhóm chưa thể freeze toàn bộ ngay, phải freeze 5 contract này trước khi code sâu:

```text
1. SensorEvent
   Simulator -> Backend

2. PredictionRequest / PredictionResponse
   Backend <-> ML

3. RiskDecision + Alert DTO + Alert lifecycle
   Backend internal + Frontend

4. MaintenanceBriefRequest / Response + Citation
   Backend <-> RAG

5. Health / Metrics / Correlation logging
   All services -> Platform

6. Drift Reference + Drift Report
   ML artifact / Runtime DB -> Drift Worker -> Platform
```

Năm contract đầu tiên vẫn là prerequisite cho critical path. Drift contract có thể freeze song song trong giai đoạn ML/observability, nhưng phải ổn định trước milestone Observable & Resilient.
