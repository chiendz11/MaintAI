> **Source update23/10 — SDK0.2.0/bundle1.1.0:** keyed runner/coordinator,
> durable handler/DB fence, ordered relay, migration002 và transport wrappers đã
> có source/tests. Full app callbacks/probes/images/deployment còn cần tích hợp.
> [Implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) và
> readiness §19 ghi bằng chứng, commands và giới hạn; các snapshot cũ đọc theo ngày.

# OBS-01 — Metrics và bàn giao Grafana cho MaintAI

**Revision:** `1.0.0`, quyết định ngày2026-10-20; bổ sung System Contracts v1.4
§20/§41. Đây là đặc tả implementation cần áp dụng, không phải xác nhận các
metrics/exporters/dashboards đã chạy. Không đổi HTTP/Kafka/DB schema bằng Markdown.

Đọc cùng [MLflow integration](MLFLOW_INTEGRATION_GUIDE.md),
[UI Architecture](UI_ARCHITECTURE.md), [readiness](implementation-readiness.md).
Catalog trong §3 là tên/type/labels chuẩn. Các tên ngắn trong Guide cũ là aliases
tham khảo đã bị thay thế; không tự phát hành thêm một họ metrics khác.

**Bổ sung23/10:** PROC-ORDER-01/1.0.0 ủy quyền catalog extension §11 cho keyed
Processing/ordered relay; SDK collectors đã implement, production panels/scrape còn pending. Catalog gốc
OBS-01/1.0.0 và extension có identity riêng, không tự tăng wire versions.

## 1. Repo, owner và đường đi dữ liệu

| Phần việc | Source/repo | Người chịu trách nhiệm |
| --- | --- | --- |
| REST/ingest, DQ/ML client/risk, outbox, projections/alerts/history/SSE metrics | App: ingestion-api/, processing/, backend/ | Người1 |
| Inference metrics, model info, training params/evaluation/lineage | App: ml-service/ | Người2 |
| MLflow Server image/base/dependency pins, local dev, signed image publication | App: ml-service/mlflow/ | Người2 + Người4 |
| MLflow production deployment/config/auth, DB/artifact-store access | MaintAI-gitops | Người4 |
| Retrieval/generation/grounding/citation metrics | App: rag-service/ | Người3 |
| Sensor, Drift publication, Storage metrics | App: sensor/, drift-worker/, storage-consumer/ | Người4 |
| Prometheus/Pushgateway, ServiceMonitor, exporter, dashboard JSON, queries/rules, access | MaintAI-gitops | Người4; Người2 review ML/drift |
| Cluster, network, cloud DB/S3/IAM | Maint-infrastructure | Người4 |

```text
Long-running app -> internal GET /metrics <- Prometheus <- Grafana server
Drift batch -> persist report/checkpoint -> Pushgateway <- Prometheus <- Grafana
Training -> MLflow SDK/API -> MLflow Server -> mlflow_db + artifact store
```

Frontend không scrape Prometheus, không giữ DB/S3 credentials. Grafana report/
histogram chi tiết cần Drift-owned read-only view hoặc authenticated adapter theo
UI-01; metrics không thay report DB. MLflow phục vụ nội dung đã log, không tự đo
latency/consumer lag thay app. Grafana/MLflow UI là công cụ platform, không thêm
React frontend hoặc deployment mới trong app.

## 2. Quy tắc dùng chung

### 2.1. Registry, endpoint và lifecycle

- Một `CollectorRegistry` cho mỗi process của long-running runtime. Khởi tạo
  collectors một lần khi startup; inject cùng registry vào handlers/callbacks.
  Không tạo Counter/Histogram lại trong mỗi request hoặc mỗi Kafka message.
- Expose `GET /metrics` bằng Prometheus exposition format và Content-Type từ
  thư viện. NetworkPolicy/management network bảo vệ endpoint; không mở qua
  Business API cho browser. Worker/read-api/relay là các scrape targets riêng.
- Kafka loop và metrics HTTP thread/task ở cùng runtime; không thêm service chỉ
  để đếm events. Endpoint không thực hiện ML, query lịch sử lớn hoặc chờ S3.
- Baseline Python: một process/runtime mỗi Pod, tăng replicas khi cần. Không
  dùng nhiều HTTP workers với registry riêng rồi scrape ngẫu nhiên một worker.
  Nếu đổi sang Gunicorn multiprocess, cần PR riêng: shared metrics directory
  được wipe trước startup, MultiProcessCollector, worker-death hook và gauge
  aggregation đúng. Không trộn multiprocess mode với Pushgateway/Info/Enum.
- Counter là số quan sát của process, có thể reset qua restart và mất một ít
  increments nếu crash trước scrape. Không dùng metrics làm audit ledger hoặc
  chứng minh exactly-once. Durable business KPIs lấy từ owner DB/archive.
- Lỗi instrumentation không đổi ACK/offset/Alert/risk. Scrape thất bại báo
  `up=0`; thiếu dữ liệu không hiển thị thành0 hay STABLE. Không nuốt lỗi âm thầm:
  structured log/error metric riêng và cảnh báo thiếu series ở platform.

### 2.2. Labels và đơn vị

`C` = Counter, `G` = Gauge, `H(profile)` = classic Histogram. Tên ở §3 là tên
constructor/base; histogram tự sinh `_bucket{le}`, `_sum`, `_count`. Không tự
register một metric tên `_bucket` hoặc `_p95`. Thời lượng dùng seconds; timestamp
gauge dùng UTC Unix seconds, MLflow timestamps dùng milliseconds.

| Label | Miền giá trị / cách dùng |
| --- | --- |
| `route` | Router template, ví dụ `/api/v1/simulator/machines/{machine_id}/pause`; unmatched=`__unmatched__`, tuyệt đối không URL chứa ID/query |
| `method` | GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS/OTHER |
| HTTP `status` | Mã HTTP hữu hạn100–599; nếu không phát response dùng `UNHANDLED`, không exception text |
| Operation `status`/`outcome` | Chỉ enum nêu ở từng dòng; map giá trị ngoài allowlist về OTHER hoặc reject theo contract |
| `dependency` | kafka/processing_db/business_db/drift_db/knowledge_db/s3/ml/rag/mlflow/oidc |
| `error_type` | TIMEOUT/UNAVAILABLE/AUTH/INVALID_RESPONSE/CONFLICT/OTHER; stack trace ở logs |
| `topic` | Allowlist trong contracts/kafka/topology.json, không tên tùy ý |
| `feature` | Sáu features trong contract, không feature/model-generated tùy ý |
| `model_version` | Chỉ metric info của model **đang load**; gỡ series cũ khi hot-reload; không label mọi request/run |

`job`, `instance`, `namespace`, `service`, `environment` là scrape/target labels
do platform gán/relabel, không app tự thêm mỗi nơi một kiểu. Người4 phải kiểm tra
labels thực của ServiceMonitor trước khi dùng query lọc `service`/`environment`.
Không gắn `machine_id`, event/prediction/alert/report/run/release IDs, hashes,
source SHA, user ID, exception message hay timestamp vào metric labels. Các định
danh đó vào logs/traces/MLflow/report metadata. `partition`/`consumergroup` của
Kafka exporter là ngoại lệ có budget theo topology, không sinh group theo UUID.

### 2.3. Bucket profiles và budgets

| Profile | Finite boundaries; client tự thêm `+Inf` | Dùng cho |
| --- | --- | --- |
| HTTP | 0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2,5,10 | REST và broker ACK |
| ML | 0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2,5 | Inference server/client |
| PIPELINE | 0.05,0.1,0.25,0.5,1,2,5,10,30,60,120,300 | Cross-service outcome/projection |
| IO | 0.01,0.05,0.1,0.25,0.5,1,2,5,10,30,60,120 | Archive/S3, outbox publish |
| RAG | 0.05,0.1,0.25,0.5,1,2,5,10,20,30,60,120 | Retrieval/generation |
| PROBABILITY | 0,0.01,0.05,0.1,0.2,0.4,0.6,0.8,0.9,0.95,0.99,1 | Failure probability [0,1] |
| CHUNKS | 0,1,2,3,5,10,20,50 | Số chunks thực retrieve mỗi request |

Đây là initial instrumentation profiles, không phải SLO production. Không đổi
buckets giữa replicas trong một rollout mà vẫn aggregate chung; thêm metric
version/migration phù hợp và phối hợp queries. p95 dùng histogram_quantile, không
trung bình các p95 của từng Pod. Default budget: dưới2000 app-specific active
series/Pod, chưa tính standard process/exporter collectors; owner tính product
labels × (finite buckets+3, thêm _created nếu client bật) và review trước khi tăng. Không tạo full Cartesian
product bằng pre-initialize mọi route/status/model. Budget thay đổi qua review.

## 3. Catalog chuẩn theo runtime

### 3.1. HTTP và dependency dùng chung

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_http_requests_total` | C requests | route,method,status | Một lần khi REST kết thúc; SSE ghi handshake một lần, không đếm từng frame |
| `maintai_http_request_duration_seconds` | H(HTTP) seconds | route,method | perf_counter từ vào handler tới response; loại `/metrics`, health và thời lượng stream SSE |
| `maintai_dependency_errors_total` | C failed attempts | dependency,error_type | Mỗi dependency call thực thất bại; caller và server là hai phép đo khác nhau |
| `maintai_metric_samples_dropped_total` | C observations | reason | SOURCE_TIME_MISSING/CLOCK_SKEW/INVALID_VALUE; bỏ latency/probability không hợp lệ, không clamp về0 |

Không ghi probe/scrape vào REST SLO. HTTP200 RAG insufficient evidence không phải
server failure; DQ invalid hay model FAILURE cũng không phải HTTP500. Instrument
middleware map route template sau router matching, kể cả error/exception paths.

### 3.2. Sensor và Ingestion

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_simulator_generated_total` | C readings | — | Sau khi reading ID/body/sequence đã commit vào durable spool |
| `maintai_simulator_send_attempts_total` | C attempts | outcome | ACCEPTED/REJECTED/RETRY/AUTH_ERROR/OTHER; sau từng HTTP attempt, không lẫn số reading unique |
| `maintai_simulator_machines` | G machines | state | RUNNING/PAUSED, số máy theo persisted control state; không chứng minh telemetry còn fresh |
| `maintai_simulator_spool_pending_count` | G readings | — | Số pending bền vững; chỉ0 khi đọc spool thành công và thật sự rỗng |
| `maintai_ingestion_accepted_total` | C publish ACKs | — | Broker ACK cho sensor.readings, trước trả202; HTTP retry có thể làm tăng lần nữa |
| `maintai_ingestion_rejected_total` | C requests | reason | MALFORMED_JSON/VALIDATION_FAILED/AUTH/SIZE/OTHER; một terminal rejection/request; auth/size không mặc nhiên archive |
| `maintai_ingestion_publish_failures_total` | C attempts | error_type | Publish unavailable/uncertain; không ghi như ACCEPTED |
| `maintai_ingestion_ack_duration_seconds` | H(HTTP) seconds | outcome | ACCEPTED/REJECTED/ERROR; request admission tới ACK accepted/rejected topic hoặc terminal publish error |

`202` chỉ phản ánh Kafka ACK, không DQ/ML/Alert success. Sensor browser preview
không xuất các metric server này. Simulator UI chỉ hiển thị control DTO có thật;
không lấy counters Prometheus làm response fields của Control API.

### 3.3. Processing Worker, Read API và Outbox Relay

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_sensor_events_total` | C validated events | validation_status | VALID/INVALID, sau commit DQ cho event mới; không đếm dedup/retry thành new event |
| `maintai_invalid_sensor_events_total` | C events | reason | TYPE/RANGE/MISSING/CONFLICT/OTHER; một primary reason/event để không nhân số record |
| `maintai_processing_events_total` | C committed outcomes | status | PREDICTION_COMPLETED/DATA_INVALID/INFERENCE_UNAVAILABLE, sau outcome+outbox transaction commit |
| `maintai_alert_signals_total` | C risk decisions | outcome | ENQUEUED/SUPPRESSED, sau VALID prediction+risk transaction commit; ENQUEUED có signal outbox, không đếm INVALID như risk decision |
| `maintai_processing_attempts_total` | C attempts | outcome | COMMITTED/RETRY/DUPLICATE/FAILED; sau attempt, Kafka offset chưa commit không thành outcome mới |
| `maintai_processing_ml_client_duration_seconds` | H(ML) seconds | outcome | SUCCESS/TIMEOUT/ERROR, quanh một HTTP ML attempt |
| `maintai_event_to_prediction_duration_seconds` | H(PIPELINE) seconds | — | Chỉ prediction thành công, source accepted timestamp tới prediction commit |
| `maintai_outbox_pending_count` | G rows | topic | Số pending theo bounded topic; snapshot DB owner, không Kafka lag |
| `maintai_outbox_oldest_pending_age_seconds` | G seconds | — | now - oldest pending created_at; empty verified=0, DB read lỗi=không xuất stale snapshot như fresh |
| `maintai_outbox_publish_duration_seconds` | H(IO) seconds | topic,outcome | ACK/ERROR; mỗi publish attempt, ACK không chứng minh downstream consumer commit |
| `maintai_outbox_publish_attempts_total` | C attempts | topic,outcome | ACK/ERROR/LEASE_LOST, không tăng success trước broker ACK/fencing check |
| `maintai_history_queries_total` | C queries | source,outcome | HOT/ARCHIVE/MIXED; SUCCESS/PARTIAL/ERROR, sau Read API response |
| `maintai_history_query_duration_seconds` | H(IO) seconds | source | Quanh history read, không label machine/query/cursor |

Worker không làm HTTP history. Read API expose HTTP metrics của runtime mình;
Relay expose relay metrics của mình. DB-wide outbox gauges được sample từ cùng
processing_db có thể xuất ở nhiều replicas: dashboard dùng `max`, không `sum`
làm nhân số rows. Mỗi mẫu có độ mới scrape/collector; collector lỗi không set0.

### 3.4. Business API/Consumer

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_business_inbox_duplicates_total` | C deliveries | topic | Inbox xác nhận duplicate; không đếm thành alert dedup nghiệp vụ |
| `maintai_alerts_created_total` | C alerts | severity | Alert mới commit; severity dùng enum contract, không theo model FAILURE count |
| `maintai_alerts_deduplicated_total` | C signals | severity | Event/signal mới được policy gộp vào alert cũ, sau commit |
| `maintai_alert_transitions_total` | C transitions | from_status,to_status | Chỉ transition hợp lệ đã commit cùng audit; HTTP retry/idempotency replay không tạo transition mới |
| `maintai_event_to_telemetry_visible_duration_seconds` | H(PIPELINE) seconds | — | Accepted envelope tới latest telemetry projection commit |
| `maintai_event_to_alert_visible_duration_seconds` | H(PIPELINE) seconds | — | Accepted source event tới alert transaction commit, tức API-readable visibility proxy |
| `maintai_sse_connections` | G connections | — | Tăng sau authenticated handshake; giảm trong finally khi disconnect/cancel/shutdown |
| `maintai_sse_resync_total` | C resync attempts | reason | RECONNECT/GAP/NOTIFY_LOST/OTHER, phía server nếu nhận/khởi tạo được resync; không giả định browser đã áp dụng snapshot |

`visible` tại đây là DB projection/alert đọc được qua API, **không** thời điểm
browser vẽ UI. Muốn đo actual browser latency phải có RUM/receipt contract riêng.
Thời lượng cross-service lấy `server_accepted_at` từ accepted envelope hoặc
`source_server_accepted_at` của outcome1.4. Không dùng sensor `received_at`, thời
điểm Kafka consume hoặc latest telemetry của một event khác để thay thế.

Alert Signal1.0 hiện chưa mang accepted timestamp. Business phải lấy timestamp
đúng **event_id** từ owner API/provenance đã lưu, hoặc migrate event/DTO được
review trước khi đo alert histogram. Không query processing_db trực tiếp, không
chặn alert creation chỉ để lấy metrics. Khi chưa có nguồn: bỏ observation, tăng
`maintai_metric_samples_dropped_total{reason="SOURCE_TIME_MISSING"}`; panel báo
coverage unavailable. Đưa coverage này vào readiness, không báo p95 giả.

Cross-host timestamp cần đồng bộ clock. Nếu timestamp missing/negative hoặc
không parse được, bỏ mẫu; nếu thời lượng lớn nhưng hợp lệ vẫn observe vào+Inf,
không bỏ các sự cố chậm làm đẹp SLO. Timestamp gốc và correlation ID giữ ở logs.

### 3.5. ML Inference và RAG

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_ml_inference_requests_total` | C predictions | status | SUCCESS/INVALID_INPUT/NOT_READY/ERROR, một lần/request ML |
| `maintai_ml_inference_duration_seconds` | H(ML) seconds | — | preprocess+predict+build result trong server; failures được đo ở finally |
| `maintai_ml_predictions_total` | C successful outputs | prediction | NORMAL/FAILURE, chỉ response prediction hợp lệ |
| `maintai_ml_failure_probability` | H(PROBABILITY) probability | — | Chỉ xác suất finite trong[0,1], không convert ML error thành0 |
| `maintai_ml_model_info` | G value1 | model_version | Load+checksum+feature schema thành công trước readiness; chỉ version effective |
| `maintai_rag_requests_total` | C briefs | status,grounded_status | SUCCESS/ERROR; GROUNDED/INSUFFICIENT_EVIDENCE/VALIDATION_FAILED/NOT_EVALUATED |
| `maintai_rag_request_duration_seconds` | H(RAG) seconds | — | Toàn retrieval+generation+validation, gồm error attempts |
| `maintai_rag_retrieval_duration_seconds` | H(RAG) seconds | — | Chỉ phần vector retrieval |
| `maintai_rag_retrieved_chunks` | H(CHUNKS) chunks | — | Số chunks thực retrieve/request, trước generation |
| `maintai_rag_citation_validation_failures_total` | C outputs | — | Một lần/output bị citation validator reject, không mỗi citation |

RAG SUCCESS nghĩa service xử lý xong request đúng protocol; INSUFFICIENT_EVIDENCE
vẫn có thể là HTTP200. NOT_EVALUATED chỉ dùng cho failure trước grounding.
Training metrics như test recall/PR-AUC nằm trong MLflow theo run/split, không
gắn run_id lên Prometheus. Label enums đo operation, không thay domain DTO enums.

### 3.6. Storage và training operational signals

| Base name | Type / unit | App labels | Điểm cập nhật |
| --- | --- | --- | --- |
| `maintai_storage_batches_total` | C attempts | status | COMMITTED/RETRY/FAILED, COMMITTED sau object+manifest verified durable trước offset commit |
| `maintai_storage_write_duration_seconds` | H(IO) seconds | outcome | SUCCESS/ERROR, mỗi batch write/verify attempt |
| `maintai_storage_oldest_uncommitted_age_seconds` | G seconds | — | Tuổi batch pending đang sở hữu; không có pending=0 chỉ khi state đã biết |
| `maintai_training_last_result` | G 0/1 | status | SUCCEEDED/FAILED/TRACKING_PENDING/UNKNOWN, latest coordinated pipeline outcome từ durable ledger adapter |
| `maintai_training_last_success_timestamp_seconds` | G Unix seconds | — | RESULT_COMMITTED sau training/tracking/registry/artifact verification; không phải deploy success |

Storage accepted/rejected/outcome adapters dùng cùng names, phân biệt bằng scrape
target/group topology; nếu chung process, thêm bounded `archive_kind` cần revision
catalog và phối hợp dashboards, không tự thay label signature hiện hành.
Training gauges do **một ledger collector hoặc một serialized batch publisher**
được chỉ định, không mọi parallel candidate PUT cùng một grouping key. Collector
đó chưa có; Argo Workflow/infra metrics do platform cung cấp, không giả bằng app.
SUCCEEDED chỉ khi RESULT_COMMITTED, không khi một child run FINISHED; failed
quality gate hoặc registry/tracking chưa rõ kết quả không cập nhật success timestamp.

### 3.7. Drift Worker

| Base name | Type / unit | App labels | Giá trị / cập nhật |
| --- | --- | --- | --- |
| `maintai_drift_feature_score` | G score | feature,metric | metric=PSI trong baseline; chỉ score tính được, thiếu mẫu/error thì không emit0 |
| `maintai_drift_feature_status` | G 0/1 | feature,status | One-hot theo5 DriftStatus; mỗi feature đúng một status=1 |
| `maintai_drift_overall_status` | G 0/1 | status | One-hot STABLE/WARNING/CRITICAL/INSUFFICIENT_DATA/ERROR |
| `maintai_drift_features_drifted` | G features | — | Count WARNING+CRITICAL |
| `maintai_drift_window_samples` | G samples | — | Số mẫu hợp lệ của window đã xét |
| `maintai_drift_reports_total` | C reports | status | Durable cumulative số report unique/status, retry cùng report không tăng |
| `maintai_drift_worker_last_success_timestamp_seconds` | G Unix seconds | — | Timestamp checkpoint success đã commit, không timestamp lúc Pushgateway scrape |
| `maintai_prediction_failure_ratio` | G ratio | — | Failure ratio current matched-model window nếu tính được; không ground-truth accuracy |

Không dùng mã số0..4 làm giá trị `feature_status`: consumer phải đọc đúng series
`{status="..."} == 1`. Counts của reports phải monotonic từ durable aggregate;
nếu retention xóa reports thì không COUNT(*) retained rows rồi giả cumulative
counter. Implementation hiện count reports chưa có retention aggregate riêng.

## 4. Mẫu Python instrumentation

Ví dụ tham chiếu cho một **ML server process**, chưa nhập vào ml-service source.
Framework route/exception mapping phải nối theo contract thật. Khóa chính xác
prometheus-client/dependencies trước khi dùng; snippet không khởi tạo model,
HTTP server, multiprocess hoặc endpoint mới ngoài scope source PR.

```python
import math
import time
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest
from prometheus_client import CONTENT_TYPE_LATEST

class InvalidInput(Exception):
    pass

class ModelNotReady(Exception):
    pass

registry = CollectorRegistry()
requests = Counter(
    "maintai_ml_inference_requests_total", "ML requests completed", ["status"], registry=registry
)
duration = Histogram(
    "maintai_ml_inference_duration_seconds", "Preprocess and inference seconds",
    registry=registry, buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5)
)
predictions = Counter(
    "maintai_ml_predictions_total", "Valid prediction outputs", ["prediction"], registry=registry
)
probability = Histogram(
    "maintai_ml_failure_probability", "Valid failure probabilities", registry=registry,
    buckets=(0, 0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 0.99, 1)
)

def measure_prediction(predict_and_validate, features):
    started = time.perf_counter()
    status = "ERROR"
    try:
        # Adapter must raise typed validation/not-ready errors and map them separately.
        result = predict_and_validate(features)
        if result["prediction"] not in {"NORMAL", "FAILURE"}:
            raise ValueError("Invalid provider prediction")
        value = result["failure_probability"]
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError("Invalid provider probability")
        probability.observe(value)
        predictions.labels(prediction=result["prediction"]).inc()
        status = "SUCCESS"
        return result
    except InvalidInput:
        status = "INVALID_INPUT"
        raise
    except ModelNotReady:
        status = "NOT_READY"
        raise
    finally:
        requests.labels(status=status).inc()
        duration.observe(time.perf_counter() - started)

def metrics_response():
    return generate_latest(registry), CONTENT_TYPE_LATEST
```

Hai exception classes là ví dụ adapter, thay bằng typed errors thật của provider.
Production error handlers phải map INVALID_INPUT/NOT_READY trước finally; không
catch mọi exception rồi trả prediction NORMAL. HTTP adapter trả bytes/Content-Type
trên `/metrics`. Với async handler, đo quanh awaited work; background future chưa
hoàn tất không tăng SUCCESS. DB counters đặt **sau commit** và sau xác định new row;
không tăng ở code branch trước transaction rồi dựa rollback để sửa Counter.

## 5. Pushgateway, checkpoint và concurrency của Drift

1. Claim single-writer/lease cho worker stream, resolve đúng model/reference.
2. Tính report và persist idempotent vào Drift owner. Metric score chưa tính được
   phải absent, còn status phản ánh ERROR/INSUFFICIENT_DATA thật.
3. PUT registry snapshot với stable grouping `job=maintai-drift-worker`,
   `environment=<configured>`; không thêm Pod/run/report/machine IDs vào grouping.
   PUT thay toàn bộ group để gỡ stale scores; không pushadd để giữ series cũ.
4. Nếu required publication lỗi: giữ report, không advance checkpoint; retry
   publication từ saved report, không tạo report mới hoặc tính baseline fallback.
5. Sau checkpoint commit, publish heartbeat từ timestamp đã commit. Heartbeat
   push lỗi không rollback report/checkpoint: báo freshness stale, retry heartbeat.
6. Không hai overlapping jobs ghi cùng grouping key; concurrencyPolicy Forbid
   chỉ hỗ trợ schedule, strict failover cần lease và triage writer cũ.
7. Pushgateway giữ dữ liệu sau khi job chết. Dashboard kiểm tra cả tồn tại metric,
   up của Pushgateway, heartbeat age và report status; up=1 không chứng minh Worker
   chạy gần đây. Xóa retired grouping keys theo runbook khi đổi environment/job.

Source hiện có [Drift metrics](../drift-worker/src/drift_monitor/metrics.py) và
[worker](../drift-worker/src/drift_monitor/worker.py) đã làm render/PUT/checkpoint
phần baseline. Nó chưa chứng minh target data adapters, strict lease/fencing,
retention aggregate hoặc Grafana provisioning. Không sửa source trong lượt docs.

## 6. Queries và dashboard semantics

PromQL dưới đây dùng toàn bộ selected environment qua dashboard/recording rules;
platform phải gắn/filter target labels đúng trước khi aggregate nhiều môi trường.

```promql
# ML p95 trên nhiều replicas: rate từng bucket rồi aggregate, giữ le.
histogram_quantile(0.95,
  sum by (le) (rate(maintai_ml_inference_duration_seconds_bucket[5m])))

# Error ratio; không có traffic/series => no data, không ép về0.
sum(rate(maintai_ml_inference_requests_total{status="ERROR"}[5m]))
 / sum(rate(maintai_ml_inference_requests_total[5m]))

# Outbox DB-wide gauge có thể lặp ở nhiều replicas: dùng max.
max(maintai_outbox_oldest_pending_age_seconds)

# KafkaExporter naming được dùng trong GitOps hiện tại.
sum by (consumergroup, topic) (kafka_consumergroup_lag)

# Kết luận drift: phải ghép điều kiện heartbeat fresh, không hiển thị riêng như sức khỏe tổng thể.
maintai_drift_overall_status{status="CRITICAL"} == 1

# Age của worker checkpoint, khác push_time_seconds của gateway.
time() - maintai_drift_worker_last_success_timestamp_seconds

# Chưa có heartbeat cũng là tình huống cần cảnh báo, không để biểu thức stale mất series.
absent(maintai_drift_worker_last_success_timestamp_seconds)
```

Gauges state không dùng rate(). Counters dùng rate()/increase(), không sum giá trị
raw qua process restarts để đếm business totals. Một numerator status chưa từng
xuất hiện có thể absent: owner có thể initialize bounded status collectors về0,
nhưng panel vẫn cần up/traffic/coverage guard; không global `or vector(0)` để che
monitoring outage. p95 thiếu traffic hiện NO_DATA; bucket +Inf growth cần xem
tail/timeout, không khẳng định mọi request nhanh hơn bucket cuối.

Năm dashboard UI-01 dùng các metrics tương ứng: overview, inference, Kafka/
Processing, infrastructure và drift. CPU/memory/DB pool/broker disk là exporter
signals, không tự thêm app counters tên giống hệt. Histograms Reference/Current
chi tiết lấy saved drift report qua datasource được owner duyệt, không metric
label chứa từng bin/reportID. SLO thresholds ở GitOps hiện là demo objectives,
phải load-test/calibrate trước khi gọi production SLO.

## 7. Migrate aliases và snapshot hiện tại

| Tên/mô tả cũ | Chốt OBS-01 |
| --- | --- |
| http_requests_total, http_request_duration_seconds | maintai_http_requests_total, maintai_http_request_duration_seconds |
| sensor_events_total, invalid_sensor_events_total | maintai_sensor_events_total, maintai_invalid_sensor_events_total |
| alerts_total / alert_dedupe_total / alert_lifecycle_actions_total | maintai_alerts_created_total / maintai_alerts_deduplicated_total / maintai_alert_transitions_total; transition labels theo catalog |
| ml_client_errors_total / rag_client_errors_total | maintai_dependency_errors_total với dependency=ml/rag, error_type bounded |
| ml_inference_requests_total / ml_inference_duration_seconds | maintai_ml_inference_requests_total / maintai_ml_inference_duration_seconds |
| ml_prediction_total{label}, ml_predictions_total / ml_model_info{version} | maintai_ml_predictions_total{prediction} / maintai_ml_model_info{model_version} |
| rag_requests_total / rag_*duration_seconds | maintai_rag_requests_total / maintai_rag_*duration_seconds |
| rag_insufficient_evidence_total | maintai_rag_requests_total{grounded_status="INSUFFICIENT_EVIDENCE"}; không tạo counter trùng |
| maintai_ml_failure_probability_bucket | Histogram base maintai_ml_failure_probability; suffix _bucket do library sinh |
| maintai_rag_retrieved_chunks_count được mô tả như số chunks | Histogram base maintai_rag_retrieved_chunks; generated _count là số request quan sát, _sum là tổng chunks, không nhầm hai đại lượng |
| kafka_consumer_lag / consumer_lag | KafkaExporter kafka_consumergroup_lag, không app mirror/rename exporter series |
| ack_latency_p95 / event_to_prediction_latency | histogram base maintai_ingestion_ack_duration_seconds / maintai_event_to_prediction_duration_seconds; p95 là query |
| Drift state gauge mã0..4 | One-hot status label với value0/1, giữ đúng renderer hiện tại |

Phần lớn app metrics chưa implement nên PR source đầu tiên dùng canonical names.
Nếu môi trường khác đã có alias thực, owner inventory trước: deploy producer mới
và query compatibility trong migration window, rồi gỡ alias; không sum alias cũ
và mới gây double count. Chưa đổi GitOps source trong lượt này. GitOps hiện có
ServiceMonitor manifests, dashboard2 panels, rules và required metric list; roots
observability còn disabled. Thiếu target labels thống nhất, heartbeat-missing
rule, full5 dashboards và coverage cho alert accepted-time phải bổ sung trước enable.
Pushgateway scrape cần honor_labels theo grouping semantics đã chốt (baseline
chọn true để giữ job/environment do authorized publisher đặt); kiểm tra actual
job/exported_job và target labels trong integration, không đoán label từ YAML name.

## 8. Bàn giao và tiêu chí nghiệm thu

Mỗi module PR phải đính kèm: source collector path, runtime/port, catalog revision,
rendered `/metrics` fixture (không credentials), type/labels/buckets, clock source,
điểm increment/commit, reset/retry semantics, projected series budget và test result.
Người4 nối ServiceMonitor/service labels/NetworkPolicy và queries bằng PR GitOps;
không đặt secret values, PrometheusRule hoặc Grafana dashboard source vào app repo.

| Owner | Checklist cần chứng minh |
| --- | --- |
| Người1 | ACK202 khác outcome; failed transaction không tăng committed counter; retry/dedup không tạo alert count mới; outbox DB snapshot không sum replicas; alert latency thiếu accepted time không fabricate |
| Người2 | SUCCESS khác FAILURE prediction; failed inference không emit probability0; model_info đúng artifact đã load; buckets consistent; MLflow theo tài liệu riêng |
| Người3 | Insufficient evidence là grounded result riêng; retrieval/generation timings tách; citation failure đếm output; không label document/chunk/request IDs |
| Người4 | Sensor counters khác browser mock; Storage offset sau durable archive; Drift status one-hot/heartbeat; Kafka exporter lag; scrape labels/ACL; dashboard/rule versioning |

Các checks cần thực thi trong PR source/platform tương ứng: schema exposition,
allowlist labels/cardinality, success/error/timeout/invalid paths, rollback/retry,
restart counter reset, unavailable collector không báo0, single/multi-replica
aggregation, missing heartbeat/no data, auth/network exposure và query fixture.
Trước enable cần real scrape→Prometheus→Grafana evidence, không chỉ dashboard JSON.
Lượt docs hiện tại chỉ kiểm tra consistency/links/Python syntax/hash, không chạy
app tests, SDK network, Prometheus, container hoặc deployment.

## 9. Tài liệu chính thức đã đối chiếu

- Quy tắc units/names/labels: [Prometheus naming](https://prometheus.io/docs/practices/naming/).
- Counter/gauge/instrumentation: [Prometheus instrumentation](https://prometheus.io/docs/practices/instrumentation/).
- Bucket constructor và generated series: [Python Histogram](https://prometheus.github.io/client_python/instrumenting/histogram/).
- PUT/group replacement: [Python Pushgateway](https://prometheus.github.io/client_python/exporting/pushgateway/).
- Khi đổi process model cần tuân thủ [Python multiprocess limitations](https://prometheus.github.io/client_python/multiprocess/).


## 10. Bằng chứng Sensor0.3.0

Sensor có custom CollectorRegistry và `/metrics`, Counter generated sau SQLite
spool commit, send attempts theo từng HTTP attempt, gauges đọc state/journal và
HTTP histogram theo buckets/route labels của catalog. Offline dry-run artifact
chuyển sang prefix maintai_replay_ và không được scrape thành server metrics.
Retry count/oldest_unacked_event_age/buffer_used_bytes còn có trong structured
spool logs; không biến chúng thành fields của ControlState. Không thêm metric
family/labels ngoài catalog. Python/provider tests kiểm commit points; xem
[Sensor README](../sensor/README.md) và [implementation report](SENSOR_IMPLEMENTATION_REPORT.md).
Prometheus/ServiceMonitor/Grafana rollout ở GitOps vẫn chưa được xác minh.

## 11. PROC-ORDER-01 — metrics cho keyed Processing và ordered Outbox

**Catalog extension PROC-ORDER-01/1.0.0 ngày23/10**, được Contracts §42 ủy quyền,
bổ sung OBS-01/1.0.0 §3.3. Names/types bên dưới đã có optional SDK collectors;
owner HTTP registry/scrape/Grafana panels vẫn cần tích hợp. Không diễn giải missing series là0/healthy.
Đọc [decision](PROCESSING_CONCURRENCY_AND_ORDERING.md) §3–7 để hiểu commit/frontier.

| Base name | Type / unit | App labels | Điểm cập nhật / ý nghĩa |
| --- | --- | --- | --- |
| `maintai_processing_active_keys` | G machines | — | Snapshot số máy có active handler trong replica; không số source machines |
| `maintai_processing_queue_records` | G receipts | state | FETCHED/QUEUED/IN_FLIGHT/COMPLETED_WAITING_COMMIT; trạng thái loại trừ nhau, gồm done-behind-hole và done-awaiting-offset-ACK |
| `maintai_processing_pending_bytes` | G bytes | — | Payload/key/headers + accounting reserve còn giữ ở ledger/buffers, gồm done-behind-hole; không process RSS |
| `maintai_processing_queue_wait_seconds` | H(PIPELINE) seconds | — | Monotonic time từ poll tới first handler dispatch của mỗi receipt attempt; không unique business count |
| `maintai_processing_uncommitted_records` | G receipts | — | Tổng receipts chưa được offset ACK/current ownership reconcile; overlap với queue records, không cộng hai gauge thành tổng |
| `maintai_processing_frontier_stall_seconds` | G seconds | — | Max tuổi blocking hole trong các assigned partitions có backlog; khởi đầu lúc hole chặn prefix,0 khi xác minh không còn hole |
| `maintai_processing_paused_partitions` | G partitions | reason | QUEUE_BUDGET/FRONTIER_BUDGET/DRAIN; một primary reason/partition, không label partition/machine |
| `maintai_processing_stale_completions_total` | C observations | stage | HANDLER/DB_FENCE/OFFSET_COMMIT; khi từ chối completion/write/commit dưới stale generation, không successful business outcome |
| `maintai_outbox_blocked_keys` | G keys | topic,reason | BACKOFF/LEASED/UNKNOWN/ERROR; DB snapshot các keys có successor chờ head, một primary reason/key |
| `maintai_outbox_head_blockage_age_seconds` | G seconds | topic | Max now-head.created_at của blocked keys có successor; verified empty=0, DB lỗi không xuất giá trị cũ như fresh |
| `maintai_outbox_order_conflicts_total` | C observations | topic,reason | CURSOR/IDENTITY/FENCE; phát hiện order/identity/fence conflict, log metadata để reconcile, không auto skip |

Queues/active keys/bytes là **per-process**, cộng giữa replicas cho tổng; count
không tăng sau rollback để giả completed. Receipt metrics có thể đếm redelivery,
khác committed logical outcomes trong §3.3. Histograms lấy clocks trong cùng
process; accepted-to-prediction latency vẫn theo source timestamp/cross-service
rules hiện hành. Remove/reset assignment-local snapshots sau revoke, không giữ
series của owner cũ như pending hiện tại.

Outbox snapshot có thể DB-wide ở nhiều relay replicas: dùng `max by(topic,reason)`
cho blocked keys và `max by(topic)` cho blockage age, không sum lặp. Nếu sau này
đổi ownership/sharding của gauge phải review aggregation cùng dashboard. Primary
reason ưu tiên ERROR → UNKNOWN → BACKOFF → LEASED, chỉ key có successor mới tính.
Query lỗi ⇒ skip fresh sample + dependency error; không set0. Traces/logs giữ
machine/event/partition/offset/generation, app metric labels tiếp tục cấm IDs.

Bàn giao Người4/GitOps: panels queue wait/active-vs-limit, pending bytes/records,
frontier stall + broker lag, ML/DB saturation, blocked head age và stale completion
rate. Ngưỡng theo measured handler/recovery/SLO budgets, không copy20 keys/2s thành
production threshold. Kafka lag thấp không tự chứng minh Alert/SSE đúng hoặc nhanh.
Giữ series budget §2.3; tests sau này phải cover backpressure/rebalance/unknown
ACK/no-data và per-process-vs-DB aggregation trước enable dashboard.

## 12. Quan sát baseline100 và tối ưu khiX>100

Baseline100 máy×1 reading/s=100/s. Kiến trúc scale-ready; khiX tăng, dùng cùng
catalog metrics để tìm bottleneck, không thêm machine/event IDs vào labels.
Theo dõi offered/generated/accepted unique rate, Kafka lag/oldest age, keyed queues/
frontier, inference/DB pool latency, outbox/archive age, Business/SSE và browser
render. Chỉ backend DB-visible metric không chứng minh target alert UI p95≤2s.

Tính n/c/pool theo [Capacity §11](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100),
đo lại sau mỗi thay đổi để giữ latency/coverage. Planning utilization70% là giả định;
không có threshold HPA hoặc SLO metric mới được tự áp dụng bởi mục này.
