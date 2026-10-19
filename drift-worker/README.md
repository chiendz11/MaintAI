# Drift Worker

**Target 1.3.1:** xem [Dockerfile](Dockerfile). Code/commands bên dưới là worker v1.1 dùng PostgreSQL loader; target Processing API/curated S3 adapters và drift_db ownership chưa migrate. Không dùng implementation direct DB hiện tại làm template truy cập SQL xuyên owner cho kiến trúc mới.

Worker Python 3.12 chạy một lượt rồi kết thúc, theo System Contracts và Implementation Development Guide v1.1 DRIFT. Đọc reference theo model/dataset version, lấy cửa sổ sensor `VALID` từ PostgreSQL, tính PSI, lưu report, xuất metrics và cập nhật checkpoint. Worker không gọi inference, không retrain và không thay đổi machine state, risk hay maintenance alerts.

## Cài đặt

Chạy từ root repository:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r drift-worker/requirements-dev.lock
.venv/bin/python -m pip install --no-deps -e ./drift-worker -e ./sensor
```

`requirements.lock` dành cho runtime; `requirements-dev.lock` thêm công cụ test, build và audit. Dockerfile sử dụng runtime lock và user không có quyền root.

## Reference và phiên bản

Reference JSON phải khớp cả `model_version` và `dataset_version`, chứa đủ sáu feature và probabilities/bins hợp lệ. Reference production được export từ training data hoặc tập reference đã phê duyệt cùng model artifact. Không dùng test set làm baseline production. Worker không tự tạo reference khi chạy. Thiếu, sai format hoặc lệch version trả `ERROR / DRIFT_REFERENCE_NOT_FOUND`.

Lệnh export offline nhận JSONL với `features` đúng contract. Khi dùng trong pipeline training, chuyển tập training/reference sang format này trước:

```bash
.venv/bin/maintai-drift build-reference \
  --events /path/to/approved-reference.jsonl \
  --reference-source approved_reference \
  --model-version YOUR_DEPLOYED_MODEL_VERSION \
  --dataset-version YOUR_REFERENCE_DATASET_VERSION \
  --output /path/to/drift_reference.json
```

Nếu cần prediction drift, cung cấp thêm `--failure-rate` từ predictions của chính model trên reference. Không suy ra baseline này từ nhãn ground truth. `--reference-source test_fixture` chỉ dành cho bài kiểm tra/demo. Baseline có provenance, timestamp, sample count; artifact production cần được quản lý cùng model bởi pipeline training.

## Chạy với PostgreSQL

Backend quản lý `sensor_events` và `predictions`. Query chỉ lấy sensor events `validation_status='VALID'`, ghép predictions theo event ID và đúng phiên bản model. Event thiếu prediction vẫn được dùng cho feature drift; prediction drift chỉ kết luận khi số predictions đúng model đạt minimum samples.

```bash
export DRIFT_DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE'
export DRIFT_MODEL_VERSION='YOUR_DEPLOYED_MODEL_VERSION'
export DRIFT_DATASET_VERSION='YOUR_REFERENCE_DATASET_VERSION'
export DRIFT_REFERENCE_PATH='/path/to/drift_reference.json'
export DRIFT_PUSHGATEWAY_URL='http://pushgateway:9091'
export DRIFT_PUSHGATEWAY_REQUIRED=true
export ENVIRONMENT=production

# Chạy một lần trong bước migration; chỉ tạo bảng worker sở hữu.
.venv/bin/maintai-drift init-db
.venv/bin/maintai-drift run --output drift-worker/output/report.json
```

`init-db` tạo `drift_reports`, `drift_checkpoints` và indexes; không tạo các bảng backend. SQL migration đóng gói trong wheel ở `src/drift_monitor/sql/001_drift.sql`. Trong production nên cấp quyền migration riêng, runtime chỉ đọc bảng backend và ghi bảng worker. Không ghi connection string/password vào log.

Cấu hình mẫu: [config/drift.example.yaml](config/drift.example.yaml). `--config` đọc mapping `drift`; artifact path tương đối được tính từ thư mục YAML. Thứ tự ưu tiên: CLI → environment → YAML → defaults. Model/dataset/reference là bắt buộc; điền bằng giá trị của model đang được triển khai. `.env.example` là cấu hình demo; CLI không tự load `.env`.

## Detector, report và retry

Mặc định lấy 500 events mới nhất, sắp theo `(received_at, event_id)`; tối thiểu 200 samples. Không đủ trả `INSUFFICIENT_DATA`, không kết luận drift. Numeric và categorical dùng PSI; epsilon `1e-6` xử lý bucket rỗng. Numeric giá trị vượt reference bins được gộp vào hai bucket biên để không mất probability mass. Category `L/M/H` giữ đúng thứ tự trong reference artifact.

PSI ≥ 0.10 là `WARNING`, ≥ 0.20 là `CRITICAL` ở cấp feature. Report chỉ lên `CRITICAL` khi ít nhất hai features critical; một feature critical vẫn đưa tổng thể lên `WARNING`. Prediction drift dùng PSI của tỷ lệ FAILURE/NORMAL và có thể đưa tổng thể lên `WARNING`. Snapshot policy, version và cửa sổ được lưu trong report để giải thích được kết luận sau này.

Report ID được xác định từ window, predictions, reference và policy. Chạy lại cùng input sử dụng report đã lưu, không tăng counter và giữ `computed_at` ban đầu. Prediction đến muộn hoặc policy/reference thay đổi tạo report mới. Checkpoint ghi tiến độ thành công; không dùng để loại bỏ phần dữ liệu trùng giữa các cửa sổ rolling. Checkpoint có điều kiện chống lùi về event cũ.

Thứ tự: lưu report → PUT metrics lên Pushgateway → commit checkpoint → cập nhật heartbeat thành công. Khi Pushgateway bắt buộc và lần xuất report thất bại, checkpoint không tiến; retry có thể xuất lại report đã lưu. Heartbeat chỉ phản ánh checkpoint đã commit. Lỗi xuất heartbeat sau commit được log và lượt sau cập nhật lại. Một worker/model/environment nên có một lượt chạy đồng thời; cấu hình lịch và concurrency thuộc repo GitOps.

`STABLE`, `WARNING`, `CRITICAL`, `INSUFFICIENT_DATA` trả exit code 0; `ERROR` hoặc lỗi thực thi trả 1; lỗi input/config trả 2; ngắt bằng Ctrl+C trả 130. JSON report ra stdout hoặc `--output`, structured logs ra stderr.

## Metrics

Pushgateway dùng PUT với grouping key cố định `job/environment`, thay cả nhóm để bỏ score cũ khi report mới thiếu dữ liệu. Labels chỉ chứa feature, metric, status và environment; không chứa machine/event/report ID.

- `maintai_drift_feature_score`, `maintai_drift_feature_status`
- `maintai_drift_features_drifted`, `maintai_drift_overall_status`
- `maintai_drift_window_samples`, `maintai_drift_reports_total`
- `maintai_drift_worker_last_success_timestamp_seconds`
- `maintai_prediction_failure_ratio` khi có predictions đúng model

Counter dựa trên số report duy nhất trong DB, không dựa trên số lần process chạy. `--metrics-file` xuất Prometheus text cho kiểm tra offline. Prometheus scrape Pushgateway; CronJob, alert rules và Grafana dashboards thuộc repo GitOps/hạ tầng.

## Kiểm tra cùng sensor

Backend hiện còn skeleton. Bài integration dùng bảng DB tối thiểu trong test harness để nhận JSONL đúng contract từ sensor và đánh dấu `VALID`; chưa mô phỏng HTTP ingestion, ML inference hoặc luồng maintenance của backend thật.

Test local không cần Docker:

```bash
.venv/bin/pytest drift-worker/tests
```

Nếu chưa đặt `DRIFT_TEST_DATABASE_URL`, tests PostgreSQL được skip. Có thể đặt URI của DB **dành riêng cho test**; mỗi test tạo và xóa schema ngẫu nhiên riêng.

Khi Docker daemon đang chạy, test container gồm sensor + worker + PostgreSQL 17 + Pushgateway:

```bash
.venv/bin/python drift-worker/tests/integration/run_docker_demo.py
```

Script dùng Docker Compose fixture trong `tests/integration/compose.yaml`, cổng localhost 5433 và 9092. Bốn scenarios: normal → STABLE; sudden/gradual drift → CRITICAL; categorical drift → WARNING. Kiểm tra schema, report/checkpoint trong DB, retry không trùng và metrics thực tế; machine state vẫn NORMAL. Chỉ sử dụng CSV fixture và reference `test_fixture`, không phải baseline model production. Sau khi chạy script dọn containers/volumes của project `maintai-drift-test`.

CI chạy unit/contract tests, integration PostgreSQL thật và bài container này. Không có production deployment manifests trong thư mục worker.

## Mã nguồn

| Module | Trách nhiệm |
| --- | --- |
| `models.py`, `config.py` | Contract và policy/config |
| `reference.py` | Đọc/kiểm tra reference, export offline |
| `data_loader.py`, `repository.py` | Input JSONL, truy vấn và persistence PostgreSQL |
| `detector.py`, `report.py` | PSI, aggregation, report có ID ổn định |
| `worker.py`, `cli.py` | Điều phối one-shot và CLI |
| `metrics.py`, `logging.py` | Pushgateway và structured logs |

Schema hiện hành nằm trong
[drift-reference.schema.json](../contracts/v1.3/schemas/drift-reference.schema.json)
và [drift-report.schema.json](../contracts/v1.3/schemas/drift-report.schema.json).
Payload version vẫn1.0.0, khớp runtime. Xuất bản đối chiếu vào output ngoài Git:

```bash
.venv/bin/maintai-drift export-schemas --directory drift-worker/output/schemas
```


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
