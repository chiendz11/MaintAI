# ML baseline — hướng dẫn chạy thử

## Phạm vi

Bản này phục vụ thử tích hợp pipeline bằng Logistic Regression.
Chưa phải model được phê duyệt vận hành hoặc đạt quality gate cuối cùng.

- Model: logreg-baseline-v0.
- Preprocessor: onehot-standardscaler-v0.
- Threshold: 0.5, chưa tối ưu.
- Python: 3.12.
- Caller nghiệp vụ dự kiến: Processing Worker.
- ML không tạo alert hoặc quyết định maintenance severity.

## Model quality

Kết quả trên validation gồm 2.000 mẫu, trong đó 68 mẫu failure:

| Metric | Giá trị |
| --- | --- |
| Precision Failure | 0.80 |
| Recall Failure | 0.17647058823529413 |
| F1 Failure | 0.2891566265060241 |
| Average Precision | 0.4881109181600781 |
| TN / FP / FN / TP | 1929 / 3 / 56 / 12 |

Chưa đạt mục tiêu Recall Failure >= 0.85 và Precision Failure >= 0.50.
Không dùng kết quả tests API để kết luận model đạt chất lượng.
Chưa dùng test set để chọn model hoặc threshold.

## Artifact cần bàn giao riêng

Source nằm trong Git. Artifact được tạo local, không commit vào Git.

Thư mục bundle hiện tại:
ml-service/artifacts/baselines/logreg-h12wvxz9/

Có bốn file:

- pipeline.joblib: fitted preprocessing và model.
- metadata.json: features, threshold, versions, hashes và metrics.
- evaluation.json: báo cáo validation và quality gate.
- drift_reference.json: phân bố tham chiếu từ train.

Chỉ load artifact từ nguồn tin cậy.
Checksum đối chiếu với metadata không thay thế xác thực nguồn artifact.

## Build image

Chạy từ root repo bằng PowerShell, Docker Desktop phải đang chạy Linux Engine:

```powershell
docker build `
    -f .\ml-service\inference\Dockerfile `
    -t maintai-ml:baseline-v0 `
    .
```

## Chạy local

Nếu dùng bundle ở vị trí khác, thay đường dẫn ModelPath.

```powershell
$ModelPath = (Resolve-Path .\ml-service\artifacts\baselines\logreg-h12wvxz9).Path

docker run --rm `
    --name maintai-ml-baseline `
    -p 127.0.0.1:8001:8001 `
    -e MAINTAI_LOCAL_ONLY=1 `
    --mount "type=bind,source=$ModelPath,target=/models/current,readonly" `
    maintai-ml:baseline-v0
```

Model được mount read-only, không nằm trong image.
Chạy một worker; metrics hiện lưu trong bộ nhớ process và reset khi restart.

## API

| Method | Path | Chức năng |
| --- | --- | --- |
| POST | /api/v1/predict | Nhận sáu raw features, trả dự đoán |
| GET | /health/live | Process còn phản hồi HTTP |
| GET | /health/ready | Model đã qua kiểm tra startup |
| GET | /metrics | Prometheus metrics |

Request mẫu lấy từ:
contracts/v1.3/examples/http/ml/predict-request.json

```powershell
$body = Get-Content -Raw -Encoding UTF8 .\contracts\v1.3\examples\http\ml\predict-request.json

Invoke-RestMethod `
    -Uri http://127.0.0.1:8001/api/v1/predict `
    -Method Post `
    -ContentType "application/json" `
    -Body $body |
    ConvertTo-Json -Depth 10
```

Với bundle hiện tại, request mẫu trả:
NORMAL, probability xấp xỉ 0.012020897810832838, threshold 0.5.

Processing gửi raw features, không tự encode/scale trước.
event_id và machine_id là trace metadata, không phải model features.
Quy tắc phân loại: probability >= threshold là FAILURE.

## Failure behavior

| Tình huống | Kết quả |
| --- | --- |
| Input sai, ví dụ RPM âm | 422 VALIDATION_FAILED |
| JSON không hợp lệ | 400 INVALID_REQUEST |
| Model không sẵn sàng | 503 MODEL_NOT_READY |
| Lỗi khi inference | 500 INTERNAL_ERROR |
| Thiếu model lúc startup | Live UP, ready DOWN, predict 503 |

Caller không được biến lỗi ML thành NORMAL.
Timeout/retry và xử lý nghiệp vụ thuộc Processing theo contract.

## Monitoring

Các metrics:

- maintai_ml_inference_requests_total{status}
- maintai_ml_inference_duration_seconds
- maintai_ml_predictions_total{prediction}
- maintai_ml_failure_probability
- maintai_ml_model_info{model_version}

Status hiện có: SUCCESS, INVALID_INPUT, NOT_READY, ERROR.
Không dùng event_id/machine_id làm metric labels.
Duration đo phần inference trong server, không phải latency toàn hệ thống.

## Drift reference

- model_version: logreg-baseline-v0
- dataset_version: ai4i2020-train-split-v1-seed42
- reference_source: training
- sample_count: 6000
- predicted failure rate: 0.008666666666666666, tương ứng 52/6000 mẫu
- SHA-256:
  ba9cac5d5ebae48cd5b68fe8710758d53e3b976af4131bd2ea2e2d843d3fea71

Numeric profiles dùng quantile bins, gom under/overflow vào bin biên.
Categorical order: L, M, H.
Prediction reference là tỉ lệ model dự đoán FAILURE trên train,
không phải tỉ lệ nhãn failure thật.

Drift owner cần cấu hình model/dataset version và reference_path tương ứng,
thay cho các giá trị fixture.

## Kiểm chứng đã thực hiện

- 30 tests API/startup/contract đã được chạy, các test báo ok.
- Docker image đã build thành công trên Linux Engine.
- Container load model thật, live/ready UP và predict thành công.
- Request sai RPM trả 422.
- Metrics đếm đúng một SUCCESS, một INVALID_INPUT và một prediction.
- Container không mount model: live 200, ready 503, predict 503 MODEL_NOT_READY.

- Tổng 32 tests API/startup/contract/metrics PASS.
- Loader thực của Drift Worker đọc reference thành công.
- Phân bố sáu features được tính lại từ train bằng code worker và khớp reference.
- Worker từ chối model_version hoặc dataset_version không khớp.
- Kiểm tra reference là offline compatibility, chưa chạy worker với DB/runtime data.
- Log kiểm thử đi kèm thư mục bàn giao: ml-tests.txt và reference-check.txt.

## Giới hạn tích hợp

Bản hiện tại là local prototype, chưa có xác thực JWT.
MAINTAI_LOCAL_ONLY=1 là điều kiện cấu hình, không phải cơ chế xác thực.

Lệnh chạy trên chỉ publish port lên loopback của máy host.
Kết nối từ Processing ở container/máy khác hoặc cluster cần chốt networking
và xác thực theo contract trước khi dùng chung.

Chưa kiểm chứng E2E Processing/Kafka/Business, triển khai cluster,
MLflow release hoặc full contract v1.4.

Dependency trực tiếp đã pin; chưa có lock đầy đủ kèm hashes.
Base image tag chưa được pin digest trong Dockerfile.