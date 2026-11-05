# Bàn giao HGB — candidate thử tích hợp

## 1. Trạng thái bàn giao

- Model: hgb-interactions-candidate-v1.
- Threshold cố định: 0.08943418823732646.
- Input: sáu raw features theo contract ML.
- Transformer tính ba feature tương tác bên trong pipeline.
- Validation: precision 87,88%; recall 85,29%.
- Previously used test: precision 77,92%; recall 88,24%.
- Independent new holdout: NO.
- Approved for release: NO.

Đọc thêm `ml-service/training/MODEL_CARD_HGB.md` để xem so sánh model,
lý do lựa chọn, các slice yếu và giới hạn đánh giá.

Bản này dùng thử tích hợp local. Shared deployment cần hoàn thiện
authentication/authorization theo contract và quy trình phê duyệt release.

## 2. Thành phần cần nhận

### Source

Lấy source từ nhánh ML tại đúng commit bàn giao, gồm:

- inference API, Dockerfile và requirements-serving.txt.
- shared/preprocessing/feature_interactions.py.
- training scripts, tests và tài liệu liên quan.

Không chỉ copy riêng pipeline.joblib: model cần custom transformer
và các thư viện tương thích.

### Bundle

Nhận nguyên thư mục:

ml-service/artifacts/integration/hgb-interactions-m56w5ff4

Giữ đủ các file và thư mục bên trong, bao gồm pipeline, metadata,
evaluation, drift reference, source transformer và manifest.

Bundle không mặc nhiên có trong Git vì thư mục artifacts có thể bị ignore.
Người bàn giao phải cung cấp bundle riêng cùng source commit.

Không cần gửi .venv, credentials hoặc toàn bộ dataset để chạy inference.

### Evidence

Nhận các báo cáo tương ứng:

- Model card.
- Benchmark comparison và validation slices.
- integration_candidate.json.
- reused_test_report.json.
- MLflow tracking và Registry receipts.
- Kết quả smoke test, fault injection, latency và input behavior.

Không coi báo cáo cũ của Logistic Regression là kết quả HGB.

## 3. Chạy bằng Docker trên Windows PowerShell

Điều kiện:
- Docker Desktop đang chạy Linux containers.
- Source ở C:\MaintAI.
- Bundle đã đặt đúng đường dẫn dưới đây.

```powershell
cd C:\MaintAI

docker build `
    -f .\ml-service\inference\Dockerfile `
    -t maintai-ml:hgb-candidate-v1 `
    .
```

Chỉ chạy bước tiếp theo khi build thành công.

```powershell
$HgbBundle = (
    Resolve-Path .\ml-service\artifacts\integration\hgb-interactions-m56w5ff4
).Path

docker run --rm -d `
    --name maintai-ml-hgb `
    -p 127.0.0.1:8003:8001 `
    -e MAINTAI_LOCAL_ONLY=1 `
    --mount "type=bind,source=$HgbBundle,target=/models/current,readonly" `
    maintai-ml:hgb-candidate-v1
```

Model được mount read-only; image chứa API và transformer.

Nếu container tên này đã tồn tại, kiểm tra trước:

```powershell
docker ps -a --filter "name=maintai-ml-hgb"
docker logs --tail 80 maintai-ml-hgb
```

Không chạy thêm container trùng tên.

## 4. Kiểm tra API

Chờ startup hoàn tất rồi kiểm tra:

```powershell
Invoke-RestMethod http://127.0.0.1:8003/health/live |
    ConvertTo-Json -Depth 5

Invoke-RestMethod http://127.0.0.1:8003/health/ready |
    ConvertTo-Json -Depth 5
```

Readiness mong đợi: status UP và checks.model_loaded = true.

Gửi request mẫu:

```powershell
$Body = Get-Content -Raw -Encoding UTF8 `
    .\contracts\v1.3\examples\http\ml\predict-request.json

Invoke-RestMethod `
    -Uri http://127.0.0.1:8003/api/v1/predict `
    -Method Post `
    -ContentType "application/json" `
    -Body $Body |
    ConvertTo-Json -Depth 10
```

Kiểm tra response:
- event_id khớp request.
- model_version là hgb-interactions-candidate-v1.
- model_threshold là 0.08943418823732646.
- failure_probability hữu hạn, trong [0, 1].
- FAILURE khi score >= threshold, ngược lại NORMAL.

Không dùng score của Logistic Regression làm expected score cho HGB.

Metrics:

```powershell
(Invoke-WebRequest -UseBasicParsing `
    http://127.0.0.1:8003/metrics).Content
```

## 5. Kết nối Processing

Processing chạy trực tiếp trên cùng máy host có thể gọi:

http://127.0.0.1:8003/api/v1/predict

127.0.0.1 trong container Processing chỉ tới chính container đó,
không phải container ML.

Nếu Processing chạy trong container, cần phối hợp cấu hình network và
service address. Trong cùng Docker network, địa chỉ đích dùng tên
service/container ML và cổng nội bộ 8001.

Việc mở network dùng chung phải đi kèm cấu hình xác thực phù hợp contract.
MAINTAI_LOCAL_ONLY=1 là cờ cho prototype; không phải authentication.

Chưa có bằng chứng hoàn tất Processing → ML toàn luồng trong bản bàn giao này.

## 6. Drift reference

Dùng drift_reference.json của chính bundle HGB:

- model_version: hgb-interactions-candidate-v1.
- dataset_version: ai4i2020-train-split-v1-seed42.
- reference_source: training.
- sample_count: 6000.

Reference mô tả sáu raw features.
Prediction reference là tỷ lệ model dự đoán FAILURE trên train.

Đã kiểm tra bằng loader Drift Worker:
- Đọc reference thành công.
- Từ chối model version hoặc dataset version không khớp.

Chưa kiểm tra toàn bộ worker, DB hoặc scheduling trong lượt bàn giao này.
Không ghép reference Logistic Regression với model HGB.

## 7. MLflow

Local tracking URI: http://127.0.0.1:5000.

HGB run:
1adb4db4db6f40e0a73237b68d82d029

Registry model:
models:/maintai-local-hgb-candidate-1adb4db4/1

Registry status: READY.
Approved for release: NO.

Run và Registry version nằm trong MLflow server của người tạo.
Clone Git hoặc copy inference bundle không tự mang theo database và
artifact storage của MLflow.

Nếu nhóm dùng server khác, cần chuyển/log/register có kiểm chứng;
run ID hoặc version mới phải được ghi lại trong provenance tương ứng.

Inference hiện load bundle từ MODEL_DIR.
Chưa tự động tải model từ Registry hoặc thực hiện GitOps promotion.

## 8. Bằng chứng kiểm thử hiện có

- 32 tests inference/API/contract/startup/metrics PASS.
- Candidate behavior tests PASS.
- HTTP NORMAL/FAILURE khớp score offline.
- Input sai bị từ chối.
- 13/13 tình huống input behavior PASS.
- Transformer hash sai: readiness và predict trả 503.
- Drift reference loader compatibility PASS.
- MLflow upload/download và package hashes PASS.
- Sklearn/pyfunc load trong process mới, transformer được đóng gói: PASS.

Benchmark local, mỗi lượt 100 request sau 10 warmup:

| Concurrency | Thành công | HTTP p95 | Throughput |
| --- | ---: | ---: | ---: |
| 1 | 100/100 | 56,84 ms | 29,00 request/s |
| 4 | 100/100 | 227,75 ms | 20,67 request/s |

Đây là phép đo local nhỏ, chưa chứng minh production capacity.

## 9. Xử lý lỗi

| Triệu chứng | Kiểm tra |
| --- | --- |
| Không kết nối được | Docker/container, port và startup logs |
| Liveness 200 nhưng readiness 503 | Artifact mount, metadata, checksum, phiên bản thư viện |
| Transformer checksum mismatch | Source/image và bundle có cùng phiên bản không |
| ModelNotFound trong Registry | Tracking/Registry URI và đúng server chứa version |
| HTTP 422 | Sáu feature, kiểu dữ liệu, category, RPM và tool wear |
| HTTP 500 | Inference logs theo request ID; không chuyển lỗi thành NORMAL |

Không sửa hash trong metadata chỉ để vượt qua lỗi.
Không tự bỏ validation hoặc đổi threshold khi xử lý lỗi triển khai.

## 10. Dừng và rollback

Dừng container local:

```powershell
docker stop maintai-ml-hgb
```

Container được tạo với --rm sẽ bị xóa khi dừng; bundle host vẫn còn.

Trước khi đổi phiên bản cần lưu:
- Image identity.
- Bundle path và manifest.
- Model/preprocessor version.
- Threshold.
- Drift reference tương ứng.

Rollback phải dùng cả image và bundle tương thích đã được kiểm chứng.
Sau rollback kiểm tra readiness, identity và request mẫu.

Không tự fallback sang model khác khi artifact lỗi.
Logistic Regression cũ chỉ là baseline demo, không phải model đã đạt quality gate.