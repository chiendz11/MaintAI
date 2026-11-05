# ML training — chuẩn bị dữ liệu, thực nghiệm và export

Đã có các script chạy local để chuẩn bị dữ liệu, so sánh candidate,
đánh giá và đóng gói model. Image training batch và tích hợp Argo
chưa được xác minh hoàn tất.

Không có HTTP inference endpoint trong workload training.
Ranh giới workload được mô tả tại Dockerfile và runtime.json.

## Thành phần đã triển khai

| Thư mục | Chức năng |
| --- | --- |
| data_preparation/ | Kiểm tra dữ liệu, frozen split và EDA trên train |
| model/ | Baseline, benchmark candidates, train-only CV và feature ablation |
| evaluation/ | Metrics, threshold, slices và báo cáo đánh giá |
| export/ | Khóa lựa chọn, xuất bundle/reference, MLflow tracking và Registry |
| tests/ | Kiểm thử split, feature engineering, metrics và hành vi candidate |

Custom transformer dùng chung nằm tại:
../shared/preprocessing/feature_interactions.py

## Dữ liệu và protocol

- Dataset: AI4I 2020.
- Split stratified, seed 42.
- Train: 6000 dòng, 203 Failure.
- Validation: 2000 dòng, 68 Failure.
- Test: 2000 dòng, 68 Failure.
- Split indices và SHA-256 được lưu trong manifest.
- Fit preprocessing và model chỉ trên train.
- Cross-validation năm fold chạy trong train.
- Chọn candidate và threshold bằng validation.
- Không dùng IDs, target hoặc failure-mode labels làm feature.

Kiểm tra frozen split từ repository root:

    .\.venv\python.exe .\ml-service\training\data_preparation\create_split.py --verify-only

Dataset cần được tải và kiểm tra checksum theo hướng dẫn Sensor.
Không commit dataset đầy đủ hoặc artifacts sinh ra ngoài fixtures được quản lý.

## Trình tự thực nghiệm đã thực hiện

1. EDA train và Logistic Regression baseline.
2. So sánh dummy, rule-based, Logistic Regression, Decision Tree,
   Random Forest và HistGradientBoosting.
3. Khóa RF v1 và đánh giá held-out test: không đạt quality gate.
4. Tiếp tục phát triển bằng train-only CV và validation.
5. Ablation nhóm ba feature tương tác trên cùng các fold.
6. Chọn HGB interactions làm candidate tích hợp.
7. Khóa pipeline, transformer và threshold.
8. Đánh giá HGB trên previously used test; công khai giới hạn test reuse.
9. Export bundle, drift reference và MLflow Model; kiểm chứng Registry.

Các script tạo thư mục output riêng. Khi chạy tiếp, sử dụng đường dẫn
output thực tế của bước trước; không mặc định tên thư mục của máy khác.

Không chạy lại các bước đánh giá hoặc đăng ký model chỉ để ghi đè receipt.
Đọc guard và trạng thái artifacts trước khi tiếp tục một lần chạy bị gián đoạn.

## Candidate hiện tại

- Model: hgb-interactions-candidate-v1.
- Threshold: 0.08943418823732646.
- Validation: precision 87,88%; recall 85,29%.
- Previously used test: precision 77,92%; recall 88,24%.
- Independent new holdout: NO.
- Approved for release: NO.

Quality gate dùng đồng thời:
Recall Failure >= 0.85 và Precision Failure >= 0.50.

Kết quả HGB đạt gate theo point estimate trên reused test,
nhưng không thay thế một đánh giá độc lập mới hoặc phê duyệt release.

Xem [model card](MODEL_CARD_HGB.md) để đọc lý do lựa chọn,
so sánh, tham số, slices, độ bất định và giới hạn.

## Export và serving

Bundle gồm pipeline, metadata, evaluation, drift reference và thông tin
kiểm chứng nguồn gốc/toàn vẹn tương ứng.

Drift reference:
- Tạo từ train, không lấy từ validation/test hoặc runtime window.
- Mô tả sáu raw features.
- Prediction reference dùng tỷ lệ model dự đoán FAILURE trên train.
- Phải khớp model version và dataset version.

API vẫn nhận sáu raw features; transformer tính feature tương tác nội bộ.

Xem [hướng dẫn bàn giao HGB](../inference/HGB_HANDOFF.md).

## Dependencies và kiểm thử

Môi trường thực nghiệm hiện tại dùng Python 3.12.
Phiên bản thư viện và tham số được lưu cùng bằng chứng thực nghiệm.

- requirements-eda.in và snapshot Windows phục vụ EDA.
- ../inference/requirements-ci.in khai báo dependencies trực tiếp cho
  các suite kiểm thử ML đã chọn.
- ../inference/requirements-ci.lock là snapshot phiên bản của môi trường
  kiểm thử sạch; không phải lock đầy đủ cho mọi tác vụ training/MLflow.
- MLflow Model package lưu requirements tương ứng với model đã đóng gói.

Đã chạy 68 tests trong môi trường Windows sạch:
- Inference/API/contract/startup/metrics: 32.
- Split: 11.
- Feature interactions: 15.
- Metrics và threshold selection: 10.

Job ml-model-ci trong workflow chung chạy các suite này trên Linux,
build inference image và kiểm tra dependencies/import trong image.
Kết quả CI phải được đối chiếu với đúng commit trên GitHub Actions.

Các tests trên không chứng minh model đạt quality gate,
không chạy full training và không thay thế kiểm thử toàn hệ thống.

## Phần chưa hoàn tất

- Training image/batch entrypoint và thực thi Argo.
- Dependency lock đầy đủ cho toàn bộ workflow training/export.
- Đánh giá độc lập mới cho candidate sau quá trình phát triển thích nghi.
- Release approval, lấy artifact đã duyệt và tích hợp GitOps.
- Kiểm chứng Processing → ML trong môi trường triển khai chung.

Training thành công hoặc Registry báo READY không tự động cho phép release.