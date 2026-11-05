# Model card — HGB interactions candidate v1

Ngày cập nhật: 2026-11-05.

## 1. Trạng thái và phạm vi sử dụng

- Model: hgb-interactions-candidate-v1.
- Thuật toán: HistGradientBoostingClassifier.
- Trạng thái: candidate phục vụ thử tích hợp.
- Approved for release: NO.
- Bài toán: phân loại Machine failure từ điều kiện vận hành trong AI4I 2020.
- Đầu ra: score của lớp Failure và nhãn nhị phân theo threshold.
- Không dự đoán thời gian còn lại trước hỏng (RUL).
- Không quyết định severity, tạo alert hoặc tự điều khiển thiết bị.
- Score chưa được chứng minh đã calibration tốt.
- Chưa xác minh khả năng tổng quát sang nhà máy thực tế.

Processing Worker gửi raw features; inference thực hiện preprocessing đã fit.
Không fit lại preprocessing hoặc model trong quá trình phục vụ request.

## 2. Dữ liệu và chống leakage

Dataset: AI4I 2020, 10.000 bản ghi, 14 cột.

- Normal: 9.661.
- Failure: 339.
- Tỷ lệ Failure: 3,39%.
- Kiểm tra dữ liệu: không có ô thiếu, dòng sai số cột hoặc trùng sáu feature
  theo các kiểm tra đã thực hiện.

SHA-256 dataset:
dc6630cd9b1f0f853922fad78a1b6436570d3f1ec863f1dd5c4340ac56bc8a8e

Split stratified cố định, seed 42:

| Tập | Normal | Failure | Tổng |
| --- | ---: | ---: | ---: |
| Train | 5797 | 203 | 6000 |
| Validation | 1932 | 68 | 2000 |
| Test | 1932 | 68 | 2000 |

SHA-256 split manifest:
94889871b1c814f5d36d67357c44b6fabc68a18040e7674658a64f7761483752

Fit preprocessing và model chỉ trên train.
Cross-validation dùng năm fold trong tập train.
Validation dùng so sánh candidate và chọn threshold.

Không dùng UDI, Product ID, Machine failure hoặc TWF/HDF/PWF/OSF/RNF
làm feature. IDs trong request chỉ phục vụ truy vết.

Test đã được mở để đánh giá RF v1 trước khi tiếp tục phát triển HGB.
Vì vậy kết quả HGB trên tập này được ghi là reused test, không phải
một holdout mới độc lập với toàn bộ quá trình phát triển.

## 3. Đầu vào và feature engineering

API nhận đúng sáu raw features:

| Feature | Ý nghĩa / kiểm tra |
| --- | --- |
| product_type | L, M hoặc H |
| air_temperature_k | Nhiệt độ không khí, K; số hữu hạn |
| process_temperature_k | Nhiệt độ quá trình, K; số hữu hạn |
| rotational_speed_rpm | Tốc độ quay, rpm; lớn hơn 0 |
| torque_nm | Mô-men xoắn, Nm; số hữu hạn |
| tool_wear_min | Độ mòn tính bằng phút; không âm |

Không tự lấy min/max quan sát trong dataset làm giới hạn vật lý.

Transformer tính thêm ba feature nội bộ:

- temperature_difference_k = process_temperature_k - air_temperature_k.
- mechanical_power_w = torque_nm × rotational_speed_rpm × 2π / 60.
- wear_torque_interaction = tool_wear_min × torque_nm.

Các phép tính chỉ dùng sáu raw features, không dùng nhãn.
mechanical_power_w là đại lượng tính theo công thức công suất cơ học;
không đồng nhất với điện năng tiêu thụ đo thực tế.

Pipeline:
SensorInteractions → OneHotEncoder và numeric passthrough → HGB.

Type được one-hot theo L/M/H. Sau preprocessing có 11 cột:
3 cột category, 5 sensor gốc và 3 feature dẫn xuất.

API vẫn giữ nguyên contract sáu raw features.

## 4. Tham số và quy tắc lựa chọn

Cấu hình HGB đã thử nghiệm:

- max_iter: 300.
- learning_rate: 0.05.
- max_leaf_nodes: 31.
- min_samples_leaf: 10.
- l2_regularization: 1.0.
- early_stopping: false.
- random_state: 42.

Các tham số đầy đủ được lưu trong metadata thí nghiệm.

Quality gate áp dụng đồng thời tại cùng một threshold:

- Recall Failure >= 0.85.
- Precision Failure >= 0.50.

Ưu tiên operating point đạt cả hai gate, sau đó so sánh F2.
F2 nhấn mạnh recall hơn precision; không đại diện cho một ma trận
chi phí thiệt hại đã được nhà máy xác nhận.

Threshold đã khóa: 0.08943418823732646.

Quy tắc nhãn:
- score >= threshold: FAILURE.
- score < threshold: NORMAL.

Threshold thấp không có nghĩa mọi score đều đã được calibration.
Threshold được chọn từ validation, không phải từ reused test.

## 5. Các phương án đã so sánh

Đã thử:
- Always Normal.
- Rule-based theo torque tại quantile 99% của train.
- Logistic Regression.
- Logistic Regression với class weight.
- Decision Tree.
- Random Forest.
- HistGradientBoosting.
- Một số cấu hình RF/HGB qua train-only cross-validation.
- RF/HGB có và không có nhóm ba feature tương tác.

Rule-based là đối chứng thống kê; không phải quy tắc an toàn nhà máy.

Một số kết quả validation tại operating point đã chọn:

| Phương án | Precision | Recall | F2 | Average precision |
| --- | ---: | ---: | ---: | ---: |
| Logistic Regression, threshold 0.5 | 0.80000 | 0.17647 | 0.20906 | 0.48811 |
| RF v1, threshold validation | 0.50000 | 0.86765 | 0.75641 | 0.68481 |
| HGB v1, threshold validation | 0.50435 | 0.85294 | 0.74935 | 0.77138 |
| RF interactions | 0.67442 | 0.85294 | 0.81006 | 0.88751 |
| HGB interactions | 0.87879 | 0.85294 | 0.85799 | 0.89404 |

Average precision được tính từ score, không phụ thuộc một threshold cụ thể.

Ablation trên cùng năm fold train:

| Phương án | Mean fold AP | Std fold AP |
| --- | ---: | ---: |
| RF raw | 0.75306 | 0.03664 |
| RF interactions | 0.87305 | 0.02561 |
| HGB raw | 0.78742 | 0.04316 |
| HGB interactions | 0.87655 | 0.03893 |

Nhóm feature tương tác tăng AP trên cả năm fold của mỗi family.
Thí nghiệm này đánh giá cả nhóm feature, chưa xác định đóng góp riêng
của từng feature và chưa chứng minh quan hệ nhân quả.

Lý do chọn HGB interactions làm candidate tích hợp:
- Đạt cả hai gate trên validation.
- Cùng phát hiện 58/68 Failure như RF interactions.
- Chỉ có 8 false positive, so với 28 của RF interactions.
- F2 validation cao hơn trong hai candidate này.
- Đã kiểm chứng lưu/load và đóng gói custom transformer.

## 6. Kết quả đánh giá và giới hạn

### Validation

- TN: 1924; FP: 8; FN: 10; TP: 58.
- Precision Failure: 87,88%.
- Recall Failure: 85,29%.
- F2: khoảng 0,85799.
- Average precision: khoảng 0,89404.
- Point-estimate gate: PASS.

### Previously used test

Giữ nguyên pipeline và threshold đã khóa trước lượt đánh giá HGB.

- TN: 1915; FP: 17; FN: 8; TP: 60.
- Precision Failure: 77,92%.
- Recall Failure: 88,24%.
- F1: 0,82759.
- F2: 0,85960.
- Average precision: 0,92002.
- False negative rate: 11,76%.
- False positives trên 1.000 bản ghi: 8,5.
- Point-estimate gate: PASS.
- Independent new holdout: NO.
- Approved for release: NO.

Wilson 95%:
- Recall: khoảng 78,47%–93,92%.
- Precision: khoảng 67,46%–85,73%.

Khoảng tin cậy giả định các quan sát độc lập; không hiệu chỉnh ảnh hưởng
của việc tiếp tục phát triển sau khi đã xem kết quả test RF v1.
Không diễn giải việc đạt gate trên reused test là chứng nhận production.

### Slice validation cần chú ý

- Type M: recall 17/22 = 77,27%.
- Type H: chỉ có 2 Failure; recall 100% không đủ để kết luận chắc chắn.
- RPM trong khoảng train Q25–Q75: phát hiện 1/5 Failure.
- Tool wear trên train Q75: phát hiện 25/33 Failure.

Gate toàn bộ tập không có nghĩa mọi slice đều đạt gate.
Các slice ít Failure có độ bất định lớn; không cộng các slice chồng lấn.

## 7. Kiểm chứng phần mềm và tích hợp local

Các bằng chứng đã có:

- 32 tests inference/API/startup/contract/metrics PASS.
- 6 tests hành vi candidate PASS.
- API Docker thật trả cả NORMAL và FAILURE, score khớp offline.
- RPM sai bị từ chối bằng 422 VALIDATION_FAILED.
- Transformer hash sai: liveness vẫn 200, readiness 503,
  predict trả 503 MODEL_NOT_READY, không trả dự đoán giả.
- 13/13 kiểm tra đầu vào biên và sai hợp đồng PASS.
- Drift Worker loader đọc được reference của HGB.
- Sai model version hoặc dataset version bị loader từ chối.

13 tình huống input chỉ xác minh hành vi API.
Không dùng chúng làm bằng chứng accuracy, physical safety hoặc khả năng
chịu domain shift.

Chưa kiểm thử toàn bộ Drift Worker, DB và scheduling trong lượt này.

### Đo HTTP local

Mỗi cấu hình có 100 request đo, sau 10 warmup:

| Concurrency | Thành công | HTTP p50 ms | HTTP p95 ms | HTTP p99 ms | Request thành công/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 100/100 | 27.34 | 56.84 | 96.99 | 29.00 |
| 4 | 100/100 | 184.71 | 227.75 | 255.80 | 20.67 |

Client Windows, Docker local, timeout 10 giây, không retry,
urllib không cấu hình connection pooling riêng.

Concurrency tăng làm latency tăng và throughput giảm trong lượt đo.
Chưa xác định nguyên nhân bằng profiling.
Đây là phép đo nhỏ local, không phải production capacity hoặc SLO validation.

Một ảnh chụp docker stats sau phép đo ghi RAM 133,1 MiB.
Đây không phải peak memory trong lúc chịu tải.

## 8. Drift reference

- Dataset version: ai4i2020-train-split-v1-seed42.
- Reference source: training.
- Sample count: 6000.
- Chỉ mô tả sáu raw features.
- Numeric bins lấy từ train và được giữ cố định khi so sánh runtime.
- Mẫu dưới/trên biên được gom vào bucket biên, không bị loại.
- Categorical profile dùng L/M/H.
- Prediction reference Failure rate: 0.034166666666666665.

Prediction reference là tỷ lệ model dự đoán FAILURE trên train tại
threshold đã khóa; không phải tỷ lệ nhãn thật trong dataset.

SHA-256 drift reference:
3e397ddcd23aed2d0c38ab9102b092a089b729c21eb48fd3b21c65414feb378d

## 9. MLflow và truy xuất nguồn gốc

Local MLflow server/client: 3.17.1.

Đã kiểm chứng:
- Log params, metrics, tags và artifacts; đọc lại đối chiếu.
- Upload/download artifacts và kiểm tra SHA-256.
- Dữ liệu tracking còn nguyên sau restart local Compose.
- Đóng gói transformer cùng MLflow Model.
- Load sklearn và pyfunc trong process mới; so score trên 6000 mẫu train.
- Đăng ký model version và tải lại từ Registry.
- Đối chiếu package hashes và registry-generated metadata.

Run ID:
1adb4db4db6f40e0a73237b68d82d029

Model URI:
models:/maintai-local-hgb-candidate-1adb4db4/1

Registry status: READY.
Approved for release: NO.

READY xác nhận trạng thái version trong Registry, không thay thế
quyết định phê duyệt model.

Pipeline SHA-256:
f4bf8f91ee3b61596caad755b417f1d05c80c0bf814f587a3f80a5132745d0b2

Transformer SHA-256:
e6d3bda130933dac596f5b530520c87780949d42f49e8d313ca569b930ac22c0

Integration decision SHA-256:
d8c699bc75e6af5a12b0080855f2d61095284c4ea317058ad05434b8d38abc08

Registry package manifest SHA-256:
690571f408ec556d3cac469231a342f76cd1ee7f5c952498bc7429f39f40dc7c

SHA-256 kiểm tra tính toàn vẹn so với giá trị tin cậy;
không phải chữ ký phê duyệt hoặc bằng chứng nguồn gốc độc lập.

## 10. Đối chiếu bảy tiêu chí đánh giá

| Tiêu chí | Bằng chứng / giới hạn |
| --- | --- |
| Correctness and fitness for purpose | Bài toán binary failure; phạm vi candidate; chưa xác minh nhà máy thực |
| Validation versus software verification | Tách metrics model khỏi unit/contract/API tests |
| Metrics, baselines, thresholds, operating points | Dummy, rule, LR, cây; AP/F2/precision/recall; threshold khóa |
| Offline-evaluation pitfalls | Split/hash; fit trên train; CV train-only; công khai test reuse |
| Evaluation slices and behavioral testing | Slice Type/RPM/torque/wear; lỗi và đầu vào biên API |
| Invariants and metamorphic relations | Thứ tự cột/dòng, single/batch, save/load, score và nhãn-threshold |
| Test-data adequacy | Báo số Failure và Wilson interval; cảnh báo slice ít mẫu, domain shift |

## 11. Phần còn lại trước nghiệm thu đầy đủ

- Hoàn thiện README bàn giao, cách khởi động, xử lý lỗi và rollback.
- Chốt dependency và CI evidence cho đúng source commit bàn giao.
- Phối hợp kiểm tra Processing → ML trong môi trường nhóm.
- Hoàn thiện xác thực/authorization cho shared deployment theo contract.
- Hoàn thiện cơ chế lấy đúng artifact đã được phê duyệt khi khởi động.
- Phối hợp Platform về image publication, kho artifacts và GitOps release.
- Thống nhất quyết định nghiệm thu với giới hạn reused test hiện có.
- Bổ sung dữ liệu đánh giá độc lập hoặc validation phù hợp trước khi
  khẳng định khả năng tổng quát ngoài môi trường thử nghiệm.

Bản hiện tại cung cấp candidate có bằng chứng nghiên cứu và kiểm thử local;
chưa tuyên bố hoàn thành production release.