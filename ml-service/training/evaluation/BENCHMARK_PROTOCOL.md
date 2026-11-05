# MaintAI — Protocol so sánh mô hình v1

## 1. Mục tiêu và phạm vi

Dự đoán nhãn Machine failure từ sáu đặc trưng hợp lệ của AI4I 2020.

Đây là bài toán phân loại trạng thái theo bản ghi điều kiện vận hành.
Chưa có bằng chứng dự báo hỏng trước một khoảng thời gian cụ thể hoặc
ước lượng remaining useful life (RUL).

Mô hình hỗ trợ hệ thống cảnh báo; ML không tự quyết định severity,
tạo alert hoặc điều khiển máy.

Quality gate:
- Recall Failure >= 0.85.
- Precision Failure >= 0.50.
- Hai điều kiện phải đồng thời đạt tại cùng một threshold.

Gate trên validation dùng để chọn candidate.
Đánh giá cuối trên held-out test được báo cáo riêng.
Không tuyên bố đạt gate test chỉ từ kết quả validation.

## 2. Dữ liệu và chống leakage

Dataset: AI4I 2020, UCI.

SHA-256 CSV:
dc6630cd9b1f0f853922fad78a1b6436570d3f1ec863f1dd5c4340ac56bc8a8e

Split manifest:
data_preparation/splits/ai4i-stratified-v1-seed42.json

SHA-256 split:
94889871b1c814f5d36d67357c44b6fabc68a18040e7674658a64f7761483752

Split cố định:
- Train: 6000 dòng, 203 Failure.
- Validation: 2000 dòng, 68 Failure.
- Test: 2000 dòng, 68 Failure.
- Seed: 42, không lựa chọn seed theo kết quả.

Chỉ dùng sáu features:
- product_type
- air_temperature_k
- process_temperature_k
- rotational_speed_rpm
- torque_nm
- tool_wear_min

Không dùng UDI, Product ID, timestamps, target hoặc các cột
TWF/HDF/PWF/OSF/RNF làm feature.

Fit preprocessing, resampling và model chỉ trên train.
Không fit scaler/encoder trên toàn bộ dataset.
Không loại outlier chỉ để cải thiện điểm.
Không dùng test để chọn model, feature, tham số hoặc threshold.

Random stratified split đánh giá khả năng tổng quát trong phạm vi
dataset này; không chứng minh tổng quát theo thời gian, theo máy
hoặc trên dữ liệu nhà máy thực tế.

## 3. Các phương án đối chiếu

| Phương án | Câu hỏi cần kiểm chứng |
| --- | --- |
| Dummy luôn Normal | Accuracy cao do lệch lớp có che giấu việc bỏ sót toàn bộ Failure không? |
| Rule-based đơn giản | Model học được có mang lại lợi ích so với luật cảnh báo đơn giản không? |
| Logistic Regression hiện tại | Baseline tuyến tính giải quyết được đến đâu? |
| Logistic Regression, class_weight=balanced | Tăng trọng số Failure thay đổi recall và precision thế nào? |
| Decision Tree | Quan hệ theo ngưỡng và tương tác phi tuyến có hữu ích không? |
| Random Forest | Kết hợp nhiều cây có cải thiện so với một cây không? |
| Một model Gradient Boosting | Học các cây bổ sung tuần tự có cải thiện chất lượng với chi phí chấp nhận được không? |

Danh sách trên là kế hoạch thử nghiệm, chưa phải kết luận model thắng.

Luật đối chứng phải ghi rõ công thức, nguồn và cách chọn ngưỡng.
Nếu dùng thông tin về cách sinh nhãn AI4I, phải công bố đây là
đối chứng có sử dụng kiến thức sinh dữ liệu, không coi là bằng chứng
độc lập về hiệu quả trên nhà máy thực tế.

Không biến nhãn cứng của rule-based thành xác suất được hiệu chỉnh.

## 4. So sánh công bằng và chọn threshold

Mọi candidate dùng cùng train/validation và cùng sáu đầu vào.

Với model trả score Failure, báo cáo hai operating points:
1. Threshold mặc định 0.5 để đối chiếu.
2. Threshold được chọn trên validation theo quy tắc dưới đây.

Dự đoán FAILURE khi score >= threshold, ngược lại NORMAL.

Xét các threshold tạo ra quyết định khác nhau trên validation,
kèm các điểm biên 0 và 1.

Nếu có threshold đạt cả recall >= 0.85 và precision >= 0.50:
- Chọn threshold có F2 cao nhất trong nhóm đạt gate.
- Nếu hòa: ưu tiên recall cao hơn, tiếp theo precision cao hơn.
- Nếu vẫn hòa: chọn threshold lớn hơn để quyết định xác định.

Nếu không có threshold đạt gate:
- Báo rõ candidate không đạt.
- Có thể báo operating point F2 tốt nhất để phân tích.
- Không gắn operating point đó thành model đạt yêu cầu.

F2 ưu tiên recall hơn precision nhưng không phải ma trận chi phí
kinh tế của nhà máy. Không tự bịa chi phí false negative/false positive.

## 5. Metrics và lựa chọn candidate

Báo cáo trên validation:
- Precision Failure, Recall Failure.
- F1, F2.
- Average Precision (AP); ghi rõ đây là AP, không nhập nhằng với
  diện tích PR tính bằng quy tắc hình thang.
- TN, FP, FN, TP.
- False negative rate.
- False positives trên 1000 bản ghi.
- Threshold.
- Thời gian fit, thời gian predict và dung lượng artifact.

Đo thời gian trong cùng môi trường, ghi phiên bản thư viện,
phần cứng, batch size và cách warm-up.
Không coi một request đơn lẻ là p95 hoặc latency toàn hệ thống.

Lựa chọn provisional candidate:
1. Chỉ xét nhóm đạt đồng thời hai quality gates trên validation.
2. Ưu tiên F2 validation cao nhất tại threshold đã chọn.
3. Khi bằng nhau, ưu tiên AP cao hơn.
4. Nếu tiếp tục bằng nhau, ưu tiên model đơn giản hơn.

Kiểm tra slices, hành vi lỗi và giới hạn tài nguyên trước khi chốt.
Nếu một vấn đề khiến candidate bị loại, ghi rõ bằng chứng và lý do.

Nếu không model nào đạt, kết luận chưa đạt; không tự hạ gate.
Ghi lại mọi thay đổi protocol thành phiên bản mới.

## 6. Đánh giá cuối

Khóa model, preprocessing, tham số và threshold trước khi đánh giá test.

Phiên bản đầu giữ model fit trên train; không tự refit train+validation,
vì refit có thể thay đổi phân phối score và operating point đã chọn.

Báo cáo test riêng, gồm confusion matrix và độ bất định của metrics.
68 Failure là số lượng nhỏ: một prediction thay đổi recall khoảng
1.47 điểm phần trăm.

Nếu test không đạt gate, báo cáo trung thực.
Không tiếp tục tune theo các lỗi test rồi gọi test đó là độc lập.

## 7. Bảy nhóm bằng chứng

| Tiêu chí | Bằng chứng cần có |
| --- | --- |
| Correctness and fitness for purpose | Định nghĩa bài toán, giới hạn sử dụng, quality gate, model card |
| Validation versus software verification | Báo cáo model riêng với unit/contract/startup/integration tests |
| Metrics, baselines, thresholds, operating points | Bảng candidates, AP, F2, threshold, FP/FN và quy tắc lựa chọn |
| Offline-evaluation pitfalls | Frozen split, chống leakage, train-only preprocessing, giới hạn random split |
| Evaluation slices and behavioral testing | L/M/H, vùng sensor, failure modes khi phù hợp, input lỗi và model unavailable |
| Invariants and metamorphic relations | Đổi ID/thứ tự field không đổi score; score hữu hạn; nhãn đúng threshold; save/load nhất quán |
| Test-data adequacy | Số mẫu và Failure từng slice, độ bất định, tình huống chưa được bao phủ |

Không tự giả định tăng một sensor luôn làm nguy cơ tăng.
Quan hệ đơn điệu chỉ được kiểm thử như yêu cầu khi có căn cứ miền nghiệp vụ.

## 8. Tracking và kiểm soát model

Mỗi thí nghiệm lưu:
- Dataset/split hash và source identity.
- Candidate, tham số, seed, preprocessing.
- Phiên bản thư viện.
- Metrics và threshold.
- Artifact hash và MLflow run ID.

So sánh các run trước khi lựa chọn.
Register model không đồng nghĩa được phê duyệt deploy.
Chỉ phát hành bundle được kiểm chứng, đúng version và đúng quy trình nhóm.