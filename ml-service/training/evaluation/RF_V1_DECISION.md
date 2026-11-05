# Quyết định đánh giá Random Forest v1

## Trạng thái

Không đạt quality gate trên held-out test.
Không phê duyệt release.

## Lựa chọn trước khi mở test

Candidate: random_forest.
Threshold: 0.22747921181939895.
Model fit trên 6000 dòng train, không refit trước test.

Model SHA-256:
3fd4df75163efbd2b9945794760e25dff2398a612e25ee031e4314dda94c3ef9

Frozen selection SHA-256:
4ddd760641a6147fc6d4481aa99f1c9553d2e65eb9f1d1ad7da73e4f0d672857

Random Forest được chọn vì đạt đồng thời hai gate trên validation
và có F2 cao nhất trong nhóm candidate đạt gate.

HistGradientBoosting có AP validation cao hơn nhưng F2 tại operating
point đã chọn thấp hơn. Không chọn model bằng kết quả test.

## Kết quả held-out test

- Số mẫu: 2000.
- Failure thật: 68.
- TP: 55.
- FN: 13.
- FP: 58.
- TN: 1874.
- Precision Failure: 0.48672566371681414.
- Recall Failure: 0.8088235294117647.
- F1 Failure: 0.6077348066298343.
- F2 Failure: 0.7142857142857143.
- Average Precision: 0.7071816429863165.
- False positives / 1000 records: 29.0.

Quality gate:
Recall >= 0.85 AND Precision >= 0.50.
Kết quả: FAIL cả hai điều kiện.

Khoảng Wilson 95%:
- Recall: [0.699893585399794, 0.8847270596907507].
- Precision: [0.3965321831240263, 0.5777919991458662].

Các khoảng trên giả định quan sát độc lập; không bao phủ domain shift
hoặc toàn bộ độ bất định của quá trình phát triển model.

Test report SHA-256:
fa43e175d7fdbc8c124efab5936a64625b26e97b0bcda72772599761f336285e

## Giới hạn và diễn giải

Model đạt validation nhưng không đạt test.
Validation có thể lạc quan sau khi dùng để chọn candidate và threshold.
Số Failure nhỏ làm các ước lượng dao động đáng kể.
Chưa đủ bằng chứng xác định một nguyên nhân duy nhất cho chênh lệch.

Behavioral tests PASS chứng minh các hành vi phần mềm đã kiểm tra;
không chứng minh chất lượng dự đoán đạt yêu cầu.

## Hành động

- Giữ nguyên model, threshold, selection và test report v1.
- Không tune threshold theo test.
- Không thử các candidate còn lại trên test để chọn model thắng.
- Không hạ gate hoặc dùng human review để thay thế recall của model.
- Có thể lưu run và artifacts lên MLflow với trạng thái rejected /
  not approved; không gắn trạng thái Production hoặc approved.
- Vòng nghiên cứu tiếp theo chỉ dùng dữ liệu phát triển và phải có
  protocol đánh giá mới được ghi lại trước khi thực hiện.

Test này đã được mở. Nếu tiếp tục phát triển sau khi biết kết quả,
mọi lần đánh giá lại trên cùng test phải công bố đây là benchmark
đã được sử dụng, không mô tả là một lần kiểm định độc lập mới.