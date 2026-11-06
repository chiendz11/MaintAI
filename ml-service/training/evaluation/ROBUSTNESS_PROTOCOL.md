# Robustness protocol — frozen HGB v1

## Mục tiêu

Đo độ nhạy của model đã khóa khi số đo đầu vào bị nhiễu hoặc lệch.
Không dùng kết quả để sửa model, threshold hoặc chọn seed trong lượt này.

## Dữ liệu và model

- Dùng 2.000 bản ghi validation theo frozen split.
- Không đánh giá test.
- HGB interactions và threshold giữ nguyên theo integration_candidate.json.
- Kiểm tra hash model, transformer, dataset và split trước khi chạy.
- Baseline score phải khớp validation_predictions.csv trước khi thử nhiễu.

## Kịch bản

Mỗi kịch bản chỉ thay đổi một sensor; các feature còn lại giữ nguyên.
Transformer tự tính lại các feature tương tác từ raw features đã thay đổi.

### Nhiễu đo giả lập

- Gaussian, trung bình 0.
- Độ lệch chuẩn bằng 1% và 5% IQR của sensor trên train.
- Thử riêng từng sensor số trong năm sensor.
- Seeds cố định: 11, 22, 33, 44, 55.
- Không chọn seed theo kết quả.
- Nếu IQR bằng 0, ghi rõ kịch bản không tạo nhiễu hữu ích.

### Sai lệch hệ thống

- Cộng hoặc trừ 5% IQR train vào một sensor.
- Thử riêng từng sensor.
- Đây là mức perturbation phục vụ thí nghiệm, không phải thông số
  sai số cảm biến đã được nhà máy xác nhận.

## Input không hợp lệ

- Không clip, sửa hoặc âm thầm bỏ mẫu vi phạm.
- Đếm riêng mẫu không hữu hạn, RPM <= 0 hoặc tool wear < 0.
- Chỉ suy luận các mẫu còn hợp lệ.
- Báo số mẫu hợp lệ và bị từ chối của từng kịch bản.
- Baseline so sánh phải dùng đúng cùng các hàng hợp lệ đó.

## Metrics chính

- Số lượng và tỷ lệ input hợp lệ/bị từ chối.
- Mean absolute score change và p95 absolute score change.
- Tỷ lệ đổi nhãn so với baseline.
- NORMAL -> FAILURE và FAILURE -> NORMAL.
- Số lần chạy, seed và mức perturbation.

Không tính precision/recall cho dữ liệu perturbation trong protocol này:
chưa có bằng chứng nhãn thật giữ nguyên khi điều kiện vận hành thay đổi.

## Diễn giải

- Đổi nhãn sau perturbation là độ nhạy, không tự động là prediction sai.
- Nhãn không đổi cũng không chứng minh model vẫn chính xác.
- Không giả định score phải tăng đơn điệu theo mọi sensor.
- Báo từng seed và tổng hợp; không coi các lượt trên cùng bản ghi
  là các mẫu kiểm thử độc lập.
- Chưa đặt gate robustness vì chưa có yêu cầu sai số cảm biến/SLO
  được thống nhất. Kết quả là characterization, không certification.

## Kiểm soát

- Không retrain, calibrate hoặc tune threshold.
- Không thay bundle đã bàn giao.
- Lưu cấu hình, input hashes, script hash, phiên bản thư viện và kết quả.
- Kiểm chứng phần mềm chạy đúng tách biệt với kết luận model có robust hay không.