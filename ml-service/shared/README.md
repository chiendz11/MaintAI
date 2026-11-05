# Code dùng chung giữa training và inference

`preprocessing/feature_interactions.py` cung cấp `SensorInteractions`,
nhận sáu raw features theo contract và tạo ba feature dẫn xuất khi enabled:

- Chênh lệch nhiệt độ: process_temperature_k - air_temperature_k.
- Công suất cơ học: torque_nm × rotational_speed_rpm × 2π / 60, đơn vị W.
- Tương tác độ mòn và mô-men: tool_wear_min × torque_nm.

Các phép biến đổi không sử dụng target hoặc failure-mode labels.
API vẫn nhận sáu raw features; bên gọi không phải tính feature dẫn xuất.

Training fit các thành phần cần học trên train split và lưu cùng model trong
pipeline. Inference load pipeline đã fit, không fit lại trên request.

Transformer phải giữ import path tương thích khi lưu/load artifact.
Bundle HGB kèm source transformer và SHA-256; inference kiểm tra source đang
được import trước khi chấp nhận model. MLflow package cũng kèm transformer;
load bằng sklearn và pyfunc đã được kiểm tra trong process mới.

Đây là source dùng chung khi build, không phải service hoặc image riêng.
Không đặt training entrypoint hoặc HTTP handler tại đây.
HTTP/payload/artifact contracts chuẩn vẫn nằm trong `contracts/` và
System Contracts; thư mục này không tạo thêm bộ schema riêng.