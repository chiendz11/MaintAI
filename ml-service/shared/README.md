# Code dùng chung giữa hai image

`preprocessing/` giữ định nghĩa sáu features, thứ tự/category và preprocessing để
training/serving dùng cùng một cách biến đổi dữ liệu. Hiện chỉ có `.gitkeep`.

Training fit encoder/scaler trên train split và xuất fitted artifact. Inference
load artifact đó để transform, không fit lại trên dữ liệu runtime. Nếu artifact
tham chiếu class/module dùng chung, import path và version phải tương thích ở
cả hai image.

Đây là source dùng chung khi build, không phải service hoặc image thứ ba. Không
đặt training entrypoint, HTTP handler hoặc dependencies chỉ dành cho training ở
đây. HTTP/payload/artifact contracts chuẩn vẫn nằm trong `MaintAI/contracts/` và
System Contracts; thư mục này không tạo thêm một bộ schema riêng.
