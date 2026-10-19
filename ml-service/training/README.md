# Image training — batch workload

Skeleton cho image logic `training`, chạy như một task trong Argo Workflow.
Không có HTTP inference endpoint. Ranh giới workload nằm ở [Dockerfile](Dockerfile) và [runtime.json](runtime.json).

| Thư mục | Trách nhiệm khi implement |
| --- | --- |
| `data_preparation/` | Đọc approved dataset, kiểm tra sáu features và target, chia train/validation/test với seed. |
| `model/` | Fit preprocessing từ `../shared/preprocessing/` trên train, huấn luyện candidates, chọn threshold trên validation. |
| `evaluation/` | Đánh giá recall/precision/F1/PR-AUC và quality gates; không dùng test để chọn model/threshold. |
| `export/` | Xuất model, fitted preprocessor, metadata, evaluation và drift reference khớp model/dataset version. |
| `tests/` | Placeholder cho kiểm tra training khi có implementation. |

Drift reference lấy từ training split hoặc approved reference theo contract.
Dataset và labels không được đưa vào serving request. Model không tự phát hành
chỉ vì training thành công: candidate đạt quality gate mới đi qua PR GitOps.

Entrypoint batch, dependency lock và runtime versions sẽ được chốt khi triển khai.
Khi implement image, build context có thể là root repo MaintAI; image lấy `training/` và `shared/`,
không cần đóng gói source inference. Xem [ranh giới hai image](../README.md).
