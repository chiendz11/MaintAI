# ML codebase — ba image: training, inference và MLflow

`ml-service/` là code owner, **không phải một deployment duy nhất**.

| Runtime folder | Workload | Image group | Trách nhiệm |
| --- | --- | --- | --- |
| [`inference/`](inference/Dockerfile) | Deployment `ml-service` | `ml-service` | Raw VALID features từ Processing → fitted preprocessing → predict. |
| [`training/`](training/Dockerfile) | Argo WorkflowTask `training` | `training` | Approved curated data → train/evaluate/export model bundle. |
| [`mlflow/`](mlflow/README.md) | Platform Deployment `mlflow` | `mlflow` | Đóng gói upstream Tracking/Registry/UI, local dev và app CI publisher; production manifests ở GitOps. |
| [`shared/`](shared/README.md) | Không có runtime | Không có image riêng | Shared preprocessing và compatible artifact classes. |
| `artifacts/` | Không có runtime | — | Local generated output, không commit vào Git. |

Mỗi runtime có Dockerfile và dependencies riêng.

Trạng thái đã kiểm chứng:

- Training: image batch đã build và chạy fit/evaluate RF và HGB có interaction
  features trong Docker Linux. Dataset và đầu vào ablation được mount chỉ đọc;
  output ghi riêng; chạy không cần mạng.
- Tái lập Windows–Linux: metrics, threshold và prediction trên 2.000 mẫu
  validation khớp cho cả RF và HGB với rtol=1e-10, atol=1e-12.
  Kết luận giới hạn ở hai lượt chạy đã kiểm tra, không yêu cầu artifact
  giống từng byte trên mọi môi trường.
- Inference: image HGB candidate đã chạy với bundle thật; kiểm tra health,
  prediction, validation, metrics và lỗi checksum transformer đã PASS.
- MLflow: server local và PostgreSQL đã chạy; tracking, artifact round-trip,
  persistence sau restart và Registry candidate đã được kiểm chứng.

Training entrypoint hiện thực hiện fit/evaluate. Export, tracking và registration
được thực hiện bằng các script riêng; không tự approve hoặc release.
Inference load preprocessing đã fit, không fit lại trên request.

HGB là integration candidate, chưa được duyệt release. Kết quả HGB trên tập test
đã từng được sử dụng là reused-test evidence, không phải holdout độc lập mới.
Xem [model card](training/MODEL_CARD_HGB.md) và
[hướng dẫn bàn giao](inference/HGB_HANDOFF.md).

Argo Workflow, approved curated S3, tải artifact được duyệt khi khởi động,
xác thực cho shared deployment và release qua GitOps chưa được xác minh
end-to-end trong các kiểm thử local này.

Image identity độc lập với model/preprocessor/dataset version. Model mới có thể
dùng lại serving image nếu serialization, transformer và dependencies tương thích.

`training/`: data_preparation, model, evaluation, export, tests.
`inference/`: api, model_loader, prediction, observability, tests.
Caller `/predict` là **Processing Worker**; Business API/Consumer không tính ML/risk.
Training bundle phát hành qua GitOps PR, không promote chỉ vì raw S3 upload,
drift hoặc feedback. MLflow Server sở hữu mlflow_db, trainer chỉ gọi MLflow API.

Đọc System Contracts v1.4 §7–8/34–41 và Guide §15–16/43/49. Machine-readable HTTP
bundle hiện baseline1.3.2, xem [migration status](../docs/architecture/repository-layout.md).
MLflow image được publish bởi workflow MaintAI `maintai-publish.yml`; GitOps chỉ
nhận signed digest qua PR. ML review client/server compatibility; Platform/MLOps
vận hành service, credentials, DB/S3 và release. Không fork MLflow hoặc viết lại UI.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/subsystems/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/architecture/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
