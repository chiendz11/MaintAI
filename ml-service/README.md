# ML codebase — ba image: training, inference và MLflow

`ml-service/` là code owner, **không phải một deployment duy nhất**.

| Runtime folder | Workload | Image group | Trách nhiệm |
| --- | --- | --- | --- |
| [`inference/`](inference/Dockerfile) | Deployment `ml-service` | `ml-service` | Raw VALID features từ Processing → fitted preprocessing → predict. |
| [`training/`](training/Dockerfile) | Argo WorkflowTask `training` | `training` | Approved curated data → train/evaluate/export model bundle. |
| [`mlflow/`](mlflow/README.md) | Platform Deployment `mlflow` | `mlflow` | Đóng gói upstream Tracking/Registry/UI, local dev và app CI publisher; production manifests ở GitOps. |
| [`shared/`](shared/README.md) | Không có runtime | Không có image riêng | Shared preprocessing và compatible artifact classes. |
| `artifacts/` | Không có runtime | — | Local generated output, không commit vào Git. |

Mỗi runtime có `Dockerfile` riêng để nhìn rõ ranh giới triển khai và `runtime.json`
để bổ sung metadata. Training/inference còn placeholder; MLflow có Dockerfile,
dependency locks/local Compose/publisher nhưng chưa pin base hoặc build image.
Khi implement, training/inference dùng dependency và
entrypoint riêng; không import training vào serving. Image version/digest độc
lập model/preprocessor/dataset version; model mới có thể dùng lại serving image
nếu serialization/runtime tương thích.

`training/`: data_preparation, model, evaluation, export, tests.
`inference/`: api, model_loader, prediction, observability, tests.
Caller `/predict` là **Processing Worker**; Business API/Consumer không tính ML/risk.
Training bundle phát hành qua GitOps PR, không promote chỉ vì raw S3 upload,
drift hoặc feedback. MLflow Server sở hữu mlflow_db, trainer chỉ gọi MLflow API.

Đọc System Contracts v1.4 §7–8/34–41 và Guide §15–16/43/49. Machine-readable HTTP
bundle hiện baseline1.3.2, xem [migration status](../docs/repository-layout.md).
MLflow image được publish bởi workflow MaintAI `maintai-publish.yml`; GitOps chỉ
nhận signed digest qua PR. ML review client/server compatibility; Platform/MLOps
vận hành service, credentials, DB/S3 và release. Không fork MLflow hoặc viết lại UI.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
