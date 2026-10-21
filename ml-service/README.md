# ML codebase — training và inference là hai workload

`ml-service/` là code owner, **không phải một deployment duy nhất**.

| Runtime folder | Workload | Image group | Trách nhiệm |
| --- | --- | --- | --- |
| [`inference/`](inference/Dockerfile) | Deployment `ml-service` | `ml-service` | Raw VALID features từ Processing → fitted preprocessing → predict. |
| [`training/`](training/Dockerfile) | Argo WorkflowTask `training` | `training` | Approved curated data → train/evaluate/export model bundle. |
| [`shared/`](shared/README.md) | Không có runtime | Không có image riêng | Shared preprocessing và compatible artifact classes. |
| `artifacts/` | Không có runtime | — | Local generated output, không commit vào Git. |

Mỗi runtime có `Dockerfile` riêng để nhìn rõ ranh giới triển khai và `runtime.json`
để bổ sung metadata. Hai Dockerfile hiện là placeholder. Chưa có entrypoint, dependency
lock hoặc image được build. Khi implement, training/inference dùng dependency và
entrypoint riêng; không import training vào serving. Image version/digest độc
lập model/preprocessor/dataset version; model mới có thể dùng lại serving image
nếu serialization/runtime tương thích.

`training/`: data_preparation, model, evaluation, export, tests.
`inference/`: api, model_loader, prediction, observability, tests.
Caller `/predict` là **Processing Worker**; Business API/Consumer không tính ML/risk.
Training bundle phát hành qua GitOps PR, không promote chỉ vì raw S3 upload,
drift hoặc feedback. MLflow Server sở hữu mlflow_db, trainer chỉ gọi MLflow API.

Đọc System Contracts v1.3 §7–8/31–33 và Guide §15–16. Machine-readable OpenAPI
hiện vẫn là baseline v1.1, xem [migration status](../docs/repository-layout.md).
Trainer tham chiếu trong repo MaintAI_gitops vẫn ở runtime/maintai_platform/train.py;
skeleton này chưa di chuyển code đó hoặc đổi deployment manifests/CI publisher.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
