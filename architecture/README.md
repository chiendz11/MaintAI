# Runtime inventory baseline1.3.2 — docs target v1.4

[Deployment Architecture v1.4](../docs/DEPLOYMENT_ARCHITECTURE.md) mô tả target mới.
Bảng và runtime JSON dưới đây là inventory đã có baseline1.3.2, chưa tự đồng bộ
quarantine/provenance. Simulator UI đã có React client/demo, descriptor và Dockerfile
riêng; Sensor Control provider0.3.0 đã implement; chưa có container/rollout evidence. MLflow là platform workload ở
repo GitOps/hạ tầng, không cộng lẫn với14 app runtime folders.

**Mỗi thư mục runtime có `Dockerfile` riêng tương ứng một workload**.
`runtime.json` bổ sung loại workload, source owner và trạng thái implementation.
`backend/`, `processing/`, `ml-service/` là codebase/ownership containers, không phải
deployment. Các thư mục `shared/`, `src/handlers/` và `migrations/` là module dùng
chung trong một owner. Metadata này không phải Kubernetes manifests.

| Runtime | Thư mục | Workload | DB owner | Trạng thái |
| --- | --- | --- | --- | --- |
| `sensor` | [`sensor/`](../sensor/Dockerfile) | Deployment | — | implemented_sensor_provider |
| `ingestion-api` | [`ingestion-api/`](../ingestion-api/Dockerfile) | Deployment | — | implemented_local (M1) |
| `processing-worker` | [`processing/worker/`](../processing/worker/Dockerfile) | Deployment | processing_db | skeleton |
| `processing-read-api` | [`processing/read-api/`](../processing/read-api/Dockerfile) | Deployment | processing_db | skeleton |
| `processing-outbox-relay` | [`processing/outbox-relay/`](../processing/outbox-relay/Dockerfile) | Deployment | processing_db | skeleton |
| `storage-consumer` | [`storage-consumer/`](../storage-consumer/Dockerfile) | Deployment | — | skeleton |
| `business-api` | [`backend/business-api/`](../backend/business-api/Dockerfile) | Deployment | business_db | skeleton |
| `business-consumer` | [`backend/business-consumer/`](../backend/business-consumer/Dockerfile) | Deployment | business_db | skeleton |
| `ml-service` | [`ml-service/inference/`](../ml-service/inference/Dockerfile) | Deployment | — | skeleton |
| `training` | [`ml-service/training/`](../ml-service/training/Dockerfile) | WorkflowTask | — | skeleton |
| `rag-service` | [`rag-service/`](../rag-service/Dockerfile) | Deployment | knowledge_db | skeleton |
| `frontend` | [`frontend/`](../frontend/Dockerfile) | Deployment | — | skeleton |
| `simulator-ui` | [`sensor-simulator-UI/`](../sensor-simulator-UI/Dockerfile) | Deployment | — | frontend_and_sensor_provider_implemented |
| `drift-worker` | [`drift-worker/`](../drift-worker/Dockerfile) | CronJob | drift_db | legacy_partial |

`business-api` và `business-consumer` cùng owner/codebase/migrations và có thể
dùng cùng image `business-backend` với entrypoint khác. Processing worker/read API/
outbox relay cũng cùng owner/image group nhưng được tách runtime trong skeleton.
ML training và inference dùng hai image group độc lập. `WorkflowTask` là batch task
trong Argo Workflow; `CronJob` chạy định kỳ, khác Deployment thường trực.

Kafka/PostgreSQL/S3 hoặc MinIO/MLflow Server/Argo/monitoring thuộc GitOps và hạ tầng
ở hai repo riêng; không tạo platform manifests trong app repo. Xem
[runtime-map.json](runtime-map.json) và [ghi chú migration](../docs/repository-layout.md).


## Chi tiết kế thừa1.3.2 và quyết định dự án

Simulator UI có [hướng dẫn chạy](../sensor-simulator-UI/README.md), demo driver và live
client theo baseline5 operations, OIDC và command recovery. Browser checks dùng
API fixtures và provider HTTP thật; provider có TLS JWKS/OAuth fixtures. Image chưa
build/deploy, production IdP/Kafka integration chưa verify.
Business API bỏ control
facade; Sensor giữ control backend. Processing maintenance và Business job loops
nằm trong owner runtimes hiện có, chưa thêm deployment mới. History/archive/auth/
recovery giữ chi tiết tại [docs index](../docs/README.md) và các phụ lục được dẫn ở đó.
Runtime JSON đã mô tả target1.3.2; Dockerfiles/source còn cần implement. Bảng
trên không chứng minh entrypoint hoặc deployment đã chạy.
