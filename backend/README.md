# Business Backend — một codebase, hai Deployments

`backend/` là **source ownership**, không phải một deployment ôm ingestion,
validation, inference và risk như v1.1. Target là System Contracts 1.3.1 §1/33.

| Runtime folder | Workload | Trách nhiệm |
| --- | --- | --- |
| [`business-api/`](business-api/Dockerfile) | Deployment `business-api` | REST/SSE, RBAC, actions/feedback/Copilot, Processing history facade, enqueue/poll durable jobs, read drift catalog. |
| [`business-consumer/`](business-consumer/Dockerfile) | Deployment `business-consumer` | Ba handlers telemetry/prediction/alert, latest projections/alert lifecycle và bounded durable job loop. |
| `shared/` | Không phải deployment | Alerts, audit, feedback, ordering, inbox, notifications và persistence của Business. |
| `migrations/` | Không phải deployment | Migrations chỉ thuộc `business_db`, dùng chung cho API/consumer. |

Hai runtime có thể dùng cùng image Business với entrypoint khác. Các handlers là
module trong một consumer runtime, không phải telemetry/prediction microservices.
`business_db` giữ machines/alerts/feedback và latest snapshots, không full raw hoặc
toàn bộ processing history. Consumer commit projection/inbox trước notification
đến API replicas; REST snapshot khôi phục khi mất SSE/NOTIFY.

Không query `processing_db`; gọi Processing read API/S3 theo contract. Không chạy
ML/risk tại Business. App callback/API ở đây hiện là skeleton; Kafka3 role loops
và transactional inbox dùng [messaging SDK](../messaging/README.md), hướng dẫn
Người1 tại [Kafka guide §6](../docs/KAFKA_DEVELOPMENT_GUIDE.md). Xem
[runtime map](../architecture/README.md) và [migration](../docs/repository-layout.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
