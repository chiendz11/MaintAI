# Processing — một owner, ba runtime folders

| Runtime folder | Workload | Trách nhiệm |
| --- | --- | --- |
| [`worker/`](worker/Dockerfile) | Deployment `processing-worker` | Kafka → DQ → ML → risk; persist history/outbox. |
| [`read-api/`](read-api/Dockerfile) | Deployment `processing-read-api` | API nội bộ cho status/history/VALID windows. |
| [`outbox-relay/`](outbox-relay/Dockerfile) | Deployment `processing-outbox-relay` | Durable outbox → Kafka `inference.results` và `alert.signals`. |
| `shared/` | Không phải deployment | data_quality, inference_client, risk_engine, events, ordering, outbox và persistence. |
| `migrations/` | Không phải deployment | Schema `processing_db`, một owner cho cả ba runtime. |

Skeleton chọn relay thành runtime riêng để thấy rõ deployment/restart boundary;
Guide §6 cho phép các entrypoints cùng Processing codebase. Không có ba DB riêng.
Risk tạo alert signal; Business mới dedupe/tạo/quản lý alert. Kafka offset chỉ
commit sau durable side effects; retry cần stable IDs/inbox/outbox.

Các module DQ/inference client/risk được chuyển từ backend skeleton cũ sang đây.
Shared [messaging SDK](../messaging/README.md) có durable Kafka consumer,
inbox/outbox SQL, OutcomeFactory và relay. Người1 tích hợp callback DQ/ML/risk,
history SQL và owner migrations; Read API và app entrypoints/probes vẫn skeleton.
Writer result1.4.0, reader hỗ trợ legacy2.0.0. Xem
[hướng dẫn nhóm §5](../docs/KAFKA_DEVELOPMENT_GUIDE.md) và
[runtime map](../architecture/README.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
