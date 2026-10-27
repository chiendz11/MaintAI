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

## PROC-ORDER-01 — transport source đã có, app còn cần tích hợp

Package transport0.1.0 ở processing/src/maintai_processing/ dùng SDK0.2.0 và
bundle1.1.0. Dockerfile Worker/Relay vẫn placeholder; full app chưa implement.

| Phần | Source đã có | Owner còn làm |
| --- | --- | --- |
| worker.py / handlers.py | Wrapper + factory đọc prepare/persist callbacks, đúng owner DB/group | Real DQ/ML/risk/history callbacks, health server và image |
| SDK keyed.py | Bounded FIFO từng máy, fair scheduling, safe offset ledger, drain/cancel | Chốt pool/budgets, load/rolling evidence |
| SDK ordering.py | Durable assignment/source cursor và atomic inbox/history callback/outbox | Migration/activation owner DB thật |
| outbox_relay.py | Wrapper cho ordered head-of-key relay | Metrics/probes/image/GitOps activation review |

Prepare chạy ngoài SQL với deadline/cancellation context. Persist nhận cùng
connection ghi history/risk/outbox; phải enqueue đúng một terminal result.
INVALID/UNAVAILABLE không phát signal. Duplicate dùng outcome cũ; source order
conflict fail closed. Offset coordinator không commit vượt lower-offset hole.

[Runbook/commands/tests](../docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md)
ghi rõ explicit001/002 + activation sau drain. Initial legacy inbox cần reviewed
cursor bootstrap; không xóa inbox để bật mode. [Read API](read-api/README.md) đã có
internal status SELECT-only; history/chart/archive/export chưa triển khai;
scheduler/coordinator không là deployment thứ tư.
