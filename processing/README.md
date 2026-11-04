# Processing — một owner, ba runtime folders

**RISK-POLICY-01:** Source policy/schema/catalog ở
[`config/risk-policies/`](config/risk-policies/README.md). GitOps chọn version/hash
và tạo ConfigMap read-only cho Worker; infrastructure không sở hữu nội dung
policy. Xem [quyết định30/10](../docs/RISK_POLICY_OWNERSHIP.md). Policy demo dùng
ngưỡng tham khảo, không có production calibration hoặc guarantee tạo Alert.

| Runtime folder | Workload | Trách nhiệm |
| --- | --- | --- |
| [`worker/`](worker/Dockerfile) | Deployment `processing-worker` | Kafka → DQ → ML → risk; persist history/outbox. |
| [`read-api/`](read-api/Dockerfile) | Deployment `processing-read-api` | Status/history/chart; M6 snapshot/export runner và metadata writer quyền hẹp. |
| [`outbox-relay/`](outbox-relay/Dockerfile) | Deployment `processing-outbox-relay` | Durable outbox → Kafka `inference.results` và `alert.signals`. |
| `shared/` | Không phải deployment | data_quality, inference_client, risk_engine, events, ordering, outbox và persistence. |
| `migrations/` | Không phải deployment | Schema `processing_db`, một owner cho cả ba runtime. |

Skeleton chọn relay thành runtime riêng để thấy rõ deployment/restart boundary;
Guide §6 cho phép các entrypoints cùng Processing codebase. Không có ba DB riêng.
Risk tạo alert signal; Business mới dedupe/tạo/quản lý alert. Kafka offset chỉ
commit sau durable side effects; retry cần stable IDs/inbox/outbox.

Các module DQ/inference client/risk được chuyển từ backend skeleton cũ sang đây.
Shared [messaging SDK](../messaging/README.md) có durable Kafka consumer,
inbox/outbox SQL, OutcomeFactory và relay. Source hiện có callbacks DQ/ML/risk,
history SQL, owner migrations, app entrypoints/probes và Read API.
Writer và reader chỉ outcome1.4.0; không tương thích legacy2.0.0. Xem
[hướng dẫn nhóm §5](../docs/KAFKA_DEVELOPMENT_GUIDE.md) và
[runtime map](../architecture/README.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.

## PROC-ORDER-01 — transport source đã có, app còn cần tích hợp

Package ở processing/src/maintai_processing/ dùng SDK0.3.1 và bundle2.0.0.
Worker/Relay có app entrypoints và images; metadata/image source không tự chứng
minh activation hoặc triển khai shared.

| Phần | Source | Giới hạn vận hành |
| --- | --- | --- |
| worker.py / handlers.py | Prepare/persist DQ/ML/risk/history, đúng owner DB/group | Provider credentials, release và load cần kiểm riêng |
| SDK keyed.py | Bounded FIFO từng máy, fair scheduling, safe offset ledger, drain/cancel | Chốt pool/budgets, load/rolling evidence |
| SDK ordering.py | Durable assignment/source cursor và atomic inbox/history callback/outbox | Migration/activation owner DB thật |
| outbox_relay.py | Ordered head-of-key relay, metrics/probes và image | GitOps activation và sizing cần kiểm riêng |

Prepare chạy ngoài SQL với deadline/cancellation context. Persist nhận cùng
connection ghi history/risk/outbox; phải enqueue đúng một terminal result.
INVALID/UNAVAILABLE không phát signal. Duplicate dùng outcome cũ; source order
conflict fail closed. Offset coordinator không commit vượt lower-offset hole.

[Runbook/commands/tests](../docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md)
ghi rõ explicit001/002 + activation sau drain. Initial legacy inbox cần reviewed
cursor bootstrap; không xóa inbox để bật mode. [Read API](read-api/README.md) đã có
internal status/point-read và M6 history/chart/raw/export. Source pool SELECT-only,
metadata/job/catalog pools dùng credential riêng trong cùng Read API; xem
[cấu hình M6](read-api/HISTORY.md). Outcomes giữ DB, không bật processed archive
hoặc source cleaner; scheduler/coordinator không là deployment thứ tư.

## Đóng gói source distribution

Chạy `python -m build processing` tại gốc repository trong môi trường đã cài
công cụ build. Hai schema M6 lấy từ nguồn chuẩn `contracts/v1.3/schemas/`.
Command sdist sao chép chúng vào `contracts/v1.3/schemas/` bên trong archive và
đổi đường dẫn data-files trong bản metadata của archive. Không tạo bản sao schema
được quản lý bằng Git trong thư mục Processing hoặc sửa metadata checkout.

Source distribution tự chứa đủ đầu vào để build wheel ở ngoài repository.
Wheel tiếp tục cài schema vào `share/maintai-processing/contracts`; đường dẫn
runtime và biến `PROCESSING_SCHEMA_DIRECTORY` giữ nguyên.
