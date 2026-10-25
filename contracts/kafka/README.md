# Kafka contracts — bundle 1.1.0

Nguồn wire contract cho SDK [messaging/](../../messaging/README.md), target thiết
kế MaintAI1.4.0. [Hướng dẫn nhóm](../../docs/KAFKA_DEVELOPMENT_GUIDE.md) giải thích
cách tích hợp và quyền của từng thành viên. Đây không phải một deployment mới.

| Topic | Writer mới | Reader tương thích | Kafka key |
| --- | --- | --- | --- |
| `sensor.readings` | 1.3.0 | 1.3.0 | `machine_id` |
| `sensor.rejected` | 1.4.0 | 1.4.0 | `source_identity` |
| `inference.results` | 1.4.0, discriminator `kind` | 1.4.0 và legacy2.0.0 `result_kind` | `machine_id` |
| `alert.signals` | 1.0.0 | 1.0.0 | `machine_id` |

Version của tài liệu, bundle, Python package và từng event độc lập. Result1.4.0
là lineage wire mới theo target §35, **không phải** một nâng cấp SemVer tương thích
của result2.0.0. Không có writer result1.1.0. Cần rollout reader trước writer;
reader giữ payload gốc, không tự thêm provenance giả vào legacy event.

- [topology.json](topology.json): topics, partitions, groups và schema/version map.
- [events/](events/): accepted/quarantine, tagged result union và alert signal.
  `DATA_INVALID`/`INFERENCE_UNAVAILABLE` không có prediction hoặc model lineage giả.
- [archive/](archive/): manifest cho raw microbatch một Kafka receipt.
- [migrations/001_messaging.sql](migrations/001_messaging.sql): inbox/outbox áp dụng
  riêng trong DB của từng owner; không tạo DB chung hoặc quyền SQL xuyên owner.
- [examples/manifest.json](examples/manifest.json): positive/negative wire cases.
  Digest/model/identity trong fixture chỉ phục vụ kiểm thử, không dùng triển khai.
- [manifest.json](manifest.json): SHA-256 của mọi JSON/SQL input cùng hai dependency
  schema baseline. SDK kiểm tra hash trước khi nhận/publish event.

Accepted/alert/legacy wrappers dùng các schema baseline đã freeze trong
`../v1.3/`. SDK wheel/sdist đóng gói đúng các dependency đó để chạy độc lập;
không cần checkout toàn bộ application khi cài wheel.

```bash
maintai-kafka validate-contracts
python messaging/scripts/lock_contracts.py --check
```

Khi sửa JSON/SQL hoặc dependency schema: review producer/consumer, thêm fixture,
đổi bundle version rồi chạy `lock_contracts.py --bundle-version <version-mới>`.
Không chỉ sửa manifest để che một thay đổi wire không tương thích. Không sửa
baseline HTTP/SSE/ML/RAG/Drift chỉ để đồng bộ nhãn version Kafka.

## PROC-ORDER-01 — migration002, wire giữ nguyên

Bundle1.1.0/SDK0.2.0 thêm [002](migrations/002_processing_ordering.sql), giữ
byte-identical001, event schemas và topology. [Manifest1.0.0](releases/1.0.0.json)
được lưu nguyên; wheel/sdist đóng gói cả hai migrations.

002 chỉ cho Processing owner: capability mặc định tắt, assignment generation,
source cursor và outbox dispatch sequence/key cursor/lease. Migration khác
activation, không tự chạy ở startup. Phải drain legacy binaries; pending rows
hoặc legacy source inbox chưa bootstrap sẽ bị chặn. Không sort created_at để
backfill hoặc xóa inbox để bật mode. Business DB không activate Processing mode.
Fields mới là DB metadata, không thêm vào closed Kafka/HTTP payload. Đọc
[runbook](../../docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md) trước cutover.
