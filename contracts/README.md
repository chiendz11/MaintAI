# MaintAI API contracts

Bộ contract máy đọc được nằm trong [v1.3/](v1.3/README.md), baseline **1.3.2**, riêng Ingestion M1 có HTTP revision **1.4.0**.
[System Contracts v1.4 §34–39](../docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md)
là target thiết kế mới. [Kafka bundle](kafka/README.md) bổ sung canonical wire
schemas1.4, inbox/outbox SQL và fixtures dùng bởi Python SDK. HTTP/provider/release
provenance migrations chưa hoàn tất; xem [readiness](../docs/implementation-readiness.md).
Bộ tài liệu/contracts kiến trúc v1.1 đã gỡ theo yêu cầu người dùng; schema và ví dụ
còn hiệu lực đã chuyển sang bundle hiện hành, không còn tham chiếu bộ cũ.

```text
contracts/
├── README.md
├── requirements.in
├── requirements.lock
├── kafka/             # Topics/groups, v1.4-only outcome, strict readers, fixtures, SQL và hash lock
└── v1.3/
    ├── openapi/       # Business, Ingestion, Processing, Simulator, ML, RAG
    ├── schemas/       # Runtime DTOs, shared/provider schemas, Drift report/reference
    ├── events/        # Sensor, processing outcomes, alert, archive, Drift, SSE
    ├── examples/      # Positive/negative fixtures, HTTP examples, SSE frames
    ├── README.md
    └── validate.py
```

Main Frontend chỉ gọi Business REST/SSE. Simulator UI riêng gọi Sensor Control API.
ML/RAG là provider nội bộ; Drift vẫn là batch worker, không có HTTP server riêng.
Sáu OpenAPI mô tả interface mục tiêu; chúng không xác nhận runtime đã implement.

## Versions và code hiện có

Version baseline1.3.2 độc lập với wire protocol: ML/RAG giữ1.1.0, Drift report/reference
giữ1.0.0, sensor envelope1.3.0 và processing outcome1.4.0. Outcome/history entrypoints dùng chung schema Kafka;
xem [OUTCOME-14-ONLY](../docs/OUTCOME_V1_4_MIGRATION.md). Không tự chuyển dữ liệu cũ.

- Sensor0.3.0 dùng SensorRequest1.3.0, ACK202 và Control baseline1.3.2. Tests đối chiếu
  runtime.schema.json hiện hành; frozen fixture synchronous v1.1 đã gỡ khi migrate.
  Generated readings vẫn giữ strict sáu-feature allowlist trước fault injection.
- Drift tests đối chiếu schema runtime với [reference](v1.3/schemas/drift-reference.schema.json)
  và [report](v1.3/schemas/drift-report.schema.json) trong bundle hiện hành.
- Kafka transport/inbox/outbox/raw S3 adapters nằm tại `messaging/`; SSE/application
  history/curated archive vẫn cần owner implement. Không coi schemas là full app.
  Xem [implementation readiness](../docs/implementation-readiness.md).

## Validation và CI

Chạy từ root với environment đã cài dependencies:

```bash
python3.12 -m venv .venv-contracts
.venv-contracts/bin/python -m pip install -r contracts/requirements.lock
.venv-contracts/bin/python contracts/v1.3/validate.py
```

Validator kiểm tra cấu trúc OpenAPI/JSON Schema, local refs, HTTP examples/defaults,
SSE và positive/negative fixtures. Nó phát hiện citation sai retrieval context,
label lệch threshold, lineage sai và drift summary bất nhất. Không import ứng dụng,
không cần Docker/DB và không tải remote schemas. Không được tham chiếu ra ngoài
bundle `v1.3/`.

Job `api-contracts` chạy validator này và vẫn thuộc `CI / required`. Khi đổi API,
cập nhật đặc tả, schema, ví dụ và provider/consumer tests liên quan. Kiểm tra tài
liệu không thay thế kiểm tra authorization, transaction, retry hoặc performance
của runtime. Mở file trong `v1.3/openapi/` bằng viewer hỗ trợ local references;
giữ nguyên cấu trúc thư mục khi chia sẻ bundle.

Kafka có validation riêng: `maintai-kafka validate-contracts` và
`python messaging/scripts/lock_contracts.py --check`; xem
[hướng dẫn nhóm](../docs/KAFKA_DEVELOPMENT_GUIDE.md). Không đổi hoặc xóa baseline
v1.3 đang là dependency của accepted/alert/legacy Kafka schemas.
