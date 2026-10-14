# MaintAI

Skeleton thư mục theo [kiến trúc đã trao đổi](https://chatgpt.com/s/t_6ac8527fa2c081918c64c8929c655dee).
`sensor/` và `drift-worker/` đã có implementation Python theo Implementation
Development Guide và System Contracts v1.1 DRIFT. Các component khác vẫn là skeleton; `.gitkeep`
giúp Git lưu thư mục trống. GitOps và hạ tầng được quản lý ở hai repo riêng.

Tài liệu thiết kế được quản lý trong [docs/](docs/README.md): System Contracts
v1.1 DRIFT và Implementation Development Guide v1.1 DRIFT. Đọc contract để xác
định giao tiếp/dữ liệu, rồi đọc hướng dẫn của module trước khi triển khai.
[AGENTS.md](AGENTS.md) chỉ dẫn coding agent cách đọc và đối chiếu tài liệu.

```text
MaintAI/
├── AGENTS.md
├── docs/
│   ├── README.md
│   ├── MaintAI_System_Contracts_v1_1_DRIFT.md
│   └── MaintAI_Implementation_Development_Guide_v1_1_DRIFT.md
├── sensor/
│   ├── src/simulator/
│   ├── data/
│   ├── scenarios/
│   ├── tests/
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── README.md
├── backend/
│   ├── api/
│   ├── sensor-ingestion/
│   ├── data-quality/
│   ├── inference-client/
│   ├── risk-decision-engine/
│   ├── alert-manager/
│   ├── audit-logger/
│   ├── feedback-manager/
│   ├── rag-client/
│   └── persistence/
├── frontend/
│   ├── fleet-dashboard/
│   ├── machine-detail/
│   ├── sensor-history/
│   ├── alert-detail/
│   ├── ai-explanation/
│   ├── monitoring/
│   ├── api/
│   └── shared/
├── ml-service/
│   ├── preprocessing/
│   ├── model/
│   ├── prediction/
│   └── artifacts/
├── rag-service/
│   ├── query-builder/
│   ├── metadata-filter/
│   ├── vector-retriever/
│   ├── prompt-builder/
│   ├── llm-generator/
│   ├── citation-validator/
│   └── knowledge/
└── drift-worker/
    ├── src/
    │   └── drift_monitor/
    └── tests/
        ├── unit/
        └── integration/
```

JSON schema dùng chung cho sensor nằm trong `contracts/sensor-event.schema.json`.
Xem [hướng dẫn sensor](sensor/README.md) để cài đặt, tải AI4I và replay:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r sensor/requirements-dev.lock
.venv/bin/python -m pip install --no-deps -e ./sensor
.venv/bin/maintai-sensor download-data
.venv/bin/maintai-sensor replay --dry-run --limit 3
```

Sensor gửi readings tới backend. Backend kiểm tra chất lượng dữ liệu, gọi ML,
quyết định risk rồi tạo/quản lý alert. Dữ liệu invalid được lưu cùng lỗi chất
lượng và không gọi ML. Backend gọi RAG/LLM để giải thích cảnh báo HIGH/CRITICAL;
frontend hiển thị dữ liệu và kết quả qua backend.

Risk Decision Engine, Alert Manager, Audit Logger và Feedback Manager nằm trong
backend. ML phụ trách preprocessing và prediction. RAG/LLM phụ trách retrieval
tài liệu, tạo giải thích/khuyến nghị và kiểm tra citations; PostgreSQL/pgvector
là đích lưu trữ theo kiến trúc trong chat.

Theo [chat bổ sung drift worker](https://chatgpt.com/s/t_6accf009315081918266e3664cba5a6d),
`drift-worker/` là workload độc lập chạy định kỳ: đọc reference theo model version,
đọc cửa sổ sensor readings hợp lệ từ database, phát hiện drift rồi lưu report và
checkpoint cho monitoring. Xem [hướng dẫn Drift Worker](drift-worker/README.md)
để cấu hình reference, database, Pushgateway và kiểm tra cùng sensor.

GitOps và hạ tầng sẽ nằm ở hai repo riêng do bạn tạo.

## CI

GitHub Actions nằm ở [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
CI kiểm tra cấu trúc 6 component. Sensor có CI thật: lint/format, tests,
build Python distribution, dependency audit và container smoke test. Drift Worker
có lint/format, tests với PostgreSQL thật, build/audit và integration container cùng
sensor/Pushgateway. Matrix cho 4 component còn lại đang tắt.
Xem [hướng dẫn CI](.github/README.md).
