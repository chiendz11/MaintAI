# MaintAI

Skeleton thư mục theo [kiến trúc đã trao đổi](https://chatgpt.com/s/t_6ac8527fa2c081918c64c8929c655dee).
Chưa chọn framework, chưa có code, dependency hay cấu hình triển khai.
Các file `.gitkeep` giúp Git lưu được thư mục trống.

```text
MaintAI/
├── sensor/
│   ├── simulator/
│   ├── scenarios/
│   └── config/
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
└── rag-service/
    ├── query-builder/
    ├── metadata-filter/
    ├── vector-retriever/
    ├── prompt-builder/
    ├── llm-generator/
    ├── citation-validator/
    └── knowledge/
```

Sensor gửi readings tới backend. Backend kiểm tra chất lượng dữ liệu, gọi ML,
quyết định risk rồi tạo/quản lý alert. Dữ liệu invalid được lưu cùng lỗi chất
lượng và không gọi ML. Backend gọi RAG/LLM để giải thích cảnh báo HIGH/CRITICAL;
frontend hiển thị dữ liệu và kết quả qua backend.

Risk Decision Engine, Alert Manager, Audit Logger và Feedback Manager nằm trong
backend. ML phụ trách preprocessing và prediction. RAG/LLM phụ trách retrieval
tài liệu, tạo giải thích/khuyến nghị và kiểm tra citations; PostgreSQL/pgvector
là đích lưu trữ theo kiến trúc trong chat.

GitOps và hạ tầng sẽ nằm ở hai repo riêng do bạn tạo.

## CI skeleton

GitHub Actions nằm ở [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
Hiện CI kiểm tra cấu trúc repo; matrix cho 5 component đã được chừa sẵn và tắt
đến khi có lệnh lint/test/build thật. Xem [hướng dẫn CI](.github/README.md).
