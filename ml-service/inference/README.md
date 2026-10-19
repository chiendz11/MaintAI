# Image inference — online serving

Skeleton cho image logic `ml-service`, chạy Deployment riêng với training tại
port 8001. Ranh giới workload nằm ở [Dockerfile](Dockerfile) và [runtime.json](runtime.json); chưa có API handler.

| Thư mục | Trách nhiệm khi implement |
| --- | --- |
| `api/` | `POST /api/v1/predict`, `GET /health/live`, `GET /health/ready`, `GET /metrics` theo contract. |
| `model_loader/` | Load đúng model/preprocessor/metadata; kiểm tra version, feature order và runtime compatibility trước readiness. |
| `prediction/` | Nhận raw validated features, transform bằng fitted preprocessor và trả prediction/probability. |
| `observability/` | Structured logs, readiness/liveness và inference metrics; không dùng IDs làm metric labels. |
| `tests/` | Placeholder cho contract/startup/prediction checks khi có implementation. |

Processing Worker là caller của `/predict`; frontend chỉ gọi Business API. ML không quyết định
maintenance severity hoặc tạo alert. Khi model chưa load được, readiness phải
false và `/predict` trả lỗi rõ ràng. [OpenAPI hiện hành](../../contracts/v1.3/openapi/openapi-ml.yaml)
có description1.3.2 và giữ provider wire1.1.0; đọc System Contracts v1.4 và migration notes trước khi implement.

Image lấy `inference/` và `shared/`, không import source/dependencies chỉ dành cho
training. Runtime load approved bundle ở `MODEL_DIR` (blueprint hiện dùng
`/models/current`), không gọi fit/train/evaluate trên request. Xem
[ranh giới hai image](../README.md) về model release và các lớp version.
