# System Contracts v1.1 DRIFT

Đây là bộ contract máy đọc được cho MaintAI, cụ thể hóa
[Markdown baseline](../docs/MaintAI_System_Contracts_v1_1_DRIFT.md) và
[phụ lục hoàn thiện HTTP](../docs/api-contract-decisions.md). Các spec mô tả hành vi
mục tiêu; không khẳng định backend/ML/RAG đã được triển khai hoặc deploy.

## API documents

| File | Provider → consumer | Nội dung |
| --- | --- | --- |
| [openapi-backend.yaml](openapi-backend.yaml) | Backend → frontend/sensor | Ingestion, fleet/machine/history, alert lifecycle/feedback, brief facade, monitoring/drift, optional SSE, health/metrics |
| [openapi-ml.yaml](openapi-ml.yaml) | ML → backend | Prediction request/response, model readiness, validation/dependency errors, health/metrics |
| [openapi-rag.yaml](openapi-rag.yaml) | RAG → backend | Brief generation, GROUNDED/INSUFFICIENT_EVIDENCE/VALIDATION_FAILED, provider failure, health/metrics |

Frontend chỉ gọi backend. `/api/v1/predict` thuộc ML, `/api/v1/briefs` thuộc RAG;
chúng không phải routes để frontend gọi trực tiếp. Drift Worker vẫn là batch,
không có OpenAPI hoặc REST server riêng.

Các OpenAPI dùng format 3.1.1 và `info.version=1.1.0`. Đây là version đặc tả API,
độc lập model_version, policy_version và schema_version của payload drift.
`servers` là ví dụ local, không phải địa chỉ service đã deploy.

```text
contracts/
├── openapi-backend.yaml
├── openapi-ml.yaml
├── openapi-rag.yaml
├── schemas/
│   ├── sensor-event.schema.json        # $ref tới runtime schema ở root
│   ├── prediction.schema.json
│   ├── alert.schema.json
│   ├── maintenance-brief.schema.json
│   ├── drift-reference.schema.json     # $ref tới runtime schema ở root
│   ├── drift-report.schema.json        # $ref tới runtime schema ở root
│   ├── common.schema.json              # enums, error envelope, health
│   ├── backend.schema.json             # read DTOs, lifecycle, feedback, copilot
│   ├── brief-request.schema.json
│   ├── monitoring.schema.json
│   └── sse-event.schema.json
├── examples/
│   ├── backend/
│   ├── ml/
│   ├── rag/
│   ├── drift/
│   ├── errors/
│   ├── operations/
│   ├── shared/
│   ├── sse/
│   ├── negative/                       # cố ý sai để kiểm tra validator
│   └── manifest.json
├── sensor-event.schema.json            # schema sinh bởi sensor
├── drift-reference.schema.json         # schema sinh bởi worker
├── drift-report.schema.json            # schema sinh bởi worker
├── requirements.in
├── requirements.lock
└── validate.py
```

Ba runtime schemas giữ nguyên vị trí và nội dung để không phá các lệnh export
và tests sensor/worker hiện có. Files cùng tên trong `schemas/` chỉ tham chiếu
bằng `$ref`, không sao chép hoặc tự định nghĩa một bản khác. Schemas cho các
component chưa có implementation là design contracts cho provider/consumer.

## Đọc và sử dụng

Mỗi operation có summary, description, nguồn baseline/phụ lục, parameters,
request/response schema, status codes và examples theo từng kết quả. Security
profile MVP còn tùy chọn; đọc AUTH-01 trước khi implement auth. Role/identity
metadata ở `x-maintai-roles`, không coi actor_id trong request là quyền truy cập.

Không suy ra nghiệp vụ chỉ từ HTTP success:

- 200 danh sách rỗng → EMPTY; 404 resource → not found.
- 200 brief → kiểm tra grounded_status; thiếu evidence không phải 5xx.
- 202 request brief → PENDING; tiếp tục đọc GET facade để nhận outcome.
- 200 đọc drift report → report vẫn có thể mang overall_status ERROR.
- 422 sensor → SensorInvalid nếu có IDs hợp lệ; nếu không có IDs hợp lệ, nhận
  standard error envelope. Cả hai đều không được tạo ML prediction.
- 503/504 sensor do ML → UNKNOWN với prediction/alert null, không phải NORMAL.
- 409 lifecycle → state đã thay đổi hoặc action sai; refresh alert trước khi chọn
  thao tác tiếp theo, không retry action mù.

Chi tiết thêm về pagination, cursor timestamp trùng nhau, nullability và failure
semantics nằm trong [phụ lục](../docs/api-contract-decisions.md).

## Validate và mở bằng API viewer

Chạy từ root repository với Python 3.12:

```bash
python3.12 -m venv .venv-contracts
.venv-contracts/bin/python -m pip install -r contracts/requirements.lock
.venv-contracts/bin/python contracts/validate.py
```

Validator không import ứng dụng, không cần Docker/DB và không truy xuất remote
schemas. Nó kiểm tra OpenAPI, JSON Schemas, local references, mọi examples HTTP,
các frame SSE, defaults và counterexamples; đối chiếu HTTP endpoints/enums đã có
với baseline. Các checks bổ sung phát hiện citation trỏ ra ngoài retrieval context,
prediction label lệch threshold và một số bất nhất trong drift report.

Để mở từng API bằng viewer/codegen không hỗ trợ nhiều file, xuất bundle độc lập:

```bash
.venv-contracts/bin/python contracts/validate.py --bundle-dir /tmp/maintai-openapi
```

Import một trong ba file `openapi-backend.json`, `openapi-ml.json`, `openapi-rag.json`
ở thư mục output. Bundle chứa schema và example values inline; không commit output
này, vì nó được tạo từ source contracts. YAML gốc vẫn là tài liệu sửa bằng Git.

## CI và cập nhật

Job `API contracts` thuộc `CI / required`. Khi sửa interface:

1. Đối chiếu baseline và phụ lục; ghi quyết định mới nếu phần đó chưa được chốt.
2. Cập nhật OpenAPI, schema được tham chiếu và examples thành công/lỗi liên quan.
3. Thêm counterexample nếu cần bảo vệ một invariant có thể kiểm tra được.
4. Chạy validator và cập nhật consumer/provider tests khi có implementation.

Đây là kiểm tra đặc tả và fixtures, chưa phải kiểm tra service HTTP thật. Rule
nghiệp vụ như authorization, idempotency, transaction/audit, timeout và query DB
còn cần được kiểm tra trong implementation. Các extensions `x-maintai-*` mô tả
semantics, không tự thực thi chúng.

Tài liệu chuẩn cho tooling: [OpenAPI 3.1.1](https://spec.openapis.org/oas/v3.1.1.html),
[JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-core),
[openapi-spec-validator](https://openapi-spec-validator.readthedocs.io/en/latest/).

## Sensor runtime schema

`sensor-event.schema.json` cụ thể hóa **System Contracts v1.1 DRIFT**, mục 2 và 5.
Các field của request nằm trong `features`, có `source=AI4I_SIMULATOR`, timezone
trong `received_at`, rpm > 0 và tool wear >= 0. Feature ngoài allowlist không được gửi.
Các range nghiệp vụ khác do backend validator quản lý; schema không tự thêm range.

Model runtime: `sensor/src/simulator/models.py`. Xuất lại schema từ root:

```bash
.venv/bin/maintai-sensor export-schema --output contracts/sensor-event.schema.json
```

Contract tests kiểm tra schema đã commit khớp model và kiểm tra payload/scenario
thực tế bằng Draft 2020-12 + date-time format checker. JSON chuẩn không cho phép
NaN/Infinity; runtime cũng kiểm tra finite number trước khi serialize.

Guide §5.3 có ví dụ feature ở cấp ngoài; simulator dùng cấu trúc lồng trong
System Contracts §5.2 làm chuẩn. Contract tests của backend thật sẽ được thêm
khi backend được triển khai; hiện sensor dùng backend giả lập để kiểm thử giao tiếp.

## Drift reference và report

`drift-reference.schema.json` và `drift-report.schema.json` cụ thể hóa payload
`schema_version=1.0.0` trong phần Drift Monitoring của System Contracts v1.1.
Models nằm trong `drift-worker/src/drift_monitor/models.py`.

Reference bắt buộc đủ sáu feature, numeric bins/probabilities, categorical
categories/probabilities, model và dataset version. Runtime kiểm tra thêm bins
tăng nghiêm ngặt, tổng probabilities bằng 1, danh mục L/M/H và version khớp model.
Report gồm window, tổng kết, feature PSI, prediction drift, reason và snapshot
policy; trường policy/provenance là phần mở rộng phục vụ audit. Timestamp phải có
timezone; số phải finite; feature ngoài allowlist không được chấp nhận.

```bash
.venv/bin/maintai-drift export-schemas --directory contracts
```

Tests kiểm tra schema đã commit khớp model và validate reports từ cả sensor
scenarios và PostgreSQL/container integration. Fixture reference chỉ dành cho
test; production cần reference artifact của model triển khai.
