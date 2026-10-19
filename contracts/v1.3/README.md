# Machine-readable contracts — baseline1.3.2, chờ migration v1.4

Six OpenAPI3.1.1 descriptions ở `openapi/`: Business, Ingestion, Processing,
Simulator, ML và RAG. JSON Schema2020-12 ở `schemas/`/`events/`; fixtures positive/
negative ở `examples/manifest.json`. Không endpoint/runtime nào được implement
hoặc deploy chỉ bằng các spec này.

## Versions và precedence

Target docs mới là v1.4; xem [docs index](../../docs/README.md) và
[readiness](../../docs/implementation-readiness.md). Bộ này giữ baseline1.3.2 cho
compatibility; chưa có `sensor.rejected`/provenance schema v1.4. API description1.3.2
khác provider payload ML/RAG1.1.0, sensor envelope1.3.0, result union2.0.0,
alert signal/archive/SSE và Drift report/reference schema1.0.0. ML/RAG và các
Business DTOs còn hiệu lực được quản lý trực tiếp trong `schemas/`, không tham
chiếu bundle cũ. Chỉ giữ các schema/definitions còn được dùng; không giữ DTO
synchronous ingestion cũ trong bundle hiện hành.

Các ví dụ HTTP nằm trong `examples/http/`; `examples/manifest.json` khai báo
fixture độc lập và phản ví dụ cố ý sai. Chúng là dữ liệu mẫu cho UI/tooling và
validator, không phải dữ liệu production hoặc endpoint implementation.

## Provider routing

| Spec | Caller / boundary |
| --- | --- |
| [Business](openapi/openapi-business.yaml) | Main UI REST/SSE, scoped reads/actions/jobs; không ingest/control |
| [Ingestion](openapi/openapi-ingestion.yaml) | Sensor; envelope guard + broker ACK202 |
| [Processing](openapi/openapi-processing.yaml) | Business/Drift service identities; hot/archive history/status/VALID windows |
| [Simulator](openapi/openapi-simulator.yaml) | Simulator UI riêng; Sensor backend, persisted control version |
| [ML](openapi/openapi-ml.yaml) | Processing, fitted preprocessing+predict, provider1.1 |
| [RAG](openapi/openapi-rag.yaml) | Business durable job runner, provider1.1 |

Không có Drift HTTP server; reports qua versioned S3 manifest/catalog. No real
hosts, buckets, model scores hoặc credentials trong fixtures; artifacts fixture
không có nội dung để download hoặc coi như model release.

## Validation

Dùng dependency lock hiện có `contracts/requirements.lock`:

```bash
.venv/bin/python contracts/v1.3/validate.py
```

Validator kiểm tra schema/OpenAPI, local references, HTTP examples/defaults, SSE,
auth boundary, fixture outcomes và lineage/threshold/citation/drift semantics.
Mọi JSON example phải được một OpenAPI hoặc fixture manifest kiểm tra; local refs
không được thoát khỏi bundle này. Không cần Docker/DB/network. Những
invariant transaction, state transition, snapshot pins, query budgets, coverage,
authorization enforcement và performance cần tests runtime khi implementation.
CI job `api-contracts` chạy trực tiếp validator này; `CI / required` vẫn tổng hợp job đó.
