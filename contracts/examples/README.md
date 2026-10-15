# API examples

Các payload ở đây là fixtures minh họa contract, không phải dữ liệu production
hoặc kết quả đo hiệu năng/model thực tế. Những files drift/reference dùng
`reference_source=test_fixture`; không dùng chúng làm baseline production.

| Thư mục | Những tình huống chính |
| --- | --- |
| `backend/` | Sensor completed/invalid/ML unavailable/timeout; fleet empty/unknown; alerts; lifecycle/feedback; copilot pending/unavailable; monitoring stale/no report |
| `ml/` | Predict request, NORMAL và FAILURE response |
| `rag/` | Generate request; grounded, insufficient evidence, validation failed |
| `drift/` | Reference đầy đủ và năm DriftStatus |
| `errors/` | Standard error envelope theo code; xem OpenAPI để biết HTTP tương ứng |
| `shared/` | PredictionResponse và Alert DTO đầy đủ |
| `sse/` | Wire frames text/event-stream của năm loại sự kiện |
| `operations/` | Liveness/readiness và Prometheus text minh họa |
| `negative/` | Payload cố ý sai; không dùng làm success example hoặc mock hợp lệ |

Các OpenAPI trỏ tới example bằng `externalValue`. Validator tự tìm request/response
examples theo operation và kiểm tra schema trong đúng context, kể cả error codes
và response lifecycle riêng. `manifest.json` khai báo thêm standalone examples và
counterexamples không phải responses hợp lệ. Mọi JSON fixture phải được kiểm tra.

Chạy từ root repository sau khi cài `contracts/requirements.lock`:

```bash
python contracts/validate.py
```

Examples còn phải giữ semantics không biểu diễn đủ bằng JSON Schema: citations
trỏ tới chunk đã retrieve; prediction label khớp threshold; report summary khớp
feature results. Việc các fixtures này pass không chứng minh service thật có
transaction, authorization, idempotency hoặc query đúng.
