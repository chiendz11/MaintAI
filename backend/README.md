# MaintAI Backend — Sensor Ingestion (B1)

`app.main:create_app` có `POST /api/v1/sensor-readings` và `GET /health/live`.
Service ingestion quản lý trace IDs, lỗi request, kiểm tra máy, chống xử lý trùng
`event_id`, kết quả validation và kết quả từ các module phía sau. Event `INVALID`
không được gửi đến ML. Những ràng buộc dữ liệu đã chốt trong contract hiện nằm
trong `SensorReadingRequest`; range riêng theo máy cần policy được phê duyệt và
có version trước khi thêm vào validator.

## Các phần của module

- `app/main.py`: chuyển đổi HTTP request/response, gắn trace headers.
- `app/schemas/`: model request, response và lỗi theo contract.
- `app/services/data_quality.py`: kết quả và version của validator nội bộ.
- `app/services/sensor_ingestion.py`: luồng điều phối B1.
- `SensorEventStore`: interface khóa theo event, kiểm tra máy, lưu event và outcome.
  Cần triển khai bằng PostgreSQL để chạy production.
- `ValidEventProcessor`: interface cho luồng ML → risk → alert sau này. Nó phải
  trả response `COMPLETED` đúng contract hoặc báo `MLUnavailable`/`MLTimeout`.

Repo hiện chưa có PostgreSQL adapter và processor thật. `app.main:app` vẫn trả
liveness, nhưng request sensor cần các dependency đó sẽ nhận `500 INTERNAL_ERROR`.
Ứng dụng không tự tạo prediction hoặc coi event là đã xử lý thành công. Tests dùng
fixture cục bộ; kết quả fixture không phải hành vi production.

Để thử HTTP trên máy, `app.dev:app` dùng bộ nhớ của một process và seed máy mẫu
`M-001`. Event valid trả `503 ML_UNAVAILABLE` vì chưa nối ML. Bạn có thể thử event
invalid, gửi trùng và payload xung đột. Dữ liệu mất khi restart; không dùng app
này làm persistence production.

Khi có dependency thật, tạo `SensorIngestionService(store, processor)` rồi truyền
vào `create_app(service)`. Store phải khóa `event_id` an toàn giữa các worker, lưu
validation trước khi gọi processor và lưu outcome để trả lại khi event được gửi
lặp. Nếu event đã lưu nhưng outcome còn thiếu, service báo lỗi thay vì tự gọi ML
lần nữa và có nguy cơ tạo prediction trùng.

## Kiểm tra local

Từ thư mục `backend/` với môi trường Python 3.12 hiện có:

```bash
.venv/bin/python -B -m unittest discover -s tests -v
.venv/bin/uvicorn app.main:app --reload
.venv/bin/uvicorn app.dev:app --reload  # app thử sensor, chạy thay cho dòng trên
```

HTTP tests dùng fixture store và processor, không cần Docker/PostgreSQL. Job CI
backend hiện vẫn là placeholder, vì vậy cần chạy lệnh test trực tiếp.

Hiện canonical payload được tạo từ event đã parse bằng JSON với key được sắp xếp.
Contract chưa quy định thuật toán canonical JSON giữa các ngôn ngữ; repository
production phải giữ cùng semantics khi triển khai idempotency.
