# Hướng dẫn làm việc trong MaintAI

Áp dụng cho toàn bộ repository. Tuân thủ hướng dẫn hệ thống/công cụ và yêu cầu
hiện tại của người dùng; các tài liệu kỹ thuật dưới đây không tự mở rộng phạm vi
công việc. Yêu cầu chỉ tạo skeleton hoặc chỉ sửa CI/tài liệu phải giữ đúng phạm vi.

## Tài liệu cần đọc

Trước khi triển khai hoặc thay đổi một module:

1. Đọc `docs/README.md`, README của component, source và tests hiện có.
2. Đọc `docs/MaintAI_System_Contracts_v1_1_DRIFT.md`: mục 0–4 và những mục liên
   quan đến module/interface theo bảng trong `docs/README.md`.
3. Đọc các mục tương ứng trong
   `docs/MaintAI_Implementation_Development_Guide_v1_1_DRIFT.md`.
4. Xem JSON Schemas liên quan trong `contracts/`; kiểm tra cả provider và consumer
   của interface trước khi sửa payload hoặc semantics.
5. Với HTTP API, đọc `contracts/README.md`, OpenAPI tương ứng và
   `docs/api-contract-decisions.md`. Phụ lục bổ sung chi tiết baseline chưa chốt;
   không dùng phụ lục để ghi đè invariants đã có trong System Contracts.

Với thay đổi chỉ về tài liệu/CI, đọc phần đặc tả liên quan đến thay đổi; không
cần triển khai module hoặc chạy toàn bộ app tests chỉ vì tài liệu có checklist.

## Xử lý khác biệt

- System Contracts là nguồn chuẩn cho payload, enums, API, DB/artifact và failure
  semantics. Nếu ví dụ trong Guide khác contract, dùng contract.
- Stack/layout ghi là đề xuất trong Guide không bắt buộc. Giữ tên thư mục repo:
  `sensor/`, `backend/`, `frontend/`, `ml-service/`, `rag-service/`, `drift-worker/`.
- Đặc tả mô tả hệ thống mục tiêu, không chứng minh tính năng đã được triển khai.
  Kiểm tra source và CI; không mô tả skeleton/mock/fixture là service production.
- Nếu thông tin cần thiết chưa có hoặc contract tự mâu thuẫn, nêu rõ điểm thiếu
  và tác động; không tự bịa payload hoặc âm thầm thay đổi contract để code chạy.

## Ranh giới và invariants

- GitOps/hạ tầng được quản lý trong hai repo riêng. Deployment manifests, lịch
  CronJob, Prometheus rules và Grafana dashboards thuộc các repo đó. Compose dành
  riêng cho integration tests có thể nằm trong component tests.
- Backend sở hữu validation, inference client, risk decision, alert, audit,
  feedback và RAG client. ML trả prediction/probability; frontend dùng backend API.
- Chỉ event `VALID` đi vào ML. `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE` không được
  chuyển thành `NORMAL`; xử lý lỗi phải giữ đúng failure semantics trong contract.
- RAG/LLM enrich alert và cần citations; lỗi RAG không chặn luồng tạo alert.
- Sensor/ML features chỉ dùng allowlist sáu features trong contract. Không đưa
  IDs, timestamps, target hoặc fault labels vào model features.
- Drift là batch ngoài critical alert path; `DATA_DRIFT` khác `INVALID_DATA`.
  Drift không tự đổi machine state/severity/risk policy, retrain hay promote model.
- Reference drift phải khớp model/dataset version và đến từ training/approved
  reference; không tạo fallback từ runtime window. Test fixture không phải baseline
  production. Report/checkpoint/metrics phải giữ khả năng retry và traceability.

## Thực hiện và kiểm tra

- Làm rõ interface của module bằng contract trước khi thêm implementation; giữ
  nhất quán giữa đặc tả, models, JSON Schemas và consumer/provider.
- Khi đổi interface, áp dụng Contracts mục 25 về versioning và cập nhật các tests
  liên quan; không cập nhật schema để hợp thức hóa một thay đổi trái contract.
- Chạy kiểm tra phù hợp với thay đổi và yêu cầu hiện tại. Với sensor/drift, dùng
  lệnh trong README component và jobs tương ứng ở `.github/workflows/ci.yml`.
- Với thay đổi API docs/schemas/examples, chạy `python contracts/validate.py` bằng
  environment đã cài `contracts/requirements.lock`; giữ job `api-contracts` trong
  `CI / required`. Schema/docs validation không thay thế consumer/provider tests.
- Phân biệt unit/contract tests, mock/fixture integration, DB/container tests và
  production validation. Báo rõ kiểm tra đã chạy, đã skip và giới hạn còn lại.
- Nếu Docker chưa chạy, không coi container tests là đã pass local. Có thể dùng
  kết quả CI của đúng commit làm bằng chứng và ghi rõ nơi thực thi.
- Giữ secrets/connection strings khỏi logs; không commit dataset đầy đủ,
  credentials hoặc output sinh ra ngoài các fixtures đã quản lý bằng Git.
