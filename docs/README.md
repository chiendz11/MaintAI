# Tài liệu thiết kế MaintAI

Hai tài liệu v1.1 DRIFT được sao chép nguyên nội dung từ các file người dùng
cung cấp. Chúng mô tả kiến trúc mục tiêu; mức độ hoàn thành từng module phải
được kiểm tra trong source code, README component và CI thực tế.

| Tài liệu | Vai trò |
| --- | --- |
| [Mô tả chức năng hệ thống](MaintAI_Functional_Overview.md) | Các chức năng mục tiêu, vai trò người dùng, luồng sử dụng, trạng thái UI và ranh giới phạm vi; phân biệt đặc tả với mức độ đã triển khai. |
| [System Contracts v1.1 DRIFT](MaintAI_System_Contracts_v1_1_DRIFT.md) | Chuẩn giao tiếp: payload, enums, API, database, artifact, lỗi, traceability và drift semantics. Contract version bên trong là `1.1.0`. |
| [Implementation Development Guide v1.1 DRIFT](MaintAI_Implementation_Development_Guide_v1_1_DRIFT.md) | Hướng dẫn triển khai từng module, luồng xử lý, gợi ý stack, tests, integration và Definition of Done. |
| [Phụ lục HTTP API](api-contract-decisions.md) | Các quyết định hoàn thiện pagination, nullability, lifecycle responses, brief facade, monitoring, auth profile và HTTP/UI states còn thiếu trong baseline. |

Đặc tả máy đọc được nằm trong [contracts/](../contracts/README.md): ba OpenAPI,
shared schemas và examples. Đây là contract mục tiêu để implement; không mặc định
rằng mọi endpoint đã có service chạy. Hai file tài liệu gốc vẫn giữ nguyên nội dung.

## Cách dùng khi triển khai

1. Xác định module và phạm vi yêu cầu hiện tại; đọc [AGENTS.md](../AGENTS.md).
2. Đọc Contracts mục 0–4 và Guide mục 1–3 để hiểu các nguyên tắc chung.
3. Đọc các mục liên quan đến module trong bảng dưới và các interface mà module
   cung cấp hoặc sử dụng; xem cả source/schema/tests hiện có.
4. Với payload và semantics, ưu tiên System Contracts nếu ví dụ trong Guide
   khác contract. Các phần ghi `[Đề xuất triển khai]` là gợi ý, không bắt buộc
   thêm stack, đổi folder hoặc triển khai ngoài phạm vi yêu cầu.
5. Khi thay đổi một interface, cập nhật đồng bộ đặc tả, schema và kiểm tra của
   các component liên quan; ghi rõ version và ảnh hưởng tới provider/consumer.

Tài liệu là đặc tả kỹ thuật, không tự mở rộng công việc được yêu cầu. Ví dụ
Guide có đề cập Kubernetes, Grafana và platform, nhưng GitOps/hạ tầng của dự án
được quản lý ở hai repo riêng theo lựa chọn của người dùng.

## Mục cần đọc theo module

Số mục tham chiếu tới tiêu đề trong hai tài liệu, không phải số dòng.

| Module/thay đổi | System Contracts | Implementation Guide |
| --- | --- | --- |
| `sensor/` | 2–5; 25A về drift scenarios | 5; 21A về drift integration |
| `backend/` | 5–7, 9–13, 15–18, 23; 25A khi đọc drift reports | 6–14, 19, 25 |
| `ml-service/` | 7–8, 17–18; 25A về reference/prediction drift | 15–16; 21A về reference artifact |
| `rag-service/` | 13–14, 17–18, 23 | 17–19, 25 |
| `frontend/` | 11–12, 15–16, 24; 25A về monitoring | 20; 21/21A về monitoring |
| `drift-worker/` | 3.8, 8.2, 17, 20–21, 25A | 21A; 24–25 về tests/failure handling |
| CI/contract tests | 25–26 | 23–25, 33–34 |
| Health/logs/metrics | 18–21, 23, 25A | 21/21A, 25, 31 |

## Đối chiếu tên thư mục

Guide mô tả một layout đề xuất. Repo đang dùng các tên sau:

| Ví dụ trong Guide | Đường dẫn thực tế |
| --- | --- |
| `sensor-simulator/` | `sensor/` |
| `ml-inference/` | `ml-service/` |
| `drift-worker/app/` | `drift-worker/src/drift_monitor/` |
| `platform/` | GitOps/hạ tầng ở hai repo riêng; Compose trong tests là fixture kiểm thử |

Giữ cấu trúc hiện có khi triển khai module, trừ khi yêu cầu hiện tại bao gồm
việc tổ chức lại repository. Tên thư mục trong ví dụ không làm thay đổi contract.

## Phiên bản và tính nhất quán

- Guide có nhắc BRD và tên cũ `MaintAI_System_Contracts.md`; file contract hiện
  được đưa vào repo là bản v1.1 DRIFT ở trên. BRD chưa được đính kèm tại đây.
- Tên bản tài liệu `v1_1` và `schema_version=1.0.0` của một payload là hai cấp
  version khác nhau; không đổi schema version chỉ để khớp tên file tài liệu.
- JSON Schemas trong [contracts/](../contracts/README.md) cụ thể hóa payloads.
  Schema sensor/drift sinh từ models phải khớp runtime; schema cho backend/ML/RAG
  mô tả contract mục tiêu, kể cả module chưa được triển khai. Đọc phụ lục HTTP
  cùng baseline; khi có khác biệt về invariant đã chốt, baseline được ưu tiên.
- Thay đổi contract theo quy tắc ở Contracts mục 25, ghi rõ thay đổi và kiểm tra
  tương thích của cả bên cung cấp lẫn bên sử dụng interface.

`docs/` giúp mọi người và agent dùng chung tài liệu được version bằng Git.
`AGENTS.md` chỉ dẫn việc đọc; tests/CI kiểm tra phần hành vi đã triển khai.
Không mặc định rằng mọi công cụ AI sẽ tự đọc toàn bộ file trong `docs/`.
