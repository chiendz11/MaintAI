# Rà soát dấu vết kiến trúc cũ — cập nhật2026-10-19

Phạm vi: app repo MaintAI và dependency giữa source, docs, contracts, CI.
Không xóa dữ liệu local, .venv, credentials hoặc các repo platform bên cạnh.
Lượt dọn2026-10-18 dùng target1.3.2; lượt nhập docs2026-10-19 dùng target1.4.
Executable Sensor/Drift vẫn còn contract1.1, phần còn lại skeleton.

## 1. Placeholder đã xác nhận không dùng và đã xóa

| Đường dẫn đã bỏ | Bằng chứng | Lý do |
| --- | --- | --- |
| `backend/business-api/src/simulator_facade/.gitkeep` | Thư mục chỉ có một .gitkeep, không code/import/caller | Business không còn Simulator Control facade |
| `frontend/src/features/simulator_control/.gitkeep` | Thư mục chỉ có một .gitkeep, tham chiếu còn lại chỉ là cây README cũ | Main UI không còn control tab; Simulator UI riêng gọi Sensor |

Hai thư mục rỗng tương ứng cũng được xóa. Không tạo module thay thế giả hoặc
chuyển controller sang Business. Simulator UI riêng mới ở mức đặc tả/planned.
Các dấu `D` của những backend/frontend .gitkeep đã di chuyển ở lượt trước là
thay đổi Git chưa commit, không phải file cũ vẫn còn trên filesystem.

## 2. Tài liệu/contracts kiến trúc cũ đã gỡ

Theo lựa chọn người dùng: **chỉ xóa tài liệu/contracts cũ, giữ code Sensor/Drift**.

| Đã gỡ | Dependency đã xử lý |
| --- | --- |
| `docs/MaintAI_System_Contracts_v1_1_DRIFT.md` | Bỏ baseline validator; target1.3.2 là nguồn đặc tả hiện hành |
| `docs/MaintAI_Implementation_Development_Guide_v1_1_DRIFT.md` | Guide1.3 hiện hành thay thế; xóa cả bản có chỉnh sửa local theo yêu cầu người dùng |
| `docs/api-contract-decisions_v1_1_DRIFT.md` | API decisions hiện hành và OpenAPI target thay thế |
| Ba OpenAPI ở root `contracts/` | Sáu provider specs hiện hành trong `contracts/v1.3/openapi/` |
| `contracts/schemas/`, `contracts/examples/` | Chuyển phần còn hiệu lực vào bundle hiện hành, bỏ DTO synchronous ingestion và SSE cũ |
| Ba root schemas và root validator | Drift schemas chuyển sang bundle hiện hành; Sensor wire schema chuyển thành fixture module; CI dùng validator hiện hành |

`contracts/v1.3/` tự chứa, không còn tham chiếu schema/example ở bundle đã gỡ.
Các component/schema definitions không còn dùng cũng đã được loại bỏ, thay vì
chuyển nguyên bộ đặc tả ingestion đồng bộ cũ vào thư mục mới.

## 3. Phần còn hiệu lực đã chuyển vị trí

- Shared Business/provider schemas ở `contracts/v1.3/schemas/`: business DTOs,
  common/error/health, features, prediction, brief request/response và monitoring.
- Drift report/reference schemas1.0.0 chuyển nguyên nội dung vào cùng thư mục;
  unit tests vẫn đối chiếu bằng schema export từ runtime. Không bump payload version.
- Các ví dụ HTTP còn dùng ở `contracts/v1.3/examples/http/`; fixture manifest giữ
  các trường hợp provider/grounding/threshold/drift phù hợp. Validator kiểm tra
  mọi JSON example, từ chối orphan fixtures và references thoát khỏi bundle.
- Sensor wire synchronous v1.1 giữ nguyên trong
  `sensor/tests/fixtures/sensor-wire.schema.json`. Đây là frozen compatibility
  fixture của module hiện có, không phải contract Ingestion ACK202 mục tiêu.
- Sensor/Drift test paths, README, AGENTS và CI đã chuyển sang các vị trí mới.
  Job `api-contracts` vẫn thuộc `CI / required`; không đổi branch protection.

## 4. Code và dữ liệu được giữ

Không xóa hoặc đổi executable source Sensor/Drift, Dockerfile, scenarios,
fixtures tích hợp, SQL loaders hoặc dữ liệu local. Sensor client hiện vẫn dùng
synchronous v1.1; Drift direct-SQL adapter vẫn là implementation cũ. Chỉ đổi
đường dẫn schema trong test code để chúng tiếp tục chạy sau khi dọn contracts.

13 runtime descriptors vẫn cùng target_contract_revision1.3.2; implementation_status
vẫn skeleton/legacy_partial. Sensor/Drift có implemented_contract_revision1.1.0,
phần chưa implement là null. Simulator UI vẫn planned-without-source.
Việc dọn tài liệu không implement Kafka/SSE/control/history/archive, không thay
DB ownership trong code và không triển khai thêm runtime.

## 5. Kiểm tra sau dọn

- Target validator PASS: sáu OpenAPI, 52 operations, 651 local refs,
  96 HTTP example/default checks, 53 positive/negative cases và SSE envelope.
- Sensor: 88 tests PASS; Drift: 43 unit tests PASS, gồm schema export equality.
- Source snapshot trước/sau dùng để xác nhận executable Sensor/Drift được giữ.
- Kiểm tra refs, local Markdown links, provenance hashes và CI wiring ở bundle
  hiện hành; không sử dụng tài liệu/contracts đã gỡ làm dependency.
- Không chạy Docker/DB integration, build, benchmark, deployment hoặc thay đổi
  repository GitOps/hạ tầng. Test component hiện có không chứng minh runtime
  target1.3.2 đã hoàn tất.

## 6. Nhập tài liệu v1.4 và dọn bản được thay thế — 2026-10-19

Đã nhập sáu file Downloads: Deployment Architecture, Runtime Dataflow, Versioning
& Provenance, System Contracts v1.4, Implementation Guide v1.4 và Implementation Plan.
Đã sửa relative links theo vị trí thực tế `docs/`, giữ phân công4 người và Simulator UI riêng.

| Đã xóa | Thay thế / phần được bảo toàn |
| --- | --- |
| `MaintAI_System_Contracts_v1_3_REALTIME_UI.md` | System Contracts v1.4; chi tiết operational còn cần giữ ở history/recovery/API phụ lục |
| `MaintAI_Implementation_Development_Guide_v1_3_REALTIME_UI.md` | Guide v1.4; giữ project Simulator UI decision và cập nhật nhóm4 người |
| `architecture-decisions-v1.3.2.md` | Docs index giữ precedence/auth/Simulator UI/ownership; sáu tài liệu mới thay phần overview/versioning |

Giữ Functional Overview vì mô tả chức năng/UI; giữ API decisions vì baseline
OpenAPI còn dùng; giữ HISTORY-01/RECOVERY-01 vì còn chi tiết archive/jobs/failure
không được sáu file mới thay thế đầy đủ. Layout/readiness/audit/provenance manifest
có vai trò inventory và traceability riêng. Không xóa application code/tests/data,
không thay payload schema, CI jobs, runtime version/status hoặc các repo platform.

Source manifest giữ upstream/current hashes và metadata bản đã retire. Các
schema/scaffold v1.4 chỉ được nhắc tới nhưng chưa được cung cấp được liệt kê trong
readiness; không tạo giả để coi bộ bàn giao đã đầy đủ.

## 7. Rà soát khi thêm Kafka SDK — 2026-10-19

Không xác định thêm file tài liệu/contracts thừa để xóa. Functional/API/history/
recovery/layout/readiness vẫn có mục đích riêng; `contracts/v1.3/` tiếp tục là
dependency thực của validators, Drift và Kafka accepted/alert/legacy readers.
Giữ source Sensor/Drift và local data. Các `.gitkeep` ở module nghiệp vụ chưa
implement không bị xóa chỉ vì transport đã chuyển sang thư viện chung.

Thêm `contracts/kafka/` làm nguồn wire chuẩn, `messaging/` làm SDK; wheel đóng gói
bản sao đúng hash để phân phối, không phải một nguồn contract thứ hai để sửa tay.
Thêm duy nhất [Kafka development guide](KAFKA_DEVELOPMENT_GUIDE.md) làm hướng dẫn
cho4 người và cập nhật index/readiness/layout, thay vì tạo4 bản hướng dẫn trùng.
