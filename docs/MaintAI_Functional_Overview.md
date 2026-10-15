# MaintAI — Mô tả chức năng hệ thống

Ngày cập nhật: 2026-10-15. Baseline: System Contracts `1.1.0` và phụ lục HTTP API.

Tài liệu mô tả **hệ thống sẽ làm gì, người dùng sử dụng ra sao và kết quả mong
đợi trong từng tình huống**. Đây là phạm vi chức năng mục tiêu, không phải thông
báo rằng mọi tính năng đã được triển khai hoặc chạy trên production.

Tại thời điểm viết, `sensor/` và `drift-worker/` đã có implementation; `backend/`,
`frontend/`, `ml-service/`, `rag-service/` còn là skeleton. Các API trong
`contracts/` là đặc tả để triển khai những module này.

## 1. Mục tiêu, phạm vi và nguồn tham chiếu

MaintAI là hệ thống **hỗ trợ bảo trì dựa trên dự đoán và hỗ trợ kỹ thuật viên**.
Hệ thống tiếp nhận dữ liệu cảm biến, dự đoán khả năng máy gặp lỗi, áp dụng chính
sách rủi ro để tạo cảnh báo và cung cấp hướng dẫn kiểm tra có trích dẫn tài liệu.
Một worker độc lập theo dõi sự thay đổi phân bố dữ liệu để hỗ trợ giám sát ML.

Ví dụ một phiên sử dụng:

1. Simulator gửi thông số của máy `M-001` đến backend.
2. Backend kiểm tra dữ liệu, gọi ML nếu hợp lệ rồi quyết định mức rủi ro.
3. Nếu policy yêu cầu, backend tạo cảnh báo để kỹ thuật viên tiếp nhận.
4. Copilot bổ sung giải thích và checklist từ tài liệu bảo trì.
5. Kỹ thuật viên kiểm tra thực tế, cập nhật cảnh báo và gửi phản hồi.
6. Drift Worker định kỳ kiểm tra dữ liệu vận hành có khác reference hay không.

Tài liệu này diễn giải chức năng, không định nghĩa lại payload. Khi triển khai,
đọc theo thứ tự:

| Nguồn | Nội dung cần tra cứu |
| --- | --- |
| [System Contracts](MaintAI_System_Contracts_v1_1_DRIFT.md) | Invariants, enums, giao tiếp giữa các thành phần, traceability và failure semantics; là chuẩn ưu tiên. |
| [Phụ lục HTTP API](api-contract-decisions.md) | Các chi tiết bổ sung về phân trang, vòng đời cảnh báo, brief, monitoring và trạng thái HTTP/UI. |
| [OpenAPI, schemas và examples](../contracts/README.md) | Đường dẫn API, request/response, lỗi và dữ liệu mẫu cụ thể. |
| [Implementation Guide](MaintAI_Implementation_Development_Guide_v1_1_DRIFT.md) | Hướng dẫn tổ chức và triển khai từng module; các phần đề xuất không mặc định là yêu cầu bắt buộc. |

## 2. Người dùng và các thành phần tham gia

### 2.1. Người dùng

| Vai trò | Nhu cầu và thao tác chính |
| --- | --- |
| Người theo dõi — `VIEWER` | Xem danh sách máy, trạng thái, dữ liệu, cảnh báo và monitoring; không thay đổi nghiệp vụ. |
| Kỹ thuật viên — `TECHNICIAN` | Xem dữ liệu, tiếp nhận và xử lý cảnh báo, yêu cầu Copilot, ghi nhận kết quả kiểm tra và đánh giá khuyến nghị. |
| Quản trị viên — `ADMIN` | Có các quyền nghiệp vụ trên; theo baseline, việc thay policy hoặc quản trị cấu hình chỉ dành cho admin nếu triển khai các chức năng quản trị đó. |

Auth là phần tùy chọn của prototype. Khi bật auth, backend phải kiểm tra danh
tính và quyền cho từng thao tác. Contract hiện chưa định nghĩa màn hình đăng
nhập, đăng ký, quản lý tài khoản hoặc API thay policy; bảng vai trò không có nghĩa
những màn hình/API đó đã nằm trong phạm vi triển khai hiện tại.

### 2.2. Các thành phần hệ thống

| Thành phần | Trách nhiệm |
| --- | --- |
| `sensor/` | Mô phỏng dữ liệu máy từ AI4I, replay và tạo các tình huống phục vụ kiểm tra/demo. |
| `backend/` | Tiếp nhận, validation, gọi ML, quyết định risk, quản lý cảnh báo, audit, feedback và gọi RAG. |
| `ml-service/` | Chuẩn bị model offline; khi phục vụ online, preprocessing và dự đoán khả năng lỗi. |
| `rag-service/` | Chuẩn bị kho tài liệu, truy xuất bằng chứng và sinh Maintenance Brief/checklist có citations. |
| `frontend/` | Hiển thị thông tin và nhận thao tác người dùng thông qua API backend. |
| `drift-worker/` | Đánh giá drift theo batch, lưu báo cáo và công bố metrics. |
| PostgreSQL/pgvector | Lưu dữ liệu nghiệp vụ, tài liệu/chunks/embeddings và báo cáo drift theo ownership của từng module. |

Frontend chỉ giao tiếp với backend để thực hiện các chức năng ứng dụng. Backend
gọi ML và RAG; frontend không truy cập trực tiếp các service nội bộ hoặc database.

## 3. Ba luồng hoạt động

```text
Luồng phát hiện và xử lý cảnh báo:
Sensor → Backend: validation → ML → Backend: risk decision → alert → UI

Luồng hỗ trợ kỹ thuật viên:
Alert đã tồn tại → Backend → RAG/LLM + kho tài liệu → brief/checklist → Backend → UI

Luồng giám sát drift:
Reference theo model + dữ liệu runtime VALID → Drift Worker → report/checkpoint
                                                    └→ Pushgateway → Prometheus/Grafana
```

Luồng cảnh báo phải hoạt động độc lập với Copilot và Drift Worker. RAG lỗi không
được làm mất cảnh báo; worker drift lỗi không được chặn tiếp nhận dữ liệu hoặc
suy luận ML.

## 4. Tạo dữ liệu cảm biến mô phỏng

**Người sử dụng:** developer/người chạy demo. **Module:** `sensor/`.

### 4.1. Replay dữ liệu AI4I

Simulator đọc dataset AI4I, chuyển từng bản ghi thành sensor event và gửi tới
backend. Một event gồm ID riêng, máy được gán, thời gian có timezone, nguồn và
sáu features:

| Feature | Ý nghĩa | Đơn vị/giá trị |
| --- | --- | --- |
| `product_type` | Loại chất lượng sản phẩm | `L`, `M`, `H` |
| `air_temperature_k` | Nhiệt độ không khí | Kelvin |
| `process_temperature_k` | Nhiệt độ quá trình | Kelvin |
| `rotational_speed_rpm` | Tốc độ quay | Vòng/phút |
| `torque_nm` | Mô-men xoắn | Nm |
| `tool_wear_min` | Thời gian mòn dụng cụ | Phút |

Người chạy có thể chọn dataset, scenario, seed, tốc độ replay và giới hạn số
event. Chế độ dry-run xuất payload để kiểm tra mà không gọi backend. Có thể xuất
JSONL, log và metrics phục vụ kiểm tra lượt chạy.

`machine_id` và thời gian được tạo để mô phỏng hệ thống. Không dùng chúng làm
features của model hoặc coi replay là lịch sử lỗi thật của từng máy. Ground truth
và các nhãn loại lỗi trong dataset không được gửi vào model input.

### 4.2. Tình huống dữ liệu lệch phân bố

| Scenario | Tình huống muốn mô phỏng |
| --- | --- |
| `normal` | Replay baseline để làm đối chứng. |
| `sudden-drift` | Một số thông số đổi phân bố đột ngột sau giai đoạn baseline. |
| `gradual-drift` | Các thông số dịch chuyển dần theo thời gian replay. |
| `categorical-drift` | Tỷ lệ `L/M/H` thay đổi. |

Các scenario drift phải giữ dữ liệu hợp lệ. Tên scenario không bảo đảm kết luận
drift trên mọi model/reference; Drift Worker quyết định theo dữ liệu, reference
và policy thực tế. Kết quả từ test fixture không phải bằng chứng về model production.

### 4.3. Tình huống lỗi để kiểm tra hệ thống

Simulator hỗ trợ tạo lỗi thiếu field, sai kiểu, ngoài miền hợp lệ, gửi trùng event
và giả lập timeout/disconnect tại client. Mục tiêu là kiểm tra backend từ chối
đúng, retry có giới hạn và không tạo kết quả trùng khi response bị mất.

Ví dụ: tốc độ quay âm là dữ liệu invalid; torque vẫn đúng kiểu và miền hợp lệ
nhưng phân bố cả cửa sổ thay đổi có thể là drift. Hai tình huống cần xử lý riêng.

## 5. Tiếp nhận và kiểm tra chất lượng dữ liệu

**Module:** backend ingestion và Data Quality Validator.
**API:** `POST /api/v1/sensor-readings`.

### 5.1. Tiếp nhận event

Backend nhận JSON, xác định máy, quản lý trace IDs và kiểm tra event trước khi
gọi ML. Máy phải được khai báo/seed từ trước; gửi nhầm một `machine_id` không tồn
tại trả `404 MACHINE_NOT_FOUND`, không tự tạo máy mới.

### 5.2. Validation

Các kiểm tra bao gồm cấu trúc, field bắt buộc, kiểu dữ liệu, số hữu hạn,
category hợp lệ và domain/range constraints. Ví dụ rpm phải lớn hơn 0, thời gian
mòn phải không âm. Các range rules bổ sung phải được quản lý bằng cấu hình/version.

| Kết quả | Hành vi backend | Kết quả để hiển thị |
| --- | --- | --- |
| `VALID` | Lưu event và tiếp tục gọi ML. | Dữ liệu hợp lệ; trạng thái máy còn phụ thuộc kết quả suy luận và risk policy. |
| `INVALID`, đủ IDs hợp lệ để ghi nhận | Lưu dữ liệu lỗi và validation issues; không gọi ML, không tạo maintenance alert từ event này. | Máy ở `INVALID`, hiển thị field sai và nguyên nhân; response `422`. |
| Không đủ IDs hợp lệ để ghi nhận | Trả lỗi request `422`; không bịa IDs hoặc prediction. | Hiển thị lỗi request. |
| JSON không parse được | Trả `400 INVALID_REQUEST`. | Người gửi cần sửa request. |

Dữ liệu invalid được giữ trong lịch sử khi có thể ghi nhận để phục vụ điều tra;
không được âm thầm sửa thành hợp lệ hoặc hiển thị máy là `NORMAL`.

### 5.3. Chống xử lý trùng

- Cùng `event_id` và cùng canonical payload: trả outcome đã lưu, không inference
  lần hai và không tạo thêm cảnh báo từ việc gửi lại.
- Cùng `event_id` nhưng payload khác: trả `409 EVENT_CONFLICT`.

Chức năng này khác với dedupe cảnh báo: một chức năng chống xử lý lại cùng
event, chức năng còn lại chống tạo quá nhiều cảnh báo từ các event khác nhau.

## 6. Dự đoán khả năng lỗi và quyết định rủi ro

### 6.1. Suy luận ML online

**Module:** `ml-service/`. **API nội bộ:** `POST /api/v1/predict`.

Backend chỉ gửi raw features đã được xác nhận `VALID`. ML dùng preprocessing
đi cùng model artifact rồi trả:

- `failure_probability`: score xác suất từ 0 đến 1.
- `prediction`: `NORMAL` hoặc `FAILURE`, theo `model_threshold`.
- ID prediction/event, phiên bản model/preprocessor, thời gian và độ trễ suy luận.

Khi probability bằng hoặc lớn hơn model threshold, nhãn là `FAILURE`. Output này
là dự đoán nhị phân, không khẳng định nguyên nhân lỗi cụ thể hay máy chắc chắn hỏng.

Nếu ML chưa sẵn sàng, mất kết nối hoặc timeout sau retry cho phép, event hiện
tại không có prediction và máy là `UNKNOWN`. Backend trả lỗi dependency rõ ràng;
không lấy prediction cũ hoặc score 0 để biểu thị kết quả của event mới.

### 6.2. Quyết định rủi ro tại backend

Risk Decision Engine áp dụng policy có version lên probability để quyết định
trạng thái máy và việc cần tạo cảnh báo.

| Trạng thái | Ý nghĩa nghiệp vụ |
| --- | --- |
| `NORMAL` | Có kết luận hợp lệ và risk dưới mức cần theo dõi theo policy. |
| `WATCH` | Cần theo dõi thêm. |
| `HIGH` | Rủi ro cao; tạo alert khi policy yêu cầu. |
| `CRITICAL` | Mức ưu tiên kiểm tra cao nhất; tạo alert khi policy yêu cầu. |
| `INVALID` | Dữ liệu đầu vào hiện tại không hợp lệ. |
| `UNKNOWN` | Chưa thể kết luận, ví dụ do ML không sẵn sàng. |

Model threshold quyết định nhãn ML; risk thresholds quyết định mức rủi ro của
sản phẩm. Hai loại ngưỡng có thể khác nhau. Các số ngưỡng minh họa trong tài liệu
gốc không phải giá trị đã được chứng minh phù hợp cho production.

Frontend hiển thị state backend trả, không tự tính severity từ probability.
Mỗi decision cần lưu `policy_version` để giải thích lại kết quả sau này.

### 6.3. Chuẩn bị model offline

Hệ thống cần pipeline phát triển model tách khỏi API suy luận:

1. Chuẩn bị dataset chỉ với sáu features được cho phép và target `Machine failure`.
2. Cố định train/validation/test split và seed để tái tạo kết quả.
3. Huấn luyện baseline ML, một model candidate khác và so sánh rule-based baseline.
4. Đánh giá recall, precision, F1, PR-AUC, false negative rate và confusion matrix;
   không chỉ dựa vào accuracy.
5. Chọn threshold trên validation, sau đó đánh giá cuối trên test.
6. Xuất model, preprocessor, metadata và drift reference tương ứng phiên bản.

Guide đặt mục tiêu `Recall_failure >= 0.85`, `Precision_failure >= 0.50`. Đây là
mục tiêu cần đo và báo cáo, không phải kết quả hiện đã đạt. Drift reference dùng
training split hoặc approved reference; không lấy test set làm baseline production.

## 7. Theo dõi đội máy và xem chi tiết máy

### 7.1. Fleet Dashboard

**API:** `GET /api/v1/machines`.

Người dùng xem danh sách máy gồm tên/ID, trạng thái hiện tại, risk score nếu có,
chất lượng dữ liệu, lần nhận dữ liệu gần nhất và số cảnh báo đang active. Có thể
lọc theo trạng thái và phân trang để tìm nhóm máy cần chú ý.

Máy đã được khai báo nhưng chưa có dữ liệu hiển thị `UNKNOWN`; thời gian nhận,
data status và risk chưa có được thể hiện là chưa có dữ liệu. Danh sách không có
kết quả hiển thị trạng thái rỗng thay vì lỗi server.

### 7.2. Machine Detail

**API:** `GET /api/v1/machines/{machine_id}`.

Từ dashboard, người dùng mở một máy để xem:

- Sáu thông số sensor mới nhất, thời điểm và kết quả validation.
- Trạng thái máy; nhãn ML, probability và model version nếu có prediction hiện tại.
- Các cảnh báo đang active để mở màn hình xử lý.
- Lịch sử sensor qua chức năng ở mục 7.3.

Khi event mới nhất invalid hoặc inference lỗi, prediction hiện tại là null.
Prediction cũ vẫn thuộc lịch sử; UI không được trình bày nó như kết luận mới.
Các alert active từ trước vẫn được giữ, không tự đóng vì máy chuyển `INVALID`
hoặc `UNKNOWN`.

### 7.3. Lịch sử sensor

**API:** `GET /api/v1/machines/{machine_id}/sensor-readings`.

Người dùng xem các bản ghi mới nhất trước, bao gồm event ID, thời gian, features,
validation issues và liên kết prediction ID nếu có. Record invalid giữ raw data
để chỉ ra lỗi, nên UI phải chịu được field thiếu hoặc sai kiểu.

Lịch sử được tải từng trang bằng cursor; frontend sử dụng cả timestamp và event
ID trả về để không bỏ sót các event có cùng thời gian. API hiện chưa định nghĩa
endpoint tra cứu prediction bất kỳ hoặc lịch sử prediction đầy đủ riêng biệt.

## 8. Tạo và xử lý cảnh báo bảo trì

### 8.1. Tự tạo và chống trùng cảnh báo

Alert Manager nhận quyết định từ Risk Engine, tạo alert khi policy yêu cầu và
lưu liên kết về máy, event, prediction, model version, policy version và trace IDs.
Alert được tạo trước khi cần kết quả Copilot.

Nếu nhiều event liên tiếp có cùng máy/severity trong dedupe window và alert cũ
còn active, hệ thống giữ một active alert thay vì tạo thêm liên tục. Cửa sổ dedupe
là cấu hình; `300` giây trong baseline là ví dụ cấu hình, không phải cam kết cố định.

`severity` cho biết mức ưu tiên (`WATCH`, `HIGH`, `CRITICAL`); `status` cho biết
tiến độ xử lý (`OPEN`, `ACKNOWLEDGED`, ...). Tiếp nhận một alert không làm severity
tự giảm và không làm máy tự chuyển `NORMAL`.

### 8.2. Danh sách và chi tiết cảnh báo

**APIs:** `GET /api/v1/alerts`, `GET /api/v1/alerts/{alert_id}`.

Danh sách hỗ trợ lọc theo status, severity, machine ID và phân trang. Màn hình
chi tiết tập hợp alert, sensor snapshot, prediction, risk score, model/policy
versions, trạng thái Copilot và timeline xử lý để kỹ thuật viên hiểu bối cảnh.

### 8.3. Các thao tác của kỹ thuật viên

Các action dùng `POST /api/v1/alerts/{alert_id}/{action}`:

| Action | Người dùng muốn làm gì? | Trạng thái nguồn hợp lệ | Trạng thái đích |
| --- | --- | --- | --- |
| `ack` | Xác nhận đã tiếp nhận cảnh báo. | `OPEN` | `ACKNOWLEDGED` |
| `start` | Bắt đầu hoặc tiếp tục xử lý. | `ACKNOWLEDGED`, `ESCALATED` | `IN_PROGRESS` |
| `escalate` | Chuyển cảnh báo lên mức cần hỗ trợ/xử lý tiếp. | `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS` | `ESCALATED` |
| `resolve` | Kết thúc xử lý, ghi nội dung kết quả. | `ACKNOWLEDGED`, `IN_PROGRESS`, `ESCALATED` | `RESOLVED` |
| `false-alert` | Ghi nhận kiểm tra không xác nhận bất thường. | `OPEN`, `ACKNOWLEDGED`, `IN_PROGRESS` | `FALSE_ALERT` |

Các thao tác ghi nhận người thực hiện và note theo contract; `resolve` bắt buộc
có `resolution`, `false-alert` bắt buộc có `reason`. Nếu bật auth, actor phải khớp
người đã xác thực và có quyền; tự điền `actor_id` không cấp quyền thực hiện.

`RESOLVED` và `FALSE_ALERT` là trạng thái kết thúc. Contract hiện không có reopen.
Chuyển sai trạng thái, kể cả gửi lại action đã hoàn tất, trả
`409 INVALID_ALERT_TRANSITION`. UI cần tải lại alert để hiển thị các action còn hợp lệ.

Action chỉ được báo thành công sau khi trạng thái và audit history đã được lưu.
Hai kỹ thuật viên cùng thao tác phải được xử lý theo state hiện tại tại backend;
UI không quyết định chuyển state chỉ bằng trạng thái đã đọc từ trước.

## 9. Technician Copilot: giải thích và hướng dẫn có căn cứ

### 9.1. Nội dung hỗ trợ

Copilot hoạt động trên alert đã tồn tại, tự enrich theo luồng backend hoặc khi
kỹ thuật viên yêu cầu. Ba intent được hỗ trợ trong contract:

| Intent | Nội dung |
| --- | --- |
| `MAINTENANCE_BRIEF` | Tóm tắt cảnh báo và bối cảnh kiểm tra. |
| `TECHNICIAN_CHECKLIST` | Các bước kiểm tra có dẫn nguồn. |
| `ALERT_EXPLANATION` | Giải thích cảnh báo trong phạm vi prediction và bằng chứng. |

Brief gồm summary, uncertainty, checklist và citations. Mỗi actionable checklist
item phải có nguồn tới chunk thực sự đã được retrieval trả về. Citation giữ
document ID/version, section/page và chunk ID để người dùng đối chiếu nguồn.

Đây là hỗ trợ quyết định theo bối cảnh alert, chưa phải một giao diện chat tự do.
Hiển thị citation metadata không đồng nghĩa đã có API tải hoặc mở PDF gốc.

### 9.2. Yêu cầu và đọc kết quả qua backend

1. UI gửi `POST /api/v1/alerts/{alert_id}/brief` với intent.
2. Backend chỉ trả `202 PENDING` khi yêu cầu đã được ghi nhận bền vững; đây là
   xác nhận tiếp nhận công việc, chưa phải brief đã sinh thành công.
3. Backend gọi RAG ngoài luồng ingestion; yêu cầu cùng alert/intent khi còn pending
   sử dụng lại công việc đang chờ.
4. UI đọc `GET /api/v1/alerts/{alert_id}/brief` bằng polling hoặc cập nhật sau
   sự kiện `brief.ready` nếu triển khai SSE.

API GET đọc kết quả đã lưu, không tự gọi LLM. Backend gọi API nội bộ
`POST /api/v1/briefs` của RAG; frontend không gọi service này trực tiếp.

### 9.3. Các trạng thái cần hiển thị

Có hai lớp trạng thái khác nhau:

| Lớp | Giá trị | Cách hiểu |
| --- | --- | --- |
| `copilot_status` | `NOT_REQUESTED` | Chưa yêu cầu brief; không phải lỗi. |
| `copilot_status` | `PENDING` | Yêu cầu đã tiếp nhận, đang chờ kết quả. |
| `copilot_status` | `AVAILABLE` | Có kết quả; cần đọc tiếp `grounded_status`. |
| `copilot_status` | `UNAVAILABLE` | Không cung cấp được brief lúc này; alert vẫn xử lý được. |
| `grounded_status` | `GROUNDED` | Có checklist và citations đã qua kiểm tra liên kết nguồn. |
| `grounded_status` | `INSUFFICIENT_EVIDENCE` | Thiếu bằng chứng; thông báo cần manual review, checklist/citations rỗng. |
| `grounded_status` | `VALIDATION_FAILED` | Kết quả sinh không đạt kiểm tra grounding; không hiển thị actionable advice. |

`AVAILABLE` không bảo đảm có lời khuyên: brief hoàn tất nhưng thiếu bằng chứng
vẫn có thể là `AVAILABLE / INSUFFICIENT_EVIDENCE`, HTTP `200`. Khi RAG/provider
unavailable, backend biểu diễn `copilot_status=UNAVAILABLE`; alert giữ nguyên.

### 9.4. Chuẩn bị và sử dụng kho tri thức

Phía RAG cần pipeline ingestion tài liệu: parse, giữ metadata trang/mục, chia
chunks, tạo embeddings và lưu PostgreSQL/pgvector. Phiên bản tài liệu mới không
được xóa lịch sử nguồn đã dùng cho brief cũ.

Khi sinh brief, RAG xây query từ bối cảnh, lọc metadata khi phù hợp, truy xuất
chunks, tạo prompt, gọi LLM và kiểm tra citations. Thiếu evidence phải trả trạng
thái rõ ràng; không tự bù hướng dẫn kỹ thuật từ trí nhớ chung của LLM. Chất lượng
retrieval cần được đánh giá bằng gold set và `Recall@k` theo Guide.

Đây là chức năng chuẩn bị dữ liệu nội bộ; contract hiện chưa có màn hình/API
upload, xóa hay quản trị tài liệu dành cho người dùng.

## 10. Ghi nhận kết quả kiểm tra và phản hồi

**API:** `POST /api/v1/alerts/{alert_id}/feedback`.

Kỹ thuật viên có thể ghi nhận hai đánh giá độc lập:

| Nhóm phản hồi | Giá trị | Ý nghĩa |
| --- | --- | --- |
| Kết quả kiểm tra — `technician_verdict` | `CONFIRMED_ISSUE` | Có vấn đề được kiểm tra xác nhận. |
| Kết quả kiểm tra | `FALSE_ALERT` | Không xác nhận được bất thường. |
| Kết quả kiểm tra | `INCONCLUSIVE` | Chưa đủ cơ sở kết luận. |
| Độ hữu ích — `recommendation_feedback` | `USEFUL`, `PARTIALLY_USEFUL`, `NOT_USEFUL`, `NOT_AVAILABLE` | Đánh giá khuyến nghị hữu ích, hữu ích một phần, không hữu ích hoặc không có để đánh giá. |

Feedback có thể kèm note; khi lưu thành công backend trả `201` cùng feedback ID,
alert ID và thời gian ghi nhận. Dữ liệu này phục vụ audit và đánh giá sau này.

Gửi feedback `FALSE_ALERT` không tự chuyển lifecycle alert sang `FALSE_ALERT`.
Muốn đổi trạng thái phải gọi action tương ứng. Feedback cũng không tự điều
chỉnh policy, retrain hoặc promote model. Không retry POST feedback một cách
mù quáng nếu chưa biết lần gửi trước đã được lưu hay chưa.

## 11. Theo dõi drift dữ liệu và prediction

**Module:** `drift-worker/`. **Đối tượng sử dụng kết quả:** người vận hành/ML và
người theo dõi monitoring.

### 11.1. Câu hỏi chức năng cần trả lời

Worker đánh giá: **dữ liệu vận hành gần đây có khác đáng kể so với reference
được dùng cho model đang đánh giá không?** Ví dụ từng record torque vẫn hợp lệ,
nhưng cả cửa sổ có tỷ lệ torque cao hơn nhiều so với reference.

Worker tính drift trên sáu features; có thể đánh giá thêm thay đổi tỷ lệ dự đoán
`FAILURE/NORMAL` khi có đủ predictions đúng model và prediction reference.
Prediction drift là thay đổi phân bố đầu ra, không chứng minh model đã giảm
độ chính xác. Performance drift cần ground truth đáng tin cậy mới kết luận được.

### 11.2. Dữ liệu đầu vào và policy

- Chỉ lấy events `VALID`; không đưa invalid records, IDs, timestamp hoặc target
  vào features để tính drift.
- Reference phải khớp model/dataset version; không tự chọn reference gần nhất
  hoặc tạo reference từ runtime window để che mất drift.
- Implementation hiện lấy tối đa 500 events hợp lệ mới nhất trên toàn bộ tập
  runtime; minimum mặc định là 200. Đây là cửa sổ theo số bản ghi, không phải mặc
  định 15 phút hoặc báo cáo riêng cho từng máy.
- Các rolling windows có thể chồng lấp. Checkpoint ghi tiến độ thành công,
  không biến window thành batch chỉ chứa những event chưa từng được đọc.
- Numeric và categorical features dùng PSI. Policy hiện có warning `0.10`,
  critical `0.20` ở cấp feature; report tổng thể critical khi ít nhất hai features
  critical. Các giá trị này cần version hóa và hiệu chỉnh bằng evidence.

Một feature critical đưa report tổng thể lên warning nếu chưa đủ điều kiện
aggregate critical. Prediction drift có thể góp vào cảnh báo tổng thể warning;
không được hiểu mọi nhãn critical cấp feature đều làm máy thành critical.

### 11.3. Kết quả và xử lý lỗi

| `overall_status` | Ý nghĩa để người vận hành xử lý |
| --- | --- |
| `STABLE` | Chưa có bằng chứng drift vượt policy hiện tại. |
| `WARNING` | Có tín hiệu lệch phân bố cần theo dõi/điều tra. |
| `CRITICAL` | Drift đạt mức nghiêm trọng theo aggregate policy. |
| `INSUFFICIENT_DATA` | Chưa đủ mẫu để kết luận; không báo stable cho trường hợp này. |
| `ERROR` | Không tính được report, ví dụ thiếu reference đúng version. |

Report lưu model/reference/detector versions, cửa sổ, số mẫu, scores,
thresholds và lý do để truy vết. Retry cùng input dùng report đã có thay vì tạo
bản sao vô hạn. Prediction đến muộn hoặc reference/policy thay đổi có thể tạo
report mới theo identity của input.

Worker chạy một lượt rồi thoát: lưu report → công bố metrics → commit checkpoint
→ cập nhật heartbeat. Khi metrics publication là bắt buộc nhưng thất bại,
checkpoint không tiến; lượt sau có thể dùng report đã lưu để thử lại.

Drift chỉ tạo kết quả monitoring. Nó không tự đổi trạng thái máy, severity hoặc
policy; không tự tạo maintenance alert, retrain hoặc promote model. Platform
có thể cảnh báo drift/stale/error bằng Prometheus rules ở repo GitOps riêng.

### 11.4. Xem danh sách và chi tiết báo cáo

**APIs:** `GET /api/v1/drift-reports`, `GET /api/v1/drift-reports/{report_id}`.

Người dùng xem reports đã lưu, lọc theo model version/overall status, tải trang
cũ hơn và mở chi tiết các scores cùng reference/policy context. API đọc không
chạy lại detector và hiện không có filter theo `machine_id`.

Report tồn tại với status `ERROR` hoặc `INSUFFICIENT_DATA` vẫn trả HTTP `200`.
Chỉ report không tồn tại mới trả `404 DRIFT_REPORT_NOT_FOUND`.

## 12. Giám sát sức khỏe hệ thống và độ mới dữ liệu

### 12.1. Monitoring trên ứng dụng

**API:** `GET /api/v1/monitoring/summary`.

Màn hình monitoring cho biết trạng thái backend/ML/RAG, thông báo dependency,
thời điểm quan sát, report drift gần nhất và tình trạng worker. Dependency có
thể là `AVAILABLE`, `DEGRADED` hoặc `UNAVAILABLE`; đọc được observation degraded
vẫn là request HTTP `200` thành công.

Cần phân biệt ba thông tin:

| Thông tin | Câu hỏi được trả lời |
| --- | --- |
| Report `overall_status` | Phân bố dữ liệu được đánh giá là stable, drift hay chưa kết luận được? |
| Worker `FRESH` / `STALE` / `NOT_AVAILABLE` | Worker có hoàn tất thành công gần đây theo checkpoint không? |
| `latest_report.window_end_at` | Dữ liệu dùng trong report mới đến thời điểm nào? |

Worker có thể còn fresh nhưng dữ liệu đầu vào đã cũ. Report được dùng lại có thể
giữ `computed_at` cũ dù worker vừa chạy thành công; không dùng riêng thời gian
tính report làm heartbeat. Ngưỡng stale là cấu hình môi trường.

### 12.2. Health, metrics và logs cho vận hành

Backend, ML và RAG cần `/health/live`, `/health/ready` và `/metrics`. Liveness
cho biết process sống; readiness cho biết khả năng phục vụ. RAG không sẵn sàng
không được tự làm backend mất readiness của luồng cảnh báo.

Metrics phục vụ theo dõi request/errors/latency, chất lượng dữ liệu, prediction,
cảnh báo, retrieval/citation failures và drift. Worker one-shot công bố metrics
qua Pushgateway để Prometheus đọc khi process đã thoát. Structured logs giữ
trace IDs để điều tra, không ghi secrets.

Prometheus/Grafana cung cấp quan sát kỹ thuật sâu hơn. API monitoring hiện chưa
trả các chỉ số p95/error rate đầy đủ và chưa có URL Grafana trong DTO; việc thêm
dashboard/link trên UI cần cấu hình hoặc bổ sung contract phù hợp. Manifests,
lịch worker, alert rules và Grafana dashboards thuộc repo GitOps/hạ tầng riêng.

## 13. Cập nhật giao diện và xử lý trạng thái

### 13.1. Tải dữ liệu và cập nhật gần thời gian thực

REST là cách đọc trạng thái chuẩn. MVP có thể dùng polling. SSE là tính năng
tùy chọn qua `GET /api/v1/events/stream`, với các events:

`machine.updated`, `alert.created`, `alert.updated`, `brief.ready`, `service.degraded`.

SSE giúp UI cập nhật nhanh, không bảo đảm replay không mất event. Sau reconnect,
UI phải tải lại REST. Việc chưa triển khai SSE không chặn hoàn thành luồng nghiệp vụ.

### 13.2. Những trạng thái UI phải phân biệt

| Tình huống | Hành vi giao diện |
| --- | --- |
| Đang tải request | Hiển thị `LOADING`; không kết luận trạng thái máy từ dữ liệu chưa có. |
| Danh sách rỗng | Hiển thị `EMPTY`, không coi là lỗi server. |
| `NORMAL` / `WATCH` / `HIGH` / `CRITICAL` | Hiển thị đúng state từ backend, kèm risk nếu có. |
| Dữ liệu `INVALID` | Chỉ ra lỗi validation; không render như trạng thái bình thường. |
| Máy `UNKNOWN` | Thông báo chưa thể kết luận; không thay bằng score 0 hoặc màu/nhãn normal. |
| Copilot pending/unavailable | Hiển thị trạng thái riêng; giữ phần cảnh báo và các action hoạt động. |
| Brief thiếu evidence/grounding thất bại | Hiển thị thông báo và không đưa actionable checklist. |
| Request lỗi mạng/API | Hiển thị `ERROR` và hướng xử lý phù hợp; không spinner vô hạn. |
| Resource không tồn tại | Hiển thị not found; khác danh sách không có phần tử. |
| Action bị `409` | Thông báo state đã thay đổi/không hợp lệ, tải lại dữ liệu. |
| Không có quyền khi bật auth | Xử lý `401/403`; không gửi lại liên tục như lỗi transient. |

`LOADING`, `EMPTY`, `ERROR` là trạng thái request/render. Chúng không được thêm
vào enum trạng thái máy hoặc dùng để thay thế `INVALID`/`UNKNOWN`.

## 14. Audit và truy vết quyết định

Từ một cảnh báo, hệ thống cần truy ngược được:

```text
Alert → Prediction → Sensor event → Máy + validated features
      → Model/preprocessor versions
      → Risk policy version
      → Timeline thao tác + người thực hiện
      → Feedback
      → Brief → Document version + chunk + page/section
```

Timeline trình bày các sự kiện theo thời gian, gồm tạo cảnh báo và thay đổi
trạng thái, trạng thái trước/sau, actor, note và timestamp. Các mốc thời gian
được lưu có timezone; frontend có thể hiển thị theo timezone người dùng.

Mục tiêu là trả lời được: cảnh báo do dữ liệu nào, model/policy nào, ai đã xử lý,
khuyến nghị lấy từ tài liệu nào và người dùng phản hồi ra sao. Request/correlation
IDs hỗ trợ nối log giữa các service; không đưa các ID riêng lẻ vào metric labels.

## 15. Các kịch bản sử dụng tiêu biểu

| Kịch bản | Chuỗi hành vi mong đợi |
| --- | --- |
| Máy hoạt động bình thường | Sensor valid → ML có prediction → risk policy trả normal → dashboard hiển thị bình thường. |
| Có rủi ro cần xử lý | Sensor valid → ML → risk policy yêu cầu alert → alert xuất hiện → technician ACK/start → kiểm tra → resolve và ghi feedback. |
| RAG không hoạt động | Alert vẫn tạo và xử lý được → Copilot báo unavailable → technician tiếp tục kiểm tra theo quy trình của mình. |
| RAG thiếu bằng chứng | Brief hoàn tất với insufficient evidence → không có checklist giả → người dùng được yêu cầu manual review. |
| Sensor gửi dữ liệu lỗi | Backend lưu lỗi khi đủ IDs → không gọi ML → UI hiển thị invalid và nguyên nhân; alert active trước đó vẫn còn. |
| ML timeout | Event không có prediction hiện tại → UI hiển thị unknown/dependency failure → không suy đoán máy normal. |
| Gửi lại cùng event do mất response | Backend trả outcome đã lưu → không phát sinh prediction/alert thứ hai. |
| Dữ liệu đổi phân bố nhưng vẫn hợp lệ | Online path tiếp tục → worker batch đánh giá drift → monitoring cảnh báo riêng; machine severity vẫn theo risk policy. |
| Worker lâu không thành công | Monitoring hiển thị stale theo checkpoint → người vận hành điều tra; việc tiếp nhận sensor không bị chặn. |

## 16. Ranh giới chức năng hiện tại

Những phần dưới đây chưa nằm trong chức năng đã chốt hoặc cần quyết định riêng
trước khi triển khai:

- Điều khiển máy, PLC/actuator hoặc tự động thực hiện bảo trì.
- Dự đoán chính xác nguyên nhân/loại lỗi hoặc remaining useful life; model hiện
  có nhiệm vụ binary machine failure classification.
- Tự retrain/promote model hoặc tự đổi risk policy khi có drift/feedback.
- Chat Copilot tự do, gọi RAG trực tiếp từ frontend, hoặc đưa hướng dẫn không có evidence.
- Quản lý lịch bảo trì, phụ tùng, đơn công việc, phân công người xử lý hoặc gửi
  email/SMS; các API hiện tại chưa định nghĩa những chức năng này.
- CRUD máy, quản lý người dùng, UI upload tài liệu, API chỉnh policy hoặc API
  xem/tải file nguồn; quyền dự kiến và dữ liệu lưu trữ không tự tạo ra các API đó.
- Báo cáo performance drift khi chưa có ground truth đủ tin cậy.

Các chức năng tùy chọn như auth và SSE phải được đánh dấu đúng khi lập kế hoạch.
Nếu mở rộng phạm vi, cập nhật contract, API docs và chức năng liên quan cùng nhau.

## 17. Bản đồ chức năng để triển khai

| Nhóm chức năng | Module chính | Tham chiếu System Contracts |
| --- | --- | --- |
| Replay, fault injection và drift scenarios | `sensor/` | 2–5, 25A.5 |
| Ingestion, validation, event idempotency | `backend/` | 4–6 |
| Prediction và artifacts/reference | `ml-service/` | 7–8 |
| Risk decision và tạo/dedupe alert | `backend/` | 9–10 |
| Lifecycle, audit và feedback | `backend/`, `frontend/` | 11–12, 17–18 |
| Brief, citations và knowledge base | `rag-service/`, `backend/`, `frontend/` | 13–14 |
| Fleet, máy, sensor history và alert views | `frontend/`, `backend/` | 15, 24 |
| Cập nhật SSE tùy chọn | `backend/`, `frontend/` | 16 |
| Drift reports, persistence và batch execution | `drift-worker/`; backend/frontend đọc kết quả | 3.8, 8.2, 17, 25A |
| Monitoring, health, metrics, logs | Các service; platform ở repo riêng | 19–21, 23, 25A |
| Phân quyền khi bật auth | `backend/`, các service liên quan | 22; phụ lục AUTH-01 |

Các bổ sung HTTP chưa có đầy đủ ở baseline, như brief facade, monitoring summary
và read APIs của drift reports, được giải thích trong
[phụ lục HTTP](api-contract-decisions.md) và ba OpenAPI tại `contracts/`.
