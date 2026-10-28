# OUTCOME-14-ONLY — Thống nhất kết quả Processing theo outcome 1.4.0

Quyết định ngày **2026-10-28**, revision **1.0.0**. Người dùng chọn dùng duy nhất
outcome `1.4.0`; không bổ sung hoặc duy trì cách tính risk cho legacy `2.0.0`.
Quyết định này thay các đoạn trước đây cho phép reader nhận cả hai định dạng.

## 1. Luồng dữ liệu và ý nghĩa của risk

```text
Sensor → Ingestion → sensor.readings → Processing Worker
                                         │
                                  DQ → ML → Risk policy
                                         │
                                processing_db + outbox
                                         │
                                    Outbox Relay
                                         │
                         inference.results: outcome 1.4.0
                                         │
                         Business Consumer → business_db
                                         │
                           Business API → Fleet / UI
```

Processing chịu trách nhiệm tạo `risk_score`. Business lưu nguyên điểm này và
đọc nó để trả `latest_risk_score` trong Fleet. Business không lấy probability
thay risk, không gọi lại model và không chạy lại policy trong request GET.

Ví dụ fixture có `failure_probability=0.8` và `risk_score=0.5`: UI phải nhận
`latest_risk_score=0.5`. Hai giá trị có ý nghĩa khác nhau; policy hiện tại có thể
chọn chúng bằng nhau nhưng consumer không được mặc định điều đó.

## 2. Một định dạng outcome duy nhất

Schema chuẩn là [Kafka outcome](../contracts/kafka/events/inference-result.schema.json).
Topology chỉ cho phép đọc và ghi `schema_version=1.4.0` trên `inference.results`.
Không đổi tên topic, partition, Kafka key `machine_id` hoặc consumer group.

| Trường hợp | Nội dung bắt buộc và cách Business sử dụng |
| --- | --- |
| `kind=PREDICTION_COMPLETED` | `risk_score` là số từ 0 đến 1, cùng prediction, model/policy identity và provenance; Consumer lưu risk gốc |
| `kind=DATA_INVALID` | Có lỗi DQ; không thêm prediction hoặc risk; giữ prediction hợp lệ cũ ở trục lịch sử |
| `kind=INFERENCE_UNAVAILABLE` | Có `failure.code` và thông tin lỗi; không tạo prediction/risk thành công giả |

Source ID trong outcome là `event_id`; `source_event_id` vẫn hợp lệ ở những DTO
khác, chẳng hạn wrapper lịch sử hoặc LiveSnapshot. Field đó không phải dấu hiệu
legacy nếu nó thuộc DTO khác. `result_kind` ở SSE cũng không tự đổi theo Kafka:
SSE và outcome là hai contract khác nhau.

Event `2.0.0`, thiếu risk khi COMPLETED, thiếu lineage hoặc sai schema đều bị
Business runner từ chối trước callback DB. Runner không commit offset của record
lỗi, không bỏ qua nó và không tự chuyển sang event kế tiếp. Đây là lỗi đầu vào
cần xử lý tại producer/môi trường, không phải risk bằng 0 hoặc máy NORMAL.

Storage Consumer có trách nhiệm khác: lưu nguyên Kafka bytes cùng tọa độ và
manifest để giữ bằng chứng, kể cả event sai schema. Archive không xác nhận event
đó hợp lệ cho nghiệp vụ và không biến event cũ thành outcome mới.

## 3. Schema API/lịch sử phải dùng cùng nguồn chuẩn

`contracts/v1.3/schemas/runtime.schema.json` giữ tên entrypoint `ProcessingResult`,
nhưng trỏ tới schema Kafka 1.4.0. Các definitions union 2.0.0 đã được gỡ; examples
outcome và history dùng payload 1.4.0. Validator offline nạp thêm đúng schema
Kafka này. Không cần sao chép một union khác vào bộ HTTP.

History wrapper vẫn dùng `source_event_id`; validator so nó với `outcome.event_id`.
Versions của Business REST/SSE, sensor, ML/RAG và Drift giữ phạm vi riêng, không
bump tất cả chỉ để giống outcome. Ví dụ cũ có version 2.0.0 chỉ còn là **negative
fixtures**: chúng chứng minh reader phải từ chối, không phải hỗ trợ compatibility.

## 4. Phiên bản package và migration SQL

| Artifact | Phiên bản sau thay đổi | Lý do |
| --- | --- | --- |
| Outcome trên Kafka | `1.4.0` | Định dạng producer đã có, nay là định dạng duy nhất được nhận |
| Python SDK `maintai-messaging` | `0.3.0` | Bỏ compatibility reader và thuộc tính `DecodedEvent.legacy` |
| Kafka contract bundle | `2.0.0` | Breaking change: thu hẹp tập payload được reader chấp nhận |

**Bundle 2.0.0 không phải outcome legacy 2.0.0.** Bundle là phiên bản cả bộ
topology/schema/SQL; outcome là version nằm trong từng message.

Backend, Processing, Ingestion và Storage cùng pin SDK 0.3.0; CI build wheel này.
Các release receipts 1.0.0 và 1.1.0 được giữ để truy vết. SQL `001_messaging.sql`
và `002_processing_ordering.sql` giữ nguyên bytes. Wrapper Alembic của Processing
kiểm SHA của migration002 thay vì buộc cả bundle phải bằng 1.1.0, nên cài mới với
bundle 2.0.0 vẫn chạy đúng SQL đã duyệt. Không thêm migration backfill legacy risk.

Chế độ relay tên `legacy` còn lại là cơ chế claim **theo row** trước ordered
relay; nó không có nghĩa hỗ trợ outcome 2.0.0. Migration này không thay thuật toán
concurrency, ownership fence, ordered dispatch hoặc cách commit offset.

## 5. Cách áp dụng cho môi trường chạy

Nếu môi trường mới hoặc chưa có event legacy: build lại các image từ cùng commit,
chạy owner migrations hiện hành, provision machine registry và khởi động các
service theo hướng dẫn từng module. Worker/Relay tạo outcome 1.4.0; Consumer nhận
risk có sẵn. Đây không phải xác nhận full pipeline đã sẵn sàng: Alert source,
provider ML/RAG, deployment và các chức năng chưa implement vẫn cần hoàn thiện.

Nếu môi trường đã chứa legacy, không chỉ đổi image rồi coi là đã migrate data:

1. Dừng/drain producer, relay và consumer liên quan; kiểm kê pending outbox,
   retained Kafka records, committed offsets và các projection đã lưu.
2. Với demo bỏ được dữ liệu cũ, chủ môi trường có thể chọn môi trường Kafka/DB
   cô lập mới và chạy Sensor lại. Không chỉ đổi group hoặc reset offset trong DB
   cũ: inbox/CAS và dữ liệu latest vẫn có thể không cùng lịch sử.
3. Nếu phải giữ dữ liệu cũ, tạm dừng cutover và lập kế hoạch lưu trữ/reprocess
   riêng từ raw source cùng model/policy/provenance thật. Không đổi nhãn version,
   bịa hash, xóa inbox hoặc suy ra risk từ probability để lách schema.
4. Chỉ chuyển reader mới vào luồng đã xác nhận phù hợp; theo dõi lỗi schema,
   consumer lag và cập nhật Fleet. Gặp event cũ sẽ dừng, không tự skip offset.

Thay đổi source này không xóa topic/DB, reset offset hay chạy migration trên
môi trường của người dùng. Không có migration chuyển dữ liệu cũ trong PR này.

## 6. Kiểm tra bắt buộc

- Native completed/invalid/unavailable phải decode/encode được; wire/hash giữ nguyên.
- COMPLETED thiếu risk, risk null/sai kiểu/ngoài [0,1] phải bị từ chối.
- Cả ba loại outcome legacy phải bị từ chối; Business không callback/commit.
- Chỉ đổi version và vài tên field của legacy vẫn thất bại vì thiếu provenance thật.
- Fleet trả đúng risk gốc dù khác probability; INVALID/FAILED không xóa prediction lịch sử.
- HTTP/history examples dùng cùng union chuẩn; sai lineage vẫn bị validator bắt.
- Archive vẫn giữ bytes lỗi nguyên vẹn; SDK mới không làm hỏng Ingestion/Storage.
- Build wheel và kiểm hash bundle/SQL; chạy DB/Kafka/container suites trong CI hoặc
  môi trường test cô lập. Unit/contract checks không thay bằng chứng triển khai.
