# M6-RAW-01 — Protocol raw archive catalog

Ngày29/10/2026; revision **1.0.0**, theo
[M6-HISTORY-01](M6_HISTORY_ARCHITECTURE_DECISION.md). Protocol provider có schema và helper đóng gói trong `maintai-storage-consumer==0.2.1`;
HTTP DTO M6 và export có schema riêng trong `contracts/v1.3`.
Giữ nguyên raw-manifest1.0.0, Kafka bundle2.0.0, SDK0.3.x và archive identity hiện có.

## 1. Đã có gì, cần bổ sung gì

Storage0.1.0 ghi exact raw bytes + key/headers vào JSONL.gz; conditional PUT,
checksum, object VersionId, immutable manifest; commit Kafka sau object+manifest.
Đường dẫn hiện có theo topic/partition/offset. Retry không được thay bytes tại cùng
locator. Xem [Storage README](../storage-consumer/README.md).

M6 cần thêm lớp metadata tra theo **máy + received_at**, và bằng chứng tập receipts
đã có thể query. Object/manifest đã ghi không tự tạo catalog; Kafka committed
offset hoặc metric lag cũng không thay coverage của catalog. Không trao quyền
SQL processing_db/business_db cho Storage.

## 2. Giao tiếp đã chọn: catalog S3/MinIO, không thêm service

Storage công bố immutable catalog generations và một pointer `current.json` nhỏ.
Read API đọc pointer, pin generation/object versions rồi sync metadata vào
`history_meta` bằng credential giới hạn của chính Processing. History request
query index này, không scan toàn bucket. Bounded scans/reconciliation chạy nền
trong runtime Read API/Storage, không trong HTTP request hay Processing Worker.

Layout catalog bổ sung, độc lập raw prefix hiện hành:

```text
raw-catalog/v1/current.json
raw-catalog/v1/generations/<generation_id>.json
raw-catalog/v1/segments/<segment_id>.json
```

Pointer dùng `schema_version`, `generation_id` và exact `generation_ref`.
Generation dùng `schema_version`, `source_cluster_id`, `parent_ref`, `segments`
và full partition coverage. `segments` là danh sách ObjectRef trực tiếp; segment
chứa `schema_version` và `entries`, tối đa100 entries và2 MiB mỗi document.

Generation là **delta**, không chứa lại toàn bộ lịch sử. Reader duyệt newest-first
qua exact `parent_ref` đến checkpoint đã commit hoặc root; lưu checkpoint để tiếp
tục sau restart và không công bố dữ liệu nhập dở là đầy đủ. Metadata index query
theo máy/time; không quét bucket mỗi HTTP request. Xem
[reader guide](../storage-consumer/M6_READER_GUIDE.md) và
[schema provider](../storage-consumer/src/maintai_storage/schemas/catalog.json).

Publisher ghi segments → verify refs → immutable generation → CAS pointer. CAS
phải reconcile các partitions khi nhiều replicas/rebalance; không cho generation
mới bỏ mất receipts đã publish. Parent generation/ref và start offsets cho phép
rebuild/catch-up. Pointer chỉ là discovery, không thay generation đã pin. Các keys
metadata không lấy trực tiếp từ text machine_id không được sanitize.

## 3. Logical catalog entry đã freeze

| Field | Kiểu / quy tắc |
| --- | --- |
| `catalog_schema_version` | `1.0.0`, protocol riêng của catalog |
| `receipt_id` | Identity ổn định từ topic/partition/offset; cùng locator chỉ một logical receipt |
| `topic`, `partition`, `offset` | String allowlisted, integer>=0, integer>=0; không so offset khác partition |
| `source_event_id`, `machine_id` | String khi parse được; null cho rejected/poison thiếu trường |
| `received_at` | UTC nguồn, null nếu không hợp lệ; không tự suy từ upload time |
| `server_accepted_at` | UTC từ trusted ingress metadata khi có; không fabricate |
| `source_identity`, `request_id` | Metadata được phép công bố, nullable; quarantine query được kiểm quyền |
| `receipt_kind` | `ACCEPTED` hoặc `REJECTED`; không tự gắn DQ VALID từ raw |
| `raw_object_ref` | bucket/key/version_id/sha256, exact ref từ manifest |
| `raw_manifest_ref` | Exact ObjectRef của raw manifest đã verify |
| `event_locator` | Record index trong object; hiện record0 vì microbatch1 |

Raw-manifest hiện hành không có đủ machine/time fields nên publisher phải trích
metadata có giới hạn từ frame/envelope hoặc lúc archive, giữ exact raw bytes.
Malformed data vẫn giữ receipt entry; không cho một poison entry làm mất phần
còn lại của catalog. Raw accepted không có received_at hợp lệ bị tách quarantine,
không âm thầm bỏ khỏi coverage receipts hoặc ép vào machine timeline.

Manifest/object refs phải kiểm checksum và version; ETag chỉ dùng concurrency CAS,
không thay SHA-256. S3/MinIO versioning bắt buộc cho full M6 local; không dùng
`S3_ALLOW_UNVERSIONED=true` làm bằng chứng exact-version reader đã hoạt động.

## 4. Coverage theo partition và ý nghĩa COMPLETE

Generation giữ `generation_id`, `published_at`, `parent_ref`, segment refs và
`partitions[]`. Mỗi partition có:

| Field | Ý nghĩa |
| --- | --- |
| `topic`, `partition`, `topic_id` | Scope và identity topic; generation giữ source_cluster_id để phát hiện đổi nguồn |
| `start_offset` | Điểm bắt đầu đã xác định; không ngầm hứa dữ liệu trước đó |
| `archived_next_offset` | Frontier đã xác minh raw+manifest bền vững; exclusive |
| `indexed_next_offset` | Frontier đã đưa vào catalog và xác minh; <= archived frontier |
| `captured_log_end_offset`, `captured_at` | Mốc Kafka quan sát để mô tả lag của generation; nullable khi chưa có bằng chứng |
| `gaps` | Các ranges thiếu/retention loss/corruption đã biết, kèm reason |
| `status` | `COMPLETE`, `LAGGING`, `GAP`, `UNKNOWN`, xét theo scope đã công bố |

Frontier là **safe prefix của receipts trong log đã kiểm tra**; offset control/
transactional hoặc offset không chứa user record không được biến thành missing
reading chỉ vì số offset không liên tiếp. Phải phát hiện retention/truncation và
thay đổi source log; không đổi start_offset để che mất gap. Một catalog thiếu entry
không được tiến indexed frontier vượt receipt đó.

`COMPLETE` chỉ nghĩa đầy đủ từ start_offset đến frontier đã chốt cho partition,
không khẳng định mọi sensor đã gửi mọi dữ liệu trước một timestamp. Không dùng
max(received_at) hoặc max(offset đã thấy) làm watermark completeness. Với12
partitions phải giữ12 trạng thái; không lấy một partition nhanh đại diện cả hệ thống.

Read API kết hợp scope query, generation, gaps và thời điểm sync để quyết định
response. Snapshot COMPLETE là đầy đủ **trong tập receipts/cutoff công bố**, vẫn
có thể nhận dữ liệu đến muộn khi refresh. available_from/available_through chỉ
công bố khi có bằng chứng theo clock query; nếu không có cutoff thời gian đáng tin
thì trả null + offset coverage, không bịa một timestamp. Strict request cần phần
chưa verify trả503 HISTORY_UNAVAILABLE với reason PENDING_ARCHIVE/CATALOG_LAG/GAP;
chỉ trả200 empty khi đã xác minh query thực sự rỗng trong scope snapshot.

## 5. Durability và reconciliation

Giữ boundary hiện tại: raw object + raw manifest bền vững trước commit Kafka.
Catalog có thể publish trễ; lag đó phải được công bố riêng, không dùng Kafka ACK
làm ACK rằng History đã query được. Sau crash/rebalance, reconciliation phải đọc
lại durable manifests và khôi phục catalog entries; không phụ thuộc RAM hoặc chỉ
đọc các Kafka records chưa commit. Nếu không thể khôi phục đầy đủ, báo UNKNOWN/GAP.

Catalog generation chỉ pin data đã verify. Checkpoint/pointer mất ACK phải reconcile
theo IDs/versions; không publish COMPLETE giả hoặc tạo duplicate receipt. Full
rebuild catalog từ durable raw manifests phải có operator runbook và budget riêng;
không chạy toàn bộ rebuild trong mỗi browser request.

Processed outcomes không nằm trong contract này. Optional outcome wire archive của
Storage vẫn là capability SDK cũ, **không bật trong M6** và không thay DB history.

## 6. Schema, fixtures và quyền truy cập

Package Storage cung cấp schema/examples và helper đọc exact-version: một generation có timestamps trùng, duplicate event khác locator,
feature-invalid, rejected thiếu máy, catalog lag, gap và checksum mismatch. Fixture
phải ghi rõ dữ liệu giả; Processing/Business owner viết reader/facade/UI theo contract, không cần đợi AWS.

Môi trường local dùng MinIO đã có source cấu hình ở MaintAI-gitops/local/platform.
Storage owner xác minh bucket/versioning, endpoint từ Pod, credentials Secrets và policies;
source manifests không chứng minh MinIO hoặc Storage đã được deploy. Cần quyền:

| Identity | Quyền |
| --- | --- |
| Storage | Consume hai raw groups; PUT/GET raw+manifest+catalog theo allowlist; không SQL owner khác |
| Read API | GET/LIST raw/catalog; GET/PUT prefix private exports; không sửa raw/catalog |
| Browser | Business auth; không có S3 credentials hoặc quyền list bucket |

GitOps/infrastructure cần migration/grants metadata riêng cho Read API và Secrets/
IAM/policies tương ứng. Những cấu hình cũ chỉ có processing_ro phải được cập nhật
khi bật M6; không bật app với owner credential để bỏ qua bước phân quyền.

## 7. Điều kiện demo và nghiệm thu

- Chưa có AWS: dùng MinIO local; vẫn có upload raw và verify manifest/catalog.
- Chưa có object store/catalog: demo history outcomes từ DB và CSV nhỏ; raw routes
  báo unavailable. Không gọi đây là full M6 hoặc fake empty history.
- Chứng minh restart sau raw commit/trước catalog publish phục hồi được index.
- Chứng minh stale publisher/CAS retry không làm lùi coverage hoặc mất partition.
- Reader không duplicate/missing khi timestamp trùng, catalog lag hoặc pin generation.
- Object/version/checksum hỏng, scope sai, source retention loss trả lỗi rõ ràng.
- Nhóm chưa duyệt retention: không bật cleaner/lifecycle xóa raw/outcomes.

Storage0.2.1 có catalog publisher và schema đóng gói; các fixtures offline không thay bằng chứng
SQL grants hoặc platform rollout mới. Các mục nghiệm thu là việc phải thực hiện,
kết quả đã chạy được ghi riêng trong [báo cáo M6](M6_IMPLEMENTATION_REPORT.md).

## 8. Machine schemas và coverage response

Các định nghĩa provider fixture cũ trong [raw-catalog-m6.schema.json](../contracts/v1.3/schemas/raw-catalog-m6.schema.json) chỉ phục vụ compatibility/offline fixtures; PartitionCoverage được HTTP M6 sử dụng. Native provider được validate bằng schema đóng gói của Storage, không thay raw-manifest1.0.0. Pointer chứa generation_id/generation_ref; generation chứa parent_ref, segment refs và partitions. Schema provider trong package Storage là nguồn chuẩn cho pointer/generation/segment/entry. ObjectRef provider chỉ có bucket/key/version_id/sha256, không có size_bytes. Segment giữ entries với identity `topic:partition:offset`. Export ObjectRef trong HTTP DTO vẫn bắt buộc size_bytes; không dùng schema export để validate provider.

Raw pages/chart dùng [history-m6.schema.json](../contracts/v1.3/schemas/history-m6.schema.json): available_from/available_through được null khi không có bằng chứng theo clock nguồn; partition_coverage và catalog_generation giữ cutoff đã pin. Chỉ trả COMPLETE khi mọi partition cần cho scope query đã verify, không có gap trong phần query yêu cầu. Offset coverage không chứng minh completeness của mọi readings trước một thời gian nguồn; late receipts xuất hiện khi refresh.

Các fixtures trong [examples/m6](../contracts/v1.3/examples/m6/) là metadata giả để validate offline, không là objects đã upload hoặc bằng chứng Storage publisher đã chạy. Coverage frontier ordering, duplicate locator/partition và gaps được kiểm trong validator; exact object versions, checksum thực và CAS/reconciliation cần integration runtime.

Raw page/chart partition_coverage không lặp topic/partition; response membership
được giới hạn bởi generation/snapshot đã pin. Không suy thiếu receipts từ khoảng
cách số offset (control records có thể tạo offset trống). Fixtures generation
fixture giữ hai receipts cùng event/time ở offset0/1; raw page không phân trang
phải trả cả hai, giảm dần offset, chart samplecount phải khớp tập đó.
