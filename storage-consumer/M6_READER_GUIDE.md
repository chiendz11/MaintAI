# M6 Reader Guide — Handoff từ Chiến/Người4

Storage0.2.0 cung cấp protocol M6-RAW-01/1.0.0. Reader HTTP/SQL/UI thuộc Người1;
module dưới đây là helper đã đóng gói, không là một History API hay deployment mới.

## 1. Contracts và package

- [Schema](src/maintai_storage/schemas/catalog.json): ObjectRef, Entry, Coverage,
  Segment, Generation, Pointer; local refs tự chứa, additionalProperties=false.
- [Examples](src/maintai_storage/examples/catalog.json): fixtures giả cho timestamps
  trùng, duplicate source event khác locator và rejected thiếu machine/time.
- [Contract hash manifest](src/maintai_storage/schemas/manifest.json): schema/examples
  được kiểm bằng `maintai-storage validate-catalog`, cả ngoài checkout từ wheel.
- Kafka raw-manifest1.0.0/SDK0.3.0 không đổi. Cài wheels messaging0.3.0 và
  storage-consumer0.2.0 cùng locked dependencies; không copy riêng mã boto client.

## 2. Đọc generation và sync index của Processing

```python
from maintai_storage.catalog import CatalogStore, CatalogReader

# s3_reader: boto3 client đã có endpoint/Secret và quyền GET raw/catalog.
store = CatalogStore(s3_reader, raw_bucket, prefix="raw-catalog/v1")
current, etag = store.current()
if current is None:
    # API M6 chuyển thành HISTORY_UNAVAILABLE; không coi là collection rỗng.
    raise RuntimeError("Catalog chưa được publish")
pointer, current_generation = current
reader = CatalogReader(store)
for generation in reader.generations(pointer, max_generations=100):
    for entry in reader.entries(generation):
        upsert_catalog_metadata(entry)  # M6 implement; receipt_id + exact raw checksum.
```

Generations là **delta + parent_ref**, không phải toàn bộ history trong một file.
Không lấy riêng entries của current generation rồi gọi đó là full collection.
Sync M6 dùng metadata pool/transaction của chính Processing; chỉ advance checkpoint
sau các entries + source generation/cutoff đã được lưu bền vững. Duplicate cùng
receipt_id nhưng khác raw checksum là conflict, không last-write-wins.

Helper yield newest-first. Có checkpoint cũ thì dùng `stop_id`; không tìm thấy
ancestor sẽ lỗi. Hơn100 generations thì helper raise budget error, không xác nhận
chain đã complete. Sync lớn cần lưu backlog refs và resume từng window: tạo pointer
cho parent_ref tiếp theo (đọc generation_id từ exact parent doc), không scan bucket
trong browser request. Không đánh dấu generation fully indexed trước khi xử lý hết
chuỗi về checkpoint/root. Sau đó query DB index theo machine_id + received_at +
typed topic/partition/offset và tạo snapshot theo M6-HISTORY-01.

## 3. Coverage và raw detail

`current_generation.partitions` là full coverage state của các partitions đã
discovery; runtime đưa partitions mới vào UNKNOWN trước audit. Source Kafka
cluster_id/topic_id dùng để phát hiện topic recreate hoặc đổi cluster. Frontier
exclusive, không cộng số offset khác partition; actual broker records/EOF được
audit để tránh nhầm control-offset gaps thành reading bị thiếu.

COMPLETE áp dụng start_offset → indexed_next_offset của snapshot, không là event-time
watermark toàn hệ thống. Không suy available_through từ max(received_at). Dữ liệu
đến muộn có thể xuất hiện trong generation sau. LAGGING/UNKNOWN hoặc thiếu partition
scope cần query: strict History không được trả empty200 giả đầy đủ. Catalog cache
có thời gian sync/freshness riêng; raw corruption có thể bị phát hiện khi đọc detail.

Sau khi authorize machine/source và lookup entry trong snapshot index:

```python
compressed_frame = reader.raw(entry)
# bytes gzip: exact frame của SDK, chứa value_base64/key_base64/headers và locator.
# M6 decode bounded bytes, render raw safely; không gắn DQ VALID từ archive.
```

Reader kiểm bucket/prefix, exact versions, SHA-256 và manifest/locator; không cho
browser cung cấp tùy ý raw ObjectRef. Runtime không cung cấp browser auth, SQL
metadata schema, chart, cursor pagination hoặc CSV export; đó là phần Người1 M6.

## 4. Bắt đầu ngay khi chưa có AWS

Người1 fetch/pull main sau PR bàn giao; dùng schema/examples để làm fixtures trước.
Khi tích hợp thật, chạy [local harness](local/README.md) hoặc MinIO k3d do Chiến cấu
hình. History outcomes DB và CSV nhỏ không cần object store; full raw/catalog phải
upload vào MinIO local. Xem [M6 architecture](../docs/M6_HISTORY_ARCHITECTURE_DECISION.md)
và [handoff contract](../docs/M6_RAW_ARCHIVE_HANDOFF.md).
