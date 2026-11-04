# Storage Consumer0.2.0 — Demo component local với Kafka + MinIO

Harness này chạy riêng Storage, không thay hạ tầng toàn hệ thống k3d + Argo CD.
Không cần AWS S3. Docker Compose cung cấp broker Kafka, MinIO versioning, bootstrap
và một Storage container. Chỉ publish cổng loopback; credentials trong Compose là
fixtures local công khai, không dùng cho môi trường chung. Không có processed archive
hoặc cleanup source data. MinIO fixture build từ source có SHA pin như infrastructure
local; lần build đầu có thể lâu. Volume giữ dữ liệu xuyên restart, chưa là HA.

## 1. Bật component

Từ root MaintAI, Docker đã chạy:

```bash
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml up -d --build --wait --wait-timeout 180
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml logs --tail=50 storage
curl --fail http://127.0.0.1:18008/health/ready
curl --fail http://127.0.0.1:18008/metrics
```

Bootstrap chỉ tạo versioned bucket `maintai-storage-demo` và topics fixture.
MinIO API ở127.0.0.1:19000, console ở127.0.0.1:19001; không đưa các credentials này
vào GitOps production. Container Storage dùng `kafka:9092`, `minio:9000`, không
localhost. CLI trong container nhận cùng cấu hình như service.

## 2. Gửi một reading demo

```bash
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml exec -T storage python - <<'PY'
import json
from maintai_storage.settings import Settings
from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.producer import KafkaPublisher

settings = Settings.from_env()
contracts = ContractRegistry()
payload = json.loads((contracts.directory / 'examples/sensor-reading.json').read_text())
with KafkaPublisher(settings.kafka, 'ingestion') as publisher:
    delivery = publisher.publish('sensor.readings', payload)
    print({'topic': delivery.topic, 'partition': delivery.partition, 'offset': delivery.offset})
PY
```

Lệnh này dùng fixture Kafka, không kiểm chứng HTTP Ingestion/Sensor hoặc full pipeline.
Gửi lại cùng event tạo raw receipt khác nếu Kafka nhận một offset khác; catalog
giữ cả hai, không gộp theo event_id. Feature-invalid vẫn được giữ raw; Storage không DQ.

## 3. Kiểm tra catalog và phục hồi

```bash
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml exec storage maintai-storage catalog-inspect
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml restart storage
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml exec storage maintai-storage catalog-sync
```

Catalog opt-in mặc định đã bật trong harness, sync nền mỗi1 giây cộng thời gian audit.
Coverage COMPLETE nghĩa safe prefix đến cutoff offsets được công bố, không phải
mọi máy đã gửi đủ dữ liệu đến một giờ thực tế. Source-time available_through không
được suy từ timestamp mới nhất. Raw pending tạo LAGGING; retention/truncation tạo
UNKNOWN và sticky gap. Read API cần đọc freshness/dependency state và kiểm scope.

Nếu mất catalog nhưng raw còn, lần sync dựng lại entries trong Kafka retention từ
durable manifests. Khi có raw cũ hơn Kafka hoặc cần full reconciliation:

```bash
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml exec storage maintai-storage catalog-rebuild
```

Rebuild scan theo bounded pages, khôi phục entries từ exact manifests; không xóa
raw, không commit/reset group offsets, không xóa gap để tự nhận COMPLETE. Chạy
catalog-sync trước để thiết lập source identity; nếu bucket đang có source khác,
phải dùng namespace mới và migration có review. Không đổi prefix/group tùy tiện.
Nếu công cụ bị ngắt, chạy lại an toàn về receipt identity; có thể có cùng entry
trong nhiều delta generations, reader phải upsert/dedupe bằng receipt_id + checksum.

## 4. Dùng trong k3d hiện có

Không bật Compose này cùng cổng của cluster. Trong hai repo platform, cấu hình
Storage image/source mới và các env trong `.env.example`: catalog enabled, prefix,
MinIO endpoint nội bộ, raw bucket versioning và credentials Secret. Storage cần
GET/PUT raw+manifest+catalog, LIST cho operator rebuild và GetBucketVersioning;
Kafka cần READ/DESCRIBE hai topics. Audit dùng manual assignment/read_committed,
không subscribe/commit hoặc tạo consumer group nghiệp vụ mới.

Read API cần identity đọc raw/catalog và identity ghi export prefix riêng; policy
processing-reader cũ chỉ đọc processed bucket chưa đủ cho raw M6. Metadata DB grants
của Read API do Người1 thực hiện khi M6 có code. Lượt cập nhật app này không tự sửa
hoặc deploy hai repo platform; không dùng owner DB role để bỏ qua grants.

## 5. Kiểm thử giống CI và dừng component

Sau khi cài locked dependencies/local packages theo README Storage:

```bash
MAINTAI_STORAGE_INTEGRATION=1 \
STORAGE_TEST_KAFKA_BOOTSTRAP=127.0.0.1:19092 \
STORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:19000 \
STORAGE_TEST_S3_ACCESS_KEY=storage-local-fixture \
STORAGE_TEST_S3_SECRET_KEY=storage-local-fixture-password \
STORAGE_TEST_IMAGE=maintai-storage-catalog:local \
python -m pytest storage-consumer/tests/integration
docker compose -p maintai-storage-demo -f storage-consumer/local/compose.yaml down
```

Tests dùng unique fixture buckets/groups, xóa đúng bucket test ở finally; không xóa
bucket demo hoặc Kafka topics. `down` giữ volumes; không thêm `--volumes` nếu cần
giữ dữ liệu demo. CI tạo project/buckets tách biệt và chỉ xóa tài nguyên fixture.
