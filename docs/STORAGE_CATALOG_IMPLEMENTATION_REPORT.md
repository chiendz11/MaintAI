# Storage Consumer0.2.0 — Bàn giao catalog M6 và demo local

Ngày29/10/2026. Phạm vi: code Storage + CI + docs/metadata hiện có; không implement
Processing Read API/UI/SQL M6, không deploy k3d/AWS và không bật processed archive
hoặc source cleanup. Nhánh triển khai: `feat/storage-catalog-local-demo`.

## 1. Thay đổi và lý do

| File/nhóm | Chức năng |
| --- | --- |
| storage-consumer/src/maintai_storage/catalog.py | Bounded S3 reads, exact version/SHA refs, raw frame metadata, immutable delta segments/generations, CAS pointer, helper reader |
| catalog_runtime.py | Audit Kafka bằng manual assignment/read_committed, không commit; proof actual record/EOF, safe prefix, sticky retention/truncation gaps, source cluster/topic identity, S3 rebuild |
| settings.py/runtime.py/cli.py | Catalog opt-in trong cùng Storage runtime; quản lý lifecycle, CLI validate/sync/rebuild/inspect; không tạo service/DB mới |
| state/health hiện có + metrics.py | Dependency catalog riêng, sync result/entry attempts và bounded topic/status partition counts; không dùng machine/event labels |
| schemas/catalog.json + schemas/manifest.json | Protocol1.0.0 tự chứa và hash schema/examples; không sửa frozen Kafka wire/SQL |
| examples/catalog.json | Fixtures có duplicate source event/timestamp và rejected thiếu machine/time; không là data production |
| tests/test_catalog.py | Failure/recovery/CAS/retention/identity/version/scope/config và offline contract tests |
| tests/integration/test_catalog_minio.py | Opt-in Kafka+MinIO thật, raw commit trước catalog, restart recovery, duplicate receipts, exact refs, rebuild và packaged container contracts |
| local/compose.yaml/bootstrap.py/Minio.Dockerfile | Harness isolated component: Kafka pinned image, source-pinned MinIO, versioned bucket và non-root Storage image |
| M6_READER_GUIDE.md + local/README.md | Cách Người1 sync metadata/checkpoint/coverage, dùng fixtures và chạy component trước k3d integration |
| .github/workflows/ci.yml | Storage Python3.11/3.12, installed wheel contract validation và required Kafka+MinIO recovery job; logs/JUnit khi lỗi |

Package tăng0.1.0 →0.2.0; raw-manifest1.0.0, SDK0.3.0 và Kafka bundle2.0.0 giữ nguyên.
Raw commit vẫn đợi object+manifest, không đợi catalog. Catalog lỗi làm dependency
catalog unavailable; archive tiếp tục độc lập. Generation/coverage không bịa
complete từ Kafka commit hoặc max timestamp. Catalog audit không subscribe/commit
vào các group nghiệp vụ; không tạo command topic mới.

## 2. Lệnh kiểm tra và giới hạn bằng chứng

Local Python3.12 dùng venv riêng, cài storage-consumer/requirements-dev.lock và
local wheels/editable packages. Local đã PASS: **76 tests,2 integration tests skip**
vì Docker chưa bật; Ruff lint/format; build sdist/wheel; offline contract CLI;
installed wheels ngoài checkout; immutable Kafka bundle; docs/diagram hashes và
Compose config. Các lệnh verification:

```bash
ruff check storage-consumer
ruff format --check storage-consumer
python -m pytest storage-consumer/tests
maintai-storage validate-catalog
python -m build --no-isolation storage-consumer
python .github/scripts/check_repository.py
python docs/diagrams/render.py --check
python messaging/scripts/lock_contracts.py --check
docker compose -p maintai-storage-catalog-ci -f storage-consumer/local/compose.yaml config --quiet
```

Docker laptop đang tắt; không có local Kafka/MinIO/container/K8s execution evidence
trong lúc viết source. Unit/contract tests không thay kiểm thử provider. Job required
`Storage catalog Kafka and MinIO recovery` trên GitHub Actions dựng dependencies
và chạy opt-in integration; dùng kết quả đúng PR head làm bằng chứng trước merge.
Trạng thái/kết quả thực tế của PR được báo ở phần bàn giao trong cuộc trao đổi;
không ghi số run hoặc commit giả trước khi GitHub cấp định danh.

## 3. Bắt đầu công việc M6 sau khi merge

Người1 fetch origin/main và merge vào nhánh M6 của mình, đọc
[M6 Reader Guide](../storage-consumer/M6_READER_GUIDE.md),
[M6-HISTORY-01](M6_HISTORY_ARCHITECTURE_DECISION.md) và
[M6-RAW-01](M6_RAW_ARCHIVE_HANDOFF.md). Có schema/examples và helper library để
làm catalog sync/index phía Processing; fixtures có thể dùng ngay khi chưa có AWS.

Người1 vẫn phải làm owner migration/grants history_meta, snapshot/history/chart/
exports APIs, Business facade/UI và authorization. Không lấy current delta làm full
collection; cần sync hết parent chain về checkpoint/root, dedupe receipt/checksum
và chỉ advance durable index checkpoint sau commit.

## 4. Những việc không được suy là đã hoàn tất

- Full-system demo k3d + Argo: platform còn cần source/image receipts mới, catalog
  env/Secret/policies, Read API raw access và metadata grants khi M6 được bật.
- Catalogue dài: generations là append-only delta chain; SDK reader có budget,
  Read API phải persist resumable sync. Chưa compact chain hoặc benchmark100 readings/s.
- Retention/truncation loss giữ UNKNOWN/gaps, không tự certify lại completeness.
  Rebuild scan raw manifests theo bounded pages, có thể cần thời gian trên bucket lớn.
- Catalog fields không xác nhận DQ VALID; rejected/poison không có máy/time vẫn
  được giữ receipt. Raw time không phải global event-time completeness watermark.
- Không tự promote model, train, tạo Alert hoặc chuyển raw vào business_db.
- Không đổi quyền production, tự tạo bucket trong runtime serve, xóa raw/source
  data hoặc coi Compose fixture credentials là production identity.

Lượt bàn giao cũng giữ các chỉnh sửa PDF/nhóm hiện có và đồng bộ docs M6. Những
file đó không làm thay application runtime ngoài Storage; không claim pipeline ML/
RAG đầy đủ chỉ vì feature branch đã merge.


## 5. Storage0.2.1 — Post-merge regression và bounded Kafka audit progress

CI PR17 push/pull_request pass; run110 trên squash commit phát hiện catalog restart
không tiến trong120s. Unit regression tái hiện reader bị reassign trước khi delayed
fetch trả về. Bản sửa giữ assignment/buffer bounded từng active partition, chia sẻ
watermark probes theo topic, không poll partitions rỗng và giữ pending receipts khi
raw manifest chưa có. Tối đa128 active fetch clients, mỗi key tối đa batch limit100;
không thêm group subscription/offset commit hoặc đổi raw durability.

Thêm failure diagnostics (coverage/cached health/error classes, không raw/config
secrets) cho integration. Local sau sửa:78 tests PASS,2 provider tests skip do Docker
tắt. CI vẫn bắt buộc Kafka+MinIO thật; không tăng timeout hoặc bỏ required job để
né regression. Xem PR sửa tiếp theo và CI đúng commit khi bàn giao main cuối cùng.
