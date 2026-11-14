# LOCAL-DEMO-01 — Chuẩn bị model identity và knowledge cho demo

Quyết định ngày 30/10/2026 (cập nhật đồng bộ CLI vận hành ngày 14/11/2026). Áp dụng cho local k3d; không thay provenance bắt buộc của production, wire `outcome 1.4.0` hoặc SQL đã freeze. Infrastructure local thuộc `Maint-infrastructure`; desired state/cache PVC thuộc `MaintAI-gitops`; mã nguồn model/ingestion/helper thuộc `MaintAI`.

> **Tham chiếu vận hành:** Xem hướng dẫn chạy demo 1-click và vòng đời cụm `k3d` tại [LOCAL_DEMO_GUIDE.md](LOCAL_DEMO_GUIDE.md) và khung DevOps chuẩn tại [DEVOPS_FRAMEWORK_AND_OPERATIONS_GUIDE.md](DEVOPS_FRAMEWORK_AND_OPERATIONS_GUIDE.md).

## 1. Model identity khi chưa có MLflow

Worker hiện chỉ dùng local identity helper cho profile `local-baseline`. Tool `processing.shared.inference_client.local_identity` hỗ trợ riêng bundle có status `local_baseline_not_approved_release`. Nó kiểm pipeline/dataset/split theo metadata đã bàn giao, allowlist/order sáu features và schema; không load pickle hoặc tạo một prediction giả để điền provenance.

- `mlflow_run_id = not-recorded:local-demo` là marker công khai rằng **không có MLflow run**, không phải run ID có thể tra cứu. Đây là quy ước local, không là permission dùng ở shared/production.
- Dataset manifest được tái dựng từ delivery và ghi `approved_training_manifest=false`; không gọi nó là original approved training manifest.
- `model_artifact_sha256` và `preprocessor_sha256` cùng là hash `pipeline.joblib`, vì fitted preprocessing và classifier nằm trong một sklearn Pipeline. Feature schema và split được hash riêng. Release ID thay đổi khi image/source/model đổi.
- Model quality/production approval đều `false`, không được biến thành PASS vì identity hợp lệ. Bundle baseline hiện chưa đạt quality gate.

### Lệnh operator trên Infrastructure (xác minh OCI RepoDigest)

Sau khi build image ML và Worker từ cùng app commit đã khóa, chạy tại thư mục gốc `Maint-infrastructure`:

```bash
.venv/bin/python -m localdev model-identity
```

Tool đối chiếu image receipt với Docker store để lấy **RepoDigest OCI thật** của image ML (tuyệt đối không ghép Docker config ID thành registry digest; engine thiếu RepoDigest cần export/publish OCI hợp lệ trước), sau đó gọi helper trong image Worker để sinh private `local/.local/data/fixtures/model-identity.json` và evidence checksum.

## 2. Credential Groq & Knowledge preparation

### 2.1. Cấp Secret `rag-groq`
Operator lưu Groq API key mới tại `Maint-infrastructure/local/.local/secrets/groq-api-key` (mode `0600`, directory `0700`) — không commit vào Git hoặc lấy key từ chat/log:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev groq-secret
```

Deployment `rag-service` nhận `GROQ_API_KEY` qua `secretKeyRef` tới `rag-groq`; CI chỉ dùng fixtures, key bị từ chối không fallback Ollama và lỗi RAG không chặn Sensor/ML/Alert pipeline.

### 2.2. Chuẩn bị vector knowledge (`pgvector` + cache PVC)
App giữ `rag-service/embedding-lock.json` và `knowledge-demo.json`: model ID (`MiniLM multilingual`), Hugging Face commit 40 hex, vector 384 chiều, PDF checksum và document/version/section. Generation dùng Groq API.

Sau khi PostgreSQL/pgvector và PVC `local-rag-embedding-cache` sẵn sàng, operator chạy qua `Maint-infrastructure` CLI (gọi entrypoint `python -m ingestion.prepare_demo` bên trong RAG image):

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev prepare-rag
```

Hoặc chạy trực tiếp bên trong container có sẵn env/mount:
```bash
python -m ingestion.prepare_demo
```

**Cơ chế & ràng buộc an toàn:**
- Job được cấp `DATABASE_URL` từ Secret `knowledge` owner, cache PVC writable và `/tmp`; **không cần Groq key** để seed.
- Nó kiểm manifest/PDF trước khi gọi DB, lấy advisory lock chống hai preparation chạy đồng thời, migrate schema, tải/load đúng revision MiniLM, ingest PDF rồi chạy truy vấn vector và kiểm metadata citations.
- Chạy lại cùng document/version/hash/revision giữ nguyên số chunks (`inserted=0`, idempotent). Cùng ID/version nhưng PDF/model khác hoặc thiếu provenance phải fail — operator chọn version mới hoặc review migration, không tự xóa knowledge cũ.
- Job và RAG Deployment dùng chung cache PVC, model/revision/cache path. API chạy `MODEL_LOCAL_FILES_ONLY=true` với root filesystem read-only (chỉ cache và `/tmp` ghi được). PVC tồn tại độc lập với Deployment (`prune=false`).
- Receipt private `rag-knowledge-receipt.json` lưu kết quả chạy thật trên cluster đích; report nêu số chunks và IDs retrieval và ghi rõ `groq_generation_tested=false`.

## 3. Kiểm chứng

- **App CI (`CI / required`):** RAG unit/provider/manifest tests và image smoke; fixture tests không gọi Groq hoặc tải model thật.
- **Container verification thực (ngày 30/10/2026):** Đã chạy MiniLM 384 chiều với PostgreSQL 17/pgvector 0.8 dưới `UID 10001` và root filesystem read-only: PDF 46 trang tạo **106 chunks** (lần đầu `inserted=106`, lần hai `inserted=0` và `stored=106`; retrieval trả về chunks `96/35/23`). Sửa lỗi đóng gói Dockerfile PDF checksum qua app PR #21.
- **Lưu ý phân tách bằng chứng:** Cần ghi riêng evidence build image, real embedding/pgvector ingestion/retrieval trên cluster đích, actual Groq generation và E2E UI; không lấy kết quả container test làm receipt của cluster hoặc bằng chứng hoàn tất Business Copilot M7.

## Serial consumer prefetch

SDK `0.3.1` sửa đường đọc mặc định của Business/Storage: một handler chạy tại một thời điểm, tối đa 64 records đã lấy từ Kafka gồm cả record đang xử lý. Consumer chỉ pause khi đạt giới hạn và resume khi còn tối đa 32 records. Không pause toàn bộ partition sau từng reading; cách cũ gây backlog và phục vụ các máy không đều trong demo liên tục. Đây là bounded prefetch, không thêm handler song song hoặc đổi quy tắc ordering/offset. Poll, assignment và commit vẫn thuộc poll thread.

Record chỉ được commit sau `DurableReceipt` đúng identity và assignment generation còn hợp lệ. Shutdown/handler failure/commit failure không commit các record chỉ mới nằm trong buffer. Khi restart, chúng được Kafka gửi lại và inbox/archive chịu trách nhiệm idempotency. Bundle `2.0.0`, wire `outcome 1.4.0` và SQL `001/002` giữ nguyên; các package consumer và CI wheel paths được pin đồng bộ SDK `0.3.1`.

Local chọn `KAFKA_POLL_INTERVAL_SECONDS=0.01` cho Business/Storage sau khi kiểm luồng thực. Thay đổi này không là benchmark 100 máy hay bằng chứng production HA. Bằng chứng triển khai, ảnh UI và kết quả kịch bản được ghi riêng trong `Maint-infrastructure/docs/LOCAL_DEMO_RUN_2026-10-30.md`; không xem unit tests hoặc image build là pipeline đã chạy.
