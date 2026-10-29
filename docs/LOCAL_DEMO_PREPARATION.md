# LOCAL-DEMO-01 — Chuẩn bị model identity và knowledge cho demo

Quyết định ngày30/10/2026. Áp dụng cho local k3d; không thay provenance bắt buộc
của production, wire outcome1.4.0 hoặc SQL đã freeze. Infrastructure local thuộc
Maint-infrastructure; desired state/cache PVC thuộc MaintAI-gitops.

## Model identity khi chưa có MLflow

Worker hiện chỉ cho profile local-fixture với loopback adapters. Tool
`processing.shared.inference_client.local_identity` hỗ trợ riêng bundle có
status `local_baseline_not_approved_release`. Nó kiểm pipeline/dataset/split
theo metadata đã bàn giao, allowlist/order sáu features và schema. Không load
pickle hoặc tạo một prediction giả để điền provenance.

`mlflow_run_id = not-recorded:local-demo` là marker công khai rằng **không có
MLflow run**, không phải run ID có thể tra cứu. Đây là quy ước local trong
string hiện có, không là permission dùng ở shared/production. Dataset manifest
được tái dựng từ delivery và ghi `approved_training_manifest=false`; không gọi
nó là original approved training manifest. Dataset version dùng namespace local.
Model quality/production approval đều false, không được biến thành PASS vì
identity hợp lệ. Bundle baseline hiện chưa đạt quality gate.

`model_artifact_sha256` và `preprocessor_sha256` cùng là hash pipeline.joblib,
vì fitted preprocessing và classifier nằm trong một sklearn Pipeline. Feature
schema và split được hash riêng. Release ID thay đổi khi image/source/model đổi.

Infrastructure phải lấy **RepoDigest OCI thật** của image ML đã build, đối chiếu
với Docker inspection và receipt rồi mới gọi helper trong image Worker cùng
app commit. Không ghép Docker config ID thành registry digest. Engine không có
RepoDigest cần export/publish OCI hợp lệ trước; tool phải từ chối thay vì đoán.
Identity/evidence chỉ lưu trong private local data, được checksum khi release.

## Knowledge preparation

App giữ `rag-service/embedding-lock.json` và `knowledge-demo.json`: model ID,
Hugging Face commit40 hex, vector384, PDF checksum và document/version/section.
Model hiện dùng MiniLM multilingual; generation dùng Groq, không cần Qwen.

Sau khi PostgreSQL/pgvector và cache PVC sẵn sàng, owner chạy image RAG:

```bash
python -m ingestion.prepare_demo
```

Job được cấp DATABASE_URL từ Secret knowledge owner, cache writable và /tmp;
không cần Groq key để seed. Nó kiểm manifest/PDF trước khi gọi DB, lấy advisory
lock chống hai preparation đồng thời, migrate schema, tải/load đúng embedding,
ingest PDF rồi chạy truy vấn vector và kiểm metadata citations. Chạy lại cùng
document/version/hash/revision giữ số chunks. Cùng ID/version nhưng PDF/model
khác hoặc thiếu provenance phải fail; operator chọn version mới hoặc review
migration, không tự xóa knowledge cũ.

Source PDF hash, embedding model/revision được lưu trên mỗi chunk. Sửa PDF phải
đổi version/manifest. Report nêu số chunks và IDs được retrieval; nó ghi rõ
`groq_generation_tested=false`, không phải kiểm chứng câu trả lời LLM.

Job và RAG Deployment dùng cùng PVC, model/revision/cache path. API chạy
MODEL_LOCAL_FILES_ONLY=true; không tự tải model trên request. Pod vẫn cần Secret
Groq riêng để startup/provider readiness. Chạy seed xong không chứng minh
Business Copilot M7 đã có hoặc toàn pipeline đã được nghiệm thu.

## Kiểm chứng

RAG unit/provider/manifest tests và image smoke được gọi từ MaintAI CI và nằm
trong `CI / required`. Fixture tests không gọi Groq hoặc tải model thật. Phải ghi
riêng evidence build image, real embedding/pgvector ingestion/repeat/retrieval,
actual Groq generation và E2E UI; không dùng một mức để thay mức còn lại.
