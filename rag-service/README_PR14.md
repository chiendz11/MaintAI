# PR #14 — RAG deployment (k3d + Ollama on laptop)

Build context **repository root**: `docker build -f rag-service/Dockerfile -t maintai-rag:pr14 .`

Runtime requires DATABASE_URL, OLLAMA_BASE_URL, OLLAMA_MODEL, EMBEDDING_MODEL; copy `.env.example` and supply real credentials using Kubernetes Secret (never commit `.env`). Ollama must bind to a reachable host interface; `localhost` inside k3d is **not** the laptop. Confirm connectivity from a pod. Protect Ollama access (trusted network only).

Commands (from `rag-service/` with Python dependencies installed):

- Initialize existing DB: `python -m migrations.cli up`
- Prepare/cache embedding: `python -m ingestion.prepare_embedding`
- Ingest PDF: `python -m ingestion.cli --pdf documents/pdffile.pdf --document-id abb-motor --version 1 --section maintenance`
- Run API: `uvicorn main:app --host 0.0.0.0 --port 8002`
- Check live: `curl http://localhost:8002/health/live`
- Check ready: `curl -i http://localhost:8002/health/ready`

In k3d, mount a persistent writable volume at `/opt/maintai/model-cache`; run `MODEL_LOCAL_FILES_ONLY=false python -m ingestion.prepare_embedding` **in that same volume** before starting with `MODEL_LOCAL_FILES_ONLY=true`. PDF ingestion must use a PDF accessible to the container (mount or copy it). Run migrations against existing `knowledge_db`, not only at PostgreSQL container initialization.

This patch does **not** include a validated k3d manifest, a verified lockfile, or completed GitHub required-check configuration. Those require access to the actual repository and build environment. See README for implementation details; do not mark production/deployment verified before smoke testing.
# PR #14 - Triển khai RAG Service trên k3d

## 1. Kiến trúc

- RAG Service: FastAPI, chạy trong Kubernetes (k3d), cổng 8002.
- Cơ sở dữ liệu: PostgreSQL 16 và pgvector.
- LLM: Ollama chạy trên laptop, sử dụng Qwen 2.5 3B.
- Embedding: paraphrase-multilingual-MiniLM-L12-v2, vector 384 chiều.
- RAG kết nối Ollama qua http://host.k3d.internal:11434.

## 2. Cấu hình môi trường

Các biến bắt buộc:

- DATABASE_URL: Chuỗi kết nối PostgreSQL.
- OLLAMA_BASE_URL: Địa chỉ API Ollama.
- OLLAMA_MODEL: Tên mô hình Qwen.
- EMBEDDING_MODEL: Tên mô hình embedding.

Các biến bổ sung:

- OLLAMA_TIMEOUT: Thời gian chờ, mặc định 120 giây.
- HF_HOME: Thư mục cache Hugging Face.
- SENTENCE_TRANSFORMERS_HOME: Thư mục cache embedding.
- MODEL_LOCAL_FILES_ONLY: Chỉ sử dụng model đã tải khi đặt true.

Tham khảo .env.example để cấu hình.
Không commit file .env hoặc thông tin đăng nhập.

## 3. Build Docker

Chạy từ thư mục gốc của repository:

    docker build -f rag-service/Dockerfile -t maintai-rag:pr14 .

## 4. Khởi tạo database

Chạy từ thư mục rag-service:

    python -m migrations.cli up

Migration tạo extension pgvector, bảng knowledge_chunks và các index.
Có thể chạy lại nếu dữ liệu hiện có không vi phạm ràng buộc unique.

## 5. Chuẩn bị embedding model

    python -m ingestion.prepare_embedding

Kubernetes sử dụng initContainer để chuẩn bị model trước khi chạy API.
Cache được chia sẻ giữa initContainer và container chính.

Hiện tại cache dùng emptyDir, nên Pod mới sẽ phải chuẩn bị lại model.

## 6. Nạp tài liệu PDF

    python -m ingestion.cli --pdf documents/abb_motor_en.pdf --document-id abb-motor --version 1.0 --section maintenance

Metadata của chunk bao gồm:
document_id, document_version, section, page, chunk_index.

CLI báo số chunk mới được lưu và tổng số chunk của tài liệu.

## 7. Chạy API trực tiếp

    uvicorn main:app --host 0.0.0.0 --port 8002

Các endpoint:

- GET /health/live: Kiểm tra tiến trình API.
- GET /health/ready: Kiểm tra DB, vector store, Ollama và embedding.
- POST /api/v1/briefs: Sinh bản hướng dẫn bảo trì.

## 8. Triển khai trên k3d

Import Docker image vào cluster:

    k3d image import maintai-rag:pr14 -c maintai

Tạo Secret từ file cấu hình cục bộ:

    kubectl create secret generic maintai-rag-env --from-env-file=.env.docker

Áp dụng manifest:

    kubectl apply -f k8s/deployment.yaml
    kubectl apply -f k8s/service.yaml

Lưu ý:
- Secret phải chứa DATABASE_URL truy cập được từ Pod.
- OLLAMA_BASE_URL phải trỏ tới Ollama trên laptop.
- PostgreSQL phải truy cập được từ mạng k3d.
- Không commit .env.docker.

Nếu PostgreSQL chạy trong container maintai-postgres và chưa kết nối mạng k3d:

    docker network connect k3d-maintai maintai-postgres

Truy cập API từ laptop:

    kubectl port-forward deployment/maintai-rag 8003:8002

API có địa chỉ http://localhost:8003.

## 9. Kiểm thử và CI

Chạy kiểm thử từ rag-service:

    python -m pytest -q tests

Workflow CI: .github/workflows/rag-ci.yml.

Hai job:
- unit-and-http: Unit test và HTTP contract test.
- image-build: Build Docker image và smoke test liveness.

Required checks cần được cấu hình riêng trong GitHub Rulesets hoặc Branch Protection.

## 10. Kết quả đã kiểm chứng trước các thay đổi mới nhất

- Docker image đã build thành công.
- RAG Pod trên k3d đạt trạng thái 1/1 Running.
- Kubernetes Service truy cập được trong cluster.
- Readiness từng xác nhận kết nối DB và Ollama.
- API /api/v1/briefs từng trả về GROUNDED cùng citations.

Các thay đổi mới nhất về migration, ingestion và CI chưa được kiểm thử tổng thể.
