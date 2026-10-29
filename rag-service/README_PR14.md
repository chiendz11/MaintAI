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
