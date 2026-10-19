# RAG/LLM — một Deployment, knowledge owner

Xem [Dockerfile](Dockerfile) và [runtime.json](runtime.json). Source hiện là
skeleton cho một service RAG online; các thư mục dưới đây là modules, không phải
mỗi thư mục một microservice:

- `api/`: interface nội bộ cho Business RAG client.
- `query-builder/`, `metadata-filter/`, `vector-retriever/`, `prompt-builder/`,
  `llm-generator/`, `citation-validator/`: grounded brief/checklist pipeline.
- `ingestion/parsing/`, `chunking/`, `embedding/`: offline knowledge preparation.
- `knowledge/`, `migrations/`: metadata/schema của `knowledge_db` (pgvector), S3 manuals.
- `evaluation/`, `tests/`: placeholder cho retrieval/grounding checks.

Không SQL JOIN sang business/processing DB. Copilot không chặn alert lifecycle,
không thay risk và không tự tạo maintenance alert. Knowledge ingestion chưa được
khai báo là một deployment/job khác trong skeleton này.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
