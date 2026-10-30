# RAG-GROQ-01 — Chuyển generation sang Groq API

Quyết định30/10/2026: RAG dùng Groq Chat Completions cho bước sinh brief/checklist.
Không còn dependency Ollama/Qwen local. Đây là thay provider bên trong RAG,
không đổi API request/response1.1.0, data ownership hoặc Alert lifecycle.

Luồng: Business/internal caller → RAG → retrieve chunks trong knowledge_db →
Groq HTTPS → validate JSON/citations → MaintenanceBriefResponse. Metadata
generator ghi `provider=groq` và model ID được cấu hình/đối chiếu với response.

Embedding MiniLM384 vẫn chạy local trong RAG và cần cache/knowledge ingestion.
Không gửi sensor trực tiếp cho Groq để thay ML inference, không tự đổi risk.
Lỗi RAG không chặn Alert. Business Copilot M7 vẫn cần integration riêng.

## Runtime và secrets

App cung cấp client, settings, validation, probes, tests và `.env.example`.
Cần DATABASE_URL, EMBEDDING_MODEL, GROQ_MODEL cùng một trong GROQ_API_KEY hoặc
GROQ_API_KEY_FILE. Model ví dụ là llama-3.3-70b-versatile, output budget2048,
timeout60s. Không có model fallback hoặc key mặc định thật.

GitOps/hạ tầng cần supply key qua Secret/file, cho Pod outbound HTTPS tới
api.groq.com, bỏ Ollama host routing và cấp writable embedding cache. Không đưa
key vào public runtime config/profile. Trước build/prepare phải cập nhật app
source locks sau khi review commit RAG mới; thay đổi này không tự deploy.

## Kiểm tra và lỗi

Liveness không gọi dependencies. Readiness dùng GET models có auth (cache30s/5s),
không tạo completion, không chứng minh quota hoặc grounded quality. JSON object
mode không thay thế schema/checklist/citation validation của app.

Lỗi provider trả HTTP503 RAG_UNAVAILABLE, kèm safe reason và retryable. Auth,
request/output không hợp lệ và truncated output không được tự retry. Rate limit,
timeout/connection/5xx được đánh dấu retryable nhưng client không tự lặp POST
unknown outcome. Không trả/log provider error body, API key hoặc retrieved prompt.

Tests dùng fake HTTP/DB/retrieval để kiểm wire và failure semantics. Chỉ được ghi
live Groq verified khi có credential thật và thực sự gọi provider; unit/CI smoke
không là bằng chứng đó. Đọc [RAG README](../rag-service/README.md) cho lệnh và handoff.
