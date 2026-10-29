# MaintAI RAG — Groq API generation

RAG tìm bằng chứng trong PostgreSQL/pgvector bằng embedding MiniLM, gọi **Groq
Chat Completions** để sinh JSON, rồi kiểm citations trước khi trả brief. Không
cần chạy Ollama hoặc tải Qwen cho generation nữa. Embedding vẫn chạy trong RAG;
chuyển LLM provider không thay model/vector384 hoặc yêu cầu ingest tài liệu.

RAG vẫn là một service/deployment sở hữu `knowledge_db`; các folder retrieval,
prompt, generation và citation là modules nội bộ. Không SQL JOIN sang DB của
Business/Processing, không thay risk và không tự tạo maintenance Alert. Knowledge
ingestion là bước operator riêng, không tự trở thành một service mới.

## Cấu hình

Copy `.env.example` thành `.env` ngoài Git. Cần:

| Biến | Ý nghĩa |
| --- | --- |
| `DATABASE_URL` | Secret kết nối knowledge_db/pgvector |
| `GROQ_API_KEY` **hoặc** `GROQ_API_KEY_FILE` | Một credential source; file có thể mount từ Kubernetes Secret |
| `GROQ_MODEL` | Model ID rõ ràng; ví dụ hiện tại `llama-3.3-70b-versatile` |
| `GROQ_TIMEOUT_SECONDS` | Timeout HTTP, mặc định60s |
| `GROQ_MAX_COMPLETION_TOKENS` | Output budget, mặc định2048; model cụ thể có thể có giới hạn riêng |
| `EMBEDDING_MODEL` | `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` |
| `SENTENCE_TRANSFORMERS_HOME` / `HF_HOME` | Cache embedding ghi được, mặc định image `/opt/maintai/model-cache` |
| `MODEL_LOCAL_FILES_ONLY` | true khi model embedding đã chuẩn bị trong cùng cache |

Không cấu hình đồng thời key env và key file. Không ghi key vào image, profile
public, runtime.json hoặc logs. Đổi credential/model cần restart process để thay
cached generator. Các biến `OLLAMA_*` cũ không được dùng và không có fallback.

Client dùng HTTPS endpoint cố định `https://api.groq.com/openai/v1/chat/completions`,
Bearer auth, messages và `response_format={"type":"json_object"}`. JSON mode
không bảo đảm đúng schema/citations; local validation vẫn là bắt buộc. Model ID
trả về phải khớp cấu hình để metadata không ghi sai provider/model.

## Chạy local

Python3.11 theo Dockerfile, dependencies từ `requirements-runtime.lock` và test
dependencies từ `requirements-test.txt`. Lock hiện chỉ pin direct dependencies,
chưa là transitive/hash lock. Sau khi chuẩn bị `.env`:

```bash
python -m migrations.cli up
MODEL_LOCAL_FILES_ONLY=false python -m ingestion.prepare_embedding
python -m ingestion.cli --pdf documents/pdffile.pdf --document-id abb-motor --version 1 --section maintenance
uvicorn main:app --host 0.0.0.0 --port 8002
```

Các CLI ingestion không tự load `.env`; export cấu hình vào môi trường hoặc chạy
qua operator có env/Secret trước khi gọi. API entrypoint load `.env` cho local
development. Trong k3d, cache phải là volume writable dùng chung giữa bước chuẩn
bị/ingestion và RAG Pod; root filesystem read-only không thay thế cache mount.

Groq là external API nên Pod cần outbound HTTPS/DNS tới `api.groq.com`.
Không cần route `host.k3d.internal:11434` hoặc Ollama Pod. Runtime credentials
được cung cấp từ Secret; phần cấp Secret và cache volume thuộc GitOps/hạ tầng.

## Failure semantics và probes

- `/health/live`: chỉ kiểm process; không gọi DB/Groq hoặc tải model.
- `/health/ready`: kiểm DB/table, cache và `GET /models` có auth để xác nhận model
  khả dụng. Cache kết quả provider30s khi tốt/5s khi lỗi. Không sinh completion
  để probe; readiness không chứng minh quota hoặc chất lượng câu trả lời.
- Không có bằng chứng: không gọi LLM, trả `INSUFFICIENT_EVIDENCE`.
- Sai citations: `VALIDATION_FAILED`, không trả checklist không có căn cứ.
- Groq401/403 hoặc request/output không hợp lệ: HTTP503 `RAG_UNAVAILABLE`,
  `retryable=false`; không phản chiếu provider error body hoặc credential.
- Rate limit429, timeout, lỗi kết nối/5xx: HTTP503 `RAG_UNAVAILABLE`,
  `retryable=true`. Client không tự retry POST generation bị unknown outcome.
- JSON bị cắt do token budget hoặc model response không khớp bị từ chối.

RAG lỗi vẫn nằm ngoài critical Alert path. Business Copilot M7 và full Worker
model identity là các phần độc lập, không tự hoàn tất khi đổi provider.

## Tests và deploy handoff

`python -m pytest -q tests` dùng httpx MockTransport và retrieval fixtures, không
gọi Groq thật, không tải embedding weights hoặc cần API key thật. CI build image
và smoke liveness dùng credential fixture; không coi đó là live provider evidence.

Trước khi build/prepare từ hai platform repos, review provider metadata/env mới,
thêm Groq key vào scoped Secret, giữ embedding cache writable và cập nhật source
locks tới commit app này. Các locks nhánh policy cũ không tự chấp nhận source mới.
Không đặt key vào component env trong public profile. Không bật workload chỉ để
bỏ qua source-lock/runtime-review gates.

Tham khảo: [Groq Chat API](https://console.groq.com/docs/api-reference),
[JSON mode](https://console.groq.com/docs/structured-outputs),
[models](https://console.groq.com/docs/models).
README_PR14.md là tài liệu lịch sử của deployment Ollama cũ.

## Chuẩn bị demo có version

Embedding revision được khóa trong `embedding-lock.json`; `knowledge-demo.json`
khóa PDF/version/checksum. Job owner chạy `python -m ingestion.prepare_demo`
để migrate, cache embedding, ingest và kiểm retrieval; chạy lại không thêm
chunks trùng. Job không gọi Groq. Chi tiết tại
[LOCAL-DEMO-01](../docs/LOCAL_DEMO_PREPARATION.md). Image cài PyTorch CPU trước
requirements runtime để không tải CUDA không dùng trong demo laptop.
