# MaintAI Sensor Simulator 0.3.0

Sensor backend là **một runtime** gồm scheduler, durable spool, HTTP sender,
Sensor Control API và metrics. Simulator frontend ở `../sensor-simulator-UI/`,
image/release riêng. Sensor không chạy inference, không tạo/resolve Alert và
không chứa MLflow Server/client.

Triển khai theo [System Contracts v1.4](../docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md)
§31–34/40–41, [UI-01](../docs/UI_ARCHITECTURE.md) và
[OBS-01](../docs/OBSERVABILITY_IMPLEMENTATION.md). HTTP description baseline1.3.2,
reading wire1.3.0; package0.3.0 là version độc lập. Không đổi frozen OpenAPI/schema.

## Cài đặt

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r sensor/requirements-dev.lock
.venv/bin/python -m pip install --no-deps -e ./sensor
.venv/bin/maintai-sensor download-data
```

UCI AI4I được kiểm SHA256 khi download, CSV đầy đủ nằm ngoài Git. Source/cursor
được pin bằng dataset/config hash trong journal. Runtime đọc sáu feature từ CSV,
không dùng IDs/timestamps/failure labels làm ML features. Mỗi dòng gán vào một
máy giả lập; AI4I không phải time series bảo trì thật của nhiều máy.

## Chạy local với frontend

Từ root repo, chỉ bật anonymous một cách explicit trên loopback:

```bash
SIM_AUTH_MODE=local SIM_INGESTION_AUTH=none \
BACKEND_BASE_URL=http://127.0.0.1:8000 \
SIM_CORS_ORIGINS='["http://127.0.0.1:5174"]' \
.venv/bin/maintai-sensor serve --dataset sensor/data/ai4i2020.csv
```

Control ở `http://127.0.0.1:8005`. Ingestion wrapper hiện vẫn là module khác/chưa
có runtime hoàn chỉnh; không có Ingestion thì readings chờ trong spool, không
biến thành accepted. Đổi frontend runtime-config sang live, API URL này và
`auth.mode=none` theo [hướng dẫn UI](../sensor-simulator-UI/README.md).

Default5 máy, interval1000ms/máy, speed1 → **mục tiêu5 readings/s**. Tăng speed
hoặc số máy qua cấu hình runtime, chưa có HTTP rate mutation. Mạng/CPU/disk/quota
có thể làm actual rate thấp hơn; scheduler không catch-up burst. Mỗi máy có
state/cursor/sequence riêng. Đóng browser không dừng Sensor.

## Shared deployment và auth

Xem `.env.example`; file `.env` không tự load. Mặc định auth fail-closed:

- `SIM_AUTH_MODE=jwt`; issuer/JWKS HTTPS được cấu hình trước, algorithms chỉ RS256/ES256.
- Kiểm signature, issuer, audience (`simulator-control`), exp/iat/sub, scope
  `simulator:control`, role SIMULATOR_OPERATOR hoặc ADMIN, và machine scope.
- Claims mặc định: `roles` là array, `machine_ids` là array. `"*"` cấp quyền mọi
  máy một cách explicit; ADMIN cũng phải có scope/machine permission. Đổi claim
  path bằng `SIM_ROLES_CLAIM=realm_access.roles` hoặc `SIM_MACHINES_CLAIM=...`.
- JWKS cache300s/timeout5s; lỗi dependency trả503, token sai401, thiếu quyền403.
  List chỉ trả máy trong scope. Không tin X-Forwarded-For để mở local anonymous.
- Browser dùng OIDC Code+PKCE ở frontend; Sensor không cấp token/login session.
- Sensor → Ingestion dùng service credential riêng: `SIM_INGESTION_AUTH=token_file`
  + `SIM_INGESTION_TOKEN_FILE`, hoặc `oauth` với token URL/client ID/secret file
  và client-credentials scope. Secret/token không ghi vào DB payload hoặc logs.
- OAuth token cache theo expiry; lỗi credential/401/403 giữ spool ở `auth`, chặn
  gửi tiếp máy đó để tránh spam. Sửa credential rồi dùng maintenance command để retry.
- Anonymous Ingestion chỉ được cấu hình khi cả runtime và Ingestion ở loopback.
- CORS chỉ origins cụ thể, không wildcard. Management routing/TLS/NetworkPolicy,
  IdP provisioning và production credentials thuộc GitOps/hạ tầng, không nằm ở đây.

Các test TLS JWKS/OAuth dùng IdP fixture có chữ ký RSA thật; chưa xác minh với
IdP của môi trường thật hoặc browser code exchange qua IdP đó.

## Control API và semantics

| Operation | Request |
| --- | --- |
| List scoped machines | GET `/api/v1/simulator/machines` |
| Read persisted state | GET `/api/v1/simulator/machines/{machine_id}/control` |
| Pause | POST `/api/v1/simulator/machines/{machine_id}/pause` |
| Resume | POST `/api/v1/simulator/machines/{machine_id}/resume` |
| Scenario | PUT `/api/v1/simulator/machines/{machine_id}/scenario` |

Mutation cần `Idempotency-Key` và JSON `expected_control_version`; scenario thêm
`scenario`. State/version + command receipt + audit được commit cùng transaction
**trước200**. Cùng actor/key/body trả lại receipt cũ qua restart; khác body409;
version cũ409. Key tối đa128 ASCII printable; body tối đa4096bytes mặc định.
No-op không tăng version, nhưng vẫn ghi receipt/audit. Receipt không tự hết hạn;
đạt quota thì từ chối command mới503, không xóa key rồi thực thi trùng.

Pause chỉ ngừng generation của máy đó. Retry/in-flight vẫn tiếp tục; không đụng
Alert, ML hoặc RAG. Đổi scenario áp dụng future generation, không auto-resume.
GET state không chứng minh riêng một command timeout đã hoàn tất; UI giữ unknown
và retry cùng key/body. Rate/counters/queue/preview live chưa có trong closed DTO.

## Durable state, ordering và retry

SQLite/WAL, synchronous FULL, process lock tại `SIM_STATE_PATH` (mặc định
`sensor/output/state.sqlite3`). **Mount persistent local/block volume**, một process,
một replica với Recreate. Không hỗ trợ SQLite shared/network filesystem hoặc HA
multi-controller; cần thiết kế shared lease/fencing riêng trước khi scale.

Transaction generation atomically giữ per-machine sequence/cursor/scenario step
và payload. Sequence không reset qua Pause/Resume/restart; stream_epoch là lineage
phiên chạy, không dùng so sánh thứ tự. Pending payload giữ nguyên event ID, time,
features và correlation ID qua retry. Chỉ xóa sau ACK202 hợp lệ/đúng identity.
Crash sau Kafka ACK nhưng trước xóa có thể gửi trùng: downstream phải idempotent.
Không tuyên bố exactly-once delivery. Không tự polling status_url hoặc đợi prediction.

Spool có quota records/bytes (bao gồm rejected/auth/expired) và TTL. Đầy thì dừng
sinh thêm, không drop cũ hoặc tiêu thụ sequence. TTL chuyển `expired`, giữ payload
cho operator xử lý; không silent purge. Sender giới hạn concurrency, mỗi máy một
in-flight; backoff máy này không chặn máy khác. Retry transport/408/429/5xx theo
policy/jitter, tôn trọng Retry-After tối đa300s.401/403 không retry mù;400/422/409
lưu lỗi terminal. ACK sai identity/body là uncertain, giữ payload để retry.

**Khi controller đã dừng**, maintenance dùng cùng process lock:

```bash
.venv/bin/maintai-sensor spool list --state-path sensor/output/state.sqlite3
.venv/bin/maintai-sensor spool retry --state-path sensor/output/state.sqlite3 \
  --event-id evt_example --reason 'credential rotated'
.venv/bin/maintai-sensor spool discard --state-path sensor/output/state.sqlite3 \
  --event-id evt_example --reason 'exported evidence; approved discard'
```

Retry/discard có audit, retry không thay reading ID/body. Chỉ discard sau khi đã
quyết định xử lý dữ liệu; không có HTTP discard cho browser. Dataset/config hash
khác thì startup fail; migration phải preserve sequence/pending payload. **Không
xóa volume/DB rồi tiếp tục M-001 với sequence1**: restore backup/ordering state
hoặc thống nhất namespace máy mới với downstream. Source hash lưu ở journal;
không thêm provenance fields ngoài frozen wire vào JSON.

## Scenarios và fault injection

Live allowlist lấy từ tên YAML trong `sensor/scenarios/`:
normal/degradation/critical/recovery khớp UI. Degradation tăng stress dần; critical
chỉ là **kịch bản input**, không ép model/risk thành CRITICAL; recovery ramp về
baseline. Runtime giữ cuối phase sau khi hết ramp, không tự quay về baseline.
Categorical RNG được derive từ durable position, không reset cursor ngẫu nhiên.
Các YAML sudden/gradual/categorical-drift vẫn giữ cho kiểm thử Drift.

Fault feature missing/type/range vẫn có envelope hợp lệ →202, Processing INVALID.
`invalid_envelope` xóa machine_id →400/422 và local rejected journal. Các fault khác:
duplicate_event, network_timeout, network_disconnect. Production/demo shared mặc
định `SIM_FAULT_MODE=none`; injection chỉ phục vụ thử nghiệm có chủ đích.

Offline harness không gửi HTTP hoặc thay durable state:

```bash
.venv/bin/maintai-sensor replay --dry-run --limit 3
.venv/bin/maintai-sensor replay --dry-run --scenario gradual-drift --limit 1000
```

Live finite replay dùng cùng journal/sender; `--limit` là tổng unique readings.
Hết drain budget mà còn pending thì exit1 và giữ spool, không làm mất dữ liệu.
Dry-run `--metrics-file` là artifact chẩn đoán prefix `maintai_replay_`, không phải
metrics server sau spool commit. Các flags retry-count/on-error của CLI cũ được
thay bằng spool/TTL/backoff của runtime. Dry-run có thể dùng interval0; durable runtime yêu cầu interval dương.

## Probes, metrics và kiểm tra

`/health/live`, `/health/ready`, `/metrics` thuộc cùng runtime. Ready kiểm tra
journal và controller loop; không đồng nghĩa Ingestion/Kafka hay ML đã khỏe.
Metrics nội bộ, NetworkPolicy/management routing do platform quản lý:

- `maintai_simulator_generated_total`: sau spool commit.
- `maintai_simulator_send_attempts_total{outcome}`: mỗi HTTP attempt, không lẫn unique reading.
- `maintai_simulator_machines{state}` và `maintai_simulator_spool_pending_count`: đọc persisted state.
- HTTP Counter/Histogram theo OBS-01; route template, không machine/event IDs.
- Structured logs có trace IDs, control actor/version; spool summary có counts,
  buffer_used_bytes, oldest_unacked_event_age. Metrics không thay audit ledger.

```bash
.venv/bin/ruff check sensor
.venv/bin/ruff format --check sensor
.venv/bin/python -m pytest sensor/tests --cov=simulator
.venv/bin/python -m build sensor
.venv/bin/pip-audit --disable-pip --no-deps -r sensor/requirements.lock
```

UI browser suite chạy provider Python thật với fixture Ingestion, ngoài các mock
failure/PKCE tests. Container Dockerfile pin base digest, user10001, port8005 và
serve entrypoint. Mount CSV read-only/state writable, supply JWT/service credentials.
Image/container/Kubernetes rollout cần kiểm chứng riêng; không suy từ tests Python.

Tham khảo: [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/),
[PyJWT verification](https://pyjwt.readthedocs.io/en/stable/api.html),
[Prometheus registry](https://prometheus.github.io/client_python/registry/).
