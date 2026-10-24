# Sensor0.3.0 — báo cáo implementation và file thay đổi

Nhánh `feat/sensor-simulator-ui`, ngày2026-10-20. Bảng dưới so với snapshot ngay
trước lượt nâng cấp Sensor này; các thay đổi UI/docs có trước được giữ lại.
**68 file: 43 sửa, 20 thêm, 5 xóa.** Đây là snapshot trước lượt cấu hình CI/push;
trạng thái commit và checks mới nhất xem trên GitHub Actions.

## 1. Kết quả và ranh giới

- Sensor → Ingestion: SensorRequest1.3.0, ACK202 only; không đợi inference/Alert.
- SQLite local persistent journal: atomic sequence/cursor+payload, state/version,
  command receipts/audit. Retry giữ exact event/body/correlation qua restart.
- 5 Control APIs baseline1.3.2, auth/JWKS/scoped role/machines; state commit trước200.
- Scheduler và bounded sender cùng một Sensor Deployment; Pause không dừng máy
  khác hoặc xóa pending/in-flight/Alert. YAML normal/degradation/critical/recovery.
- Probes/OBS-01 metrics; UI hiển thị actor và được kiểm với provider Python thật.
- Không đổi frozen OpenAPI/JSON schemas, không tạo Kafka client/ML inference ở Sensor.
  Không đổi GitOps/infrastructure hoặc source Drift/Business/Processing.
- Rate/counters/preview live chưa có wire capability. UI unavailable là đúng contract;
  muốn bật phải freeze extension trước. Browser demo vẫn ghi rõ synthetic data.

## 2. Các kiểm tra

| Kiểm tra | Kết quả/phạm vi |
| --- | --- |
| Sensor Python |134 tests PASS: unit/current schema, mock/real HTTP, SQLite transactions/restarts, CLI process restart, auth và TLS JWKS/OAuth fixtures |
| Drift regression |43 unit tests PASS; scenarios cũ vẫn được giữ; không chạy DB/container integration khi daemon thiếu |
| Simulator UI | TypeScript/Prettier,18 unit/contract/recovery tests và production static build PASS |
| Chromium |9 cases PASS, có real Sensor HTTP provider + fixture202 ingress; provider case được rerun sau đổi auth/probes |
| API machine bundle |6 OpenAPIs/52 operations/651 refs,96 HTTP examples/defaults,53 positive/negative cases PASS |
| Python distribution | Sensor0.3.0 wheel/sdist build PASS; kiểm module mới và7 scenario YAML trong wheel |
| Runtime dependency audit | pip-audit locked runtime dependencies: no known vulnerabilities tại lần kiểm này |
| Docs/metadata | Local links/anchors, hashes source manifest, runtime paths và git diff whitespace được kiểm riêng |

Local Python3.12, Node26; CI còn khai báo Python3.11/3.12 và Node24, chưa chạy
GitHub CI cho changes chưa commit. TestClient có deprecation notice của Starlette
về httpx adapter; không phải test failure. Không coi tests fixture là production.

## 3. Cách dùng và vận hành

Đọc [Sensor README](../sensor/README.md) cho local/shared commands, env/claims,
volume mount, repair pending/rejected/auth/expired. Read [UI README](../sensor-simulator-UI/README.md)
để chọn live mode. Một controller/Recreate + persistent local/block volume;
strict multi-controller HA cần lease/fencing/shared state riêng.

Spool quota/TTL không silent drop: quota dừng generation, TTL giữ expired payload.
Maintenance chỉ chạy khi controller đã dừng; retry/discard có audit. Receipt không
silent expire để tránh re-execute key cũ. Dataset/config hash đổi cần migration;
volume mất cần restore ordering state, không reuse M-001 với sequence1.

Docker daemon không chạy ở máy nên chưa build/run image hoặc xác minh output
digest. Chưa triển khai Kubernetes/PVC/rolling/failover/load, production IdP/browser
code exchange hoặc full Kafka/Processing/Business E2E. Python base image digest đã
pin từ public registry; digest base không phải output image đã build.

## 4. Danh sách từng file và lý do

| Thay đổi | File | Lý do |
| --- | --- | --- |
| Sửa | [.github/README.md](../.github/README.md) | Giải thích phạm vi CI: durable/auth/TLS provider fixtures, browser gọi Python thật, các giới hạn production. |
| Sửa | [.github/workflows/ci.yml](../.github/workflows/ci.yml) | Cài Sensor runtime lock trong Drift regression; cài Python/provider dependencies cho UI browser job; giữ CI / required và không thêm publish/deploy. |
| Sửa | [AGENTS.md](../AGENTS.md) | Hướng agent dùng schema hiện hành thay frozen wire1.1; phân biệt mock/TLS/provider integration và deployment. |
| Sửa | [README.md](../README.md) | Cập nhật trạng thái Sensor0.3.0, Control provider, cadence/durable/auth và kiểm tra đã có. |
| Sửa | [architecture/README.md](../architecture/README.md) | Đổi trạng thái Sensor và Simulator UI theo source thật; giữ14 app runtimes và platform riêng. |
| Sửa | [contracts/README.md](../contracts/README.md) | Gỡ liên kết fixture1.1 đã bỏ; chỉ rõ SensorRequest1.3.0/ACK202/Control1.3.2 mà tests sử dụng. |
| Sửa | [docs/DEPLOYMENT_ARCHITECTURE.md](../docs/DEPLOYMENT_ARCHITECTURE.md) | Ghi local persistent SQLite/WAL state/spool, one controller/Recreate và provider đã implement; chưa claim rollout. |
| Sửa | [docs/IMPLEMENTATION_PLAN.md](../docs/IMPLEMENTATION_PLAN.md) | Ghi tiến độ Sensor UI-P2/instrumentation; giữ các việc production platform/E2E còn mở. |
| Sửa | [docs/MaintAI_Functional_Overview.md](../docs/MaintAI_Functional_Overview.md) | Đổi implementation mapping từ legacy/missing provider sang ACK202/control/auth hiện có. |
| Sửa | [docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) | Sửa provider status và thêm §50: source boundaries, auth, journal/recovery, metrics, cách dùng và giới hạn. |
| Sửa | [docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md](../docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) | Cập nhật ghi chú implementation provider; giữ nguyên API/payload, domain enums và quy tắc chuẩn. |
| Sửa | [docs/OBSERVABILITY_IMPLEMENTATION.md](../docs/OBSERVABILITY_IMPLEMENTATION.md) | Ghi bằng chứng instrumentation Sensor; phân biệt offline maintai_replay_ với counters sau spool commit. |
| Sửa | [docs/README.md](../docs/README.md) | Cập nhật index và trạng thái Sensor/Drift/UI, thêm report chi tiết; không coi modules khác đã migrate. |
| Sửa | [docs/RUNTIME_DATAFLOW.md](../docs/RUNTIME_DATAFLOW.md) | Thêm luồng source thực thi: journal trước HTTP, delete sau ACK, control persist trước200. |
| Thêm | [docs/SENSOR_IMPLEMENTATION_REPORT.md](../docs/SENSOR_IMPLEMENTATION_REPORT.md) | Bàn giao thay đổi từng file, quyết định kỹ thuật, cách kiểm tra và giới hạn thực tế của lượt này. |
| Sửa | [docs/UI_ARCHITECTURE.md](../docs/UI_ARCHITECTURE.md) | Ghi provider HTTP đã test, source Sensor đã có; vẫn giữ closed Control DTO và release độc lập. |
| Sửa | [docs/implementation-readiness.md](../docs/implementation-readiness.md) | Sửa dòng cũ UI chưa có source; đồng bộ bảng component/provider, thêm §15 với bằng chứng và khoảng cách còn lại. |
| Sửa | [docs/legacy-artifact-audit.md](../docs/legacy-artifact-audit.md) | Phân biệt snapshot dọn19/10 và migration Sensor20/10; ghi việc gỡ fixture1.1 sau khi chuyển tests. |
| Sửa | [docs/repository-layout.md](../docs/repository-layout.md) | Các module control/ordering/retry_spool/scheduling đã có implementation trong cùng Sensor runtime. |
| Sửa | [docs/source-manifest.json](../docs/source-manifest.json) | Đăng ký report và cập nhật SHA256 tài liệu active; giữ nguyên upstream hashes/retired provenance. |
| Sửa | [sensor-simulator-UI/README.md](../sensor-simulator-UI/README.md) | Cách chạy real provider, prerequisites Python, actor, browser/TLS fixture coverage và các giới hạn còn lại. |
| Sửa | [sensor-simulator-UI/playwright.config.ts](../sensor-simulator-UI/playwright.config.ts) | Khởi động Sensor provider Python thật cùng static preview; Python executable có override/cách chọn local hoặc CI. |
| Sửa | [sensor-simulator-UI/runtime.json](../sensor-simulator-UI/runtime.json) | Phản ánh provider đã có, phân biệt fixture ingestion/local auth và production integration. |
| Sửa | [sensor-simulator-UI/src/App.tsx](../sensor-simulator-UI/src/App.tsx) | Hiển thị last_controlled_by cạnh thời gian cập nhật; lấy từ provider, không suy từ tài khoản UI. |
| Thêm | [sensor-simulator-UI/tests/browser/provider.spec.ts](../sensor-simulator-UI/tests/browser/provider.spec.ts) | Browser đi qua provider HTTP thật để Resume/Pause/Recovery/reload/actor, kiểm máy khác không đổi. |
| Thêm | [sensor-simulator-UI/tests/provider/server.py](../sensor-simulator-UI/tests/provider/server.py) | Fixture browser: real Sensor provider + temp journal + HTTP202 ingress local; teardown và ports chỉ dành tests. |
| Sửa | [sensor/.env.example](../sensor/.env.example) | Mẫu secure JWT/JWKS/claims/CORS và outbound token-file/OAuth; quota/TTL/workers/volume, local profile explicit. |
| Sửa | [sensor/Dockerfile](../sensor/Dockerfile) | Pin Python base digest, tạo state directory writable non-root, serve entrypoint, port8005/probes; không bake CSV/secrets. |
| Sửa | [sensor/README.md](../sensor/README.md) | Hướng dẫn cài/chạy local/shared, API/auth/claims, durable state/recovery, quota, scenarios, metrics và validation. |
| Sửa | [sensor/pyproject.toml](../sensor/pyproject.toml) | Bump package0.3.0; thêm FastAPI/Uvicorn, PyJWT crypto, Prometheus cho runtime thật. |
| Sửa | [sensor/requirements-dev.lock](../sensor/requirements-dev.lock) | Khóa môi trường CI/test/build/audit cùng runtime deps mới. |
| Sửa | [sensor/requirements.lock](../sensor/requirements.lock) | Khóa chính xác runtime dependencies và transitive deps cho provider/JWT/metrics. |
| Sửa | [sensor/runtime.json](../sensor/runtime.json) | Entrypoint serve đã implement, contract baseline1.3.2, wire1.3.0; ghi single-controller và trạng thái chưa build image. |
| Thêm | [sensor/scenarios/critical.yaml](../sensor/scenarios/critical.yaml) | Input stress cho scenario UI critical; model/risk policy vẫn quyết định kết quả. |
| Thêm | [sensor/scenarios/degradation.yaml](../sensor/scenarios/degradation.yaml) | Ramped input stress cho scenario UI degradation; giữ validation và không ép risk. |
| Thêm | [sensor/scenarios/recovery.yaml](../sensor/scenarios/recovery.yaml) | Ramped input về baseline cho recovery; không tự Resume hoặc resolve Alert. |
| Sửa | [sensor/src/simulator/__init__.py](../sensor/src/simulator/__init__.py) | Cập nhật package version0.3.0 và wire/control version độc lập. |
| Sửa | [sensor/src/simulator/cli.py](../sensor/src/simulator/cli.py) | Thêm serve và spool maintenance; finite live replay dùng journal/sender; bỏ đường gửi synchronous và flags cũ không còn hiệu lực. |
| Sửa | [sensor/src/simulator/client.py](../sensor/src/simulator/client.py) | Thay completed prediction DTO bằng ACK202; verify identity/absolute status URL, retryable/Retry-After, credential callback và attempt metrics. |
| Thêm | [sensor/src/simulator/control/__init__.py](../sensor/src/simulator/control/__init__.py) | Đóng gói submodule Python trong cùng Sensor runtime; đảm bảo wheel có module và import rõ ràng. |
| Thêm | [sensor/src/simulator/control/api.py](../sensor/src/simulator/control/api.py) | 5 frozen operations, scoped auth, bounded JSON/keys, CAS/idempotency, error envelopes, trace IDs, CORS, probes và metrics. |
| Thêm | [sensor/src/simulator/control/auth.py](../sensor/src/simulator/control/auth.py) | Verify RSA/EC JWT bằng trusted JWKS, issuer/audience/expiry/roles/scopes/machine scope; outbound secret-file/OAuth cache. |
| Sửa | [sensor/src/simulator/event_factory.py](../sensor/src/simulator/event_factory.py) | Offline harness phát payload1.3.0 và sequence theo máy, giữ deterministic seeded replay. |
| Sửa | [sensor/src/simulator/fault_injection.py](../sensor/src/simulator/fault_injection.py) | Feature invalid không còn đồng nghĩa ingress reject; thêm fault invalid_envelope riêng. |
| Sửa | [sensor/src/simulator/logging.py](../sensor/src/simulator/logging.py) | JSON stderr handler theo stderr hiện tại, tránh giữ stream đã đóng khi CLI được gọi lại trong tests. |
| Thêm | [sensor/src/simulator/metrics.py](../sensor/src/simulator/metrics.py) | Custom registry theo OBS-01: committed generated, send attempts, states/pending gauges, HTTP Counter/Histogram chuẩn labels/buckets. |
| Sửa | [sensor/src/simulator/models.py](../sensor/src/simulator/models.py) | Generated SensorEvent thêm schema_version/epoch/per-machine sequence; vẫn strict allowlist sáu features. |
| Thêm | [sensor/src/simulator/ordering/__init__.py](../sensor/src/simulator/ordering/__init__.py) | Đóng gói submodule Python trong cùng Sensor runtime; đảm bảo wheel có module và import rõ ràng. |
| Thêm | [sensor/src/simulator/ordering/store.py](../sensor/src/simulator/ordering/store.py) | SQLite WAL/FULL transaction journal: state/version/sequence/cursor/payload/receipt/audit; process lock, quota, TTL, retry/quarantine/maintenance. |
| Thêm | [sensor/src/simulator/retry_spool/__init__.py](../sensor/src/simulator/retry_spool/__init__.py) | Đóng gói submodule Python trong cùng Sensor runtime; đảm bảo wheel có module và import rõ ràng. |
| Thêm | [sensor/src/simulator/retry_spool/replay.py](../sensor/src/simulator/retry_spool/replay.py) | Finite live CLI dùng cùng durable engine; drain budget giữ pending, xuất actual runtime metrics và luôn đóng resources. |
| Sửa | [sensor/src/simulator/runner.py](../sensor/src/simulator/runner.py) | Offline diagnostic summary dùng prefix maintai_replay_ để không giả server counter sau durable commit. |
| Thêm | [sensor/src/simulator/scheduling/__init__.py](../sensor/src/simulator/scheduling/__init__.py) | Đóng gói submodule Python trong cùng Sensor runtime; đảm bảo wheel có module và import rõ ràng. |
| Thêm | [sensor/src/simulator/scheduling/engine.py](../sensor/src/simulator/scheduling/engine.py) | Generation và sender thread pool độc lập; per-machine scheduling/one in-flight, persisted cursors, stable payload retry và graceful shutdown. |
| Thêm | [sensor/src/simulator/settings.py](../sensor/src/simulator/settings.py) | Validate cấu hình persistent runtime; fail-closed auth/HTTPS/origins, local-only anonymous, quotas và service credentials. |
| Sửa | [sensor/tests/conftest.py](../sensor/tests/conftest.py) | Fixture ingress độc lập dùng frozen SensorRequest, ACK202 và backend processing observation riêng; runtime config/real HTTP fixture. |
| Sửa | [sensor/tests/test_cli_config_download.py](../sensor/tests/test_cli_config_download.py) | Payload/schema/metrics mới; live CLI dùng local profile + temp journal; thêm subprocess serve/HTTP/restart receipt. |
| Sửa | [sensor/tests/test_client_runner.py](../sensor/tests/test_client_runner.py) | Tests chuyển sang ACK202 và feature-invalid accepted; bảo vệ trace identity/retryability/Retry-After và deterministic offline cadence. |
| Thêm | [sensor/tests/test_control_api.py](../sensor/tests/test_control_api.py) | Schema độc lập cho5 operations/errors, idempotency/version conflict, invalid/oversized JSON, scope, JWT claims, probes và safe config. |
| Sửa | [sensor/tests/test_dataset_events.py](../sensor/tests/test_dataset_events.py) | Kiểm fields wire1.3.0, feature leakage và đối chiếu với current machine schema thay snapshot cũ. |
| Thêm | [sensor/tests/test_durable_runtime.py](../sensor/tests/test_durable_runtime.py) | Crash/lost ACK/restart, sequence rollback/parallel allocation, process lock, Pause/backlog, nonblocking sender, quota/TTL/auth, CAS/receipt quota/recovery/metrics. |
| Thêm | [sensor/tests/test_oidc_integration.py](../sensor/tests/test_oidc_integration.py) | Ephemeral TLS JWKS/OAuth server với RSA signature thật; provider issuer/audience/expiry/permissions và outbound client-credentials cache. |
| Sửa | [sensor/tests/test_scenarios_faults.py](../sensor/tests/test_scenarios_faults.py) | Tách generated-feature validity với ingress envelope rejection, giữ regression drift transforms. |
| Xóa | `sensor/src/simulator/control/.gitkeep` | Thư mục đã có implementation thật; bỏ placeholder không còn dùng. |
| Xóa | `sensor/src/simulator/ordering/.gitkeep` | Thư mục đã có implementation thật; bỏ placeholder không còn dùng. |
| Xóa | `sensor/src/simulator/retry_spool/.gitkeep` | Thư mục đã có implementation thật; bỏ placeholder không còn dùng. |
| Xóa | `sensor/src/simulator/scheduling/.gitkeep` | Thư mục đã có implementation thật; bỏ placeholder không còn dùng. |
| Xóa | `sensor/tests/fixtures/sensor-wire.schema.json` | Gỡ frozen synchronous1.1 fixture; không còn test consumer sau migration sang current wire/schema. |

## 5. Tài liệu thư viện đã đối chiếu

[FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/),
[PyJWT verification/JWKS](https://pyjwt.readthedocs.io/en/stable/api.html),
[Prometheus Python registry](https://prometheus.github.io/client_python/registry/).
Contract/Guide MaintAI và machine schema vẫn quyết định wire/semantics dự án.
