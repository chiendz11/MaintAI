# MaintAI CI

Workflow: [workflows/ci.yml](workflows/ci.yml), theo
[cú pháp GitHub Actions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).

- Chạy khi push hoặc mở/cập nhật pull request; có khai báo `workflow_dispatch`
  để chạy thủ công khi workflow đã có trên default branch.
- Job `structure` kiểm tra README,10 source-owner folders và thư viện `messaging/`; deployment boundaries
  xem `architecture/runtime-map.json` (14 runtime descriptors).
- Job `simulator-ui-ci` dùng Node24, npm lockfile, typecheck/format, unit/JSON Schema
  checks, production build và Chromium browser tests; là dependency bắt buộc của
  `CI / required`. Browser suite có real Sensor provider/local auth và fixture Ingestion, cùng các
  mocked API/PKCE cases; không phải production IdP/Kafka E2E.
- Job `api-contracts` chạy `python contracts/v1.3/validate.py`: kiểm tra sáu OpenAPI,
  JSON Schemas, local references, HTTP examples/defaults, SSE và counterexamples.
  Bộ contract tự chứa baseline1.3.2, không phụ thuộc tài liệu/contracts v1.1 đã gỡ.
  Kafka1.4 có bundle/validator riêng; HTTP/release provenance chưa migrate đủ.
  Validator này không chứng minh full v1.4 đã được kiểm thử.
- Job `kafka-ci` chạy Python3.11/3.12: lint/format, bundle SHA lock, wire fixtures,
  unit tests, build sdist/wheel và validate resources từ wheel ngoài repo.
- Job `kafka-integration` chạy Kafka4.0.0/PostgreSQL17 biệt lập: broker ACK,
  transaction rollback, concurrent dedupe, outbox retry/fencing và Business delivery.
  Không gọi AWS/S3 thật hoặc deploy app; raw archive được test bằng fake object store.
- Job `sensor-ci` chạy trên Python 3.11/3.12: lint/format, unit/contract tests,
  mock/real HTTP, SQLite restart/idempotency/quota, signed JWT và TLS JWKS/OAuth;
  build wheel/sdist rồi cài wheel trong venv mới ngoài checkout để kiểm modules,
  bundled scenarios và CLI dry-run. Python3.12 thêm runtime dependency audit.
- Job `sensor-container` build image, dry-run offline, rồi chạy serve non-root,
  kiểm probes/metrics/control và restart cùng SQLite volume để xác minh sequence,
  pending payload và command receipt được giữ. Container test dùng network none,
  Ingestion cố ý offline và anonymous control chỉ bind loopback trong container.
- Job `simulator-ui-container` build frontend image riêng bằng OCI base digests
  trong `sensor-simulator-UI/ci-images.json`. Kiểm non-root/read-only filesystem,
  Nginx config, probes, built assets, runtime config no-store và `/api/` trả404.
  Các digests này là inputs CI, không tự thay release/config ở GitOps.
- Job `repo-metadata` kiểm SHA256 active docs trong source-manifest, local links/
  heading anchors, runtime Dockerfile/source paths và pinned UI CI images. Liên
  kết tới hai repo platform bên cạnh được phân loại ngoài checkout, không clone
  hoặc giả vờ kiểm tra các file của chúng trên app runner.
- Job `drift-ci` chạy Python 3.12: lint/format, unit/contract tests, PostgreSQL 17
  integration, bốn sensor scenarios, build wheel/sdist và runtime dependency audit.
- Job `drift-container` build sensor/worker và chạy cùng PostgreSQL + Pushgateway,
  kiểm tra drift reports, retry, checkpoints và metrics của bốn scenarios.
- Jobs `ingestion-ci`/`ingestion-integration` kiểm tra M1 unit/HTTP contracts,
  real Kafka và container dùng JWKS fixture; đều thuộc `CI / required`. Không
  chứng minh shared provider OIDC/TLS/ACL hoặc deploy.
- Job `component-ci` dùng matrix cho `backend`, `frontend`,
  `ml-service`, `rag-service`; chỉ bật khi repository variable
  `ENABLE_COMPONENT_CI` có giá trị `true` (hiện chưa được đặt).
- Job `required` có tên check cố định `CI / required`, chạy với `always()` và
  tổng hợp toàn bộ các jobs trên. Job bắt buộc bị fail/cancel/skip đều làm check
  này thất bại. Chỉ placeholder `component-ci` được phép skip khi chưa bật biến
  `ENABLE_COMPONENT_CI=true`. Khi thêm CI job mới, thêm nó vào `required.needs`.
- Quyền workflow là `contents: read`; chưa cần secrets.

Audit dependencies của Sensor/Drift vẫn chạy và lưu cảnh báo trong log nhưng
không chặn merge, theo policy đã merge ở PR #5. Tests, contracts, metadata,
build và container checks vẫn là các kiểm tra bắt buộc.

Với các component còn lại, thêm setup runtime, cache, install dependencies, lint,
tests và build khi có code. Có thể dùng điều kiện `matrix.component` hoặc tách thành job
riêng nếu stack khác nhau. Thay bước placeholder, sau đó đặt repository variable
`ENABLE_COMPONENT_CI=true` trong Settings → Secrets and variables → Actions → Variables
để bật job. Placeholder chủ động trả lỗi nếu bị bật khi chưa cấu hình.

CI xanh xác nhận cấu trúc repo và các kiểm tra Sensor/Drift/Kafka đã cấu hình; không kiểm tra
nghiệp vụ của các component còn lại. Chưa có publish image, cập nhật GitOps hoặc deploy.

Container integration dùng fixture DB ingestion vì backend vẫn còn skeleton.
Đây chưa phải E2E HTTP ingestion/inference/maintenance với backend production.
Lịch chạy định kỳ của worker thuộc repo GitOps/hạ tầng, không phải lịch CI.

## Bảo vệ main

Ruleset `protect-main` áp dụng chính xác cho `refs/heads/main`:

- Bắt buộc Pull Request, 0 required approvals và resolve hết review discussions.
- Check `CI / required` từ GitHub Actions phải thành công; PR phải cập nhật theo
  `main` trước khi merge (`strict_required_status_checks_policy=true`).
- Chỉ Squash merge; merge commit và rebase merge bị tắt ở repository settings.
- Chặn xóa `main` và force push; danh sách bypass rỗng.
- Không yêu cầu CODEOWNERS approval, latest-push approval hoặc dismiss stale reviews.

Payload cấu hình nằm ở [rulesets/protect-main.json](rulesets/protect-main.json).
File là bản cấu hình được version bằng Git; trạng thái thực thi nằm trên GitHub,
không tự đồng bộ chỉ bằng việc sửa JSON trong repository.

```bash
gh api repos/chiendz11/MaintAI/rules/branches/main
```


Sensor0.3.0 CI cài locked FastAPI/JWT/Prometheus dependencies và kiểm journal,
Control API, restart, scoped JWT, TLS JWKS/OAuth fixture. Drift regression job cài
thêm runtime lock Sensor trước editable install để CLI fixture không thiếu JWT.
Simulator UI job cài Python provider dependencies; browser suite khởi động real
Sensor HTTP provider + fixture Ingestion, ngoài mocked failure/PKCE cases.
Các tests này không phải production IdP/Kafka hoặc Kubernetes rollout evidence.


## Bằng chứng và cách kiểm tra CI mới

Sensor lưu JUnit/coverage XML và browser lưu JUnit/failure traces thành Actions
artifacts, retention7 ngày. Actions dùng commit SHA; Node24/Python3.11–3.12 và
runtime lockfiles giữ versioning. Job `required` còn tổng hợp thêm repo-metadata
và simulator-ui-container; không dùng continue-on-error hoặc skip test để giả xanh.

```bash
python3 .github/scripts/check_repository.py
.venv/bin/ruff check sensor .github/scripts sensor-simulator-UI/tests/provider --config sensor/pyproject.toml
.venv/bin/python -m pytest sensor/tests
cd sensor-simulator-UI
npm run check
npm test
npm run build
npm run test:e2e
```

Container helpers chạy sau khi build image và cần Docker daemon:

```bash
python3 .github/scripts/container_smoke.py sensor --image maintai-sensor:ci
python3 .github/scripts/container_smoke.py ui --image maintai-simulator-ui:ci
```

Container/volume tạo tên ngẫu nhiên, không publish port và cleanup ở finally.
CI không publish registry, không dùng production secrets và không deploy lên môi
trường. Xem run đúng commit trên GitHub để xác nhận container jobs đã chạy.
