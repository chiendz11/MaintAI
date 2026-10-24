# MLflow Server image — MaintAI source, GitOps deployment

`ml-service/mlflow/` sở hữu **đóng gói image** của upstream MLflow Tracking +
Registry + UI. Training, inference và MLflow có ba image/vòng phát hành riêng.
Không fork MLflow, không viết lại UI, không thêm database/service mới khi chuyển
Dockerfile từ GitOps. Platform/MLOps vận hành server; ML owner kiểm SDK compatibility.

## 1. File và chủ sở hữu

| File/phần việc | Vai trò |
| --- | --- |
| `Dockerfile`, `requirements.in` / `requirements.lock` | Image upstream + exact PostgreSQL/S3 adapter dependencies; non-root10001 |
| `image-lock.json` | App Git giữ exact upstream server patch/base digest và compatibility review |
| `compose.dev.yaml`, `.env.example` | Local-only PostgreSQL17 + filesystem artifact volume; UI chỉ bind127.0.0.1 |
| `release.py`, `github_app.py` | Build/smoke/push/sign exact image; dispatch intent, không merge/deploy |
| `contracts/` | Byte-identical copy + SHA của existing GitOps workload-release1.0.0 |
| `requirements-release.lock`, `requirements-dev.lock`, `tests/` | Release tooling/tests, không cài vào server image |
| `runtime.json` | Platform descriptor trong app source; không phải deployment manifests |
| `MaintAI-gitops/platform/mlops/` | Production Deployment/Service, config/secret references, resources, IRSA và enablement |
| `Maint-infrastructure` | RDS/S3/IAM/ECR/network/cluster |

Dependency locks ở đây bảo toàn hai direct dependencies của Dockerfile GitOps cũ
(`boto3==1.40.61`, `psycopg2-binary==2.9.11`) và freeze transitives; base image
upstream vẫn mang MLflow/Python dependencies của nó. Build chạy `pip check`,
publisher kiểm exact server version/required flags/adapters/UID. Những kiểm tra đó
không thay real PostgreSQL/S3/IdP và SDK compatibility validation.

## 2. Trạng thái versioning

`image-lock.json` hiện để `base_image` và `mlflow_version` là **null**,
`compatibility_reviewed=false`, giữ trạng thái chưa chốt trước khi chuyển repo.
Không điền digest giả hoặc tự coi một patch mới là đã tương thích. ML + Platform
cần chọn exact patch và official `ghcr.io/mlflow/mlflow[:tag]@sha256:...`, xác minh
digest/architecture, SDK pair/serialization/dependencies cùng server flags rồi
commit pins qua PR. Tag `latest` hoặc tag đơn thuần không được publisher chấp nhận.
Review chỉ đặt true sau kiểm thử cặp thực; base/config file là source of truth,
không có production override qua Actions variable `MLFLOW_BASE_IMAGE` nữa.

`python release.py check-source` kiểm config/schema **offline**, có thể pass khi
publication_ready=false. Đây là kiểm source, không xác nhận image được build hoặc
service đang chạy. Production image digest vẫn là null/disabled trong GitOps.

## 3. Local dev

Sau khi có reviewed base pin và Docker daemon, chạy từ folder này:

```bash
cp .env.example .env
# Điền MLFLOW_BASE_IMAGE đúng image-lock.json và POSTGRES_IMAGE (PostgreSQL17@sha256).
# Giữ password local-only, URL-safe; không dùng credential production.
docker compose --env-file .env -f compose.dev.yaml up --build -d
```

Mở `http://127.0.0.1:5000`; training chạy trên host dùng
`MLFLOW_TRACKING_URI=http://127.0.0.1:5000`. Đổi port bằng MLFLOW_LOCAL_PORT.
Containers cùng Compose network dùng `http://mlflow:5000`; networks của các app
stack khác phải nối có chủ đích, không tự thêm vào production.

Compose build từ **chính Dockerfile này**. PostgreSQL giữ metadata/registry;
named volume artifacts do server serve qua HTTP. Không đưa AWS credentials vào
browser/container local. Đây là PostgreSQL + **filesystem artifacts**, chưa phải
MinIO/S3 integration hoặc môi trường production. Để kiểm S3 cần endpoint/credential
và fixture riêng. Cùng Dockerfile không có nghĩa cấu hình auth/storage giống nhau.
Stack này không có production auth và chỉ bind loopback; không đổi bind thành
public để dùng thay deployment GitOps.

Muốn pull image đã phát hành cho dev, giữ `MLFLOW_LOCAL_IMAGE=ECR_REPO@sha256:...`
và dùng `docker compose ... up --no-build` sau khi có registry login hợp lệ.
Không dùng một image vừa build local để ghi đè digest production. `down` giữ named
volumes; `down -v` xóa metadata/artifacts local nên chỉ dùng khi chủ đích reset.

## 4. Publisher và GitOps receiver

Workflow thật nằm ở [maintai-publish.yml](../../.github/workflows/maintai-publish.yml),
hiện chỉ expose group `mlflow`. Các service khác chưa được bật publish bởi thay đổi
này. Workflow chỉ chạy manual trên `chiendz11/MaintAI` main, khi
`ENABLE_IMAGE_PUBLICATION=true`, qua environment `prod`; không tự bật biến hoặc
gửi workflow trong lượt authoring này.

1. Kiểm base/version/compatibility/schema trước assume AWS role.
2. Kiểm checkout SHA/context, target ECR group/region; build image từ module context.
3. Smoke imports MLflow/PostgreSQL/S3 adapters, exact version/required CLI flags và
   non-root UID trước push. Chưa phải DB/S3 runtime integration.
4. Push immutable `git-<full SHA>` tag, lấy digest do ECR trả, Cosign keyless-sign
   digest với annotations `git_sha`, `image_group=mlflow`, `source_repository`.
5. GitHub App chỉ mint token Actions-write cho receiver repo và dispatch existing
   `workload-release.yml`. ACK204 không chứng minh PR đã tạo/merge hoặc deploy.

GitOps chỉ chấp nhận identity:

```text
https://github.com/chiendz11/MaintAI/.github/workflows/maintai-publish.yml@refs/heads/main
issuer: https://token.actions.githubusercontent.com
source_repository annotation: chiendz11/MaintAI
```

Actor phải khớp APP_RELEASE_ALLOWED_ACTOR ở receiver; image phải đúng ECR allowlist.
Identity cũ của GitOps publisher không được nhận cho group mlflow. Không auto-sign
foreign/unsigned image đã có tag: rerun chỉ reuse digest đã verify đúng signer và
annotations. Nếu push xong nhưng chưa ký đã crash, tag unsigned cần operator triage
hoặc commit mới; không tự nhận làm artifact trusted. Docker immutable tag policy
vẫn phải do infrastructure cấu hình.

Intent giữ existing workload-release1.0.0/handoff architecture1.3.1+simulator-ui.1;
không migrate toàn bộ platform1.4 bằng thay đổi source. `config_schema_sha256={}`
vì MLflow là platform group; không gửi `enable` hoặc runtime/DB/S3/secret config.
Receiver chỉ mở PR cập nhật desired image/source identity. Enablement và rollout
vẫn là PR/operator action riêng trong GitOps.

## 5. Inputs cần cấu hình trước publish

- App Actions vars: `ENABLE_IMAGE_PUBLICATION`, `IMAGE_REPOSITORIES_JSON` chứa
  mlflow ECR repo, `AWS_REGION`, `APP_IMAGE_PUBLISHER_ROLE_ARN`,
  `APP_RELEASE_PUBLISHER_APP_ID`, `APP_RELEASE_PUBLISHER_INSTALLATION_ID`.
- App secret: `APP_RELEASE_PUBLISHER_PRIVATE_KEY`; không đưa vào Docker build args.
- GitOps vars: app publisher actor và reader role/approved platform ECR allowlist;
  GitHub App phải có Actions-write trên `MaintAI-gitops` và actual repo/workflows tồn tại.
- **Infra còn cần sửa trước activation:** `terraform/platform/iam.tf` ở
  Maint-infrastructure hiện cấp ECR mlflow cho GitOps publisher. Chuyển grant đó
  sang app publisher, giữ GitOps publisher chỉ có automation; review/apply riêng.

Lượt này sửa app + GitOps, không sửa Terraform, cấp quyền cloud, pin version thay
owner, publish image hoặc deploy. Giữ production mlflow enabled=false/image=null.

## 6. Kiểm tra source

Từ repo root, không cần MLflow server, AWS hoặc Docker:

```bash
python -m pip install -r ml-service/mlflow/requirements-dev.lock
python ml-service/mlflow/release.py check-source
python -m pytest ml-service/mlflow/tests
ruff check ml-service/mlflow
ruff format --check ml-service/mlflow
```

Tests mock Docker/AWS/Cosign/GitHub, kiểm pin/owner/schema, publish ordering,
reused/unsigned artifact và token scope. Không chạy chúng như integration proof.
CI source job nằm trong `CI / required`. Base chưa pin nên không có required image
build giả xanh; image build/smoke thật nằm ở manual publisher sau khi pins sẵn sàng.
Theo dõi trạng thái chung tại [MLFLOW-01](../../docs/MLFLOW_INTEGRATION_GUIDE.md)
và [readiness](../../docs/implementation-readiness.md).
