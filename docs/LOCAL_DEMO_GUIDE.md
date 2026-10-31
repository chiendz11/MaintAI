# MaintAI — Hướng dẫn khởi động hệ thống local từ 3 repository

**Cập nhật:** 31/10/2026  
**Môi trường mục tiêu:** Linux (Arch Linux / Ubuntu hoặc tương đương), Docker Engine, k3d v5.9.0, Kubernetes/kubectl (1.34–1.36), Argo CD.  
**Phạm vi:** Local k3d cluster `maintai-local`. Không can thiệp Terraform/AWS production và không coi kết quả CI là bằng chứng thay thế cho runtime evidence thực tế.

---

> [!IMPORTANT]
> **Trạng thái các PR và Commit trên nhánh `main` của `MaintAI`:**
> 1. **PR #22 (`fix/kafka-serial-prefetch`):** Đã squash-merge vào `main` tại commit `575c8b4` (nâng cấp Kafka SDK 0.3.1, sửa lỗi prefetch làm treo telemetry).
> 2. **PR #23 (`feat(history): M6 history, charts and exports`):** Vừa squash-merge vào `main` tại commit `4ef10de` (bổ sung toàn bộ tính năng tra cứu lịch sử M6, Parquet/CSV export jobs, Storage catalog native 0.2.1 và 3 DB migrations mới).
>
> **LỰA CHỌN BASELINE TRƯỚC KHI CHẠY:**
> - **Baseline A (Khuyến nghị để chạy demo ổn định ngay):** Sử dụng nhánh **`demo/local-k3d`** ở cả hai repo `MaintAI-gitops` và `Maint-infrastructure`. Nhánh này đã được lock tương thích với bản sửa Kafka SDK 0.3.1 (commit `2df0a914` / `575c8b4`) và đã được nghiệm thu chạy thật thành công ngày 30/10/2026 (xem [Báo cáo chạy demo 30/10/2026](../../Maint-infrastructure/docs/LOCAL_DEMO_RUN_2026-10-30.md)).
> - **Baseline B (Muốn thử nghiệm Milestone M6 mới nhất - commit `4ef10de`):** Cần cập nhật `app-source.lock.json` trên cả hai repo platform để ghi nhận commit `4ef10de`, thêm các migrations mới (`0003_history`, `0004_catalog`, `0004_history_markers`), đóng gói `maintai-storage-consumer==0.2.1` vào Processing image và cấp quyền DB metadata.

---

## 0. Sơ đồ kiến trúc và phân định trách nhiệm

```text
                             Laptop Linux
 ┌────────────────────────────────────────────────────────┐
 │ Maint-infrastructure                                   │
 │ localdev CLI → k3d, private kubeconfig/Secrets/TLS     │
 │                build, k3d image import, owner Jobs     │
 │                         │                              │
 │                         ▼                              │
 │               k3d: maintai-local                       │
 │              Argo CD (bootstrap)                       │
 │                         │ Git pull                     │
 │                         ▼                              │
 │ MaintAI-gitops: environments/local/                    │
 │    ├── platform        Kafka/PostgreSQL/MinIO/IdP      │
 │    ├── observability   Prometheus/Grafana             │
 │    └── apps            App Deployments                │
 │                         ▲                              │
 │ MaintAI: source/Dockerfiles/SDK/contracts/model code   │
 │       (build OCI images; không deploy trực tiếp)       │
 └────────────────────────────────────────────────────────┘
```

| Repository | Đường dẫn local mẫu | Trách nhiệm khi chạy local |
| --- | --- | --- |
| **MaintAI** | `/home/chienqt/Dev/newBTL/MaintAI` | Ứng dụng nguồn, Dockerfiles, Kafka SDK, Risk Policy catalog, ML/RAG models |
| **MaintAI-gitops** | `/home/chienqt/Dev/newBTL/MaintAI-gitops` | Khai báo desired-state Kustomize cho Argo CD; quản lý version lock, manifests và release receipts |
| **Maint-infrastructure** | `/home/chienqt/Dev/newBTL/Maint-infrastructure` | Khởi tạo cluster k3d, private data/Secrets, build/import images, bootstrap Argo CD và chạy DB owner migrations/grants |

**Nguyên tắc vận hành:**
- Argo CD *không* build images và *không* đọc các file GitOps chưa push lên remote branch được chỉ định.
- `localdev up` *không* tự deploy platform hay applications.
- File profile mẫu `profiles/local.example.json` mặc định để `enabled: false` cho toàn bộ workload.
- Các bước: build image, import vào k3d, migrate DB, cấu hình profile, push GitOps và xác minh readiness là các bước độc lập và có bằng chứng xác thực (provenance evidence).

---

## 1. Môi trường công cụ và bố cục thư mục

### 1.1. Yêu cầu công cụ
- **Docker Engine**: Đang chạy (`docker info` thành công; user nằm trong group `docker`).
- **k3d**: Đúng bản pin **v5.9.0** (`k3d version`).
- **kubectl**: Bản client minor từ **1.34 – 1.36** (`kubectl version --client`).
- **Python**: **3.12** (`python3.12 --version`), hỗ trợ `venv`.
- **Git** và **OpenSSL**.
- Phần cứng khuyến nghị: Tối thiểu **8 CPU / 16 GB RAM**, còn trống ít nhất 30 GB đĩa cho Docker images, volumes MinIO, PostgreSQL và model weights.

### 1.2. Bố cục thư mục làm việc (Sibling Directory Layout)
Ba repository phải nằm **cùng một thư mục cha**. Nếu bạn đã có sẵn 3 thư mục trong workspace (ví dụ `/home/chienqt/Dev/newBTL/`), **không cần clone lại**:

```bash
# Cấu trúc thư mục thực tế:
# /home/chienqt/Dev/newBTL/
# ├── MaintAI/
# ├── MaintAI-gitops/
# └── Maint-infrastructure/

cd /home/chienqt/Dev/newBTL

# Cập nhật remote cho cả 3 repo:
git -C MaintAI fetch origin
git -C MaintAI-gitops fetch origin
git -C Maint-infrastructure fetch origin
```

> [!WARNING]
> Không dùng repository `MaintAI-k3d` cũ để quản lý cluster này. Nếu cluster `maintai-local` từng được tạo bằng công cụ khác, `localdev up` sẽ dừng lại vì thiếu cluster ownership marker.

---

## 2. Đồng bộ version và Source Locks giữa 3 Repo

Hệ thống sử dụng cơ chế bảo vệ nghiêm ngặt: `localdev build` và `localdev prepare apps` sẽ đối chiếu SHA256 từng file mã nguồn với file khóa `app-source.lock.json`. CLI sẽ chủ động từ chối nếu phát hiện source bị sai lệch hoặc commit chưa được review.

### 2.1. Kiểm tra trạng thái branch và source lock hiện tại

```bash
cd /home/chienqt/Dev/newBTL

# 1. Kiểm tra branch hiện tại của 3 repo
git -C MaintAI branch --show-current
git -C MaintAI-gitops branch --show-current
git -C Maint-infrastructure branch --show-current

# 2. Kiểm tra commit đang khóa trong lock files
python3.12 - <<'PY'
import json
from pathlib import Path
for p in [
    Path('Maint-infrastructure/local/versions/app-source.lock.json'),
    Path('MaintAI-gitops/config/local/app-source.lock.json'),
    Path('MaintAI-gitops/config/local/risk-policy.lock.json'),
]:
    if p.exists():
        v = json.loads(p.read_text())
        print(f'{p}: locked SHA = {v.get("source_git_sha")}')
    else:
        print(f'{p}: NOT FOUND')
PY
```

### 2.2. Lựa chọn hướng thực hiện

#### Phương án A — Dùng baseline ổn định đã nghiệm thu trên nhánh `demo/local-k3d` (Khuyến nghị)
Nhánh `demo/local-k3d` trên cả `MaintAI-gitops` và `Maint-infrastructure` đã được cấu hình hoàn chỉnh để chạy demo với Kafka SDK 0.3.1 (commit `2df0a914...` hoặc `575c8b4`):

```bash
cd /home/chienqt/Dev/newBTL

# Chuyển GitOps và Infrastructure sang nhánh demo đã kiểm chứng
git -C MaintAI-gitops switch demo/local-k3d
git -C MaintAI-gitops pull --ff-only origin demo/local-k3d

git -C Maint-infrastructure switch demo/local-k3d
git -C Maint-infrastructure pull --ff-only origin demo/local-k3d

# Với MaintAI: checkout commit 2df0a914 (commit lock của demo branch) hoặc main (575c8b4)
git -C MaintAI switch --detach 2df0a914287a86f3500731ba05a2ba866bbee160
```

Kiểm tra tính khớp của các file đã khóa:
```bash
python3.12 - <<'PY'
import hashlib, json
from pathlib import Path
root = Path('MaintAI')
manifest = Path('Maint-infrastructure/local/versions/app-source.lock.json')
data = json.loads(manifest.read_text())
mismatches = [
    f for f, exp in data['files'].items()
    if not (root / f).is_file() or hashlib.sha256((root / f).read_bytes()).hexdigest() != exp
]
print("Mismatched files:", len(mismatches))
if mismatches:
    for m in mismatches[:10]: print(" -", m)
else:
    print("✓ Toàn bộ source files khớp 100% với lock!")
PY
```

#### Phương án B — Cập nhật toàn bộ hệ thống lên Milestone M6 mới nhất (`4ef10de`)
Nếu muốn demo các tính năng mới của PR #23 (tra cứu lịch sử kết quả, export Parquet/CSV, giao diện History trên UI):
1. Giữ `MaintAI` ở commit mới nhất trên `main` (`4ef10de`).
2. Cập nhật `source_git_sha` và SHA256 các file thay đổi (migrations M6, schema contracts `history-m6.schema.json`, Dockerfiles, Read API) vào đồng thời hai file:
   - `Maint-infrastructure/local/versions/app-source.lock.json`
   - `MaintAI-gitops/config/local/app-source.lock.json`
3. Cập nhật `sdk_version: 0.3.1` trong `contract.json` và bổ sung wheel `maintai-storage-consumer==0.2.1` vào build receipt.
4. Commit và push lên feature branch, mở PR và merge vào cả 3 repo.

---

## 3. Cấu hình môi trường riêng Infrastructure

Chuyển vào thư mục `Maint-infrastructure`:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Tạo virtualenv nếu chưa có
test -d .venv || python3.12 -m venv .venv
.venv/bin/python -m pip install -r local/requirements.lock

# Khởi tạo thư mục private .local/ (nếu chưa có)
.venv/bin/python -m localdev init
```

Kiểm tra file cấu hình private `local/.local/settings.json`:
```json
{
  "schema_version": "1.0.0",
  "gitops_repository": "https://github.com/chiendz11/MaintAI-gitops.git",
  "gitops_revision": "demo/local-k3d",
  "gitops_checkout": "../MaintAI-gitops",
  "app_checkout": "../MaintAI",
  "machine_count": 5
}
```

> [!IMPORTANT]
> - `gitops_revision`: Phải khớp với nhánh bạn đang checkout trên `MaintAI-gitops` (`demo/local-k3d` hoặc `main`). Argo CD sẽ dùng nhánh này để pull manifests.
> - `gitops_checkout` & `app_checkout`: Đường dẫn tương đối trỏ sang hai repo anh em.
> - `machine_count`: Mặc định là `5` máy. Giữ nguyên giá trị này để đảm bảo đúng với kịch bản kiểm thử.

---

## 4. Khởi tạo k3d Cluster và Bootstrap Argo CD

### Bước 4.1 — Khởi tạo cluster và platform images
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

.venv/bin/python -m localdev up
.venv/bin/python -m localdev build-platform
.venv/bin/python -m localdev import-images --component minio --component mc --component dev-tools
.venv/bin/python -m localdev prepare platform
```
*Lưu ý:* `prepare platform` sẽ ghi nhận các image receipts vào thư mục GitOps `environments/local/platform`.

### Bước 4.2 — Review và Push Desired State của Platform
```bash
cd /home/chienqt/Dev/newBTL/MaintAI-gitops

git status --short
git diff -- environments/local/platform

# Commit và push lên nhánh được cấu hình trong settings.json (ví dụ demo/local-k3d):
git add environments/local/platform
git commit -m "chore(local): prepare platform image receipts"
git push origin $(git branch --show-current)
```

### Bước 4.3 — Bootstrap Argo CD và kiểm tra Platform
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

.venv/bin/python -m localdev bootstrap-argocd
.venv/bin/python -m localdev status
```

Kiểm tra trạng thái cluster bằng kubeconfig riêng của localdev:
```bash
export KUBECONFIG="$PWD/local/.local/kubeconfig"
kubectl --context k3d-maintai-local get applications -n argocd
kubectl --context k3d-maintai-local -n maintai-local get pods,svc,pvc
```
Khi Kafka, PostgreSQL, MinIO và Keycloak đều ở trạng thái `Running` / `Ready`, tiếp tục bước kiểm tra Kafka broker:
```bash
.venv/bin/python -m localdev smoke-kafka
```

---

## 5. Build và Import Application Images

Thực hiện trong **Maint-infrastructure**, CLI sẽ tự động đọc mã nguồn từ `app_checkout` (`../MaintAI`):

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# 1. Build các images ứng dụng cốt lõi:
.venv/bin/python -m localdev build \
  --component ingestion-api \
  --component processing-worker \
  --component processing-read-api \
  --component processing-outbox-relay \
  --component business-api \
  --component business-consumer \
  --component storage-consumer \
  --component sensor \
  --component frontend \
  --component simulator-ui

# 2. Build ML inference service:
.venv/bin/python -m localdev build --component ml-service

# 3. (Tùy chọn) Build RAG service nếu muốn demo trợ lý Copilot:
# .venv/bin/python -m localdev build --component rag-service

# 4. Import toàn bộ images vừa build vào cluster k3d:
.venv/bin/python -m localdev import-images
```

> [!NOTE]
> Khi build M6 (`processing-worker` và `processing-read-api`), builder sẽ tự động đóng gói cả internal wheel `maintai-storage-consumer==0.2.1` vào container image để phục vụ truy vấn catalog lịch sử.

---

## 6. DB Migrations, Phân quyền, Provision máy và Kích hoạt Ordering

Chỉ thực hiện sau khi PostgreSQL platform đã sẵn sàng:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Áp dụng SQL migrations qua Kubernetes owner Jobs
.venv/bin/python -m localdev migrate

# Phân quyền hạn mức truy cập database (SELECT-only cho Read API, owner grants cho Worker/Business)
.venv/bin/python -m localdev grants

# Khởi tạo thông tin 5 máy (M-001 đến M-005) vào Business DB
.venv/bin/python -m localdev provision

# Kích hoạt chế độ xử lý tuần tự (PROC-ORDER-01)
.venv/bin/python -m localdev activate-ordering --legacy-writers-drained
```

---

## 7. Chuẩn bị Dữ liệu, Model và Identity (Bắt buộc cho ML Inference)

Dữ liệu và model bundle được lưu trữ tại `local/.local/data/`:
```text
local/.local/data/
├── datasets/ai4i2020.csv
├── models/logreg-baseline-v0/
│   ├── pipeline.joblib
│   ├── metadata.json
│   ├── evaluation.json
│   └── drift_reference.json
└── fixtures/model-identity.json
```

Nếu chạy lần đầu (chưa có sẵn dữ liệu trong `.local/`):
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Chép dataset AI4I gốc từ MaintAI
mkdir -p local/.local/data/datasets
cp ../MaintAI/sensor/data/ai4i2020.csv local/.local/data/datasets/ai4i2020.csv

# Tạo model identity receipt từ OCI RepoDigest thực tế
.venv/bin/python -m localdev model-identity
```

---

## 8. (Tùy chọn) Chuẩn bị RAG/Groq Service

Nếu muốn chạy tính năng hỏi đáp Copilot với mô hình LLM qua Groq:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Nhập API key của Groq vào file bảo mật (chmod 600)
echo -n "gsk_your_groq_api_key_here" > local/.local/secrets/groq-api-key
chmod 600 local/.local/secrets/groq-api-key
.venv/bin/python -m localdev groq-secret

# Build, import và chuẩn bị knowledge embedding vào pgvector
.venv/bin/python -m localdev build --component rag-service
.venv/bin/python -m localdev import-images --component rag-service
.venv/bin/python -m localdev prepare-rag
```

---

## 9. Cấu hình Profile và Bật Workloads qua GitOps

Profile quyết định workload nào được kích hoạt kèm theo các receipts và checksum tương ứng.

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Nếu đã có file profile review từ đợt chạy trước (local/.local/reviewed-demo-profile.json),
# bạn có thể tái sử dụng trực tiếp:
PROFILE="$PWD/local/.local/reviewed-demo-profile.json"

# Nếu tạo mới, copy từ example của GitOps:
test -f "$PROFILE" || cp ../MaintAI-gitops/profiles/local.example.json "$PROFILE"

# Áp dụng profile vào GitOps manifests:
.venv/bin/python -m localdev prepare apps --profile "$PROFILE"
```

Push desired state của applications lên GitOps remote:
```bash
cd /home/chienqt/Dev/newBTL/MaintAI-gitops

git status --short
git diff -- environments/local/apps

# Commit và push lên remote branch tương ứng
git add environments/local/apps
git commit -m "chore(local): enable reviewed MaintAI demo workloads"
git push origin $(git branch --show-current)
```

Argo CD sẽ tự động đồng bộ (reconcile) các Deployment và Pods ứng dụng từ remote branch.

---

## 10. Truy cập Giao diện và Kiểm tra Hoạt động (E2E Verification)

### 10.1. Mở Port-forward
Trong các terminal riêng biệt, duy trì port-forward:

```bash
# Terminal 1: Argo CD Dashboard
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev forward argocd
# Mở trình duyệt: https://127.0.0.1:18443

# Terminal 2 (nếu cần debug DB/Kafka):
# .venv/bin/python -m localdev forward postgres  # 127.0.0.1:15432
# .venv/bin/python -m localdev forward kafka     # 127.0.0.1:19092
```

### 10.2. Danh sách các URL dịch vụ trên máy

| Dịch vụ | URL trên laptop | Thông tin truy cập / Điều kiện |
| :--- | :--- | :--- |
| **Business UI** | http://127.0.0.1:18080 | Theo dõi Telemetry, Dự đoán ML, Cảnh báo (Alerts), Lịch sử & Export |
| **Simulator UI** | http://127.0.0.1:18081 | Bảng điều khiển giả lập Sensor 5 máy (Trạng thái PAUSED / RUNNING, inject lỗi) |
| **Keycloak (IdP)** | https://127.0.0.1:18490 | Đăng nhập tài khoản demo: `technician` / `administrator` (xem trong `credentials.json`) |
| **Grafana** | http://127.0.0.1:13000 | Xem metrics Prometheus, pipeline lag, throughput |
| **MinIO Console** | http://127.0.0.1:19001 | Xem raw sensor archives và parquet exports trong bucket |
| **Argo CD UI** | https://127.0.0.1:18443 | Xem trạng thái đồng bộ GitOps (user: `admin`) |

> [!TIP]
> **Xác thực TLS/OIDC cho trình duyệt:**
> Khi đăng nhập lần đầu vào Keycloak, trình duyệt có thể cảnh báo chứng chỉ tự ký. Hãy import file chứng chỉ Root CA tại `Maint-infrastructure/local/.local/tls/ca.crt` vào trình duyệt (Authorities / Trusted Root) để đăng nhập thông suốt.

### 10.3. Checklist xác minh kịch bản Demo hoạt động:
1. **Kiểm tra trạng thái Pods:** Tất cả các Pods trong namespace `maintai-local` đều ở trạng thái `Running` (`kubectl -n maintai-local get pods`).
2. **Khởi động Sensor phát dữ liệu:**
   - Mở Simulator UI tại `http://127.0.0.1:18081`. Ban đầu cả 5 máy đều ở trạng thái **PAUSED**.
   - Bấm **Resume** trên máy `M-001` (hoặc tất cả các máy).
   - Sensor bắt đầu phát readings qua Ingestion API (nhận HTTP ACK 202) → Kafka `sensor.readings`.
3. **Quan sát Pipeline xử lý Realtime:**
   - Worker nhận readings, chạy kiểm tra DQ (Data Quality) → gọi ML inference → tính toán Risk Policy → ghi kết quả vào Processing DB/Outbox.
   - Outbox Relay đẩy kết quả sang Kafka `processing.results` / `processing.signals`.
   - Business Consumer cập nhật telemetry mới nhất và dự đoán hợp lệ lên Business DB.
   - Mở Business UI tại `http://127.0.0.1:18080` để thấy telemetry nhảy số liên tục theo thời gian thực qua Server-Sent Events (SSE).
4. **Kịch bản Kích hoạt Alert & Xử lý:**
   - Trên Simulator UI, tăng giá trị nhiệt độ hoặc rung động vượt ngưỡng trên máy `M-001` để kích hoạt trạng thái `CRITICAL`.
   - Quan sát cảnh báo mới xuất hiện trên Business UI tab **Alerts**.
   - Đăng nhập với tài khoản `technician`, thực hiện quy trình vòng đời cảnh báo: **ACK** → **START** → nhập phản hồi (feedback) → **RESOLVE**.
5. **Kịch bản Lịch sử & Export (M6):**
   - Mở chi tiết máy `M-001` trên Business UI, chuyển sang tab **History**.
   - Xem đồ thị chuỗi thời gian (SVG Chart) thể hiện các điểm dữ liệu lịch sử.
   - Chọn khung thời gian và bấm **Export CSV** (tải trực tiếp) hoặc **Export Parquet** (tạo background job).

---

## 11. Quy trình Dừng / Khởi động lại / Chạy lại (Rerun / Resume)

Khi bạn đã hoàn thành thiết lập một lần và thư mục `.local/` đã có sẵn đầy đủ cấu hình, không cần làm lại từ đầu:

### Khởi động lại cluster đã có (Resume nhanh)
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Khởi động lại k3d cluster mà không làm mất dữ liệu DB/MinIO
.venv/bin/python -m localdev start
.venv/bin/python -m localdev status

# Mở lại port-forward Argo CD
.venv/bin/python -m localdev forward argocd
```

### Tạm dừng cluster để tiết kiệm tài nguyên
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev stop
```

### Xóa toàn bộ cluster làm lại từ đầu (Chỉ khi cần thiết)
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev delete --confirm-delete-cluster
```

---

## 12. Xử lý các sự cố thường gặp (Troubleshooting)

| Vấn đề | Nguyên nhân khả dĩ | Hướng xử lý an toàn |
| :--- | :--- | :--- |
| `Source differs from reviewed lock` | Commit của `MaintAI` không khớp với `source_git_sha` trong `app-source.lock.json` | Kiểm tra xem đang chạy Phương án A (checkout `2df0a914`) hay B (cập nhật lock lên `4ef10de`). Không sửa checksum bừa bãi. |
| `bootstrap-argocd` bị từ chối | Nhánh GitOps local chưa push lên remote hoặc HEAD khác với remote branch | Commit và push toàn bộ thay đổi lên remote branch (`git push origin <branch>`), đảm bảo working tree sạch. |
| Pod `ImagePullBackOff` | Image chưa được import vào k3d cluster | Chạy lại `.venv/bin/python -m localdev import-images` để nạp các image có receipt vào k3d. |
| Telemetry trên UI không cập nhật | Sensor vẫn đang ở trạng thái `PAUSED` | Mở Simulator UI (`http://127.0.0.1:18081`) và bấm nút **Resume** để máy bắt đầu phát dữ liệu. |
| Lỗi đăng nhập Keycloak OIDC | Trình duyệt chưa tin cậy TLS CA của local cluster | Cài đặt chứng chỉ `local/.local/tls/ca.crt` vào danh sách Trusted Root Certificates của hệ điều hành/trình duyệt. |
| Lỗi `OAuth empty scope` trên Keycloak | Client credentials gửi scope rỗng | Đảm bảo GitOps và Infrastructure đang ở commit mới nhất của nhánh `demo/local-k3d` (đã có bản sửa bỏ scope rỗng). |
| Docker hết dung lượng đĩa | Caches build Docker và volumes k3d chiếm nhiều dung lượng | Chạy `docker system prune -f` để dọn dẹp ảnh không dùng; tránh xóa volume của k3d khi chưa backup. |

