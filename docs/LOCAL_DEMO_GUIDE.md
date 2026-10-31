# MaintAI — Hướng dẫn khởi động hệ thống local từ 3 repository

**Cập nhật:** 31/10/2026  
**Môi trường mục tiêu:** Linux (Arch Linux / Ubuntu hoặc tương đương), Docker Engine, k3d v5.9.0, Kubernetes/kubectl (1.34–1.36), Argo CD.  
**Phạm vi:** Local k3d cluster `maintai-local`. Tích hợp hoàn chỉnh cả hai mô hình: **Cloud Production Flow (Argo CD + GHCR)** và **Local Airgapped Flow (Docker Build tại chỗ)**.

---

> [!IMPORTANT]
> **Trạng thái tích hợp các PR trên nhánh `main` của `MaintAI`:**
> 1. **PR #22 (`fix/kafka-serial-prefetch`):** Merge tại commit `575c8b4` (nâng cấp Kafka SDK 0.3.1, sửa lỗi prefetch làm treo telemetry).
> 2. **PR #23 (`feat(history): M6 history, charts and exports`):** Merge tại commit `4ef10de` (bổ sung toàn bộ tính năng tra cứu lịch sử M6, Parquet/CSV export jobs, Storage catalog native 0.2.1 và 3 DB migrations mới).
> 3. **PR #24 (`feat(automation): sync platform source locks`):** Merge tại commit `2b34c16` (tự động băm 66 files nguồn và đồng bộ khóa `app-source.lock.json` vào 2 repo platform).
> 4. **PR #25 (`feat(ci): Cloud Production CI/CD & PreSync Hooks`):** Merge tại commit `a995109` (tự động build & publish OCI images lên GHCR `ghcr.io/chiendz11/...`, cấu hình Argo CD PreSync DB migration hooks và chuyển `imagePullPolicy: IfNotPresent`).
>
> **TRẠNG THÁI 3 REPOSITORY HIỆN TẠI:**
> - Cả 3 repositories (`MaintAI`, `MaintAI-gitops`, `Maint-infrastructure`) trên nhánh **`demo/local-k3d`** và **`main`** hiện đã được đồng bộ 100% đến phiên bản mới nhất (`a995109`).
> - Hệ thống đã sẵn sàng chạy ngay lập tức mà không gặp bất kỳ lỗi lệch pha (mismatch) nào!

---

## 0. Lựa chọn Chế độ Chạy Demo (Two Operating Modes)

Hệ thống hỗ trợ 2 chế độ vận hành:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ CHẾ ĐỘ 1: CLOUD PRODUCTION GITOPS (KHUYẾN NGHỊ — TỰ ĐỘNG HOÀN TOÀN)        │
│                                                                             │
│  [MaintAI: main] ──(GitHub Actions)──► [GHCR: ghcr.io/chiendz11/...]        │
│                                       ▲                                     │
│  [MaintAI-gitops: demo/local-k3d] ────┤ (Argo CD tự động đồng bộ)           │
│                                       │                                     │
│  [Laptop: k3d cluster] ◄──────────────┴─ PreSync: DB migrate + grants       │
│                                          Deploy: Pods tự kéo image GHCR     │
│                                                                             │
│  => KHÔNG CẦN build Docker local, KHÔNG CẦN migrate thủ công!               │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ CHẾ ĐỘ 2: LOCAL AIRGAPPED FLOW (DÀNH CHO CHẠY OFFLINE KHÔNG CÓ MẠNG)       │
│                                                                             │
│  [MaintAI source] ──(Docker Laptop)──► [localdev build & import-images]     │
│  [Maint-infrastructure] ─────────────► [localdev migrate & grants]          │
│  [MaintAI-gitops] ───────────────────► [localdev prepare apps & push]       │
│                                                                             │
│  => Build và kiểm soát 100% offline tại chỗ qua Python CLI.                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Chế độ | Ưu điểm | Khi nào nên dùng? |
| :--- | :--- | :--- |
| **Chế độ 1: Cloud Production GitOps** | Tốc độ khởi động nhanh nhất; không tốn CPU/RAM build máy; Argo CD và PreSync Hooks tự động cập nhật mọi thứ như môi trường Cloud thực thụ. | **Khuyến nghị cho tất cả các buổi demo**, kiểm thử tính năng và đánh giá đồ án. |
| **Chế độ 2: Local Airgapped Flow** | Hoạt động 100% offline, không cần kết nối internet; can thiệp sâu vào từng lệnh build/receipt. | Khi laptop không có mạng internet hoặc lập trình viên muốn sửa code và test offline ngay tại chỗ. |

---

## 1. Môi trường công cụ và bố cục thư mục

### 1.1. Yêu cầu công cụ
- **Docker Engine**: Đang chạy (`docker info` thành công; user nằm trong group `docker`).
- **k3d**: Bản pin **v5.9.0** (`k3d version`).
- **kubectl**: Bản client minor từ **1.34 – 1.36** (`kubectl version --client`).
- **Python**: **3.12** (`python3.12 --version`), hỗ trợ `venv`.
- **Git** và **OpenSSL**.
- Phần cứng khuyến nghị: Tối thiểu **8 CPU / 16 GB RAM**, còn trống ít nhất 20 GB đĩa.

### 1.2. Bố cục thư mục làm việc (Sibling Directory Layout)
Ba repository phải nằm **cùng một thư mục cha** (ví dụ `/home/chienqt/Dev/newBTL/`):

```bash
cd /home/chienqt/Dev/newBTL

# Cập nhật mã nguồn mới nhất cho cả 3 repo:
git -C MaintAI pull origin main
git -C MaintAI-gitops pull origin demo/local-k3d
git -C Maint-infrastructure pull origin demo/local-k3d
```

> [!TIP]
> Nhờ workflow tự động hóa vừa triển khai, cả 3 repo local của bạn sau khi `git pull` đã lập tức sở hữu:
> - Commit lock mới nhất (`a995109`).
> - Manifests K8s đã trỏ sẵn sang image GHCR.
> - Argo CD PreSync migration hooks đã sẵn sàng kích hoạt.

---

## 2. Cấu hình môi trường riêng Infrastructure

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

---

## 3. Khởi tạo k3d Cluster và Bootstrap Argo CD

### Bước 3.1 — Khởi tạo cụm và nạp các thành phần Platform cơ sở
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# 1. Khởi động cụm k3d local
.venv/bin/python -m localdev up

# 2. Build và nạp các công cụ platform (MinIO, MinIO Client, dev-tools adapter)
.venv/bin/python -m localdev build-platform
.venv/bin/python -m localdev import-images --component minio --component mc --component dev-tools
.venv/bin/python -m localdev prepare platform
```

### Bước 3.2 — Push Platform Receipts lên GitOps (nếu có thay đổi)
```bash
cd /home/chienqt/Dev/newBTL/MaintAI-gitops

if [ -n "$(git status --porcelain environments/local/platform)" ]; then
  git add environments/local/platform
  git commit -m "chore(local): prepare platform image receipts"
  git push origin demo/local-k3d
fi
```

### Bước 3.3 — Bootstrap Argo CD và kiểm tra Platform Ready
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Bootstrap Argo CD vào cụm k3d
.venv/bin/python -m localdev bootstrap-argocd
.venv/bin/python -m localdev status
```

Kiểm tra trạng thái cluster:
```bash
export KUBECONFIG="$PWD/local/.local/kubeconfig"
kubectl --context k3d-maintai-local get applications -n argocd
kubectl --context k3d-maintai-local -n maintai-local get pods
```
Chờ cho các dịch vụ nền tảng (**Kafka, PostgreSQL, MinIO, Keycloak**) chuyển sang trạng thái `Running` / `Ready`, sau đó chạy kiểm tra kết nối Kafka broker:
```bash
.venv/bin/python -m localdev smoke-kafka
```

---

## 4. Chuẩn bị Dữ liệu, Model AI và Khởi tạo Máy

Các workload ML và Sensor yêu cầu dữ liệu mẫu AI4I và model baseline đã qua kiểm định:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# 1. Chép dataset AI4I 2020 vào thư mục runtime dữ liệu
mkdir -p local/.local/data/datasets
cp ../MaintAI/sensor/data/ai4i2020.csv local/.local/data/datasets/ai4i2020.csv

# 2. Sinh chứng nhận Model Identity hợp lệ
.venv/bin/python -m localdev model-identity

# 3. Khởi tạo danh sách 5 máy (M-001 đến M-005) vào Business Database
.venv/bin/python -m localdev provision

# 4. Kích hoạt cơ chế xử lý dữ liệu theo thứ tự (PROC-ORDER-01)
.venv/bin/python -m localdev activate-ordering --legacy-writers-drained
```

---

## 5. KÍCH HOẠT VẬN HÀNH (Chọn 1 trong 2 Chế độ)

### >>> CÁCH 1: Chế độ Cloud Production GitOps (Khuyến nghị) <<<

Nhánh `demo/local-k3d` của `MaintAI-gitops` đã được cấu hình hoàn chỉnh:
- Trỏ toàn bộ Deployments sang image GHCR (`ghcr.io/chiendz11/maintai-...`).
- Tích hợp sẵn 2 Job PreSync: `processing-db-migration` và `business-db-migration`.
- Cấu hình `imagePullPolicy: IfNotPresent`.

**Bạn chỉ cần để Argo CD tự động đồng bộ:**
Argo CD trong k3d sẽ tự động định kỳ quét nhánh `demo/local-k3d`. Để kích hoạt sync ngay lập tức:

```bash
export KUBECONFIG="/home/chienqt/Dev/newBTL/Maint-infrastructure/local/.local/kubeconfig"

# Bắt buộc Argo CD sync ngay lập tức:
kubectl --context k3d-maintai-local -n argocd patch application maintai-local-apps \
  --type merge -p '{"operation":{"sync":{"prune":false}}}'
```

**Quá trình diễn ra tự động:**
1. **Giai đoạn PreSync:** Argo CD khởi chạy 2 Jobs `processing-db-migration` và `business-db-migration`. Hai job này tự động nâng cấp schema database lên bản M6 mới nhất và phân quyền truy cập bảng cho các roles.
2. **Giai đoạn Sync:** Khi migrations thành công (Exit 0), Argo CD đồng loạt triển khai toàn bộ các Pods (`processing-worker`, `business-api`, `frontend`, `simulator-ui`...).
3. **Kéo Image:** Kubelet tự động kéo image từ `ghcr.io` về máy và khởi động các container lên `Running`!

Kiểm tra kết quả:
```bash
kubectl --context k3d-maintai-local -n maintai-local get pods
```

---

### >>> CÁCH 2: Chế độ Local Airgapped Flow (Dành cho chạy Offline) <<<

Nếu bạn không có internet hoặc muốn tự build lại Docker images từ mã nguồn local:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# 1. Build toàn bộ application images bằng Docker trên máy:
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
  --component simulator-ui \
  --component ml-service

# 2. Nạp images từ Docker daemon vào k3d cluster:
.venv/bin/python -m localdev import-images

# 3. Chạy DB migrations và phân quyền thủ công:
.venv/bin/python -m localdev migrate
.venv/bin/python -m localdev grants

# 4. Render lại manifests GitOps với receipt local:
PROFILE="$PWD/local/.local/reviewed-demo-profile.json"
test -f "$PROFILE" || cp ../MaintAI-gitops/profiles/local.example.json "$PROFILE"
.venv/bin/python -m localdev prepare apps --profile "$PROFILE"

# 5. Commit và push manifests mới lên GitOps:
cd /home/chienqt/Dev/newBTL/MaintAI-gitops
git add environments/local/apps
git commit -m "chore(local): deploy local offline build receipts"
git push origin demo/local-k3d
```

---

## 6. (Tùy chọn) Chuẩn bị RAG Copilot Service (Groq API)

Nếu muốn trải nghiệm tính năng trợ lý ảo Copilot trả lời khuyến nghị kỹ thuật qua Groq LLM:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Nhập Groq API key (yêu cầu API key thật từ console.groq.com)
echo -n "gsk_your_groq_api_key_here" > local/.local/secrets/groq-api-key
chmod 600 local/.local/secrets/groq-api-key
.venv/bin/python -m localdev groq-secret

# Build và chuẩn bị vector knowledge base
.venv/bin/python -m localdev build --component rag-service
.venv/bin/python -m localdev import-images --component rag-service
.venv/bin/python -m localdev prepare-rag
```

---

## 7. Mở Port-Forward và Truy cập Giao diện Demo

Trong một terminal riêng, duy trì kết nối port-forward của Argo CD:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev forward argocd
```

### Danh mục URL và Tài khoản Truy cập:

| Giao diện / Dịch vụ | URL trên laptop | Tài khoản / Ghi chú |
| :--- | :--- | :--- |
| **Business UI (Chính)** | **http://127.0.0.1:18080** | Dashboard giám sát thời gian thực, Cảnh báo, Tra cứu Lịch sử (M6) & Export CSV/Parquet |
| **Simulator UI** | **http://127.0.0.1:18081** | Bảng điều khiển 5 máy ảo Sensor: Resume / Pause, inject lỗi tăng nhiệt độ/rung động |
| **Keycloak IdP** | **https://127.0.0.1:18490** | Đăng nhập tài khoản kỹ thuật viên: `technician` / `technician123` hoặc `administrator` / `admin123` |
| **Grafana Observability**| **http://127.0.0.1:13000** | Metrics hệ thống, Kafka throughput, pipeline latency, consumer lag |
| **MinIO Object Store** | **http://127.0.0.1:19001** | Lưu trữ raw sensor readings và file parquet lịch sử (user/pass: `minioadmin` / `minioadmin`) |
| **Argo CD UI** | **https://127.0.0.1:18443** | Quản lý đồng bộ GitOps (user: `admin`, password xem tại `local/.local/secrets/argocd-admin-password`) |

> [!TIP]
> **Chứng chỉ TLS / Keycloak:**
> Khi trình duyệt cảnh báo chứng chỉ tự ký tại cổng `18490`, bạn chỉ cần bấm **Advanced -> Proceed (Tiếp tục truy cập)** hoặc import chứng chỉ Root CA tại `Maint-infrastructure/local/.local/tls/ca.crt` vào trình duyệt.

---

## 8. Kịch bản Trình diễn Demo (E2E Walkthrough)

1. **Khởi động Sensor phát Telemetry:**
   - Mở **Simulator UI** (`http://127.0.0.1:18081`). Mặc định cả 5 máy đang ở trạng thái `PAUSED`.
   - Bấm **Resume** trên máy `M-001` (hoặc cả 5 máy).
   - Sensor bắt đầu đọc dữ liệu từ dataset AI4I và đẩy qua `Ingestion API` (HTTP 202) → Kafka `sensor.readings`.

2. **Xem Luồng Xử lý Realtime trên Business UI:**
   - Mở **Business UI** (`http://127.0.0.1:18080`).
   - Telemetry (Nhiệt độ khí, Nhiệt độ xử lý, Tốc độ quay, Mô-men xoắn, Độ mòn dụng cụ) nhảy số liên tục theo thời gian thực qua kênh SSE (Server-Sent Events).
   - Mô hình ML baseline đánh giá rủi ro cho từng reading và cập nhật thẻ trạng thái của máy.

3. **Mô phỏng Sự cố và Kích hoạt Cảnh báo (Alert Lifecycle):**
   - Trên **Simulator UI**, chọn máy `M-001`, tăng đột biến nhiệt độ hoặc rung động để kích thích lỗi.
   - Trên **Business UI**, chuyển sang tab **Alerts**, cảnh báo mức `CRITICAL` lập tức xuất hiện.
   - Đăng nhập tài khoản `technician`, thực hiện quy trình xử lý chuẩn công nghiệp:
     `ACK` (Tiếp nhận) ➔ `START` (Bắt đầu sửa) ➔ Nhập ghi chú phản hồi ➔ `RESOLVE` (Hoàn thành xử lý).

4. **Trình diễn Tính năng M6 — Tra cứu Lịch sử & Export (Mới nhất):**
   - Nhấp vào chi tiết máy `M-001`, chọn tab **History**.
   - Xem biểu đồ chuỗi thời gian (SVG Chart) hiển thị các thông số cảm biến trong quá khứ.
   - Thử nghiệm tính năng **Export CSV** (tải file trực tiếp về trình duyệt) hoặc **Export Parquet** (tạo job xuất dữ liệu lớn lưu trữ trên MinIO).

---

## 9. Quy trình Dừng / Khởi động lại (Fast Resume)

Khi bạn đã hoàn thành thiết lập, dữ liệu đã nằm trên PVC của cụm, không cần làm lại từ đầu:

### Tạm dừng cụm để tiết kiệm RAM/CPU:
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev stop
```

### Khởi động lại nhanh (Fast Resume):
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev start
.venv/bin/python -m localdev status

# Mở lại port-forward
.venv/bin/python -m localdev forward argocd
```

### Xóa toàn bộ cluster làm lại từ đầu (Khi muốn clean up hoàn toàn):
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev delete --confirm-delete-cluster
```

---

## 10. Bảng Xử lý Sự cố Nhanh (Troubleshooting)

| Hiện tượng | Nguyên nhân | Cách khắc phục |
| :--- | :--- | :--- |
| `Source differs from reviewed lock` | Code trên máy có thay đổi so với file lock | Chạy `git status` kiểm tra code. Nếu vừa merge PR mới, chỉ cần chạy `git pull origin demo/local-k3d` ở các repo để lấy lockfile mới nhất. |
| Pod `ImagePullBackOff` | K3d không kết nối được internet để kéo image từ GHCR | Kiểm tra kết nối mạng của laptop. Hoặc chuyển sang **Chế độ 2** (chạy `.venv/bin/python -m localdev build` và `import-images` tại chỗ). |
| Telemetry trên UI đứng yên | Sensor đang ở trạng thái `PAUSED` | Mở Simulator UI (`http://127.0.0.1:18081`) và bấm nút **Resume**. |
| Lỗi đăng nhập Keycloak OIDC | Trình duyệt chưa cho phép chứng chỉ HTTPS cổng 18490 | Truy cập trực tiếp `https://127.0.0.1:18490` trên trình duyệt một lần, bấm **Advanced -> Proceed** để chấp nhận chứng chỉ. |
| Port `18080` hoặc `18081` bị từ chối kết nối | NodePort service chưa ready hoặc edge container đang khởi động | Kiểm tra `kubectl -n maintai-local get pods` xem pod `business-edge` và `simulator-edge` đã `Running` chưa. |
| `bootstrap-argocd` báo lỗi remote HEAD | Nhánh GitOps local chưa được push lên remote | Chạy `git -C ../MaintAI-gitops push origin demo/local-k3d`. |
