# MaintAI — Hướng dẫn Khởi động Hệ thống Demo (Cloud Production GitOps Flow)

**Cập nhật:** 01/11/2026  
**Môi trường mục tiêu:** Linux (Arch Linux / Ubuntu hoặc tương đương), Docker Engine, k3d v5.9.0, Kubernetes/kubectl (1.34–1.36), Argo CD.  
**Kiến trúc:** Chuẩn **Cloud Production GitOps** — Toàn bộ images ứng dụng được build và phân phối qua GitHub Container Registry (`ghcr.io`), Argo CD tự động đồng bộ và chạy Database PreSync Migration Hooks.

---

## 0. Tổng quan Kiến trúc Vận hành

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      GitHub Actions (Trên Cloud)                       │
 │                                                                        │
 │  MaintAI (Code nguồn & Models) ──► Build OCI Images ──► ghcr.io        │
 │                                                              │         │
 │  Tự động tính Lock SHA & Cập nhật Image Tags ──────────────┐ │         │
 │                                                            ▼ ▼         │
 │  MaintAI-gitops (Nhánh: demo/local-k3d) ─────────────────────────────┐ │
 └──────────────────────────────────────────────────────────────┬───────┼─┘
                                                                │       │
                     Laptop Linux Cá nhân                       │ Git   │ Pull
 ┌──────────────────────────────────────────────────────────────┼───────┼─┐
 │                                                              ▼       │ │
 │  k3d Cluster (maintai-local)                                         │ │
 │  ├── Argo CD ◄───────────────────────────────────────────────┘       │ │
 │  │    ├── 1. Kích hoạt PreSync Hooks (Chạy DB migrations & grants)   │ │
 │  │    └── 2. Đồng bộ Deployments Pods                                │ │
 │  └── containerd ◄────────────────────────────────────────────────────┘ │
 │       (Tự động kéo images từ ghcr.io/chiendz11/maintai-... về chạy)    │
 └────────────────────────────────────────────────────────────────────────┘
```

> [!NOTE]
> **Điểm nổi bật của phiên bản hiện tại:**
> - **Zero Local Build:** Không cần dùng Docker trên laptop để build 10 microservices, tiết kiệm tối đa RAM/CPU.
> - **Zero Manual Migration:** Không cần gõ lệnh migrate DB thủ công. Argo CD PreSync Hooks sẽ tự động chạy `maintai-processing-migrate upgrade` và `maintai-business-migrate upgrade` ngay khi đồng bộ.
> - **Đồng bộ tuyệt đối:** Cả 3 repositories (`MaintAI`, `MaintAI-gitops`, `Maint-infrastructure`) trên nhánh `demo/local-k3d` và `main` đã được khóa đồng nhất tại commit mới nhất, tích hợp đầy đủ Milestone M6 (History, Charts & Parquet Export).

---

## 1. Chuẩn bị Môi trường và Bố cục 3 Repository

### 1.1. Công cụ yêu cầu
- **Docker Engine**: Đang chạy (`docker info` thành công).
- **k3d**: Đúng bản pin **v5.9.0** (`k3d version`).
- **kubectl**: Bản client minor từ **1.34 – 1.36** (`kubectl version --client`).
- **Python**: **3.12** (`python3.12 --version`), hỗ trợ `venv`.
- **Git** và **OpenSSL**.

### 1.2. Bố cục thư mục làm việc (Sibling Directory Layout)
Ba repository phải nằm **cùng một thư mục cha** (ví dụ `/home/chienqt/Dev/newBTL/`):

```bash
cd /home/chienqt/Dev/newBTL

# Cập nhật mã nguồn mới nhất cho cả 3 repo:
git -C MaintAI pull origin main
git -C MaintAI-gitops pull origin demo/local-k3d
git -C Maint-infrastructure pull origin demo/local-k3d
```

---

## 2. Cấu hình Môi trường Hạ tầng (Infrastructure)

Chuyển vào thư mục `Maint-infrastructure`:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# 1. Tạo môi trường ảo Python nếu chưa có:
test -d .venv || python3.12 -m venv .venv
.venv/bin/python -m pip install -r local/requirements.lock

# 2. Khởi tạo cấu hình private .local/ (nếu chưa có):
.venv/bin/python -m localdev init
```

Kiểm tra file cấu hình private `local/.local/settings.json` đảm bảo đúng thông tin:
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

## 3. Khởi tạo Hạ tầng bằng Terraform (Khuyến nghị)

Chúng ta có thể khởi tạo và quản lý toàn bộ hạ tầng demo local (`k3d`, `Argo CD`, `MinIO`, `PostgreSQL`, `Kafka`, `Keycloak`) hoàn toàn bằng **Terraform code** tại thư mục [`terraform/local`](file:///home/chienqt/Dev/newBTL/Maint-infrastructure/terraform/local/):

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure/terraform/local

# 1. Khởi tạo Terraform provider (null, local)
terraform init

# 2. Khởi tạo và kích hoạt toàn bộ cụm hạ tầng bằng Terraform:
terraform apply -auto-approve
```

*(Hoặc sử dụng Makefile từ thư mục gốc `Maint-infrastructure`: `make tf-init && make tf-apply`)*

### Terraform sẽ tự động thực thi các tài nguyên sau:
1. **`null_resource.init`**: Cấu hình môi trường private, TLS certs tự ký và credentials.
2. **`null_resource.k3d_cluster`**: Khởi tạo cụm k3d `maintai-local` với đúng cấu hình cổng NodePort (18080, 18081, 18490, 13000, 19001, 18443).
3. **`null_resource.platform_bootstrap`**: Nạp công cụ nền tảng và bootstrap Argo CD vào cụm.
4. **`null_resource.provisioning`**: Chép dataset AI4I, sinh Model Identity và khởi tạo 5 máy ảo.
5. **`null_resource.argocd_sync`**: Ra lệnh cho Argo CD kích hoạt PreSync Database Migrations và kéo images GHCR về chạy.

Khi chạy xong, Terraform sẽ in ra toàn bộ bảng URL truy cập trực tiếp trên màn hình:
```text
Apply complete! Resources: 5 added, 0 changed, 0 destroyed.

Outputs:
argocd_ui_url    = "https://127.0.0.1:18443"
business_ui_url  = "http://127.0.0.1:18080"
cluster_name     = "maintai-local"
grafana_url      = "http://127.0.0.1:13000"
keycloak_url     = "https://127.0.0.1:18490"
minio_url        = "http://127.0.0.1:19001"
simulator_ui_url = "http://127.0.0.1:18081"
```

---

## 4. (Tùy chọn) Khởi tạo Thủ công từng bước bằng Python CLI

Nếu bạn muốn chạy từng bước độc lập thay vì chạy một lệnh Terraform apply duy nhất:

### Bước 4.1 — Khởi động Cụm và Nạp Công cụ Nền tảng
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

.venv/bin/python -m localdev up
.venv/bin/python -m localdev build-platform
.venv/bin/python -m localdev import-images --component minio --component mc --component dev-tools
.venv/bin/python -m localdev prepare platform
```

### Bước 4.2 — Push Platform Receipts (nếu có thay đổi)
```bash
cd /home/chienqt/Dev/newBTL/MaintAI-gitops

if [ -n "$(git status --porcelain environments/local/platform)" ]; then
  git add environments/local/platform
  git commit -m "chore(local): prepare platform image receipts"
  git push origin demo/local-k3d
fi
```

### Bước 4.3 — Cài đặt Argo CD và Kích hoạt Hệ thống
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

.venv/bin/python -m localdev bootstrap-argocd
.venv/bin/python -m localdev model-identity
.venv/bin/python -m localdev provision
.venv/bin/python -m localdev activate-ordering --legacy-writers-drained
```

---

## 5. Đồng bộ Ứng dụng qua Argo CD (Tự động Hoàn toàn)

Trên nhánh `demo/local-k3d` của `MaintAI-gitops`, toàn bộ cấu hình Deployments đã được trỏ sang các OCI images trên GHCR và tích hợp 2 Jobs PreSync.

Bạn chỉ cần kích hoạt Argo CD đồng bộ:

```bash
export KUBECONFIG="/home/chienqt/Dev/newBTL/Maint-infrastructure/local/.local/kubeconfig"

# Ra lệnh cho Argo CD đồng bộ applications ngay lập tức:
kubectl --context k3d-maintai-local -n argocd patch application maintai-local-apps \
  --type merge -p '{"operation":{"sync":{"prune":false}}}'
```

### Điều gì sẽ diễn ra tự động bên trong Cụm?
1. **PreSync Stage:** Argo CD chạy 2 Kubernetes Jobs:
   - `processing-db-migration`: Chạy `maintai-processing-migrate upgrade` (tạo các bảng `maintai_history`, `maintai_storage_catalog`...) và cấp quyền SELECT/INSERT/UPDATE cho các roles.
   - `business-db-migration`: Chạy `maintai-business-migrate upgrade` và cấp quyền cho business roles.
2. **Sync Stage:** Sau khi migrations hoàn tất thành công, Argo CD đồng loạt tạo các Deployment Pods.
3. **Pull Images:** Kubelet tự động kéo các images tương ứng (`maintai-worker`, `business-api`, `frontend`, `simulator-ui`...) từ `ghcr.io/chiendz11/...` về cụm và khởi chạy các containers lên `Running`.

Kiểm tra trạng thái toàn bộ ứng dụng:
```bash
kubectl --context k3d-maintai-local -n maintai-local get pods
```
*(Tất cả các pods ứng dụng sẽ ở trạng thái `Running` 1/1 hoặc 2/2)*

---

## 6. Mở Port-Forward và Truy cập Giao diện Demo

Trong một terminal riêng biệt, chạy lệnh port-forward:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python -m localdev forward argocd
```

### Danh mục URL Dịch vụ:

| Giao diện / Dịch vụ | URL trên laptop | Thông tin đăng nhập / Mục đích |
| :--- | :--- | :--- |
| **Business UI (Chính)** | **http://127.0.0.1:18080** | Dashboard giám sát thời gian thực, Cảnh báo (Alerts), Lịch sử M6 & Export CSV/Parquet |
| **Simulator UI** | **http://127.0.0.1:18081** | Bảng điều khiển 5 máy ảo Sensor: Bật/Tắt phát dữ liệu, inject sự cố nhiệt độ/rung động |
| **Keycloak IdP** | **https://127.0.0.1:18490** | Đăng nhập tài khoản kỹ thuật viên: `technician` / `technician123` hoặc `administrator` / `admin123` |
| **Grafana Observability**| **http://127.0.0.1:13000** | Quan sát metrics hệ thống, Kafka throughput, pipeline latency, consumer lag |
| **MinIO Console** | **http://127.0.0.1:19001** | Lưu trữ raw sensor readings và file parquet lịch sử (user/pass: `minioadmin` / `minioadmin`) |
| **Argo CD UI** | **https://127.0.0.1:18443** | Quản lý đồng bộ GitOps (user: `admin`, mật khẩu tại `local/.local/secrets/argocd-admin-password`) |

> [!TIP]
> **Xác thực TLS Keycloak:**
> Khi trình duyệt cảnh báo chứng chỉ tự ký tại cổng `18490`, chỉ cần bấm **Advanced -> Proceed to 127.0.0.1 (Tiếp tục)** hoặc import file CA tại `Maint-infrastructure/local/.local/tls/ca.crt` vào trình duyệt.

---

## 7. Kịch bản Trình diễn Demo (E2E Walkthrough)

1. **Khởi động Sensor phát Dữ liệu:**
   - Mở **Simulator UI** (`http://127.0.0.1:18081`). Ban đầu cả 5 máy đang ở trạng thái `PAUSED`.
   - Bấm nút **Resume** trên máy `M-001` (hoặc cả 5 máy).
   - Sensor bắt đầu đọc dữ liệu từ dataset AI4I và đẩy qua `Ingestion API` (HTTP 202) → Kafka `sensor.readings`.

2. **Xem Dữ liệu Thời gian Thực trên Business UI:**
   - Mở **Business UI** (`http://127.0.0.1:18080`).
   - Các chỉ số Telemetry (Nhiệt độ khí, Nhiệt độ xử lý, Tốc độ quay, Mô-men xoắn, Độ mòn dao) nhảy số liên tục qua kênh SSE (Server-Sent Events).
   - Mô hình Machine Learning đánh giá rủi ro cho từng reading và cập nhật thẻ trạng thái của máy.

3. **Mô phỏng Sự cố & Quy trình Vòng đời Cảnh báo (Alert Lifecycle):**
   - Trên **Simulator UI**, chọn máy `M-001`, tăng đột biến nhiệt độ hoặc rung động để tạo lỗi.
   - Trên **Business UI**, chuyển sang tab **Alerts**, cảnh báo mức `CRITICAL` lập tức xuất hiện.
   - Đăng nhập với tài khoản `technician`, thực hiện quy trình xử lý chuẩn công nghiệp:
     `ACK` (Tiếp nhận) ➔ `START` (Bắt đầu sửa) ➔ Nhập ghi chú phản hồi ➔ `RESOLVE` (Hoàn thành xử lý).

4. **Trình diễn Tính năng M6 — Tra cứu Lịch sử & Export Dữ liệu:**
   - Nhấp vào chi tiết máy `M-001`, chọn tab **History**.
   - Xem biểu đồ chuỗi thời gian (SVG Chart) hiển thị các thông số cảm biến trong quá khứ.
   - Thử nghiệm tính năng **Export CSV** (tải file trực tiếp về trình duyệt) hoặc **Export Parquet** (tạo job xuất dữ liệu lớn lưu trữ trên MinIO).

---

## 8. Quy trình Dừng và Khởi động lại Nhanh (Fast Resume)

Khi đã thiết lập một lần, dữ liệu đã được lưu trữ bền vững trên các ổ đĩa của cụm:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

# Tạm dừng cụm để giải phóng RAM/CPU:
.venv/bin/python -m localdev stop

# Khởi động lại nhanh cụm:
.venv/bin/python -m localdev start
.venv/bin/python -m localdev status
.venv/bin/python -m localdev forward argocd
```

### Hủy toàn bộ cụm làm lại từ đầu (Clean Reset):
- **Bằng Terraform:**
  ```bash
  cd /home/chienqt/Dev/newBTL/Maint-infrastructure/terraform/local
  terraform destroy -auto-approve
  # Hoặc: make -C ../.. tf-destroy
  ```
- **Bằng Python CLI:**
  ```bash
  cd /home/chienqt/Dev/newBTL/Maint-infrastructure
  .venv/bin/python -m localdev delete --confirm-delete-cluster
  ```

---

## 9. Xử lý Sự cố Nhanh (Troubleshooting)

| Hiện tượng | Nguyên nhân | Hướng xử lý |
| :--- | :--- | :--- |
| `Source differs from reviewed lock` | Code trên máy có thay đổi so với file lock | Chạy `git pull` ở cả 3 repo để cập nhật lockfile mới nhất. |
| Pod `ImagePullBackOff` | K3d không kết nối được internet để kéo image từ GHCR | Kiểm tra kết nối mạng của máy hoặc kiểm tra xem Docker/firewall có chặn cổng ra ngoài không. |
| Telemetry trên UI không cập nhật | Sensor vẫn đang ở trạng thái `PAUSED` | Mở Simulator UI (`http://127.0.0.1:18081`) và bấm nút **Resume**. |
| Lỗi đăng nhập Keycloak OIDC | Trình duyệt chưa cho phép chứng chỉ HTTPS cổng 18490 | Truy cập trực tiếp `https://127.0.0.1:18490`, bấm **Advanced -> Proceed** để chấp nhận chứng chỉ. |
| Port `18080` hoặc `18081` bị từ chối kết nối | NodePort service hoặc bridge container chưa ready | Kiểm tra `kubectl -n maintai-local get pods` xem pod `business-edge` và `simulator-edge` đã `Running` chưa. |
| `bootstrap-argocd` báo lỗi remote HEAD | Nhánh GitOps local chưa được push lên remote | Chạy `git -C ../MaintAI-gitops push origin demo/local-k3d`. |
