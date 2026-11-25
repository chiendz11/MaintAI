# MaintAI — Hướng dẫn Khởi động & Vận hành Cụm Demo Local (`k3d` + Terraform + Argo CD)

**Cập nhật:** 25/11/2026 (đồng bộ sau khi hoàn thiện Mosquitto MQTT QoS 1 + Spool v2, KServe/Knative KPA, Per-Key Micro-Batching & Commit Batching trên `main`)  
**Môi trường mục tiêu:** Linux (Arch Linux / Ubuntu hoặc tương đương), Docker Engine, `k3d v5.9.0`, `kubectl` (1.34–1.36), Terraform, Argo CD + Argo CD Image Updater `v1.5.0`, KServe `v0.18.1` + Knative Serving `v1.21.3` + Kourier `v1.21.1`.  
**Kiến trúc:** Chuẩn **Cloud Production GitOps** & **Pure Declarative Overlays** — Toàn bộ images ứng dụng được build và phân phối qua GitHub Container Registry (`ghcr.io`), **Argo CD Image Updater `v1.5.0`** tự mở Pull Request cập nhật tag sang `MaintAI-gitops`, `sensor` chạy MQTT QoS 1 qua **Mosquitto 2.1.4** (`ingestion-mqtt-bridge` 1 pod không `$share`), ML Champion chạy trên **KServe `InferenceService` + Knative KPA** (`minReplicas=2`, `maxReplicas=6`, `UVICORN_WORKERS=1`), giao tiếp nội bộ trực tiếp qua Kubernetes CoreDNS + TLS (`cert-manager`), và Argo CD tự động đồng bộ + chạy PreSync Migration Hooks.

> [!NOTE]
> Tài liệu này hợp nhất **Sổ tay Khởi động Demo**, **Quy trình Vận hành Vòng đời Cụm (`k3d` Stop/Start/Zero-Touch Provisioning)** và đặc tả nền tảng **`LOCAL-K3D-01`**. Để tìm hiểu sâu về kiến trúc DevOps 4-tier và kịch bản demo Rolling Update CI/CD, xem [DEVOPS_FRAMEWORK_AND_OPERATIONS_GUIDE.md](DEVOPS_FRAMEWORK_AND_OPERATIONS_GUIDE.md). Để chuẩn bị model identity và dữ liệu tri thức RAG, xem [LOCAL_DEMO_PREPARATION.md](LOCAL_DEMO_PREPARATION.md).

---

## 0. Tổng quan Kiến trúc Vận hành & Đặc tả `LOCAL-K3D-01`

```text
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      GitHub Actions (Trên Cloud)                       │
 │                                                                        │
 │  MaintAI (main) ───────────────► Build OCI Images ──► ghcr.io          │
 │                                                              │         │
 │  Argo CD Image Updater v1.5.0 mở PR cập nhật Kustomize ◄────┘         │
 │      │                                                                 │
 │      ▼                                                                 │
 │  MaintAI-gitops (Nhánh: main) ───────────────────────────────────────┐ │
 └──────────────────────────────────────────────────────────────┬───────┼─┘
                                                                │       │
                     Laptop Linux Cá nhân                       │ Git   │ Pull
 ┌──────────────────────────────────────────────────────────────┼───────┼─┐
 │                                                              ▼       │ │
 │  k3d Cluster (maintai-local)                                         │ │
 │  ├── Argo CD ◄───────────────────────────────────────────────┘       │ │
 │  │    ├── 1. Kích hoạt PreSync Hooks (Migrations, Grants, RAG Seed)  │ │
 │  │    └── 2. Đồng bộ Pure Declarative Overlays (CoreDNS + TLS)       │ │
 │  └── containerd ◄────────────────────────────────────────────────────┘ │
 │       (Tự động kéo images từ ghcr.io/chiendz11/maintai-... về chạy)    │
 └────────────────────────────────────────────────────────────────────────┘
```

### Bảng thông số nền tảng Local Profile (`LOCAL-K3D-01`)

| Hạng mục | Cấu hình Local Profile | Invariant được bảo toàn |
| :--- | :--- | :--- |
| **Cluster** | `k3d 5.9.0` / `k3s 1.35.5`, 1 server node, private kubeconfig tại `Maint-infrastructure/local/.local/kubeconfig` | Không đổi AWS/production desired state; không ghi đè `~/.kube/config` |
| **Edge MQTT & Spool** | `sensor` (`SIM_TRANSPORT_MODE=mqtt`, SQLite Spool v2 `100k` / `128 MiB`) → `mosquitto` `2.1.4` (`:1883`, PVC `1Gi` `sync-wave: '-30'`) → `ingestion-mqtt-bridge` (`1` replica, không `$share`, topic `maintai/v1/site/+/machines/+/telemetry`) | Giữ thứ tự từng máy trước khi vào Kafka; HTTP `POST /api/v1/sensor-readings` vẫn chạy song song |
| **Kafka** | 4 topics × 12 partitions, KRaft 1 broker, `replica=1 / minISR=1` (local), producer `KAFKA_LINGER_MS=2` + `lz4` | Group độc lập, key `machine_id`, FIFO/safe prefix/fences (`PROC-ORDER-01`), `PROCESSING_MAX_BATCH_PER_KEY=16`, `KAFKA_COMMIT_BATCH_SIZE=16` |
| **PostgreSQL** | PostgreSQL 17 + pgvector 0.8, 5 owner DBs (`processing_db`, `business_db`, `drift_db`, `mlflow_db`, `knowledge_db`) + Processing read role | Không query DB xuyên owner; `business-db-migration` chờ `postgres:5432` TCP sẵn sàng cho `kube-router` |
| **Object Storage** | MinIO S3-compatible với versioned buckets + scoped owner policies | Raw/quarantine tách biệt processed history; giữ `VersionId` + SHA-256 |
| **Networking & TLS** | Kubernetes CoreDNS (`postgres:5432`, `kafka:9092`, `mosquitto:1883`, `ml-service-mirror:8001`) + `cert-manager` internal certs + `*.maintai.localhost` Ingress | Main UI và Simulator UI deploy riêng; Main không proxy simulator control |
| **Monitoring** | Prometheus + Loki + Promtail + Grafana native UI | Metrics thực; thiếu series hoặc Pod `DOWN` không giả `healthy` |
| **ML & Rollout** | **KServe `v0.18.1` + Knative Serving `v1.21.3` + Kourier `v1.21.1`**: `InferenceService/ml-service-champion` (`logreg-baseline-v0`, KPA `min=2/max=6`, `scaleTarget=4`, `UVICORN_WORKERS=1`, zero-pandas batch) + `ml-service-mirror` (5% shadow sang `ml-service-shadow`) | Không gộp training/inference/server; giữ `UVICORN_WORKERS=1` tránh lệch `/metrics` đa tiến trình; chỉ bundle/image được review mới chạy |

> [!IMPORTANT]
> **Phân biệt phạm vi Local Demo và Production:**
> - **Baseline thiết kế 100 máy vs Baseline vận hành Local 50 máy vs Smoke 5 máy:** Kiến trúc thiết kế gốc đặt mục tiêu 100 máy × 1 reading/s (`scale-ready` cho $X > 100$ theo [02_ASSUMPTIONS_AND_CAPACITY.md §11](../design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100)). Trên laptop đơn nút 15–16 GiB RAM, **baseline vận hành local chuẩn là 50 máy (`M-001`..`M-050`, `SIM_MACHINE_COUNT=50`, `50 readings/s`)** với cấu hình replica và số đo đã nghiệm thu tại [LOCAL_BASELINE_50_MACHINES_REPORT.md](../reports/LOCAL_BASELINE_50_MACHINES_REPORT.md); profile tối thiểu `5` máy (`M-001`..`M-005`) giữ cho smoke nhanh.
> - **Giới hạn local & Lưu ý đĩa `/data`:** 1 broker không kiểm HA/replication; MinIO không thay AWS IAM/IRSA/KMS; secrets, TLS CA, datasets và receipts nằm trong `Maint-infrastructure/local/.local/` (ngoài Git). Không đưa PLAINTEXT Kafka, replication 1 hoặc marker `not-recorded:local-demo` lên production. k3s sẽ bật `DiskPressure=True` nếu `/data` vượt ngưỡng `85%` (`<15%` trống), vì vậy nên tạm `Pause` các máy trên Simulator UI khi không theo dõi; nếu đổi `SIM_MACHINE_COUNT` trên PVC `local-sensor-state` cũ cần tách/reset volume do guard `source_identity`.

---

## 1. Chuẩn bị Môi trường & Bố cục 3 Repository

### 1.1. Công cụ & Biến môi trường
- **Docker Engine**: Đang chạy (`docker info` thành công).
- **k3d**: Đúng bản pin **v5.9.0** (`k3d version`).
- **kubectl**: Bản client minor từ **1.34 – 1.36** (`kubectl version --client`).
- **Terraform**, **Python 3.12**, **Git**, **OpenSSL**, **jq**.

Thiết lập `KUBECONFIG` cô lập trỏ tới cụm local trước khi chạy mọi lệnh `kubectl`:

```bash
export KUBECONFIG="/home/chienqt/Dev/newBTL/Maint-infrastructure/local/.local/kubeconfig"
```

*(Có thể thêm dòng trên vào `~/.bashrc` hoặc `~/.zshrc` để dùng xuyên suốt các terminal.)*

### 1.2. Bố cục thư mục (Sibling Directory Layout)
Ba repository đặt cạnh nhau dưới cùng thư mục cha (ví dụ `/home/chienqt/Dev/newBTL/`):

```bash
cd /home/chienqt/Dev/newBTL
git -C MaintAI pull origin main
git -C MaintAI-gitops pull origin main
git -C Maint-infrastructure pull origin main
```

### 1.3. Khởi tạo môi trường Python & cấu hình private Infrastructure
Chuyển vào `Maint-infrastructure`:

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

test -d .venv || python3.12 -m venv .venv
.venv/bin/python -m pip install -r local/requirements.lock
.venv/bin/python -m localdev init
```

Kiểm tra file cấu hình private `local/.local/settings.json`:
```json
{
  "schema_version": "1.0.0",
  "gitops_repository": "https://github.com/chiendz11/MaintAI-gitops.git",
  "gitops_revision": "main",
  "gitops_checkout": "../MaintAI-gitops",
  "app_checkout": "../MaintAI",
  "machine_count": 50
}
```

---

## 2. Khởi tạo Cụm từ Đầu bằng Terraform (Zero-Touch Provisioning)

Toàn bộ hạ tầng local (`k3d`, LocalStack, ESO, Ingress-NGINX, `cert-manager`, Argo CD bootstrap) được tự động hóa bằng Terraform tại `Maint-infrastructure/terraform/local/`:

```mermaid
flowchart TD
    A["1. terraform destroy / k3d cluster delete<br/>(Tùy chọn: Xóa cụm cũ nếu làm lại từ đầu)"] --> B["2. terraform apply<br/>(Tạo k3d + LocalStack + ESO + Ingress + cert-manager + Argo CD)"]
    B --> C["3. Argo CD Reconcile (nhánh main)<br/>(Đồng bộ platform, observability, apps)"]
    C --> D["4. PreSync Migration Hooks<br/>- business-db-migration: Seed M-001..M-050 (hoặc 5 máy smoke) + Grants<br/>- processing-db-migration: Ordering Activation + History Grants<br/>- rag-knowledge-init: Cache MiniLM + Ingest PDF"]
    D --> E["5. Deployments Ready<br/>(Toàn bộ microservices & Shadow ML Healthy)"]
```

```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure/terraform/local

# 1. Khởi tạo provider (null, local)
terraform init

# 2. Khởi tạo và kích hoạt toàn bộ cụm hạ tầng:
terraform apply -auto-approve
```
*(Hoặc chạy từ thư mục gốc `Maint-infrastructure`: `make tf-init && make tf-apply`.)*

Khi chạy xong, Terraform in ra danh sách URL truy cập:
```text
Apply complete! Resources: ...
Outputs:
argocd_ui_url    = "https://127.0.0.1:18443"
business_ui_url  = "http://127.0.0.1:18080"
cluster_name     = "maintai-local"
grafana_url      = "http://127.0.0.1:13000"
keycloak_url     = "https://127.0.0.1:18490"
minio_url        = "http://127.0.0.1:19001"
simulator_ui_url = "http://127.0.0.1:18081"
```

### (Tùy chọn) Khởi tạo Thủ công từng bước bằng Python CLI
Nếu muốn tự build/import image local hoặc kiểm soát từng bước thay vì 1-click Terraform:
```bash
cd /home/chienqt/Dev/newBTL/Maint-infrastructure

.venv/bin/python -m localdev up
.venv/bin/python -m localdev bootstrap-argocd
.venv/bin/python -m localdev image-updater-pat --username chiendz11
.venv/bin/python -m localdev model-identity
.venv/bin/python -m localdev groq-secret
.venv/bin/python -m localdev prepare-rag
.venv/bin/python -m localdev provision
.venv/bin/python -m localdev activate-ordering --legacy-writers-drained
```

---

## 3. Đồng bộ Argo CD, PreSync Hooks & Theo dõi Trực tiếp

### 3.1. Kích hoạt đồng bộ (nếu cần sync ngay)
```bash
export KUBECONFIG="/home/chienqt/Dev/newBTL/Maint-infrastructure/local/.local/kubeconfig"
kubectl --context k3d-maintai-local -n argocd patch application maintai-local-apps \
  --type merge -p '{"operation":{"sync":{"prune":false}}}'
```

Khi đồng bộ, Argo CD tự động thực thi:
1. **PreSync Stage:** Chạy các Kubernetes Jobs:
   - `business-db-migration`: Chạy `maintai-business-migrate upgrade`, seed danh sách máy local (`M-001`..`M-050` cho baseline local 50 máy, hoặc `M-001`..`M-005` cho smoke) và cấp quyền cho business roles.
   - `processing-db-migration`: Chạy `maintai-processing-migrate upgrade`, kích hoạt cơ chế FIFO Ordered Processing và cấp quyền `processing_history_meta`.
   - `rag-knowledge-init`: Tải trước embedding model MiniLM vào PVC cache và index tài liệu PDF mẫu vào pgvector.
2. **Sync Stage:** Pull container images từ `ghcr.io/chiendz11/...` và rollout toàn bộ Pods (`Running`).

### 3.2. Các lệnh Live Monitoring
```bash
# 1. Theo dõi 3 ứng dụng Argo CD (Synced / Healthy):
kubectl get applications -n argocd -w

# 2. Theo dõi tiến trình Pods trong namespace maintai-local:
kubectl get pods -n maintai-local -w

# 3. Kiểm tra chi tiết PreSync Migration Hooks:
kubectl get jobs -n maintai-local
kubectl logs -n maintai-local job/business-db-migration
kubectl logs -n maintai-local job/rag-knowledge-init

# 4. Kiểm tra đồng bộ Secrets từ LocalStack qua External Secrets Operator:
kubectl get externalsecrets -n maintai-local   # Phải đạt SecretSynced: True
```

### 3.3. Script Sanity Check Toàn Diện (One-Liner)
```bash
echo "=== KIỂM TRA TRẠNG THÁI HỆ THỐNG MAINTAI ===" && \
export KUBECONFIG="/home/chienqt/Dev/newBTL/Maint-infrastructure/local/.local/kubeconfig" && \
echo "1. Argo CD Applications:" && kubectl get applications -n argocd --no-headers && \
echo "--------------------------------------------------" && \
echo "2. Số lượng Pods không ở trạng thái Running/Completed:" && \
kubectl get pods -n maintai-local --no-headers | grep -v "Running" | grep -v "Completed" | wc -l && \
echo "--------------------------------------------------" && \
echo "3. Kiểm tra Frontend UI (Port 18080):" && \
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://127.0.0.1:18080/ && \
echo "--------------------------------------------------" && \
echo "4. Kiểm tra API 5 máy mẫu:" && \
SECRET=$(kubectl get secret local-oidc-registry -n maintai-local -o jsonpath='{.data.client_secret}' | base64 -d) && \
TOKEN=$(curl -k -s -X POST "https://127.0.0.1:18490/realms/maintai/protocol/openid-connect/token" \
  -d "grant_type=client_credentials&client_id=maintai-registry&client_secret=${SECRET}" | jq -r .access_token) && \
curl -s -H "Authorization: Bearer ${TOKEN}" "http://127.0.0.1:18080/api/v1/machines" | jq '.total, .items[].machine_id'
```

---

## 4. Vòng đời Cụm (Tạm dừng / Khởi động lại / Xóa cụm)

### 4.1. Tạm dừng (Stop) và Bật lại (Start / Resume) bảo toàn 100% Dữ liệu
Dùng khi muốn giải phóng RAM/CPU nhưng giữ nguyên PostgreSQL DBs, Kafka topics, MinIO objects và Secrets trên PVC:

```bash
# Tạm dừng cụm (bảo toàn volume/SQLite state):
k3d cluster stop maintai-local
# Hoặc qua CLI: .venv/bin/python -m localdev stop

# Bật lại cụm (khoảng 15–30 giây):
k3d cluster start maintai-local
# Hoặc qua CLI: .venv/bin/python -m localdev start && .venv/bin/python -m localdev status

# Kiểm tra lại trạng thái:
kubectl get nodes && kubectl get applications -n argocd && kubectl get pods -n maintai-local

# Nếu LocalStack in-memory mất secret sau khi restart cluster (ExternalSecret báo SecretSyncedError):
cd /home/chienqt/Dev/newBTL/Maint-infrastructure
.venv/bin/python local/manifests/seed_localstack_secrets.py
.venv/bin/python -c "from localdev import history; history.seed()"
```

### 4.2. Hủy hoàn toàn cụm (Clean Reset)
```bash
# Cách 1 — Chuẩn Terraform IaC:
cd /home/chienqt/Dev/newBTL/Maint-infrastructure/terraform/local
terraform destroy -auto-approve

# Cách 2 — Qua k3d hoặc Python CLI:
k3d cluster delete maintai-local
# Hoặc: .venv/bin/python -m localdev delete --confirm-delete-cluster
```
*(Lưu ý: Dữ liệu private tại `Maint-infrastructure/local/.local/` trên host vẫn được giữ nguyên nếu không xóa thủ công.)*

---

## 5. Bảng Tra cứu URL & Mở Argo CD Dashboard

| Giao diện / Dịch vụ | URL trên laptop | Thông tin đăng nhập / Mục đích |
| :--- | :--- | :--- |
| **Business Dashboard UI** | `https://app.maintai.localhost` *(hoặc `http://127.0.0.1:18080`)* | Giám sát máy thời gian thực, Cảnh báo (Alerts), Lịch sử M6, Export CSV/Parquet & Graceful Degradation UI |
| **Simulator Control UI** | `https://simulator.maintai.localhost` *(hoặc `http://127.0.0.1:18081`)* | Điều khiển tối đa 50 máy ảo Sensor (`M-001`..`M-050` theo baseline local): Resume/Pause, đổi Scenario & inject sự cố |
| **Keycloak IdP** | `https://auth.maintai.localhost` *(hoặc `https://127.0.0.1:18490`)* | Tài khoản kỹ thuật viên: `technician` hoặc `administrator` (mật khẩu lấy ở `local/.local/credentials.json` → `.users`) |
| **Grafana Observability** | `https://grafana.maintai.localhost` *(hoặc `http://127.0.0.1:13000`)* | User `admin`, mật khẩu `.grafana_password` trong `credentials.json`; Metrics Prometheus (gồm Mosquitto/Spool/KServe), Consumer Lag Kafka & Logs Loki |
| **Prometheus Server** | `https://prometheus.maintai.localhost` *(hoặc `http://127.0.0.1:19090`)* | Truy vấn metrics time-series trực tiếp |
| **MinIO Object Console** | `https://minio-console.maintai.localhost` *(hoặc `http://127.0.0.1:19001`)* | User `maintai-root`, mật khẩu `.minio_root_password` trong `credentials.json`; S3 raw storage (`maintai-raw*`), models & exported Parquet |
| **MLflow Model Registry** | `https://mlflow.maintai.localhost` *(hoặc `http://127.0.0.1:15000`)* | Quản lý vòng đời model (`Champion` / `@shadow`) |
| **Argo CD Dashboard** | `https://127.0.0.1:18443` *(hoặc `https://localhost:8080`)* | User `admin`; mở qua port-forward bên dưới |

**Mở Port-Forward cho Argo CD:**
```bash
# Cách 1: qua localdev
cd /home/chienqt/Dev/newBTL/Maint-infrastructure && .venv/bin/python -m localdev forward argocd

# Cách 2: kubectl port-forward trực tiếp
kubectl port-forward svc/argocd-server -n argocd 8080:443
# Lấy password admin:
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d && echo
```

> [!TIP]
> **Xác thực TLS Keycloak (`:18490`):** Khi trình duyệt cảnh báo chứng chỉ tự ký, chọn **Advanced → Proceed to 127.0.0.1** hoặc import CA tại `Maint-infrastructure/local/.local/tls/ca.crt`.

---

## 6. Kịch bản Trình diễn Demo (E2E Walkthrough)

1. **Khởi động Sensor phát Dữ liệu:** Mở **Simulator UI** (`https://simulator.maintai.localhost`). Ban đầu toàn bộ máy (tối đa `50` máy `M-001`..`M-050` ở baseline local, hoặc `5` máy ở smoke) ở trạng thái `PAUSED`. Bấm **Resume** trên `M-001` (hoặc bật toàn bộ 50 máy). `sensor` ghi SQLite Spool v2 và đẩy MQTT QoS 1 (`maintai/v1/site/local/machines/{id}/telemetry`) qua `mosquitto:1883` → `ingestion-mqtt-bridge` → Kafka `sensor.readings` (song song HTTP `POST /api/v1/sensor-readings` vẫn hoạt động bình thường).
2. **Xem Telemetry & Prediction Real-time trên Business UI:** Mở **Business UI** (`https://app.maintai.localhost`). Các chỉ số (`Air temp`, `Process temp`, `Rotational speed`, `Torque`, `Tool wear`) cập nhật qua SSE; `processing-worker` (`per_key=1`, micro-batch `<=16`) gọi `ml-service-mirror` → Kourier → KServe `InferenceService/ml-service-champion` (`UVICORN_WORKERS=1`, vectorized fast path), đồng thời mirror 5% sang `ml-service-shadow`.
3. **Mô phỏng Sự cố & Vòng đời Cảnh báo (Alert Lifecycle):** Trên Simulator UI chọn `M-001` chuyển sang scenario `CRITICAL` / `DEGRADATION`. Trên Business UI mở tab **Alerts** và đăng nhập tài khoản `technician` thực hiện: `ACK` → `START` → nhập ghi chú phản hồi → `RESOLVE`.
4. **Kiểm chứng Graceful Degradation UI & Biểu đồ Xu hướng Vật lý khi AI/Processing gặp sự cố:** Trên trang chi tiết máy `M-001`, bên dưới hai panel `Live Telemetry` và `Latest Valid Prediction`, biểu đồ **Physical Feature Trends** hiển thị trực tiếp cửa sổ trượt `60` điểm gần nhất (tự warm-start 5 phút gần nhất từ M6 + cập nhật Live SSE) với đủ `8` chế độ chọn thông số (`Stacked Overview` và `7` chỉ số vật lý/dẫn xuất — bấm `📈 View Trend` trên bất kỳ ô cảm biến nào để nhảy thẳng tới thông số đó), trục tung có lưới & đơn vị, trục hoành thời gian `HH:mm:ss`, dải tô nền/nhãn ngưỡng `WARN`/`CRIT` và màu điểm theo trạng thái giúp tách bạch rõ ràng với kết quả dự báo AI hợp lệ gần nhất (`Latest Valid Prediction`).
5. **Trình diễn M6 — Tra cứu Lịch sử & Export:** Vào chi tiết máy `M-001` → tab **History** xem biểu đồ chuỗi thời gian, thử nghiệm **Export CSV** và **Export Parquet** (job bất đồng bộ tải từ MinIO).

---

## 7. Xử lý Sự cố Nhanh (Troubleshooting)

| Hiện tượng | Nguyên nhân | Hướng xử lý |
| :--- | :--- | :--- |
| Lỗi sync hoặc image mismatch | Tag image trong `kustomization.yaml` chưa tồn tại hoặc PR Image Updater chưa merge | Kiểm tra PR mở tự động trên `MaintAI-gitops` và kiểm tra workflow `publish-and-deploy.yml` trên GitHub Actions. |
| Pod `ImagePullBackOff` | Cụm k3d không kéo được image từ GHCR hoặc package bị Private | Kiểm tra kết nối mạng/firewall và trạng thái Public của GHCR package. |
| Telemetry trên UI không cập nhật | Sensor đang ở trạng thái `PAUSED` | Mở Simulator UI (`https://simulator.maintai.localhost`) và bấm **Resume**. |
| `ExternalSecret` báo `SecretSyncedError` sau restart cụm | `localstack` chạy in-memory nên mất dữ liệu sau `k3d cluster start` | Chạy lại `.venv/bin/python local/manifests/seed_localstack_secrets.py` và `localdev.history.seed()` trong `Maint-infrastructure`, rồi annotate `force-sync` cho `ExternalSecret`. |
| Lỗi đăng nhập Keycloak OIDC | Trình duyệt chưa tin cậy chứng chỉ HTTPS cổng `18490` | Truy cập trực tiếp `https://127.0.0.1:18490` chọn **Proceed** hoặc trust `ca.crt`. |
| Port `18080` / `18081` từ chối kết nối | Service/Ingress/Pod Edge chưa `Running` | Kiểm tra `kubectl -n maintai-local get pods` xem `frontend` và `simulator-ui` đã `Running` chưa. |
| `bootstrap-argocd` báo lỗi remote HEAD | Nhánh GitOps trong settings chưa push lên remote | Đảm bảo `settings.json` trỏ `gitops_revision: "main"` và đã push nhánh lên GitHub. |
