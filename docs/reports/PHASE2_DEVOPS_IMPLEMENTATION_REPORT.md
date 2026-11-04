# Báo Cáo Triển Khai Giai Đoạn 2: Enterprise DevOps & AWS Parity

**Ngày hoàn tất:** 04/11/2026  
**Nhánh Git:** `feat/phase-2-enterprise-devops` (đồng bộ trên cả 4 repository: `MaintAI`, `MaintAI-gitops`, `Maint-infrastructure`, `MaintAI-system-tests`)  
**Môi trường thực thi & kiểm chứng:** Cụm k3d local (`maintai-local`, Kubernetes v1.31+)

---

## 1. Tổng Quan & Mục Tiêu

Giai đoạn 2 nâng cấp hạ tầng Kubernetes của hệ thống MaintAI từ trạng thái nguyên mẫu (Baseline v1) lên chuẩn Enterprise DevOps với 4 trụ cột chính:
1. **Trụ Cột 1 (Secrets Management):** Thay thế secret tĩnh bằng LocalStack Secrets Manager kết hợp External Secrets Operator (ESO) và `ClusterSecretStore`.
2. **Trụ Cột 2 (Edge Routing):** Định tuyến Ingress-NGINX mô phỏng AWS ALB với IngressClass `alb` và 5 virtual host domain nội bộ.
3. **Trụ Cột 3 (K8s Policies & Scaling):** Triển khai HPA (HorizontalPodAutoscaler), PDB (PodDisruptionBudget) và Zero-Trust NetworkPolicies.
4. **Trụ Cột 4 (Observability Parity):** Triển khai Grafana Loki + Promtail DaemonSet cho centralized log collection kết hợp Prometheus + Grafana dual-datasources.

---

## 2. Chi Tiết Triển Khai 4 Trụ Cột

### Trụ Cột 1: Quản Lý Bí Mật Tự Động (LocalStack + ESO)
- **LocalStack Secrets Manager (`:4566`):**
  - Manifest: [`local/manifests/localstack.yaml`](file:///home/chienqt/Dev/newBTL/Maint-infrastructure/local/manifests/localstack.yaml) trong `Maint-infrastructure`.
  - Service: `localstack.localstack.svc.cluster.local:4566`.
  - Script nạp bí mật: [`local/manifests/seed_localstack_secrets.py`](file:///home/chienqt/Dev/newBTL/Maint-infrastructure/local/manifests/seed_localstack_secrets.py) tự động nạp các secret `maintai/local/database`, `maintai/local/llm`, `maintai/local/storage`, `maintai/local/auth`.
- **External Secrets Operator (ESO):**
  - Cài đặt Helm chart v2.9.0 với CRDs trong namespace `external-secrets`.
  - Cấu hình biến môi trường `AWS_ENDPOINT_URL=http://localstack.localstack.svc.cluster.local:4566` và `AWS_DEFAULT_REGION=ap-southeast-1` cho ESO Controller.
  - Manifest `ClusterSecretStore`: [`local/manifests/cluster-secret-store.yaml`](file:///home/chienqt/Dev/newBTL/Maint-infrastructure/local/manifests/cluster-secret-store.yaml).
- **GitOps ExternalSecrets:**
  - Khai báo [`environments/local/apps/external-secrets.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/external-secrets.yaml) cho 5 dịch vụ nhạy cảm: `business-api`, `processing-read-api`, `processing-worker`, `rag-service`, `storage-consumer`.
  - Toàn bộ 5 ExternalSecrets đạt trạng thái `SecretSynced: True` và `Ready: True`.

### Trụ Cột 2: Edge Routing & Virtual Host Resolution (Ingress-NGINX ALB Simulation)
- **Ingress Controller:** Cài đặt `ingress-nginx` với `controller.ingressClassResource.name=alb`, `controller.ingressClass=alb`, `controller.ingressClassResource.default=true`.
- **Virtual Hosts Routing:**
  - Manifest [`environments/local/apps/ingress.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/ingress.yaml) (và [`workloads/edge/ingress.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/workloads/edge/ingress.yaml)).
  - Danh sách routes:
    - `app.maintai.local` $\rightarrow$ `frontend:8080`
    - `simulator.maintai.local` $\rightarrow$ `simulator-ui:8080`
    - `api.maintai.local` $\rightarrow$ `business-api:8003` (kèm route SSE live stream và alert)
    - `ingest.maintai.local` $\rightarrow$ `ingestion-api:8000`
    - `processing.maintai.local` $\rightarrow$ `processing-read-api:8004`

### Trụ Cột 3: K8s Policies & Scaling (HPA, PDB & Zero-Trust NetPol)
- **HPA ([`environments/local/apps/hpa.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/hpa.yaml)):**
  - Thiết lập cho `business-api`, `processing-worker`, `ingestion-api`, `frontend`: min 1, max 4 replicas, ngưỡng CPU 70%.
  - Tích hợp với `metrics-server` đã chạy sẵn trong cụm k3d.
- **PDB ([`environments/local/apps/pdb.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/pdb.yaml)):**
  - Thiết lập `maxUnavailable: 1` cho các dịch vụ stateless cốt lõi.
- **Zero-Trust NetworkPolicies ([`environments/local/apps/network-policies.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/environments/local/apps/network-policies.yaml)):**
  - `default-deny-all`: Khóa toàn bộ Ingress/Egress mặc định trong namespace `maintai-local`.
  - `allow-system-dns`: Cho phép truy vấn DNS tới `kube-system/coredns:53` (UDP/TCP).
  - `allow-intra-namespace`: Cho phép các microservice nội bộ liên lạc với nhau.
  - `allow-ingress-nginx`: Cho phép Ingress Controller chuyển tiếp traffic từ namespace `ingress-nginx` vào các Pod `frontend`, `simulator-ui`, `business-api`, `ingestion-api`, `processing-read-api`.
  - `allow-rag-external-egress`: Cho phép `rag-service` mở kết nối ra Internet qua cổng HTTPS 443 (gọi Groq LLM API).

### Trụ Cột 4: Observability Parity (Loki + Promtail + Grafana)
- **Loki & Promtail ([`local/observability/loki-promtail.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/local/observability/loki-promtail.yaml)):**
  - Triển khai `grafana/loki:3.0.0` phục vụ cổng 3100.
  - Triển khai `grafana/promtail:3.0.0` dưới dạng DaemonSet, mount volume `/var/log/pods` và `/var/lib/docker/containers` trên k3d node để thu thập stdout/stderr của toàn bộ container.
- **Dual Datasources ([`local/observability/datasources.yaml`](file:///home/chienqt/Dev/newBTL/MaintAI-gitops/local/observability/datasources.yaml)):**
  - Cấu hình tự động nạp cả `MaintAI Prometheus` (metrics) và `MaintAI Loki` (logs) vào Grafana.

---

## 3. Bằng Chứng Kiểm Thử Tự Động (E2E Test Suite)

Toàn bộ 4 trụ cột đã được tự động hóa kiểm thử trong [`tests/e2e/test_phase2_devops.py`](file:///home/chienqt/Dev/newBTL/MaintAI-system-tests/tests/e2e/test_phase2_devops.py) thuộc repo `MaintAI-system-tests`.

### Kết quả chạy kiểm thử thực tế trên cụm live `maintai-local`:
```text
============================= test session starts ==============================
platform linux -- Python 3.14.7, pytest-9.0.3, pluggy-1.6.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /data/Dev/newBTL/MaintAI-system-tests
configfile: pytest.ini
plugins: anyio-4.14.2
collecting ... collected 9 items                                                              

tests/e2e/test_phase2_devops.py::TestPillar1SecretsManagement::test_cluster_secret_store_ready PASSED [ 11%]
tests/e2e/test_phase2_devops.py::TestPillar1SecretsManagement::test_external_secrets_synced PASSED [ 22%]
tests/e2e/test_phase2_devops.py::TestPillar2EdgeRouting::test_ingress_controller_and_class PASSED [ 33%]
tests/e2e/test_phase2_devops.py::TestPillar2EdgeRouting::test_maintai_ingress_routes PASSED [ 44%]
tests/e2e/test_phase2_devops.py::TestPillar3AutoscalingAndPolicies::test_hpa_configured PASSED [ 55%]
tests/e2e/test_phase2_devops.py::TestPillar3AutoscalingAndPolicies::test_pdb_configured PASSED [ 66%]
tests/e2e/test_phase2_devops.py::TestPillar3AutoscalingAndPolicies::test_zero_trust_network_policies PASSED [ 77%]
tests/e2e/test_phase2_devops.py::TestPillar4Observability::test_loki_and_promtail_running PASSED [ 88%]
tests/e2e/test_phase2_devops.py::TestPillar4Observability::test_loki_ready_endpoint PASSED [100%]

============================== 9 passed in 1.54s ===============================
```

### Trạng thái các Pods trên cụm k3d `maintai-local`:
- Toàn bộ 22 microservices và platform workloads trong namespace `maintai-local` đạt `100% Running` (0 restart, 0 crashloop).
- LocalStack: `1/1 Running`.
- External Secrets Operator: `3/3 Running`.
- Ingress NGINX Controller: `1/1 Running`.
- Loki + Promtail DaemonSet: `2/2 Running`.
