# LOCAL-K3D-01 — chạy MaintAI trên máy dev

Ngày23/10/2026, revision1.0.0. Môi trường local được authoring riêng tại
[MaintAI-k3d](../../MaintAI-k3d/README.md), cạnh repo app/AWS/GitOps.
Không thay System Contracts v1.4, PROC-ORDER-01 hoặc production release rules.

## Phân biệt “đã có Kafka” và “đã có pipeline”

SDK0.2.0/bundle1.1.0 đã có keyed concurrency, safe offset coordinator, durable SQL
fence và ordered relay. [Implementation report](PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md)
ghi87 tests với real Kafka/PG trong harness riêng. Đây là bằng chứng thư viện và
durability adapter; không phải actual application DQ/ML/risk/history/SSE E2E.

Full pipeline còn cần Ingestion HTTP/auth/producer, Processing domain callbacks +
Read API + migrations + health/image, Business consumers/alerts/REST/SSE/RAG jobs,
ML provider/model bundle, RAG và Main UI. Hoàn tất riêng ML/RAG/UI chưa đủ.
Sensor/Storage/Simulator UI có source nhưng vẫn cần actual images, credentials,
dataset/volumes và provider integration. Drift/training chạy batch ngoài online path.

## Local profile có gì

| Hạng mục | Local profile | Giữ nguyên invariant |
| --- | --- | --- |
| Cluster | k3d5.9.0/k3s1.35.5,1 node,private kubeconfig | Không đổi AWS/production desired state |
| Kafka |4 topics ×12 partition,KRaft1 broker,replica1/minISR1 | Group độc lập,key machine_id,FIFO/safe prefix/fences |
| DB | PostgreSQL17/pgvector0.8.0,5 owner DB + Processing read role | Không query DB xuyên owner |
| Storage | MinIO versioned buckets + owner policies | Raw/quarantine khác processed history; VersionId + SHA256 |
| Monitoring | Prometheus từng Pod,Grafana native,Pushgateway | Metrics thực; series thiếu/DOWN không giả healthy |
| App images | Digest hoặc imported image-ID receipt; disabled mặc định | Không placeholder provider hoặc ACK/inference giả |
| MLflow | App-owned image, còn chờ image/version/compatibility review | Không gộp training/inference/server hoặc bỏ image lock |

Các image upstream khóa digest; MinIO/MC build từ checksum-locked release source
vì public image registry không còn truy xuất được khi authoring. Chưa build/chạy
các image local này. K3s1.35 khác cloud baseline1.33; local không thay kiểm tra AWS.

## Trình tự tích hợp

1. Theo [README local](../../MaintAI-k3d/README.md) để init/check/render offline.
2. Khi chủ động chạy: Docker → k3d up → build local platform images → bootstrap
   core/monitoring. Các bước này chưa được chạy trong lần tạo folder.
3. Owner hoàn thiện source/probes/auth/image và cấu hình app. Import actual image,
   pin source/config, thực hiện domain migrations riêng + messaging migrations và
   explicit ordered activation; xem [App integration](../../MaintAI-k3d/docs/APP_INTEGRATION.md).
4. Bật từng workload đủ điều kiện; Sensor1 replica/Recreate/PVC, PAUSED trước.
   Sensor trong Pod vẫn cần JWT, HTTPS Ingestion/issuer/JWKS và trust/DNS thực;
   anonymous loopback không áp dụng cho Pod0.0.0.0. Local profile chưa cấp IdP/TLS.
5. Resume và kiểm từ source ID/sequence đến ACK202/Kafka/ML/risk/outbox/Business/
   alert/SSE/UI, rồi crash/retry/rebalance/reconnect/load. Bảng chi tiết:
   [Pipeline readiness](../../MaintAI-k3d/docs/PIPELINE_READINESS.md).

Main UI và Simulator UI deploy riêng; Grafana/MLflow dùng UI native. Local
Simulator UI proxy `/api/` tới Sensor cùng origin; Business không proxy simulator.
Config UI không chứa client secret; token chỉ qua Authorization header.

## Giới hạn release và vận hành

Local CLI apply không mô phỏng production Argo CD/signature/PR/config3 receiver
cutover.1 broker không kiểm HA/replication. MinIO không chứng minh AWS IAM/IRSA/KMS.
Không auto-trigger training từ raw S3 upload hoặc drift; local training/Drift
scheduled jobs chưa có renderer ở profile0.1. Approved dataset → batch training →
evaluation/candidate → GitOps PR vẫn là target production.

Stop/start giữ metadata/PVC. Delete cluster mất metadata/PV bindings dù host data
còn; không tự restore bằng up. Secrets/datasets/SQLite/object versions/DB data
nằm ngoài Git trong local private state. Không đưa local plaintext Kafka, account
keys, replica1/minISR1 hoặc image-ID review booleans sang production.

Source/config/hash/schema checks và offline tooling tests không chứng minh K8s
server acceptance, Docker startup, full app E2E hay capacity. Chỉ nâng readiness
sau khi có evidence của đúng source/image/config/dataset/model/migration.

## Baseline100 máy trên local và các mức mở rộng

Giả định vận hành ban đầu100 máy×1 reading/s;5 máy là smoke/default source hiện có.
Kiến trúc scale-ready khiX>100 qua tính/đo capacity từng stage; laptop không tự được
coi là đủ CPU/RAM/IOPS cho mọiX. Chọn source rate, replicas và ML/DB pools sau khi
providers/images/auth/migrations hoàn tất; tài liệu không bật app hoặc tạo cluster.
Theo [Capacity §11](design-basis/02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100)
để tối ưu latency, đồng thời giữ source/Kafka/DB/S3/Business/UI metrics và evidence.
