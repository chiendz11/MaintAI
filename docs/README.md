# MaintAI docs — thiết kế mục tiêu v1.4

Cập nhật: **2026-10-19**. Sáu tài liệu v1.4 đã được nhập vào repo và hiệu chỉnh
đường dẫn cùng các quyết định dự án đã chốt. Bộ Kafka Python dùng chung đã được
thêm riêng; DB/application/image/deployment chưa tự migrate theo tài liệu.

## Đọc theo thứ tự

| Tài liệu | Vai trò |
| --- | --- |
| [System Contracts v1.4](MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md) §34–39 | Nguồn chuẩn cho các yêu cầu v1.4; ưu tiên phần kế thừa |
| [Deployment Architecture](DEPLOYMENT_ARCHITECTURE.md) | Deployments/Services, DB roles, Kafka topics, quarantine và giới hạn scale |
| [Runtime Dataflow](RUNTIME_DATAFLOW.md) | Accepted/rejected, DQ/inference/alerts, history và crash boundaries |
| [Versioning & Provenance](VERSIONING_AND_PROVENANCE.md) | Dataset/split/model/image/policy/manual/release lineage, approvals và rollback |
| [Implementation Guide v1.4](MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) §42–47 | Triển khai các yêu cầu Deployment/provenance vào module |
| [Implementation Plan](IMPLEMENTATION_PLAN.md) | Thứ tự PR 00–10, migration và tiêu chí nghiệm thu cho công việc triển khai sau này |
| [Functional Overview](MaintAI_Functional_Overview.md) | Chức năng và trải nghiệm của người dùng |
| [Readiness / khoảng cách tích hợp](implementation-readiness.md) | Implementation thực tế, artifacts chưa được cung cấp và điểm phải freeze |
| [Repository layout](repository-layout.md) | Folder/runtime đang có và phần vẫn skeleton |
| [API decisions](api-contract-decisions.md) | Chi tiết HTTP của baseline máy đọc được1.3.2; chờ migration v1.4 |
| [History/storage detail](history-storage-contract.md) | Thiết kế archive/chart/export chi tiết kế thừa; chỉ áp dụng khi không trái v1.4 |
| [Recovery/jobs detail](recovery-jobs-operations.md) | Durable jobs, Drift catalog, replay/rolling; giữ các phần không bị v1.4 thay thế |
| [Machine-readable baseline](../contracts/v1.3/README.md) | Sáu OpenAPI cùng schemas/examples1.3.2 đang được validator/CI kiểm tra |
| [Kafka development guide](KAFKA_DEVELOPMENT_GUIDE.md) | SDK Python, contracts Kafka, inbox/outbox/S3, local/CI và cách tích hợp cho4 người |
| [Artifact audit](legacy-artifact-audit.md) | Tài liệu đã xóa và lý do giữ các tài liệu/code còn cần |

## Thứ tự ưu tiên

1. Yêu cầu người dùng và các quyết định dự án tại mục dưới.
2. System Contracts v1.4 §34–39, cùng các điều khoản Deployment/Dataflow/Provenance tương ứng.
3. Các chương kế thừa và Guide; đặc tả chi tiết history/recovery/API bổ sung những phần chưa bị thay thế.
4. Source, runtime metadata và machine contracts là bằng chứng trạng thái đang có,
   không tự chứng minh đã thực hiện yêu cầu v1.4.

Khi các tài liệu còn khác nhau về field/version/cursor, ghi lại trong readiness,
freeze schema/migration trước khi đổi producer hoặc consumer. Không tự sửa schema
đang được CI kiểm tra chỉ để đổi nhãn thành v1.4.

## Quyết định dự án còn hiệu lực

- **Nhóm 4 người:** Người 1 làm Backend + Main Frontend + Ingestion + toàn bộ
  Processing; Người 2 làm ML; Người 3 làm RAG; Người 4 làm Platform/MLOps,
  Sensor + Simulator UI, Drift và Storage Consumer. Kafka cluster/topics/ACL do
  Người4 quản lý. Clients/consumer loops/inbox/outbox/S3 dùng chung từ `messaging/`;
  Người1 tích hợp callbacks nghiệp vụ, Người4 cấu hình/vận hành Storage adapter.
- **Main UI → Business REST/SSE. Simulator UI riêng → Sensor Control API.**
  Business không làm simulator-control facade. Câu chữ inherited về panel control
  trong Main UI/Business proxy phải đọc theo quyết định này. Simulator UI chưa có
  source/runtime descriptor; việc thêm dòng trong sơ đồ không tạo deployment.
- Shared deployment bắt buộc auth/RBAC/machine scope; external OIDC, không thêm
  Auth Service. Local anonymous chỉ khi bật explicit cho localhost/fixtures.
  Browser dùng Authorization Code + PKCE; Sensor/Business/providers kiểm tra token
  và scope theo nhiệm vụ. Không đặt access token trong SSE query string.
- Ingestion ACK `status_url`/`Location` dùng absolute Business API URL được cấu
  hình; Ingestion không sở hữu status endpoint. Ví dụ relative URL kế thừa cần
  được chuẩn hóa khi migrate DTO/fixtures.
- Processing sở hữu DQ/inference/risk và processed history/archive; Storage sở hữu
  raw accepted/rejected archive; Business sở hữu alerts/lifecycle và latest projections.
  Không SQL xuyên DB owner. `processing-read-api` là runtime riêng cùng Processing,
  dùng DB read-only; các thao tác ghi snapshot/export phải do owner writer thực hiện.
- ML training và inference có image riêng. Approved dataset kích hoạt batch
  training; model phát hành qua PR GitOps. Drift không tự retrain/promote hay đổi risk.
- GitOps/hạ tầng nằm ở hai repo riêng; mỗi module owner tự làm tests, Dockerfile,
  probes và cấu hình workload, Người 4 giữ pipeline/môi trường chung.

## Trạng thái tích hợp

Tài liệu mục tiêu là **1.4.0**; `contracts/v1.3/` và 13 runtime descriptors vẫn là
**baseline1.3.2**, source Sensor/Drift còn implementation1.1. `contracts/kafka/`
bundle1.0.0 bổ sung rejected1.4/result1.4 cùng compatibility reader2.0 và `messaging/`
có transport/durable adapters; không phải toàn bộ implementation v1.4. Chưa có
release manifest/provider DTO migrations hoặc full app/K8s scaffold của bộ bàn
giao. Các lệnh `implementation/scripts/...`
trong Plan là ví dụ cho artifacts chưa được cung cấp, không phải lệnh repo đang chạy.
Chi tiết và khác biệt cần giải quyết ở [readiness](implementation-readiness.md).

## Provenance và tài liệu đã thay thế

Hai file System Contracts/Guide v1.3 đã được thay bằng v1.4. ADR versioned1.3.2
đã gỡ sau khi giữ các quyết định dự án ở đây và chi tiết còn cần trong các phụ lục.
Functional Overview, API/history/recovery, readiness/layout/audit vẫn còn chức năng
riêng nên được giữ và cập nhật, không phải bản sao thừa.

[source-manifest.json](source-manifest.json) giữ hash nguyên bản của sáu file
Downloads, hash bytes hiện tại và metadata các bản đã retire. File nguồn Downloads
không bị sửa. Không chạy kế hoạch triển khai trong tài liệu chỉ vì nhập docs.


## Cập nhật Ingestion M1 — 2026-10-23

Ingestion có HTTP runtime và local tests/image; HTTP Ingestion OpenAPI additive1.4.0,
các API khác giữ baseline1.3.2. Xem [runbook](../ingestion-api/README.md) và
[báo cáo local](superpowers/plans/2026-10-23-m1-verification.md). Không thay kết luận
full-system readiness hoặc nghiệm thu shared OIDC/TLS/ACL/deployment.
