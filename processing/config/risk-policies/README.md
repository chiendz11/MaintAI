# Risk policy — source thuộc Processing

Quyết định **RISK-POLICY-01, 30/10/2026**: app repo là nguồn chuẩn cho nội dung
policy, schema, parser và tests. GitOps chọn đúng policy/version/hash từ app commit
đã khóa, tạo ConfigMap và mount read-only vào Worker. Infrastructure cung cấp
cluster/bootstrap; không quản lý một bản policy chỉnh tay trong private data.

## Các file và trạng thái

- `risk-policy.schema.json`: định dạng cấu hình1.0.0; không là Kafka event schema.
- `manifest.json`: catalog policy có version, checksum file và checksum canonical.
- `demo-risk-v1.json`: policy tham khảo cho demo. Ngưỡng0.40/0.65/0.85 kế thừa ví dụ
  trong Contract §9.2, **chưa calibration, không phải production approval**.
  Tên scenario `critical` không bảo đảm model vượt ngưỡng này.

Policy không thêm field metadata ngoài ba field mà `RiskPolicy.from_dict` đọc.
Usage/calibration status được ghi ở manifest. Parser vẫn kiểm thứ tự tăng dần
`watch < high < critical`; JSON Schema kiểm kiểu, miền giá trị và field names.

## Versioning

- `sha256` là SHA256 **bytes nguyên bản** của file policy, để kiểm nội dung lấy
  từ app commit. Đổi whitespace có thể đổi checksum này.
- `policy_sha256` là SHA256 JSON chuẩn hóa: sort keys, separators `,`/`:`, UTF-8,
  ensure_ascii=false, allow_nan=false. Phải bằng giá trị `RiskPolicy.policy_sha256`
  mà Worker ghi trong outcome. Đổi whitespace không đổi checksum canonical.
- Một thay đổi ngưỡng hoặc `create_alert_for` tạo policy version/file mới và PR;
  không âm thầm sửa version đang được một release tham chiếu.
- GitOps giữ selector chứa app Git SHA, path, version và hai checksum. ConfigMap
  được materialize từ checkout đã xác minh; không sửa giá trị policy độc lập ở GitOps.

## Runtime

GitOps local cấp `PROCESSING_RISK_POLICY_FILE=/local/config/risk-policy.json`.
ConfigMap content hash nằm trong tên và Pod reference để đổi policy tạo rollout.
Worker đọc policy lúc khởi động, không có hot reload và không tự tạo defaults.
Policy được cấp như cấu hình, không cần bake vào wheel/image hoặc gửi secret.

`model-identity.json` vẫn là input riêng. Chuyển policy sang ConfigMap không tạo
MLflow run, model identity, image receipts hoặc bật workloads. Profile mẫu vẫn tắt.

Xem [quyết định và hướng dẫn đầy đủ](../../../docs/RISK_POLICY_OWNERSHIP.md).
