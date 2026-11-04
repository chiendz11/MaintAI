# RISK-POLICY-01 — Policy thuộc app, GitOps quản lý việc triển khai

Quyết định30/10/2026. Tài liệu này chốt vị trí source và cách cấp policy cho
Processing Worker. Nó bổ sung Contract §9/25/37, không đổi Kafka wire1.4.0,
RiskPolicy DTO, ML threshold hoặc quyền sở hữu Alert của Business.

## 1. Vì sao policy nằm trong app repo?

Risk policy là quy tắc nghiệp vụ của Processing. Dev Processing cần review
ngưỡng, schema, parser và quyết định tạo alert cùng nhau. Nguồn chuẩn nằm ở:

```text
MaintAI/processing/config/risk-policies/
├── README.md
├── risk-policy.schema.json
├── manifest.json
└── demo-risk-v1.json
```

App giữ nội dung JSON và metadata policy; GitOps chỉ chọn một version/path/hash
đã được review. Infrastructure cung cấp cluster/bootstrap/storage, không giữ
một bản policy chuẩn riêng trong thư mục private state.

## 2. Phân công và nguồn chuẩn

| Phần việc | Owner/source |
| --- | --- |
| Schema, parser, tests, nội dung và ý nghĩa policy | Processing trong app repo |
| Chọn policy/version cùng app commit và hai checksum | GitOps `config/local/risk-policy.lock.json` |
| Kiểm tra policy từ checkout, tạo ConfigMap và mount Worker | GitOps local release tooling |
| Cluster, bootstrap, private credentials, model/data mounts | Infrastructure |
| Tạo, dedupe và xử lý lifecycle Alert từ signal | Business; không đọc lại policy để tính risk |

GitOps không cung cấp chỗ chỉnh ngưỡng qua env. Muốn đổi ngưỡng hoặc mức tạo
alert, tạo policy version mới trong app, review/tests, rồi cập nhật selector
GitOps. Mọi bản nội dung materialize trong GitOps đều được sinh từ source app;
không có hai bản JSON được chỉnh tay độc lập.

## 3. Policy demo và hai checksum

`demo-risk-v1` dùng các giá trị tham khảo trong Contract §9.2:
WATCH0.40, HIGH0.65, CRITICAL0.85; tạo signal ở HIGH/CRITICAL. Manifest ghi rõ
`usage=demo`, `calibration_status=illustrative_not_calibrated`. Đây không phải
policy đã được hiệu chỉnh cho artifact `logreg-baseline-v0` hoặc được duyệt
production. Scenario Critical không ép prediction/risk đạt CRITICAL.

Phân biệt:

- `sha256`: checksum bytes của JSON, bảo vệ nội dung lấy từ app commit.
- `policy_sha256`: checksum JSON canonical của **policy config**, chính là hash
  Worker ghi vào outcome. Canonical serialization: UTF-8, sort keys,
  separators `,`/`:`, ensure_ascii=false, allow_nan=false.
- `schema_sha256`: bảo vệ schema dùng để kiểm policy; catalog manifest cũng được
  khóa bởi app source lock. GitOps không sửa checksum để chấp nhận source chưa review.

Chỉ có JSON hợp lệ, đúng schema, version, checksum và thứ tự ngưỡng
`watch < high < critical` mới được materialize. Policy thay đổi phải có version
mới; không sửa âm thầm một version đã được release tham chiếu.

## 4. Luồng triển khai local

```text
Policy/schema/catalog đã commit trong app
    → GitOps selector khóa app SHA + version + checksum
    → prepare apps đọc checkout app đã xác minh
    → Kustomize ConfigMap generator chứa policy
    → GitOps diff được review/commit/push
    → Argo CD đồng bộ ConfigMap + Deployment
    → Worker đọc /local/config/risk-policy.json lúc khởi động
```

Infrastructure truyền `--app-root` khi gọi GitOps prepare. Có thể gọi trực tiếp
GitOps prepare với app root rõ ràng; tool không tự fetch branch latest và không
chạy code app để lấy policy. Nó đọc JSON/schema và kiểm app Git SHA/file hashes.

Env Worker: `PROCESSING_RISK_POLICY_FILE=/local/config/risk-policy.json`.
Mount ConfigMap read-only tại `/local/config`, không dùng hostPath cho policy.
ConfigMap generator giữ content hash trong tên; Pod reference đổi theo hash và
được annotate policy version/hash/app SHA. Worker không hot reload, nên đổi policy
đi kèm rollout. Không có Deployment mới hoặc policy HTTP service.

## 5. Profile, model identity và migration cấu hình cũ

Profile Worker chỉ khai báo `model_identity` trong nhóm `files`, với path
`/local/fixtures/model-identity.json` và checksum thật. Policy không còn là file
operator cần đặt ở `Maint-infrastructure/local/.local/data/fixtures/`.

Nếu profile cũ còn `risk_policy` trong `files`, hoặc override env
`PROCESSING_RISK_POLICY_FILE`, chuẩn bị release phải từ chối và hướng dẫn bỏ
override. Không âm thầm dùng file cũ. Sau khi chuyển, file cũ trên laptop có thể
giữ cho audit nhưng không được Worker đọc.

Model identity, model artifact, auth và image receipts vẫn là đầu vào độc lập.
Policy ConfigMap không tự hoàn tất chúng. Mọi workload mẫu giữ disabled; không
thực hiện bootstrap/sync chỉ vì policy có source hợp lệ.

## 6. Kiểm chứng và giới hạn

- App tests kiểm schema/catalog/raw hash/canonical hash và boundary decisions.
- GitOps tests kiểm selector sai, source sửa, thứ tự ngưỡng, override cũ và
  ConfigMap/Pod references. Fixture receipts chỉ dùng để kiểm generator offline.
- Infrastructure chuyển source app tới generator và tiếp tục kiểm checksum các
  artifact thực sự cần mount, như dataset/model identity/model bundle.
- Source/unit/manifest checks không chứng minh Argo sync hoặc pipeline đã chạy.
- AWS/production runtime schemas đã freeze vẫn giữ nguyên. Thay đổi lần này
  triển khai flow local; cutover production cần review desired state riêng.
