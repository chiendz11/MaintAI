# CI skeleton

Workflow: [workflows/ci.yml](workflows/ci.yml), theo
[cú pháp GitHub Actions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).

- Chạy khi push hoặc mở/cập nhật pull request; có khai báo `workflow_dispatch`
  để chạy thủ công khi workflow đã có trên default branch.
- Job `structure` kiểm tra README và 5 folder chính.
- Job `component-ci` dùng matrix cho `sensor`, `backend`, `frontend`,
  `ml-service`, `rag-service`; chỉ bật khi repository variable
  `ENABLE_COMPONENT_CI` có giá trị `true` (hiện chưa được đặt).
- Quyền workflow là `contents: read`; chưa cần secrets.

Khi có code, thêm setup runtime, cache, install dependencies, lint, tests và build
cho từng component. Có thể dùng điều kiện `matrix.component` hoặc tách thành job
riêng nếu stack khác nhau. Thay bước placeholder, sau đó đặt repository variable
`ENABLE_COMPONENT_CI=true` trong Settings → Secrets and variables → Actions → Variables
để bật job. Placeholder chủ động trả lỗi nếu bị bật khi chưa cấu hình.

CI xanh ở giai đoạn này chỉ xác nhận cấu trúc thư mục; chưa xác nhận chất lượng
code hay build. Skeleton này chưa có publish image, cập nhật GitOps hoặc deploy.
