# Main UI — Business REST và realtime SSE

React frontend độc lập cho Fleet và Machine Detail. Browser chỉ gọi Business REST/SSE;
không gọi Sensor Control, ML/RAG, Kafka hoặc database trực tiếp. Giao diện giữ ba vùng
Live Telemetry, Latest Valid Prediction và Latest Processing với source/age riêng.

## Cài đặt và chạy

Dùng Node24 trở lên và npm cùng lockfile. Trình duyệt cần BigInt, fetch streaming,
AbortSignal.any và JSON.parse có source context. App kiểm tra khả năng đọc số nguyên
chính xác trước khi khởi động; trình duyệt thiếu hỗ trợ sẽ báo cần cập nhật.

```bash
cd frontend
npm ci --ignore-scripts
npm run dev
```

Dev bind localhost5173, proxy `/api` tới Business ở localhost8003. Đổi origin proxy
bằng `MAINTAI_BUSINESS_PROXY_ORIGIN`; đây là cấu hình dev/preview phía server, không
là credential hoặc service token trong browser.

```bash
npm run check
npm test
npm run build
npm run preview
npm run test:e2e
```

Preview dùng localhost4173. Browser checks cần Chromium đã cài bằng Playwright.
Các lệnh dev/check/test/build tự sinh standalone validators từ schemas đã khóa
vào cache trong `node_modules/`. Trình duyệt chỉ chạy validators đã biên dịch;
không cần `unsafe-eval` trong Content Security Policy.

## Runtime config

`public/runtime-config.json` được copy vào output và đọc với no-store khi mở app.
Có thể mount file riêng vào `/usr/share/nginx/html/runtime-config.json` khi chạy image.
Mặc định là demo, có nhãn “Dữ liệu mô phỏng”; live không tự fallback sang demo.

```json
{
  "mode": "live",
  "apiBaseUrl": "/api/v1",
  "freshMs": 2000,
  "staleMs": 5000,
  "auth": {
    "authority": "https://issuer.example.invalid",
    "clientId": "business-ui",
    "scope": "openid profile",
    "audience": "business-api"
  }
}
```

Giá trị IdP/client/audience trên là ví dụ cấu hình, không phải registry đang tồn tại.
Public client dùng Authorization Code + PKCE, token chỉ trong memory; sessionStorage
chỉ giữ transient state/PKCE. Main UI dùng public client riêng, không client secret
hoặc credential Business→Processing. Live không có anonymous profile. HTTP issuer
chỉ được phép khi cả UI và issuer ở loopback cho local tests.

Business API phải nằm ở đường dẫn cùng origin. Shared ingress định tuyến `/api` tới
Business; static frontend không làm Business proxy. Cần tắt proxy buffering SSE,
đặt idle timeout lớn hơn heartbeat15s và truyền Authorization. `/api` không được
fallback thành HTML của SPA. Các cấu hình production này thuộc GitOps.

## Màn hình và trạng thái

- `/machines`: Fleet, state filter, pagination và nhập ID máy.
- `/machines/{encodedMachineId}`: Detail, metadata và ba panel `/live` độc lập.
- `/alerts`, `/alerts/{alert_id}`: danh sách/filter, evidence/timeline, lifecycle và feedback.
- `/auth/callback`: OIDC callback, sau đó trở lại danh sách.

Fleet chỉ dùng fields API hiện có. Không tính total toàn fleet từ một trang, không
tự suy risk hoặc operational state từ probability/raw data. M5 đã nối nguồn Alert thật ở backend. Khi dependency không khả dụng,
Fleet/Detail có thể503; UI báo unavailable, vẫn cho mở máy bằng ID để đọc `/live`.
Metadata lỗi không che các panel live; không tạo alert count0 hoặc alerts[] giả.

INVALID/FAILED không xóa prediction hợp lệ lịch sử. Card prediction luôn ghi source
event và source age; trạng thái model lịch sử không phải kết luận cho telemetry mới.
Telemetry age dùng server_accepted_at, prediction age dùng source_server_accepted_at.
Ngưỡng2s/5s là cấu hình hiển thị minh họa, không phải SLO đã benchmark.
Telemetry hết ngưỡng fresh mang nhãn Last Telemetry cùng trạng thái DELAYED/STALE.

SSE mở trước REST bootstrap; events được buffer rồi merge bằng cursors riêng.
Reconnect dùng backoff và REST resync; REST cũng refresh30s khi trang hiển thị
và khi tab hoạt động lại. Không coi Last-Event-ID là durable replay.
Admission429 là lỗi tạm thời và được thử lại với backoff. Với trạng thái xử lý
nonterminal thay đổi cùng cursor, REST snapshot quyết định giá trị hiện tại;
không xếp hạng PROCESSING/RETRYING hoặc sắp theo timestamp.

Sequence được đọc từ numeric lexeme và giữ dạng BigInt nội bộ; raw numeric evidence
không bị làm tròn khi hiển thị. REST/frame/bootstrap buffer có budget2MiB. REST
timeout10s; stream không nhận data/heartbeat45s thì reconnect. Lỗi401 xóa màn hình
dữ liệu và yêu cầu đăng nhập lại; đổi máy/logout abort requests và stream cũ.

ID có slash/Unicode phải encode đúng một lần. Dot-only IDs `.` và `..` chưa dùng
được từ browser vì WHATWG URL chuẩn hóa chúng; UI chặn trước request thay vì gửi
sang resource sai. Giải pháp interface cho trường hợp này cần review riêng.

## Build image

Build context là repository root vì frontend validate schemas từ contracts chung:

```bash
docker build -f frontend/Dockerfile -t maintai-frontend:local .
```

Node/Nginx base dùng digest đã pin trong repo. Image chạy UID101 trên8080, có
`/health/live`, `/health/ready`, static assets có hash và runtime config no-store.
Main UI và Simulator có source/image/config/release riêng. Image không tự deploy.

## Tests và giới hạn

Unit tests kiểm DTO, numeric precision, reducer, parser, auth failures và cleanup.
Browser demo tests kiểm screens/routes/mobile. Live browser fixture bật bằng
`MAINTAI_M4_INTEGRATION=1`, dùng PostgreSQL/Kafka localhost đã khởi động riêng và
`MAINTAI_M3_DATABASE_URL` trỏ database test. Không trỏ môi trường production.

Fixture tạo hai owner databases, least-privilege Business roles, hai API replicas
và Consumer; PKCE issuer/JWT ký thật, model/provenance/policy là fixtures. Gateway
dùng SDK gateway; không chạy outer Ingestion HTTP trong case browser mới. Tests
chứng minh Consumer/SSE/browser với DB/Kafka thật, không thay shared provider acceptance.
Playwright dừng fixture bằng SIGTERM để cleanup databases/roles/processes.

M5 đã có Alert lifecycle, timeline, feedback và nguồn Alert. History/export,
Copilot hoặc Drift screens còn thuộc M6/M7; shared OIDC/TLS/ACL/ingress/provider
thật cần nghiệm thu riêng. Tests hoặc
screenshot không chứng minh throughput100máy, production rollout hoặc toàn hệ thống hoàn tất.

Thiết kế: [Frontend guide](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md),
[UI Architecture](../docs/UI_ARCHITECTURE.md), [Business SSE](../backend/README.md).

## M5 — Cảnh báo và xử lý lỗi mạng

Live chỉ gọi Business REST/SSE. Menu Cảnh báo có list/filter/pagination và detail
giữ evidence gốc, model/policy/risk, timeline, Copilot UNAVAILABLE, actions và
feedback. Session lấy subject cùng role hints từ ID token; đổi tên roles claim
bằng public config `auth.rolesClaim` (mặc định roles). Backend vẫn kiểm quyền,
machine scope và actor_id. VIEWER không có form mutation.

Actions chỉ hiển thị transition hợp lệ, resolve yêu cầu resolution, false-alert
yêu cầu reason. Không optimistic báo success; request pending khóa nút. 409 tải
lại detail. Mất phản hồi POST không tự retry: action đối soát qua detail/timeline;
feedback báo kết quả chưa xác định vì AlertDetail không liệt kê feedback.

Alert SSE chỉ invalidation: tải lại REST, không xếp trạng thái bằng timestamp/ID.
Fleet, Detail metadata và Alerts tự refresh khi có signal, reconnect, tab resume
và mỗi30s khi visible; response cũ bị hủy/bỏ. Trên màn hình máy dùng stream M4
kèm Alert events; các màn hình còn lại dùng một stream chung theo scope.

Demo có lifecycle/feedback riêng và nhãn mô phỏng; live không fallback sang demo.
Browser M5 bật MAINTAI_M4_INTEGRATION=1 và MAINTAI_M5_INTEGRATION=1, dùng HTTP
Ingestion thật, Kafka Worker/Relay và hai API replicas; ML/policy/registry/OIDC
là fixtures. MAINTAI_TEST_PYTHON chọn interpreter của harness khi chạy local.
Các job CI Người1 đã cài Ingestion và bật flags M5; không tự publish image hoặc deploy.
