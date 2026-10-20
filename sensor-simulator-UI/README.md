# Sensor Simulator UI

Ứng dụng React/TypeScript độc lập với Business UI và Sensor backend. Source,
package/lockfile, runtime config, build và Dockerfile riêng. UI `0.1.0` dùng năm
operations của [Simulator OpenAPI baseline1.3.2](../contracts/v1.3/openapi/openapi-simulator.yaml),
theo [UI Architecture](../docs/UI_ARCHITECTURE.md). Không sửa HTTP wire schema.

## Chạy ngay giao diện demo

Yêu cầu Node.js24 trở lên và npm. Từ root MaintAI:

```bash
cd sensor-simulator-UI
npm ci --ignore-scripts
npm run dev
```

Mở **http://127.0.0.1:5174**. Mặc định `public/runtime-config.json` chọn `demo`.

- Năm máy giả lập có Pause/Resume, bốn scenario, chọn/tìm/lọc máy, nhật ký lệnh.
- Demo driver lưu control state và command receipts trong `sessionStorage` của
  tab, giữ state qua reload. Dữ liệu này không phải Sensor backend persistence.
- Preview sáu features, biểu đồ RPM và reading feed là dữ liệu tổng hợp trong
  trình duyệt. Tốc độ preview áp dụng trên mỗi máy đang RUNNING; không phải tốc
  độ Sensor process. Preview/sequence chỉ tồn tại trong phiên render hiện tại.
- Đổi scenario không tự Resume. Pause không thay alerts, backlog hoặc model.
- Không gọi Control API, Ingestion, Kafka hoặc ML ở demo. Đóng tab chỉ dừng
  browser preview; không tác động Sensor process thật.
- AI4I không chứa time series nhiều máy. Preview này không phải replay dữ liệu
  gốc, kết quả inference hay bằng chứng chất lượng model.

Thiết kế lấy bố cục từ `MaintAI_UI_Suite_v1_6_FIXED.zip`: sidebar, fleet cards,
control/scenario, preview và activity feed; assets/HTML của ZIP không được nhập
thành backend hoặc bản sao UI Grafana/MLflow.

## Kết nối Control API thật

**Sensor Control provider đã có trong `../sensor/`0.3.0.** Chạy `maintai-sensor serve`
với persistent state và auth theo [Sensor README](../sensor/README.md). UI không
điều khiển offline replay harness; provider dùng scheduler/spool độc lập browser.

Runtime config là file public tải bằng `no-store` khi mở trang. Production mount
file vào `/usr/share/nginx/html/runtime-config.json`; không cần build lại assets
để đổi URL. Mọi trường đều công khai; tuyệt đối không thêm client secret/token,
DB/S3 credentials hoặc Kafka secrets.

Ví dụ cho shared deployment dùng OIDC public SPA client:

```json
{
  "mode": "live",
  "apiBaseUrl": "",
  "pollIntervalMs": 5000,
  "requestTimeoutMs": 8000,
  "staleAfterMs": 20000,
  "auth": {
    "mode": "oidc",
    "authority": "https://identity.example.com/realms/maintai",
    "clientId": "maintai-simulator-ui",
    "scope": "openid profile simulator:control"
  },
  "links": {
    "business": "https://app.example.com",
    "grafana": "https://grafana.example.com",
    "mlflow": "https://mlflow.example.com"
  }
}
```

Các domain, client ID và scope trên chỉ minh họa. IdP/provider owner phải chốt
audience, scopes, roles và machine scope; nếu IdP dùng OAuth `audience` query
parameter, có thể thêm `auth.audience`. Server kiểm tra token trên mọi request;
frontend đăng nhập được không chứng minh có quyền điều khiển.

- Đăng ký redirect URI chính xác `https://<simulator-origin>/auth/callback` tại IdP.
  SDK dùng Authorization Code + PKCE; không dùng implicit grant/client secret.
- Access/refresh/ID tokens chỉ ở memory, không local/session storage. Session
  storage riêng namespace chỉ giữ transient PKCE state và command identities.
  Reload yêu cầu đăng nhập lại; IdP SSO có thể giữ phiên của IdP. Lệnh chưa có ACK
  được khôi phục sau khi cùng tài khoản đăng nhập lại, không tự gửi lại.
- Không auto renew ngầm. Phiên hết hạn/401 yêu cầu đăng nhập lại bằng màn hình
  Kết nối & phiên truy cập. Đăng xuất xóa phiên app, không đăng xuất mọi ứng dụng SSO.
- `apiBaseUrl: ""` dùng cùng origin; management Ingress phải route
  `/api/v1/simulator/...` tới Sensor backend. Nginx frontend không làm Business
  proxy và trả404 nếu đường `/api/` bị route nhầm tới frontend.
- Nếu dùng API origin khác, Sensor cần CORS cho origin UI, Authorization,
  Content-Type và Idempotency-Key. Production dùng HTTPS; không có token trong URL.
- Links chưa cấu hình hiển thị chưa khả dụng; không tạo URL giả hoặc truyền token
  khi mở Business/Grafana/MLflow. Mỗi ứng dụng giữ quyền truy cập riêng.
- Local development có thể đặt `auth: {"mode":"none"}` **chỉ khi cả UI và API
  đều ở loopback** và provider cũng bật chế độ local explicit. Không dùng cho
  shared environment.

Để proxy dev tới Sensor thật ở localhost, đặt trong `.env.local`:

```dotenv
SENSOR_CONTROL_PROXY_TARGET=http://127.0.0.1:8005
```

Port chỉ minh họa; dùng port provider đã cấu hình. Đặt config mode `live`,
`apiBaseUrl: ""`; khởi động lại Vite sau khi đổi proxy. Biến này chỉ ở dev server,
không được nhúng vào production bundle. Không có backend thì UI báo unavailable;
không tự chuyển sang demo.

## Các API và trạng thái lệnh

| Tác vụ          | Request                                                |
| --------------- | ------------------------------------------------------ |
| Danh sách máy   | `GET /api/v1/simulator/machines`                       |
| Đối chiếu state | `GET /api/v1/simulator/machines/{machine_id}/control`  |
| Pause           | `POST /api/v1/simulator/machines/{machine_id}/pause`   |
| Resume          | `POST /api/v1/simulator/machines/{machine_id}/resume`  |
| Scenario        | `PUT /api/v1/simulator/machines/{machine_id}/scenario` |

Mutation gửi `Idempotency-Key` + `expected_control_version`; scenario thêm
`scenario`. Bốn lựa chọn UX là normal/degradation/critical/recovery; backend vẫn
sở hữu allowlist, có thể trả422 nếu chưa hỗ trợ. Schema cho scenario là string,
UI không sửa schema để ép provider chấp nhận các kịch bản.

1. Lưu command identity/body trước HTTP, khóa các lệnh mới cho cùng máy.
2. `200` đúng DTO/target mới hiển thị Đã xác nhận; không optimistic Pause.
3. `409`: GET reconcile, không tự gửi ý định mới với version khác.
4. Timeout/network/5xx: Chưa rõ kết quả; GET cập nhật state nhưng **không chứng
   minh command cụ thể đã thành công**. Retry do operator chọn giữ đúng key/body.
5. Reload khôi phục lệnh pending thành unknown trong tab/cùng tài khoản/API.
   Xóa log hiển thị không xóa unresolved commands.
6. `401/403`: không tự retry mutation; yêu cầu đăng nhập/quyền phù hợp.

List polling có single-flight, exponential backoff tối đa60s, Retry-After cho
429 tối đa300s, dừng nền khi tab ẩn và resync khi online/visible. Không polling
mutations. Control state merge theo `control_version`, không rollback vì poll cũ.
Freshness dựa lần đọc thành công, không dựa `updated_at` (máy PAUSED có thể không
đổi state lâu nhưng vẫn đọc được). Snapshot `as_of` quá cũ/clock lệch bị từ chối.

Baseline chưa có rate/counters/queue/preview/SSE/batch controls. Live không gọi
endpoint tự phát minh, không phát sensor readings và không lấy telemetry từ
Business. Các panel thiếu nguồn đọc hiển thị unavailable thay số liệu demo.
Control panel hiển thị thời gian và `last_controlled_by` từ provider.
Nhật ký UI là hỗ trợ operator; không thay audit log bền vững của Sensor.

## Kiểm tra và build

```bash
npm run check
npm test
npm run build
npx playwright install chromium
npm run test:e2e
```

Unit tests đọc JSON Schemas/fixture thật từ `../contracts/v1.3/`, kiểm tra wire,
ordering và recovery. Browser tests chạy Chromium trên production build, gồm
demo, contract-mocked live,409/403/unknown/reload, mobile/keyboard và OIDC login
gate. Suite còn có browser integration gọi Sensor provider thật qua HTTP: Pause/Resume,
Recovery, actor và reload; Ingestion là fixture202/local auth. Python tests kiểm
TLS JWKS/OAuth với fixture ký RSA. Chưa có production IdP/browser token exchange,
Kafka E2E hoặc kiểm thử Docker. Browser suite cần Python + Sensor runtime deps
(`../.venv/bin/python` hoặc `MAINTAI_SENSOR_PYTHON`); CI tự cài lockfile Sensor.
CI có job `simulator-ui-ci` bắt buộc qua `CI / required`.

`dist/` là artifact static. Version app, HTTP schema, Sensor image và model
version độc lập. Lockfile và package versions được quản lý trong Git.

## Container độc lập

Dockerfile hai stage build assets rồi phục vụ Nginx unprivileged ở8080. Hai base
images phải được owner chọn/pin bằng OCI digest trước khi build:

```bash
docker build \
  --build-arg NODE_IMAGE="$MAINTAI_NODE_IMAGE" \
  --build-arg NGINX_IMAGE="$MAINTAI_NGINX_IMAGE" \
  --tag maintai-simulator-ui:0.1.0 sensor-simulator-UI
```

Lệnh Docker chạy từ root MaintAI; biến phải chứa `image@sha256:...` thật của
Node24+ và Nginx unprivileged Alpine (UID101). Không có digest mặc định giả.
CI dùng [ci-images.json](ci-images.json) để cấp base digests và kiểm container
Nginx riêng (non-root/read-only, probes/config/assets/API404). Đây là inputs CI;
platform vẫn chọn/pin base và output image cho release.
Artifact release phải pin digest image đầu ra, source SHA và runtime config;
không bump model chỉ vì sửa UI. Mount runtime config read-only, cung cấp `/tmp`
writable nếu dùng read-only filesystem; probes ở `/health/live`, `/health/ready`
chỉ kiểm tra static server, không kiểm tra Sensor engine.

Nginx cache hashed assets immutable, HTML no-cache và runtime config no-store.
Auth callback dùng SPA fallback. Production CSP cho kết nối same-origin/HTTPS;
platform có thể thu hẹp allowlist theo IdP thực tế. Deployment/Ingress, TLS,
secrets, GitOps release và rollout vẫn thuộc hai repo platform, chưa chạy trong
lượt implement frontend.

Tham khảo chính thức: [Vite](https://vite.dev/guide/),
[oidc-client-ts](https://authts.github.io/oidc-client-ts/),
[Playwright webServer](https://playwright.dev/docs/test-webserver).
