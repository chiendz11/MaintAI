# MaintAI Keycloak Custom Theme Image

Source và Dockerfile cho custom image **Keycloak 26.4.0** có đóng gói sẵn login theme `maintai` (kế thừa từ `keycloak.v2` / PatternFly 5) và pre-build Quarkus qua `/opt/keycloak/bin/kc.sh build`.

## Ranh giới sở hữu

- **`MaintAI` (`platform/keycloak/`):** chứa theme `themes/maintai/login/` và `Dockerfile` multi-stage build ra image `ghcr.io/chiendz11/maintai-keycloak:sha-<commit>`.
- **`MaintAI-gitops`:** sở hữu `StatefulSet/keycloak`, `maintai-realm.json`, NetworkPolicy, Ingress/TLS và tag pin `sha-<commit>` trong `kustomization.yaml`.
- **`Maint-infrastructure`:** seed secrets cho admin Keycloak, user mẫu và OIDC clients vào LocalStack / ESO.

## Cấu trúc theme `maintai`

- `themes/maintai/login/theme.properties`: kế thừa `keycloak.v2`, nạp `css/styles.css css/maintai.css`, `darkMode=false`, `encoding=UTF-8`.
- `themes/maintai/login/footer.ftl`: hiển thị pill định danh workspace (`maintai-business-ui` / `maintai-simulator-ui`) và ghi chú OIDC + PKCE.
- `themes/maintai/login/resources/css/maintai.css`: stylesheet PatternFly 5 đồng bộ bảng màu MaintAI (`#101e2c`, `#102c3a`, `#168b75`, `#2cba94`, `#52d2b0`).
- `themes/maintai/login/resources/img/{maintai-logo,maintai-bg}.svg`: brand mark và nền telemetry vector.
- `themes/maintai/login/messages/messages_{vi,en}.properties`: bản địa hóa tiếng Việt (mặc định) và tiếng Anh.

## Build cục bộ

```bash
docker build -f platform/keycloak/Dockerfile -t maintai-keycloak:local platform/keycloak
```
