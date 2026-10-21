# Frontend — một Deployment, Realtime UI C

Target design1.3.2: Fleet/Machines/Alerts/Monitoring, chỉ gọi Business REST/SSE.
Simulator UI riêng gọi Sensor; không thuộc Main UI. Không gọi ML/RAG/DB trực tiếp.
Xem [Dockerfile](Dockerfile), Guide §40–41 và System Contracts §32–33.

```text
src/
├── app/                         # Layout và routes
├── api/                         # Business API client
├── hooks/
├── types/
├── realtime/
│   ├── sse/                     # Subscribe, reconnect, REST resync
│   ├── reducers/                # Merge theo source event/sequence từng trục
│   └── freshness/               # LIVE/STALE và age, độc lập risk/control
├── features/
│   ├── fleet/
│   ├── machines/
│   │   ├── detail/
│   │   ├── history/
│   │   ├── live_telemetry/
│   │   ├── latest_prediction/
│   │   └── processing_status/
│   ├── alerts/
│   ├── copilot/
│   └── monitoring/
└── shared/
```

Live telemetry có thể UNVERIFIED trong khi latest valid prediction đến từ event
cũ. Mỗi panel phải có source/age riêng; INVALID mới không xóa prediction hợp lệ
lịch sử. Simulator PAUSED không bằng STALE/UNKNOWN/RESOLVED. Mở SSE và bootstrap
REST, merge có ordering guards để không ghi đè dữ liệu mới bằng event cũ.

Hiện chỉ có skeleton; chưa tạo React app, dependencies, UI components hoặc SSE
client thực thi. `history/` là view thông qua Business facade/Processing API hoặc
archive API đã chốt, không đồng nghĩa Business DB giữ full raw history.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
