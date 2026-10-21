# Ingestion API — một Deployment độc lập

Runtime `ingestion-api` được khai báo tại [runtime.json](runtime.json) và
[Dockerfile](Dockerfile). `src/`/`tests/` hiện chỉ có `.gitkeep`.

- `http_ingestion/`: HTTP routing/auth/correlation, size/rate guards.
- `envelope/`: JSON/envelope tối thiểu; feature object có thể chứa giá trị sai.
- `kafka_producer/`: điểm tích hợp shared SDK; accepted key `machine_id`, rejected key source identity.
- `observability/`: HTTP/Kafka ACK/error metrics và logging.

Target trả `202` sau Kafka ACK; feature-invalid sẽ được Processing đánh INVALID
sau đó. Không gọi ML/risk, không business DB credential, không SSE. Envelope bị
reject sau auth/size guard đi topic `sensor.rejected` HASH_ONLY và chỉ trả400/422
sau broker ACK theo target1.4. Code sender sensor hiện chưa hỗ trợ ACK
202 mới; cần migrate riêng, không suy ra service đã hoạt động từ skeleton.

Kafka/envelope/ACK implementation nằm ở [messaging/](../messaging/README.md),
`IngestionGateway` + `KafkaPublisher`. Người1 viết HTTP/auth wrapper và lifecycle,
không copy producer loop. Xem [hướng dẫn nhóm §5](../docs/KAFKA_DEVELOPMENT_GUIDE.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
