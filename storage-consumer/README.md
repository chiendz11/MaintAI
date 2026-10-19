# Storage Consumer — một Deployment archive raw

Runtime `storage-consumer` có groups `raw-archive-v1` trên `sensor.readings` và
`raw-rejected-archive-v1` trên `sensor.rejected`; optional `outcome-archive-v1`
trên `inference.results`. Đầu ra JSONL.gz/manifest S3, không có business DB.
Xem [Dockerfile](Dockerfile) và [runtime.json](runtime.json).

`src/consumer/`, `batching/`, `archive/`, `checkpoints/`, `observability/` là module
của cùng runtime. Kafka offsets/checkpoints chỉ tiến sau durable idempotent batch
write. Lưu mọi accepted raw, kể cả feature-invalid; quarantine HASH_ONLY giữ hash
và metadata, không khôi phục body đã bỏ. Auth/oversize rejects không được lưu tùy tiện.

[messaging SDK](../messaging/README.md) có implementation `S3RecordArchive` và
CLI `maintai-kafka consume --role storage`; Người4 tích hợp owner entrypoint,
probes/IRSA, không copy commit logic. Raw microbatch1 receipt,2 PUTs/record cần đo
cost/capacity. Không thay Processing-owned history/curated export/cleanup. Xem
[Kafka guide §8](../docs/KAFKA_DEVELOPMENT_GUIDE.md).


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
