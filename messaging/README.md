# MaintAI messaging

Shared Python Kafka plumbing, versioned contracts, owner-local inbox/outbox and
S3 archive adapters. This package is a library, not another Deployment.

Canonical contract source: `../contracts/kafka/`. Wheels/sdists include those
files and the exact legacy schemas needed by compatibility readers.

See [the four-person development guide](../docs/KAFKA_DEVELOPMENT_GUIDE.md) for
installation, local Kafka, handler factories, transaction examples and GitOps.
Business/Processing algorithms, UI, ML, RAG and authorization middleware remain
owned by their application modules. No deployment is performed by installing this library.
