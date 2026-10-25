# MaintAI messaging

Shared Python Kafka plumbing, versioned contracts, owner-local inbox/outbox and
S3 archive adapters. This package is a library, not another Deployment.

Canonical contract source: `../contracts/kafka/`. Wheels/sdists include those
files and the exact legacy schemas needed by compatibility readers.

See [the four-person development guide](../docs/KAFKA_DEVELOPMENT_GUIDE.md) for
installation, local Kafka, handler factories, transaction examples and GitOps.
Business/Processing algorithms, UI, ML, RAG and authorization middleware remain
owned by their application modules. No deployment is performed by installing this library.

## SDK0.2.0 / bundle1.1.0 — PROC-ORDER-01

- ConsumerRunner remains the serial default for Storage/Business.
- KeyedConsumerRunner is Processing-only, requires fenced acquire/release/handle,
  uses bounded per-machine FIFO/execution and poll-thread-owned Kafka APIs.
- OffsetCoordinator commits durable completed prefixes, including offset gaps;
  completed-behind-hole receipts still count toward memory/admission limits.
- KeyedTransactionalHandler separates network prepare from short fenced SQL;
  owner callbacks persist real DQ/history/risk and exactly one terminal outbox.
- OrderedOutboxRelay dispatches only topic/machine heads. Retry blocks successors;
  late physical duplicates and cross-topic reordering still require inbox/CAS.
- Migration002 and activation are explicit Processing-owner operations. Active DB
  gates reject legacy writers. Legacy source inbox needs reviewed cursor bootstrap;
  this release refuses timestamp guesses or automatic destructive reset.
- Optional OBS collectors expose queue/frontier/blocked-head metrics. Owners serve
  the registry; collection errors cannot change durable processing results.

Commands, callback API, defaults/evidence/limitations:
[implementation report](../docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md).
Wire schemas/topics/frozen001 are unchanged; bundle1.0.0 manifest is archived at
contracts/kafka/releases/1.0.0.json. Application code/images are independent of SDK.
