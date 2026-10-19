"""Local topic bootstrap only; production topic/ACL ownership stays in GitOps."""

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .contracts import ContractRegistry
from .errors import ConfigurationError


def create_local_topics(settings, *, contracts=None, client=None):
    if not settings.local_mode:
        raise ConfigurationError("Production topics must be provisioned through GitOps")
    contracts = contracts or ContractRegistry()
    client = client or AdminClient(settings.client_config())
    entries = contracts.topology["topics"]
    topics = [
        NewTopic(
            name,
            num_partitions=value["partitions"],
            replication_factor=1,
            config={
                "min.insync.replicas": "1",
                "retention.ms": str(value["retention_ms"]),
                "max.message.bytes": str(value["max_message_bytes"]),
                "cleanup.policy": "delete",
            },
        )
        for name, value in entries.items()
    ]
    futures = client.create_topics(topics, request_timeout=20)
    for future in futures.values():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    metadata = client.list_topics(timeout=10)
    for name, value in entries.items():
        topic = metadata.topics.get(name)
        if topic is None or topic.error or len(topic.partitions) != value["partitions"]:
            raise ConfigurationError(
                "Local topic topology differs; no automatic deletion or repartition"
            )
    return list(entries)
