import json

from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.ingestion import IngestionGateway
from maintai_messaging.producer import KafkaPublisher

from maintai_ingestion.config import AppSettings
from maintai_ingestion.mqtt_bridge import MqttKafkaBridge, QueuedMqttMessage
from maintai_ingestion.observability import Observability, ObservedPublisher


class FakeProducerClient:
    def __init__(self, *, fail_delivery=False):
        self.calls = []
        self.pending = []
        self.fail_delivery = fail_delivery

    def produce(self, topic, **kwargs):
        self.calls.append((topic, kwargs))
        self.pending.append((topic, kwargs))

    def poll(self, _timeout):
        while self.pending:
            topic, kwargs = self.pending.pop(0)
            kwargs["on_delivery"](
                RuntimeError("broker down") if self.fail_delivery else None,
                type(
                    "Msg",
                    (),
                    {
                        "topic": lambda s, t=topic: t,
                        "partition": lambda s: 0,
                        "offset": lambda s: 1,
                    },
                )(),
            )
        return 1

    def flush(self, _timeout):
        return 0


class FakeMqttClient:
    def __init__(self):
        self.acks = []
        self.subscribed = []

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def ack(self, mid, qos):
        self.acks.append((mid, qos))

    def disconnect(self):
        pass

    def loop_stop(self):
        pass


def mqtt_settings():
    return AppSettings.from_env(
        {
            "INGESTION_ENV": "local",
            "INGESTION_HOST": "127.0.0.1",
            "AUTH_MODE": "local-fixture",
            "LOCAL_SOURCE_SUBJECT": "mqtt-sim",
            "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
            "KAFKA_LOCAL_MODE": "true",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "BUSINESS_PUBLIC_ORIGIN": "http://business-api:8000",
            "INGESTION_MQTT_ENABLED": "true",
            "INGESTION_MQTT_HOST": "mosquitto",
            "INGESTION_MQTT_PORT": "1883",
            "INGESTION_MQTT_TOPIC_FILTER": "maintai/v1/site/local/machines/+/telemetry",
            "INGESTION_MQTT_BATCH_SIZE": "16",
            "INGESTION_MQTT_BATCH_WAIT_MS": "5",
        }
    )


def test_mqtt_settings_reject_share_subscription():
    try:
        AppSettings.from_env(
            {
                "INGESTION_ENV": "local",
                "INGESTION_HOST": "127.0.0.1",
                "AUTH_MODE": "local-fixture",
                "LOCAL_SOURCE_SUBJECT": "mqtt-sim",
                "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "BUSINESS_PUBLIC_ORIGIN": "http://business-api:8000",
                "INGESTION_MQTT_ENABLED": "true",
                "INGESTION_MQTT_TOPIC_FILTER": "$share/g/maintai/+/readings",
            }
        )
    except ValueError:
        pass
    else:
        raise AssertionError("$share topic filter must be rejected")


def test_mqtt_bridge_acks_only_after_kafka_durability():
    contracts = ContractRegistry()
    valid_payload = json.loads(
        (contracts.directory / "examples/sensor-request.json").read_text(encoding="utf-8")
    )
    raw_valid = json.dumps(valid_payload).encode()
    raw_bad = b'{"bad":'

    settings = mqtt_settings()
    obs = Observability()
    producer = ObservedPublisher(
        KafkaPublisher(
            settings.kafka, "ingestion", contracts=contracts, client=FakeProducerClient()
        ),
        obs,
    )
    gateway = IngestionGateway(producer, settings.business_origin, contracts=contracts)
    fake_mqtt = FakeMqttClient()
    bridge = MqttKafkaBridge(settings, gateway, client=fake_mqtt)
    bridge._on_connect(fake_mqtt, None, None, 0)
    assert bridge.connected
    assert fake_mqtt.subscribed == [(settings.mqtt_topic_filter, 1)]

    def msg(mid, topic, payload):
        return type("Msg", (), {"mid": mid, "qos": 1, "topic": topic, "payload": payload})()

    bridge._on_message(
        fake_mqtt, None, msg(8, "maintai/v1/site/local/machines/controller/status", b"ONLINE")
    )
    bridge._on_message(
        fake_mqtt, None, msg(9, "maintai/v1/site/local/machines/M-001/telemetry", raw_valid)
    )
    assert bridge.flush_once() == 1
    assert fake_mqtt.acks == [(8, 1), (9, 1)]
    fake_mqtt.acks.clear()

    acked = bridge.process_messages(
        [
            QueuedMqttMessage(
                10, 1, "maintai/v1/site/local/machines/M-001/readings", raw_valid, 0.0
            ),
            QueuedMqttMessage(11, 1, "maintai/v1/site/local/machines/M-001/readings", raw_bad, 0.0),
        ]
    )
    assert acked == 2
    assert fake_mqtt.acks == [(10, 1), (11, 1)]

    # Now simulate broker uncertainty/unavailability -> MUST NOT ACK MQTT!
    failing_producer = ObservedPublisher(
        KafkaPublisher(
            settings.kafka,
            "ingestion",
            contracts=contracts,
            client=FakeProducerClient(fail_delivery=True),
        ),
        obs,
    )
    bridge_fail = MqttKafkaBridge(
        settings,
        IngestionGateway(failing_producer, settings.business_origin, contracts=contracts),
        client=FakeMqttClient(),
    )
    acked_fail = bridge_fail.process_messages(
        [QueuedMqttMessage(12, 1, "maintai/v1/site/local/machines/M-001/readings", raw_valid, 0.0)]
    )
    assert acked_fail == 0
    assert bridge_fail.client.acks == []
    assert bridge_fail.deferred_count == 1
