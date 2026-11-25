"""Unit tests for Sensor MQTT QoS 1 transport and SQLite Spool v2 hardening."""

import sqlite3

import paho.mqtt.client as mqtt
import pytest

from simulator.client import MqttTransport
from simulator.fault_injection import FaultMode
from simulator.ordering.store import StateStore
from simulator.scheduling.engine import SensorEngine


class _FakePublishInfo:
    def __init__(self, *, rc=mqtt.MQTT_ERR_SUCCESS, published=True):
        self.rc = rc
        self._published = published
        self.waited = False

    def wait_for_publish(self, timeout=None):
        self.waited = True

    def is_published(self):
        return self._published


class _FakeMqttClient:
    def __init__(self, *, published=True, rc=mqtt.MQTT_ERR_SUCCESS):
        self.published_calls = []
        self.will = None
        self.connected = False
        self.disconnected = False
        self.loop_started = False
        self.loop_stopped = False
        self.on_connect = None
        self.on_disconnect = None
        self._published = published
        self._rc = rc

    def will_set(self, topic, payload=None, qos=0, retain=False):
        self.will = (topic, payload, qos, retain)

    def reconnect_delay_set(self, min_delay=1, max_delay=120):
        pass

    def connect_async(self, host, port, keepalive=60):
        self.connected = (host, port, keepalive)

    def loop_start(self):
        self.loop_started = True
        if self.on_connect:
            self.on_connect(self, None, None, 0)

    def loop_stop(self):
        self.loop_stopped = True

    def disconnect(self):
        self.disconnected = True

    def publish(self, topic, payload=None, qos=0, retain=False):
        self.published_calls.append((topic, payload, qos, retain))
        return _FakePublishInfo(rc=self._rc, published=self._published)


def _sample_payload(machine_id="M-001", seq=1):
    return {
        "schema_version": "1.3.0",
        "stream_epoch": "ep-1",
        "event_id": f"evt_{machine_id}_{seq}",
        "machine_id": machine_id,
        "sequence_number": seq,
        "received_at": "2026-11-25T00:00:00Z",
        "source": "AI4I_SIMULATOR",
        "features": {
            "product_type": "M",
            "air_temperature_k": 298.1,
            "process_temperature_k": 308.6,
            "rotational_speed_rpm": 1551.0,
            "torque_nm": 42.8,
            "tool_wear_min": 10.0,
        },
    }


def test_mqtt_transport_publishes_qos1_with_lwt_and_birth():
    fake = _FakeMqttClient(published=True)
    transport = MqttTransport("mosquitto", port=1883, client=fake, observe=lambda _: None)
    fake.on_connect = transport._on_connect
    with transport:
        res = transport.send(_sample_payload("M-007", 3), correlation_id="cor-7")
    assert res.outcome == "accepted"
    assert res.retryable is False
    # First publish is Birth ONLINE on connect, second is telemetry QoS 1
    assert fake.published_calls[0][0].endswith("/controller/status")
    assert fake.published_calls[0][1] == b"ONLINE"
    assert fake.published_calls[0][2] == 1
    assert fake.published_calls[0][3] is True
    assert fake.published_calls[1][0].endswith("/M-007/telemetry")
    assert fake.published_calls[1][2] == 1
    assert fake.disconnected is True
    assert fake.loop_stopped is True


def test_mqtt_transport_requires_puback_and_handles_timeout():
    fake = _FakeMqttClient(published=False)
    transport = MqttTransport("mosquitto", port=1883, timeout_seconds=0.1, client=fake)
    fake.on_connect = transport._on_connect
    with transport:
        res = transport.send(_sample_payload("M-001", 1))
        fault_res = transport.send(_sample_payload("M-001", 2), fault=FaultMode.NETWORK_TIMEOUT)
    assert res.outcome == "failed" and res.retryable is True and res.error == "NETWORK_TIMEOUT"
    assert fault_res.outcome == "failed" and fault_res.retryable is True


def test_store_v1_to_v2_migration_preserves_pending_and_auth_and_quarantines_old(tmp_path):
    db_path = tmp_path / "v1.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE machines (
            machine_id TEXT PRIMARY KEY, stream_state TEXT NOT NULL,
            scenario TEXT NOT NULL, control_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL, last_controlled_by TEXT NOT NULL,
            sequence_number INTEGER NOT NULL, row_cursor INTEGER NOT NULL,
            scenario_step INTEGER NOT NULL
        );
        CREATE TABLE spool (
            id INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
            machine_id TEXT NOT NULL, payload TEXT NOT NULL, size INTEGER NOT NULL,
            created_at REAL NOT NULL, next_at REAL NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending',
            error TEXT
        );
        CREATE TABLE commands (
            actor TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
            response TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY(actor,key)
        );
        CREATE TABLE audit (
            id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, actor TEXT NOT NULL,
            action TEXT NOT NULL, target TEXT NOT NULL, detail TEXT NOT NULL
        );
        INSERT INTO meta VALUES ('schema_version', '1');
        INSERT INTO spool(event_id,machine_id,payload,size,created_at,next_at,status,error) VALUES
          ('e1','M-001','{}',10,100,100,'pending',NULL),
          ('e2','M-001','{}',12,100,100,'auth','HTTP_401'),
          ('e3','M-002','{}',15,100,100,'expired','TTL_EXPIRED'),
          ('e4','M-003','{}',16,100,100,'rejected','HTTP_422');
    """)
    conn.commit()
    conn.close()

    store = StateStore(db_path)
    try:
        with store.transaction() as db:
            ver = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
            stats = db.execute(
                "SELECT pending_count, pending_bytes FROM spool_stats WHERE machine_id='__global__'"
            ).fetchone()
            q_cnt = db.execute("SELECT COUNT(*) FROM spool_quarantine").fetchone()[0]
            auth_cnt = db.execute("SELECT COUNT(*) FROM spool WHERE status='auth'").fetchone()[0]
        assert ver == "2"
        assert (stats["pending_count"], stats["pending_bytes"]) == (1, 10)
        assert q_cnt == 2
        assert auth_cnt == 1
        assert store.summary()["counts"] == {"pending": 1, "auth": 1, "expired": 1, "rejected": 1}
    finally:
        store.close()


def test_quarantine_fifo_cap_and_per_machine_cap_round_robin(runtime_settings):
    drops = []
    settings = runtime_settings.model_copy(
        update={
            "machine_count": 4,
            "spool_max_records": 100,
            "spool_max_records_per_machine": 2,
            "quarantine_max_records": 3,
        }
    )
    engine = SensorEngine(settings)
    try:
        # Pause M-001..M-003 -> only M-004 active -> hits per-machine cap of 2
        assert engine.generate_once("M-004") is not None
        assert engine.generate_once("M-004") is not None
        dropped_events = engine.store.generate_many(
            ["M-001", "M-004"],
            engine.build,
            max_records=100,
            max_bytes=1_000_000,
            max_records_per_machine=2,
            on_drop=drops.append,
        )
        assert [e["machine_id"] for e in dropped_events] == ["M-001"]
        assert "machine_records" in drops

        # Round-robin rotation prevents tail machine starvation under tight global cap
        settings2 = runtime_settings.model_copy(
            update={
                "state_path": runtime_settings.state_path.with_name("rr.sqlite3"),
                "machine_count": 4,
            }
        )
        eng2 = SensorEngine(settings2)
        try:
            r1 = eng2.store.generate_many(
                ["M-001", "M-002", "M-003", "M-004"],
                eng2.build,
                max_records=2,
                max_bytes=1_000_000,
                on_drop=drops.append,
            )
            r2 = eng2.store.generate_many(
                ["M-001", "M-002", "M-003", "M-004"],
                eng2.build,
                max_records=4,
                max_bytes=1_000_000,
                on_drop=drops.append,
            )
            assert [e["machine_id"] for e in r1] == ["M-001", "M-002"]
            # Offset rotated to index 1 -> M-002..M-004 get turns rather than always M-001 first
            assert [e["machine_id"] for e in r2] == ["M-002", "M-003"]
        finally:
            eng2.close()

        # Expire 4 events into quarantine with cap=3 -> FIFO trims oldest
        engine.store.generate_many(
            ["M-001", "M-002", "M-003"],
            engine.build,
            max_records=100,
            max_bytes=1_000_000,
            now=100.0,
        )
        engine.store.due(2000.0, 500.0, 10, quarantine_max_records=3)
        with engine.store.transaction() as db:
            q_rows = db.execute("SELECT machine_id FROM spool_quarantine ORDER BY id").fetchall()
        assert len(q_rows) == 3
    finally:
        engine.close()


def test_runtime_settings_mqtt_validation(runtime_settings):
    s = runtime_settings.model_copy(update={"transport_mode": "mqtt", "mqtt_host": "mosquitto"})
    validated = type(s).model_validate(s.model_dump())
    assert validated.transport_mode == "mqtt"
    assert validated.mqtt_qos == 1
    assert validated.spool_max_records_per_machine is not None
    with pytest.raises(ValueError, match="wildcards"):
        type(s).model_validate({**s.model_dump(), "mqtt_topic_prefix": "maintai/+/machines"})


def test_mqtt_tls_and_auth_settings_and_transport(tmp_path, runtime_settings):
    class _TlsRecordingMqttClient(_FakeMqttClient):
        def __init__(self):
            super().__init__(published=True)
            self.tls_args = None
            self.auth_args = None

        def tls_set(self, **kwargs):
            self.tls_args = kwargs

        def username_pw_set(self, username, password=None):
            self.auth_args = (username, password)

    from simulator.settings import RuntimeSettings

    pw_file = tmp_path / "mqtt.pass"
    pw_file.write_text("mqtt-pass\n", encoding="utf-8")

    # Local cluster JWT mode with internal mosquitto and plaintext remains valid
    local_jwt = RuntimeSettings.model_validate(
        {
            **runtime_settings.model_dump(),
            "auth_mode": "jwt",
            "jwt_issuer": "https://keycloak.local/realms/maintai",
            "jwks_url": "https://keycloak.local/realms/maintai/certs",
            "backend_base_url": "https://ingestion-api.local",
            "transport_mode": "mqtt",
            "mqtt_host": "mosquitto",
            "mqtt_port": 1883,
            "mqtt_tls_enabled": False,
            "state_path": tmp_path / "local.sqlite3",
            "cors_origins": ["https://sim.maintai.localhost"],
        }
    )
    assert local_jwt.mqtt_tls_enabled is False

    # Shared external broker without TLS+auth fails closed
    with pytest.raises(ValueError, match="Shared external MQTT"):
        RuntimeSettings.model_validate(
            {
                **runtime_settings.model_dump(),
                "auth_mode": "jwt",
                "jwt_issuer": "https://keycloak.example.com/realms/maintai",
                "jwks_url": "https://keycloak.example.com/realms/maintai/certs",
                "backend_base_url": "https://ingestion.example.com",
                "transport_mode": "mqtt",
                "mqtt_host": "broker.example.com",
                "mqtt_port": 8883,
                "mqtt_tls_enabled": False,
                "state_path": tmp_path / "bad.sqlite3",
                "cors_origins": ["https://sim.example.com"],
            }
        )

    fake = _TlsRecordingMqttClient()
    transport = MqttTransport(
        "broker.example.com",
        port=8883,
        tls_enabled=True,
        ca_certs="/certs/ca.crt",
        username="sensor-edge",
        password=pw_file.read_text(encoding="utf-8").strip(),
        client=fake,
        observe=lambda _: None,
    )
    assert fake.tls_args is not None
    assert fake.tls_args["ca_certs"] == "/certs/ca.crt"
    assert fake.auth_args == ("sensor-edge", "mqtt-pass")
    assert transport.host == "broker.example.com"
