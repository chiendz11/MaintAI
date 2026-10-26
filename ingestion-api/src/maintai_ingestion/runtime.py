"""Application-owned resources; SDK owns Kafka transport semantics."""

import asyncio
import logging
from dataclasses import dataclass, field

from maintai_messaging.ingestion import IngestionGateway
from maintai_messaging.producer import KafkaPublisher

from .auth import LocalFixtureVerifier, OidcIdentityVerifier, jwks_fetcher
from .health import DependencyState, KafkaReadinessProbe
from .observability import Observability, ObservedPublisher
from .request_guards import AdmissionGate


async def run_blocking(function, *args, **kwargs):
    # A cancelled HTTP task must not drop the admission slot while its worker publishes.
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        # Retrieve worker errors without exposing them on a disconnected request.
        try:
            task.result()
        except Exception:
            pass
        raise asyncio.CancelledError
    return task.result()


@dataclass
class Runtime:
    settings: object
    publisher: object
    gateway: object
    verifier: object
    probe: object
    gate: AdmissionGate
    health: DependencyState
    obs: Observability
    _close_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _closed: bool = field(default=False, init=False)

    async def refresh_health(self):
        async def safe_check(check):
            try:
                return bool(await run_blocking(check))
            except Exception:
                return False

        kafka, auth = await asyncio.gather(
            safe_check(self.probe.check), safe_check(self.verifier.check)
        )
        self.health.kafka, self.health.auth_keys = kafka, auth
        import time

        self.health.checked_at = time.monotonic()

    async def monitor(self, stop):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.settings.probe_interval)
            except TimeoutError:
                await self.refresh_health()

    async def close(self):
        async with self._close_lock:
            if self._closed:
                return
            self.gate.begin_shutdown()
            await self.gate.empty.wait()
            try:
                await run_blocking(self.publisher.close)
            except Exception:
                logging.getLogger("maintai.ingestion").error(
                    "Producer shutdown acknowledgement uncertain"
                )
            finally:
                self._closed = True


def build_runtime(settings, *, publisher_factory=None, verifier=None, probe=None):
    obs = Observability()
    if verifier is None:
        if settings.auth_mode == "local-fixture":
            verifier = LocalFixtureVerifier(settings.local_subject, settings.local_machines)
        else:
            verifier = OidcIdentityVerifier(
                issuer=settings.issuer,
                audience=settings.audience,
                algorithms=settings.algorithms,
                roles_claim=settings.roles_claim,
                machines_claim=settings.machines_claim,
                fetch_jwks=jwks_fetcher(settings.jwks_url, settings.jwks_timeout),
                cache_seconds=settings.jwks_cache,
                lock_timeout=settings.jwks_timeout,
                observability=obs,
            )
    if probe is None:
        probe = KafkaReadinessProbe(settings.kafka, settings.probe_timeout)
    if publisher_factory is None:

        def publisher_factory(cfg):
            return KafkaPublisher(cfg, "ingestion")

    raw_publisher = publisher_factory(settings.kafka)
    publisher = ObservedPublisher(raw_publisher, obs)
    try:
        gateway = IngestionGateway(publisher, settings.business_origin)
    except Exception:
        publisher.close()
        raise
    return Runtime(
        settings,
        publisher,
        gateway,
        verifier,
        probe,
        AdmissionGate(settings.max_inflight),
        DependencyState(),
        obs,
    )
