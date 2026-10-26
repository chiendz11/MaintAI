"""Kafka worker entrypoint with one HTTP loop and explicit local-fixture scope."""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import threading
from concurrent.futures import TimeoutError as FutureTimeout

import httpx
from maintai_messaging.consumer import ConsumerRunner
from maintai_messaging.contracts import default_contracts
from maintai_messaging.keyed import KeyedConsumerRunner
from maintai_messaging.metrics import ProcessingMetrics
from maintai_messaging.postgres import connect
from processing.shared.data_quality.machine_registry import HTTPMachineRegistry
from processing.shared.inference_client.client import InferenceClient
from processing.shared.persistence.connection import validate_execution_mode

from .config import WorkerSettings
from .handler import KeyedProcessingHandler, ProcessingHandler
from .observability import RuntimeObservability, monitor_dependencies, start_probe_server


async def run_worker(settings, stop, *, max_records=None):
    def validate():
        with connect(settings.database_url) as connection:
            validate_execution_mode(connection, ordered=settings.execution_mode == "keyed")

    await asyncio.to_thread(validate)
    observer = RuntimeObservability("worker", profile=settings.profile, stop=stop)
    server, probe_thread = start_probe_server(
        observer, settings.health_port, host=settings.health_host
    )
    monitoring = asyncio.create_task(
        monitor_dependencies(observer, settings.kafka, settings.database_url, stop)
    )
    try:
        async with httpx.AsyncClient(trust_env=False) as http:
            registry = HTTPMachineRegistry(
                http,
                base_url=settings.business_url,
                schema_directory=settings.schema_directory,
                local_fixture=True,
                bearer_token=settings.registry_token,
            )
            ml = InferenceClient(
                http,
                base_url=settings.ml_url,
                feature_validator=settings.validator,
                local_fixture=True,
            )
            loop = asyncio.get_running_loop()

            def wait_for_http(awaitable, budget):
                future = asyncio.run_coroutine_threadsafe(awaitable, loop)
                try:
                    return future.result(timeout=budget)
                except FutureTimeout:
                    future.cancel()
                    raise RuntimeError("Processing HTTP bridge deadline expired") from None

            def predict(**kwargs):
                with observer.ml_latency.time():
                    result = wait_for_http(ml.predict(**kwargs), 5)
                observer.ml_results.labels(result.failure_code or "SUCCESS").inc()
                return result

            contracts = default_contracts()
            group = (
                settings.kafka.group_prefix
                + contracts.topology["profiles"]["processing"]["group_id"]
            )
            handler = ProcessingHandler(
                lambda: observer.database_connection(lambda: connect(settings.database_url)),
                consumer_group=group,
                validator=settings.validator,
                registry_lookup=lambda machine_id, **kw: wait_for_http(
                    registry.lookup(machine_id, **kw), 3
                ),
                predict=predict,
                identity_resolver=lambda response: settings.identity_fixture,
                policy=settings.policy,
                producer_git_sha=settings.kafka.producer_git_sha,
                contracts=contracts,
                validation_observer=observer.record_validation,
            )

            def durable(event, record):
                observer.records.labels("worker").inc()
                logging.getLogger("maintai.processing").info(
                    json.dumps(
                        {
                            "runtime": "processing-worker",
                            "event_id": event.message_id,
                            "correlation_id": event.payload["correlation_id"],
                            "source_partition": record.partition,
                            "source_offset": record.offset,
                            "durable": True,
                            "profile": settings.profile,
                        }
                    )
                )

            def observed(event, record):
                try:
                    receipt = handler(event, record)
                    observer.records.labels("worker").inc()
                    logging.getLogger("maintai.processing").info(
                        json.dumps(
                            {
                                "runtime": "processing-worker",
                                "event_id": event.message_id,
                                "correlation_id": event.payload["correlation_id"],
                                "source_partition": record.partition,
                                "source_offset": record.offset,
                                "durable": True,
                                "profile": settings.profile,
                            }
                        )
                    )
                    return receipt
                except Exception:
                    observer.errors.labels("worker").inc()
                    raise

            if settings.execution_mode == "keyed":
                keyed_handler = KeyedProcessingHandler(
                    handler,
                    on_receipt=durable,
                    on_error=lambda: observer.errors.labels("worker").inc(),
                )
                runner = KeyedConsumerRunner(
                    settings.kafka,
                    "processing",
                    keyed_handler,
                    limits=settings.keyed_limits,
                    metrics=ProcessingMetrics(observer.registry),
                    contracts=contracts,
                )
            else:
                runner = ConsumerRunner(settings.kafka, "processing", observed, contracts=contracts)
            await asyncio.to_thread(runner.run, stop, max_records=max_records)
    finally:
        stop.set()
        observer.draining = True
        monitoring.cancel()
        await asyncio.gather(monitoring, return_exceptions=True)
        server.should_exit = True
        await asyncio.to_thread(probe_thread.join, 5)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=("Processing worker (local-fixture runtime; shared serving handoff pending)")
    )
    parser.add_argument("--processing-mode", choices=["serial", "keyed"])
    args = parser.parse_args(argv)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        env = dict(os.environ)
        if args.processing_mode:
            env["PROCESSING_EXECUTION_MODE"] = args.processing_mode
        settings = WorkerSettings.from_env(env)
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        asyncio.run(run_worker(settings, stop))
        return 0
    except Exception as exc:
        print(f"Processing worker failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
