import argparse
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from urllib.error import URLError
from zipfile import BadZipFile

from pydantic import ValidationError

from simulator import __version__
from simulator.config import SimulatorConfig, default_dataset_path
from simulator.download import download_dataset
from simulator.fault_injection import FaultMode
from simulator.logging import configure_logging, log
from simulator.models import sensor_event_schema
from simulator.runner import ReplayRunner
from simulator.scenarios import ScenarioLoader
from simulator.settings import RuntimeSettings


def parser() -> argparse.ArgumentParser:
    app = argparse.ArgumentParser(description="MaintAI AI4I sensor simulator")
    app.add_argument("--version", action="version", version=__version__)
    commands = app.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay", help="Replay AI4I features as sensor events")
    replay.add_argument("--dataset", dest="dataset_path", type=Path)
    replay.add_argument("--backend-url", dest="backend_base_url")
    replay.add_argument(
        "--interval-ms",
        type=float,
        help="Logical interval per machine in ms (default: 1000; 0: no wait)",
    )
    replay.add_argument("--speed", type=float, help="Replay speed multiplier (default: 1)")
    replay.add_argument("--seed", type=int)
    replay.add_argument(
        "--machine-count", type=int, help="Machines visited in round-robin order (default: 5)"
    )
    replay.add_argument("--scenario", help="Built-in scenario name")
    replay.add_argument("--scenario-path", type=Path, help="Scenario directory")
    replay.add_argument("--scenario-file", type=Path, help="Explicit custom YAML file")
    replay.add_argument("--fault-mode", choices=list(FaultMode))
    replay.add_argument("--fault-every", type=int, help="Inject fault every Nth generated event")
    replay.add_argument("--fault-field")
    replay.add_argument("--timeout-seconds", type=float)
    replay.add_argument("--retry-backoff-ms", type=float)
    replay.add_argument("--limit", type=int, help="Maximum generated events, excluding duplicates")
    replay.add_argument("--loop-dataset", action="store_true", default=None)
    replay.add_argument("--dry-run", action="store_true", default=None, help="JSONL, no HTTP/sleep")
    replay.add_argument("--output", type=Path, help="Write delivered payloads as JSONL")
    replay.add_argument("--metrics-file", type=Path, help="Write end-of-run Prometheus text")
    replay.add_argument(
        "--run-id", help="Fixed replay identity for reproducibility/idempotent replay"
    )
    replay.add_argument("--start-time", help="Timezone-aware ISO timestamp for logical event time")
    download = commands.add_parser("download-data", help="Download checksum-verified AI4I from UCI")
    download.add_argument("--output", type=Path, default=default_dataset_path())
    download.add_argument("--force", action="store_true", help="Replace an existing dataset")
    schema = commands.add_parser("export-schema", help="Export SensorEvent JSON schema")
    schema.add_argument("--output", type=Path)
    serve = commands.add_parser("serve", help="Run durable scheduler, spool and Sensor Control API")
    for cmd in (serve, replay):
        cmd.add_argument("--state-path", type=Path)
        cmd.add_argument(
            "--local-anonymous",
            action="store_true",
            default=None,
            help="Explicit local-only auth profile; never bind externally",
        )
        cmd.add_argument("--ingestion-auth", choices=["token_file", "oauth", "none"])
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--dataset", dest="dataset_path", type=Path)
    serve.add_argument("--backend-url", dest="backend_base_url")
    spool = commands.add_parser(
        "spool", help="Inspect or explicitly repair spool while controller is stopped"
    )
    spool.add_argument("action", choices=["list", "retry", "discard"])
    spool.add_argument("--state-path", type=Path, required=True)
    spool.add_argument("--event-id")
    spool.add_argument("--reason")
    return app


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    configure_logging()
    try:
        if args.command == "download-data":
            path = download_dataset(args.output, force=args.force)
            log("dataset_ready", path=str(path))
            return 0
        if args.command == "export-schema":
            content = json.dumps(sensor_event_schema(), indent=2) + "\n"
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(content, encoding="utf-8")
            else:
                sys.stdout.write(content)
            return 0
        if args.command == "spool":
            from simulator.ordering.store import StateStore

            if not args.state_path.is_file():
                raise ValueError("State file not found; maintenance must not create a new journal")
            store = StateStore(args.state_path)
            try:
                if args.action == "list":
                    sys.stdout.write(json.dumps(store.inspect(), indent=2) + "\n")
                else:
                    if not args.event_id or not args.reason:
                        raise ValueError("Spool repair requires --event-id and --reason")
                    store.repair(args.event_id, args.action, args.reason)
            finally:
                store.close()
            return 0
        overrides = {key: value for key, value in vars(args).items() if key != "command"}
        if overrides.pop("local_anonymous", False):
            overrides["auth_mode"] = "local"
        if args.command == "serve":
            import uvicorn

            from simulator.control.api import create_app

            settings = RuntimeSettings.from_env(overrides)
            uvicorn.run(
                create_app(settings),
                host=settings.host,
                port=settings.port,
                workers=1,
                proxy_headers=False,
                access_log=False,
            )
            return 0
        runtime_keys = {
            k: overrides.pop(k)
            for k in list(overrides)
            if k in ("state_path", "auth_mode", "ingestion_auth")
        }
        config = SimulatorConfig.from_env(overrides)
        if not config.dry_run:
            from simulator.retry_spool.replay import replay_durable

            return replay_durable(RuntimeSettings.from_env({**config.model_dump(), **runtime_keys}))
        scenario = ScenarioLoader.load(config.scenario_yaml)
        with ExitStack() as stack:
            output = sys.stdout if config.dry_run else None
            if config.output:
                config.output.parent.mkdir(parents=True, exist_ok=True)
                output = stack.enter_context(config.output.open("w", encoding="utf-8"))
            stats = ReplayRunner(config, scenario, output=output).run()
            if config.metrics_file:
                config.metrics_file.parent.mkdir(parents=True, exist_ok=True)
                config.metrics_file.write_text(stats.prometheus_text(), encoding="utf-8")
            return stats.exit_code
    except ValidationError as exc:
        # Config error output must not echo credentials from an invalid URL/environment.
        log("configuration_error", errors=exc.errors(include_input=False, include_context=False))
        return 2
    except (OSError, ValueError, URLError, BadZipFile) as exc:
        log("simulator_error", error=str(exc))
        return 2
    except KeyboardInterrupt:
        log("simulator_interrupted")
        return 130
