"""Two serial SDK groups; Alert Signals remain disabled until their handler exists."""

import argparse
import sys


def make_handlers(connection_factory, settings, *, metrics=None):
    from maintai_messaging.contracts import default_contracts

    from .handlers.prediction import build_handler as prediction
    from .handlers.telemetry import build_handler as telemetry

    profiles = default_contracts().topology["profiles"]
    return {
        role: builder(
            connection_factory, settings.group_prefix + profiles[role]["group_id"], metrics=metrics
        )
        for role, builder in (("business-live", telemetry), ("business-prediction", prediction))
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Business telemetry/prediction Consumer")
    parser.parse_args(argv)
    try:
        from .config import Settings
        from .runtime import serve

        serve(Settings.from_env())
        return 0
    except Exception as exc:
        print(f"Business Consumer failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
