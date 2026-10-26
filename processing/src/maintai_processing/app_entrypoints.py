"""Owner app commands with dependency-light help for SDK wheel verification.

The generic SDK transport wrappers remain available as python -m entry modules.
Running the full owner application still needs the locked app environment.
"""

import argparse
import sys


def worker(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if any(arg in {"-h", "--help"} for arg in args):
        parser = argparse.ArgumentParser(
            description="Processing owner worker; shared handoff pending"
        )
        parser.add_argument("--processing-mode", choices=["serial", "keyed"])
        parser.parse_args(args)
        return 0
    from consumer.main import main

    return main(args)


def relay(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if any(arg in {"-h", "--help"} for arg in args):
        parser = argparse.ArgumentParser(description="Processing owner outbox relay")
        parser.add_argument("--mode", choices=["legacy", "ordered"])
        parser.add_argument("--once", action="store_true")
        parser.parse_args(args)
        return 0
    from relay.main import main

    return main(args)
