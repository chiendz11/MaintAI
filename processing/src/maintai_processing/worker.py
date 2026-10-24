"""Start the Processing consumer with a reviewed application handler factory."""

import os
import sys

from maintai_messaging import cli


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    factory = os.environ.get("PROCESSING_HANDLER_FACTORY")
    if factory and "--handler-factory" not in args:
        args = ["--handler-factory", factory, *args]
    return cli.main(["consume", "--role", "processing", *args])


if __name__ == "__main__":
    raise SystemExit(main())
