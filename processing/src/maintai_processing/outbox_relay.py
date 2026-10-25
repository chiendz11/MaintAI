"""Start the Processing relay; ordered mode requires explicit DB activation."""

import sys

from maintai_messaging import cli


def main(argv=None):
    return cli.main(["relay", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    raise SystemExit(main())
