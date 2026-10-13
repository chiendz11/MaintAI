import json
import logging
import sys
from datetime import UTC, datetime

logger = logging.getLogger("maintai.drift")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "service": "drift-worker",
                "level": record.levelname,
                "event": record.getMessage(),
                **getattr(record, "context", {}),
            },
            allow_nan=False,
        )


def configure_logging():
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.handlers, logger.propagate = [handler], False
    logger.setLevel(logging.INFO)


def log(event: str, **context):
    logger.info(event, extra={"context": context})
