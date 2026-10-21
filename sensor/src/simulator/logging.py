import json
import logging
import sys
from datetime import UTC, datetime

logger = logging.getLogger("maintai.sensor")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "service": "sensor-simulator",
                "event": record.getMessage(),
                **getattr(record, "context", {}),
            },
            allow_nan=False,
        )


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False


def log(event: str, *, level: int = logging.INFO, **context) -> None:
    logger.log(level, event, extra={"context": context})
