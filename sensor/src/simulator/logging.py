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


class StderrHandler(logging.StreamHandler):
    def emit(self, record):
        self.stream = sys.stderr
        super().emit(record)


def configure_logging() -> None:
    handler = StderrHandler()
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False


def log(event: str, *, level: int = logging.INFO, **context) -> None:
    logger.log(level, event, extra={"context": context})
