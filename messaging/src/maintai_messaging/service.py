"""Multiple independent consumer groups in one application process."""

from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

from .consumer import ConsumerRunner
from .errors import ConfigurationError


def serve_consumers(settings, handlers, stop, *, contracts=None, keyed_limits=None, metrics=None):
    if not handlers:
        raise ValueError("At least one durable handler is required")
    with ThreadPoolExecutor(
        max_workers=len(handlers), thread_name_prefix="maintai-consumer"
    ) as pool:
        futures = []
        try:
            for role, handler in handlers.items():
                if keyed_limits is not None and role == "processing":
                    from .keyed import KeyedConsumerRunner

                    runner = KeyedConsumerRunner(
                        settings,
                        role,
                        handler,
                        contracts=contracts,
                        limits=keyed_limits,
                        metrics=metrics,
                    )
                else:
                    if not callable(handler):
                        raise ConfigurationError("Serial mode requires a callable durable handler")
                    runner = ConsumerRunner(settings, role, handler, contracts=contracts)
                futures.append(pool.submit(runner.run, stop))
            done, _ = wait(futures, return_when=FIRST_EXCEPTION)
            for future in done:
                future.result()
        finally:
            stop.set()
        for future in futures:
            future.result()
