"""Multiple independent consumer groups in one application process."""

from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait

from .consumer import ConsumerRunner


def serve_consumers(settings, handlers, stop, *, contracts=None):
    if not handlers:
        raise ValueError("At least one durable handler is required")
    with ThreadPoolExecutor(
        max_workers=len(handlers), thread_name_prefix="maintai-consumer"
    ) as pool:
        futures = [
            pool.submit(ConsumerRunner(settings, role, handler, contracts=contracts).run, stop)
            for role, handler in handlers.items()
        ]
        try:
            done, _ = wait(futures, return_when=FIRST_EXCEPTION)
            for future in done:
                future.result()
        finally:
            stop.set()
        for future in futures:
            future.result()
