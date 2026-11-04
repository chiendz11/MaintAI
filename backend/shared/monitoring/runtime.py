"""Independent bounded catalog reconciliation and cached health observations."""

import asyncio
import logging
import threading
import time

import httpx


class CatalogRunner:
    def __init__(self, catalog, stop):
        self.catalog, self.stop = catalog, stop
        self.threads = []

    def start(self):
        def work(full):
            while not self.stop.is_set():
                try:
                    if full:
                        with self.catalog.factory() as connection:
                            state = connection.execute(
                                "SELECT complete_scan_at,clock_timestamp() AS now "
                                "FROM drift_catalog_state"
                            ).fetchone()
                        if (
                            state["complete_scan_at"] is not None
                            and (state["now"] - state["complete_scan_at"]).total_seconds() < 86400
                        ):
                            self.stop.wait(60)
                            continue
                    self.catalog.sync(full=full, max_pages=1)
                except Exception:
                    logging.getLogger(__name__).warning("Drift catalog scan unavailable")
                if not full:
                    try:
                        self.catalog.cleanup(batch=10000)
                    except Exception:
                        logging.getLogger(__name__).warning("Drift snapshot cleanup unavailable")
                self.stop.wait(60)

        self.threads = [
            threading.Thread(
                target=work,
                args=(full,),
                daemon=True,
                name="drift-full" if full else "drift-recent",
            )
            for full in (False, True)
        ]
        for thread in self.threads:
            thread.start()

    def close(self):
        for thread in self.threads:
            thread.join(timeout=5)


async def probe(url, *, transport=None, budget=2):
    if not url:
        return {"status": "UNAVAILABLE", "message": "Chưa cấu hình nguồn kiểm tra."}
    try:
        async with (
            asyncio.timeout(budget),
            httpx.AsyncClient(
                trust_env=False, follow_redirects=False, timeout=budget, transport=transport
            ) as client,
        ):
            async with client.stream(
                "GET", url + "/health/ready", headers={"Accept-Encoding": "identity"}
            ) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 65536:
                        raise ValueError("Health response exceeds bound")
                status = (
                    "AVAILABLE"
                    if response.status_code == 200
                    else "DEGRADED"
                    if response.status_code == 503
                    else "UNAVAILABLE"
                )
                return {"status": status, "message": "Kết quả readiness của dịch vụ."}
    except (TimeoutError, httpx.HTTPError, ValueError):
        return {"status": "UNAVAILABLE", "message": "Không đọc được readiness trong thời hạn."}


def health(config):
    lock = threading.Lock()
    values, checked_at = {}, float("-inf")

    def unavailable():
        return {
            name: {"status": "UNAVAILABLE", "message": "Chưa có quan sát readiness mới."}
            for name in ("ml", "rag")
        }

    def refresh():
        nonlocal values, checked_at

        async def fetch():
            return await asyncio.gather(probe(config.ml_url), probe(config.rag_url))

        try:
            observed = asyncio.run(fetch())
            values = dict(zip(("ml", "rag"), observed, strict=True))
            checked_at = time.monotonic()
        finally:
            lock.release()

    def observe():
        # No network work in the API request pool; at most one refresher per API process.
        if time.monotonic() - checked_at <= 15:
            return values
        if lock.acquire(blocking=False):
            threading.Thread(target=refresh, daemon=True, name="monitoring-health").start()
        return unavailable()

    return observe


def catalog(config, factory):
    from pathlib import Path

    from .catalog import DriftCatalog
    from .s3 import build

    return DriftCatalog(
        factory,
        build(config),
        Path(config.cursor_key_file).read_bytes(),
        model_version=config.model_version,
        services=health(config),
    )
