import asyncio

import httpx


def test_slow_stream_health_probe_has_absolute_deadline():
    from backend.shared.monitoring.runtime import probe

    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(10):
                await asyncio.sleep(0.1)
                yield b"x"

    async def run():
        transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Slow()))
        result = await probe("http://127.0.0.1", transport=transport, budget=0.03)
        assert result["status"] == "UNAVAILABLE"

    asyncio.run(run())
