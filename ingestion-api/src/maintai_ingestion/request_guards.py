"""Transport guards applied before SDK envelope parsing."""

import asyncio

from .errors import EdgeFailure


class AdmissionGate:
    def __init__(self, capacity):
        self.capacity, self.active, self.draining = capacity, 0, False
        self.empty = asyncio.Event()
        self.empty.set()

    def try_acquire(self):
        if self.draining or self.active >= self.capacity:
            return False
        self.active += 1
        self.empty.clear()
        return True

    def release(self):
        self.active -= 1
        if self.active == 0:
            self.empty.set()

    def begin_shutdown(self):
        self.draining = True


def check_media(headers):
    media = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    encoding = headers.get("content-encoding", "identity").strip().lower()
    if media != "application/json" or encoding != "identity":
        raise EdgeFailure(415, "UNSUPPORTED_MEDIA_TYPE")


async def read_bounded_body(request, *, limit, timeout_seconds):
    length = request.headers.get("content-length")
    if length is not None:
        if not length.isascii() or not length.isdigit():
            raise EdgeFailure(400, "INVALID_REQUEST")
        if int(length) > limit:
            raise EdgeFailure(413, "PAYLOAD_TOO_LARGE")
    chunks, total = [], 0
    try:
        async with asyncio.timeout(timeout_seconds):
            async for chunk in request.stream():
                total += len(chunk)
                if total > limit:
                    raise EdgeFailure(413, "PAYLOAD_TOO_LARGE")
                chunks.append(chunk)
    except TimeoutError:
        raise EdgeFailure(408, "REQUEST_TIMEOUT", retryable=True) from None
    return b"".join(chunks)
