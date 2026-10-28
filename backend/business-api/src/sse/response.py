"""Always release admission, including cancellation before the first body frame."""

from fastapi.responses import StreamingResponse


class SSEResponse(StreamingResponse):
    def __init__(self, hub, subscription):
        self.hub, self.subscription = hub, subscription
        super().__init__(
            self.frames(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def frames(self):
        self.hub.connected(self.subscription)
        yield b": connected\nretry: 3000\n\n"
        while True:
            frame = await self.hub.next_frame(self.subscription)
            if frame is None:
                return
            yield frame

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.hub.unsubscribe(self.subscription)
