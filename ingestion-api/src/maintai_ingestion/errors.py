"""Sanitized transport failures; never carry raw request/provider details."""


class EdgeFailure(Exception):
    def __init__(self, status: int, code: str, *, retryable: bool = False):
        self.status, self.code, self.retryable = status, code, retryable
        super().__init__(code)
