import asyncio

import pytest
from starlette.requests import Request

from maintai_ingestion.errors import EdgeFailure
from maintai_ingestion.request_guards import AdmissionGate, check_media, read_bounded_body


def request(chunks, headers=()):
    iterator = iter(chunks)

    async def receive():
        return next(iterator)

    return Request({"type": "http", "headers": list(headers)}, receive)


@pytest.mark.parametrize("size,status", [(7, None), (8, None), (9, 413)])
def test_actual_stream_limit(size, status):
    r = request([{"type": "http.request", "body": b"x" * size, "more_body": False}])

    async def run():
        return await read_bounded_body(r, limit=8, timeout_seconds=1)

    if status:
        with pytest.raises(EdgeFailure) as exc:
            asyncio.run(run())
        assert exc.value.status == status
    else:
        assert asyncio.run(run()) == b"x" * size


def test_chunked_and_lying_length_is_bounded():
    r = request(
        [
            {"type": "http.request", "body": b"12345", "more_body": True},
            {"type": "http.request", "body": b"6789", "more_body": False},
        ],
        [(b"content-length", b"1")],
    )
    with pytest.raises(EdgeFailure) as exc:
        asyncio.run(read_bounded_body(r, limit=8, timeout_seconds=1))
    assert exc.value.status == 413


@pytest.mark.parametrize("length", [b"-1", b"no", b"999"])
def test_invalid_or_oversize_content_length(length):
    r = request([], [(b"content-length", length)])
    with pytest.raises(EdgeFailure):
        asyncio.run(read_bounded_body(r, limit=8, timeout_seconds=1))


def test_body_timeout_and_disconnect():
    async def receive():
        await asyncio.Event().wait()

    r = Request({"type": "http", "headers": []}, receive)
    with pytest.raises(EdgeFailure) as exc:
        asyncio.run(read_bounded_body(r, limit=8, timeout_seconds=0.01))
    assert exc.value.status == 408
    from starlette.requests import ClientDisconnect

    with pytest.raises(ClientDisconnect):
        asyncio.run(
            read_bounded_body(request([{"type": "http.disconnect"}]), limit=8, timeout_seconds=1)
        )


def test_admission_releases_and_drains():
    gate = AdmissionGate(1)
    assert gate.try_acquire()
    assert not gate.try_acquire()
    gate.release()
    assert gate.try_acquire()
    gate.release()
    gate.begin_shutdown()
    assert not gate.try_acquire()


@pytest.mark.parametrize(
    "content_type,encoding,ok",
    [
        ("application/json", None, True),
        ("application/json; charset=utf-8", None, True),
        (None, None, False),
        ("text/plain", None, False),
        ("application/json", "gzip", False),
    ],
)
def test_media_type(content_type, encoding, ok):
    headers = {}
    if content_type:
        headers["content-type"] = content_type
    if encoding:
        headers["content-encoding"] = encoding
    if ok:
        check_media(headers)
    else:
        with pytest.raises(EdgeFailure) as exc:
            check_media(headers)
        assert exc.value.status == 415
