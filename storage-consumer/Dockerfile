# Build context: MaintAI repository root (the local messaging SDK is not on PyPI).
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS builder
WORKDIR /build
RUN python -m pip install --no-cache-dir setuptools==84.0.0 wheel==0.48.0
COPY messaging/pyproject.toml messaging/setup.py messaging/README.md ./messaging/
COPY messaging/src/ ./messaging/src/
COPY contracts/kafka/ ./contracts/kafka/
COPY contracts/v1.3/schemas/runtime.schema.json ./contracts/v1.3/schemas/runtime.schema.json
COPY contracts/v1.3/events/alert-signal.schema.json ./contracts/v1.3/events/alert-signal.schema.json
COPY storage-consumer/pyproject.toml storage-consumer/README.md ./storage-consumer/
COPY storage-consumer/src/ ./storage-consumer/src/
RUN python -m pip wheel --no-cache-dir --no-deps --no-build-isolation \
    --wheel-dir /wheels ./messaging ./storage-consumer

FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 STORAGE_HTTP_HOST=0.0.0.0 STORAGE_HTTP_PORT=8008
WORKDIR /app
COPY storage-consumer/requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir --requirement requirements.lock \
    && useradd --uid 10001 --user-group --create-home storage
COPY --from=builder /wheels/ /wheels/
RUN python -m pip install --no-cache-dir --no-deps /wheels/*.whl && rm -rf /wheels
USER 10001:10001
EXPOSE 8008
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('STORAGE_HTTP_PORT', '8008') + '/health/ready', timeout=3)"
ENTRYPOINT ["maintai-storage"]
CMD ["serve"]
