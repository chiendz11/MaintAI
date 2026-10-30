# Build context: repository root. Actual upstream digest resolved 2026-10-23.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70
FROM ${PYTHON_IMAGE} AS builder
WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
RUN python -m pip install build==1.6.1 setuptools==84.0.0 wheel==0.48.0
COPY messaging/ /build/messaging/
COPY contracts/ /build/contracts/
COPY storage-consumer/ /build/storage-consumer/
RUN python -m build --wheel --no-isolation messaging --outdir /wheels \
    && python -m build --wheel --no-isolation storage-consumer --outdir /wheels
COPY processing/requirements.lock /build/processing/requirements.lock
# Large wheels can exceed pip's short default read timeout on the CI build network.
ENV PIP_DEFAULT_TIMEOUT=120
# Never resolve the internal SDK from an external index.
RUN python -c "from pathlib import Path; p=Path('processing/requirements.lock'); Path('/tmp/deps.lock').write_text('\n'.join(l for l in p.read_text().splitlines() if not l.startswith(('maintai-messaging==', 'maintai-storage-consumer==')))+'\n')" \
    && python -m pip wheel -r /tmp/deps.lock --wheel-dir /wheels
COPY processing/ /build/processing/
RUN python -m build --wheel --no-isolation processing --outdir /wheels

FROM ${PYTHON_IMAGE} AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PROCESSING_SCHEMA_DIRECTORY=/app/http-contracts
COPY --from=builder /wheels /wheels
COPY processing/requirements.lock /tmp/requirements.lock
COPY contracts/v1.3/schemas/ /app/http-contracts/
RUN python -m pip install --no-cache-dir --no-index --find-links /wheels -r /tmp/requirements.lock /wheels/maintai_processing-0.1.0-py3-none-any.whl \
    && groupadd --gid 10001 maintai \
    && useradd --uid 10001 --gid 10001 --no-create-home maintai
USER 10001:10001
EXPOSE 8004
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PROCESSING_READ_PORT','8004')+'/health/live',timeout=2)"
ENTRYPOINT ["maintai-processing-read-api"]
