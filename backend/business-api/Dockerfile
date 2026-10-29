# Build context: repository root. Shared Business package; separate runtime entrypoints.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70
FROM ${PYTHON_IMAGE} AS builder
WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
RUN python -m pip install build==1.6.1 setuptools==84.0.0 wheel==0.48.0
COPY messaging/ /build/messaging/
COPY contracts/ /build/contracts/
RUN python -m build --wheel --no-isolation messaging --outdir /wheels
COPY backend/requirements.lock /build/backend/requirements.lock
# The internal SDK is built from this checkout, never resolved from a public index.
RUN python -c "from pathlib import Path; p=Path('backend/requirements.lock'); Path('/tmp/deps.lock').write_text('\n'.join(l for l in p.read_text().splitlines() if not l.startswith('maintai-messaging=='))+'\n')" \
    && python -m pip wheel -r /tmp/deps.lock --wheel-dir /wheels
COPY backend/ /build/backend/
RUN python -m build --wheel --no-isolation backend --outdir /wheels

FROM ${PYTHON_IMAGE} AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY --from=builder /wheels /wheels
COPY backend/requirements.lock /tmp/requirements.lock
RUN python -m pip install --no-cache-dir --no-index --find-links /wheels -r /tmp/requirements.lock /wheels/maintai_business-0.1.0-py3-none-any.whl \
    && groupadd --gid 10001 maintai \
    && useradd --uid 10001 --gid 10001 --no-create-home maintai
USER 10001:10001
EXPOSE 8003
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('BUSINESS_PORT','8003')+'/health/live',timeout=2)"
ENTRYPOINT ["maintai-business-api"]
