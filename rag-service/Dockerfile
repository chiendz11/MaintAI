# Official Python OCI index resolved 2026-10-29; includes linux/amd64.
FROM python:3.11-slim@sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HF_HOME=/opt/maintai/model-cache SENTENCE_TRANSFORMERS_HOME=/opt/maintai/model-cache
WORKDIR /app
RUN groupadd -r maintai && useradd -r -g maintai -d /app maintai && mkdir -p /opt/maintai/model-cache /tmp/maintai && chown -R maintai:maintai /opt/maintai /tmp/maintai /app
COPY rag-service/requirements-runtime.lock /app/requirements-runtime.lock
RUN pip install --no-cache-dir -r requirements-runtime.lock
COPY rag-service/ /app/
RUN chown -R maintai:maintai /app
USER maintai
EXPOSE 8002
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
