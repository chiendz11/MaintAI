# Build context: root repository MaintAI.
# Image thử tích hợp baseline; chưa phải production release.

# Official Python OCI index resolved 2026-10-29; includes linux/amd64.
FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY ml-service/inference/requirements-serving.txt /app/requirements-serving.txt

RUN python -m pip install --no-cache-dir -r /app/requirements-serving.txt \
    && useradd --uid 10001 --create-home mluser

COPY ml-service/inference/api/local_baseline.py /app/local_baseline.py

# Model được mount khi chạy, không đóng cứng vào image.
ENV MODEL_DIR=/models/current

USER mluser

EXPOSE 8001

CMD ["python", "-m", "uvicorn", "local_baseline:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1", "--no-proxy-headers"]
