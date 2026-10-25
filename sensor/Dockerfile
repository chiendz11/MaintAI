FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SIM_DATASET_PATH=/app/data/ai4i2020.csv \
    SIM_SCENARIO_PATH=/app/scenarios \
    SIM_STATE_PATH=/app/state/state.sqlite3

WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --requirement requirements.lock

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY scenarios/ ./scenarios/
RUN python -m pip install --no-cache-dir --no-deps . \
    && useradd --uid 10001 --user-group --create-home simulator \
    && mkdir -p /app/data /app/output /app/state \
    && chown -R simulator:simulator /app

USER 10001:10001
EXPOSE 8005
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8005/health/ready', timeout=3)"
ENTRYPOINT ["maintai-sensor"]
CMD ["serve", "--host", "0.0.0.0"]
