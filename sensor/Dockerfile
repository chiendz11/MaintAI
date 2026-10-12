FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SIM_DATASET_PATH=/app/data/ai4i2020.csv \
    SIM_SCENARIO_PATH=/app/scenarios

WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir --requirement requirements.lock

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY scenarios/ ./scenarios/
RUN python -m pip install --no-cache-dir --no-deps . \
    && useradd --uid 10001 --user-group --create-home simulator \
    && mkdir -p /app/data /app/output \
    && chown -R simulator:simulator /app

USER 10001:10001
ENTRYPOINT ["maintai-sensor"]
CMD ["replay"]
