FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock ./
RUN python -m pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN python -m pip install --no-cache-dir --no-deps . \
    && useradd --uid 10001 --user-group --create-home worker
USER 10001:10001
ENTRYPOINT ["maintai-drift"]
CMD ["run"]
