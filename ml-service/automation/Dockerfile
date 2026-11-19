# Build context: repository root MaintAI.
FROM python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY ml-service/automation/requirements.lock /opt/maintai/automation-requirements.lock
RUN python -m pip install --no-cache-dir -r /opt/maintai/automation-requirements.lock \
    && python -m pip check \
    && useradd --uid 10001 --user-group --create-home automation

COPY ml-service/automation/pyproject.toml /app/ml-service/automation/pyproject.toml
COPY ml-service/automation/src/ /app/ml-service/automation/src/
RUN python -m pip install --no-cache-dir --no-deps /app/ml-service/automation

USER 10001:10001

ENTRYPOINT ["python", "-m", "maintai_automation"]
CMD ["--help"]
