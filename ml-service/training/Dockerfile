# Build context: repository root.
# Batch fit/evaluate candidates; không tự export hoặc approve release.

FROM python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

WORKDIR /app

COPY ml-service/training/requirements-batch.lock /opt/maintai/requirements-batch.lock

RUN python -m pip install --no-cache-dir -r /opt/maintai/requirements-batch.lock \
    && python -m pip check \
    && useradd --uid 10001 --create-home mluser \
    && mkdir -p /app/sensor/data /inputs/ablation /outputs \
    && chown mluser:mluser /outputs

COPY ml-service/training/model/ /app/ml-service/training/model/
COPY ml-service/training/data_preparation/create_split.py /app/ml-service/training/data_preparation/create_split.py
COPY ml-service/training/data_preparation/splits/ /app/ml-service/training/data_preparation/splits/
COPY ml-service/shared/preprocessing/ /app/ml-service/shared/preprocessing/
COPY contracts/v1.3/schemas/ /app/contracts/v1.3/schemas/

ARG SOURCE_REVISION
ENV MAINTAI_SOURCE_REVISION=${SOURCE_REVISION}
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}

RUN python -c "import os,re; revision=os.environ.get('MAINTAI_SOURCE_REVISION',''); assert re.fullmatch(r'[0-9a-f]{40}',revision), 'SOURCE_REVISION must be a full Git commit'"

USER mluser

ENTRYPOINT ["python", "/app/ml-service/training/model/fit_interaction_candidates.py"]
CMD ["--help"]