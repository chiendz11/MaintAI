# Source/image owner: MaintAI/ml-service/mlflow. Deployment/config owner: MaintAI-gitops.
# Supply the reviewed upstream MLflow image@sha256 from image-lock.json.
ARG MLFLOW_BASE_IMAGE
FROM ${MLFLOW_BASE_IMAGE}

USER 0
COPY requirements.lock /opt/maintai-mlflow/requirements.lock
RUN python -m pip install --no-cache-dir -r /opt/maintai-mlflow/requirements.lock \
    && python -m pip check \
    && mkdir -p /home/maintai /var/lib/mlflow/artifacts \
    && chown -R 10001:10001 /home/maintai /var/lib/mlflow

ENV HOME=/home/maintai PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /home/maintai
USER 10001:10001
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/health', timeout=3)"
ENTRYPOINT ["mlflow"]
CMD ["server", "--host", "0.0.0.0", "--port", "5000"]
