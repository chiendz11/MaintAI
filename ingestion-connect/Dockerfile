FROM docker.redpanda.com/redpandadata/connect:4.50.0

COPY --chown=10001:10001 ingestion-connect/connect.yaml /etc/connect/connect.yaml
COPY --chown=10001:10001 contracts/v1.3/schemas/features.schema.json /etc/connect/schemas/features.schema.json
COPY --chown=10001:10001 contracts/kafka/events/sensor-rejected.schema.json /etc/connect/schemas/sensor-rejected.schema.json

USER 10001:10001
EXPOSE 4195
CMD ["run", "/etc/connect/connect.yaml"]
