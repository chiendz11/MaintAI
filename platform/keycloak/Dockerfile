# Platform/image owner: MaintAI/platform/keycloak. Deployment/config owner: MaintAI-gitops.
ARG KEYCLOAK_BASE_IMAGE=quay.io/keycloak/keycloak:26.4.0@sha256:5f3fb534cde6bf006d79f5912473e5d2c828c707cdfc52e16972803aca9d43dd

FROM ${KEYCLOAK_BASE_IMAGE} AS builder

COPY --chown=1000:0 themes/maintai /opt/keycloak/themes/maintai
RUN /opt/keycloak/bin/kc.sh build \
    --db=postgres \
    --health-enabled=true \
    --metrics-enabled=true

FROM ${KEYCLOAK_BASE_IMAGE}
COPY --from=builder --chown=1000:0 /opt/keycloak/ /opt/keycloak/
USER 1000
ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
