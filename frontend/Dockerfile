# Build context: repository root; Business UI is independent of Simulator UI.
ARG NODE_IMAGE=node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:stable-alpine@sha256:f5116ed4606f3eb015dd4a8e28cb9b351476dc0ad4bc6e9f93b19cec130cdf03
FROM ${NODE_IMAGE} AS build
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts
COPY contracts/ /build/contracts/
COPY frontend/src/ ./src/
COPY frontend/public/ ./public/
COPY frontend/scripts/ ./scripts/
COPY frontend/index.html frontend/tsconfig.json frontend/vite.config.ts ./
RUN npm run build

FROM ${NGINX_IMAGE} AS runtime
USER root
COPY frontend/nginx.conf /etc/nginx/nginx.conf
COPY --from=build --chown=101:101 /build/frontend/dist /usr/share/nginx/html
USER 101:101
EXPOSE 8080
ENTRYPOINT ["nginx", "-g", "daemon off;"]
