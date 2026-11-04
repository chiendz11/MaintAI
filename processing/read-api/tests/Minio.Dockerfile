# Local integration fixture only; not the shared platform MinIO image.
FROM golang:1.24.8-bookworm AS builder
ADD --checksum=sha256:c9598dcce3440977e79f787f2ba0e7e4d92c8d556bd51e7cef3785bafd6635f3 https://github.com/minio/minio/archive/refs/tags/RELEASE.2025-09-07T16-13-09Z.tar.gz /tmp/minio.tar.gz
RUN mkdir /source && tar -xzf /tmp/minio.tar.gz -C /source --strip-components=1
WORKDIR /source
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/minio .

FROM python:3.12-slim-bookworm
COPY --from=builder /out/minio /usr/local/bin/minio
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/minio"]
