# syntax=docker/dockerfile:1.7
# Isolated Storage local/CI fixture; same pinned source as infrastructure local. Upstream source/license retained; no unpinned download.
FROM library/golang:1.24.6-bookworm@sha256:ab1d1823abb55a9504d2e3e003b75b36dbeb1cbcc4c92593d85a84ee46becc6c AS build
ENV GOTOOLCHAIN=local CGO_ENABLED=0
WORKDIR /src
ADD --checksum=sha256:1a63209927b9d1fb239b2a776b7b01d1902eff19379a108f4bc7005edcc0968c https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-04-22T22-12-26Z /tmp/source.tar.gz
RUN tar -xzf /tmp/source.tar.gz -C /src --strip-components=1 && rm /tmp/source.tar.gz
RUN go build -mod=readonly -trimpath -buildvcs=false -o /out/minio .

FROM library/debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/LICENSE /usr/share/licenses/minio/LICENSE
RUN mkdir -p /data && chown 1000:1000 /data
ENV HOME=/tmp
USER 1000:1000
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
