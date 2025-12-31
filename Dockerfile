FROM golang:1.24 AS build

ARG TARGETARCH
ARG RELEASE

ENV GOPATH=/go
ENV CGO_ENABLED=0

RUN apt update && apt install -y git

WORKDIR /build

COPY . .
RUN CGO_ENABLED=0 GOOS=$(go env GOOS) GOARCH=$(go env GOARCH) go build -tags kqueue -trimpath --ldflags "$(go run buildscripts/gen-ldflags.go)" -o minio 1>/dev/null

FROM gcr.io/distroless/static-debian12

ARG TARGETARCH
ARG RELEASE

COPY --from=build --chown=1000:1000 /build/minio /usr/bin/minio
COPY --from=build --chown=1000:1000 /build/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

USER 1000:1000
CMD ["minio"]
