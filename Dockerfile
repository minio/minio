# MinIO Dockerfile - Build from source
FROM golang:1.24-alpine AS builder

ARG REPO_URL
ARG COMMIT_ID
ARG SHORT_COMMIT_ID
ARG RELEASE_TAG

ENV GOPATH=/go
ENV CGO_ENABLED=0

WORKDIR /workspace

RUN apk add -U --no-cache ca-certificates git curl bash

COPY . .

RUN VERSION_TIME=$(echo ${RELEASE_TAG} | sed 's/RELEASE\.//' | sed 's/T/ /' | sed 's/-/:/3' | sed 's/-/:/3' | sed 's/ /T/') && \
    COPYRIGHT_YEAR=$(date +%Y) && \
    echo "Building MinIO: $RELEASE_TAG ($SHORT_COMMIT_ID) from ${REPO_URL}" && \
    go build -tags kqueue -trimpath \
    -ldflags "-s -w \
    -X github.com/minio/minio/cmd.Version=${VERSION_TIME} \
    -X github.com/minio/minio/cmd.CopyrightYear=${COPYRIGHT_YEAR} \
    -X github.com/minio/minio/cmd.ReleaseTag=${RELEASE_TAG} \
    -X github.com/minio/minio/cmd.CommitID=${COMMIT_ID} \
    -X github.com/minio/minio/cmd.ShortCommitID=${SHORT_COMMIT_ID} \
    -X github.com/minio/minio/cmd.GOPATH=${GOPATH} \
    -X github.com/minio/minio/cmd.GOROOT=$(go env GOROOT)" \
    -o /usr/bin/minio .

RUN /usr/bin/minio --version

RUN curl -sL https://dl.min.io/client/mc/release/linux-$(go env GOARCH)/mc -o /usr/bin/mc && \
    chmod +x /usr/bin/mc

RUN chmod +x dockerscripts/download-static-curl.sh && \
    ./dockerscripts/download-static-curl.sh

FROM registry.access.redhat.com/ubi9/ubi-micro:latest

ARG REPO_URL

LABEL name="MinIO" \
      vendor="MinIO Inc <dev@min.io>" \
      maintainer="MinIO Inc <dev@min.io>" \
      summary="MinIO is a High Performance Object Storage, API compatible with Amazon S3 cloud storage service." \
      description="MinIO object storage is fundamentally different. Designed for performance and the S3 API, it is 100% open-source." \
      org.opencontainers.image.source="${REPO_URL}" \
      org.opencontainers.image.licenses="AGPL-3.0"

RUN chmod -R 777 /usr/bin

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/bin/minio /usr/bin/minio
COPY --from=builder /usr/bin/mc /usr/bin/mc
COPY --from=builder /go/bin/curl /usr/bin/curl
COPY --from=builder /workspace/CREDITS /licenses/CREDITS
COPY --from=builder /workspace/LICENSE /licenses/LICENSE
COPY --from=builder /workspace/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_UPDATE_MINISIGN_PUBKEY="RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav" \
    MINIO_CONFIG_ENV_FILE=config.env \
    MC_CONFIG_DIR=/tmp/.mc

EXPOSE 9000 9001

VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]