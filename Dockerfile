FROM golang:1.16-alpine as builder

LABEL maintainer="MinIO Inc <dev@min.io>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on

RUN  \
     apk add --no-cache git && \
     git clone https://github.com/minio/minio && cd minio && \
     git checkout master && go install -v -ldflags "$(go run buildscripts/gen-ldflags.go)"

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.3

ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_UPDATE_MINISIGN_PUBKEY="RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav"

EXPOSE 9000

COPY --from=builder /go/bin/minio /usr/bin/minio
COPY --from=builder /go/minio/CREDITS /licenses/CREDITS
COPY --from=builder /go/minio/LICENSE /licenses/LICENSE
COPY --from=builder /go/minio/dockerscripts/docker-entrypoint.sh /usr/bin/

RUN  \
     microdnf update --nodocs && \
     microdnf install curl ca-certificates shadow-utils util-linux --nodocs && \
     microdnf clean all && \
     echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

CMD ["minio"]
