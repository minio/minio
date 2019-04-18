FROM golang:1.12-alpine

LABEL maintainer="MinIO Inc <dev@min.io>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on

RUN  \
     apk add --no-cache git && \
     git clone https://github.com/minio/minio && cd minio && \
     go install -v -ldflags "$(go run buildscripts/gen-ldflags.go)" && \
     cd dockerscripts; go build -ldflags "-s -w" -o /usr/bin/healthcheck healthcheck.go

FROM alpine:3.9

ENV MINIO_UPDATE off
ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key

EXPOSE 9000

COPY --from=0 /go/bin/minio /usr/bin/minio
COPY --from=0 /usr/bin/healthcheck /usr/bin/healthcheck
COPY dockerscripts/docker-entrypoint.sh /usr/bin/

RUN  \
     apk add --no-cache ca-certificates 'curl>7.61.0' && \
     echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

HEALTHCHECK --interval=1m CMD healthcheck

CMD ["minio"]
