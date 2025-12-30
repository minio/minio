FROM golang:1.24-alpine AS build

ARG TARGETARCH
ARG RELEASE

ENV GOPATH=/go
ENV CGO_ENABLED=0

COPY . .

WORKDIR /build

RUN LDFLAGS=$(go run buildscripts/gen-ldflags.go) CGO_ENABLED=0 GOOS=$(go env GOOS) GOARCH=$(go env GOARCH) go build -tags kqueue -trimpath --ldflags "$(LDFLAGS)" -o $(PWD)/minio 1>/dev/null

FROM minio/minio:latest

ARG TARGETARCH
ARG RELEASE

RUN chmod -R 777 /usr/bin

COPY --from=build /build/minio /usr/bin/minio

COPY dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

CMD ["minio"]
