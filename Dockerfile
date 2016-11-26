FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-11-26T02:23:47Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-11-26T02-23-47Z -X github.com/minio/minio/cmd.CommitID=9625629fc7c4660912037ec3db684a3ad84dec32" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/docker/README.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
