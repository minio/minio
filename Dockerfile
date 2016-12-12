FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-12-12T23:44:33Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-12-12T23-44-33Z -X github.com/minio/minio/cmd.CommitID=2062add05f306841c1d681c40206fa64568c1fa0" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/docker/README.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
