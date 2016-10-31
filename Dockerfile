FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-10-24T21:23:47Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-10-24T21-23-47Z -X github.com/minio/minio/cmd.CommitID=048af5e5cdc1344e83231c09079828a3d289e6df" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
