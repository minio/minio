FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2017-02-16T01:47:30Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-02-16T01-47-30Z -X github.com/minio/minio/cmd.CommitID=3d98311d9f4ceb78dba996dcdc0751253241e697" && \
	mkdir -p /export/docker && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
