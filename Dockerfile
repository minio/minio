FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2017-01-25T03:14:52Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-01-25T03-14-52Z -X github.com/minio/minio/cmd.CommitID=f8e4700a11065967242b8857045ce7dee607722d" && \
	mkdir -p /export/docker && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
