FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2017-01-25T02:49:01Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-01-25T02-49-01Z -X github.com/minio/minio/cmd.CommitID=d41dcb784b4a40639525c27841b9250c123dcb56" && \
	mkdir -p /export/docker && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
