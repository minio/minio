FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app
RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-10-07T01:16:39Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-10-07T01-16-39Z -X github.com/minio/minio/cmd.CommitID=b06a514ce0cf10f1d4c8bbc7b5cdbd956c567ab9" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["go-wrapper", "run"]
VOLUME ["/export"]
