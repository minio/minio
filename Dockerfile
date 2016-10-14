FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app
RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-10-14T04:00:39Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-10-14T04-00-39Z -X github.com/minio/minio/cmd.CommitID=8c59a4166865e65192e91986e8bee329851bbf7c" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["go-wrapper", "run"]
VOLUME ["/export"]
