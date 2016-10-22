FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app
RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-10-22T00:50:41Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-10-22T00-50-41Z -X github.com/minio/minio/cmd.CommitID=d1331ecc5cc4abc7304157cc26be64618821a77c" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["go-wrapper", "run"]
VOLUME ["/export"]
