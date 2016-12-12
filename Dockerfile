FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2016-12-12T18:35:43Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2016-12-12T18-35-43Z -X github.com/minio/minio/cmd.CommitID=6b4e6bcebf1127f7b1444790580bb5b64f31c6e7" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/docker/README.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
