FROM golang:1.7-alpine

WORKDIR /go/src/app

COPY . /go/src/app

RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install -ldflags "-X github.com/minio/minio/cmd.Version=2017-02-15T22:55:24Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-02-15T22-55-24Z -X github.com/minio/minio/cmd.CommitID=3e770defae48e86f4406209d3942dcac85cba73a" && \
	mkdir -p /export/docker && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
