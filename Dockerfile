FROM alpine:3.5

ENV GOPATH /go
ENV PATH $PATH:$GOPATH/bin
ENV CGO_ENABLED 0

WORKDIR /go/src/github.com/minio/

RUN  \
     apk add --no-cache ca-certificates && \
     apk add --no-cache --virtual .build-deps git go musl-dev && \
     go get -v -d github.com/minio/minio && \
     cd /go/src/github.com/minio/minio && \
     go install -v -ldflags "-X github.com/minio/minio/cmd.Version=2017-03-16T21:50:32Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-03-16T21-50-32Z -X github.com/minio/minio/cmd.CommitID=5311eb22fd681a8cd4a46e2a872d46c2352c64e8" && \
     rm -rf /go/pkg /go/src /usr/local/go && apk del .build-deps

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
