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
     go install -v -ldflags "-X github.com/minio/minio/cmd.Version=2017-04-25T01:27:49Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-04-25T01-27-49Z -X github.com/minio/minio/cmd.CommitID=710db6bdadb1b228bf6dee7e2a6958000091125b" && \
     rm -rf /go/pkg /go/src /usr/local/go && apk del .build-deps

EXPOSE 9000
ENTRYPOINT ["minio"]
VOLUME ["/export"]
