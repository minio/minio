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
     git checkout release && \
     go install -v -ldflags "-X github.com/minio/minio/cmd.Version=2017-05-05T01:14:51Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2017-05-05T01-14-51Z -X github.com/minio/minio/cmd.CommitID=40985cc4e3eec06b7ea82dc34c8d907fd2e7aa12" && \
     rm -rf /go/pkg /go/src /usr/local/go && apk del .build-deps

EXPOSE 9000

COPY buildscripts/docker-entrypoint.sh /usr/bin/

RUN chmod +x /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/export"]

CMD ["minio"]
