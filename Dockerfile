FROM alpine:3.6

MAINTAINER Minio Inc <dev@minio.io>

ENV GOPATH /go
ENV PATH $PATH:$GOPATH/bin
ENV CGO_ENABLED 0

COPY . /go/src/github.com/minio/minio
WORKDIR /go/src/github.com/minio/

# add our user and group first with fixed uid and gid, this ensures
# multiple updates to the minio image dont have permission issues.
RUN addgroup -g 190 -S minio && adduser -u 190 -S -G minio minio

# grab su-exec for easy step-down from root
RUN apk add --no-cache 'su-exec>=0.2'

RUN  \
     apk add --no-cache ca-certificates && \
     apk add --no-cache --virtual .build-deps git go musl-dev && \
     echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf && \
     cd /go/src/github.com/minio/minio && \
     go install -v -ldflags "$(go run buildscripts/gen-ldflags.go)" && \
     rm -rf /go/pkg /go/src /usr/local/go && apk del .build-deps

EXPOSE 9000

COPY buildscripts/docker-entrypoint.sh /usr/bin/

RUN chmod +x /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/export"]

CMD ["minio"]
