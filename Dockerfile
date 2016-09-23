FROM golang:1.7-alpine

WORKDIR /go/src/app
ENV ALLOW_CONTAINER_ROOT=1

COPY . /go/src/app
RUN \
	apk add --no-cache git bash && \
	go-wrapper download && \
        go-wrapper install -ldflags "`./buildscripts/docker-flags.sh`" && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git bash

EXPOSE 9000
ENTRYPOINT ["minio", "server"]
VOLUME ["/export"]
CMD ["/export"]
