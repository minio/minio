FROM golang:1.6-alpine

WORKDIR /go/src/app
ENV ALLOW_CONTAINER_ROOT=1

COPY . /go/src/app
RUN \
	apk add --no-cache git && \
	go-wrapper download && \
	go-wrapper install && \
	mkdir -p /export/docker && \
	cp /go/src/app/docs/Docker.md /export/docker/ && \
	rm -rf /go/pkg /go/src && \
	apk del git

EXPOSE 9000
ENTRYPOINT ["go-wrapper", "run", "server"]
VOLUME ["/export"]
CMD ["/export"]
