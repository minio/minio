FROM golang:1.6

RUN mkdir -p /go/src/app
WORKDIR /go/src/app

COPY . /go/src/app
RUN go-wrapper download
RUN go-wrapper install

ENV ALLOW_CONTAINER_ROOT=1
RUN mkdir -p /export/docker && cp /go/src/app/Docker.md /export/docker/

EXPOSE 9000
ENTRYPOINT ["go-wrapper", "run", "server"]
CMD ["/export"]
