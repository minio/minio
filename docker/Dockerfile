# use "make dockerimage" to build
FROM alpine:3.3
RUN apk add --no-cache ca-certificates bash
ADD minio.dockerimage /minio
ADD export /export
EXPOSE 9000
COPY start.sh /start.sh
ENV ALLOW_CONTAINER_ROOT=1
ENTRYPOINT ["/start.sh"]
CMD ["/export"]
