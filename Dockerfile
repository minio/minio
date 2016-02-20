# use "make dockerimage" to build
FROM alpine:3.3
RUN apk add --no-cache ca-certificates
ADD minio.dockerimage /minio
ADD export /export
EXPOSE 9000
ENTRYPOINT ["/minio"]
CMD ["server", "/export"]
