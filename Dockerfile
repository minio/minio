# use "make dockerimage" to build
FROM scratch
ADD minio.dockerimage /minio
ADD export /export
EXPOSE 9000
ENTRYPOINT ["/minio"]
CMD ["server", "/export"]
