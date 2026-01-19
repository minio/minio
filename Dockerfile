FROM minio/minio:latest

ARG TARGETARCH
ARG RELEASE

RUN chmod -R 777 /usr/bin

COPY ./minio-${TARGETARCH}.${RELEASE} /usr/bin/minio
COPY ./minio-${TARGETARCH}.${RELEASE}.minisig /usr/bin/minio.minisig
COPY ./minio-${TARGETARCH}.${RELEASE}.sha256sum /usr/bin/minio.sha256sum

COPY dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

CMD ["minio"]
