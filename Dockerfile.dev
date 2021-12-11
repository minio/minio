FROM minio/minio:latest

COPY ./minio /opt/bin/minio

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

CMD ["minio"]
