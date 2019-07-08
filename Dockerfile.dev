FROM alpine:3.9

LABEL maintainer="MinIO Inc <dev@min.io>"

COPY dockerscripts/docker-entrypoint.sh dockerscripts/healthcheck dockerscripts/check-user /usr/bin/
COPY minio /usr/bin/

ENV MINIO_UPDATE off
ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key

RUN \
     apk add --no-cache ca-certificates 'curl>7.61.0' 'su-exec>=0.2' && \
     echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf && \
     chmod +x /usr/bin/minio  && \
     chmod +x /usr/bin/docker-entrypoint.sh && \
     chmod +x /usr/bin/healthcheck && \
     chmod +x /usr/bin/check-user

EXPOSE 9000

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data"]

HEALTHCHECK --interval=1m CMD healthcheck

CMD ["minio"]
