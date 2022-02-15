FROM minio/minio:latest

ENV PATH=/opt/bin:$PATH

COPY ./minio /opt/bin/minio
COPY dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

VOLUME ["/data","/data1","/data2","/data3","/data4"]

CMD ["minio"]
