#!/bin/sh

set -e

reply=$(curl -s -o /dev/null -w %{http_code} --connect-timeout 3 http://127.0.0.1:9000/minio/health/live)
if [ "$reply" != 200 ]; then
  exit 1;
fi

mc alias set local http://localhost:9000/ $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

reply=$(mc ls local --debug 2>&1)
if [[ ! "$reply" =~ "HTTP/1.1 200 OK" ]]; then
  exit 1;
fi
