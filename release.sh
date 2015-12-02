#!/bin/bash

echo -n "Making official release binaries.. "
export MINIO_RELEASE=OFFICIAL
make 1>/dev/null
echo "Binaries built at ${GOPATH}/bin/minio"
