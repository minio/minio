#!/bin/bash

set -x

export MINIO_CI_CD=1
killall -9 minio

rm -rf ${HOME}/tmp/dist

scheme="http"
nr_servers=4

addr="localhost"
args=""
for ((i=0;i<$[${nr_servers}];i++)); do
    args="$args $scheme://$addr:$[9100+$i]/${HOME}/tmp/dist/path1/$i"
done

echo $args

for ((i=0;i<$[${nr_servers}];i++)); do
    (minio server --address ":$[9100+$i]" $args 2>&1 > /tmp/log$i.txt) &
done

sleep 10s

if [ ! -f ././mc ]; then
    wget --quiet -O ./mc https://dl.minio.io/client/mc/release/linux-amd64/./mc && \
       chmod +x mc
fi

set -e

export MC_HOST_minioadm=http://minioadmin:minioadmin@localhost:9100/

./mc ls minioadm/

./mc admin user add minioadm/ testuser testuser12345

./mc admin policy attach minioadm/ consoleAdmin --user=testuser

sleep 3s # let things settle a little

export MC_HOST_testadm=http://testuser:testuser12345@localhost:9100/

./mc ls testadm/

./mc admin user disable testadm/ minioadmin

sleep 3s # let things settle a little

set +e # do not fail right away, look for exit status.

./mc ls minioadm/
if [ $? -eq 0 ]; then
    echo "listing succeeded, 'minioadmin' was not disabled"
    exit 1
fi

set -e

./mc admin user enable testadm/ minioadmin # re-enable the root user

set +e

./mc ls minioadm/
if [ $? -ne 0 ]; then
    echo "listing must succeed, 'minioadmin' was not enabled"
    exit 1
fi



