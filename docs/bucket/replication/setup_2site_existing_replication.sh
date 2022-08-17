#!/usr/bin/env bash

set -x

trap 'catch $LINENO' ERR

# shellcheck disable=SC2120
catch() {
    if [ $# -ne 0 ]; then
       echo "error on line $1"
       for site in sitea siteb; do
           echo "$site server logs ========="
           cat "/tmp/${site}_1.log"
           echo "==========================="
           cat "/tmp/${site}_2.log"
       done
    fi

    echo "Cleaning up instances of MinIO"
    pkill minio
    pkill -9 minio
    rm -rf /tmp/multisitea
    rm -rf /tmp/multisiteb
    rm -rf /tmp/data
}

catch

set -e
export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"
export MINIO_KMS_AUTO_ENCRYPTION=off
export MINIO_PROMETHEUS_AUTH_TYPE=public
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
unset MINIO_KMS_KES_CERT_FILE
unset MINIO_KMS_KES_KEY_FILE
unset MINIO_KMS_KES_ENDPOINT
unset MINIO_KMS_KES_KEY_NAME

if [ ! -f ./mc ]; then
    wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc && \
        chmod +x mc
fi

minio server --address 127.0.0.1:9001 "http://127.0.0.1:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
      "http://127.0.0.1:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_1.log 2>&1 &
minio server --address 127.0.0.1:9002 "http://127.0.0.1:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
      "http://127.0.0.1:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_2.log 2>&1 &

minio server --address 127.0.0.1:9003 "http://127.0.0.1:9003/tmp/multisiteb/data/disterasure/xl{1...4}" \
      "http://127.0.0.1:9004/tmp/multisiteb/data/disterasure/xl{5...8}" >/tmp/siteb_1.log 2>&1 &
minio server --address 127.0.0.1:9004 "http://127.0.0.1:9003/tmp/multisiteb/data/disterasure/xl{1...4}" \
      "http://127.0.0.1:9004/tmp/multisiteb/data/disterasure/xl{5...8}" >/tmp/siteb_2.log 2>&1 &

sleep 10s

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001
export MC_HOST_siteb=http://minio:minio123@127.0.0.1:9004

./mc mb sitea/bucket

## Create 100 files
mkdir -p /tmp/data
for i in $(seq 1 10); do
    echo "T" > /tmp/data/file_${i}.txt
done

./mc mirror /tmp/data sitea/bucket/
./mc version enable sitea/bucket

./mc cp /tmp/data/file_1.txt sitea/bucket/marker
./mc rm sitea/bucket/marker

./mc mb siteb/bucket/
./mc version enable siteb/bucket/

echo "adding replication config for site a -> site b"
remote_arn=$(./mc admin bucket remote add sitea/bucket/ \
   http://minio:minio123@127.0.0.1:9004/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
echo "adding replication rule for a -> b : ${remote_arn}"

./mc replicate add sitea/bucket/ \
   --remote-bucket "${remote_arn}"
sleep 1

./mc replicate resync start sitea/bucket/ --remote-bucket "${remote_arn}"
sleep 10s ## sleep for 10s idea is that we give 100ms per object.

count=$(./mc replicate resync status sitea/bucket --remote-bucket "${remote_arn}" --json | jq .resyncInfo.target[].replicationCount)

./mc ls -r --versions sitea/bucket > /tmp/sitea.txt
./mc ls -r --versions siteb/bucket > /tmp/siteb.txt

out=$(diff -qpruN /tmp/sitea.txt /tmp/siteb.txt)
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected no missing entries after replication: $out"
    exit 1
fi

if [ $count -ne 12 ]; then
    echo "resync not complete after 10s unexpected failure"
    ./mc diff sitea/bucket siteb/bucket
    exit 1
fi

./mc cp /tmp/data/file_1.txt sitea/bucket/marker_new
./mc rm sitea/bucket/marker_new

sleep 12s ## sleep for 12s idea is that we give 100ms per object.

./mc ls -r --versions sitea/bucket > /tmp/sitea.txt
./mc ls -r --versions siteb/bucket > /tmp/siteb.txt

out=$(diff -qpruN /tmp/sitea.txt /tmp/siteb.txt)
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected no 'diff' after replication: $out"
    exit 1
fi

./mc rm -r --force --versions sitea/bucket/marker
sleep 14s ## sleep for 14s idea is that we give 100ms per object.

./mc ls -r --versions sitea/bucket > /tmp/sitea.txt
./mc ls -r --versions siteb/bucket > /tmp/siteb.txt

out=$(diff -qpruN /tmp/sitea.txt /tmp/siteb.txt)
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected no 'diff' after replication: $out"
    exit 1
fi

catch
