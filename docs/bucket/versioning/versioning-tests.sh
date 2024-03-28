#!/usr/bin/env bash

if [ -n "$TEST_DEBUG" ]; then
	set -x
fi

trap 'catch $LINENO' ERR

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
		echo "server logs ========="
		cat "/tmp/sitea_1.log"
		echo "==========================="
		cat "/tmp/sitea_2.log"
	fi

	echo "Cleaning up instances of MinIO"
	pkill minio
	pkill -9 minio
	rm -rf /tmp/multisitea
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
	wget -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

minio server --address 127.0.0.1:9001 "http://127.0.0.1:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_1.log 2>&1 &
minio server --address 127.0.0.1:9002 "http://127.0.0.1:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_2.log 2>&1 &

sleep 60

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001

./mc mb sitea/delissue

./mc version enable sitea/delissue

echo hello | ./mc pipe sitea/delissue/hello

./mc version suspend sitea/delissue

./mc rm sitea/delissue/hello

./mc version enable sitea/delissue

echo hello | ./mc pipe sitea/delissue/hello

./mc version suspend sitea/delissue

./mc rm sitea/delissue/hello

count=$(./mc ls --versions sitea/delissue | wc -l)

if [ ${count} -ne 3 ]; then
	echo "BUG: expected number of versions to be '3' found ${count}"
	echo "===== DEBUG ====="
	./mc ls --versions sitea/delissue
fi

echo "SUCCESS:"
./mc ls --versions sitea/delissue

catch
