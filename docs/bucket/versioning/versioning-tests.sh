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
	if [ $# -ne 0 ]; then
		exit $#
	fi
}

catch

set -e
export MINIO_CI_CD=1
export MINIO_BROWSER=off
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

minio server -S /tmp/no-certs --address ":9001" "http://localhost:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
	"http://localhost:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_1.log 2>&1 &

minio server -S /tmp/no-certs --address ":9002" "http://localhost:9001/tmp/multisitea/data/disterasure/xl{1...4}" \
	"http://localhost:9002/tmp/multisitea/data/disterasure/xl{5...8}" >/tmp/sitea_2.log 2>&1 &

export MC_HOST_sitea=http://minioadmin:minioadmin@localhost:9002

./mc ready sitea

./mc mb sitea/delissue --insecure

./mc version enable sitea/delissue --insecure

echo hello | ./mc pipe sitea/delissue/hello --insecure

./mc version suspend sitea/delissue --insecure

./mc rm sitea/delissue/hello --insecure

./mc version enable sitea/delissue --insecure

echo hello | ./mc pipe sitea/delissue/hello --insecure

./mc version suspend sitea/delissue --insecure

./mc rm sitea/delissue/hello --insecure

count=$(./mc ls --versions sitea/delissue --insecure | wc -l)

if [ ${count} -ne 3 ]; then
	echo "BUG: expected number of versions to be '3' found ${count}"
	echo "===== DEBUG ====="
	./mc ls --versions sitea/delissue
fi

./mc mb sitea/testbucket

./mc version enable sitea/testbucket

./mc put --quiet README.md sitea/testbucket/file
etag1=$(./mc cat sitea/testbucket/file | md5sum --tag | awk {'print $4'})

./mc cp --quiet --storage-class "STANDARD" sitea/testbucket/file sitea/testbucket/file
etag2=$(./mc cat sitea/testbucket/file | md5sum --tag | awk {'print $4'})
if [ $etag1 != $etag2 ]; then
	echo "expected $etag1, got $etag2"
	exit 1
fi

echo "SUCCESS:"
./mc ls --versions sitea/delissue --insecure

catch
