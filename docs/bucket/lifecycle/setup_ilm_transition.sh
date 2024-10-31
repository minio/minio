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
	if [ $# -ne 0 ]; then
		exit $#
	fi
}

catch

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
	wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
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

# Wait to make sure all MinIO instances are up

export MC_HOST_sitea=http://minioadmin:minioadmin@127.0.0.1:9001
export MC_HOST_siteb=http://minioadmin:minioadmin@127.0.0.1:9004

./mc ready sitea
./mc ready siteb

./mc mb --ignore-existing sitea/bucket
./mc mb --ignore-existing siteb/bucket

sleep 10s

## Add warm tier
./mc ilm tier add minio sitea WARM-TIER --endpoint http://localhost:9004 --access-key minioadmin --secret-key minioadmin --bucket bucket

## Add ILM rules
./mc ilm add sitea/bucket --transition-days 0 --transition-tier WARM-TIER
./mc ilm rule list sitea/bucket

./mc cp README.md sitea/bucket/README.md

until $(./mc stat sitea/bucket/README.md --json | jq -r '.metadata."X-Amz-Storage-Class"' | grep -q WARM-TIER); do
	echo "waiting until the object is tiered to run heal"
	sleep 1s
done
./mc stat sitea/bucket/README.md

success=$(./mc admin heal -r sitea/bucket/README.md --json --force | jq -r 'select((.name == "bucket/README.md") and (.after.color == "green")) | .after.color == "green"')
if [ "${success}" != "true" ]; then
	echo "Found bug expected transitioned object to report 'green'"
	exit 1
fi

catch
