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

sleep 10s

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001
export MC_HOST_siteb=http://minio:minio123@127.0.0.1:9004

./mc mb sitea/bucket

## Add ILM rules
./mc ilm rule add --expire-days "300" sitea/bucket
./mc ilm rule add --expire-delete-marker sitea/bucket

## Setup site replication
./mc admin replicate add sitea siteb --replicate-ilm-expiry

## Check ilm expiry flag
./mc admin replicate info sitea --json
flag=$(./mc admin replicate info sitea --json | jq '."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: expected ILM expiry replication not set"
	exit 1
fi

## Check if ILM expiry rules replicated
count=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules | length')
if [ $count -ne 1 ]; then
	echo "BUG: ILM expiry rules not replicated"
	exit 1
fi

./mc admin replicate status sitea
./mc admin replicate status siteb

## Check edit if ILM expiry rule and its replication
id=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[] | select(.Expiration.Days==300) | .ID' | sed 's/"//g')
./mc ilm edit --id "${id}" --expire-days "100" sitea/bucket
sleep 20
count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[] | select(.ID=="${id}") | .Expiration.Days')
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[] | select(.ID=="${id}") | .Expiration.Days')
if [ $count1 -ne 100 ]; then
	echo "BUG: expiration days not changed"
	exit 1
fi
if [ $count2 -ne 100 ]; then
	echo "BUG: modified ILM expiry rule not replicated"
	exit 1
fi

## Check replication of deleted ILM expiry rules
./mc ilm rule remove --id "${id}" sitea/bucket
sleep 20
count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[] | select(.ID=="${id}") | .ID' | wc -l)
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[] | select(.ID=="${id}") | .ID' | wc -l)
if [ $count1 -ne 0 ]; then
	echo "BUG: ILM rule not removed"
	exit 1
fi
if [ $count2 -ne 0 ]; then
	echo "BUG: removed ILM expiry rule not replicated"
	exit 1
fi

## Check disabling and re enabling of ILM expiry rules replication
depID=$(./mc admin replicate info sitea --json | jq '.sites[] | select(.name=="sitea") | .deploymentID' | sed 's/"//g')
./mc admin replicate update sitea --deployment-id "${depID}" --disable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '."replicate-ilm-expiry"')
if [ "$flag" != "false" ]; then
	echo "BUG: ILM expiry replication not disabled"
	exit 1
fi
./mc admin replicate update sitea --deployment-id "${depID}" --enable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled"
	exit 1
fi

catch
