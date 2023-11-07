#!/usr/bin/env bash

set -x

trap 'catch $LINENO' ERR

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
		for site in sitea siteb sitec; do
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
	rm -rf /tmp/multisitec
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

minio server --address 127.0.0.1:9005 "http://127.0.0.1:9005/tmp/multisitec/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9006/tmp/multisitec/data/disterasure/xl{5...8}" >/tmp/sitec_1.log 2>&1 &
minio server --address 127.0.0.1:9006 "http://127.0.0.1:9005/tmp/multisitec/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9006/tmp/multisitec/data/disterasure/xl{5...8}" >/tmp/sitec_2.log 2>&1 &

# Wait to make sure all MinIO instances are up
sleep 20s

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001
export MC_HOST_siteb=http://minio:minio123@127.0.0.1:9004
export MC_HOST_sitec=http://minio:minio123@127.0.0.1:9006

./mc mb sitea/bucket
./mc mb sitec/bucket

## Setup site replication
./mc admin replicate add sitea siteb --replicate-ilm-expiry

## Add warm tier
./mc ilm tier add minio sitea WARM-TIER --endpoint http://localhost:9006 --access-key minio --secret-key minio123 --bucket bucket

## Add ILM rules
./mc ilm add sitea/bucket --transition-days 0 --transition-tier WARM-TIER --transition-days 0 --noncurrent-expire-days 2 --expire-days 3
./mc ilm rule list sitea/bucket

## Check ilm expiry flag
./mc admin replicate info sitea --json
flag1=$(./mc admin replicate info sitea --json | jq '.sites[0]."replicate-ilm-expiry"')
flag2=$(./mc admin replicate info sitea --json | jq '.sites[1]."replicate-ilm-expiry"')
if [ "$flag1" != "true" ]; then
	echo "BUG: expected ILM expiry replication not set for site1"
	exit 1
fi
if [ "$flag2" != "true" ]; then
	echo "BUG: expected ILM expiry replication not set for site2"
	exit 1
fi

## Check if ILM expiry rules replicated
sleep 20
./mc ilm rule list siteb/bucket
count=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules | length')
if [ $count -ne 1 ]; then
	echo "BUG: ILM expiry rules not replicated to siteb"
	exit 1
fi

## Check edit if ILM expiry rule and its replication
id=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[] | select(.Expiration.Days==3) | .ID' | sed 's/"//g')
./mc ilm edit --id "${id}" --expire-days "100" sitea/bucket
sleep 30
count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Expiration.Days')
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Expiration.Days')
if [ $count1 -ne 100 ]; then
	echo "BUG: expiration days not changed on sitea"
	exit 1
fi
if [ $count2 -ne 100 ]; then
	echo "BUG: modified ILM expiry rule not replicated to siteb"
	exit 1
fi

## Check disabling of ILM expiry rules replication
./mc admin replicate update sitea --disable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "false" ]; then
	echo "BUG: ILM expiry replication not disabled for sitea"
	exit 1
fi
flag=$(./mc admin replicate info siteb --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "false" ]; then
	echo "BUG: ILM expiry replication not disabled for sitea"
	exit 1
fi

## Perform individual updates of rules to sites
./mc ilm edit --id "${id}" --expire-days "999" sitea/bucket
sleep 1
./mc ilm edit --id "${id}" --expire-days "888" siteb/bucket # when ilm expiry re-enabled, this should win

## Check re-enabling of ILM expiry rules replication
./mc admin replicate update sitea --enable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled for sitea"
	exit 1
fi
flag=$(./mc admin replicate info siteb --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled for sitea"
	exit 1
fi

## Check if latest updated rules get replicated to all sites post re-enable of ILM expiry rules replication
sleep 30
count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Expiration.Days')
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Expiration.Days')
if [ $count1 -ne 888 ]; then
	echo "BUG: latest expiration days not updated on sitea"
	exit 1
fi
if [ $count2 -ne 888 ]; then
	echo "BUG: latest expiration days not updated on siteb"
	exit 1
fi

## Check replication of deleted ILM expiry rules
./mc ilm rule remove --id "${id}" sitea/bucket
sleep 30
# should error as rule doesnt exist
error=$(./mc ilm rule list siteb/bucket --json | jq '.error.cause.message' | sed 's/"//g')
if [ "$error" != "The lifecycle configuration does not exist" ]; then
	echo "BUG: removed ILM expiry rule not replicated to siteb"
	exit 1
fi

catch
