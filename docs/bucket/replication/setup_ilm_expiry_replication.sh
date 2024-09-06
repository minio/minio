#!/usr/bin/env bash

set -x

trap 'catch $LINENO' ERR

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
		for site in sitea siteb sitec sited; do
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
	rm -rf /tmp/multisited
	rm -rf /tmp/data
	if [ $# -ne 0 ]; then
		exit $#
	fi
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

minio server --address 127.0.0.1:9007 "http://127.0.0.1:9007/tmp/multisited/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9008/tmp/multisited/data/disterasure/xl{5...8}" >/tmp/sited_1.log 2>&1 &
minio server --address 127.0.0.1:9008 "http://127.0.0.1:9007/tmp/multisited/data/disterasure/xl{1...4}" \
	"http://127.0.0.1:9008/tmp/multisited/data/disterasure/xl{5...8}" >/tmp/sited_2.log 2>&1 &

# Wait to make sure all MinIO instances are up

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001
export MC_HOST_siteb=http://minio:minio123@127.0.0.1:9004
export MC_HOST_sitec=http://minio:minio123@127.0.0.1:9006
export MC_HOST_sited=http://minio:minio123@127.0.0.1:9008

./mc ready sitea
./mc ready siteb
./mc ready sitec
./mc ready sited

./mc mb sitea/bucket
./mc mb sitec/bucket

## Setup site replication
./mc admin replicate add sitea siteb --replicate-ilm-expiry

sleep 10s

## Add warm tier
./mc ilm tier add minio sitea WARM-TIER --endpoint http://localhost:9006 --access-key minio --secret-key minio123 --bucket bucket

## Add ILM rules
./mc ilm add sitea/bucket --transition-days 0 --transition-tier WARM-TIER --transition-days 0 --noncurrent-expire-days 2 --expire-days 3 --prefix "myprefix" --tags "tag1=val1&tag2=val2"
./mc ilm rule list sitea/bucket

## Check ilm expiry flag
./mc admin replicate info sitea --json
flag1=$(./mc admin replicate info sitea --json | jq '.sites[0]."replicate-ilm-expiry"')
flag2=$(./mc admin replicate info sitea --json | jq '.sites[1]."replicate-ilm-expiry"')
if [ "$flag1" != "true" ]; then
	echo "BUG: Expected ILM expiry replication not set for 'sitea'"
	exit 1
fi
if [ "$flag2" != "true" ]; then
	echo "BUG: Expected ILM expiry replication not set for 'siteb'"
	exit 1
fi

## Check if ILM expiry rules replicated
sleep 30s

./mc ilm rule list siteb/bucket
count=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules | length')
if [ $count -ne 1 ]; then
	echo "BUG: ILM expiry rules not replicated to 'siteb'"
	exit 1
fi

## Check replication of rules content
expDays=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Expiration.Days')
noncurrentDays=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].NoncurrentVersionExpiration.NoncurrentDays')
if [ $expDays -ne 3 ]; then
	echo "BUG: Incorrect expiry days '${expDays}' set for 'siteb'"
	exit 1
fi
if [ $noncurrentDays -ne 2 ]; then
	echo "BUG: Incorrect non current expiry days '${noncurrentDays}' set for siteb"
	exit 1
fi

## Make sure transition rule not replicated to siteb
tranDays=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Transition.Days')
if [ "${tranDays}" != "null" ]; then
	echo "BUG: Transition rules as well copied to siteb"
	exit 1
fi

## Check replication of rules prefix and tags
prefix=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Prefix' | sed 's/"//g')
tagName1=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Key' | sed 's/"//g')
tagVal1=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Value' | sed 's/"//g')
tagName2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Key' | sed 's/"//g')
tagVal2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Value' | sed 's/"//g')
if [ "${prefix}" != "myprefix" ]; then
	echo "BUG: ILM expiry rules prefix not replicated to 'siteb'"
	exit 1
fi
if [ "${tagName1}" != "tag1" ] || [ "${tagVal1}" != "val1" ] || [ "${tagName2}" != "tag2" ] || [ "${tagVal2}" != "val2" ]; then
	echo "BUG: ILM expiry rules tags not replicated to 'siteb'"
	exit 1
fi

## Check edit of ILM expiry rule and its replication
id=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[] | select(.Expiration.Days==3) | .ID' | sed 's/"//g')
./mc ilm edit --id "${id}" --expire-days "100" sitea/bucket
sleep 30s

count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Expiration.Days')
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Expiration.Days')
if [ $count1 -ne 100 ]; then
	echo "BUG: Expiration days not changed on 'sitea'"
	exit 1
fi
if [ $count2 -ne 100 ]; then
	echo "BUG: Modified ILM expiry rule not replicated to 'siteb'"
	exit 1
fi

## Check disabling of ILM expiry rules replication
./mc admin replicate update sitea --disable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "false" ]; then
	echo "BUG: ILM expiry replication not disabled for 'sitea'"
	exit 1
fi
flag=$(./mc admin replicate info siteb --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "false" ]; then
	echo "BUG: ILM expiry replication not disabled for 'siteb'"
	exit 1
fi

## Perform individual updates of rules to sites
./mc ilm edit --id "${id}" --expire-days "999" sitea/bucket
sleep 5s

./mc ilm edit --id "${id}" --expire-days "888" siteb/bucket # when ilm expiry re-enabled, this should win

## Check re-enabling of ILM expiry rules replication
./mc admin replicate update sitea --enable-ilm-expiry-replication
flag=$(./mc admin replicate info sitea --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled for 'sitea'"
	exit 1
fi
flag=$(./mc admin replicate info siteb --json | jq '.sites[] | select (.name=="sitea") | ."replicate-ilm-expiry"')
if [ "$flag" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled for 'siteb'"
	exit 1
fi

## Check if latest updated rules get replicated to all sites post re-enable of ILM expiry rules replication
sleep 30s
count1=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Expiration.Days')
count2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Expiration.Days')
if [ $count1 -ne 888 ]; then
	echo "BUG: Latest expiration days not updated on 'sitea'"
	exit 1
fi
if [ $count2 -ne 888 ]; then
	echo "BUG: Latest expiration days not updated on 'siteb'"
	exit 1
fi

## Check to make sure sitea transition rule is not overwritten
transDays=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Transition.Days')
if [ $transDays -ne 0 ] || [ "${transDays}" == "null" ]; then
	echo "BUG: Transition rule on sitea seems to be overwritten"
	exit 1
fi

## Check replication of edit of prefix, tags and status of ILM Expiry Rules
./mc ilm rule edit --id "${id}" --prefix "newprefix" --tags "ntag1=nval1&ntag2=nval2" --disable sitea/bucket
sleep 30s

nprefix=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Prefix' | sed 's/"//g')
ntagName1=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Key' | sed 's/"//g')
ntagVal1=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Value' | sed 's/"//g')
ntagName2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Key' | sed 's/"//g')
ntagVal2=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Value' | sed 's/"//g')
st=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[0].Status' | sed 's/"//g')
if [ "${nprefix}" != "newprefix" ]; then
	echo "BUG: ILM expiry rules prefix not replicated to 'siteb'"
	exit 1
fi
if [ "${ntagName1}" != "ntag1" ] || [ "${ntagVal1}" != "nval1" ] || [ "${ntagName2}" != "ntag2" ] || [ "${ntagVal2}" != "nval2" ]; then
	echo "BUG: ILM expiry rules tags not replicated to 'siteb'"
	exit 1
fi
if [ "${st}" != "Disabled" ]; then
	echo "BUG: ILM expiry rules status not replicated to 'siteb'"
	exit 1
fi

## Check replication of deleted ILM expiry rules
./mc ilm rule remove --id "${id}" sitea/bucket
sleep 30s

# should error as rule doesn't exist
error=$(./mc ilm rule list siteb/bucket --json | jq '.error.cause.message' | sed 's/"//g')
if [ "$error" != "The lifecycle configuration does not exist" ]; then
	echo "BUG: Removed ILM expiry rule not replicated to 'siteb'"
	exit 1
fi

## Check addition of new replication site to existing site replication setup
# Add rules again as previous tests removed all
./mc ilm add sitea/bucket --transition-days 0 --transition-tier WARM-TIER --transition-days 0 --noncurrent-expire-days 2 --expire-days 3 --prefix "myprefix" --tags "tag1=val1&tag2=val2"
./mc admin replicate add sitea siteb sited
sleep 30s

# Check site replication info and status for new site
sitesCount=$(mc admin replicate info sited --json | jq '.sites | length')
if [ ${sitesCount} -ne 3 ]; then
	echo "BUG: New site 'sited' not appearing in site replication info"
	exit 1
fi
flag3=$(./mc admin replicate info sited --json | jq '.sites[2]."replicate-ilm-expiry"')
if [ "${flag3}" != "true" ]; then
	echo "BUG: ILM expiry replication not enabled for 'sited'"
	exit 1
fi
rulesCount=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules | length')
if [ ${rulesCount} -ne 1 ]; then
	echo "BUG: ILM expiry rules not replicated to 'sited'"
	exit 1
fi
prefix=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules[0].Filter.And.Prefix' | sed 's/"//g')
tagName1=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Key' | sed 's/"//g')
tagVal1=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules[0].Filter.And.Tags[0].Value' | sed 's/"//g')
tagName2=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Key' | sed 's/"//g')
tagVal2=$(./mc ilm rule list sited/bucket --json | jq '.config.Rules[0].Filter.And.Tags[1].Value' | sed 's/"//g')
if [ "${prefix}" != "myprefix" ]; then
	echo "BUG: ILM expiry rules prefix not replicated to 'sited'"
	exit 1
fi
if [ "${tagName1}" != "tag1" ] || [ "${tagVal1}" != "val1" ] || [ "${tagName2}" != "tag2" ] || [ "${tagVal2}" != "val2" ]; then
	echo "BUG: ILM expiry rules tags not replicated to 'sited'"
	exit 1
fi

## Check replication of deleted ILM expiry rules when target has transition part as well
## Only the expiry part of rules should get removed as part if replication of removal from
## other site
id=$(./mc ilm rule list siteb/bucket --json | jq '.config.Rules[] | select(.Expiration.Days==3) | .ID' | sed 's/"//g')
# Remove rule from siteb
./mc ilm rule remove --id "${id}" siteb/bucket
sleep 30s # allow to replicate

# sitea should still contain the transition portion of rule
transitionRuleDays=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Transition.Days')
expirationRuleDet=$(./mc ilm rule list sitea/bucket --json | jq '.config.Rules[0].Expiration')
if [ ${transitionRuleDays} -ne 0 ]; then
	echo "BUG: Transition rules not retained as part of replication of deleted ILM expiry rules on 'sitea'"
	exit 1
fi
if [ ${expirationRuleDet} != null ]; then
	echo "BUG: removed ILM expiry rule not replicated to 'sitea'"
	exit 1
fi

catch
