#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	echo "minio1 ============"
	cat /tmp/minio1_1.log
	cat /tmp/minio1_2.log
	echo "minio2 ============"
	cat /tmp/minio2_1.log
	cat /tmp/minio2_2.log
	echo "minio3 ============"
	cat /tmp/minio3_1.log
	cat /tmp/minio3_2.log

	exit 1
}

cleanup() {
	echo "Cleaning up instances of MinIO"
	pkill minio
	pkill -9 minio
	rm -rf /tmp/minio-internal-idp{1,2,3}
}

cleanup

unset MINIO_KMS_KES_CERT_FILE
unset MINIO_KMS_KES_KEY_FILE
unset MINIO_KMS_KES_ENDPOINT
unset MINIO_KMS_KES_KEY_NAME

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"
export MINIO_KMS_AUTO_ENCRYPTION=off
export MINIO_PROMETHEUS_AUTH_TYPE=public
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=

if [ ! -f ./mc ]; then
	wget -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

minio server --config-dir /tmp/minio-internal --address ":9001" http://localhost:9001/tmp/minio-internal-idp1/{1...4} http://localhost:9010/tmp/minio-internal-idp1/{5...8} >/tmp/minio1_1.log 2>&1 &
site1_pid1=$!
minio server --config-dir /tmp/minio-internal --address ":9010" http://localhost:9001/tmp/minio-internal-idp1/{1...4} http://localhost:9010/tmp/minio-internal-idp1/{5...8} >/tmp/minio1_2.log 2>&1 &
site1_pid2=$!

minio server --config-dir /tmp/minio-internal --address ":9002" http://localhost:9002/tmp/minio-internal-idp2/{1...4} http://localhost:9020/tmp/minio-internal-idp2/{5...8} >/tmp/minio2_1.log 2>&1 &
site2_pid1=$!
minio server --config-dir /tmp/minio-internal --address ":9020" http://localhost:9002/tmp/minio-internal-idp2/{1...4} http://localhost:9020/tmp/minio-internal-idp2/{5...8} >/tmp/minio2_2.log 2>&1 &
site2_pid2=$!

minio server --config-dir /tmp/minio-internal --address ":9003" http://localhost:9003/tmp/minio-internal-idp3/{1...4} http://localhost:9030/tmp/minio-internal-idp3/{5...8} >/tmp/minio3_1.log 2>&1 &
site3_pid1=$!
minio server --config-dir /tmp/minio-internal --address ":9030" http://localhost:9003/tmp/minio-internal-idp3/{1...4} http://localhost:9030/tmp/minio-internal-idp3/{5...8} >/tmp/minio3_2.log 2>&1 &
site3_pid2=$!

export MC_HOST_minio1=http://minio:minio123@localhost:9001
export MC_HOST_minio2=http://minio:minio123@localhost:9002
export MC_HOST_minio3=http://minio:minio123@localhost:9003

export MC_HOST_minio10=http://minio:minio123@localhost:9010
export MC_HOST_minio20=http://minio:minio123@localhost:9020
export MC_HOST_minio30=http://minio:minio123@localhost:9030

./mc ready minio1
./mc ready minio2
./mc ready minio3
./mc ready minio10
./mc ready minio20
./mc ready minio30

./mc admin replicate add minio1 minio2

site_enabled=$(./mc admin replicate info minio1)
site_enabled_peer=$(./mc admin replicate info minio10)

[[ $site_enabled =~ "is not enabled" ]] && {
	echo "expected both peers to have same information"
	exit_1
}

[[ $site_enabled_peer =~ "is not enabled" ]] && {
	echo "expected both peers to have same information"
	exit_1
}

./mc admin user add minio1 foobar foo12345

## add foobar-g group with foobar
./mc admin group add minio2 foobar-g foobar

./mc admin policy attach minio1 consoleAdmin --user=foobar
sleep 5

./mc admin user info minio2 foobar

./mc admin group info minio1 foobar-g

./mc admin policy create minio1 rw ./docs/site-replication/rw.json

sleep 5
./mc admin policy info minio2 rw >/dev/null 2>&1

./mc admin replicate status minio1

## Add a new empty site
./mc admin replicate add minio1 minio2 minio3

sleep 10

./mc admin policy info minio3 rw >/dev/null 2>&1

./mc admin policy remove minio3 rw

./mc admin replicate status minio3

sleep 10

./mc admin policy info minio1 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin policy info minio2 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin policy info minio3 rw
if [ $? -eq 0 ]; then
	echo "expecting the command to fail, exiting.."
	exit_1
fi

./mc admin user info minio1 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'minio1', exiting.."
	exit_1
fi

./mc admin user info minio2 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'minio2', exiting.."
	exit_1
fi

./mc admin user info minio3 foobar
if [ $? -ne 0 ]; then
	echo "policy mapping missing on 'minio3', exiting.."
	exit_1
fi

./mc admin group info minio3 foobar-g
if [ $? -ne 0 ]; then
	echo "group mapping missing on 'minio3', exiting.."
	exit_1
fi

./mc admin user svcacct add minio2 foobar --access-key testsvc --secret-key testsvc123
if [ $? -ne 0 ]; then
	echo "adding svc account failed, exiting.."
	exit_1
fi

./mc admin user svcacct add minio2 minio --access-key testsvc2 --secret-key testsvc123
if [ $? -ne 0 ]; then
	echo "adding root svc account testsvc2 failed, exiting.."
	exit_1
fi

sleep 10

export MC_HOST_rootsvc=http://testsvc2:testsvc123@localhost:9002
./mc ls rootsvc
if [ $? -ne 0 ]; then
	echo "root service account not inherited root permissions, exiting.."
	exit_1
fi

./mc admin user svcacct info minio1 testsvc
if [ $? -ne 0 ]; then
	echo "svc account not mirrored, exiting.."
	exit_1
fi

./mc admin user svcacct info minio2 testsvc
if [ $? -ne 0 ]; then
	echo "svc account not mirrored, exiting.."
	exit_1
fi

./mc admin user svcacct rm minio1 testsvc
if [ $? -ne 0 ]; then
	echo "removing svc account failed, exiting.."
	exit_1
fi

sleep 10
./mc admin user svcacct info minio2 testsvc
if [ $? -eq 0 ]; then
	echo "svc account found after delete, exiting.."
	exit_1
fi

./mc admin user svcacct info minio3 testsvc
if [ $? -eq 0 ]; then
	echo "svc account found after delete, exiting.."
	exit_1
fi

./mc mb minio1/newbucket
# copy large upload to newbucket on minio1
truncate -s 17M lrgfile
expected_checksum=$(cat ./lrgfile | md5sum)

./mc cp ./lrgfile minio1/newbucket

sleep 5
./mc stat --no-list minio2/newbucket
if [ $? -ne 0 ]; then
	echo "expecting bucket to be present. exiting.."
	exit_1
fi

./mc stat --no-list minio3/newbucket
if [ $? -ne 0 ]; then
	echo "expecting bucket to be present. exiting.."
	exit_1
fi

err_minio2=$(./mc stat --no-list minio2/newbucket/xxx --json | jq -r .error.cause.message)
if [ $? -ne 0 ]; then
	echo "expecting object to be missing. exiting.."
	exit_1
fi

if [ "${err_minio2}" != "Object does not exist" ]; then
	echo "expected to see Object does not exist error, exiting..."
	exit_1
fi

./mc cp README.md minio2/newbucket/

sleep 5
./mc stat --no-list minio1/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

./mc stat --no-list minio3/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

sleep 10
./mc stat --no-list minio3/newbucket/lrgfile
if [ $? -ne 0 ]; then
	echo "expected object to be present, exiting.."
	exit_1
fi

actual_checksum=$(./mc cat minio3/newbucket/lrgfile | md5sum)
if [ "${expected_checksum}" != "${actual_checksum}" ]; then
	echo "replication failed on multipart objects expected ${expected_checksum} got ${actual_checksum}"
	exit
fi
rm ./lrgfile

./mc rm -r --versions --force minio1/newbucket/lrgfile
if [ $? -ne 0 ]; then
	echo "expected object to be present, exiting.."
	exit_1
fi

sleep 5
./mc stat --no-list minio1/newbucket/lrgfile
if [ $? -eq 0 ]; then
	echo "expected object to be deleted permanently after replication, exiting.."
	exit_1
fi

vID=$(./mc stat --no-list minio2/newbucket/README.md --json | jq .versionID)
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi
./mc tag set --version-id "${vID}" minio2/newbucket/README.md "key=val"
if [ $? -ne 0 ]; then
	echo "expecting tag set to be successful. exiting.."
	exit_1
fi
sleep 5
val=$(./mc tag list minio1/newbucket/README.md --version-id "${vID}" --json | jq -r .tagset.key)
if [ "${val}" != "val" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi
./mc tag remove --version-id "${vID}" minio2/newbucket/README.md
if [ $? -ne 0 ]; then
	echo "expecting tag removal to be successful. exiting.."
	exit_1
fi
sleep 5

replStatus_minio2=$(./mc stat --no-list minio2/newbucket/README.md --json | jq -r .replicationStatus)
if [ $? -ne 0 ]; then
	echo "expecting object to be present. exiting.."
	exit_1
fi

if [ ${replStatus_minio2} != "COMPLETED" ]; then
	echo "expected tag removal to have replicated, exiting..."
	exit_1
fi

./mc rm minio3/newbucket/README.md
sleep 5

./mc stat --no-list minio2/newbucket/README.md
if [ $? -eq 0 ]; then
	echo "expected file to be deleted, exiting.."
	exit_1
fi

./mc stat --no-list minio1/newbucket/README.md
if [ $? -eq 0 ]; then
	echo "expected file to be deleted, exiting.."
	exit_1
fi

./mc mb --with-lock minio3/newbucket-olock
sleep 5

set -x

enabled_minio2=$(./mc stat --json minio2/newbucket-olock | jq -r .ObjectLock.enabled)
if [ $? -ne 0 ]; then
	echo "expected bucket to be mirrored with object-lock but not present, exiting..."
	exit_1
fi

if [ "${enabled_minio2}" != "Enabled" ]; then
	echo "expected bucket to be mirrored with object-lock enabled, exiting..."
	exit_1
fi

enabled_minio1=$(./mc stat --json minio1/newbucket-olock | jq -r .ObjectLock.enabled)
if [ $? -ne 0 ]; then
	echo "expected bucket to be mirrored with object-lock but not present, exiting..."
	exit_1
fi

if [ "${enabled_minio1}" != "Enabled" ]; then
	echo "expected bucket to be mirrored with object-lock enabled, exiting..."
	exit_1
fi

set +x

# "Test if most recent tag update is replicated"
./mc tag set minio2/newbucket "key=val1"
if [ $? -ne 0 ]; then
	echo "expecting tag set to be successful. exiting.."
	exit_1
fi
sleep 5

val=$(./mc tag list minio1/newbucket --json | jq -r .tagset | jq -r .key)
if [ "${val}" != "val1" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi
# Create user with policy consoleAdmin on minio1
./mc admin user add minio1 foobarx foobar123
if [ $? -ne 0 ]; then
	echo "adding user failed, exiting.."
	exit_1
fi
./mc admin policy attach minio1 consoleAdmin --user=foobarx
if [ $? -ne 0 ]; then
	echo "adding policy mapping failed, exiting.."
	exit_1
fi
sleep 10

# unset policy for foobarx in minio2
./mc admin policy detach minio2 consoleAdmin --user=foobarx
if [ $? -ne 0 ]; then
	echo "unset policy mapping failed, exiting.."
	exit_1
fi

# create a bucket bucket2 on minio1.
./mc mb minio1/bucket2

sleep 10

# Test whether policy detach replicated to minio1
policy=$(./mc admin user info minio1 foobarx --json | jq -r .policyName)
if [ "${policy}" != "null" ]; then
	echo "expected policy detach to have replicated, exiting..."
	exit_1
fi

kill -9 ${site1_pid1} ${site1_pid2}

# Update tag on minio2/newbucket when minio1 is down
./mc tag set minio2/newbucket "key=val2"
# create a new bucket on minio2. This should replicate to minio1 after it comes online.
./mc mb minio2/newbucket2

# delete bucket2 on minio2. This should replicate to minio1 after it comes online.
./mc rb minio2/bucket2

# Restart minio1 instance
minio server --config-dir /tmp/minio-internal --address ":9001" http://localhost:9001/tmp/minio-internal-idp1/{1...4} http://localhost:9010/tmp/minio-internal-idp1/{5...8} >/tmp/minio1_1.log 2>&1 &
minio server --config-dir /tmp/minio-internal --address ":9010" http://localhost:9001/tmp/minio-internal-idp1/{1...4} http://localhost:9010/tmp/minio-internal-idp1/{5...8} >/tmp/minio1_2.log 2>&1 &
sleep 200

# Test whether most recent tag update on minio2 is replicated to minio1
val=$(./mc tag list minio1/newbucket --json | jq -r .tagset | jq -r .key)
if [ "${val}" != "val2" ]; then
	echo "expected bucket tag to have replicated, exiting..."
	exit_1
fi

# Test if bucket created/deleted when minio1 is down healed
diff -q <(./mc ls minio1) <(./mc ls minio2) 1>/dev/null
if [ $? -ne 0 ]; then
	echo "expected 'bucket2' delete and 'newbucket2' creation to have replicated, exiting..."
	exit_1
fi

# force a resync after removing all site replication
./mc admin replicate rm --all --force minio1
./mc rb minio2 --force --dangerous
./mc admin replicate add minio1 minio2
./mc admin replicate resync start minio1 minio2
sleep 30

./mc ls -r --versions minio1/newbucket >/tmp/minio1.txt
./mc ls -r --versions minio2/newbucket >/tmp/minio2.txt

out=$(diff -qpruN /tmp/minio1.txt /tmp/minio2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after replication resync: $out"
	exit 1
fi
