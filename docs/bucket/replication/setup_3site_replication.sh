#!/usr/bin/env bash

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
}

catch

set -e
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"
export MINIO_PROMETHEUS_AUTH_TYPE=public
export PATH=${GOPATH}/bin:${PATH}

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

sleep 30

mc alias set sitea http://127.0.0.1:9001 minio minio123
mc mb sitea/bucket
mc version enable sitea/bucket
mc mb -l sitea/olockbucket

mc alias set siteb http://127.0.0.1:9004 minio minio123
mc mb siteb/bucket/
mc version enable siteb/bucket/
mc mb -l siteb/olockbucket/

mc alias set sitec http://127.0.0.1:9006 minio minio123
mc mb sitec/bucket/
mc version enable sitec/bucket/
mc mb -l sitec/olockbucket

echo "adding replication config for site a -> site b"
remote_arn=$(mc admin bucket remote add sitea/bucket/ \
   http://minio:minio123@127.0.0.1:9004/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
echo "adding replication rule for a -> b : ${remote_arn}"
sleep 1
mc replicate add sitea/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync"
sleep 1

echo "adding replication config for site b -> site a"
remote_arn=$(mc admin bucket remote add siteb/bucket/ \
   http://minio:minio123@127.0.0.1:9001/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for b -> a : ${remote_arn}"
mc replicate add siteb/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync"
sleep 1

echo "adding replication config for site a -> site c"
remote_arn=$(mc admin bucket remote add sitea/bucket/ \
   http://minio:minio123@127.0.0.1:9006/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for a -> c : ${remote_arn}"
mc replicate add sitea/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 2
sleep 1
echo "adding replication config for site c -> site a"
remote_arn=$(mc admin bucket remote add sitec/bucket/ \
   http://minio:minio123@127.0.0.1:9001/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for c -> a : ${remote_arn}"
mc replicate add sitec/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 2
sleep 1
echo "adding replication config for site b -> site c"
remote_arn=$(mc admin bucket remote add siteb/bucket/ \
   http://minio:minio123@127.0.0.1:9006/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for b -> c : ${remote_arn}"
mc replicate add siteb/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 3
sleep 1

echo "adding replication config for site c -> site b"
remote_arn=$(mc admin bucket remote add sitec/bucket \
   http://minio:minio123@127.0.0.1:9004/bucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for c -> b : ${remote_arn}"
mc replicate add sitec/bucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 3
sleep 1
echo "adding replication config for olockbucket site a -> site b"
remote_arn=$(mc admin bucket remote add sitea/olockbucket/ \
   http://minio:minio123@127.0.0.1:9004/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket a -> b : ${remote_arn}"
mc replicate add sitea/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync"
sleep 1
echo "adding replication config for site b -> site a"
remote_arn=$(mc admin bucket remote add siteb/olockbucket/ \
   http://minio:minio123@127.0.0.1:9001/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket b -> a : ${remote_arn}"
mc replicate add siteb/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync"
sleep 1
echo "adding replication config for olockbucket site a -> site c"
remote_arn=$(mc admin bucket remote add sitea/olockbucket/ \
   http://minio:minio123@127.0.0.1:9006/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket a -> c : ${remote_arn}"
mc replicate add sitea/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 2
sleep 1
echo "adding replication config for site c -> site a"
remote_arn=$(mc admin bucket remote add sitec/olockbucket/ \
   http://minio:minio123@127.0.0.1:9001/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket c -> a : ${remote_arn}"
mc replicate add sitec/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 2
sleep 1
echo "adding replication config for site b -> site c"
remote_arn=$(mc admin bucket remote add siteb/olockbucket/ \
   http://minio:minio123@127.0.0.1:9006/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket b -> c : ${remote_arn}"
mc replicate add siteb/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 3
sleep 1
echo "adding replication config for site c -> site b"
remote_arn=$(mc admin bucket remote add sitec/olockbucket \
   http://minio:minio123@127.0.0.1:9004/olockbucket \
   --service "replication" --json | jq -r ".RemoteARN")
sleep 1
echo "adding replication rule for olockbucket c -> b : ${remote_arn}"
mc replicate add sitec/olockbucket/ \
   --remote-bucket "${remote_arn}" \
   --replicate "existing-objects,delete,delete-marker,replica-metadata-sync" --priority 3
sleep 1

echo "Set default governance retention 30d"
mc retention set --default governance 30d sitea/olockbucket

echo "Copying data to source sitea/bucket"
mc cp --quiet /etc/hosts sitea/bucket
sleep 1

echo "Copying data to source sitea/olockbucket"
mc cp --quiet /etc/hosts sitea/olockbucket
sleep 1

echo "Verifying the metadata difference between source and target"
if diff -pruN <(mc stat --json sitea/bucket/hosts | jq .) <(mc stat --json siteb/bucket/hosts | jq .) | grep -q 'COMPLETED\|REPLICA'; then
    echo "verified sitea-> COMPLETED, siteb-> REPLICA"
fi

if diff -pruN <(mc stat --json sitea/bucket/hosts | jq .) <(mc stat --json sitec/bucket/hosts | jq .) | grep -q 'COMPLETED\|REPLICA'; then
    echo "verified sitea-> COMPLETED, sitec-> REPLICA"
fi

echo "Verifying the metadata difference between source and target"
if diff -pruN <(mc stat --json sitea/olockbucket/hosts | jq .) <(mc stat --json siteb/olockbucket/hosts | jq .) | grep -q 'COMPLETED\|REPLICA'; then
    echo "verified sitea-> COMPLETED, siteb-> REPLICA"
fi

if diff -pruN <(mc stat --json sitea/olockbucket/hosts | jq .) <(mc stat --json sitec/olockbucket/hosts | jq .) | grep -q 'COMPLETED\|REPLICA'; then
    echo "verified sitea-> COMPLETED, sitec-> REPLICA"
fi

catch
