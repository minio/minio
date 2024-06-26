#!/bin/bash

if [ -n "$TEST_DEBUG" ]; then
	set -x
fi

pkill minio
pkill kes
rm -rf /tmp/xl

go install -v github.com/minio/mc@master
cp -a $(go env GOPATH)/bin/mc ./mc

if [ ! -f ./kes ]; then
	wget --quiet -O kes https://github.com/minio/kes/releases/latest/download/kes-linux-amd64 &&
		chmod +x kes
fi

if ! openssl version &>/dev/null; then
	apt install openssl || sudo apt install opensssl
fi

# Start KES Server
(./kes server --dev 2>&1 >kes-server.log) &
kes_pid=$!
sleep 5s
API_KEY=$(grep "API Key" <kes-server.log | awk -F" " '{print $3}')
(openssl s_client -connect 127.0.0.1:7373 2>/dev/null 1>public.crt)

export CI=true
export MINIO_KMS_KES_ENDPOINT=https://127.0.0.1:7373
export MINIO_KMS_KES_API_KEY="${API_KEY}"
export MINIO_KMS_KES_KEY_NAME=minio-default-key
export MINIO_KMS_KES_CAPATH=public.crt
export MC_HOST_myminio="http://minioadmin:minioadmin@localhost:9000/"

(minio server http://localhost:9000/tmp/xl/{1...10}/disk{0...1} 2>&1 >/dev/null) &
pid=$!

mc ready myminio

mc admin user add myminio/ minio123 minio123

mc admin policy create myminio/ deny-non-sse-kms-pol ./docs/iam/policies/deny-non-sse-kms-objects.json
mc admin policy create myminio/ deny-invalid-sse-kms-pol ./docs/iam/policies/deny-objects-with-invalid-sse-kms-key-id.json

mc admin policy attach myminio deny-non-sse-kms-pol --user minio123
mc admin policy attach myminio deny-invalid-sse-kms-pol --user minio123
mc admin policy attach myminio consoleAdmin --user minio123

mc mb -l myminio/test-bucket
mc mb -l myminio/multi-key-poc

export MC_HOST_myminio1="http://minio123:minio123@localhost:9000/"

mc cp /etc/issue myminio1/test-bucket
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: PutObject to bucket: test-bucket should succeed. Failed"
	exit 1
fi

mc cp /etc/issue myminio1/multi-key-poc | grep -q "Insufficient permissions to access this path"
ret=$?
if [ $ret -eq 0 ]; then
	echo "BUG: PutObject to bucket: multi-key-poc without sse-kms should fail. Succedded"
	exit 1
fi

mc cp /etc/hosts myminio1/multi-key-poc/hosts --enc-kms "myminio1/multi-key-poc/hosts=minio-default-key"
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: PutObject to bucket: multi-key-poc with valid sse-kms should succeed. Failed"
	exit 1
fi

mc cp /etc/issue myminio1/multi-key-poc/issue --enc-kms "myminio1/multi-key-poc/issue=minio-default-key-xxx" | grep "Insufficient permissions to access this path"
ret=$?
if [ $ret -eq 0 ]; then
	echo "BUG: PutObject to bucket: multi-key-poc with invalid sse-kms should fail. Succeeded"
	exit 1
fi

kill $pid
kill $kes_pid
