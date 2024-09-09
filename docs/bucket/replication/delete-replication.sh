#!/usr/bin/env bash

echo "Running $0"

if [ -n "$TEST_DEBUG" ]; then
	set -x
fi

trap 'catch $LINENO' ERR

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
		echo "dc1 server logs ========="
		cat /tmp/dc1.log
		echo "dc2 server logs ========="
		cat /tmp/dc2.log
	fi

	echo "Cleaning up instances of MinIO"
	set +e
	pkill minio
	pkill mc
	rm -rf /tmp/xl/
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

mkdir -p /tmp/xl/1/ /tmp/xl/2/

export MINIO_KMS_SECRET_KEY="my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw="
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"

./minio server --address ":9001" /tmp/xl/1/{1...4}/ 2>&1 >/tmp/dc1.log &
pid1=$!
./minio server --address ":9002" /tmp/xl/2/{1...4}/ 2>&1 >/tmp/dc2.log &
pid2=$!

sleep 3

export MC_HOST_myminio1=http://minioadmin:minioadmin@localhost:9001
export MC_HOST_myminio2=http://minioadmin:minioadmin@localhost:9002

./mc ready myminio1
./mc ready myminio2

./mc mb myminio1/testbucket/
./mc version enable myminio1/testbucket/
./mc mb myminio2/testbucket/
./mc version enable myminio2/testbucket/

./mc replicate add myminio1/testbucket --remote-bucket http://minioadmin:minioadmin@localhost:9002/testbucket/ --priority 1

# Test replication of delete markers and permanent deletes

./mc cp README.md myminio1/testbucket/dir/file
./mc cp README.md myminio1/testbucket/dir/file

sleep 1s

echo "=== myminio1"
./mc ls --versions myminio1/testbucket/dir/file

echo "=== myminio2"
./mc ls --versions myminio2/testbucket/dir/file

versionId="$(./mc ls --json --versions myminio1/testbucket/dir/ | tail -n1 | jq -r .versionId)"

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

aws s3api --endpoint-url http://localhost:9001 delete-object --bucket testbucket --key dir/file --version-id "$versionId"

./mc ls -r --versions myminio1/testbucket >/tmp/myminio1.txt
./mc ls -r --versions myminio2/testbucket >/tmp/myminio2.txt

out=$(diff -qpruN /tmp/myminio1.txt /tmp/myminio2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after replication: $out"
	exit 1
fi

./mc rm myminio1/testbucket/dir/file
sleep 1s

./mc ls -r --versions myminio1/testbucket >/tmp/myminio1.txt
./mc ls -r --versions myminio2/testbucket >/tmp/myminio2.txt

out=$(diff -qpruN /tmp/myminio1.txt /tmp/myminio2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after replication: $out"
	exit 1
fi

# Test listing of non replicated permanent deletes

set -x

./mc mb myminio1/foobucket/ myminio2/foobucket/ --with-versioning
./mc replicate add myminio1/foobucket --remote-bucket http://minioadmin:minioadmin@localhost:9002/foobucket/ --priority 1
./mc cp README.md myminio1/foobucket/dir/file

versionId="$(./mc ls --json --versions myminio1/foobucket/dir/ | jq -r .versionId)"

kill ${pid2} && wait ${pid2} || true

aws s3api --endpoint-url http://localhost:9001 delete-object --bucket foobucket --key dir/file --version-id "$versionId"

out="$(./mc ls myminio1/foobucket/dir/)"
if [ "$out" != "" ]; then
	echo "BUG: non versioned listing should not show pending/failed replicated delete:"
	echo "$out"
	exit 1
fi

out="$(./mc ls --versions myminio1/foobucket/dir/)"
if [ "$out" != "" ]; then
	echo "BUG: versioned listing should not show pending/failed replicated deletes:"
	echo "$out"
	exit 1
fi

echo "Success"
catch
