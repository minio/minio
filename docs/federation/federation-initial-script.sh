#!/usr/bin/env bash
# This script will run inside ubuntu-pod that is located at default namespace in the cluster
# This script will not and should not be executed in the self hosted runner

set -e
echo "script failed" > federation-initial.log # assume initial state

echo "sleep to wait for MinIO Server to be ready prior mc commands"
# https://github.com/minio/mc/issues/3599

MINIO_SERVER_URL="http://127.0.0.1:9000"
ALIAS_NAME=fed-docker

TIMEOUT=10
while true; do
	if [[ ${TIMEOUT} -le 0 ]]; then
		echo retry: timeout while running: mc alias set
		exit 1
	fi
	eval ./mc alias set "${ALIAS_NAME}" "${MINIO_SERVER_URL}" minioadmin minioadmin && break
	TIMEOUT=$((TIMEOUT - 1))
	sleep 1
done

./mc ready "${ALIAS_NAME}"

for i in {1..4}; do
	# Admin setup for each MinIO instance
	./mc admin user add fed-docker testuser${i} passuser${i}
	./mc admin policy attach fed-docker readwrite --user testuser${i}

	# User setup for each MinIO instance
	./mc alias set minio${i} http://0.0.0.0:901$i testuser${i} passuser${i}
	./mc ready minio${i}
done

echo "script passed" > federation-initial.log