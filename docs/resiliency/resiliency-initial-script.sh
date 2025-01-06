#!/usr/bin/env bash
# This script will run inside ubuntu-pod that is located at default namespace in the cluster
# This script will not and should not be executed in the self hosted runner

echo "script failed" >resiliency-initial.log # assume initial state

echo "sleep to wait for MinIO Server to be ready prior mc commands"
# https://github.com/minio/mc/issues/3599

MINIO_SERVER_URL="http://127.0.0.1:9000"
ALIAS_NAME=myminio
BUCKET="test-bucket"
SRC_DIR="/tmp/data"
INLINED_DIR="/tmp/inlined"
DEST_DIR="/tmp/dest"

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

./mc mb "${ALIAS_NAME}"/"${BUCKET}"
rm -rf "${SRC_DIR}" "${INLINED_DIR}" "${DEST_DIR}" && mkdir -p "${SRC_DIR}" "${INLINED_DIR}" "${DEST_DIR}"
for idx in {1..10}; do
	# generate random nr of blocks
	COUNT=$((RANDOM % 100 + 100))
	# generate random content
	dd if=/dev/urandom bs=50K count="${COUNT}" of="${SRC_DIR}"/file"$idx"
done

# create small object that will be inlined into xl.meta
dd if=/dev/urandom bs=50K count=1 of="${INLINED_DIR}"/inlined

if ./mc cp --quiet --recursive "${SRC_DIR}/" "${ALIAS_NAME}"/"${BUCKET}"/initial-data/; then
	if ./mc cp --quiet --recursive "${INLINED_DIR}/" "${ALIAS_NAME}"/"${BUCKET}"/inlined-data/; then
		echo "script passed" >resiliency-initial.log
	fi
fi
