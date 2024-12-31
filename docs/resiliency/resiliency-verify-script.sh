#!/usr/bin/env bash

echo "script failed" >resiliency-verify.log # assume initial state

ALIAS_NAME=myminio
BUCKET="test-bucket"
SRC_DIR="/tmp/data"
DEST_DIR="/tmp/dest"

./mc admin config set "$ALIAS_NAME" api requests_max=400

OBJ_COUNT_AFTER_STOP=$(./mc ls "${ALIAS_NAME}"/"${BUCKET}"/initial-data/ | wc -l)
# Count should match the initial count of 10
if [ "${OBJ_COUNT_AFTER_STOP}" -ne 10 ]; then
	echo "Expected 10 objects; received ${OBJ_COUNT_AFTER_STOP}"
	exit 1
fi

./mc ready "${ALIAS_NAME}" --json

OUT=$(./mc cp --quiet "${SRC_DIR}"/* "${ALIAS_NAME}"/"${BUCKET}"/new-data/)
RET=${?}
if [ ${RET} -ne 0 ]; then
	echo "Error copying objects to new prefix: ${OUT}"
	exit 1
fi

OBJ_COUNT_AFTER_COPY=$(./mc ls "${ALIAS_NAME}"/"${BUCKET}"/new-data/ | wc -l)
if [ "${OBJ_COUNT_AFTER_COPY}" -ne "${OBJ_COUNT_AFTER_STOP}" ]; then
	echo "Expected ${OBJ_COUNT_AFTER_STOP} objects; received ${OBJ_COUNT_AFTER_COPY}"
	exit 1
fi

OUT=$(./mc cp --quiet --recursive "${ALIAS_NAME}"/"${BUCKET}"/new-data/ "${DEST_DIR}"/)
RET=${?}
if [ ${RET} -ne 0 ]; then
	echo "Get objects failed: ${OUT}"
	exit 1
fi

# Check if check sums match for source and destination directories
CHECK_SUM_SRC=$(sha384sum <(sha384sum "${SRC_DIR}"/* | cut -d " " -f 1 | sort) | cut -d " " -f 1)
CHECK_SUM_DEST=$(sha384sum <(sha384sum "${DEST_DIR}"/* | cut -d " " -f 1 | sort) | cut -d " " -f 1)
if [ "${CHECK_SUM_SRC}" != "${CHECK_SUM_DEST}" ]; then
	echo "Checksum verification of source files and destination files failed"
	exit 1
fi

echo "script passed" >resiliency-verify.log
