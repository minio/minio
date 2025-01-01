#!/usr/bin/env bash

echo "script failed" >resiliency-verify-failure.log # assume initial state

ALIAS_NAME=myminio
BUCKET="test-bucket"
DEST_DIR="/tmp/dest"

OUT=$(./mc cp --quiet --recursive "${ALIAS_NAME}"/"${BUCKET}"/initial-data/ "${DEST_DIR}"/)
RET=${?}
if [ ${RET} -ne 0 ]; then
	# It is a success scenario as get objects should fail
	echo "GET objects failed as expected"
	echo "script passed" >resiliency-verify-failure.log
	exit 0
else
	echo "GET objects expected to fail, but succeeded: ${OUT}"
fi
