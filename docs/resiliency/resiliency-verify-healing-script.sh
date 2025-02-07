#!/usr/bin/env bash

echo "script failed" >resiliency-verify-healing.log # assume initial state

# Extract arguments from json object ...
FILE=$(echo $1 | jq -r '.args.file')
DIR=$(echo $1 | jq -r '.args.dir')
DEEP=$(echo $1 | jq -r '.args.deep')
WANT=$(echo $1 | jq 'del(.args)') # ... and remove args from wanted result

ALIAS_NAME=myminio
BUCKET="test-bucket"
JQUERY='select(.name=="'"${BUCKET}"'/'"${DIR}"'/'"${FILE}"'") | {"before":{"color": .before.color, "missing": .before.missing, "corrupted": .before.corrupted},"after":{"color": .after.color, "missing": .after.missing, "corrupted": .after.corrupted}}'
if [ "$DEEP" = "true" ]; then
	SCAN_DEEP="--scan=deep"
fi

GOT=$(./mc admin heal --json ${SCAN_DEEP} ${ALIAS_NAME}/${BUCKET}/${DIR}/${FILE})
GOT=$(echo $GOT | jq "${JQUERY}")

if [ "$(echo "$GOT" | jq -S .)" = "$(echo "$WANT" | jq -S .)" ]; then
	echo "script passed" >resiliency-verify-healing.log
else
	echo "Error during healing:"
	echo "----GOT: "$GOT
	echo "---WANT: "$WANT
fi
