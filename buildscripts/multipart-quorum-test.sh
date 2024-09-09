#!/bin/bash

if [ -n "$TEST_DEBUG" ]; then
	set -x
fi

WORK_DIR="$PWD/.verify-$RANDOM"
MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MINIO=("$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" server)

if [ ! -x "$PWD/minio" ]; then
	echo "minio executable binary not found in current directory"
	exit 1
fi

if [ ! -x "$PWD/minio" ]; then
	echo "minio executable binary not found in current directory"
	exit 1
fi

trap 'catch $LINENO' ERR

function purge() {
	rm -rf "$1"
}

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
	fi

	echo "Cleaning up instances of MinIO"
	pkill minio || true
	pkill -9 minio || true
	purge "$WORK_DIR"
	if [ $# -ne 0 ]; then
		exit $#
	fi
}

catch

function start_minio_10drive() {
	start_port=$1

	export MINIO_ROOT_USER=minio
	export MINIO_ROOT_PASSWORD=minio123
	export MC_HOST_minio="http://minio:minio123@127.0.0.1:${start_port}/"
	unset MINIO_KMS_AUTO_ENCRYPTION # do not auto-encrypt objects
	export MINIO_CI_CD=1

	mkdir ${WORK_DIR}
	C_PWD=${PWD}
	if [ ! -x "$PWD/mc" ]; then
		MC_BUILD_DIR="mc-$RANDOM"
		if ! git clone --quiet https://github.com/minio/mc "$MC_BUILD_DIR"; then
			echo "failed to download https://github.com/minio/mc"
			purge "${MC_BUILD_DIR}"
			exit 1
		fi

		(cd "${MC_BUILD_DIR}" && go build -o "$C_PWD/mc")

		# remove mc source.
		purge "${MC_BUILD_DIR}"
	fi

	"${MINIO[@]}" --address ":$start_port" "${WORK_DIR}/disk{1...10}" >"${WORK_DIR}/server1.log" 2>&1 &
	pid=$!
	disown $pid
	sleep 5

	if ! ps -p ${pid} 1>&2 >/dev/null; then
		echo "server1 log:"
		cat "${WORK_DIR}/server1.log"
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi

	"${PWD}/mc" mb --with-versioning minio/bucket

	export AWS_ACCESS_KEY_ID=minio
	export AWS_SECRET_ACCESS_KEY=minio123
	aws --endpoint-url http://localhost:"$start_port" s3api create-multipart-upload --bucket bucket --key obj-1 >upload-id.json
	uploadId=$(jq -r '.UploadId' upload-id.json)

	truncate -s 5MiB file-5mib
	for i in {1..2}; do
		aws --endpoint-url http://localhost:"$start_port" s3api upload-part \
			--upload-id "$uploadId" --bucket bucket --key obj-1 \
			--part-number "$i" --body ./file-5mib
	done
	for i in {1..6}; do
		find ${WORK_DIR}/disk${i}/.minio.sys/multipart/ -type f -name "part.1" -delete
	done
	cat <<EOF >parts.json
{
    "Parts": [
        {
            "PartNumber": 1,
            "ETag": "5f363e0e58a95f06cbe9bbc662c5dfb6"
        },
        {
            "PartNumber": 2,
            "ETag": "5f363e0e58a95f06cbe9bbc662c5dfb6"
        }
    ]
}
EOF
	err=$(aws --endpoint-url http://localhost:"$start_port" s3api complete-multipart-upload --upload-id "$uploadId" --bucket bucket --key obj-1 --multipart-upload file://./parts.json 2>&1)
	rv=$?
	if [ $rv -eq 0 ]; then
		echo "Failed to receive an error"
		exit 1
	fi
	echo "Received an error during complete-multipart as expected: $err"
}

function main() {
	start_port=$(shuf -i 10000-65000 -n 1)
	start_minio_10drive ${start_port}
}

main "$@"
