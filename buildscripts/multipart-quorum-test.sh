#!/bin/bash

set -E
set -o pipefail
set -x

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

# shellcheck disable=SC2120
catch() {
	if [ $# -ne 0 ]; then
		echo "error on line $1"
	fi

	echo "Cleaning up instances of MinIO"
	pkill minio
	pkill -9 minio
	purge "$WORK_DIR"
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

	# install aws cli
	curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
	unzip awscliv2.zip
	sudo ./aws/install

	export AWS_ACCESS_KEY_ID=minio
	export AWS_SECRET_ACCESS_KEY=minio123
	aws --endpoint-url http://localhost:"$start_port" s3api create-multipart-upload --bucket bucket --key obj-1 >upload-id.json
	uploadId=$(jq -r '.UploadId' upload-id.json)

	truncate -S 5MiB file-5mib
	for i in {1..2}; do
		aws --endpoint-url http://localhost:"$start_port" s3api upload-part \
			--upload-id "$uploadId" --bucket bucket --key obj-1 \
			--part-number "$i" --body file-5mib
	done
	for i in {1..6}; do
		find ${WORK_DIR}/disk${i}/.minio.sys/multipart/ -type f -name "part.1" -delete
	done
	cat <<EOF >parts.json
{
    "Parts": [
        {
            "PartNumber": 1,
            "ETag": "\"5f363e0e58a95f06cbe9bbc662c5dfb6\""
        },
        {
            "PartNumber": 2,
            "ETag": "\"5f363e0e58a95f06cbe9bbc662c5dfb6\""
        }
    ]
}
EOF

	aws --endpoint-url http://localhost:"$start_port" s3api complete-multipart-upload --upload-id "$uploadId" --bucket bucket --key obj-1 --multipart-upload file://./parts.json 2>error.out
	if ! grep "An error occurred (InvalidPart) when calling the CompleteMultipartUpload operation" error.out; then
		echo "Failed to receive InvalidPart error"
		exit 1
	fi
}

function purge() {
	rm -rf "$1"
}

function main() {
	start_port=$(shuf -i 10000-65000 -n 1)
	start_minio_10drive ${start_port}
}

(main "$@")
rv=$?
exit "$rv"
