#!/bin/bash -e

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

function start_minio_4drive() {
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

	"${MINIO[@]}" --address ":$start_port" "${WORK_DIR}/disk{1...4}" >"${WORK_DIR}/server1.log" 2>&1 &
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

	for i in $(seq 1 4); do
		"${PWD}/mc" cp /etc/hosts minio/bucket/testobj

		sudo chown -R root. "${WORK_DIR}/disk${i}"

		"${PWD}/mc" cp /etc/hosts minio/bucket/testobj

		sudo chown -R ${USER}. "${WORK_DIR}/disk${i}"
	done

	for vid in $("${PWD}/mc" ls --json --versions minio/bucket/testobj | jq -r .versionId); do
		"${PWD}/mc" cat --vid "${vid}" minio/bucket/testobj | md5sum
	done

	pkill minio
	sleep 3
}

function main() {
	start_port=$(shuf -i 10000-65000 -n 1)

	start_minio_4drive ${start_port}
}

function purge() {
	rm -rf "$1"
}

(main "$@")
rv=$?
purge "$WORK_DIR"
exit "$rv"
