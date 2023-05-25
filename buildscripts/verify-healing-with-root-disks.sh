#!/bin/bash -e

set -E
set -o pipefail
set -x

if [ ! -x "$PWD/minio" ]; then
	echo "minio executable binary not found in current directory"
	exit 1
fi

WORK_DIR="$(mktemp -d)"
MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MINIO=("$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" server)

function start_minio() {
	start_port=$1

	export MINIO_ROOT_USER=minio
	export MINIO_ROOT_PASSWORD=minio123
	unset MINIO_KMS_AUTO_ENCRYPTION # do not auto-encrypt objects
	unset MINIO_CI_CD
	unset CI

	args=()
	for i in $(seq 1 4); do
		args+=("http://localhost:$((start_port + i))${WORK_DIR}/mnt/disk$i/ ")
	done

	for i in $(seq 1 4); do
		"${MINIO[@]}" --address ":$((start_port + i))" ${args[@]} 2>&1 >"${WORK_DIR}/server$i.log" &
	done

	# Wait until all nodes return 403
	for i in $(seq 1 4); do
		while [ "$(curl -m 1 -s -o /dev/null -w "%{http_code}" http://localhost:$((start_port + i)))" -ne "403" ]; do
			echo -n "."
			sleep 1
		done
	done

}

# Prepare fake disks with losetup
function prepare_block_devices() {
	set -e
	mkdir -p ${WORK_DIR}/disks/ ${WORK_DIR}/mnt/
	sudo modprobe loop
	for i in 1 2 3 4; do
		dd if=/dev/zero of=${WORK_DIR}/disks/img.${i} bs=1M count=2000
		device=$(sudo losetup --find --show ${WORK_DIR}/disks/img.${i})
		sudo mkfs.ext4 -F ${device}
		mkdir -p ${WORK_DIR}/mnt/disk${i}/
		sudo mount ${device} ${WORK_DIR}/mnt/disk${i}/
		sudo chown "$(id -u):$(id -g)" ${device} ${WORK_DIR}/mnt/disk${i}/
	done
	set +e
}

# Start a distributed MinIO setup, unmount one disk and check if it is formatted
function main() {
	start_port=$(shuf -i 10000-65000 -n 1)
	start_minio ${start_port}

	# Unmount the disk, after the unmount the device id
	# /tmp/xxx/mnt/disk4 will be the same as '/' and it
	# will be detected as root disk
	while [ "$u" != "0" ]; do
		sudo umount ${WORK_DIR}/mnt/disk4/
		u=$?
		sleep 1
	done

	# Wait until MinIO self heal kicks in
	sleep 60

	if [ -f ${WORK_DIR}/mnt/disk4/.minio.sys/format.json ]; then
		echo "A root disk is formatted unexpectedely"
		cat "${WORK_DIR}/server4.log"
		exit -1
	fi
}

function cleanup() {
	pkill minio
	sudo umount ${WORK_DIR}/mnt/disk{1..3}/
	sudo rm /dev/minio-loopdisk*
	rm -rf "$WORK_DIR"
}

(prepare_block_devices)
(main "$@")
rv=$?

cleanup
exit "$rv"
