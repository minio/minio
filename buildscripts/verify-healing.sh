#!/bin/bash -e
#

set -E
set -o pipefail

if [ ! -x "$PWD/minio" ]; then
	echo "minio executable binary not found in current directory"
	exit 1
fi

WORK_DIR="$PWD/.verify-$RANDOM"
MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MINIO=("$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" server)
GOPATH=/tmp/gopath

function start_minio_3_node() {
	export MINIO_ROOT_USER=minio
	export MINIO_ROOT_PASSWORD=minio123
	export MINIO_ERASURE_SET_DRIVE_COUNT=6
	export MINIO_CI_CD=1

	first_time=$(find ${WORK_DIR}/ | grep format.json | wc -l)

	start_port=$2
	args=""
	for d in $(seq 1 3 5); do
		args="$args http://127.0.0.1:$((start_port + 1))${WORK_DIR}/1/${d}/ http://127.0.0.1:$((start_port + 2))${WORK_DIR}/2/${d}/ http://127.0.0.1:$((start_port + 3))${WORK_DIR}/3/${d}/ "
		d=$((d + 1))
		args="$args http://127.0.0.1:$((start_port + 1))${WORK_DIR}/1/${d}/ http://127.0.0.1:$((start_port + 2))${WORK_DIR}/2/${d}/ http://127.0.0.1:$((start_port + 3))${WORK_DIR}/3/${d}/ "
	done

	"${MINIO[@]}" --address ":$((start_port + 1))" $args >"${WORK_DIR}/dist-minio-server1.log" 2>&1 &
	pid1=$!
	disown ${pid1}

	"${MINIO[@]}" --address ":$((start_port + 2))" $args >"${WORK_DIR}/dist-minio-server2.log" 2>&1 &
	pid2=$!
	disown $pid2

	"${MINIO[@]}" --address ":$((start_port + 3))" $args >"${WORK_DIR}/dist-minio-server3.log" 2>&1 &
	pid3=$!
	disown $pid3

	sleep "$1"

	[ ${first_time} -eq 0 ] && upload_objects $start_port

	if ! ps -p $pid1 1>&2 >/dev/null; then
		echo "server1 log:"
		cat "${WORK_DIR}/dist-minio-server1.log"
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi

	if ! ps -p $pid2 1>&2 >/dev/null; then
		echo "server2 log:"
		cat "${WORK_DIR}/dist-minio-server2.log"
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi

	if ! ps -p $pid3 1>&2 >/dev/null; then
		echo "server3 log:"
		cat "${WORK_DIR}/dist-minio-server3.log"
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi

	if ! pkill minio; then
		for i in $(seq 1 3); do
			echo "server$i log:"
			cat "${WORK_DIR}/dist-minio-server$i.log"
		done
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi

	sleep 1
	if pgrep minio; then
		# forcibly killing, to proceed further properly.
		if ! pkill -9 minio; then
			echo "no minio process running anymore, proceed."
		fi
	fi
}

function check_heal() {
	if ! grep -q 'Status:' ${WORK_DIR}/dist-minio-*.log; then
		return 1
	fi

	for ((i = 0; i < 20; i++)); do
		test -f ${WORK_DIR}/$1/1/.minio.sys/format.json
		v1=$?
		nextInES=$(($1 + 1)) && [ $nextInES -gt 3 ] && nextInES=1
		foundFiles1=$(find ${WORK_DIR}/$1/1/ | grep -v .minio.sys | grep xl.meta | wc -l)
		foundFiles2=$(find ${WORK_DIR}/$nextInES/1/ | grep -v .minio.sys | grep xl.meta | wc -l)
		test $foundFiles1 -eq $foundFiles2
		v2=$?
		[ $v1 == 0 -a $v2 == 0 ] && return 0
		sleep 10
	done
	return 1
}

function purge() {
	rm -rf "$1"
}

function __init__() {
	echo "Initializing environment"
	mkdir -p "$WORK_DIR"
	mkdir -p "$MINIO_CONFIG_DIR"

	## version is purposefully set to '3' for minio to migrate configuration file
	echo '{"version": "3", "credential": {"accessKey": "minio", "secretKey": "minio123"}, "region": "us-east-1"}' >"$MINIO_CONFIG_DIR/config.json"

	GOPATH=/tmp/gopath go install github.com/minio/mc@latest
}

function upload_objects() {
	start_port=$1

	$GOPATH/bin/mc alias set myminio http://127.0.0.1:$((start_port + 1)) minio minio123 --api=s3v4
	$GOPATH/bin/mc ready myminio
	$GOPATH/bin/mc mb myminio/testbucket/
	for ((i = 0; i < 20; i++)); do
		echo "my content" | $GOPATH/bin/mc pipe myminio/testbucket/file-$i
	done
}

function perform_test() {
	start_port=$2

	start_minio_3_node 120 $start_port

	echo "Testing Distributed Erasure setup healing of drives"
	echo "Remove the contents of the disks belonging to '${1}' node"

	rm -rf ${WORK_DIR}/${1}/*/

	set -x
	start_minio_3_node 120 $start_port

	check_heal ${1}
	rv=$?
	if [ "$rv" == "1" ]; then
		for i in $(seq 1 3); do
			echo "server$i log:"
			cat "${WORK_DIR}/dist-minio-server$i.log"
		done
		pkill -9 minio
		echo "FAILED"
		purge "$WORK_DIR"
		exit 1
	fi
}

function main() {
	# use same ports for all tests
	start_port=$(shuf -i 10000-65000 -n 1)

	perform_test "2" ${start_port}
	perform_test "1" ${start_port}
	perform_test "3" ${start_port}
}

(__init__ "$@" && main "$@")
rv=$?
purge "$WORK_DIR"
exit "$rv"
