#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	for site in sitea siteb; do
		echo "$site server logs ========="
		cat "/tmp/${site}_1.log"
		echo "==========================="
		cat "/tmp/${site}_2.log"
	done

	exit 1
}

cleanup() {
	echo -n "Cleaning up instances of MinIO ..."
	pkill -9 minio || sudo pkill -9 minio
	rm -rf /tmp/sitea
	rm -rf /tmp/siteb
	echo "done"
}

cleanup

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"

# Start MinIO instances
echo -n "Starting MinIO instances ..."
minio server --address ":9001" --console-address ":10000" /tmp/sitea/{1...4}/disk{1...4} /tmp/sitea/{5...8}/disk{1...4} >/tmp/sitea_1.log 2>&1 &
minio server --address ":9002" --console-address ":11000" /tmp/siteb/{1...4}/disk{1...4} /tmp/siteb/{5...8}/disk{1...4} >/tmp/siteb_1.log 2>&1 &
echo "done"

if [ ! -f ./mc ]; then
	wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

export MC_HOST_sitea=http://minio:minio123@127.0.0.1:9001
export MC_HOST_siteb=http://minio:minio123@127.0.0.1:9002

./mc ready sitea
./mc ready siteb

./mc mb sitea/bucket
./mc version enable sitea/bucket
./mc mb siteb/bucket
./mc version enable siteb/bucket

# Set bucket replication
./mc replicate add sitea/bucket --remote-bucket siteb/bucket

# Run the test to make sure proxying of DEL marker doesn't happen
loop_count=0
while true; do
	if [ $loop_count -eq 100 ]; then
		break
	fi
	echo "Hello World" | ./mc pipe sitea/bucket/obj$loop_count
	./mc rm sitea/bucket/obj$loop_count
	RESULT=$({ ./mc stat sitea/bucket/obj$loop_count; } 2>&1)
	if [[ ${RESULT} != *"Object does not exist"* ]]; then
		echo "BUG: stat should fail. succeeded."
		exit_1
	fi
	loop_count=$((loop_count + 1))
done

cleanup
