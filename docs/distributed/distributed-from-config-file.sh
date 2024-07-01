#!/usr/bin/env bash

set -e

cleanup() {
	echo "Cleaning up instances of MinIO"
	pkill minio || true
	pkill -9 minio || true
	rm -rf /tmp/xl/ || true
	rm -rf /tmp/minio.configfile.{1,2,3,4} || true
}

cleanup

unset MINIO_KMS_KES_CERT_FILE
unset MINIO_KMS_KES_KEY_FILE
unset MINIO_KMS_KES_ENDPOINT
unset MINIO_KMS_KES_KEY_NAME

export MINIO_CI_CD=1

if [ ! -f ./mc ]; then
	os="$(uname -s)"
	arch="$(uname -m)"
	case "${arch}" in
	"x86_64")
		arch="amd64"
		;;
	esac

	wget -O mc https://dl.minio.io/client/mc/release/${os,,}-${arch,,}/mc &&
		chmod +x mc
fi

for i in $(seq 1 4); do
	s3Port="$((9000 + i))"
	consolePort="$((s3Port + 1000))"

	cat <<EOF >/tmp/minio.configfile.$i
version: v1
address: ':${s3Port}'
console-address: ':${consolePort}'
rootUser: 'minr0otUS2r'
rootPassword: 'pBU94AGAY85e'
pools: # Specify the nodes and drives with pools
  -
     - 'http://localhost:9001/tmp/xl/node9001/mnt/disk{1...4}/'
     - 'http://localhost:9002/tmp/xl/node9002/mnt/disk{1,2,3,4}/'
  -
     - 'http://localhost:9003/tmp/xl/node9003/mnt/disk{1...4}/'
     - 'http://localhost:9004/tmp/xl/node9004/mnt/disk1/'
     - 'http://localhost:9004/tmp/xl/node9004/mnt/disk2/'
     - 'http://localhost:9004/tmp/xl/node9004/mnt/disk3/'
     - 'http://localhost:9004/tmp/xl/node9004/mnt/disk4/'
EOF
done

minio server --config /tmp/minio.configfile.1 >/tmp/minio1_1.log 2>&1 &
site1_pid=$!
minio server --config /tmp/minio.configfile.2 >/tmp/minio2_1.log 2>&1 &
site2_pid=$!
minio server --config /tmp/minio.configfile.3 >/tmp/minio3_1.log 2>&1 &
site3_pid=$!
minio server --config /tmp/minio.configfile.4 >/tmp/minio4_1.log 2>&1 &
site4_pid=$!

export MC_HOST_minio1=http://minr0otUS2r:pBU94AGAY85e@localhost:9001
export MC_HOST_minio3=http://minr0otUS2r:pBU94AGAY85e@localhost:9003

./mc ready minio1
./mc ready minio3

./mc mb minio1/testbucket
# copy large upload to newbucket on minio1
truncate -s 17M lrgfile
expected_checksum=$(cat ./lrgfile | md5sum)

./mc cp ./lrgfile minio1/testbucket

actual_checksum=$(./mc cat minio3/testbucket/lrgfile | md5sum)

if [ "${expected_checksum}" != "${actual_checksum}" ]; then
	echo "unexpected object checksum, expected: ${expected_checksum} got: ${actual_checksum}"
	exit
fi

# Compare the difference of the list of disks and their location, with the below expected output
diff <(./mc admin info minio1 --json | jq -r '.info.servers[].drives[] | "\(.pool_index),\(.set_index),\(.disk_index) \(.endpoint)"' | sort) <(
	cat <<EOF
0,0,0 http://localhost:9001/tmp/xl/node9001/mnt/disk1
0,0,1 http://localhost:9002/tmp/xl/node9002/mnt/disk1
0,0,2 http://localhost:9001/tmp/xl/node9001/mnt/disk2
0,0,3 http://localhost:9002/tmp/xl/node9002/mnt/disk2
0,0,4 http://localhost:9001/tmp/xl/node9001/mnt/disk3
0,0,5 http://localhost:9002/tmp/xl/node9002/mnt/disk3
0,0,6 http://localhost:9001/tmp/xl/node9001/mnt/disk4
0,0,7 http://localhost:9002/tmp/xl/node9002/mnt/disk4
1,0,0 http://localhost:9003/tmp/xl/node9003/mnt/disk1
1,0,1 http://localhost:9004/tmp/xl/node9004/mnt/disk1
1,0,2 http://localhost:9003/tmp/xl/node9003/mnt/disk2
1,0,3 http://localhost:9004/tmp/xl/node9004/mnt/disk2
1,0,4 http://localhost:9003/tmp/xl/node9003/mnt/disk3
1,0,5 http://localhost:9004/tmp/xl/node9004/mnt/disk3
1,0,6 http://localhost:9003/tmp/xl/node9003/mnt/disk4
1,0,7 http://localhost:9004/tmp/xl/node9004/mnt/disk4
EOF
)

cleanup
