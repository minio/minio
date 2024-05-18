#!/bin/bash

echo "Running $0"

set -e
set -x

export CI=1

make || exit 255

killall -9 minio || true

rm -rf /tmp/xl/
mkdir -p /tmp/xl/1/ /tmp/xl/2/

export MINIO_KMS_SECRET_KEY="my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw="

NODES=4

args1=()
args2=()
for i in $(seq 1 $NODES); do
	args1+=("http://localhost:$((9000 + i))/tmp/xl/1/$i ")
	args2+=("http://localhost:$((9100 + i))/tmp/xl/2/$i ")
done

for i in $(seq 1 $NODES); do
	./minio server --address "127.0.0.1:$((9000 + i))" ${args1[@]} & # | tee /tmp/minio/node.$i &
	./minio server --address "127.0.0.1:$((9100 + i))" ${args2[@]} & # | tee /tmp/minio/node.$i &
done

sleep 10

./mc alias set myminio1 http://localhost:9001 minioadmin minioadmin
./mc alias set myminio2 http://localhost:9101 minioadmin minioadmin

./mc ready myminio1
./mc ready myminio2
sleep 1

./mc mb myminio1/testbucket/ --with-lock
./mc mb myminio2/testbucket/ --with-lock

./mc encrypt set sse-s3 my-minio-key myminio1/testbucket/
./mc encrypt set sse-s3 my-minio-key myminio2/testbucket/

./mc replicate add myminio1/testbucket --remote-bucket http://minioadmin:minioadmin@localhost:9101/testbucket --priority 1
./mc replicate add myminio2/testbucket --remote-bucket http://minioadmin:minioadmin@localhost:9001/testbucket --priority 1

sleep 1

cp README.md internal.tar

./mc cp internal.tar myminio1/testbucket/dir/1.tar
./mc cp internal.tar myminio2/testbucket/dir/2.tar

sleep 1

./mc ls -r --versions myminio1/testbucket/dir/ >/tmp/dir_1.txt
./mc ls -r --versions myminio2/testbucket/dir/ >/tmp/dir_2.txt

out=$(diff -qpruN /tmp/dir_1.txt /tmp/dir_2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no 'diff' after replication: $out"
	exit 1
fi
