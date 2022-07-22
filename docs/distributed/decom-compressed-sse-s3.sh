#!/bin/bash

if [ -n "$TEST_DEBUG" ]; then
    set -x
fi

pkill minio
rm -rf /tmp/xl

if [ ! -f ./mc ]; then
    wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc && \
	chmod +x mc
fi

export CI=true
export MINIO_COMPRESSION_ENABLE="on"
export MINIO_COMPRESSION_EXTENSIONS=".go"
export MINIO_COMPRESSION_MIME_TYPES="application/*"
export MINIO_COMPRESSION_ALLOW_ENCRYPTION="on"
export MINIO_KMS_AUTO_ENCRYPTION=on
export MINIO_KMS_SECRET_KEY=my-minio-key:OSMM+vkKUTCvQs9YL/CVMIMt43HFhkUpqJxTmGl6rYw=
export MC_HOST_myminio="http://minioadmin:minioadmin@localhost:9000/"

(minio server /tmp/xl/{1...10}/disk{0...1} 2>&1 >/dev/null)&
pid=$!

sleep 2

./mc admin user add myminio/ minio123 minio123
./mc admin user add myminio/ minio12345 minio12345

./mc admin policy add myminio/ rw ./docs/distributed/rw.json
./mc admin policy add myminio/ lake ./docs/distributed/rw.json

./mc admin policy set myminio/ rw user=minio123
./mc admin policy set myminio/ lake,rw user=minio12345

./mc mb -l myminio/versioned

./mc mirror internal myminio/versioned/ --quiet >/dev/null

## Soft delete (creates delete markers)
./mc rm -r --force myminio/versioned >/dev/null

## mirror again to create another set of version on top
./mc mirror internal myminio/versioned/ --quiet >/dev/null

expected_checksum=$(./mc cat internal/dsync/drwmutex.go | md5sum)

user_count=$(./mc admin user list myminio/ | wc -l)
policy_count=$(./mc admin policy list myminio/ | wc -l)

kill $pid

(minio server /tmp/xl/{1...10}/disk{0...1} /tmp/xl/{11...30}/disk{0...3} 2>&1 >/tmp/expanded.log) &
pid=$!

sleep 2

expanded_user_count=$(./mc admin user list myminio/ | wc -l)
expanded_policy_count=$(./mc admin policy list myminio/ | wc -l)

if [ $user_count -ne $expanded_user_count ]; then
    echo "BUG: original user count differs from expanded setup"
    exit 1
fi

if [ $policy_count -ne $expanded_policy_count ]; then
    echo "BUG: original policy count  differs from expanded setup"
    exit 1
fi

./mc version info myminio/versioned | grep -q "versioning is enabled"
ret=$?
if [ $ret -ne 0 ]; then
    echo "expected versioning enabled after expansion"
    exit 1
fi

./mc mirror cmd myminio/versioned/ --quiet >/dev/null

./mc ls -r myminio/versioned/ > expanded_ns.txt
./mc ls -r --versions myminio/versioned/ > expanded_ns_versions.txt

./mc admin decom start myminio/ /tmp/xl/{1...10}/disk{0...1}

until $(./mc admin decom status myminio/ | grep -q Complete)
do
    echo "waiting for decom to finish..."
    sleep 1
done

kill $pid

(minio server /tmp/xl/{11...30}/disk{0...3} 2>&1 >/tmp/removed.log)&
pid=$!

sleep 2

decom_user_count=$(./mc admin user list myminio/ | wc -l)
decom_policy_count=$(./mc admin policy list myminio/ | wc -l)

if [ $user_count -ne $decom_user_count ]; then
    echo "BUG: original user count differs after decommission"
    exit 1
fi

if [ $policy_count -ne $decom_policy_count ]; then
    echo "BUG: original policy count differs after decommission"
    exit 1
fi

./mc version info myminio/versioned | grep -q "versioning is enabled"
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected versioning enabled after decommission"
    exit 1
fi

got_checksum=$(./mc cat myminio/versioned/dsync/drwmutex.go | md5sum)
if [ "${expected_checksum}" != "${got_checksum}" ]; then
    echo "BUG: decommission failed on encrypted objects: expected ${expected_checksum} got ${got_checksum}"
    exit 1
fi

./mc ls -r myminio/versioned > decommissioned_ns.txt
./mc ls -r --versions myminio/versioned > decommissioned_ns_versions.txt

out=$(diff -qpruN expanded_ns.txt decommissioned_ns.txt)
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected no missing entries after decommission: $out"
    exit 1
fi

out=$(diff -qpruN expanded_ns_versions.txt decommissioned_ns_versions.txt)
ret=$?
if [ $ret -ne 0 ]; then
    echo "BUG: expected no missing entries after decommission: $out"
    exit 1
fi

# kill $pid
