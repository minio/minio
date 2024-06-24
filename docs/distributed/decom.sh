#!/bin/bash

if [ -n "$TEST_DEBUG" ]; then
	set -x
fi

pkill minio
rm -rf /tmp/xl
rm -rf /tmp/xltier

if [ ! -f ./mc ]; then
	wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

export CI=true
export MINIO_SCANNER_SPEED=fastest

(minio server http://localhost:9000/tmp/xl/{1...10}/disk{0...1} 2>&1 >/tmp/decom.log) &
pid=$!

export MC_HOST_myminio="http://minioadmin:minioadmin@localhost:9000/"

./mc ready myminio

./mc admin user add myminio/ minio123 minio123
./mc admin user add myminio/ minio12345 minio12345

./mc admin policy create myminio/ rw ./docs/distributed/rw.json
./mc admin policy create myminio/ lake ./docs/distributed/rw.json

./mc admin policy attach myminio/ rw --user=minio123
./mc admin policy attach myminio/ lake --user=minio12345

./mc mb -l myminio/versioned

./mc mirror internal myminio/versioned/ --quiet >/dev/null

## Soft delete (creates delete markers)
./mc rm -r --force myminio/versioned >/dev/null

## mirror again to create another set of version on top
./mc mirror internal myminio/versioned/ --quiet >/dev/null

expected_checksum=$(./mc cat internal/dsync/drwmutex.go | md5sum)

user_count=$(./mc admin user list myminio/ | wc -l)
policy_count=$(./mc admin policy list myminio/ | wc -l)

## create a warm tier instance
(minio server /tmp/xltier/{1...4}/disk{0...1} --address :9002 2>&1 >/dev/null) &

export MC_HOST_mytier="http://minioadmin:minioadmin@localhost:9002/"

./mc ready myminio

./mc mb -l myminio/bucket2
./mc mb -l mytier/tiered

## create a tier and set up ilm policy to tier immediately
./mc admin tier add minio myminio TIER1 --endpoint http://localhost:9002 --access-key minioadmin --secret-key minioadmin --bucket tiered --prefix prefix5/
./mc ilm add myminio/bucket2 --transition-days 0 --transition-tier TIER1 --transition-days 0

## mirror some content to bucket2 and capture versions tiered
./mc mirror internal myminio/bucket2/ --quiet >/dev/null
./mc ls -r myminio/bucket2/ >bucket2_ns.txt
./mc ls -r --versions myminio/bucket2/ >bucket2_ns_versions.txt

sleep 30

./mc ls -r --versions mytier/tiered/ >tiered_ns_versions.txt

kill $pid

(minio server http://localhost:9000/tmp/xl/{1...10}/disk{0...1} http://localhost:9001/tmp/xl/{11...30}/disk{0...3} 2>&1 >/tmp/expanded_1.log) &
pid_1=$!

(minio server --address ":9001" http://localhost:9000/tmp/xl/{1...10}/disk{0...1} http://localhost:9001/tmp/xl/{11...30}/disk{0...3} 2>&1 >/tmp/expanded_2.log) &
pid_2=$!

./mc ready myminio

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

./mc ls -r myminio/versioned/ >expanded_ns.txt
./mc ls -r --versions myminio/versioned/ >expanded_ns_versions.txt

./mc admin decom start myminio/ http://localhost:9000/tmp/xl/{1...10}/disk{0...1}

count=0
until $(./mc admin decom status myminio/ | grep -q Complete); do
	echo "waiting for decom to finish..."
	count=$((count + 1))
	if [ ${count} -eq 120 ]; then
		./mc cat /tmp/expanded_*.log
	fi
	sleep 1
done

kill $pid_1
kill $pid_2

sleep 5

(minio server --address ":9001" http://localhost:9001/tmp/xl/{11...30}/disk{0...3} 2>&1 >/tmp/removed.log) &
pid=$!

sleep 5
export MC_HOST_myminio="http://minioadmin:minioadmin@localhost:9001/"

./mc ready myminio

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

./mc ls -r myminio/versioned >decommissioned_ns.txt
./mc ls -r --versions myminio/versioned >decommissioned_ns_versions.txt

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

got_checksum=$(./mc cat myminio/versioned/dsync/drwmutex.go | md5sum)
if [ "${expected_checksum}" != "${got_checksum}" ]; then
	echo "BUG: decommission failed on encrypted objects: expected ${expected_checksum} got ${got_checksum}"
	exit 1
fi

# after decommissioning, compare listings in bucket2 and tiered
./mc version info myminio/bucket2 | grep -q "versioning is enabled"
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected versioning enabled after decommission on bucket2"
	exit 1
fi

./mc ls -r myminio/bucket2 >decommissioned_bucket2_ns.txt
./mc ls -r --versions myminio/bucket2 >decommissioned_bucket2_ns_versions.txt
./mc ls -r --versions mytier/tiered/ >tiered_ns_versions2.txt

out=$(diff -qpruN bucket2_ns.txt decommissioned_bucket2_ns.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after decommission in bucket2: $out"
	exit 1
fi

out=$(diff -qpruN bucket2_ns_versions.txt decommissioned_bucket2_ns_versions.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after decommission in bucket2x: $out"
	exit 1
fi

out=$(diff -qpruN tiered_ns_versions.txt tiered_ns_versions2.txt)
ret=$?
if [ $ret -ne 0 ]; then
	echo "BUG: expected no missing entries after decommission in warm tier: $out"
	exit 1
fi

got_checksum=$(./mc cat myminio/bucket2/dsync/drwmutex.go | md5sum)
if [ "${expected_checksum}" != "${got_checksum}" ]; then
	echo "BUG: decommission failed on encrypted objects with tiering: expected ${expected_checksum} got ${got_checksum}"
	exit 1
fi

s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://127.0.0.1:9001/ -bucket bucket2
s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://127.0.0.1:9001/ -bucket versioned

kill $pid
