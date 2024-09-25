#!/usr/bin/env bash

# shellcheck disable=SC2120
exit_1() {
	cleanup

	echo "minio1 ============"
	cat /tmp/minio1_1.log
	echo "minio2 ============"
	cat /tmp/minio2_1.log

	exit 1
}

cleanup() {
	echo -n "Cleaning up instances of MinIO ..."
	pkill minio || sudo pkill minio
	pkill -9 minio || sudo pkill -9 minio
	rm -rf /tmp/minio{1,2}
	echo "done"
}

cleanup

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"
TEST_MINIO_ENC_KEY="MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDA"

# Create certificates for TLS enabled MinIO
echo -n "Setup certs for MinIO instances ..."
wget -O certgen https://github.com/minio/certgen/releases/latest/download/certgen-linux-amd64 && chmod +x certgen
./certgen --host localhost
mkdir -p /tmp/certs
mv public.crt /tmp/certs || sudo mv public.crt /tmp/certs
mv private.key /tmp/certs || sudo mv private.key /tmp/certs
echo "done"

# Start MinIO instances
echo -n "Starting MinIO instances ..."
minio server --certs-dir /tmp/certs --address ":9001" --console-address ":10000" /tmp/minio1/{1...4}/disk{1...4} /tmp/minio1/{5...8}/disk{1...4} >/tmp/minio1_1.log 2>&1 &
minio server --certs-dir /tmp/certs --address ":9002" --console-address ":11000" /tmp/minio2/{1...4}/disk{1...4} /tmp/minio2/{5...8}/disk{1...4} >/tmp/minio2_1.log 2>&1 &
echo "done"

if [ ! -f ./mc ]; then
	echo -n "Downloading MinIO client ..."
	wget -O mc https://dl.min.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
	echo "done"
fi

export MC_HOST_minio1=https://minio:minio123@localhost:9001
export MC_HOST_minio2=https://minio:minio123@localhost:9002

./mc ready minio1 --insecure
./mc ready minio2 --insecure

# Prepare data for tests
echo -n "Preparing test data ..."
mkdir -p /tmp/data
echo "Hello world" >/tmp/data/plainfile
echo "Hello from encrypted world" >/tmp/data/encrypted
touch /tmp/data/defpartsize
shred -s 500M /tmp/data/defpartsize
touch /tmp/data/mpartobj.txt
shred -s 500M /tmp/data/mpartobj.txt
echo "done"

# Enable compression for site minio1
./mc admin config set minio1 compression enable=on extensions=".txt" --insecure
./mc admin config set minio1 compression allow_encryption=off --insecure

# Create bucket in source cluster
echo "Create bucket in source MinIO instance"
./mc mb minio1/test-bucket --insecure

# Load objects to source site
echo "Loading objects to source MinIO instance"
./mc cp /tmp/data/plainfile minio1/test-bucket --insecure
./mc cp /tmp/data/encrypted minio1/test-bucket/encrypted --enc-c "minio1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure
./mc cp /tmp/data/defpartsize minio1/test-bucket/defpartsize --enc-c "minio1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure

# Below should fail as compression and SSEC used at the same time
# DISABLED: We must check the response header to see if compression was actually applied
#RESULT=$({ ./mc put /tmp/data/mpartobj.txt minio1/test-bucket/mpartobj.txt --enc-c "minio1/test-bucket/mpartobj.txt=${TEST_MINIO_ENC_KEY}" --insecure; } 2>&1)
#if [[ ${RESULT} != *"Server side encryption specified with SSE-C with compression not allowed"* ]]; then
#	echo "BUG: Loading an SSE-C object to site with compression should fail. Succeeded though."
#	exit_1
#fi

# Add replication site
./mc admin replicate add minio1 minio2 --insecure
# sleep for replication to complete
sleep 30

# List the objects from source site
echo "Objects from source instance"
./mc ls minio1/test-bucket --insecure
count1=$(./mc ls minio1/test-bucket/plainfile --insecure | wc -l)
if [ "${count1}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/plainfile not found"
	exit_1
fi
count2=$(./mc ls minio1/test-bucket/encrypted --insecure | wc -l)
if [ "${count2}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/encrypted not found"
	exit_1
fi
count3=$(./mc ls minio1/test-bucket/defpartsize --insecure | wc -l)
if [ "${count3}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/defpartsize not found"
	exit_1
fi
sleep 120

# List the objects from replicated site
echo "Objects from replicated instance"
./mc ls minio2/test-bucket --insecure
repcount1=$(./mc ls minio2/test-bucket/plainfile --insecure | wc -l)
if [ "${repcount1}" -ne 1 ]; then
	echo "BUG: object test-bucket/plainfile not replicated"
	exit_1
fi
repcount2=$(./mc ls minio2/test-bucket/encrypted --insecure | wc -l)
if [ "${repcount2}" -ne 1 ]; then
	echo "BUG: object test-bucket/encrypted not replicated"
	exit_1
fi
repcount3=$(./mc ls minio2/test-bucket/defpartsize --insecure | wc -l)
if [ "${repcount3}" -ne 1 ]; then
	echo "BUG: object test-bucket/defpartsize not replicated"
	exit_1
fi

# Stat the SSEC objects from source site
echo "Stat minio1/test-bucket/encrypted"
./mc stat --no-list minio1/test-bucket/encrypted --enc-c "minio1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out1=$(./mc stat --no-list minio1/test-bucket/encrypted --enc-c "minio1/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj1_etag=$(echo "${stat_out1}" | jq '.etag')
src_obj1_size=$(echo "${stat_out1}" | jq '.size')
src_obj1_md5=$(echo "${stat_out1}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat minio1/test-bucket/defpartsize"
./mc stat --no-list minio1/test-bucket/defpartsize --enc-c "minio1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out2=$(./mc stat --no-list minio1/test-bucket/defpartsize --enc-c "minio1/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
src_obj2_etag=$(echo "${stat_out2}" | jq '.etag')
src_obj2_size=$(echo "${stat_out2}" | jq '.size')
src_obj2_md5=$(echo "${stat_out2}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Stat the SSEC objects from replicated site
echo "Stat minio2/test-bucket/encrypted"
./mc stat --no-list minio2/test-bucket/encrypted --enc-c "minio2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out1_rep=$(./mc stat --no-list minio2/test-bucket/encrypted --enc-c "minio2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj1_etag=$(echo "${stat_out1_rep}" | jq '.etag')
rep_obj1_size=$(echo "${stat_out1_rep}" | jq '.size')
rep_obj1_md5=$(echo "${stat_out1_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')
echo "Stat minio2/test-bucket/defpartsize"
./mc stat --no-list minio2/test-bucket/defpartsize --enc-c "minio2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json
stat_out2_rep=$(./mc stat --no-list minio2/test-bucket/defpartsize --enc-c "minio2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure --json)
rep_obj2_etag=$(echo "${stat_out2_rep}" | jq '.etag')
rep_obj2_size=$(echo "${stat_out2_rep}" | jq '.size')
rep_obj2_md5=$(echo "${stat_out2_rep}" | jq '.metadata."X-Amz-Server-Side-Encryption-Customer-Key-Md5"')

# Check the etag and size of replicated SSEC objects
if [ "${rep_obj1_etag}" != "${src_obj1_etag}" ]; then
	echo "BUG: Etag: '${rep_obj1_etag}' of replicated object: 'minio2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_etag}'"
	exit_1
fi
if [ "${rep_obj1_size}" != "${src_obj1_size}" ]; then
	echo "BUG: Size: '${rep_obj1_size}' of replicated object: 'minio2/test-bucket/encrypted' doesn't match with source value: '${src_obj1_size}'"
	exit_1
fi
if [ "${rep_obj2_etag}" != "${src_obj2_etag}" ]; then
	echo "BUG: Etag: '${rep_obj2_etag}' of replicated object: 'minio2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_etag}'"
	exit_1
fi
if [ "${rep_obj2_size}" != "${src_obj2_size}" ]; then
	echo "BUG: Size: '${rep_obj2_size}' of replicated object: 'minio2/test-bucket/defpartsize' doesn't match with source value: '${src_obj2_size}'"
	exit_1
fi

# Check content of replicated SSEC objects
./mc cat minio2/test-bucket/encrypted --enc-c "minio2/test-bucket/encrypted=${TEST_MINIO_ENC_KEY}" --insecure
./mc cat minio2/test-bucket/defpartsize --enc-c "minio2/test-bucket/defpartsize=${TEST_MINIO_ENC_KEY}" --insecure >/dev/null || exit_1

# Check the MD5 checksums of encrypted objects from source and target
if [ "${src_obj1_md5}" != "${rep_obj1_md5}" ]; then
	echo "BUG: MD5 checksum of object 'minio2/test-bucket/encrypted' doesn't match with source. Expected: '${src_obj1_md5}', Found: '${rep_obj1_md5}'"
	exit_1
fi
if [ "${src_obj2_md5}" != "${rep_obj2_md5}" ]; then
	echo "BUG: MD5 checksum of object 'minio2/test-bucket/defpartsize' doesn't match with source. Expected: '${src_obj2_md5}', Found: '${rep_obj2_md5}'"
	exit_1
fi

cleanup
