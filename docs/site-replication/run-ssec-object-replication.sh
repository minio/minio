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

# Create certificates for TLS enabled MinIO
echo -n "Setup certs for MinIO instances ..."
wget -O certgen https://github.com/minio/certgen/releases/latest/download/certgen-linux-amd64 && chmod +x certgen
./certgen --host localhost
mkdir -p ~/.minio/certs
mv public.crt ~/.minio/certs || sudo mv public.crt ~/.minio/certs
mv private.key ~/.minio/certs || sudo mv private.key ~/.minio/certs
echo "done"

# Start MinIO instances
echo -n "Starting MinIO instances ..."
minio server --address ":9001" --console-address ":10000" /tmp/minio1/{1...4} >/tmp/minio1_1.log 2>&1 &
minio server --address ":9002" --console-address ":11000" /tmp/minio2/{1...4} >/tmp/minio2_1.log 2>&1 &
echo "done"

if [ ! -f ./mc ]; then
	echo -n "Downloading MinIO client ..."
	wget -O mc https://dl.min.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
	echo "done"
fi

sleep 10

export MC_HOST_minio1=https://minio:minio123@localhost:9001
export MC_HOST_minio2=https://minio:minio123@localhost:9002

# Prepare data for tests
echo -n "Preparing test data ..."
mkdir -p /tmp/data
echo "Hello world" >/tmp/data/plainfile
echo "Hello from encrypted world" >/tmp/data/encrypted
for index in {1..10000000}; do echo "$index - The quick brown fox jumps over the lazy dog" >>/tmp/data/defpartsize; done
for index in {1..10000000}; do echo "$index - An apple a day keeps the doctor away - if you throw it hard" >>/tmp/data/custpartsize; done
echo "done"

# Create bucket in source cluster
echo "Create bucket in source MinIO instance"
./mc mb minio1/test-bucket --insecure

# Load objects to source site
echo "Loading objects to source MinIO instance"
./mc cp /tmp/data/plainfile minio1/test-bucket --insecure
./mc cp /tmp/data/encrypted minio1/test-bucket --encrypt-key "minio1/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure
./mc cp /tmp/data/defpartsize minio1/test-bucket --encrypt-key "minio1/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure
./mc put /tmp/data/custpartsize minio1/test-bucket --encrypt-key "minio1/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure --part-size 50MiB

# Add replication site
./mc admin replicate add minio1 minio2 --insecure
# sleep for replication to complete
sleep 60

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
count4=$(./mc ls minio1/test-bucket/custpartsize --insecure | wc -l)
if [ "${count4}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/custpartsize not found"
	exit_1
fi

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

repcount4=$(./mc ls minio2/test-bucket/custpartsize --insecure | wc -l)
if [ "${repcount4}" -ne 1 ]; then
	echo "BUG: object test-bucket/custpartsize not replicated"
	exit_1
fi

# Stat the SSEC objects from source site
echo "Stat minio1/test-bucket/encrypted"
./mc stat minio1/test-bucket/encrypted --encrypt-key "minio1/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out1=$(./mc stat minio1/test-bucket/encrypted --encrypt-key "minio1/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure --json)
src_obj1_etag=$(echo "${stat_out1}" | jq '.etag')
src_obj1_size=$(echo "${stat_out1}" | jq '.size')
echo "Stat minio1/test-bucket/defpartsize"
./mc stat minio1/test-bucket/defpartsize --encrypt-key "minio1/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out2=$(./mc stat minio1/test-bucket/defpartsize --encrypt-key "minio1/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure --json)
src_obj2_etag=$(echo "${stat_out2}" | jq '.etag')
src_obj2_size=$(echo "${stat_out2}" | jq '.size')
echo "Stat minio1/test-bucket/custpartsize"
./mc stat minio1/test-bucket/custpartsize --encrypt-key "minio1/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out3=$(./mc stat minio1/test-bucket/custpartsize --encrypt-key "minio1/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure --json)
src_obj3_etag=$(echo "${stat_out3}" | jq '.etag')
src_obj3_size=$(echo "${stat_out3}" | jq '.size')

# Stat the SSEC objects from replicated site
echo "Stat minio2/test-bucket/encrypted"
./mc stat minio2/test-bucket/encrypted --encrypt-key "minio2/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out1_rep=$(./mc stat minio2/test-bucket/encrypted --encrypt-key "minio2/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure --json)
rep_obj1_etag=$(echo "${stat_out1_rep}" | jq '.etag')
rep_obj1_size=$(echo "${stat_out1_rep}" | jq '.size')
echo "Stat minio2/test-bucket/defpartsize"
./mc stat minio2/test-bucket/defpartsize --encrypt-key "minio2/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out2_rep=$(./mc stat minio2/test-bucket/defpartsize --encrypt-key "minio2/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure --json)
rep_obj2_etag=$(echo "${stat_out2_rep}" | jq '.etag')
rep_obj2_size=$(echo "${stat_out2_rep}" | jq '.size')
echo "Stat minio2/test-bucket/custpartsize"
./mc stat minio2/test-bucket/custpartsize --encrypt-key "minio2/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure --json
stat_out3_rep=$(./mc stat minio2/test-bucket/custpartsize --encrypt-key "minio2/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure --json)
rep_obj3_etag=$(echo "${stat_out3_rep}" | jq '.etag')
rep_obj3_size=$(echo "${stat_out3_rep}" | jq '.size')

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
if [ "${rep_obj3_etag}" != "${src_obj3_etag}" ]; then
	echo "BUG: Etag: '${rep_obj3_etag}' of replicated object: 'minio2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_etag}'"
	exit_1
fi
if [ "${rep_obj3_size}" != "${src_obj3_size}" ]; then
	echo "BUG: Size: '${rep_obj3_size}' of replicated object: 'minio2/test-bucket/custpartsize' doesn't match with source value: '${src_obj3_size}'"
	exit_1
fi

# Check content of replicated SSEC objects
./mc cat minio2/test-bucket/encrypted --encrypt-key "minio2/test-bucket/encrypted=iliketobecrazybutnotsomuchreally" --insecure
./mc cat minio2/test-bucket/defpartsize --encrypt-key "minio2/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure > /dev/null || exit_1
./mc cat minio2/test-bucket/custpartsize --encrypt-key "minio2/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure > /dev/null || exit_1

# Check last line of multi part objects and if last line replicated well, object is replicated successfully
rep_obj2_content=$(./mc cat minio2/test-bucket/defpartsize --encrypt-key "minio2/test-bucket/defpartsize=iliketobecrazybutnotsomuchreally" --insecure | head -10000000 | tail -1)
rep_obj3_content=$(./mc cat minio2/test-bucket/custpartsize --encrypt-key "minio2/test-bucket/custpartsize=iliketobecrazybutnotsomuchreally" --insecure | head -10000000 | tail -1)
if [ "${rep_obj2_content}" != "10000000 - The quick brown fox jumps over the lazy dog" ]; then
       echo "BUG: Content: '${rep_obj2_content}' not valid for replicated object 'minio2/test-bucket/defpartsize'. Expected: '1 - The quick brown fox jumps over the lazy dog'"
       exit_1
fi
if [ "${rep_obj3_content}" != "10000000 - An apple a day keeps the doctor away - if you throw it hard" ]; then
       echo "BUG: Content: '${rep_obj3_content}' not valid for replicated object 'minio2/test-bucket/custpartsize'. Expected: '1 - An apple a day keeps the doctor away - if you throw it hard'"
       exit_1
fi
