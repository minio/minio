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
	pkill -9 minio || sudo pkill -9 minio
	rm -rf /tmp/minio{1,2}
	echo "done"
}

# Function to convert number to corresponding alphabet
num_to_alpha() {
	local num=$1
	# ASCII value of 'a' is 97, so we add (num - 1) to 97 to get the corresponding alphabet
	local ascii_value=$((96 + num))
	# Convert the ASCII value to the character using printf
	printf "\\$(printf '%03o' "$ascii_value")"
}

cleanup

export MINIO_CI_CD=1
export MINIO_BROWSER=off
export MINIO_ROOT_USER="minio"
export MINIO_ROOT_PASSWORD="minio123"

# Download AWS CLI
echo -n "Download and install AWS CLI"
rm -rf /usr/local/aws-cli || sudo rm -rf /usr/local/aws-cli
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -qq awscliv2.zip
./aws/install || sudo ./aws/install
echo "done"

# Add credentials to ~/.aws/credentials
if ! [ -d ~/.aws ]; then
	mkdir -p ~/.aws
fi
cat >~/.aws/credentials <<EOF
[enterprise]
region = us-east-1
aws_access_key_id = minio
aws_secret_access_key = minio123
EOF

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
CI=on MINIO_KMS_SECRET_KEY=minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=minio123 minio server --certs-dir /tmp/certs --address ":9001" --console-address ":10000" /tmp/minio1/{1...4}/disk{1...4} /tmp/minio1/{5...8}/disk{1...4} >/tmp/minio1_1.log 2>&1 &
CI=on MINIO_KMS_SECRET_KEY=minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g= MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=minio123 minio server --certs-dir /tmp/certs --address ":9002" --console-address ":11000" /tmp/minio2/{1...4}/disk{1...4} /tmp/minio2/{5...8}/disk{1...4} >/tmp/minio2_1.log 2>&1 &
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
echo "Hello World" >/tmp/data/obj
touch /tmp/data/mpartobj
shred -s 500M /tmp/data/mpartobj
echo "done"

# Add replication site
./mc admin replicate add minio1 minio2 --insecure
# sleep for replication to complete
sleep 30

# Create bucket in source cluster
echo "Create bucket in source MinIO instance"
./mc mb minio1/test-bucket --insecure

# Load objects to source site with checksum header
echo "Loading objects to source MinIO instance"
OBJ_CHKSUM=$(openssl dgst -sha256 -binary </tmp/data/obj | base64)
aws s3api --endpoint-url=https://localhost:9001 put-object --checksum-algorithm SHA256 --checksum-sha256 "${OBJ_CHKSUM}" --bucket test-bucket --key obj --body /tmp/data/obj --no-verify-ssl --profile enterprise

split -n 10 /tmp/data/mpartobj
CREATE_MPART_OUT=$(aws s3api --endpoint-url=https://localhost:9001 create-multipart-upload --bucket test-bucket --key mpartobj --checksum-algorithm SHA256 --no-verify-ssl --profile enterprise)
UPLOAD_ID=$(echo "${CREATE_MPART_OUT}" | jq '.UploadId' | sed 's/"//g')

PARTS=""
for idx in {1..10}; do
	F_SUFFIX=$(num_to_alpha "$idx")
	PART_CHKSUM=$(openssl dgst -sha256 -binary <"xa${F_SUFFIX}" | base64)
	UPLOAD_PART_OUT=$(aws s3api --endpoint-url=https://localhost:9001 upload-part --checksum-algorithm SHA256 --checksum-sha256 "${PART_CHKSUM}" --bucket test-bucket --key mpartobj --part-number "${idx}" --body "xa${F_SUFFIX}" --upload-id "${UPLOAD_ID}" --no-verify-ssl --profile enterprise)
	PART_ETAG=$(echo "${UPLOAD_PART_OUT}" | jq '.ETag')
	if [ "${idx}" == 10 ]; then
		PARTS="${PARTS}{\"ETag\": ${PART_ETAG}, \"ChecksumSHA256\": \"${PART_CHKSUM}\", \"PartNumber\": ${idx}}"
	else
		PARTS="${PARTS}{\"ETag\": ${PART_ETAG}, \"ChecksumSHA256\": \"${PART_CHKSUM}\", \"PartNumber\": ${idx}},"
	fi
done

echo "{\"Parts\":[${PARTS}]}" >fileparts.json
jq <fileparts.json
aws s3api --endpoint-url=https://localhost:9001 complete-multipart-upload --multipart-upload file://fileparts.json --bucket test-bucket --key mpartobj --upload-id "${UPLOAD_ID}" --no-verify-ssl --profile enterprise
sleep 120

# List the objects from replicated site
echo "Objects from replicated site"
./mc ls minio2/test-bucket --insecure
count1=$(./mc ls minio2/test-bucket/obj --insecure | wc -l)
if [ "${count1}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/obj not replicated"
	exit_1
fi
count2=$(./mc ls minio2/test-bucket/mpartobj --insecure | wc -l)
if [ "${count2}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/mpartobj not replicated"
	exit_1
fi

# Stat the objects from source site
echo "Stat minio1/test-bucket/obj"
SRC_OUT_1=$(aws s3api --endpoint-url https://localhost:9001 head-object --bucket test-bucket --key obj --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
SRC_OUT_2=$(aws s3api --endpoint-url https://localhost:9001 head-object --bucket test-bucket --key mpartobj --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
SRC_OBJ_1_CHKSUM=$(echo "${SRC_OUT_1}" | jq '.ChecksumSHA256')
SRC_OBJ_2_CHKSUM=$(echo "${SRC_OUT_2}" | jq '.ChecksumSHA256')
SRC_OBJ_1_ETAG=$(echo "${SRC_OUT_1}" | jq '.ETag')
SRC_OBJ_2_ETAG=$(echo "${SRC_OUT_2}" | jq '.ETag')

# Stat the objects from replicated site
echo "Stat minio2/test-bucket/obj"
DEST_OUT_1=$(aws s3api --endpoint-url https://localhost:9002 head-object --bucket test-bucket --key obj --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
DEST_OUT_2=$(aws s3api --endpoint-url https://localhost:9002 head-object --bucket test-bucket --key mpartobj --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
DEST_OBJ_1_CHKSUM=$(echo "${DEST_OUT_1}" | jq '.ChecksumSHA256')
DEST_OBJ_2_CHKSUM=$(echo "${DEST_OUT_2}" | jq '.ChecksumSHA256')
DEST_OBJ_1_ETAG=$(echo "${DEST_OUT_1}" | jq '.ETag')
DEST_OBJ_2_ETAG=$(echo "${DEST_OUT_2}" | jq '.ETag')

# Check the replication of checksums and etags
if [[ ${SRC_OBJ_1_CHKSUM} != "${DEST_OBJ_1_CHKSUM}" && ${DEST_OBJ_1_CHKSUM} != "" ]]; then
	echo "BUG: Checksums dont match for 'obj'. Source: ${SRC_OBJ_1_CHKSUM}, Destination: ${DEST_OBJ_1_CHKSUM}"
	exit_1
fi
if [[ ${SRC_OBJ_2_CHKSUM} != "${DEST_OBJ_2_CHKSUM}" && ${DEST_OBJ_2_CHKSUM} != "" ]]; then
	echo "BUG: Checksums dont match for 'mpartobj'. Source: ${SRC_OBJ_2_CHKSUM}, Destination: ${DEST_OBJ_2_CHKSUM}"
	exit_1
fi
if [ "${SRC_OBJ_1_ETAG}" != "${DEST_OBJ_1_ETAG}" ]; then
	echo "BUG: Etags dont match for 'obj'. Source: ${SRC_OBJ_1_ETAG}, Destination: ${DEST_OBJ_1_ETAG}"
	exit_1
fi
if [ "${SRC_OBJ_2_ETAG}" != "${DEST_OBJ_2_ETAG}" ]; then
	echo "BUG: Etags dont match for 'mpartobj'. Source: ${SRC_OBJ_2_ETAG}, Destination: ${DEST_OBJ_2_ETAG}"
	exit_1
fi

echo "Set default encryption on "

# test if checksum header is replicated for encrypted objects
# Enable SSE KMS for test-bucket bucket
./mc encrypt set sse-kms minio-default-key minio1/test-bucket --insecure

# Load objects to source site with checksum header
echo "Loading objects to source MinIO instance"
OBJ_CHKSUM=$(openssl dgst -sha256 -binary </tmp/data/obj | base64)
aws s3api --endpoint-url=https://localhost:9001 put-object --checksum-algorithm SHA256 --checksum-sha256 "${OBJ_CHKSUM}" --bucket test-bucket --key obj2 --body /tmp/data/obj --no-verify-ssl --profile enterprise

split -n 10 /tmp/data/mpartobj
CREATE_MPART_OUT=$(aws s3api --endpoint-url=https://localhost:9001 create-multipart-upload --bucket test-bucket --key mpartobj2 --checksum-algorithm SHA256 --no-verify-ssl --profile enterprise)
UPLOAD_ID=$(echo "${CREATE_MPART_OUT}" | jq '.UploadId' | sed 's/"//g')

PARTS=""
for idx in {1..10}; do
	F_SUFFIX=$(num_to_alpha "$idx")
	PART_CHKSUM=$(openssl dgst -sha256 -binary <"xa${F_SUFFIX}" | base64)
	UPLOAD_PART_OUT=$(aws s3api --endpoint-url=https://localhost:9001 upload-part --checksum-algorithm SHA256 --checksum-sha256 "${PART_CHKSUM}" --bucket test-bucket --key mpartobj2 --part-number "${idx}" --body "xa${F_SUFFIX}" --upload-id "${UPLOAD_ID}" --no-verify-ssl --profile enterprise)
	PART_ETAG=$(echo "${UPLOAD_PART_OUT}" | jq '.ETag')
	if [ "${idx}" == 10 ]; then
		PARTS="${PARTS}{\"ETag\": ${PART_ETAG}, \"ChecksumSHA256\": \"${PART_CHKSUM}\", \"PartNumber\": ${idx}}"
	else
		PARTS="${PARTS}{\"ETag\": ${PART_ETAG}, \"ChecksumSHA256\": \"${PART_CHKSUM}\", \"PartNumber\": ${idx}},"
	fi
done

echo "{\"Parts\":[${PARTS}]}" >fileparts.json
jq <fileparts.json
aws s3api --endpoint-url=https://localhost:9001 complete-multipart-upload --multipart-upload file://fileparts.json --bucket test-bucket --key mpartobj2 --upload-id "${UPLOAD_ID}" --no-verify-ssl --profile enterprise
sleep 120

# List the objects from replicated site
echo "Objects from replicated site"
./mc ls minio2/test-bucket --insecure
count1=$(./mc ls minio2/test-bucket/obj2 --insecure | wc -l)
if [ "${count1}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/obj2 not replicated"
	exit_1
fi
count2=$(./mc ls minio2/test-bucket/mpartobj2 --insecure | wc -l)
if [ "${count2}" -ne 1 ]; then
	echo "BUG: object minio1/test-bucket/mpartobj2 not replicated"
	exit_1
fi

# Stat the objects from source site
echo "Stat minio1/test-bucket/obj2"
SRC_OUT_1=$(aws s3api --endpoint-url https://localhost:9001 head-object --bucket test-bucket --key obj2 --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
SRC_OUT_2=$(aws s3api --endpoint-url https://localhost:9001 head-object --bucket test-bucket --key mpartobj2 --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
SRC_OBJ_1_CHKSUM=$(echo "${SRC_OUT_1}" | jq '.ChecksumSHA256')
SRC_OBJ_2_CHKSUM=$(echo "${SRC_OUT_2}" | jq '.ChecksumSHA256')
SRC_OBJ_1_ETAG=$(echo "${SRC_OUT_1}" | jq '.ETag')
SRC_OBJ_2_ETAG=$(echo "${SRC_OUT_2}" | jq '.ETag')

# Stat the objects from replicated site
echo "Stat minio2/test-bucket/obj2"
DEST_OUT_1=$(aws s3api --endpoint-url https://localhost:9002 head-object --bucket test-bucket --key obj2 --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
DEST_OUT_2=$(aws s3api --endpoint-url https://localhost:9002 head-object --bucket test-bucket --key mpartobj2 --checksum-mode ENABLED --no-verify-ssl --profile enterprise)
DEST_OBJ_1_CHKSUM=$(echo "${DEST_OUT_1}" | jq '.ChecksumSHA256')
DEST_OBJ_2_CHKSUM=$(echo "${DEST_OUT_2}" | jq '.ChecksumSHA256')
DEST_OBJ_1_ETAG=$(echo "${DEST_OUT_1}" | jq '.ETag')
DEST_OBJ_2_ETAG=$(echo "${DEST_OUT_2}" | jq '.ETag')

# Check the replication of checksums and etags
if [[ ${SRC_OBJ_1_CHKSUM} != "${DEST_OBJ_1_CHKSUM}" && ${DEST_OBJ_1_CHKSUM} != "" ]]; then
	echo "BUG: Checksums dont match for 'obj2'. Source: ${SRC_OBJ_1_CHKSUM}, Destination: ${DEST_OBJ_1_CHKSUM}"
	exit_1
fi
if [[ ${SRC_OBJ_2_CHKSUM} != "${DEST_OBJ_2_CHKSUM}" && ${DEST_OBJ_2_CHKSUM} != "" ]]; then
	echo "BUG: Checksums dont match for 'mpartobj2'. Source: ${SRC_OBJ_2_CHKSUM}, Destination: ${DEST_OBJ_2_CHKSUM}"
	exit_1
fi
if [ "${SRC_OBJ_1_ETAG}" != "${DEST_OBJ_1_ETAG}" ]; then
	echo "BUG: Etags dont match for 'obj2'. Source: ${SRC_OBJ_1_ETAG}, Destination: ${DEST_OBJ_1_ETAG}"
	exit_1
fi
if [ "${SRC_OBJ_2_ETAG}" != "${DEST_OBJ_2_ETAG}" ]; then
	echo "BUG: Etags dont match for 'mpartobj2'. Source: ${SRC_OBJ_2_ETAG}, Destination: ${DEST_OBJ_2_ETAG}"
	exit_1
fi

cleanup
