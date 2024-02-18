#!/bin/bash

set -x

## change working directory
cd .github/workflows/multipart/

function cleanup() {
	docker-compose -f docker-compose-site1.yaml rm -s -f || true
	docker-compose -f docker-compose-site2.yaml rm -s -f || true
	for volume in $(docker volume ls -q | grep minio); do
		docker volume rm ${volume} || true
	done

	docker system prune -f || true
	docker volume prune -f || true
	docker volume rm $(docker volume ls -q -f dangling=true) || true
}

cleanup

if [ ! -f ./mc ]; then
	wget --quiet -O mc https://dl.minio.io/client/mc/release/linux-amd64/mc &&
		chmod +x mc
fi

(
	cd ./docs/debugging/s3-check-md5
	go install -v
)

export RELEASE=RELEASE.2023-08-29T23-07-35Z

docker-compose -f docker-compose-site1.yaml up -d
docker-compose -f docker-compose-site2.yaml up -d

sleep 30s

./mc alias set site1 http://site1-nginx:9001 minioadmin minioadmin --api s3v4
./mc alias set site2 http://site2-nginx:9002 minioadmin minioadmin --api s3v4

./mc ready site1/
./mc ready site2/

./mc admin replicate add site1 site2
./mc mb site1/testbucket/
./mc cp -r --quiet /usr/bin site1/testbucket/

sleep 5

s3-check-md5 -h

failed_count_site1=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site1-nginx:9001 -bucket testbucket 2>&1 | grep FAILED | wc -l)
failed_count_site2=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site2-nginx:9002 -bucket testbucket 2>&1 | grep FAILED | wc -l)

if [ $failed_count_site1 -ne 0 ]; then
	echo "failed with multipart on site1 uploads"
	exit 1
fi

if [ $failed_count_site2 -ne 0 ]; then
	echo "failed with multipart on site2 uploads"
	exit 1
fi

./mc cp -r --quiet /usr/bin site1/testbucket/

sleep 5

failed_count_site1=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site1-nginx:9001 -bucket testbucket 2>&1 | grep FAILED | wc -l)
failed_count_site2=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site2-nginx:9002 -bucket testbucket 2>&1 | grep FAILED | wc -l)

## we do not need to fail here, since we are going to test
## upgrading to master, healing and being able to recover
## the last version.
if [ $failed_count_site1 -ne 0 ]; then
	echo "failed with multipart on site1 uploads ${failed_count_site1}"
fi

if [ $failed_count_site2 -ne 0 ]; then
	echo "failed with multipart on site2 uploads ${failed_count_site2}"
fi

export RELEASE=${1}

docker-compose -f docker-compose-site1.yaml up -d
docker-compose -f docker-compose-site2.yaml up -d

./mc ready site1/
./mc ready site2/

for i in $(seq 1 10); do
	# mc admin heal -r --remove when used against a LB endpoint
	# behaves flaky, let this run 10 times before giving up
	./mc admin heal -r --remove --json site1/ 2>&1 >/dev/null
	./mc admin heal -r --remove --json site2/ 2>&1 >/dev/null
done

failed_count_site1=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site1-nginx:9001 -bucket testbucket 2>&1 | grep FAILED | wc -l)
failed_count_site2=$(s3-check-md5 -versions -access-key minioadmin -secret-key minioadmin -endpoint http://site2-nginx:9002 -bucket testbucket 2>&1 | grep FAILED | wc -l)

if [ $failed_count_site1 -ne 0 ]; then
	echo "failed with multipart on site1 uploads"
	exit 1
fi

if [ $failed_count_site2 -ne 0 ]; then
	echo "failed with multipart on site2 uploads"
	exit 1
fi

cleanup

## change working directory
cd ../../../
