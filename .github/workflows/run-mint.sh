#!/bin/bash

set -ex

export MODE="$1"
export ACCESS_KEY="$2"
export SECRET_KEY="$3"
export JOB_NAME="$4"
export MINT_MODE="full"

docker system prune -f
docker volume prune -f

## change working directory
cd .github/workflows/mint

docker-compose -f minio-${MODE}.yaml up -d
sleep 5m

docker run --rm --net=host \
       --name="mint-${MODE}-${JOB_NAME}" \
       -e SERVER_ENDPOINT="127.0.0.1:9000" \
       -e ACCESS_KEY="${ACCESS_KEY}" \
       -e SECRET_KEY="${SECRET_KEY}" \
       -e ENABLE_HTTPS=0 \
       -e MINT_MODE="${MINT_MODE}" \
       docker.io/minio/mint:edge \
	 aws-sdk-go   \
	 aws-sdk-java \
	 aws-sdk-php  \
	 aws-sdk-ruby \
	 awscli       \
	 healthcheck  \
	 mc           \
	 minio-go     \
	 minio-java   \
	 minio-js     \
	 minio-py     \
	 s3cmd        \
	 s3select     \
	 versioning

docker-compose -f minio-${MODE}.yaml down
sleep 10s

docker system prune -f
docker volume prune -f

## change working directory
cd ../../../

